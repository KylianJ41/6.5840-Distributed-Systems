package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	WaitTask   = "wait"
	DoneTask   = "done"
)

const (
	MapPhase       = "map"
	ReducePhase    = "reduce"
	CompletedPhase = "completed"
)

const TaskTimeout = 10 * time.Second

type Task struct {
	ID        int
	State     TaskState
	Type      string
	File      string // for map tasks
	StartTime time.Time
}

type Coordinator struct {
	mu                   sync.Mutex
	mapTasks             []Task
	reduceTasks          []Task
	files                []string
	nReduce              int
	jobPhase             string
	completedMapTasks    int
	completedReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkTimeouts() // Check for timed-out tasks before assigning new ones

	switch c.jobPhase {
	case MapPhase:
		err := c.getMapTask(reply)
		if reply.TaskType != WaitTask {
			log.Printf("Assigned map task %d for file %s\n", reply.TaskID, reply.FileName)
		}
		return err
	case ReducePhase:
		err := c.getReduceTask(reply)
		if reply.TaskType != WaitTask {
			log.Printf("Assigned reduce task %d\n", reply.TaskID)
		}
		return err
	default:
		return c.getFinalTaskState(reply)
	}
}

func (c *Coordinator) getMapTask(reply *GetTaskReply) error {
	for i := range c.mapTasks {
		if c.mapTasks[i].State == Idle {
			c.mapTasks[i].State = InProgress
			c.mapTasks[i].StartTime = time.Now()
			reply.TaskType = c.mapTasks[i].Type
			reply.TaskID = c.mapTasks[i].ID
			reply.FileName = c.mapTasks[i].File
			reply.NReduce = c.nReduce
			return nil
		}
	}
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) getReduceTask(reply *GetTaskReply) error {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == Idle {
			c.reduceTasks[i].State = InProgress
			c.reduceTasks[i].StartTime = time.Now()
			reply.TaskType = ReduceTask
			reply.TaskID = i
			reply.NMap = len(c.mapTasks)
			return nil
		}
	}
	return c.getFinalTaskState(reply)
}

func (c *Coordinator) areAllTasksCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) getFinalTaskState(reply *GetTaskReply) error {
	if c.areAllTasksCompleted() {
		reply.TaskType = DoneTask
	} else {
		reply.TaskType = WaitTask
	}
	return nil
}

func (c *Coordinator) checkTimeouts() {
	now := time.Now()
	if c.jobPhase == MapPhase {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == InProgress && now.Sub(c.mapTasks[i].StartTime) > TaskTimeout {
				c.mapTasks[i].State = Idle
				c.mapTasks[i].StartTime = time.Time{}
				log.Printf("Map task %d timed out and reset to Idle\n", i)
			}
		}
	} else if c.jobPhase == ReducePhase {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == InProgress && now.Sub(c.reduceTasks[i].StartTime) > TaskTimeout {
				c.reduceTasks[i].State = Idle
				c.reduceTasks[i].StartTime = time.Time{}
				log.Printf("Reduce task %d timed out and reset to Idle\n", i)
			}
		}
	}
}

func (c *Coordinator) MarkTaskCompleted(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID < len(c.mapTasks) && c.mapTasks[args.TaskID].State == InProgress {
			c.mapTasks[args.TaskID].State = Completed
			c.completedMapTasks++
			log.Printf("Map task %d completed. Total completed: %d/%d\n",
				args.TaskID, c.completedMapTasks, len(c.mapTasks))
			c.checkMapPhaseCompletion()
			reply.Acknowledged = true
		}
	case ReduceTask:
		if args.TaskID < len(c.reduceTasks) && c.reduceTasks[args.TaskID].State == InProgress {
			c.reduceTasks[args.TaskID].State = Completed
			c.completedReduceTasks++
			log.Printf("Reduce task %d completed. Total completed: %d/%d\n",
				args.TaskID, c.completedReduceTasks, len(c.reduceTasks))
			c.checkReducePhaseCompletion()
			reply.Acknowledged = true
		}
	}

	return nil
}

func (c *Coordinator) checkMapPhaseCompletion() {
	if c.completedMapTasks == len(c.mapTasks) {
		c.jobPhase = ReducePhase
		log.Println("All map tasks completed. Transitioning to Reduce phase.")
	}
}

func (c *Coordinator) checkReducePhaseCompletion() {
	if c.completedReduceTasks == len(c.reduceTasks) {
		c.jobPhase = CompletedPhase
		log.Println("All reduce tasks completed. Job finished.")
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.jobPhase == CompletedPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int, startServer bool) *Coordinator {
	c := Coordinator{
		files:                files,
		nReduce:              nReduce,
		jobPhase:             MapPhase,
		mapTasks:             make([]Task, len(files)),
		reduceTasks:          make([]Task, nReduce),
		completedMapTasks:    0,
		completedReduceTasks: 0,
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			ID:    i,
			State: Idle,
			Type:  MapTask,
			File:  file,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{
			ID:    i,
			State: Idle,
			Type:  ReduceTask,
		}
	}

	log.Printf("Coordinator created with %d map tasks and %d reduce tasks\n", len(files), nReduce)

	if startServer { // to avoid starting the server in unit tests
		c.server()
		log.Println("Coordinator server started")
	}
	return &c
}
