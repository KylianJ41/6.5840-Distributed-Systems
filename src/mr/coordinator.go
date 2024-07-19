package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

// TaskState constants
const (
	Idle TaskState = iota // is 0, Idle is the name of the var and TaskState the type
	InProgress // is 1
	Completed // is 2
)

// TaskType constants
const (
	MapTask = "map"
	ReduceTask = "reduce"
	WaitTask = "wait"
	DoneTask = "done"
)

// PhaseType constants
const (
	MapPhase = "map"
	ReducePhase = "phase"
)


type Task struct {
	ID int
	State TaskState
	Type string
	File string // For Map tasks
	StartTime time.Time
}

type Coordinator struct {
	mu sync.Mutex
	nReduce int
	mapTasks []Task
	reduceTasks []Task
	files []string
	jobPhase string // map or reduce
	isShutdown bool

	completedMapTasks int
	completedReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil // if we return nil, means there was no error !
}

func (c *Coordinator) markTaskCompleted(taskType string, taskID int) {
	if taskType == MapTask {
		if c.mapTasks[taskID].State == InProgress {
			c.mapTasks[taskID].State = Completed
			c.completedMapTasks++
		}
	} else if taskType == ReduceTask {
		if c.reduceTasks[taskID].State == InProgress {
			c.reduceTasks[TaskID].State = Completed
			c.completedReduceTasks++
		}
	}
}

func (c *Coordinator) ReportTaskCompletion(args *TaskCompletionArgs, reply TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock
	
	c.markTaskCompleted(args.TaskType, args.TaskID)
	
	if args.TaskType == MapTask && c.completedMapTasks == len(c.mapTasks) {
		c.transitionToReducePhase()
	}
}

func (c *Coordinator) transitionToReducePhase() {
	c.jobPhase = ReducePhase
	
	log.Printf("[coordinator] Transitioning to Reduce phase. %d MapTasks ompleted.\n", len(c.mapTasks))
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.jobPhase == MapTask {
		
		for i, task := range c.mapTasks {
			if task.State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskID = task.ID
				reply.FileName = c.files[task.ID]
				reply.NReduce = c.nReduce
				return nil
			}
		}
		
		reply.TaskType = WaitTask
		return nil
	}
	
	if c.jobPhase == ReduceTask {
		
		for i, task := range c.reduceTasks {
			if task.State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskID = task.ID
				reply.NMap = len(c.MapTasks)
				return nil
			}
		}
		
		reply.TaskType = WaitTask
		return nil
	}
	
	// If we reach here, all tasks are completed
	reply.TaskType = DoneTask
	return nil
}

func (c *Coordinator) checkAndResetTask(Task *Task, now time.Time, timeout time.Duration) {
	if task.State == InProgress && now.Sub(task.StartTime) > timeout {
		task.State = Idle
		task.StartTime = time.Time[} // Reset start time
	}
}

func (c *Coordinator) checkTaskTimeouts() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		if c.isShutdown {
			c.mu.Unlock()
			return
		}
		
		now := time.Now()
		timeout := 10 * time.Second
		
		if c.jobPhase == MapPhase {
			for i := range c.mapTasks {
				c.checkAndResetTask(&c.mapTasks[i], now, timeout)
			}
		} else if c.jobPhase == ReducePhase {
			for i:= range c.reduceTasks {
				c.checkAndResetTask(&c.reduceTasks[i], now, timeout)			
			}	
		}
		
		c.mu.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c) // register the coordinator as an RPC handler
	rpc.HandleHTTP() // says that we'll be using HTTP as the transport protocol/sets up rpc server to handle requests over HTTP
	//l, e := net.Listen("tcp", ":1234") // this would be used if we were to use MapReduce on multiple computers
	sockname := coordinatorSock()
	os.Remove(sockname) // cleaning any existing socket file w/ same name
	l, e := net.Listen("unix", sockname) // since we will use MapReduce locally, we use unix domain socket
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // start the listening thread
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool { // might want to optimize this later	
	c.mu.Lock()
	defer c.mu.Unlock()
	isDone := c.jobPhase == ReducePhase && c.completedReduceTasks == c.nReduce
	if isDone {
		c.isShutdown = true
	}	
	return isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// If we have nReduce = 10, it means we'll have 10 output files produced during the reduce phase.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// initialize the coordinator struct
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, len(nReduce)),
		phase: MapPhase,
	}

	
	// setup tasks (map and reduce)
	for i := range c.MapTasks {
		c.mapTasks[i] = Task{
			ID: i,
			File: files[i],
			State: Idle,
			Type: MapTask
		}
	}
	
	for i:= range c.reduceTasks {
		c.reduceTasks[i] = Task{
			ID: i, //len(files) + i,
			State: Idle,
			Type: ReduceTask,
		}
	}
	
	// initialize other necessary state ?
	
	// go c.checkTaskTimeouts()
	// start the rpc server
	c.server()
	return &c
}
