package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	MapPhase    = "map"
	ReducePhase = "phase"
)

type Task struct {
	ID    int
	State TaskState
	Type  string
	File  string // for map tasks
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	files       []string
	nReduce     int
	jobPhase    string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.jobPhase == MapPhase {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].State = InProgress
				reply.TaskType = c.mapTasks[i].Type
				reply.TaskID = c.mapTasks[i].ID
				reply.FileName = c.mapTasks[i].File
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}

	// If we get here, there are no idle tasks
	reply.TaskType = "wait"
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		jobPhase: MapPhase,
		mapTasks: make([]Task, len(files)),
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

	c.server()
	return &c
}

//////////////////////////////////////////////////////////
// Functions that are used for testing purpose only
