package mr

import "testing"

type MockCoordinator struct {
	taskGiven bool
}

func (m *MockCoordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.taskGiven = true
	reply.TaskType = "map"
	reply.TaskID = 1
	reply.FileName = "test_file.txt"
	return nil
}

func TestWorkerRequestsTask(t *testing.T) {
	mockCoord := &MockCoordinator{}

	originalRPCCall := rpcCall
	// by putting interface{} as the type of args, we can easily make the func evolve
	// for other type of args (struct) in the future
	rpcCall = func(rpcname string, args interface{}, reply interface{}) bool {
		if rpcname == "Coordinator.GetTask" {
			mockCoord.GetTask(args.(*GetTaskArgs), reply.(*GetTaskReply))
			return true
		}
		return false
	}
	defer func() { rpcCall = originalRPCCall }()

	Worker(nil, nil)

	if !mockCoord.taskGiven {
		t.Errorf("Worker did not request a task from the coordinator")
	}
}
