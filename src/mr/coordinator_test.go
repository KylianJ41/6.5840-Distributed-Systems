package mr

import "testing"

func TestCoordinatorAssignsMapTask(t *testing.T) {
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	nReduce := 2
	c := MakeCoordinator(files, nReduce)

	testMapPhase(t, c, files, nReduce)
	testWaitBeforeReducePhase(t, c)
	c.CompleteAllMapTasks()
	testReducePhase(t, c, nReduce, len(files))
	testAllTasksCompleted(t, c)
}

func testMapPhase(t *testing.T, c *Coordinator, files []string, nReduce int) {
	for i := 0; i < len(files); i++ {
		reply := getTask(t, c)

		if reply.TaskType != MapTask {
			t.Errorf("Expected map task, got %s", reply.TaskType)
		}

		if reply.FileName != files[i] {
			t.Errorf("Expected file1.txt, got %s", reply.FileName)
		}

		if reply.TaskID != i {
			t.Errorf("Expected TaskID 0, got %d", reply.TaskID)
		}

		if reply.NReduce != nReduce {
			t.Errorf("Expected NReduce 3, got %d", reply.NReduce)
		}
	}
}

func testWaitBeforeReducePhase(t *testing.T, c *Coordinator) {
	reply := getTask(t, c)
	if reply.TaskType != WaitTask {
		t.Errorf("Expected wait task after %s, got %s", MapTask, reply.TaskType)
	}
}

func testReducePhase(t *testing.T, c *Coordinator, nReduce, nMap int) {
	for i := 0; i < nReduce; i++ {
		reply := getTask(t, c)

		if reply.TaskType != ReduceTask {
			t.Errorf("Expected reduce task, got %s", reply.TaskType)
		}
		if reply.TaskID != i {
			t.Errorf("Expected TaskID %d, got %d", i, reply.TaskID)
		}
		if reply.NMap != nMap {
			t.Errorf("Expected NMap %d, got %d", nMap, reply.NMap)
		}
	}
}

func testAllTasksCompleted(t *testing.T, c *Coordinator) {
	for i := range c.reduceTasks {
		c.reduceTasks[i].State = Completed
	}

	reply := getTask(t, c)
	if reply.TaskType != DoneTask {
		t.Errorf("Expected done task, got %s", reply.TaskType)
	}
}

func getTask(t *testing.T, c *Coordinator) *GetTaskReply {
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}
	err := c.GetTask(args, reply)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}
	return reply
}
