package mr

import (
	"testing"
	"time"
)

func TestCoordinatorAssignsTasks(t *testing.T) {
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	nReduce := 2
	c := MakeCoordinator(files, nReduce, false)

	testMapPhase(t, c, files, nReduce)
	testWaitBeforeReducePhase(t, c)
	CompleteAllMapTasks(t, c)
	testReducePhase(t, c, nReduce, len(files))
	completeAllReduceTasks(t, c)
	testAllTasksCompleted(t, c)
}

func completeTasksOfType(t *testing.T, c *Coordinator, taskType string) {
	var tasks []Task
	if taskType == MapTask {
		tasks = c.mapTasks
	} else if taskType == ReduceTask {
		tasks = c.reduceTasks
	}

	for i := range tasks {
		args := TaskCompletionArgs{TaskType: taskType, TaskID: i}
		reply := TaskCompletionReply{}
		err := c.MarkTaskCompleted(&args, &reply)
		if err != nil {
			t.Fatalf("Failed to mark %s task as completed: %v", taskType, err)
		}
	}
}

func CompleteAllMapTasks(t *testing.T, c *Coordinator) {
	completeTasksOfType(t, c, MapTask)
}

func completeAllReduceTasks(t *testing.T, c *Coordinator) {
	completeTasksOfType(t, c, ReduceTask)
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
	reply := getTask(t, c)
	if reply.TaskType != DoneTask {
		t.Errorf("Expected done task, got %s", reply.TaskType)
	}
	if !c.Done() {
		t.Errorf("Expected Done() to return true, but got false")
	}
}

func TestTaskTimeout(t *testing.T) {
	files := []string{"file1.txt", "file2.txt"}
	nReduce := 2
	c := MakeCoordinator(files, nReduce, false)

	reply1 := getTask(t, c)
	if reply1.TaskType != MapTask {
		t.Fatalf("Expected map task, got %s", reply1.TaskType)
	}

	// Simulate time passing
	c.mapTasks[0].StartTime = time.Now().Add(-TaskTimeout - time.Second)

	// Get second task, should be the same as the first due to timeout
	reply2 := getTask(t, c)
	if reply2.TaskType != MapTask {
		t.Fatalf("Expected map task, got %s", reply2.TaskType)
	}
	if reply2.TaskID != reply1.TaskID {
		t.Errorf("Expected TaskID %d, got %d", reply1.TaskID, reply2.TaskID)
	}

	// Get third task, should be a new one
	reply3 := getTask(t, c)
	if reply3.TaskType != MapTask {
		t.Fatalf("Expected map task, got %s", reply3.TaskType)
	}
	if reply3.TaskID == reply1.TaskID {
		t.Errorf("Expected different task ID, got %d", reply3.TaskID)
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

func TestDoneFunction(t *testing.T) {
	files := []string{"file1.txt", "file2.txt"}
	nReduce := 2
	c := MakeCoordinator(files, nReduce, false)

	if c.Done() {
		t.Errorf("Expected Done() to return false, but got true")
	}

	// Assign and complete all map tasks
	for i := range c.mapTasks {
		reply := getTask(t, c)
		if reply.TaskType != MapTask {
			t.Fatalf("Expected map task, got %s", reply.TaskType)
		}

		args := TaskCompletionArgs{TaskType: MapTask, TaskID: i}
		completedReply := TaskCompletionReply{}
		err := c.MarkTaskCompleted(&args, &completedReply)
		if err != nil {
			t.Fatalf("Failed to mark map task as completed: %v", err)
		}
	}

	if c.Done() {
		t.Errorf("Expected Done() to return false after Map phase, but got true")
	}

	// Assign and complete all reduce tasks
	for i := 0; i < nReduce; i++ {
		reply := getTask(t, c)
		if reply.TaskType != ReduceTask {
			t.Fatalf("Expected reduce task, got %s", reply.TaskType)
		}

		args := TaskCompletionArgs{TaskType: ReduceTask, TaskID: i}
		completedReply := TaskCompletionReply{}
		err := c.MarkTaskCompleted(&args, &completedReply)
		if err != nil {
			t.Fatalf("Failed to mark reduce task as completed: %v", err)
		}
	}

	if !c.Done() {
		t.Errorf("Expected Done() to return true after all tasks are completed, but got false")
	}
}
