package mr

import "testing"

func TestCoordinatorAssignsMapTask(t *testing.T) {
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	nReduce := 2
	c := MakeCoordinator(files, nReduce)

	for i := 0; i < len(files); i++ {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}

		err := c.GetTask(args, reply)
		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

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

	// Test that we get a wait task after all map tasks are assigned
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}
	err := c.GetTask(args, reply)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if reply.TaskType != WaitTask {
		t.Errorf("Expected wait task, got %s", reply.TaskType)
	}
}
