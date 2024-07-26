package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"unicode"
)

type MockCoordinator struct {
	taskGiven bool
	taskType  string
}

func (m *MockCoordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.taskGiven = true
	reply.TaskType = m.taskType
	reply.TaskID = 1
	reply.FileName = "testdata/testfile.txt"
	return nil
}

/*
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
}*/

func TestWriteIntermediateFiles(t *testing.T) {
	mapTaskID := 1
	nReduce := 3
	kva := []KeyValue{
		{"apple", "1"},
		{"banana", "1"},
		{"cherry", "1"},
		{"date", "1"},
		{"elderberry", "1"},
	}

	cleanup := func() {
		for i := 0; i < nReduce; i++ {
			filename := fmt.Sprintf("mr-%d-%d", mapTaskID, i)
			os.Remove(filename)
		}
	}
	defer cleanup()

	writeIntermediateFiles(mapTaskID, nReduce, kva)

	// Check if files were created and contain correct data
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskID, i)
		file, err := os.Open(filename)
		if err != nil {
			t.Errorf("Failed to open file %s: %v", filename, err)
			continue
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		var kvs []KeyValue
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				t.Errorf("Failed to decode KeyValue from file %s: %v", filename, err)
				break
			}
			kvs = append(kvs, kv)
		}

		// Check if the KeyValues in this file all hash to this reduce task
		for _, kv := range kvs {
			if ihash(kv.Key)%nReduce != i {
				t.Errorf("Key %s in file %s doesn't hash to the correct reduce task", kv.Key, filename)
			}
		}
	}
}

func mockMapFunc(filename string, contents string) []KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	kva := make([]KeyValue, 0, len(words))
	for _, word := range words {
		kva = append(kva, KeyValue{Key: word, Value: "1"})
	}
	return kva
}

func mockReduceFunc(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func TestPerformMap(t *testing.T) {
	content := "Hello World\nThis is a test\nMapReduce is cool"
	tmpfile, err := ioutil.TempFile("", "mapinput")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	task := GetTaskReply{
		TaskType: MapTask,
		TaskID:   1,
		FileName: tmpfile.Name(),
		NReduce:  3,
	}

	err = performMap(mockMapFunc, task)
	if err != nil {
		t.Fatalf("performMap failed: %v", err)
	}

	// Chack if intermediate files were created
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Errorf("Intermediate file %s was not created", filename)
		}
		defer os.Remove(filename)
	}

	// Check contents of intermediate files
	expectedWords := map[string]bool{
		"Hello": true, "World": true, "This": true, "is": true, "a": true, "test": true,
		"MapReduce": true, "cool": true,
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		file, err := os.Open(filename)
		if err != nil {
			t.Fatalf("Failed to open file %s: %v", filename, err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		var kv KeyValue
		for dec.More() {
			if err := dec.Decode(&kv); err != nil {
				t.Fatalf("Failed to decode KeyValue from file %s: %v", filename, err)
			}
			if !expectedWords[kv.Key] {
				t.Errorf("Unexpected word in file %s: %s", filename, kv.Key)
			}
			// Check if the value is a number
			if kv.Value != "1" {
				t.Errorf("Expected value '1' for key %s, got %s", kv.Key, kv.Value)
			}
		}
	}

}

func TestPerformReduce(t *testing.T) {
	nMap := 3
	reduceTaskID := 1
	keys := []string{"apple", "banana", "cherry"}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskID)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, key := range keys {
			enc.Encode(KeyValue{key, "1"})
		}
		file.Close()
		defer os.Remove(filename)
	}

	task := GetTaskReply{
		TaskType: ReduceTask,
		TaskID:   reduceTaskID,
		NMap:     nMap,
	}

	err := performReduce(mockReduceFunc, task)
	if err != nil {
		t.Fatalf("performReduce failed: %v", err)
	}

	outFile := fmt.Sprintf("mr-out-%d", reduceTaskID)
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Errorf("Output file %s was not created", outFile)
	}
	defer os.Remove(outFile)

	content, err := ioutil.ReadFile(outFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	expectedOutput := "apple 3\nbanana 3\ncherry 3\n"
	if string(content) != expectedOutput {
		t.Errorf("Output content mismatch. Expected:\n%s\nGot:\n%s", expectedOutput, string(content))
	}
}