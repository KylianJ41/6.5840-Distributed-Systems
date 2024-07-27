package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// CallFunc is the type of the RPC call function
type CallFunc func(string, interface{}, interface{}) bool

// ByKey is a type to allow sorting of key-value pairs by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// rpcCall is a variable that holds the function used for RPC calls
var rpcCall CallFunc = call

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	//CallExample(call)

	for {
		task := requestTask()

		switch task.TaskType {
		case MapTask:
			err := performMap(mapf, task)
			if err != nil {
				log.Fatalf("Error performing map task: %v", err)
			}
			reportTaskCompletion(task)
		case ReduceTask:
			err := performReduce(reducef, task)
			if err != nil {
				log.Fatalf("Error performing reduce task: %v", err)
			}
			reportTaskCompletion(task)
			cleanupIntermediateFiles(task.NMap, task.TaskID)
		case WaitTask:
			log.Println("No task available. Waiting...")
			time.Sleep(1 * time.Second)
		case DoneTask:
			log.Println("All tasks completed. Worker exciting.")
			return
		}

	}
}

func requestTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := rpcCall("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatalf("Failed to get task from coordinator")
	}
	return reply
}

func reportTaskCompletion(task GetTaskReply) {
	args := TaskCompletionArgs{
		TaskID:   task.TaskID,
		TaskType: task.TaskType,
	}
	reply := TaskCompletionReply{}

	ok := rpcCall("Coordinator.MarkTaskCompleted", &args, &reply)
	if !ok {
		log.Fatalf("Failed to report task completion to coordinator")
	}
}

func performMap(mapf func(string, string) []KeyValue, task GetTaskReply) error {
	log.Printf("Starting map task %d for file %s", task.TaskID, task.FileName)

	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		return fmt.Errorf("cannot read %v: %v", task.FileName, err)
	}

	kva := mapf(task.FileName, string(content))

	log.Printf("Map function produced %d key-value pairs for task %d", len(kva), task.TaskID)

	err = writeIntermediateFiles(task.TaskID, task.NReduce, kva)
	if err != nil {
		return fmt.Errorf("failed to write intermediate files for task %d: %v", task.TaskID, err)
	}

	log.Printf("Completed map task %d", task.TaskID)
	return nil
}

func performReduce(reducef func(string, []string) string, task GetTaskReply) error {
	intermediate, err := readIntermediateFiles(task)
	if err != nil {
		return fmt.Errorf("error reading intermediate files: %v", err)
	}

	sort.Sort(ByKey(intermediate))

	tempFile, tempFilename, err := createTempFile(task.TaskID)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer cleanupTempFile(tempFile, &tempFilename)

	err = writeReduceOutput(tempFile, intermediate, reducef)
	if err != nil {
		return fmt.Errorf("error writing reduce output: %v", err)
	}

	if err := finalizeOutput(tempFile, tempFilename, task.TaskID); err != nil {
		return fmt.Errorf("error finalizing output: %v", err)
	}

	return nil
}

func writeIntermediateFiles(mapTaskID, nReduce int, kva []KeyValue) error {
	// Delete existing files first
	for i := 0; i < nReduce; i++ {
		finalName := fmt.Sprintf("mr-%d-%d", mapTaskID, i)
		if err := os.Remove(finalName); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot remove %s: %v", finalName, err)
		}
	}

	tmpFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d", mapTaskID, i))
		if err != nil {
			log.Fatalf("cannot create temp file %v", err)
		}
		tmpFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
	}

	// Cleanup function for error cases
	cleanup := func() {
		for _, f := range tmpFiles {
			if f != nil {
				f.Close()
				os.Remove(f.Name())
			}
		}
	}

	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		err := encoders[i].Encode(&kv)
		if err != nil {
			cleanup()
			return fmt.Errorf("cannot close temp file: %v", err)
		}
	}

	for _, file := range tmpFiles {
		if err := file.Close(); err != nil {
			cleanup()
			return fmt.Errorf("cannot close temp file: %v", err)
		}
	}

	for i, tmpFile := range tmpFiles {
		finalName := fmt.Sprintf("mr-%d-%d", mapTaskID, i)
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			cleanup()
			return fmt.Errorf("cannot rename %s to %s: %v", tmpFile.Name(), finalName, err)
		}
	}

	return nil
}

func readIntermediateFiles(task GetTaskReply) ([]KeyValue, error) {
	var intermediate []KeyValue
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		kvs, err := readKeyValuesFromFile(filename)
		if err != nil {
			if os.IsNotExist(err) {
				// Skip files that don't exist
				continue
			}
			return nil, fmt.Errorf("error reding from %s: %v", filename, err)
		}
		intermediate = append(intermediate, kvs...)
	}
	return intermediate, nil
}

func readKeyValuesFromFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var kvs []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

func createTempFile(taskID int) (*os.File, string, error) {
	tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-tem-reduce-%d-*", taskID))
	if err != nil {
		return nil, "", err
	}
	return tempFile, tempFile.Name(), nil
}

func cleanupTempFile(file *os.File, filename *string) {
	file.Close()
	if *filename != "" {
		os.Remove(*filename)
	}
}

func cleanupIntermediateFiles(mapTasks int, reduceTaskID int) {
	for i := 0; i < mapTasks; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskID)
		err := os.Remove(filename)
		if err != nil && !os.IsNotExist(err) {
			log.Printf("Error removing intermediate file %s: %v", filename, err)
		}
	}
}

func writeReduceOutput(file *os.File, intermediate []KeyValue, reducef func(string, []string) string) error {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = intermediate[k].Value
		}
		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func finalizeOutput(tempFile *os.File, tempFilename string, taskID int) error {
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("error closing temp file: %v", err)
	}

	oname := fmt.Sprintf("mr-out-%d", taskID)
	if err := os.Rename(tempFilename, oname); err != nil {
		return fmt.Errorf("cannot rename temp file: %v", err)
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample(callFunc CallFunc) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := rpcCall("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
