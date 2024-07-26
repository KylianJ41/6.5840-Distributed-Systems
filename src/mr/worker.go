package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			//performMap(mapf, task)
		case ReduceTask:
			// TODO
		case WaitTask:
			log.Println("No task available. Waiting...")
			// TODO
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
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-tmp-reduce-%d-*", task.TaskID))
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	tempFilename := tempFile.Name()

	defer tempFile.Close()
	defer os.Remove(tempFilename)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// Close the file before renaming
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("error closing temp file: %v", err)
	}

	// Atomically rename the temp file to the final output file
	if err := os.Rename(tempFilename, oname); err != nil {
		return fmt.Errorf("cannot rename temp file: %v", err)
	}

	// If we've successfully renamed, prevent the deferred removal
	tempFilename = ""

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
		//tmpFiles[i] = nil // Prevent removal in cleanup function
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
