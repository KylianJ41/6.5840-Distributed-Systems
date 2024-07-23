package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task := requestTask()
		
		switch task.TaskType {
		case MapTask:
			err := performMap(mapf, task)
		case ReduceTask:
			performReduce(reducef, task)
		case WaitTask:
			time.Sleep(1 * time.Second)
			continue
		case DoneTask:
			return
		}
		
		if err != nil {
			log.Printf("Error performing task: %v", err)
			// NEED TO MODIFY THIS LATER
			continue
		}
		
		reportTaskCompletion(task)
	}

}

func requestTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("Failed to get task from coordinator")
	}
	
	return Task {
		TaskType: reply.TaskType,
		TaskID: reply.TaskID,
		FileName: reply.FileName,
		NReduce: reply.NReduce,
		NMap: reply.NMap,
	}
}

func readInputFile(filename string) (string, error) {
    file, err := os.Open(filename)
    if err != nil {
        return "", fmt.Errorf("cannot open %v: %v", filename, err)
    }
    defer file.Close()

    content, err := ioutil.ReadAll(file)
    if err != nil {
        return "", fmt.Errorf("cannot read %v: %v", filename, err)
    }

    return string(content), nil
}

func createTempFiles(taskID int, nReduce int) ([]*os.File, error) {
	tempFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile("", fmt.Sprintf("map_%d_%d_", taskID, i))
		if err != nil {
			return nil, fmt.Errorf("cannot create temp file: %v", err)
		}
		tempFiles[i] = tempFile
	}
	return tempFiles, nil
}

func closeTempFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}

func renameTempFiles(tempFiles []*os.File, taskID int) error {
	for i, f := range tempFiles {
		finalName := fmt.Sprintf("mr-%d-%d", taskID, i)
		if err := os.Rename(f.Name(), finalName); err != nil {
			return fmt.Errorf("cannot rename file %v to %v: %v", f.Name(), finalName, err)
		}
	}
	return nil
}

func writeToTempFiles(kva []KeyValue, nReduce int, files[]*os.File) error {
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		encoders[i] = json.NewEncoder(files[i])
	}
	
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		if err := encoders[i].Encode(&kv); err != nil {
			return fmt.Errorf("cannot encode %v: %v", kv, err)
		}
 	}
 	return nil
}

func writeIntermediateFile(mapTaskID, reduceTaskID int, kva []KeyValue, nReduce int) error {
	tempFiles, err := createTempFiles(mapTaskID, nReduce)
	if err != nil {
		return err
	}
	defer closeTempFiles(tempFiles)
	
	err = writeToTempFiles(kva, nReduce, tempFiles)
	if err != nil {
		return err
	}
	
	err = renameTempFiles(tempFiles, taskID)
	if err != nil {
		return err
	}
	
	return nil
}

func performMap(mapf func(string, string) []KeyValue, task Task) error {
	content, err := readInputFile(task.FileName)
	if err != nil {
		return fmt.Errorf("error reading reading input file: %v", err)
	}
	
	kva := mapf(task.FileName, content)
	
	for i := 0; i < task.NReduce; i++ {
		err := writeIntermediateFile(task.TaskID, i, kva, task.NReduce)
		if err != nil {
			return fmt.Errorf("error writing intermediate file: %v", err)
		}
	}
	
	return nil
}

func readIntermediateFiles(mapTaskNum int, reduceTaskID int) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

func readAllIntermediateFiles(taskID, nMap int) ([]KeyValue, error) {
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		filnemae := fmt.Sprintf("mr-%d-%d", i, taskID)
		kva, err := readIntermediateFile(filename)
		if err != nil {
			retun nil, fmt.Errorf("cannot read %v: %v", filename, err) 
		}
		intermediate = append(intermediate, kva...)
	}
	return intermedaite, nil
}

func writeReduceOutput(reducef func(string, []string) string, taskID int, intermediate []KeyValue) error {
	tempFile, err := ioutil.TempFile("", fmt.Sprintf("reduce_%d_", taskID))
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer tempFile.Close()


	err = processAndWriteResults(reducef, intermediate, tempFile)
	if err != nil {
		return err
	}
	
	finalName := fmt.Sprinf("mr-out-%d", taskID)
	err = os.Rename(tempFile.Name(), finalName)
	if err != nil {
		return fmt.Errorf("cannot rename temp file to %v: %v", finalName, err)
	}
	
	return nil
}

func processAndWriteResults(reducef func(string, []string) string, intermediate []KeyValue, write io.Writer) error {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = intermediate.Value
		}
		output := reducef(intermediate[k].Key, values)
		
		_, err := fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return fmt.Errorf("cannot write to output: %v", err)
		}
		
		i = j
	}
	return nil
}

func performReduce(reducef func(string, []string) string, teskID int, nMap int) {
	intermediate, err := readAllIntermediateFiles(taskID, nMap)
	if err != nil {
		return err
	}
	
	sort.Sort(ByKey(intermediate))
	
	err = writeReduceOutput(reducef, taskID, intermediate)
	if err != nil {
		return err
	}
	
	return nil
}

func reportTaskCompletion(task Task) {
	args := TaskCompletionArgs{TaskID: task.TaskID, TaskType: task.TaskType}
	reply := TaskCompletionReply{}
	
	ok : call("Coordinator.ReportTaskCompletion", &args, &reply)
	if !ok {
		log.Printf("Failed to report task completion: %v", task)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
