package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := getTask()
		switch task.Phase {
		case MapPhase:
			mapper(&task, mapf)
		case ReducePhase:
			reducer(&task, reducef)
		case WaitPhase:
			time.Sleep(time.Second)
		case ExitPhase:
			return
		}
	}
}

func reducer(task *Task, reducef func(string, []string) string) {
	intermediates := *readFromLocalFile(task.IntermediateFiles)
	sort.Sort(ByKey(intermediates))

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to get current working directory", err)
	}
	tempFile, err := os.CreateTemp(dir, "mr-temp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}
	tempFile.Close()

	outputName := fmt.Sprintf("mr-out-%d", task.ID)
	if err = os.Rename(tempFile.Name(), outputName); err != nil {
		log.Fatal("Failed to rename temp file", err)
	}
	task.OutputFile = outputName
	taskCompleted(task)
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.InputFilePath)
	if err != nil {
		log.Fatal("Failed to read file: "+task.InputFilePath, err)
	}

	intermediates := mapf(task.InputFilePath, string(content))
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	filepaths := make([]string, 0, task.NReducer)
	for i := 0; i < task.NReducer; i++ {
		filepaths = append(filepaths, writeToLocalFile(task.ID, i, &buffer[i]))
	}

	task.IntermediateFiles = filepaths
	taskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kvs := []KeyValue{}
	for _, filepath := range files {
		f, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file: "+filepath, err)
		}
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}
	return &kvs
}

func writeToLocalFile(taskID, ReducerID int, kvs *[]KeyValue) string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to get current working directory", err)
	}
	tempFile, err := os.CreateTemp(dir, fmt.Sprintf("mr-temp-%d-%d", taskID, ReducerID))
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	encoder := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err = encoder.Encode(kv); err != nil {
			log.Fatal("Failed to encode key-value pair", err)
		}
	}
	tempFile.Close()

	filename := fmt.Sprintf("mr-%d-%d", taskID, ReducerID)
	if err = os.Rename(tempFile.Name(), filename); err != nil {
		log.Fatal("Failed to rename temp file", err)
	}
	return filepath.Join(dir, filename)
}

func taskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", &task, &reply)
}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply
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
