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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		var reply RPCReply
		args := RPCAgrs{Pid: os.Getpid()}
		// CallExample()
		CallAssign(&args, &reply)
		if reply.TaskType == "Map" {
			filename := reply.Filename
			nReduce := reply.NReduce
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			encs := make([]*json.Encoder, nReduce)
			for i := 0; i < nReduce; i += 1 {
				oFilename := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
				oFile, err := os.Create(oFilename)
				if err != nil {
					log.Fatalf("cannot create file %v", oFilename)
				} else {
					// fmt.Printf("Map: Create %v", file)
				}
				encs[i] = json.NewEncoder(oFile)
				defer oFile.Close()
			}
			for _, item := range kva {
				idx := ihash(item.Key) % nReduce
				encs[idx].Encode(&item)
			}
			cargs := CompleteArgs{TaskId: reply.TaskId, TaskType: reply.TaskType}
			var creply CompleteReply
			CallComplete(&cargs, &creply)
		} else if reply.TaskType == "Reduce" {
			intermediate := []KeyValue{}
			tot, _ := strconv.Atoi(reply.Filename)
			for i := 0;i < tot;i += 1 {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
				iFile, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot read file %v", filename)
				} else {
					// fmt.Printf("Reduce: Open %v", filename)
				}
				dec := json.NewDecoder(iFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			oFilename := "mr-out-" + strconv.Itoa(reply.TaskId)
			oFile, err := os.Create(oFilename)
			if err != nil {
				log.Fatalf("cannot create file %v", oFilename)
			} else {
				// fmt.Printf("Reduce: Create %v", oFilename)
			}
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
				fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			cargs := CompleteArgs{TaskId: reply.TaskId, TaskType: reply.TaskType}
			var creply CompleteReply
			CallComplete(&cargs, &creply)
		} else if reply.TaskType == "Done" {
			break 
		} else if reply.TaskType == "Wait" {
			time.Sleep(5 * time.Second)
			continue
		}
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
		// time.Sleep(10 * time.Second)
	}
}
func CallComplete(args *CompleteArgs, reply *CompleteReply) {
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		// fmt.Printf("Coordinator reply with %v", *reply)
	} else {
		fmt.Println("call failed")
	}
}
func CallAssign(args *RPCAgrs, reply *RPCReply) {
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("Coordinator reply with %v", *reply)
	} else {
		fmt.Println("call failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
