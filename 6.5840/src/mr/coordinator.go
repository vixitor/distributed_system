package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	idle = iota
	in_process
	completed
)

type Coordinator struct {
	// Your definitions here.
	file2State map[string]int
	file2Id    map[string]int
	mu         sync.Mutex
	nReduce    int
	reduceTask []int
	files      []string
	State bool 
	startTime []time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *RPCAgrs, reply *RPCReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	flag := true
	for k, v := range c.file2State {
		if v == idle {
			flag = false
			reply.TaskType = "Map"
			reply.TaskId = c.file2Id[k]
			reply.Filename = k
			reply.NReduce = c.nReduce
			c.file2State[k] = in_process
			c.startTime[reply.TaskId] = time.Now()
			// fmt.Printf("Assigning Map Task %v to Worker %v\n", reply.TaskId, args.Pid)
			return nil
		} else if v == in_process {
			flag = false
			if time.Since(c.startTime[c.file2Id[k]]) > 10*time.Second {
				reply.TaskType = "Map"
				reply.TaskId = c.file2Id[k]
				reply.Filename = k
				reply.NReduce = c.nReduce
				c.startTime[reply.TaskId] = time.Now()
				// fmt.Printf("Re-Assigning Map Task %v to Worker %v\n", reply.TaskId, args.Pid)
				return nil
			}
		}
	}
	if flag {
		for i := 0; i < c.nReduce; i += 1 {
			if c.reduceTask[i] == idle {
				flag = false
				reply.TaskType = "Reduce"
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.Filename = strconv.Itoa(len(c.files))
				c.reduceTask[i] = in_process
				c.startTime[len(c.files)+i] = time.Now()
				return nil
			} else if c.reduceTask[i] == in_process {
				flag = false
				if time.Since(c.startTime[len(c.files)+i]) > 10*time.Second {
					reply.TaskType = "Reduce"
					reply.TaskId = i
					reply.NReduce = c.nReduce
					reply.Filename = strconv.Itoa(len(c.files))
					c.startTime[len(c.files)+i] = time.Now()
					fmt.Printf("Re-Assigning Reduce Task %v to Worker %v\n", reply.TaskId, args.Pid)
					return nil
				}
			}
		}
	} 
	if flag {
		c.State = true
		reply.TaskType = "Done"
		return nil
	} else {
		reply.TaskType = "Wait"
		return nil
	}
}
func (c *Coordinator) CompleteTask(args *CompleteArgs, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.TaskId
	// fmt.Printf("%v: Worker finished task %v", args.TaskType, id)
	if args.TaskType == "Map" {
		c.file2State[c.files[id]] = completed
	} else if args.TaskType == "Reduce" {
		c.reduceTask[id] = completed
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	} else {
		fmt.Println("listening")
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.State
	// return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, file2State: make(map[string]int), file2Id: make(map[string]int), reduceTask: make([]int, nReduce), files: files, State: false, startTime: make([]time.Time, len(files) + nReduce)}
	for i, file := range files {
		c.file2State[file] = idle
		c.file2Id[file] = i
	}
	for i := 0; i < nReduce; i += 1 {
		c.reduceTask[i] = idle
	}
	// Your code here.

	c.server()
	return &c
}
