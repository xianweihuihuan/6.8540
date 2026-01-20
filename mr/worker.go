package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map 函数返回一个 KeyValue 切片（slice）。
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues []KeyValue

func (s KeyValues) Len() int {
	return len(s)
}

func (s KeyValues) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s KeyValues) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

// 对 Map 输出的每个 KeyValue，使用 ihash(key) % NReduce
// 来选择对应的 Reduce 任务编号。
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go 会调用这个函数。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 在这里实现你的 Worker 功能。
	RegisterArgs := WorkerRegisterArgs{}
	RegisterReply := WorkerRegisterReply{}

	for s := call("Coordinator.WorkerRegister", &RegisterArgs, &RegisterReply); !s; {
	}
	workerID := RegisterReply.WorkerID
	for {
		AssignTaskArgs := AssignTaskArgs{WorkerID: workerID}
		AssignTaskReply := AssignTaskReply{}
		if !call("Coordinator.AssignTask", &AssignTaskArgs, &AssignTaskReply) {
			continue
		}
		switch AssignTaskReply.TaskType {
		case MapTask:
			fileName := AssignTaskReply.FileName
			nReduce := AssignTaskReply.NReduce
			taskID := AssignTaskReply.TaskID
			file, _ := os.Open(fileName)
			newfilesenc := make([]*json.Encoder, nReduce)
			tempfile := make([]*os.File, nReduce)
			for i := 0; i < nReduce; i++ {
				tempFile := fmt.Sprintf("mr-%d-%d", taskID, i)
				f, err := os.Create(tempFile)
				if err != nil {
					log.Fatal(err)
				}
				tempfile[i] = f
				enc := json.NewEncoder(f)
				newfilesenc[i] = enc
			}
			value, _ := io.ReadAll(file)
			file.Close()
			mapret := mapf(fileName, string(value))
			sort.Sort(KeyValues(mapret))
			for _, v := range mapret {
				reduceIndex := ihash(v.Key) % nReduce
				newfilesenc[reduceIndex].Encode(v)
			}
			for _, v := range tempfile {
				v.Close()
			}
			ReportTaskDoneArgs := ReportTaskDoneArgs{
				WorkerID: workerID,
				TaskID:   taskID,
				TaskType: MapTask,
			}
			ReportTaskDoneReply := ReportTaskDoneReply{}
			for s := call("Coordinator.ReportTaskDone", &ReportTaskDoneArgs, &ReportTaskDoneReply); !s; {
				time.Sleep(50 * time.Millisecond)
			}
		case ReduceTask:
			nMap := AssignTaskReply.NMap
			taskID := AssignTaskReply.TaskID
			kva := []KeyValue{}
			for i := 0; i < nMap; i++ {
				tempfileName := fmt.Sprintf("mr-%d-%d", i, taskID)
				f, err:= os.Open(tempfileName)
				if err != nil{
					continue
				}
				dec := json.NewDecoder(f)
				for {
					kv := KeyValue{}
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				f.Close()
				//os.Remove(tempfileName)
			}
			sort.Sort(KeyValues(kva))
			outfile := fmt.Sprintf("mr-out-%d", taskID)
			f, _ := os.Create(outfile)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}

				output := reducef(kva[i].Key, values)
				fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)

				i = j
			}
			f.Close()
			ReportTaskDoneArgs := ReportTaskDoneArgs{
				WorkerID: workerID,
				TaskID:   taskID,
				TaskType: ReduceTask,
			}
			ReportTaskDoneReply := ReportTaskDoneReply{}
			for s := call("Coordinator.ReportTaskDone", &ReportTaskDoneArgs, &ReportTaskDoneReply); !s; {
				time.Sleep(50 * time.Millisecond)
			}

		case WaitTask:
			time.Sleep(time.Millisecond * 100)
			continue
		case ExitTask:
			return
		}

	}

}

// 示例函数，用于展示如何向 Coordinator 发起 RPC 调用。
//
// RPC 的参数类型和返回值类型在 rpc.go 中定义。
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

// 向 Coordinator 发送 RPC 请求，并等待响应。
// 通常会返回 true。
// 如果出现问题，则返回 false。
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
