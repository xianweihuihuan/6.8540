package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskInfo struct {
	ID        int
	State     TaskState
	WorkerID  int
	StartTime time.Time
	FileName  string
}

type Coordinator struct {
	// Your definitions here.
	nextWorkerID int
	mu           sync.Mutex

	nMap       int
	nReduce    int
	mapTask    []TaskInfo
	reduceTask []TaskInfo

	phase Phase

	mapCompleted    int
	reduceCompleted int

	taskTimeout time.Duration
}

// 在此处编写你的代码 —— 提供 worker 调用的 RPC 处理函数。

//
// 一个示例 RPC 处理函数。
//
// RPC 的参数类型和返回类型在 rpc.go 中定义。
//

// 获取一个WorkerId
func (c *Coordinator) getNextWorkerID() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextWorkerID
	c.nextWorkerID++
	return id
}

func (c *Coordinator) monitorTimeout() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mu.Lock()
		if c.phase == MapPhase {
			for i := 0; i < c.nMap; i++ {
				if c.mapTask[i].State == InProgress && time.Since(c.mapTask[i].StartTime) > c.taskTimeout {
					c.mapTask[i].State = Idle
					c.mapTask[i].StartTime = time.Time{}
					c.mapTask[i].WorkerID = -1
				}
			}
		}
		if c.phase == ReducePhase {
			for i := 0; i < c.nReduce; i++ {
				if c.reduceTask[i].State == InProgress && time.Since(c.reduceTask[i].StartTime) > c.taskTimeout {
					c.reduceTask[i].State = Idle
					c.reduceTask[i].StartTime = time.Time{}
					c.reduceTask[i].WorkerID = -1
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Worker注册rpc函数
func (c *Coordinator) WorkerRegister(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	id := c.getNextWorkerID()
	reply.WorkerID = id
	reply.Status = true
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	workerID := args.WorkerID
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	switch c.phase {
	case MapPhase:
		for i := 0; i < c.nMap; i++ {
			if c.mapTask[i].State == Idle {
				c.mapTask[i].State = InProgress
				c.mapTask[i].WorkerID = workerID
				c.mapTask[i].StartTime = time.Now()

				reply.FileName = c.mapTask[i].FileName
				reply.TaskID = c.mapTask[i].ID
				reply.TaskType = MapTask
				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil
	case ReducePhase:
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTask[i].State == Idle {
				c.reduceTask[i].State = InProgress
				c.reduceTask[i].WorkerID = workerID
				c.reduceTask[i].StartTime = time.Now()

				reply.TaskID = c.reduceTask[i].ID
				reply.TaskType = ReduceTask
				return nil
			}

		}
		reply.TaskType = WaitTask
		return nil
	case DonePhase:
		reply.TaskType = ExitTask
		return nil
	}
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	WorkerID := args.WorkerID
	TaskID := args.TaskID
	switch args.TaskType {
	case MapTask:

		if c.mapTask[TaskID].WorkerID != WorkerID {
			return nil
		}
		c.mapTask[TaskID].State = Completed
		c.mapCompleted++
		if c.mapCompleted == c.nMap {
			c.phase = ReducePhase
		}
		return nil
	case ReduceTask:
		if c.reduceTask[TaskID].WorkerID != WorkerID {
			return nil
		}
		c.reduceTask[TaskID].State = Completed
		c.reduceCompleted++
		if c.reduceCompleted == c.nReduce {
			c.phase = DonePhase
		}
		return nil
	}
	return nil
}

// 启动一个线程，用于监听来自 worker.go 的 RPC 请求
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go 会定期调用 Done()，用于判断
// 整个任务是否已经完成。
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// 创建一个 Coordinator。
// main/mrcoordinator.go 会调用这个函数。
// nReduce 是要使用的 Reduce 任务数量。
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nextWorkerID:    0,
		nMap:            len(files),
		nReduce:         nReduce,
		phase:           MapPhase,
		mapCompleted:    0,
		reduceCompleted: 0,
		taskTimeout:     10 * time.Second,
	}
	c.mapTask = make([]TaskInfo, c.nMap)
	c.reduceTask = make([]TaskInfo, c.nReduce)
	for i, file := range files {
		c.mapTask[i] = TaskInfo{
			ID:        i,
			State:     Idle,
			FileName:  file,
			StartTime: time.Time{},
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTask[i] = TaskInfo{
			ID:    i,
			State: Idle,
		}
	}
	go func() {
		c.monitorTimeout()
	}()
	// Your code here.

	c.server()
	return &c
}
