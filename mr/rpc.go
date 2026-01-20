package mr

//
// RPC 定义。
//
// 记住要将所有名称首字母大写。
//

import "os"
import "strconv"

//
// 展示一个用于说明如何声明 RPC 的参数
// 和返回值的示例。
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 在这里添加你的 RPC 定义。

type WorkerRegisterArgs struct {
}

type WorkerRegisterReply struct {
	WorkerID int
	Status   bool
}

type AssignTaskArgs struct {
	WorkerID int
}
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type AssignTaskReply struct {
	FileName string
	TaskType TaskType
	TaskID   int
	NMap     int
	NReduce  int
}

type ReportTaskDoneArgs struct{
	WorkerID int
	TaskID int
	TaskType TaskType
}
type ReportTaskDoneReply struct {
	
}

// 为 coordinator 在 /var/tmp 下
// 生成一个比较独特的 UNIX 域套接字名称。
// 不能使用当前目录，因为
// Athena AFS 不支持 UNIX 域套接字。

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
