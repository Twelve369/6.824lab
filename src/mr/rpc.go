package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Coordinator.DispatchTask RPC参数
type TaskArgs struct{}

type TaskReply struct {
	TType       TaskType
	NumReduce   int
	TaskId      int
	MapFileName string
}

// Coordinator.ToNextPhase RPC参数
type ToNextArgs struct {
	TType    TaskType
	TaskId   int
	IsFinish bool
}
type ToNextReply struct {
}

// TaskType 任务类型
type TaskType int

const (
	MapType       TaskType = 1
	ReduceType    TaskType = 2
	WaitingType   TaskType = 3
	AllFinishType TaskType = 4
)

// 看是否能进入下一阶段   Mapping -> Reducing -> Finish
// 记录所有任务的状态
// 如果map任务都完成了，就进入Reducing阶段
// 如果reduce任务都完成了，则进入Finish阶段

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
