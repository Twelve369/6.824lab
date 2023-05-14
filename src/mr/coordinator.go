package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	NumReduce       int
	FileNames       []string
	WorkPhase       Phase
	MapChannel      chan *TaskReply
	ReduceChannel   chan *TaskReply
	MapTasks        map[int]TaskState
	ReduceTasks     map[int]TaskState
	MapFinishNum    int
	ReduceFinishNum int
}

// 任务信息
//type TaskInfo struct {
//	state TaskState
//}

// TaskState 任务状态：未开始 or 运行中 or 已完成
type TaskState int

const (
	NotStart = iota
	Working
	Finish
)

// 互斥锁-任务分发
var mu sync.Mutex

// Phase 总体任务处于的阶段
type Phase int

const (
	Mapping   Phase = 1
	Reducing  Phase = 2
	Waiting   Phase = 3
	AllFinish Phase = 4
)

//
// Your code here -- RPC handlers for the worker to call.
// 给worker线程分配任务
// 如果处于map阶段，在文件列表中选择一个还没有被map的文件,将文件名作为回复
// 如果处于reduce阶段，将Key作为回复
//
func (c *Coordinator) DispatchTask(args *TaskArgs, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.WorkPhase {
	case Mapping:
		if len(c.MapChannel) > 0 {
			*reply = *<-c.MapChannel
		} else {
			reply.TType = WaitingType
		}
	case Reducing:
		if len(c.ReduceChannel) > 0 {
			*reply = *<-c.ReduceChannel
		} else {
			reply.TType = WaitingType
		}
	case Waiting:
		reply.TType = WaitingType
	case AllFinish:
		reply.TType = AllFinishType
	}
	fmt.Println(*reply)
	return nil
}

//ToNextPhase 如果map任务全部完成了，转为Reducing阶段
func (c *Coordinator) ToNextPhase(args *ToNextArgs, reply *ToNextReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.WorkPhase {
	case Mapping:
		if args.TType != MapType {
			log.Fatalf("recieve %d task in mapping phase\n", args.TType)
		}
		if args.IsFinish {
			c.updateTaskState(args.TaskId, MapType, true)
			if c.checkAllFinish(MapType) {
				c.WorkPhase = Reducing
				c.makeReduceTask()
			}
		}
	case Reducing:
		if args.TType != ReduceType {
			log.Fatalf("recieve %d task in reducing phase\n", args.TType)
		}
		if args.IsFinish {
			c.updateTaskState(args.TaskId, ReduceType, true)
			if c.checkAllFinish(ReduceType) {
				c.WorkPhase = AllFinish
			}
		}
	}
	return nil
}

func (c *Coordinator) updateTaskState(taskId int, taskType TaskType, isFinish bool) {
	if taskType == MapType {
		if isFinish {
			c.MapTasks[taskId] = Finish
			c.MapFinishNum++
		} else {
			c.MapTasks[taskId] = NotStart
		}
	} else if taskType == ReduceType {
		if isFinish {
			c.ReduceTasks[taskId] = Finish
			c.ReduceFinishNum++
		} else {
			c.ReduceTasks[taskId] = NotStart
		}
	}
}

func (c *Coordinator) checkAllFinish(taskType TaskType) bool {
	if taskType == MapType {
		//for _, v := range c.MapTasks {
		//	if v != Finish {
		//		return false
		//	}
		//}
		if c.MapFinishNum == len(c.FileNames) {
			return true
		}
	} else if taskType == ReduceType {
		//for _, v := range c.ReduceTasks {
		//	if v != Finish {
		//		return false
		//	}
		//}
		if c.ReduceFinishNum == c.NumReduce {
			return true
		}
	}
	return false
}

func (c *Coordinator) makeMapTask() {
	for i, v := range c.FileNames {
		task := TaskReply{TType: MapType,
			MapFileName: v,
			NumReduce:   c.NumReduce,
			TaskId:      i + 1,
		}
		c.MapChannel <- &task
	}
}

func (c *Coordinator) makeReduceTask() {
	for i := 1; i <= c.NumReduce; i++ {
		task := TaskReply{TType: ReduceType,
			MapFileName: "",
			NumReduce:   c.NumReduce,
			TaskId:      i,
		}
		c.ReduceChannel <- &task
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc1.bak.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil) // 一个线程提供RPC服务
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.WorkPhase == AllFinish {
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NumReduce: nReduce,
		FileNames:       files,
		WorkPhase:       Mapping, // Mapping default
		MapChannel:      make(chan *TaskReply, len(files)),
		ReduceChannel:   make(chan *TaskReply, nReduce),
		MapTasks:        make(map[int]TaskState),
		ReduceTasks:     make(map[int]TaskState),
		MapFinishNum:    0,
		ReduceFinishNum: 0,
	}

	c.makeMapTask()
	c.server()
	fmt.Println("master is ready")
	return &c
}
