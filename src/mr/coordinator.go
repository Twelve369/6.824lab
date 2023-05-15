package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
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
	MapTasks        map[int]*TaskInfo
	ReduceTasks     map[int]*TaskInfo
	MapFinishNum    int
	ReduceFinishNum int
}

// 任务信息
type TaskInfo struct {
	state     TaskState
	workerUid int
	startTime int64
	fileName  string
	taskId    int
}

// TaskState 任务状态：未开始 or 运行中 or 已完成
type TaskState int

const (
	NotStart TaskState = 0
	Working  TaskState = 1
	Finish   TaskState = 2
)

// 互斥锁-任务分发
var mu sync.Mutex

// Phase 总体任务处于的阶段
type Phase int

const (
	Mapping   Phase = 0
	Reducing  Phase = 1
	Waiting   Phase = 2
	AllFinish Phase = 3
)

//
// Your code here -- RPC handlers for the worker to call.
// 给worker线程分配任务
func (c *Coordinator) DispatchTask(args *TaskArgs, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.WorkPhase {
	case Mapping:
		if len(c.MapChannel) > 0 {
			*reply = *<-c.MapChannel
			if c.MapTasks[reply.TaskId].state == NotStart {
				c.MapTasks[reply.TaskId].state = Working
				c.MapTasks[reply.TaskId].startTime = time.Now().Unix()
				c.MapTasks[reply.TaskId].workerUid = args.WorkUid
			} else {
				reply.TType = WaitingType
			}
		} else {
			reply.TType = WaitingType
		}
	case Reducing:
		if len(c.ReduceChannel) > 0 {
			*reply = *<-c.ReduceChannel
			if c.ReduceTasks[reply.TaskId].state == NotStart {
				c.ReduceTasks[reply.TaskId].state = Working
				c.ReduceTasks[reply.TaskId].startTime = time.Now().Unix()
				c.ReduceTasks[reply.TaskId].workerUid = args.WorkUid
			} else {
				reply.TType = WaitingType
			}
		} else {
			reply.TType = WaitingType
		}
	case Waiting:
		reply.TType = WaitingType
	case AllFinish:
		reply.TType = AllFinishType
	}

	// debug
	//var taskname string
	//switch reply.TType {
	//case MapType:
	//	taskname = "map"
	//case ReduceType:
	//	taskname = "reduce"
	//case WaitingType:
	//	taskname = "waitting"
	//case AllFinishType:
	//	taskname = "allfinish"
	//}
	//fmt.Println(taskname, ":", *reply)
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
		if args.IsFinish && c.MapTasks[args.TaskId].workerUid == args.workerPid {
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
		if args.IsFinish && c.ReduceTasks[args.TaskId].workerUid == args.workerPid {
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
		if isFinish && c.MapTasks[taskId].state == Working {
			c.MapTasks[taskId].state = Finish
			c.MapFinishNum++
		} else {
			c.MapTasks[taskId].state = NotStart
		}
	} else if taskType == ReduceType {
		if isFinish && c.ReduceTasks[taskId].state == Working {
			c.ReduceTasks[taskId].state = Finish
			c.ReduceFinishNum++
		} else {
			c.ReduceTasks[taskId].state = NotStart
		}
	}
}

func (c *Coordinator) checkAllFinish(taskType TaskType) bool {
	if taskType == MapType {
		if c.MapFinishNum == len(c.FileNames) {
			return true
		}
	} else if taskType == ReduceType {
		if c.ReduceFinishNum == c.NumReduce {
			return true
		}
	}
	return false
}

func (c *Coordinator) makeMapTask() {
	for i, v := range c.FileNames {
		task := TaskReply{
			TType:       MapType,
			MapFileName: v,
			NumReduce:   c.NumReduce,
			TaskId:      i + 1,
		}
		taskInfo := TaskInfo{
			state:    NotStart,
			fileName: v,
			taskId:   i + 1,
		}
		c.MapTasks[task.TaskId] = &taskInfo
		c.MapChannel <- &task
	}
}

func (c *Coordinator) makeReduceTask() {
	for i := 1; i <= c.NumReduce; i++ {
		task := TaskReply{
			TType:     ReduceType,
			NumReduce: c.NumReduce,
			TaskId:    i,
			FileNum:   len(c.FileNames),
		}
		taskInfo := TaskInfo{
			state:  NotStart,
			taskId: i,
		}

		c.ReduceTasks[task.TaskId] = &taskInfo
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
	mu.Lock()
	defer mu.Unlock()
	if c.WorkPhase == AllFinish {
		fmt.Println("MapReduce task finish")
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
	c := Coordinator{
		NumReduce:       nReduce,
		FileNames:       files,
		WorkPhase:       Mapping,
		MapChannel:      make(chan *TaskReply, len(files)),
		ReduceChannel:   make(chan *TaskReply, nReduce),
		MapTasks:        make(map[int]*TaskInfo),
		ReduceTasks:     make(map[int]*TaskInfo),
		MapFinishNum:    0,
		ReduceFinishNum: 0,
	}

	c.makeMapTask()
	c.server()
	go c.crashDetect()

	fmt.Println("master is start")
	return &c
}

// 定期检查有没有任务的时候超过了10秒，有的话就改变该任务的状态，然后放入channel中
func (c *Coordinator) crashDetect() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		nowtime := time.Now().Unix()
		if c.WorkPhase == Mapping {
			//debug
			//fmt.Println("crash detecting", c.WorkPhase)
			//for i := 1; i <= len(c.FileNames); i++ {
			//	fmt.Printf("%v:%v ", c.MapTasks[i].taskId, c.MapTasks[i].state)
			//}
			//fmt.Println()

			for _, p := range c.MapTasks {
				if p.state == Working && nowtime-p.startTime >= 10 {
					p.state = NotStart
					task := TaskReply{
						TType:       MapType,
						NumReduce:   c.NumReduce,
						TaskId:      p.taskId,
						MapFileName: p.fileName,
					}
					//fmt.Println("make map task", c.WorkPhase)
					c.MapChannel <- &task
				}
			}
		} else if c.WorkPhase == Reducing {
			//debug
			//for i := 1; i <= c.NumReduce; i++ {
			//	fmt.Printf("%v:%v ", c.ReduceTasks[i].taskId, c.ReduceTasks[i].state)
			//}
			//fmt.Println()

			for _, p := range c.ReduceTasks {
				if p.state == Working && nowtime-p.startTime >= 10 {
					p.state = NotStart
					task := TaskReply{
						TType:     ReduceType,
						NumReduce: c.NumReduce,
						TaskId:    p.taskId,
						FileNum:   len(c.FileNames),
					}
					//fmt.Println("make reduce task", c.WorkPhase)
					c.ReduceChannel <- &task
				}
			}
		}
		if c.WorkPhase == AllFinish {
			mu.Unlock()
			break
		}
		mu.Unlock()
	}
}
