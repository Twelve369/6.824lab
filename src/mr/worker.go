package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
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

type MapTask struct {
	TaskId    int
	FileName  string
	ReduceNum int
}

type ReduceTask struct {
	TaskId    int
	ReduceNum int
}

const (
	running = true
	finish  = false
)

// MapF ReduceF 保存map和reduce函数作为全局变量
var MapF func(string, string) []KeyValue
var ReduceF func(string, []string) string

// MidKV 用来排序的类型
type MidKV []KeyValue

func (m MidKV) Len() int           { return len(m) }
func (m MidKV) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MidKV) Less(i, j int) bool { return m[i].Key < m[j].Key }

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
	MapF = mapf
	ReduceF = reducef

	workerState := running
	for workerState {
		task := askForTask()
		switch task.TType {
		case MapType:
			{
				mTask := MapTask{TaskId: task.TaskId,
					FileName:  task.MapFileName,
					ReduceNum: task.NumReduce,
				}
				ok := doMapWork(mTask)
				args := ToNextArgs{TType: MapType, IsFinish: ok, TaskId: mTask.TaskId}
				reply := ToNextReply{}
				ok = call("Coordinator.ToNextPhase", &args, &reply)
				if !ok {
					log.Fatalln("rpc error")
				}
			}
		case ReduceType:
			{
				rTask := ReduceTask{TaskId: task.TaskId,
					ReduceNum: task.NumReduce,
				}
				ok := doReduceWork(rTask)
				args := ToNextArgs{TType: ReduceType, IsFinish: ok, TaskId: rTask.TaskId}
				reply := ToNextReply{}
				ok = call("Coordinator.ToNextPhase", &args, &reply)
				if !ok {
					log.Fatalln("rpc error")
				}
			}

		case WaitingType: //任务都被分发完了，worker进入等待状态
			time.Sleep(time.Second)
		case AllFinishType:
			workerState = finish
		default:
			time.Sleep(time.Second)
		}
	}
}

// 向coordinator获取任务，然后执行
func askForTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.DispatchTask", &args, &reply)
	if ok {
		if reply.TType == MapType {
			fmt.Printf("get map task, id is %d\n", reply.TaskId)
		} else if reply.TType == ReduceType {
			fmt.Printf("get reduce task, id is %d\n", reply.TaskId)
		} else {
			fmt.Printf("get other task\n")
		}
		fmt.Println("task:", reply)
	} else {
		log.Fatalln("ask task error")
	}
	return reply
}

func doMapWork(task MapTask) bool {
	file, err := os.Open(task.FileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("file %s is not found\n", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("file %s cant read\n", task.FileName)
	}
	kva := MapF(task.FileName, string(content))
	// 对于kva中的每一个键值对，根据 ihash(key)%nReduce 来确认存到哪一个中间键值对文件,写入成json文件
	for i := 0; i < len(kva); i++ {
		idx := getIdx(kva[i].Key, task.ReduceNum)
		filename := fmt.Sprintf("mr-mid-%d-%d", task.TaskId, idx+1)
		writefp, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		processErr(err)
		// debug
		// 写入到json文件中
		enc := json.NewEncoder(writefp)
		err = enc.Encode(&kva[i])
		processErr(err)
		writefp.Close()
	}
	fmt.Printf("map task %d finish\n", task.TaskId)
	return true
}

func doReduceWork(task ReduceTask) bool {
	mapNum := 1
	reduceId := task.TaskId
	intermediate := []KeyValue{}
	for {
		filename := fmt.Sprintf("mr-mid-%d-%d", mapNum, reduceId)
		temp := openFileAndGetContent(filename)
		if temp == nil {
			break
		}
		//debug
		fmt.Printf("reduce file %s\n", filename)
		for i := 0; i < len(temp); i++ {
			intermediate = append(intermediate, temp[i])
		}
		mapNum++
	}
	sort.Sort(MidKV(intermediate))
	ofile, err := os.Create(fmt.Sprintf("mr-out-%d", reduceId))
	processErr(err)

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := ReduceF(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	return true
}

func getIdx(key string, ReduceNum int) int {
	return ihash(key) % ReduceNum
}

func openFileAndGetContent(filename string) []KeyValue {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return nil
	}
	dec := json.NewDecoder(file)
	var content []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		content = append(content, kv)
	}
	return content
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

func processErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
