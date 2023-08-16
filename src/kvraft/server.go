package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	OpType     string
	IsExistKey bool // get操作的key值是否存在
	ClientId   int64
	SeqId      int
	ServerId   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvData      map[string]string
	waitApplyCh map[int]chan Op
	seqIdMap    map[int64]int
}

//调用完start后等待日志提交
//1 如果index上提交了对应的日志，返回成功
//2 如果index上提交了别的日志，则直接返回失败
//3 如果超过一段时间日志index上都没有日志，则返回失败
//4 如果该server不是leader，直接返回失败
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    "",
		OpType:   "Get",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		ServerId: kv.me,
	}

	index, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getReplyCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitApplyCh, index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case opReply := <-ch:
		if op.ClientId != opReply.ClientId || op.SeqId != opReply.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvData[args.Key]
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		ServerId: kv.me,
	}

	index, _, ok := kv.rf.Start(op)

	if !ok {
		DPrintf("<server %v is not leader>", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getReplyCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitApplyCh, index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) getReplyCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isExist := kv.waitApplyCh[index]
	if !isExist {
		kv.waitApplyCh[index] = make(chan Op)
	}

	return kv.waitApplyCh[index]
}

// getApplyChLoop 不断循环地重applyCh中接收信息
func (kv *KVServer) getApplyChLoop() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		// 使用类型断言，将interface类型转回Op类型  Command.(Op)
		op := msg.Command.(Op)

		// 判断这个请求是否已经执行过了
		if !kv.isDuplicate(op) {
			kv.mu.Lock()

			// 根据不同操作类型对kv存储进行处理
			switch op.OpType {
			case "Put":
				kv.kvData[op.Key] = op.Value
			case "Append":
				kv.kvData[op.Key] = kv.kvData[op.Key] + op.Value // 如果key不存在，map会返回空值
			}
			kv.seqIdMap[op.ClientId] = op.SeqId
			kv.mu.Unlock()
		}

		DPrintf("<server id %v> log is ready, <Client Id>: %v,	"+
			"<Seq Id>: %v,	<opType>: %v,	<key>: %v,	"+
			"<value>: %v,	<isExistKey>: %v， <commandIndex>: %v, <serverId>: %v\n", kv.me, op.ClientId, op.SeqId, op.OpType, op.Key, kv.kvData[op.Key], op.IsExistKey, msg.CommandIndex, op.ServerId)

		// 判断是否是这个server接收的客户端请求，如果不是，就不写入管道，不然会一直阻塞
		if op.ServerId == kv.me {
			// 判断这个channel是否存在，如果不存在就不传，否则可能会导致一直阻塞，因为接受方（客户端请求处理模块）已经超时返回了
			kv.mu.Lock()
			ch, exist := kv.waitApplyCh[msg.CommandIndex]
			kv.mu.Unlock()
			if exist {
				ch <- op
			}
		}
	}
}

func (kv *KVServer) isDuplicate(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastId, isExist := kv.seqIdMap[op.ClientId]
	if !isExist {
		return false
	}
	return lastId >= op.SeqId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.waitApplyCh = make(map[int]chan Op)
	kv.kvData = make(map[string]string)
	kv.seqIdMap = make(map[int64]int)

	go kv.getApplyChLoop()

	return kv
}
