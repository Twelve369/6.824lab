package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = int(nrand() % int64(len(ck.servers)))
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++
	ok := false
	serverId := ck.leaderId
	for {
		args := GetArgs{
			Key:      key,
			ClientId: ck.clientId,
			SeqId:    ck.seqId,
		}
		reply := GetReply{}
		DPrintf("<Op>: GET,	<Key>: %v,	<Client Id>: %v,	<Seq Id>:%v, <leader id>:%v\n", key, ck.clientId, ck.seqId, serverId)
		ok = ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			DPrintf("<Command Reply>: %v\n", reply.Err)
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	ok := false
	serverId := ck.leaderId
	for {
		args := PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.clientId,
			SeqId:    ck.seqId,
		}
		reply := PutAppendReply{}

		DPrintf("<Op>: %v,	<Key>: %v,	<Value>: %v,	<Client Id>: %v,	<Seq Id>: %v	<leader id>: %v\n", op, key, value, ck.clientId, ck.seqId, serverId)
		ok = ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			DPrintf("<Op>: %v,\t<Key>: %v,\t<Value>: %v,\t<Client Id>: %v,\t<Seq Id>: %v\t<leader id>: %v\t<Command Reply>: %v\n", op, key, value, ck.clientId, ck.seqId, serverId, reply.Err)
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
