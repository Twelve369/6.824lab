package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int      // 当前选举期数，需持久化
	votedFor    int      // 当前节点给了哪个候选人选票，需持久化
	log         []string // 当前节点日志，索引起点为1，需持久化

	// 非fig2规定的内容
	lastTickTime time.Time   // 上一次收到leader消息的时间
	state        serverState // 当前server处于的状态 leader or follower or candidate
	leaderId     int
	voteNeed     int

	commitIndex int // 已经被commit的日志条目的最大索引，开始是0，单调递增，不需要持久化
	lastApplied int // 已经在状态机上运行了的日志条目的最大索引，开始是0，单调递增，不需要持久化

	// for leaders 在选举后需要重新初始化
	nextIndex  []int // 下一个发送到服务器的日志条目的索引，初始化为最后一个索引+1
	matchIndex []int // 已知要在服务器上复制的最高日志条目的索引，开始为0，单调递增
}

type serverState int

const (
	leader serverState = iota
	follower
	candidate
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
// 分情况讨论
// 1
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		return
	}
	// voteFor可能已经失效
	if rf.votedFor == -1 || args.Term > rf.currentTerm {
		rf.lastTickTime = time.Now()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 随机超时时间
		timeout := time.Duration(rand.Int63()%200+200) * time.Millisecond
		time.Sleep(timeout)
		DPrintf("[raft %d] [wake up]\n", rf.me)
		rf.mu.Lock()
		if time.Since(rf.lastTickTime) > timeout && rf.state == follower && rf.votedFor == -1 {
			rf.currentTerm++
			rf.state = candidate
			rf.votedFor = rf.me
			voteCount := 1
			thisTerm := rf.currentTerm

			DPrintf("[raft %d] [start a new election] [term %d]\n", rf.me, thisTerm)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				reply := RequestVoteReply{}

				rf.mu.Unlock()
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok == false {
					DPrintf("[raft %d] [req vote to raft %d] [fail] [rpc error] [term %d]\n", rf.me, i, thisTerm)
				}
				rf.mu.Lock()

				// 假设别的server选举周期更新
				//if reply.Term > rf.currentTerm {
				//	DPrintf("[raft %d] [req vote to raft %d] [fail] [low term]\n", rf.me, i)
				//	rf.currentTerm = reply.Term
				//	rf.state = follower
				//	rf.votedFor = -1
				//	break
				//}

				if reply.VoteGranted {
					DPrintf("[raft %d] [req vote to raft %d] [success]\n", rf.me, i)
					voteCount++
				} else {
					DPrintf("[raft %d] [req vote to raft %d] [fail]\n", rf.me, i)
				}
			}

			// 选举成功
			if voteCount >= rf.voteNeed {
				rf.state = leader
				rf.votedFor = -1
				rf.leaderId = rf.me
				DPrintf("[raft %d] [win election] [term %d]\n", rf.me, thisTerm)
			} else { //选举失败
				rf.state = follower
				rf.votedFor = -1
				DPrintf("[raft %d] [fail election] [term %d]\n", rf.me, thisTerm)
			}
		}
		rf.lastTickTime = time.Now()
		rf.mu.Unlock()
	}
}

func sleepForRandomTime(downTime, upTime int) {

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.lastTickTime = time.Now()
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rand.Seed(int64(rf.me * 100)) // 根据节点编号设置随机数种子
	if len(rf.peers)%2 == 1 {
		rf.voteNeed = len(rf.peers)/2 + 1
	} else {
		rf.voteNeed = len(rf.peers) / 2
	}

	// Your initialization code here (2A, 2B, 2C)
	go rf.sendTicker() // leader定期发送心跳

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

type AppendEntriesArgs struct {
	Term         int      // leader的选期
	LeaderId     int      // leader的Id
	PrevLogIndex int      // 新日志条目前的第一个索引
	PrevLogTerm  int      // prevLogIndex上日志条目的选期
	Entries      []string // 传输给follower的日志条目，如果为空就是心跳信息，为了效率可能会包含多个日志条目
	LeaderCommit int      // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 接受者的currentTerm，方便leader更新自己
	Success bool // 当follower包含的日志条目与prevLogIndex和prevLogTerm相匹配
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// Leader发过来的心跳信息
	if len(args.Entries) == 0 && args.Term >= rf.currentTerm {
		DPrintf("raft %d get ticker from raft %d in term %d\n", rf.me, args.LeaderId, args.Term)
		rf.state = follower
		rf.leaderId = args.LeaderId
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Success = true
		rf.lastTickTime = time.Now()
	} else {

	}
	return
}

func (rf *Raft) sendTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == leader {
			tickCount := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					rf.lastTickTime = time.Now()
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}

				rf.mu.Unlock()
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if ok == false {
					DPrintf("raft %d send ticker fail\n", rf.me)
				}
				rf.mu.Lock()

				if reply.Success {
					tickCount++
				}
				// 如果接受到的回复选举期比当前节点更新，说明发生了新的选举，当前节点不再是leader
				if reply.Term > rf.currentTerm {
					rf.state = follower
					rf.leaderId = -1
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.lastTickTime = time.Now()
					DPrintf("raft %d term is bigger, leader %d transform to follower\n", i, rf.me)
					break
				}
			}
			if tickCount < rf.voteNeed {
				rf.state = follower
				rf.leaderId = -1
				rf.votedFor = -1
				rf.lastTickTime = time.Now()
				DPrintf("raft %d dont get enough ticker reply, transform to follower\n", rf.me)
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}
