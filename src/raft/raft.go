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
	"math"
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

type LogEntries struct {
	Term    int
	Command interface{}
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

	// 持久化数据
	currentTerm int          // 当前选举期数，需持久化
	votedFor    int          // 当前节点给了哪个候选人选票，需持久化
	log         []LogEntries // 当前节点日志，索引起点为1，需持久化
	replyNum    []int        // 收到日志并回复的follower个数

	// 非持久化数据
	electionTimeout time.Time   // 上一次收到心跳或者投票RPC的时间
	state           serverState // 当前server处于的状态 leader or follower or candidate
	voteNeed        int         // 选举成功需要的票数

	applyChannel chan ApplyMsg

	// for all servers
	commitIndex int // 已经被commit的日志条目的最大索引，开始是0，单调递增，不需要持久化
	lastApplied int // 已经在状态机上运行了的日志条目的最大索引，开始是0，单调递增，不需要持久化

	// for leader 在选举后需要重新初始化
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
	var isLeader bool
	term = rf.currentTerm
	isLeader = rf.state == leader
	return term, isLeader
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

// candidate发过来的投票申请，分情况讨论
// 1.term比currentTerm小---直接拒绝
// 2.如果Term大于currentTerm---更新本地Term，本机变为follower，但是不代表同意投票（发生网络分区，处于少量server的分区因为选不出Leader会进行多轮选举，因此Term比较大，不能选它们为leader因为日志没有更新提交）

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 收到请求的server比candidate的term还要大
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		rf.electionTimeout = time.Now()
	}

	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && args.LastLogIndex >= len(rf.log)-1)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteAccess *int) {
	DPrintf("[raft %d] [request vote from raft %d] [term %d]\n", args.CandidateId, server, args.Term)

	if rf.killed() {
		return
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 过期的投票申请
	if rf.currentTerm != args.Term {
		//DPrintf("[raft %d] [currentTerm %v != args.term %v]\n", rf.me, rf.currentTerm, args.Term)
		return
	}

	// 接受RPC的server比Term更大
	if reply.Term > args.Term {
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.electionTimeout = time.Now()
		//DPrintf("[raft %d] [reply.Term %v > rf.currentTerm %v]\n", rf.me, reply.Term, rf.currentTerm)
		return
	}

	if reply.VoteGranted == true {
		*voteAccess++
		DPrintf("[raft %d] [request vote from raft %d] [success] [term %d]\n", args.CandidateId, server, args.Term)
		if *voteAccess == rf.voteNeed {
			DPrintf("[raft %d] [election success] [term %d]\n", rf.me, rf.currentTerm)
			rf.state = leader

			// 将nextIndex初始化为log长度，会导致Leader认为部分 没有最新commit日志的server（因为网络分区没有收到新的日志） 拥有最新commit的日志
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))
		}
	} else {
		//DPrintf("[raft %d] [request vote from raft %d] [fail] [term %d]\n", args.CandidateId, server, args.Term)
	}

	return
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

// Start 将客户端请求添加到log中
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader || rf.killed() {
		return -1, -1, false
	}
	DPrintf("[raft %d] [get a request from client] [%v]\n", rf.me, command)
	// 将command添加到log中
	entries := LogEntries{}
	entries.Term = rf.currentTerm
	entries.Command = command
	rf.log = append(rf.log, entries)

	return len(rf.log) - 1, rf.currentTerm, true
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

// checkAndApply 将已经commit的log应用到状态机中
func (rf *Raft) checkAndApply() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyChannel <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	var timeout time.Duration
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == follower {
			rf.mu.Unlock()
			timeout = time.Duration(rand.Int63()%300+200) * time.Millisecond
			time.Sleep(timeout)
			rf.mu.Lock()
			if time.Since(rf.electionTimeout) > timeout {
				rf.state = candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				voteAccess := 1

				DPrintf("[raft-%d] [election start] [term-%d]\n", rf.me, rf.currentTerm)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log) - 1,
						LastLogTerm:  rf.log[len(rf.log)-1].Term,
					}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply, &voteAccess)
				}
				rf.electionTimeout = time.Now()
			}
			rf.mu.Unlock()

		} else if rf.state == candidate {
			// 选举超时
			if time.Since(rf.electionTimeout) > 500*time.Millisecond {
				rf.state = follower
				rf.votedFor = -1
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else if rf.state == leader {
			// leader给所有follower发送心跳或者新日志
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				if len(rf.log)-1 >= rf.nextIndex[i] {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:      rf.log[rf.nextIndex[i]:],
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					DPrintf("[raft %v] [sent log to %v]\n", rf.me, i)
					go rf.sendAppendEntries(i, &args, &reply)
				} else {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						Entries:      nil,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					}
					reply := AppendEntriesReply{}
					DPrintf("[raft %v] [sent ticker to %v]\n", rf.me, i)
					go rf.sendAppendEntries(i, &args, &reply)
				}
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
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

	// 需要持久化的数据初始化
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntries, 1)
	rf.log[0].Term = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 不需要持久化的数据初始化
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(peers))

	rand.Seed(int64(rf.me * 100)) // 根据节点编号设置随机数种子

	rf.state = follower
	rf.voteNeed = len(rf.peers)/2 + 1
	rf.applyChannel = applyCh
	rf.electionTimeout = time.Now()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.checkAndApply()
	return rf
}

type AppendEntriesArgs struct {
	Term         int          // leader的选期
	LeaderId     int          // leader的Id
	PrevLogIndex int          // 新日志条目前的第一个索引
	PrevLogTerm  int          // prevLogIndex上日志条目的选期
	Entries      []LogEntries // 传输给follower的日志条目，如果为空就是心跳信息，为了效率可能会包含多个日志条目
	LeaderCommit int          // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 接受者的currentTerm，方便leader更新自己
	Success bool // 当follower包含的日志条目与prevLogIndex和prevLogTerm相匹配
	XTerm   int  // 与leader发生冲突的log对应的Term
	XIndex  int  // follower中term为XTerm的第一条log的index
	XLen    int  // follower在对应位置没有log，XTerm返回-1，XLen返回空白的槽位数
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = -1
		reply.Success = false
	}

	DPrintf("[raft %v] [args.Term %v] [rf.currentTerm %v] [args.LeaderId %v] \n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.state = follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.electionTimeout = time.Now()

	// Leader发过来的心跳信息
	if args.Entries == nil {

		// 更新commitIndex
		if args.PrevLogIndex <= len(rf.log)-1 && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		}

		DPrintf("[raft %v] [get ticker from raft %v] [log %v] [leader commit %v] [local commit %v] [term %v]\n", rf.me, args.LeaderId, rf.log, args.LeaderCommit, rf.commitIndex, args.Term)
		return

	} else /* Leader发过来的日志 */ {
		// 先判断PrevLogIndex上没有log
		if args.PrevLogIndex >= len(rf.log) {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex - len(rf.log) + 1
			DPrintf("[raft %v] [get log from raft %v failed] [log %v] [log entries %v] [term %v]\n", rf.me, args.LeaderId, rf.log, args.Entries, args.Term)
			return
		}

		// 在判断PrevLogIndex位置上的log有没有冲突
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			idx := args.PrevLogIndex
			for ; idx > 0; idx-- {
				if rf.log[idx-1].Term != reply.XTerm {
					break
				}
			}
			reply.XIndex = idx
			DPrintf("[raft %v] [get log from raft %v failed] [log %v] [log entries %v] [term %v]\n", rf.me, args.LeaderId, rf.log, args.Entries, args.Term)
			return
		} else {
			// 复制Leader发过来的日志到本地
			flag := false // 表示是否发生了覆盖（两个log内容不同时发生覆盖）
			p1 := args.PrevLogIndex + 1
			p2 := 0
			for p1 < len(rf.log) && p2 < len(args.Entries) {
				if rf.log[p1] != args.Entries[p2] {
					flag = true
					rf.log[p1] = args.Entries[p2]
				}
				p1++
				p2++
			}
			if p1 < len(rf.log) && flag {
				rf.log = rf.log[:p1]
			}
			for p2 < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[p2])
				p2++
			}

			reply.Term = rf.currentTerm
			reply.Success = true
			DPrintf("[raft %v] [get log from raft %v success] [log %v] [log entries %v] [term %v]\n", rf.me, args.LeaderId, rf.log, args.Entries, args.Term)
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for rf.killed() == false {

		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// 发生网络问题，导致RPC发送失败，重新发送
		for !ok {
			if rf.killed() {
				return
			}
			time.Sleep(10 * time.Millisecond)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}

		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			
		}

		// 当前Leader是旧选期的，转换为follower
		if reply.Term > args.Term {
			rf.state = follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.electionTimeout = time.Now()
			rf.mu.Unlock()
			return
		}

		// 心跳发过去了就算成功
		if args.Entries == nil {
			rf.mu.Unlock()
			return
		}

		// 发送心跳RPC后直接返回
		if args.Entries != nil {
			// RPC发送过程中，Leader状态已经发生改变
			if args.Term != rf.currentTerm || len(rf.log)-1 < rf.nextIndex[server] {
				rf.mu.Unlock()
				return
			}

			if reply.Success == true {

				tmp := args.PrevLogIndex + len(args.Entries) + 1
				if tmp > rf.nextIndex[server] {
					rf.nextIndex[server] = tmp
				}
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.logCommit(args.PrevLogIndex+1, len(args.Entries))

				rf.mu.Unlock()
				return
			} else {
				// 根据XTerm和XLen直接回退一个Term的log
				if reply.XTerm == -1 {
					DPrintf("[raft %v] [get reply from %v] [term %v] [XTerm %v] [XIndex %v] [XLen %v] \n", rf.me, server, args.Term, reply.XTerm, reply.XIndex, reply.XLen)

					// 更新nextIndex
					rf.nextIndex[server] = args.PrevLogIndex - reply.XLen + 1

					newArgs := &AppendEntriesArgs{
						Term:         args.Term,
						LeaderId:     args.LeaderId,
						LeaderCommit: args.LeaderCommit,
						PrevLogIndex: args.PrevLogIndex - reply.XLen,
						PrevLogTerm:  rf.log[args.PrevLogIndex-reply.XLen].Term,
						Entries:      rf.log[args.PrevLogIndex-reply.XLen+1:],
					}
					newReply := &AppendEntriesReply{}

					args = newArgs
					reply = newReply
				} else {
					DPrintf("[raft %v] [get reply from %v] [term %v] [XTerm %v] [XIndex %v] [XLen %v] \n", rf.me, server, args.Term, reply.XTerm, reply.XIndex, reply.XLen)

					// 更新nextIndex
					rf.nextIndex[server] = reply.XIndex

					newArgs := &AppendEntriesArgs{
						Term:         args.Term,
						LeaderId:     args.LeaderId,
						LeaderCommit: args.LeaderCommit,
						PrevLogIndex: reply.XIndex - 1,
						PrevLogTerm:  rf.log[reply.XIndex-1].Term,
						Entries:      rf.log[reply.XIndex:],
					}
					newReply := &AppendEntriesReply{}

					args = newArgs
					reply = newReply
				}
			}
		}
		rf.mu.Unlock()
	}
}

// 需要持有锁的时候调用
func (rf *Raft) updateFollower(args *AppendEntriesArgs, followerId int) {
	tmp := args.PrevLogIndex + len(args.Entries) + 1
	if tmp > rf.nextIndex[followerId] {
		rf.nextIndex[followerId] = tmp
	}
	rf.matchIndex[followerId] = rf.nextIndex[followerId] - 1
	rf.logCommit(args.PrevLogIndex+1, len(args.Entries))
}

// 需要持有锁的时候调用
func (rf *Raft) logCommit(index int, length int) {
	curIndex := index
	for i := 0; i < length; i++ {
		if curIndex > rf.commitIndex {
			count := 1
			for j := 0; j < len(rf.matchIndex); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= curIndex {
					count++
				}
			}
			if count >= rf.voteNeed && rf.log[curIndex].Term == rf.currentTerm {
				rf.commitIndex = curIndex
			}
		}
		curIndex++
	}
}
