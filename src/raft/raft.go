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
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type State interface {
	voteTimeout()
	appendInterval()
	currentRole() Role
}

type BaseState struct {
	raft *Raft
	role Role
}

func (baseState *BaseState) currentRole() Role {
	return baseState.role
}
func (baseState *BaseState) voteTimeout()    {}
func (baseState *BaseState) appendInterval() {}
func (baseState *BaseState) candidate() {
	rf := baseState.raft
	currentTerm := rf.CurrentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.Logs) - 1
	lastLogTerm := rf.Logs[lastLogIndex].Term

	go func() {
		// 一直保持在候选人状态，直到下面三个事情发生：
		//1. 赢得了大多数选票
		//2. 其他服务器成为leader
		//3. 没有任何人获得选举
		grantedCount := 1
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(peer int) { // 逐个发送请求
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if ok {
					if reply.Term > rf.CurrentTerm {
						// 发现任期更大的响应，将自己设置为追随者
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1 // 取消对自己的投票
						rf.state = newFollowerState(rf)
						rf.timerReset <- struct{}{}
						return
					}

					if currentTerm != rf.CurrentTerm {
						// 收到过期任期的响应，忽略不处理, 或者已经投票完毕
						return
					}

					if reply.VoteGranted {
						grantedCount++
						if grantedCount >= (len(rf.peers)/2+1) && rf.state.currentRole().isCandidate() {
							DPrintf("服务器%d成为Leader", rf.me)
							// 收到大多数的投票，成为Leader
							rf.state = newLeaderState(rf)
							rf.appendReset <- struct{}{} //重置计时器，马上开始发送心跳
						}
					}
				}
			}(peerIndex)
		}
	}()
}

func (baseState *BaseState) appendEntries() {
	rf := baseState.raft
	currentTerm := rf.CurrentTerm
	leaderId := rf.me
	prevLogIndex := len(rf.Logs) - 1
	prevLogTerm := rf.Logs[prevLogIndex].Term
	entries := make([]byte, 0)
	leaderCommit := prevLogIndex
	go func() {
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Term > rf.CurrentTerm {
					// 发现更高任期的响应
					rf.CurrentTerm = reply.Term
					rf.VotedFor = -1 // 取消对自己的投票
					rf.state = newFollowerState(rf)
					rf.timerReset <- struct{}{} // 马上重置当前的Timer，避免成为Follower以后马上超时
				}
			}(peerIndex)
		}
	}()
}

func newLeaderState(rf *Raft) *LeaderState {
	return &LeaderState{&BaseState{raft: rf, role: Leader}}
}

type LeaderState struct {
	*BaseState
}

func (state *LeaderState) appendInterval() {
	state.raft.mu.Lock()
	defer state.raft.mu.Unlock()
	state.appendEntries()
}

func newCandidateState(rf *Raft) *CandidateState {
	return &CandidateState{&BaseState{raft: rf, role: Candidate}}
}

type CandidateState struct {
	*BaseState
}

func (state *CandidateState) voteTimeout() {
	state.raft.mu.Lock()
	defer state.raft.mu.Unlock()
	state.raft.CurrentTerm = state.raft.CurrentTerm + 1 // 增加自己的任期号
	state.candidate()
}

func newFollowerState(rf *Raft) *FollowerState {
	return &FollowerState{&BaseState{raft: rf, role: Follower}}
}

type FollowerState struct {
	*BaseState
}

func (state *FollowerState) voteTimeout() {
	state.raft.mu.Lock()
	defer state.raft.mu.Unlock()
	state.raft.CurrentTerm = state.raft.CurrentTerm + 1 // 增加自己的任期号
	state.raft.VotedFor = state.raft.me                 // 为自己投票
	state.raft.state = newCandidateState(state.raft)
	state.candidate()
}

type Role int

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

func (role Role) isLeader() bool {
	return role == Leader
}

func (role Role) isCandidate() bool {
	return role == Candidate
}

func (role Role) isFollower() bool {
	return role == Follower
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器上面持久存在的
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	// 所有服务器上面经常变的
	CommitIndex int
	LastApplied int

	// 领导人里面经常改变的
	NextIndex  map[int]int
	MatchIndex map[int]int

	state State

	// 定时器重置Channel
	timerReset  chan struct{}
	appendReset chan struct{}
}

func (rf *Raft) voteTimeout() {
	rf.state.voteTimeout()
}

func (rf *Raft) appendInterval() {
	rf.state.appendInterval()
}

func (rf *Raft) resetTimer() {
	rf.timerReset <- struct{}{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	isLeader = rf.state.currentRole().isLeader()
	rf.mu.Unlock()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []byte
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 候选人的任期号
	Term int
	// 候选人Id
	CandidateId int
	// 候选人最后日志的索引值
	LastLogIndex int
	// 候选人最后日志的任期号
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 当前任期号
	Term int
	// 是否同意投票
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	DPrintf("服务器%d收到leader %d的心跳请求", rf.me, args.LeaderId)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = newFollowerState(rf)
	}

	rf.timerReset <- struct{}{}

	// 开始校验日志
	currentLogLength := len(rf.Logs)
	if args.PrevLogIndex > currentLogLength-1 || rf.Logs[currentLogLength-1].Term != args.Term {
		reply.Success = false
		return
	}

	reply.Success = true
	// 之后的操作都是附加日志相关的，后面再实现
}

func (rf *Raft) startAppendEntriesThread() {
	appendInterval := 150
	for {
		func() {
			timer := time.NewTimer(time.Duration(appendInterval) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-rf.appendReset:
			}
			rf.appendInterval()
		}()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if reply.VoteGranted { // 如果进行了投票，那么需要重置自己的定时器
			DPrintf("服务器%d为服务器%d投票", rf.me, args.CandidateId)
			rf.resetTimer()
		} else {
			DPrintf("服务器%d拒绝为服务器%d投票", rf.me, args.CandidateId)
		}
	}()
	// Your code here (2A, 2B).
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm { // 请求者的任期比自己小，那么拒绝投票
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		DPrintf("服务器%d收到任期更大的请求%d(old %d), 变为Follower", rf.me, args.Term, rf.CurrentTerm)
		// 如果请求的任期比自己大，那么自己切换为Follower
		// 如果请求中的任期比自己的大，对方的日志并不一定比自己的新，还需要进一步判断
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = newFollowerState(rf)
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// 检查日志是否至少和自己一样新
		currentLastLogIndex := len(rf.Logs)
		currentLastLogTerm := rf.Logs[currentLastLogIndex-1].Term
		if currentLastLogTerm < args.Term {
			reply.VoteGranted = true
		} else if currentLastLogTerm == args.Term && args.LastLogIndex >= currentLastLogIndex {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) startTimeoutVoteThread() {
	minTimeout := 400
	maxTimeout := 600
	for {
		func() {
			timeout := rand.Intn(maxTimeout-minTimeout) + minTimeout
			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				// 超时, 将自己设置为候选人, 并开始选举
				rf.voteTimeout()
			case <-rf.timerReset:
				DPrintf("服务器%d重置计时器", rf.me)
				// 1. 收到任期更高的请求或者响应, 说明有新的领导人
				// 2. 投票给其他候选人
			}
		}()
	}
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
// within a startTimeoutVoteThread interval, Call() returns true; otherwise
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (2A, 2B, 2C).
	// 初始化一些基本的状态
	rf.state = newFollowerState(rf)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{Term: 0}

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.NextIndex = make(map[int]int)
	rf.MatchIndex = make(map[int]int)

	rf.timerReset = make(chan struct{})
	rf.appendReset = make(chan struct{})

	go rf.startTimeoutVoteThread()   // 启动超时线程
	go rf.startAppendEntriesThread() // 启动心跳线程

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
