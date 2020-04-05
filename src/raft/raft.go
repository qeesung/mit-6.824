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
	"bytes"
	"labgob"
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
	applyInterval()
	currentRole() Role
}

type BaseState struct {
	raft *Raft
	role Role
}

func (baseState *BaseState) currentRole() Role {
	return baseState.role
}

func (baseState *BaseState) applyInterval() {
	rf := baseState.raft
	//DPrintf(">>>>>>>>>> 服务器 %d 对应的commit index %d, last applied index %d, log length %d", rf.me, rf.CommitIndex, rf.LastApplied, len(rf.Logs))
	if rf.LastApplied < rf.CommitIndex {
		// 开始应用到虚拟机中
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			logEntry := rf.Logs[i-rf.SnapshotIndex]
			DPrintf(">>>>> 服务器%d开始apply日志%v", rf.me, logEntry)
			if logEntry.ResultChan != nil {
				logEntry.ResultChan <- LogEntryAppendResult{Index: i, Term: logEntry.Term}
			}
			rf.LastApplied = i
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: i,
				CommandTerm:  logEntry.Term,
			}
		}
	}
}
func (baseState *BaseState) voteTimeout()    {}
func (baseState *BaseState) appendInterval() {}
func (baseState *BaseState) candidate() {
	rf := baseState.raft
	currentTerm := rf.CurrentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.Logs) - 1 + rf.SnapshotIndex
	lastLogTerm := rf.Logs[lastLogIndex-rf.SnapshotIndex].Term

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
						rf.persist()
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
							// 初始化所有的nextIndex, 初始化为最后一条日志的index加1
							for i := range rf.peers {
								rf.NextIndex[i] = len(rf.Logs) + rf.SnapshotIndex
							}

							// 初始化所有的MatchIndex为0
							for i := range rf.peers {
								rf.MatchIndex[i] = 0
							}

							// 初始化自己的nextIndex和matchIndex
							rf.NextIndex[rf.me] = len(rf.Logs) + rf.SnapshotIndex
							rf.MatchIndex[rf.me] = len(rf.Logs) - 1 + rf.SnapshotIndex

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
	leaderCommit := rf.CommitIndex
	go func() {
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(peer int) {
				// 计算当前应该发送给follower的日志条目, 以及preLog信息
				rf.mu.Lock()
				prevLogIndex := rf.NextIndex[peer] - 1
				if prevLogIndex < rf.SnapshotIndex {
					go func() {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.sendSnapshot(peer)
					}()
					rf.mu.Unlock()
					return
				}
				//SDPrintf("NextIndex: %+v, MatchIndex: %+v", rf.NextIndex, rf.MatchIndex)
				prevLogTerm := rf.Logs[prevLogIndex-rf.SnapshotIndex].Term
				//entries := make([]LogEntry, 0)
				//if len(rf.Logs)-1 >= rf.NextIndex[peer] {
				//	// Leader收到新的请求了，需要追加nextIndex之后的日志
				//	entries = append(entries, rf.Logs[rf.NextIndex[peer]])
				//}
				entries := rf.Logs[rf.NextIndex[peer]-rf.SnapshotIndex:]
				DPrintf("服务器%d开始append日志%v到服务器%d，nextIndex:%v, matchIndex:%v, commitIndex: %v, applyIndex: %v",
					rf.me, entries, peer, rf.NextIndex, rf.MatchIndex, rf.CommitIndex, rf.LastApplied)
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if ok {
					if reply.Term > rf.CurrentTerm {
						// 发现更高任期的响应
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1 // 取消对自己的投票
						rf.state = newFollowerState(rf)
						rf.timerReset <- struct{}{} // 马上重置当前的Timer，避免成为Follower以后马上超时
						rf.persist()
						return
					}

					// 忽略过期任期的响应
					if currentTerm != rf.CurrentTerm {
						return
					}

					// 避免多个Reply并发的更新nextIndex和matchIndex，判断在收到reply以后，index是否发生变化
					// 如果NextIndex发生了变化，那么说明有其他的响应更改了，那么忽略此次响应
					if prevLogIndex != rf.NextIndex[peer]-1 {
						return
					}

					// 检测PreLog是否匹配
					// 这里收到过时的响应怎么办？NextIndex和MatchIndex的值岂不是会有问题？？？
					if !reply.Success {
						DPrintf("服务器%d和Leader %d的日志不匹配, conflict index : %d, conflict term : %d", peer, rf.me, reply.ConflictIndex, reply.ConflictTerm)
						// PreLog不匹配，较少nextIndex，并稍后重试
						if reply.ConflictTerm == -1 {
							rf.NextIndex[peer] = reply.ConflictIndex
						} else {
							i := prevLogIndex - 1
							for ; i >= rf.SnapshotIndex && rf.Logs[i-rf.SnapshotIndex].Term != reply.ConflictTerm; i-- {
							}
							if i < rf.SnapshotIndex {
								rf.NextIndex[peer] = reply.ConflictIndex
							} else {
								rf.NextIndex[peer] = i + 1
							}
						}

						// 日志太落后，需要发送snapshot
						if rf.SnapshotIndex != 0 && rf.NextIndex[peer] <= rf.SnapshotIndex {
							rf.sendSnapshot(peer)
							return
						}
					} else {
						// PreLog匹配，发送nextIndex开始对应的日志
						rf.MatchIndex[peer] = prevLogIndex + len(entries)
						rf.NextIndex[peer] = rf.MatchIndex[peer] + 1
						DPrintf("服务器%d和Leader %d的日志匹配, 成功append的日志, %v", peer, rf.me, entries)
					}

					// 开始检查从commitIndex之后开始的matchIndex, 检查是否可以提交
					nextCommitIndex := rf.CommitIndex
					for i := len(rf.Logs) - 1 + rf.SnapshotIndex; i > rf.CommitIndex; i-- {
						committedPeerCount := 1 // 包括自己
						for key, value := range rf.MatchIndex {
							if key == rf.me {
								continue
							}

							if value >= i {
								// 说明服务器已经提交
								committedPeerCount++
							}
						}

						// 检测是否大多数服务器都已经提交了，如果是，那么服务器就可以开始提交了
						if committedPeerCount >= len(rf.peers)/2+1 && rf.Logs[i-rf.SnapshotIndex].Term == rf.CurrentTerm {
							nextCommitIndex = i
							break
						}
					}

					rf.CommitIndex = nextCommitIndex
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
	DPrintf("服务器%d超时，开始选举", state.raft.me)
	state.raft.CurrentTerm = state.raft.CurrentTerm + 1 // 增加自己的任期号
	state.raft.VotedFor = state.raft.me                 // 为自己投票
	state.raft.state = newCandidateState(state.raft)
	state.candidate()
	state.raft.persist()
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
	CommandTerm  int
	IsSnapshot   bool
	SnapshotData []byte
}

type LogEntry struct {
	Term       int
	Command    interface{}
	ResultChan chan LogEntryAppendResult
}

type LogEntryAppendResult struct {
	Index int
	Term  int
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

	// Snapshot
	SnapshotIndex int
	SnapshotTerm  int

	applyChan chan ApplyMsg

	state State

	// 定时器重置Channel
	timerReset  chan struct{}
	appendReset chan struct{}
}

func (rf *Raft) voteTimeout() {
	rf.state.voteTimeout()
}

func (rf *Raft) appendInterval() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state.appendInterval()
}

func (rf *Raft) applyInterval() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state.applyInterval()
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
	defer rf.mu.Unlock()
	isLeader = rf.state.currentRole().isLeader()
	term = rf.CurrentTerm
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// 所有服务器上面持久存在的
	//CurrentTerm int
	//VotedFor    int
	//Logs        []LogEntry

	//// 所有服务器上面经常变的
	//CommitIndex int
	//LastApplied int

	//// 领导人里面经常改变的
	//NextIndex  map[int]int
	//MatchIndex map[int]int

	//applyChan chan ApplyMsg

	//state State
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//DPrintf("######>>>> 服务器 %d 开始持久化配置， Current term %d, vote for %d, 日志为(日志长度为%d, %+v) %v",
	//	rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs), rf.Logs[len(rf.Logs)-1], rf.Logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.SnapshotIndex)
	e.Encode(rf.SnapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) StateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) BuildSnapshot(index int, snapshotData []byte, limit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.SnapshotIndex {
		return // duplicate
	}

	if rf.persister.RaftStateSize() < limit {
		return
	}
	oldSnapshotIndex := rf.SnapshotIndex

	SDPrintf("xxxxxxxxxx 服务器%d开始创建Snapshot[Size %d]，SnapshotIndex为%d, SnapshotTerm为%d, 日志长度为 %d, 日志为%+v",
		rf.me, rf.StateSize(), rf.SnapshotIndex, rf.SnapshotTerm, len(rf.Logs), rf.Logs)
	rf.SnapshotIndex = index
	rf.SnapshotTerm = rf.Logs[index-oldSnapshotIndex].Term
	rf.Logs = rf.Logs[index-oldSnapshotIndex:]

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.SnapshotIndex)
	e.Encode(rf.SnapshotTerm)

	stateData := w.Bytes()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)

	SDPrintf("yyyyyyyyyyyyyyyy服务器%d创建Snapshot[Size %d]完成, SnapshotIndex : %d, SnapshotTerm : %d, 日志长度为 %d, 日志为%+v",
		rf.me, rf.StateSize(), rf.SnapshotIndex, rf.SnapshotTerm, len(rf.Logs), rf.Logs)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// 所有服务器上面持久存在的
	//CurrentTerm int
	//VotedFor    int
	//Logs        []LogEntry

	//// 所有服务器上面经常变的
	//CommitIndex int
	//LastApplied int

	//// 领导人里面经常改变的
	//NextIndex  map[int]int
	//MatchIndex map[int]int

	//applyChan chan ApplyMsg

	//state State
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.SnapshotIndex)
	d.Decode(&rf.SnapshotTerm)
	DPrintf("######<<<<< 服务器 %d 加载配置成功, 当前的任期为%d, VotedFor 为%d, 日志为(长度为%d) %v", rf.me, rf.CommitIndex, rf.VotedFor, len(rf.Logs), rf.Logs)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludeIndex int
	LastIncludeTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 安装快照的过程中暂缓对其他状态的处理

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		rf.persist()
		return
	}

	if args.LastIncludeIndex <= rf.SnapshotIndex {
		// 可能重复的进行snapshot
		return
	}

	rf.resetTimer()

	SDPrintf(">>>>>> server %d[size %d] start install the snapshot...base on index: %d",
		rf.me, rf.StateSize(), args.LastIncludeIndex)
	//// 开始安装快照
	oldSnapshotIndex := rf.SnapshotIndex

	rf.SnapshotTerm = args.LastIncludeTerm
	rf.SnapshotIndex = args.LastIncludeIndex
	rf.CommitIndex = rf.SnapshotIndex
	rf.LastApplied = rf.SnapshotIndex

	// 检测快照中的最后条目和当前日志中的最后条目是否相同
	// 如果日志的长度比快照的长度最后索引长
	if len(rf.Logs)+oldSnapshotIndex-1 > args.LastIncludeIndex {
		// 判断snapshot是否有冲突
		if rf.Logs[args.LastIncludeIndex-oldSnapshotIndex].Term == args.LastIncludeTerm {
			// 不存在冲突, 保留后面的日志
			rf.Logs = rf.Logs[args.LastIncludeIndex-oldSnapshotIndex:]
		} else {
			rf.Logs = []LogEntry{{Term: args.LastIncludeTerm, Command: nil}}
		}
	} else {
		rf.Logs = []LogEntry{{Term: args.LastIncludeTerm, Command: nil}}
	}

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	rf.applyChan <- ApplyMsg{CommandIndex: rf.SnapshotIndex, IsSnapshot: true, SnapshotData: args.Data}
	SDPrintf(">>>>>> server %d[size %d] installed the snapshot...base on index: %d",
		rf.me, rf.StateSize(), args.LastIncludeIndex)
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
	DPrintf("服务器%d(任期 %d)收到leader %d(任期 %d)的心跳请求, 当前服务器的commit index %d， apply index %d, Entries %+v",
		rf.me, rf.CurrentTerm, args.LeaderId, args.Term, rf.CommitIndex, rf.LastApplied, args.Entries)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = newFollowerState(rf)
		rf.persist()
	}

	if rf.state.currentRole().isLeader() {
		rf.VotedFor = -1
		rf.state = newFollowerState(rf)
		rf.persist()
	}

	if rf.VotedFor != args.LeaderId {
		rf.VotedFor = args.LeaderId
		rf.persist()
	}

	rf.timerReset <- struct{}{}

	if args.PrevLogIndex < rf.SnapshotIndex {
		reply.Success, reply.ConflictIndex = false, rf.SnapshotIndex+1
		return
	}
	// 开始校验日志
	// 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
	currentLogLength := len(rf.Logs) + rf.SnapshotIndex
	if args.PrevLogIndex > currentLogLength-1 || rf.Logs[args.PrevLogIndex-rf.SnapshotIndex].Term != args.PrevLogTerm {
		// 如果日志的长度没有PreLogIndex指定的长度长
		// 或者在PreLogIndex位置处的日志任期不匹配
		reply.Success = false
		if args.PrevLogIndex > currentLogLength-1 {
			// 如果是日志长度不足
			reply.ConflictIndex = currentLogLength
			reply.ConflictTerm = -1
		} else {
			// 如果是日志长度相同，那么就存在冲突的日志
			reply.ConflictTerm = rf.Logs[args.PrevLogIndex-rf.SnapshotIndex].Term
			// 一次越过整个term
			i := args.PrevLogIndex
			for ; i >= rf.SnapshotIndex && rf.Logs[i-rf.SnapshotIndex].Term == reply.ConflictTerm; i-- {
			}
			reply.ConflictIndex = i + 1
		}
		return
	}

	// PreLog的已经完全匹配，开始追加日志
	reply.Success = true
	// 尝试附加日志
	appendStartIndex := args.PrevLogIndex + 1
	for _, logEntry := range args.Entries {
		// 检测对应的append位置是否有日志已经存在
		// 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
		if len(rf.Logs)-1+rf.SnapshotIndex >= appendStartIndex {
			// 判断已经存在的日志是否和将要append的日志向匹配, 如果匹配，那么跳过
			// 这里只有不匹配的情况下才进行trim，否则会在收到过期的append请求的时候，发生错误的日志截断
			currentLogEntry := rf.Logs[appendStartIndex-rf.SnapshotIndex]
			if currentLogEntry.Term != logEntry.Term /*&& currentLogEntry.Command != logEntry.Command*/ {
				// 对应位置处的日志不相等，清除该日志，以及以后的日志, 并append自己
				rf.Logs = rf.Logs[:appendStartIndex-rf.SnapshotIndex]
				rf.Logs = append(rf.Logs, LogEntry{Term: logEntry.Term, Command: logEntry.Command, ResultChan: logEntry.ResultChan})
				//continue
			}
			// 日志已经存在了，什么都不做
		} else {
			// 简单的对日志进行追加
			rf.Logs = append(rf.Logs, LogEntry{Term: logEntry.Term, Command: logEntry.Command, ResultChan: logEntry.ResultChan})
		}
		// 开始更新CommitIndex
		DPrintf("leader目前的commit index %d, 目前当前的append start index %d", args.LeaderCommit, appendStartIndex)
		appendStartIndex++
	}

	lastLogIndex := len(rf.Logs) - 1 + rf.SnapshotIndex
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit > lastLogIndex {
			rf.CommitIndex = lastLogIndex
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
	}
	//DPrintf("+++++++++ (%d)leader commit 为%d, append index 为%d, log length 为%d, 更新commit index 为 %d", rf.me, args.LeaderCommit, appendStartIndex, len(rf.Logs), rf.CommitIndex)
	rf.persist()
}

func (rf *Raft) startAppendEntriesThread() {
	appendInterval := 140
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

func (rf *Raft) startApplyIntervalThread() {
	for {
		rf.applyInterval()
		time.Sleep(20 * time.Millisecond)
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
		currentLastLogIndex := len(rf.Logs) - 1 + rf.SnapshotIndex
		currentLastLogTerm := rf.Logs[currentLastLogIndex-rf.SnapshotIndex].Term
		DPrintf("current last log index %d, last log term %d, args %+v", currentLastLogIndex, currentLastLogTerm, args)
		if (currentLastLogTerm == args.LastLogTerm && args.LastLogIndex >= currentLastLogIndex) ||
			currentLastLogTerm < args.LastLogTerm {
			rf.VotedFor = args.CandidateId
			rf.state = newFollowerState(rf)
			rf.resetTimer()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}

	rf.persist()
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

func (rf *Raft) sendSnapshot(server int) {
	args := InstallSnapshotArgs{
		Term:             rf.CurrentTerm,
		LeaderId:         rf.me,
		LastIncludeTerm:  rf.SnapshotTerm,
		LastIncludeIndex: rf.SnapshotIndex,
		Data:             rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if ok {
			// Check the result
			if reply.Term > rf.CurrentTerm {
				// 成为Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1 // 取消对自己的投票
				rf.state = newFollowerState(rf)
				rf.timerReset <- struct{}{} // 马上重置当前的Timer，避免成为Follower以后马上超时
			} else {
				rf.MatchIndex[server] = rf.SnapshotIndex
				rf.NextIndex[server] = rf.SnapshotIndex + 1
			}
		}
	}()
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state.currentRole().isLeader()
	if !isLeader {
		return -1, -1, false
	}
	DPrintf("leader %d准备处理请求%v", rf.me, command)
	// 追加日志
	rf.Logs = append(rf.Logs, LogEntry{Term: rf.CurrentTerm, Command: command, ResultChan: nil})
	rf.NextIndex[rf.me]++
	rf.MatchIndex[rf.me]++

	// 马上开始推送日志
	//rf.appendReset <- struct{}{}
	if !isLeader {
		rf.persist()
		return index, term, isLeader
	}

	DPrintf("请求命令%v成功，对应的index为%d，term为%d", command, len(rf.Logs)-1+rf.SnapshotIndex, rf.CurrentTerm)
	isLeader = rf.state.currentRole().isLeader()
	rf.persist()
	return len(rf.Logs) - 1 + rf.SnapshotIndex, rf.CurrentTerm, isLeader
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

	rf.SnapshotIndex = 0
	rf.SnapshotTerm = 0

	rf.NextIndex = make(map[int]int)
	rf.MatchIndex = make(map[int]int)

	rf.timerReset = make(chan struct{})
	rf.appendReset = make(chan struct{})

	rf.applyChan = applyCh

	rf.readPersist(persister.ReadRaftState())
	if rf.SnapshotIndex != 0 {
		rf.LastApplied = rf.SnapshotIndex
	}

	go rf.startTimeoutVoteThread()   // 启动超时线程
	go rf.startAppendEntriesThread() // 启动心跳线程
	go rf.startApplyIntervalThread() // 启动apply日志线程

	//go func() {
	//	for {
	//		SDPrintf("&&&&&& server %d state size %d, applied : %d, committed : %d, snapshot: %d, log length: %d",
	//			rf.me, rf.StateSize(), rf.LastApplied, rf.CommitIndex, rf.SnapshotIndex, len(rf.Logs))
	//		time.Sleep(100 * time.Millisecond)
	//	}
	//}()
	return rf
}
