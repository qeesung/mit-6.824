package shardmaster

import (
	"log"
	"raft"
	"strconv"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	OpCallbacks map[string]func(error, interface{}, int)
	Tickets     map[string]string
}

type OpType string

const QUERY OpType = "QUERY"
const JOIN OpType = "JOIN"
const LEAVE OpType = "LEAVE"
const MOVE OpType = "MOVE"

type Op struct {
	// Your data here.
	Type     OpType
	Ticket   string
	ClientId string
	Args     interface{}
}

func (sm *ShardMaster) doRaft(opType OpType, args Args, reply Reply) {
	op := Op{Type: opType, Ticket: args.ticket(), ClientId: args.clientId()}
	switch opType {
	case JOIN:
		op.Args = args.(*JoinArgs).Servers
	case LEAVE:
		op.Args = args.(*LeaveArgs).GIDs
	case MOVE:
		op.Args = args.(*MoveArgs).MoveShardArgs
	case QUERY:
		op.Args = args.(*QueryArgs).Num
	default:
		log.Fatal("Illegal put append op type...")
	}
	done := make(chan bool)
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.markWrongLeader()
		return
	}
	sm.mu.Lock()
	sm.OpCallbacks[strconv.Itoa(index)] = func(err error, values interface{}, applyTerm int) {
		if err != nil {
			reply.setCauseErr(Err(err.Error()))
			close(done)
			return
		}

		if term != applyTerm { // wrong leader
			reply.markWrongLeader()
		} else {
			if opType == QUERY {
				queryReplay := reply.(*QueryReply)
				queryReplay.Config = values.(Config)
			}
		}
		close(done)
	}
	sm.mu.Unlock()

	select {
	case <-done:
		DPrintf("操作%+v RPC成功", op)
	case <-time.After(3 * time.Second):
		reply.setCauseErr("timeout...")
		DPrintf("操作%+v RPC超时", op)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sm.doRaft(JOIN, args, reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.doRaft(LEAVE, args, reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.doRaft(MOVE, args, reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.doRaft(QUERY, args, reply)
}

func (sm *ShardMaster) applyOp() {
	for applyMsg := range sm.applyCh {
		op := applyMsg.Command.(Op)
		func() {
			sm.mu.Lock()
			defer sm.mu.Unlock()
			//ticket := op.Ticket
			applyTerm := applyMsg.CommandTerm
			applyIndex := applyMsg.CommandIndex

			DPrintf("操作%+v完成, apply到数据库中", applyMsg)
			defer func() {
				DPrintf("ShardMaster %d apply操作%+v到数据库中完成, 当前的数据库为 %+v", sm.rf.Me(), op, sm.configs)
				sm.Tickets[op.ClientId] = op.Ticket
			}()

			var callback = func(err error, values interface{}, term int) {}
			if ck, ok := sm.OpCallbacks[strconv.Itoa(applyIndex)]; ok {
				callback = ck
				delete(sm.OpCallbacks, strconv.Itoa(applyIndex))
			}

			duplicate := false
			if latestTicket := sm.Tickets[op.ClientId]; latestTicket == op.Ticket {
				duplicate = true
			}

			args := op.Args
			switch op.Type {
			case QUERY:
				num := args.(int)
				var config Config
				if num == -1 || num >= len(sm.configs) {
					config = sm.configs[len(sm.configs)-1]
				} else {
					config = sm.configs[num]
				}
				callback(nil, config, applyTerm)
			case JOIN:
				if duplicate {
					callback(nil, nil, applyTerm)
				}
				servers := op.Args.(map[int][]string)
				newConfig := copyConfig(sm.configs[len(sm.configs)-1])
				for groupId, serverList := range servers {
					newConfig.Groups[groupId] = serverList
				}
				newConfig.Num = len(sm.configs)
				sm.configs = append(sm.configs, reShardConfig(newConfig))
				callback(nil, nil, applyTerm)
			case LEAVE:
				if duplicate {
					callback(nil, nil, applyTerm)
				}
				groupIds := op.Args.([]int)
				newConfig := copyConfig(sm.configs[len(sm.configs)-1])
				for _, leaveGroupId := range groupIds {
					delete(newConfig.Groups, leaveGroupId)
				}
				newConfig.Num = len(sm.configs)
				sm.configs = append(sm.configs, reShardConfig(newConfig))
				callback(nil, nil, applyTerm)
			case MOVE:
				if duplicate {
					callback(nil, nil, applyTerm)
				}
				moveShardArgs := op.Args.(MoveShardArgs)
				newConfig := copyConfig(sm.configs[len(sm.configs)-1])
				newConfig.Shards[moveShardArgs.Shard] = moveShardArgs.GID
				newConfig.Num = len(sm.configs)
				sm.configs = append(sm.configs, newConfig)
				callback(nil, nil, applyTerm)
			}
		}()
	}
}

func reShardConfig(config Config) Config {
	groupCount := len(config.Groups)
	if groupCount == 0 {
		return config
	}
	groupIds := make([]int, 0)
	for groupId := range config.Groups {
		groupIds = append(groupIds, groupId)
	}
	// 注意golang的map遍历是随机的, 遍历group以后，排序抱枕每次group的顺序都是一致的
	sort.Ints(groupIds)
	
	for shardIndex := range config.Shards {
		groupIndex := shardIndex % groupCount
		config.Shards[shardIndex] = groupIds[groupIndex]
	}
	return config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register(MoveShardArgs{})
	// You may need initialization code here.
	sm.OpCallbacks = make(map[string]func(error, interface{}, int))
	sm.Tickets = make(map[string]string)

	sm.applyCh = make(chan raft.ApplyMsg, 1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.applyOp()

	return sm
}
