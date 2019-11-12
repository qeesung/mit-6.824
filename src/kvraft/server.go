package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf("@@@@@@@"+format, a...)
	}
	return
}

type OpType string

const GET OpType = "GET"
const PUT OpType = "PUT"
const APPEND OpType = "APPEND"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   OpType
	Key    string
	Value  string // If required
	Ticket string
	ID     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	OpCallbacks map[string]func(error, string, int)
	Database    map[string]string
	Tickets     map[string]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	op := Op{Type: GET, Key: key, Ticket: args.Ticket, ID: UUID()}
	// register the callback
	done := make(chan bool)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}

	kv.mu.Lock()
	kv.OpCallbacks[strconv.Itoa(index)] = func(err error, value string, applyTerm int) {
		if err != nil {
			reply.Err = Err(err.Error())
			close(done)
			return
		}

		if term != applyTerm { // wrong leader
			reply.WrongLeader = true
		} else {
			reply.Value = value
		}
		close(done)
	}
	kv.mu.Unlock()

	select {
	case <-done:
		DPrintf("操作%+v RPC成功", op)
	case <-time.After(3 * time.Second):
		reply.Err = Err("timeout...")
		DPrintf("操作%+v RPC超时", op)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var opType OpType
	switch args.Op {
	case "Put":
		opType = PUT
	case "Append":
		opType = APPEND
	default:
		log.Fatal("Illegal put append op type...")
	}

	// Your code here.
	key := args.Key
	op := Op{Key: key, Type: opType, Value: args.Value, Ticket: args.Ticket, ID: UUID()}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}

	kv.mu.Lock()
	done := make(chan bool)
	kv.OpCallbacks[strconv.Itoa(index)] = func(err error, value string, applyTerm int) {
		if err != nil {
			reply.Err = Err(err.Error())
		}
		if term != applyTerm { // wrong leader
			reply.WrongLeader = true
		}
		close(done)
	}
	kv.mu.Unlock()

	select {
	case <-done:
		DPrintf("操作%+v RPC成功", op)
	case <-time.After(3 * time.Second):
		reply.Err = Err("timeout...")
		DPrintf("操作%+v RPC超时", op)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) applyOp() {
	for applyMsg := range kv.applyCh {
		op := applyMsg.Command.(Op)
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			key := op.Key
			ticket := op.Ticket
			applyTerm := applyMsg.CommandTerm
			applyIndex := applyMsg.CommandIndex

			DPrintf("操作%+v完成, apply到数据库中", applyMsg)
			defer func() {
				DPrintf("apply操作%+v到数据库中完成, 当前的数据库为 %+v", op, kv.Database)
				kv.Tickets[ticket] = true
			}()

			var callback = func(err error, value string, term int) {}
			if ck, ok := kv.OpCallbacks[strconv.Itoa(applyIndex)]; ok {
				callback = ck
				delete(kv.OpCallbacks, strconv.Itoa(applyIndex))
			}

			duplicate := false
			if _, ok := kv.Tickets[ticket]; ok {
				duplicate = true
			}

			switch op.Type {
			case GET:
				result := ""
				if val, ok := kv.Database[key]; ok {
					result = val
				}
				callback(nil, result, applyTerm)
			case APPEND:
				newValue := op.Value

				if duplicate {
					DPrintf(">>>>>>> 操作 %s 重复，跳过APPEND处理", ticket)
					callback(nil, "", applyTerm)
					return
				}

				if val, ok := kv.Database[key]; ok {
					newValue = val + op.Value
					kv.Database[key] = newValue
				} else {
					kv.Database[key] = newValue
				}

				callback(nil, "", applyTerm)
			case PUT:
				newValue := op.Value

				if duplicate {
					DPrintf(">>>>>>> 操作 %s 重复，跳过PUT处理", ticket)
					callback(nil, "", applyTerm)
					return
				}
				kv.Database[key] = newValue
				callback(nil, "", applyTerm)
			}
		}()
	}
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
	kv.OpCallbacks = make(map[string]func(error, string, int))
	kv.Database = make(map[string]string)
	kv.Tickets = make(map[string]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyOp()

	go func() {
		for {
			DPrintf("((((()))))) %d -> %+v", me, kv.OpCallbacks)
			time.Sleep(1 * time.Second)
		}
	}()

	return kv
}
