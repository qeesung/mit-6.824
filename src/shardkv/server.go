package shardkv

// import "shardmaster"
import (
	"bytes"
	"fmt"
	syncLock "github.com/sasha-s/go-deadlock"
	"labrpc"
	"log"
	"shardmaster"
	"strconv"
	"time"
)
import "raft"
import "sync"
import "labgob"

type OpType string

const GET OpType = "GET"
const PUT OpType = "PUT"
const APPEND OpType = "APPEND"
const CONFIG OpType = "CONFIG"
const DELETE OpType = "DELETE"

// 检测超过 100 ms 的锁等待

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type            OpType
	Key             string
	Value           string // If required
	Config          shardmaster.Config
	MigrationData   [shardmaster.NShards]map[string]string
	MigrationTicket map[string]string
	DeleteNum       int
	DeleteShardId   int
	Ticket          string
	ClientId        string
}

// 一个ShardKV服务器由若干个服务器组成, 组成一个组
// 一个组可以有若干个Shard
type ShardKV struct {
	mu           syncLock.Mutex
	me           int
	sm           *shardmaster.Clerk
	config       shardmaster.Config
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	OpCallbacks map[string]func(error, string, int)
	Database    [shardmaster.NShards]map[string]string
	Tickets     map[string]string
}

func (kv *ShardKV) doRaft(configOp Op) bool {
	done := make(chan bool)
	index, term, isLeader := kv.rf.Start(configOp)
	if !isLeader {
		DPrintf("DDDDDDDDDDDD Group %d Do raft %+v failed, is not leader", kv.gid, configOp)
		return false
	}

	ok := true
	kv.mu.Lock()
	kv.OpCallbacks[strconv.Itoa(index)] = func(err error, value string, applyTerm int) {
		DPrintf("DDDDDDDDDDDDDD Group %s callback is called, op is %+v, error is %+v, value %+v", kv.gid, configOp, err, value)
		if err != nil {
			DPrintf("DDDDDDDDDDDD Group %d Do raft failed, op is %+v,  %s", kv.gid, configOp, err.Error())
			ok = false
			close(done)
			return
		}

		if term != applyTerm { // wrong leader
			DPrintf("DDDDDDDDDDDD Group %d Do raft failed, wrong leader, not right term, op is %+v", kv.gid, configOp)
			ok = false
		}
		close(done)
	}
	kv.mu.Unlock()

	select {
	case <-done:
		return ok
	case <-time.After(3 * time.Second):
		DPrintf("DDDDDDDDDDDD Group %d Do raft %+v failed, timeout", kv.gid, configOp)
		return false
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	op := Op{Type: GET, Key: key, Ticket: args.Ticket, ClientId: args.ClientId}
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
		if reply.Err == "" {
			reply.Err = OK
			DPrintf("操作%+v RPC成功", op)
		} else {
			DPrintf("操作%+v RPC失败, %s", op, reply.Err)
		}
	case <-time.After(3 * time.Second):
		reply.Err = ErrTimeout
		DPrintf("操作%+v RPC超时", op)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
	op := Op{Key: key, Type: opType, Value: args.Value, Ticket: args.Ticket, ClientId: args.ClientId}
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
		if reply.Err == "" {
			reply.Err = OK
			DPrintf("操作%+v RPC成功", op)
		} else {
			DPrintf("操作%+v RPC失败, %s", op, reply.Err)
		}
	case <-time.After(3 * time.Second):
		reply.Err = ErrTimeout
		DPrintf("操作%+v RPC超时", op)
	}
}

func (kv *ShardKV) fetchConfigIfNeed() {
	if kv.config.Num == -1 {
		kv.config = kv.sm.Query(-1)
	}
}

func (kv *ShardKV) containShard(shard int) bool {
	if shard >= shardmaster.NShards {
		return false
	}
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) isDuplicate(clientId string, nextTicket string) bool {
	if latestTicket, ok := kv.Tickets[clientId]; ok {
		latestTicketNum, err1 := strconv.Atoi(latestTicket)
		currentTicketNum, err2 := strconv.Atoi(nextTicket)
		if err1 != nil || err2 != nil {
			return false
		} else {
			return latestTicketNum >= currentTicketNum
		}
	}
	return false
}

func (kv *ShardKV) applyOp() {
	for applyMsg := range kv.applyCh {
		if applyMsg.IsSnapshot {
			kv.loadSnapshot(applyMsg.SnapshotData)
			DPrintf("KV Server %d加载snapshot数据成功, 加载以后数据为%+v", kv.rf.Me(), kv.Database)
			continue
		}
		op := applyMsg.Command.(Op)
		if op.Type == CONFIG {
			DPrintf("++++++++++ Group %d(%d) apply the op %+v", kv.gid, kv.me, op)
		}
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			key := op.Key
			ticket := op.Ticket
			applyTerm := applyMsg.CommandTerm
			applyIndex := applyMsg.CommandIndex

			DPrintf("操作%+v完成, apply到数据库中", applyMsg)

			var callback = func(err error, value string, term int) {}
			if ck, ok := kv.OpCallbacks[strconv.Itoa(applyIndex)]; ok {
				callback = ck
				delete(kv.OpCallbacks, strconv.Itoa(applyIndex))
			}

			if !kv.containShard(key2shard(key)) && op.Type != CONFIG && op.Type != DELETE {
				callback(fmt.Errorf(ErrWrongGroup), "", applyTerm)
				return
			}

			duplicate := kv.isDuplicate(op.ClientId, op.Ticket)
			DPrintf("Group %d 当前的配置为 %+v, ticket %+v, database %+v", kv.gid, kv.config, kv.Tickets, kv.Database)
			defer func() {
				if !duplicate {
					DPrintf("Group %d apply操作%+v到数据库中完成, 当前的数据库为 %+v", kv.gid, op, kv.Database)
					kv.Tickets[op.ClientId] = op.Ticket
				}
			}()

			shardIndex := key2shard(key)
			switch op.Type {
			case GET:
				result := ""
				if val, ok := kv.Database[shardIndex][key]; ok {
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

				if val, ok := kv.Database[shardIndex][key]; ok {
					newValue = val + op.Value
					kv.Database[shardIndex][key] = newValue
				} else {
					kv.Database[shardIndex][key] = newValue
				}

				callback(nil, "", applyTerm)
			case PUT:
				newValue := op.Value

				if duplicate {
					DPrintf(">>>>>>> 操作 %s 重复，跳过PUT处理", ticket)
					callback(nil, "", applyTerm)
					return
				}
				kv.Database[shardIndex][key] = newValue
				callback(nil, "", applyTerm)
			case DELETE:
				if op.DeleteNum <= kv.config.Num {
					if kv.gid != kv.config.Shards[op.DeleteShardId] {
						kv.Database[op.DeleteShardId] = make(map[string]string) // 清空
					}
				}
				callback(nil, "", applyTerm)
			case CONFIG:
				if op.Config.Num != kv.config.Num+1 {
					DPrintf("@@@@@@@@@@@@@@@@@@@@@ -> Group %d not match old(%d) new(%d)", kv.gid, kv.config.Num, op.Config.Num)
					callback(nil, "", applyTerm)
					return
				} else {
					DPrintf("@@@@@@@@@@@@@@@@@@@@@ -> Group %d match old(%d) new(%d) matched", kv.gid, kv.config.Num, op.Config.Num)
				}
				for shardId, shardData := range op.MigrationData {
					for key, value := range shardData {
						kv.Database[shardId][key] = value
					}
				}

				for clientId := range op.MigrationTicket {
					if _, ok := kv.Tickets[clientId]; !ok { // 不存在, 那么肯定设置
						kv.Tickets[clientId] = op.MigrationTicket[clientId]
					} else {
						latestTicketId, err1 := strconv.Atoi(kv.Tickets[clientId])
						nextTicketId, err2 := strconv.Atoi(op.MigrationTicket[clientId])
						if err1 != nil || err2 != nil {
							// ignore
						} else if nextTicketId > latestTicketId {
							kv.Tickets[clientId] = op.MigrationTicket[clientId]
						}
					}

				}
				previousConfig := kv.config
				kv.config = op.Config
				DPrintf("^^^^^^^^ Group %d updating the config from %d to %d", kv.gid, previousConfig.Num, kv.config.Num)
				// 将老的shard删除
				for shardId, shardData := range op.MigrationData {
					if len(shardData) > 0 {
						gid := previousConfig.Shards[shardId]
						args := DeleteShardArgs{Num: previousConfig.Num, ShardId: shardId}
						go kv.sendDeleteShard(gid, &previousConfig, &args, &DeleteShardReply{})
					}
				}
				callback(nil, "", applyTerm)
			}

		}()
		kv.buildSnapshotIfNeed(applyMsg.CommandIndex)
	}
}

func (kv *ShardKV) pollConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("!!!!!!!!!!!! Group %d(me: %d) goging to pull the lastest config..., current config is %+v", kv.gid, kv.me, kv.config)
			latestConfig := kv.sm.Query(-1)
			DPrintf("Group %d pull the lastest config %+v", kv.gid, latestConfig)
			// 下发更新配置的操作，通过raft来更新配置
			// 当前配置可能和最新的配置之间存在多个变动的配置
			// 这里的配置需要按顺序逐个进行Apply
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				nextConfig := kv.sm.Query(i)
				DPrintf("~~~~~~~~~~~~ Group %d Migrate the config from %d to %d", kv.gid, kv.config.Num, nextConfig.Num)
				entry, ok := kv.getUpdateConfigLogEntry(nextConfig)
				if !ok {
					DPrintf("!!!!!!!!Group %d migrate config from %d to %d failed, fetch log entry failed", kv.gid, kv.config.Num, nextConfig.Num)
					break
				}

				// 将Op操作推入到Raft日志中
				DPrintf("!!!!!!!!!!!!!!!!![Group %d] Push the Log to the raft, entry %+v", kv.gid, entry)
				ok = kv.doRaft(entry)
				if !ok {
					DPrintf("!!!!!!!!Group %d Push the log to the raft failed, entry %+v", kv.gid, entry)
					break
				} else {
					DPrintf("!!!!!!!!Group %d Push the log to the raft success, entry %+v", kv.gid, entry)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getUpdateConfigLogEntry(nextConfig shardmaster.Config) (reconfigureOp Op, success bool) {
	entry := Op{Type: CONFIG, Config: nextConfig}
	for i := 0; i < len(entry.MigrationData); i++ {
		entry.MigrationData[i] = make(map[string]string)
	}
	entry.MigrationTicket = make(map[string]string)
	migrationDone := true
	// 计算需要迁移的元数据
	migrationShardMetadata := kv.calcMigrationShardMetadata(nextConfig)
	DPrintf("FFFFFFFFFFFFFF Group %d migration shard metadata %+v", kv.gid, migrationShardMetadata)
	// 开始从对应的group获取所有需要进行迁移数据
	var migrationEntryLock sync.Mutex
	var wg sync.WaitGroup
	for migrationGId, migrationShardIds := range migrationShardMetadata {
		wg.Add(1)
		go func(migrationGID int, args MigrateShardArgs, reply MigrateShardReply) {
			defer wg.Done()
			// 开始请求需要迁移的数据
			if kv.fetchMigrationFromGroup(migrationGID, &args, &reply) {
				DPrintf("FFFFFFFFFFFFFF Group %d migrate from %d shard success, metadata %+v", kv.gid, migrationGID, migrationShardMetadata)
				migrationEntryLock.Lock()
				// 开始构造数据
				for _, shardId := range args.ShardIds {
					shardData := reply.Data[shardId]
					for key, value := range shardData {
						entry.MigrationData[shardId][key] = value
					}
				}

				// 开始构造客户端的响应
				for clientId := range reply.Ack {
					if _, ok := entry.MigrationTicket[clientId]; !ok {
						entry.MigrationTicket[clientId] = reply.Ack[clientId] // 更新client的ticket
					}
				}
				migrationEntryLock.Unlock()
			} else {
				DPrintf("FFFFFFFFFFFFFF Group %d migrate from %d shard failed, metadata %+v", kv.gid, migrationGID, migrationShardMetadata)
				migrationDone = false
			}
		}(migrationGId, MigrateShardArgs{
			SourceGid: kv.gid,
			Num:       nextConfig.Num,
			ShardIds:  migrationShardIds,},
			MigrateShardReply{})
	}
	wg.Wait()
	DPrintf("FFFFFFFFFFFFFF Group %d migrate metadata %+v finished, entry is %+v, result is %+v", kv.gid, migrationShardMetadata, entry, migrationDone)
	return entry, migrationDone
}

func (kv *ShardKV) fetchMigrationFromGroup(migrationGID int, args *MigrateShardArgs, reply *MigrateShardReply) bool {
	DPrintf("FFFFFFFFFFFF Group %d start fetchng migrating %+v from %d", kv.gid, args.ShardIds, migrationGID)
	for _, server := range kv.config.Groups[migrationGID] {
		endPoint := kv.make_end(server)
		ok := endPoint.Call("ShardKV.MigrateShard", args, reply)
		if ok {
			if reply.Err == OK {
				DPrintf("FFFFFFFFFFFF Group %d fetch migrating %+v from %d success", kv.gid, args.ShardIds, migrationGID)
				return true
			}

			if reply.WrongLeader {
				DPrintf("FFFFFFFFFFFF Group %d fetch migrating %+v from %d failed, wrong leader", kv.gid, args.ShardIds, migrationGID)
				continue
			}

			if reply.Err == ErrExpire {
				DPrintf("FFFFFFFFFFFF Group %d fetch migrating %+v from %d failed, expire config, %s", kv.gid, args.ShardIds, migrationGID, reply.Msg)
				return false
			}
		} else {
			DPrintf("FFFFFFFFFFFF Group %d fetch migrating %+v from %d failed, rpc failed", kv.gid, args.ShardIds, migrationGID)
			continue
			//return false
		}
	}
	return true
}

// 计算本次需要拉取的新的配置信息, 表示从哪个group迁移哪个shard到当前的服务器
// 返回 gid ->  [shard_index]
func (kv *ShardKV) calcMigrationShardMetadata(newConfig shardmaster.Config) map[int][]int {
	migrationShardMetadata := make(map[int][]int)
	for i := 0; i < len(newConfig.Shards); i++ {
		if kv.gid != kv.config.Shards[i] && kv.gid == newConfig.Shards[i] {
			// 之前不再群组，现在在群组里面的Shard
			gid := kv.config.Shards[i]
			// 初始化的情况下，不需要进行迁移
			if gid != 0 {
				if _, ok := migrationShardMetadata[gid]; !ok {
					// 需要开始从对应的group迁移数据
					migrationShardMetadata[gid] = make([]int, 0)
				}

				// 需要从gid开始迁移索引为i的Shard
				migrationShardMetadata[gid] = append(migrationShardMetadata[gid], i)
			}
		}
	}
	return migrationShardMetadata
}

func (kv *ShardKV) buildSnapshotIfNeed(index int) {
	DPrintf(">>>>>>>>>>>>>>>> Building the snapshot ...")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 {
		return
	}

	if kv.rf.StateSize() < kv.maxraftstate {
		//DPrintf("skip snapshot, raft server %d current state size is %d, less that %d, skip snapshot",
		//	kv.rf.Me(), kv.rf.StateSize(), kv.maxraftstate)
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Database)
	e.Encode(kv.Tickets)
	e.Encode(kv.config)
	data := w.Bytes()
	kv.rf.BuildSnapshot(index, data, kv.maxraftstate)
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	DPrintf("<<<<<<<<<<<<<<< Loading the snapshot ...")
	if data == nil || len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.Database)
	d.Decode(&kv.Tickets)
	d.Decode(&kv.config)
}

func (kv *ShardKV) sendDeleteShard(gid int, previewConfig *shardmaster.Config, args *DeleteShardArgs, reply *DeleteShardReply) bool {
	for _, server := range previewConfig.Groups[gid] {
		endpoint := kv.make_end(server)
		ok := endpoint.Call("ShardKV.DeleteShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}

			if reply.Err == ErrExpire {
				return false
			}
		}
	}
	return true
}

// 删除shard
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	if args.Num > kv.config.Num { // 客户端还没有取到最新的配置，需要等待
		reply.Err = ErrExpire
		return
	}

	deleteEntry := Op{Type: DELETE, DeleteNum: args.Num, DeleteShardId: args.ShardId}
	// 将日志提交到Raft中
	ok := kv.doRaft(deleteEntry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

// 迁移shard
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	DPrintf("MMMMMMMMMM Group %d start migrating the data %+v to %d", kv.gid, args, args.SourceGid)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num < args.Num {
		// 当前被请求迁移的服务器还没有获取到最新的配置, 需要等待同步配置，稍后重试
		DPrintf("MMMMMMMMMM Group %d start migrate the data config failed, too old config num %d(args config num is %d)", kv.gid, kv.config.Num, args.Num)
		reply.Err = ErrExpire
		reply.Msg = fmt.Sprintf("current config is %d", kv.config.Num)
		return
	}
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}

	// 迁移指定shard的数据
	for _, shardId := range args.ShardIds {
		for key, value := range kv.Database[shardId] {
			reply.Data[shardId][key] = value
		}
	}
	reply.Ack = make(map[string]string)
	for clientId, ticket := range kv.Tickets {
		reply.Ack[clientId] = ticket
	}
	reply.Err = OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	syncLock.Opts.DeadlockTimeout = time.Millisecond * 10000
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register(map[string]string{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.sm = shardmaster.MakeClerk(masters)
	kv.config.Num = -1

	// Your initialization code here.
	kv.OpCallbacks = make(map[string]func(error, string, int))
	kv.Database = [shardmaster.NShards]map[string]string{}
	for i := range kv.Database {
		kv.Database[i] = make(map[string]string)
	}
	kv.Tickets = make(map[string]string)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.loadSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.applyOp()
	go kv.pollConfig()

	return kv
}
