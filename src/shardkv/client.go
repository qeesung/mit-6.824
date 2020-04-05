package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"fmt"
	"labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type TicketCounter int64

func (counter *TicketCounter) increment() int64 {
	return atomic.AddInt64((*int64)(counter), 1)
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	Id              string
	TicketIdCounter TicketCounter
	mu              sync.RWMutex
	// 每一个组对应的leader
	groupLeaderIds map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config.Num = -1
	ck.Id = UUID()
	ck.TicketIdCounter = 0
	ck.groupLeaderIds = make(map[int]int)
	return ck
}

func (ck *Clerk) fetchNextTicket() string {
	return fmt.Sprintf("%d", ck.TicketIdCounter.increment())
}

func (ck *Clerk) fetchConfigIfEmpty() {
	ck.mu.Lock()
	if ck.config.Num != -1 {
		ck.mu.Unlock()
		return
	}
	ck.mu.Unlock()
	ck.updateConfig()
}

func (ck *Clerk) updateConfig() {
	newConfig := ck.sm.Query(-1)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if newConfig.Num >= ck.config.Num {
		DPrintf("[Client %s] Update the config to %+v", ck.Id, newConfig)
		ck.config = newConfig
	}
}

func (ck *Clerk) getConfig() shardmaster.Config {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	return shardmaster.CopyConfig(ck.config)
}

func (ck *Clerk) resetConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.config.Num = -1
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: ck.fetchNextTicket()}}
	args.Key = key

	for {
		shard := key2shard(key)
		ck.fetchConfigIfEmpty()
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// 如果组中没有服务器, 退出尝试重新获取配置
			if len(servers) == 0 {
				break
			}

			// 获取组中的初始化leader, 如果没有记录leader，那么设置为0
			groupLeader, ok := ck.groupLeaderIds[gid]
			if !ok {
				groupLeader = 0
			}

			// try each server for the shard.
			for si := groupLeader; si < groupLeader+len(servers); si++ {
				DPrintf("[Client %s] fetching the key %s of shard %d from %+v", ck.Id, key, shard, servers)
				shardKVServerIndex := si % len(servers)
				srv := ck.make_end(servers[shardKVServerIndex])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.groupLeaderIds[gid] = shardKVServerIndex // update the config
					return reply.Value
				}

				if !ok {
					DPrintf("[Client %s]ShardKV.GET the key %s failed, request failed", ck.Id, key)
					continue
				}

				if ok && reply.WrongLeader {
					DPrintf("[Client %s]ShardKV.GET the key %s failed, wrong leader", ck.Id, key)
					continue
				}

				if ok && reply.Err == ErrTimeout {
					DPrintf("[Client %s]ShardKV.GET the key %s failed, timeout...", ck.Id, key)
					continue
				}

				if ok && (reply.Err == ErrWrongGroup) {
					// 配置不合法，需要马上退出需要更新配置
					DPrintf("[Client %s]ShardKV.GET the key %s failed, wrong group config, refresh config later...", ck.Id, key)
					DPrintf("[Client %s] Current the config %+v", ck.Id, ck.config)
					ck.resetConfig()
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("[Client %s]Shard.%sing the key %s to value %s", ck.Id, op, key, value)
	args := PutAppendArgs{Key: key, Value: value, Op: op, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: ck.fetchNextTicket()}}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		ck.fetchConfigIfEmpty()
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// 如果组中没有服务器, 退出尝试重新获取配置
			if len(servers) == 0 {
				break
			}

			// 获取组中的初始化leader, 如果没有记录leader，那么设置为0
			groupLeader, ok := ck.groupLeaderIds[gid]
			if !ok {
				groupLeader = 0
			}

			for si := groupLeader; si < groupLeader+len(servers); si++ {
				shardKVServerIndex := si % len(servers)
				srv := ck.make_end(servers[shardKVServerIndex])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					DPrintf("[Client %s]ShardKV.%s the key %s : value %s successfully", ck.Id, op, key, value)
					ck.groupLeaderIds[gid] = shardKVServerIndex // update the config
					return
				}
				if !ok {
					DPrintf("[Client %s]ShardKV.%s the key %s failed, request failed", ck.Id, op, key)
					continue
				}

				if ok && reply.WrongLeader {
					DPrintf("[Client %s]ShardKV.%s the key %s failed, wrong leader", ck.Id, op, key)
					continue
				}

				if ok && reply.Err == ErrTimeout {
					DPrintf("[Client %s]ShardKV.%s the key %s failed, timeout...", ck.Id, op, key)
					continue
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("[Client %s]ShardKV.%s the key %s failed, wrong group config, refresh config later...", ck.Id, op, key)
					ck.resetConfig()
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
