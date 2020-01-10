package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	Id     string
	leader int
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
	ck.Id = UUID()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	DPrintf("[Client %s]ShardMaster.Querying #%d...", ck.Id, num)
	args := &QueryArgs{Num: num, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: UUID()}}
	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		srv := ck.servers[currentLeader]
		index++

		reply := QueryReply{}
		ok := srv.Call("ShardMaster.Query", args, &reply)
		if !ok {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, request failed", ck.Id, num)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, wrong leader", ck.Id, num)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, %s", ck.Id, num, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[Client %s]ShardMaster.Query #%d successfully, result is %+v", ck.Id, num, reply.Config)
		ck.leader = currentLeader
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	DPrintf("[Client %s] ShardMaster.Joining servers %+v", ck.Id, servers)
	args := &JoinArgs{Servers: servers, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: UUID()}}
	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		srv := ck.servers[currentLeader]
		index++

		reply := JoinReply{}
		ok := srv.Call("ShardMaster.Join", args, &reply)
		if !ok {
			DPrintf("[Client %s] ShardMaster.Join servers %+v failed, request failed", ck.Id, servers)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s] ShardMaster.Join servers %+v failed, wrong leader", ck.Id, servers)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s] ShardMaster.Join servers %+v failed, %s", ck.Id, servers, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		DPrintf("[Client %s] ShardMaster.Join servers %+v successfully", ck.Id, servers)
		ck.leader = currentLeader
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	DPrintf("[Client %s]ShardMaster.Leaving the group %+v...", ck.Id, gids)
	args := &LeaveArgs{GIDs: gids, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: UUID()}}

	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		srv := ck.servers[currentLeader]
		index++
		// try each known server.
		reply := LeaveReply{}
		ok := srv.Call("ShardMaster.Leave", args, &reply)

		if !ok {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, request failed", ck.Id, gids)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, wrong leader", ck.Id, gids)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, %s", ck.Id, gids, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		DPrintf("[Client %s]ShardMaster.Leave the group %+v successfully", ck.Id, gids)
		ck.leader = currentLeader
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	DPrintf("[Client %s]ShardMaster.Moving the shard %d to group %d", ck.Id, shard, gid)
	args := &MoveArgs{MoveShardArgs: MoveShardArgs{Shard: shard, GID: gid}, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: UUID()}}

	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		srv := ck.servers[currentLeader]
		index++
		// try each known server.
		reply := MoveReply{}
		ok := srv.Call("ShardMaster.Move", args, &reply)

		if !ok {
			DPrintf("[Client %s]ShardMaster.Move the shard %d to group %d failed, request failed", ck.Id, shard, gid)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Move the shard %d to group %d failed, wrong leader", ck.Id, shard, gid)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Move the shard %d to group %d failed, %s", ck.Id, shard, gid, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		DPrintf("[Client %s]ShardMaster.Move the shard %d to group %v successfully", ck.Id, shard, gid, reply.Err)
		ck.leader = currentLeader
		return
	}
}
