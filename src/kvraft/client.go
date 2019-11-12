package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
import "github.com/satori/go.uuid"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
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
	ticket := UUID()
	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		DPrintf("[client]客户端准备从服务器 查询 KEY '%s', UUID '%s'", key, ticket)
		reply := GetReply{}
		args := GetArgs{Key: key, Ticket: ticket}
		ok := ck.servers[currentLeader].Call("KVServer.Get", &args, &reply)
		index++

		if !ok {
			DPrintf("[client]客户端GET KEY '%s' UUID '%s' 到服务器 %d 失败", key, args.Ticket, currentLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.WrongLeader {
			DPrintf("[client]客户端GET KEY '%s' UUID '%s' 到服务器 %d 失败, 错误的leader", key, args.Ticket, currentLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err != "" {
			DPrintf("[client]客户端GET KEY '%s' UUID '%s' 到服务器 %d 失败, %s", key, args.Ticket, currentLeader, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = currentLeader
		DPrintf("[client]客户端查询key %s 成功，UUID '%s' 值为 %s", key, args.Ticket, reply.Value)
		return reply.Value
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

	ticket := UUID()
	index := ck.leader
	for {
		DPrintf("[client]准备%s '%s'到Key %s, Ticket %s, 服务器%d", op, value, key, ticket, index%len(ck.servers))
		currentLeader := index % len(ck.servers)
		reply := PutAppendReply{WrongLeader: false}
		args := PutAppendArgs{Key: key, Value: value, Op: op, Ticket: ticket}
		ok := ck.servers[currentLeader].Call("KVServer.PutAppend", &args, &reply)
		index++

		if !ok {
			DPrintf("[client]客户端PutAppend %s 到服务器 %d 失败", args.Ticket, currentLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.WrongLeader {
			DPrintf("[client]客户端PutAppend %s 到服务器 %d 失败，错误的Leader", args.Ticket, currentLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err != "" {
			DPrintf("[client]客户端GET KEY '%s' UUID '%s' 到服务器 %d 失败, %s", key, args.Ticket, currentLeader, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		DPrintf("[client]客户端PutAppend %s 到服务器 %d 成功", args.Ticket, currentLeader)
		ck.leader = currentLeader
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func UUID() string {
	id, _ := uuid.NewV4()
	return id.String()
}
