package raftkv

import (
	"labrpc"
	"math/rand"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	Id      string
	// You will have to modify this struct.
	leader int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.Id = UUID()
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
		args := GetArgs{Key: key, Ticket: ticket, ClientId: ck.Id}
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
		args := PutAppendArgs{Key: key, Value: value, Op: op, Ticket: ticket, ClientId: ck.Id}
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func UUID() string {
	n := 6
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
