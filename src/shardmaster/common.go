package shardmaster

import (
	"log"
	"math/rand"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CopyConfig(srcConfig Config) Config {
	newConfig := Config{
		Num:    srcConfig.Num,
		Shards: srcConfig.Shards,
		Groups: make(map[int][]string),
	}

	for key, value := range srcConfig.Groups {
		newValue := make([]string, len(value))
		copy(newValue, value)
		newConfig.Groups[key] = newValue
	}
	return newConfig
}

const (
	OK = "OK"
)

type Err string

type Args interface {
	clientId() string
	ticket() string
}

type BaseArgs struct {
	ClientId string
	Ticket   string
}

func (args BaseArgs) clientId() string {
	return args.ClientId
}

func (args BaseArgs) ticket() string {
	return args.Ticket
}

type Reply interface {
	markWrongLeader()
	setCauseErr(err Err)
}

type BaseReply struct {
	WrongLeader bool
	Err         Err
}

func (r *BaseReply) markWrongLeader() {
	r.WrongLeader = true
}

func (r *BaseReply) setCauseErr(err Err) {
	r.Err = err
}

type JoinArgs struct {
	BaseArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	BaseReply
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	BaseReply
}

type MoveShardArgs struct {
	Shard int
	GID   int
}

type MoveArgs struct {
	BaseArgs
	MoveShardArgs
}

type MoveReply struct {
	BaseReply
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	BaseReply
	Config Config
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

const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf("@@@@@@@"+format, a...)
	}
	return
}
