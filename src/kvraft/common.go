package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientId string
	Op       string // "Put" or "Append"
	Ticket   string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key      string
	ClientId string
	Ticket   string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
