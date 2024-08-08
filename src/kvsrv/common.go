package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string
	ClientId int64
	SeqNum   int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Value string
}
