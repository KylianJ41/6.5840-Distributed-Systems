package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	id     int64 // unique id of this client
	seqNum int64 // sequence number for requests
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.id = nrand()
	ck.seqNum = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
		SeqNum:   ck.seqNum,
	}
	ck.seqNum++
	for {
		reply := GetReply{}
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		SeqNum:   ck.seqNum,
	}
	ck.seqNum++
	for {
		reply := PutAppendReply{}
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
