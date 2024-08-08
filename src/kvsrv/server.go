package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu          sync.Mutex
	data        map[string]string
	lastApplied map[int64]int64
	lastValue   map[int64]string // Store last value for each client
}

func (kv *KVServer) checkDuplicate(clientId int64, seqNum int64) (bool, string) {
	lastSeq, exists := kv.lastApplied[clientId]
	if !exists || seqNum > lastSeq {
		kv.lastApplied[clientId] = seqNum
		return false, ""
	}
	return true, kv.lastValue[clientId] // Return true and the last value for this client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	isDuplicate, lastValue := kv.checkDuplicate(args.ClientId, args.SeqNum)
	if isDuplicate {
		reply.Value = lastValue
		return
	}

	kv.data[args.Key] = args.Value
	kv.lastValue[args.ClientId] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	isDuplicate, lastValue := kv.checkDuplicate(args.ClientId, args.SeqNum)
	if isDuplicate {
		reply.Value = lastValue
		return
	}

	oldValue, exists := kv.data[args.Key]
	if exists {
		kv.data[args.Key] = oldValue + args.Value
	} else {
		kv.data[args.Key] = args.Value
	}
	reply.Value = oldValue
	kv.lastValue[args.ClientId] = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastValue = make(map[int64]string)

	return kv
}
