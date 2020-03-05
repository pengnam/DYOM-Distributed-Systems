package kvraft

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (  // iota is reset to 0
	GetCommand = iota  // c0 == 0
	PutCommand = iota  // c1 == 1
	AppendCommand = iota  // c2 == 2
)
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opcode int
	Key string
	Value string
	Server int
	Sequence int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	sequence int32


	maxraftstate int // snapshot if log grows this big
	values map[string]string
	returnValues map[int] chan int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	channel := kv.tryOp(Op {
		Opcode: GetCommand,
		Key: args.Key,
		Value: "",
	})
	if channel == nil {
		reply.Err = ErrWrongLeader
		return
	}
	<- channel

	reply.Err = OK

	if val, ok := kv.values[args.Key]; ok {
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := PutCommand
	if args.Op == "Append" {
		op = AppendCommand
	}
	channel := kv.tryOp(Op{
		Key: args.Key,
		Value: args.Value,
		Opcode: op,
	})

	if channel == nil {
		reply.Err = ErrWrongLeader
		return
	}

	<- channel
	reply.Err = OK

}

func (kv *KVServer) tryOp(op Op) chan int{
	// TODO: RETRIES??
	op.Server = kv.me
	op.Sequence = int(kv.getSequenceNumber())

	_, _, leader := kv.rf.Start(op)
	if !leader {
		return nil
	}
	fmt.Println(op.Opcode, "OP for server", kv.me, ":", op.Key, op.Value)
	channel := make(chan int)
	kv.returnValues[op.Sequence] = channel
	return channel
}





func (kv *KVServer) handleApplyChan() {
	for {
		message := <- kv.applyCh
		op := message.Command.(Op)
		seq := op.Sequence
		if op.Server == kv.me {
			if channel, ok := kv.returnValues[seq]; ok {
				channel <- 1
				delete(kv.returnValues, seq)
			} else {
				log.Fatal("NO CHANNEL FOR SEQUENCE: ", seq, "AT", kv.me)
			}
		}
		kv.mu.Lock()
		switch op.Opcode {
		case GetCommand:
		case PutCommand:
			kv.values[op.Key] = op.Value
		case AppendCommand:
			if _, ok := kv.values[op.Key]; ok{
				kv.values[op.Key] += op.Value
			} else {
				kv.values[op.Key] = op.Value
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) getSequenceNumber() int32 {
	return atomic.AddInt32(&kv.sequence, 1)
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.mu = sync.Mutex{}

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.returnValues = make(map[int] chan int)
	kv.values = make(map[string] string)

	go kv.handleApplyChan()


	// You may need initialization code here.

	return kv
}
