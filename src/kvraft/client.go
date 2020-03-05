package kvraft
// TODO: There is a problem. If I timeout for a get or put request, it might be out of sync with later sequences anyway
import (
	"fmt"
	"labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int
	sequenceNumber int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var clerkId int32 = 0

func getClerkId() int {
	return int(atomic.AddInt32(&clerkId, 1))
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id  = getClerkId()
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
	fmt.Println("Clerk - Get - " + key)
	args := GetArgs{
		Key:key,
		Client: ck.id,
		SequenceNumber: ck.getSequenceNumber(),
	}
	reply := GetReply{}
	for j := 0; j < 2; j += 1 {
		for i := 0; i < len(ck.servers); i += 1 {
			ck.servers[i].Call("KVServer.Get", &args, &reply)

			if reply.Err == OK {
				fmt.Println("Clerk - Get - "+key, "SUCCESS AT", i)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				fmt.Println("Clerk - Get - "+key, "NO KEY", i)
				return ""
			}
		}
		<- time.After(200 * time.Millisecond)
		fmt.Println("Clerk - Get - " + key, "RETRY")
	}
	fmt.Println("Clerk - Get - " + key, "FAIL")

	// TODO: When there are no leaders
	return ""
}

func (ck *Clerk) getSequenceNumber() int{
	return int(atomic.AddInt32(&ck.sequenceNumber, 1))
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
	fmt.Println("Clerk -", op,"-", key, "-", value)
	args := PutAppendArgs{
		Key:  key,
		Value: value,
		Op:    op,
		Client: ck.id,
		SequenceNumber: ck.getSequenceNumber(),
	}
	reply := PutAppendReply{}
	for j := 0; j < 2; j += 1 {
		for i := 0; i < len(ck.servers); i += 1 {

			fmt.Println("Clerk -", op,"-", key, "-", value, "- trying", i)
			ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if reply.Err == OK {
				fmt.Println("Clerk -", op, "-", key, "-", value, "-", "SUCCESS AT", i)
				return
			}
		}
		<- time.After(200 * time.Millisecond)
		fmt.Println("Clerk -", op,"-", key, "-", value, "-", "RETRY")
	}
	fmt.Println("Clerk -", op,"-", key, "-", value, "-", "FAIL")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
