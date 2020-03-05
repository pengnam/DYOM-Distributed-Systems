package kvraft

import (
	"fmt"
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	fmt.Println("Clerk - Get - " + key)
	args := GetArgs{
		Key:key,
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
	}
	fmt.Println("Clerk - Get - " + key, "FAIL")

	// TODO: When there are no leaders
	return ""
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
	}
	fmt.Println("Clerk -", op,"-", key, "-", value, "-", "FAIL")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
