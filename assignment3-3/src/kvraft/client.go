package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "time"

type Clerk struct {
	me         int
	servers    []*labrpc.ClientEnd
	lastServer int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, me int) *Clerk {

	ck := new(Clerk)
	ck.me = me
	ck.servers = servers
	ck.lastServer = 0

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	ok := false
	value := ""

	for !ok {

		retry := false
		args := GetArgs{key, ck.me}
		reply := GetReply{}

		ok = ck.servers[ck.lastServer].Call("RaftKV.Get", &args, &reply)

		if ok {
			if reply.WrongLeader {
				retry = true
			} else {
				value = reply.Value
				fmt.Println("Call OK")
			}
		}

		if retry {
			ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
			time.Sleep(time.Duration(100) * time.Millisecond)
			ok = false
		}

	}

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ok := false

	for !ok {

		retry := false
		args := PutAppendArgs{key, value, op, ck.me}
		reply := PutAppendReply{}

		ok = ck.servers[ck.lastServer].Call("RaftKV.PutAppend", &args, &reply)

		if ok {
			if reply.WrongLeader {
				retry = true
			}
		} else {
			retry = true
		}

		if retry {
			ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
			time.Sleep(time.Duration(100) * time.Millisecond)
			ok = false
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
