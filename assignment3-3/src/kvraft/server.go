package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Request struct {
	Command  string
	Index    string
	Op       string
	ClientId int
	Commited bool
}

func (req *Request) ToStr() string {
	fields := make([]string, 0)
	fields = append(fields, req.Command)
	fields = append(fields, req.Index)
	fields = append(fields, req.Op)
	fields = append(fields, strconv.Itoa(req.ClientId))

	return strings.Join(fields, "|")
}

func (req *Request) FromStr(reqStr string) {
	fields := strings.Split(reqStr, "|")

	req.Command = fields[0]
	req.Index = fields[1]
	req.Op = fields[2]
	req.ClientId, _ = strconv.Atoi(fields[3])
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	requests map[int]Request
	services int
}

func (kv *RaftKV) Print(msg string, msg2 interface{}) {
	kv.rf.DebugPrint = true
	kv.rf.Print(msg, msg2)
	//kv.rf.DebugPrint = false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	kv.Print("Get", args)

	if kv.rf.GetNodeType() != "Leader" {
		reply.WrongLeader = true
		kv.Print("Get Wrong Leader", args)
		return
	}

	reply.WrongLeader = false

	//Fetch the values until commited
	result := make([]string, 0)
	ok := false
	for !ok {

		if kv.rf.GetNodeType() != "Leader" {
			reply.WrongLeader = true
			kv.Print("Get Wrong Leader", args)
			return
		}

		time.Sleep(time.Duration(100) * time.Millisecond)

		kv.mu.Lock()

		ok = true
		result = make([]string, 0)
		kv.Print("Pending Req", kv.requests)

		commitReq := make([]int, 0)

		for Idx, req := range kv.requests {

			//Found one request
			if req.Index == args.Key {

				if !req.Commited {
					ok = false
					kv.Print("Not Commited Yet", req)
					break
				} else {
					commitReq = append(commitReq, Idx)
				}
			}
		}

		//Sort the found keys
		sort.Slice(commitReq, func(i, j int) bool {
			return commitReq[i] < commitReq[j]
		})

		for _, reqIdx := range commitReq {

			req := kv.requests[reqIdx]

			if req.Op == "Put" {
				result = make([]string, 0)
			}

			//Asume append
			add := true
			for _, cmd := range result {
				if cmd == req.Command {
					add = false
					break
				}
			}

			if add {
				result = append(result, req.Command)
			}
		}

		kv.mu.Unlock()
	}

	reply.Value = strings.Join(result, "")

	kv.Print("Get Done", reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.Print("Put", args)

	if kv.rf.GetNodeType() != "Leader" {
		reply.WrongLeader = true
		kv.Print("Put Wrong Leader", args)
		return
	}

	reply.WrongLeader = false

	//Create the new requests
	req := Request{args.Value, args.Key, args.Op, args.ClientId, false}
	idx, _, _ := kv.rf.Start(req.ToStr())

	kv.mu.Lock()
	kv.requests[idx] = req
	kv.mu.Unlock()

	// Wait until commited
	/*ok := false
	  for ! ok {

	      time.Sleep(time.Duration(100) * time.Millisecond)

	      if kv.rf.GetNodeType() != "Leader" {
	          reply.WrongLeader = true
	          kv.Print("Get Wrong Leader", args)
	          return
	      }

	      kv.mu.Lock()
	      if kv.requests[idx].Commited {
	          ok = true
	      }
	      kv.mu.Unlock()
	  }*/

	kv.Print("Put Done", reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()

	for kv.services > 0 {
		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	kv.rf.Kill()
}

func (kv *RaftKV) BecameLeader() {

	kv.services += 1
	lastState := kv.rf.GetNodeType()

	for !kv.rf.IsKilled() {
		time.Sleep(time.Duration(50) * time.Millisecond)

		currentState := kv.rf.GetNodeType()

		if lastState == "Follower" && currentState == "Candidate" {
			//kv.rf.SetLeaderId(-1)
		}

		if lastState == "Candidate" && currentState == "Leader" {

			kv.Print("Migrate Requests", nil)

			kv.mu.Lock()
			kv.rf.MutexLock()

			for idx, entry := range kv.rf.GetHistory() {

				strCommand, ok := entry.Command.(string)
				if ok {
					req := Request{}
					req.FromStr(strCommand)

					if idx-1 <= kv.rf.GetCommitIndex() {
						req.Commited = true
					} else {
						req.Commited = false
					}

					kv.requests[idx-1] = req
				}
			}

			kv.Print("Migrate Requests Done", kv.requests)

			kv.rf.MutexUnlock()
			kv.mu.Unlock()
		}

		lastState = currentState
	}

	kv.services -= 1
}

func (kv *RaftKV) ReadChannelService() {

	kv.services += 1

	for !kv.rf.IsKilled() {
		time.Sleep(time.Duration(50) * time.Millisecond)

		commitIteration := 0

		select {
		case msg := <-kv.applyCh:
			kv.Print("SuperCommited", msg)
			if msg.Index != 0 {

				kv.mu.Lock()
				req := kv.requests[msg.Index]

				//Could happend a posible change of leader
				strCommand := msg.Command.(string)
				req.FromStr(strCommand)
				req.Commited = true

				kv.requests[msg.Index] = req
				kv.mu.Unlock()
			}
		default:
			commitIteration += 1

			if commitIteration > 20 {

				kv.mu.Lock()

				for idx, req1 := range kv.requests {
					if idx < kv.rf.GetCommitIndex() && !req1.Commited {

						req := kv.requests[idx]
						req.Commited = true
						kv.requests[idx] = req
						kv.Print("Force of Commit", req)
					}
				}

				kv.mu.Unlock()

				commitIteration = 0
			}

		}
	}

	kv.services -= 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.DebugPrint = true

	kv.requests = make(map[int]Request)
	kv.services = 0

	go kv.ReadChannelService()
	go kv.BecameLeader()

	return kv
}

func (kv *RaftKV) Bootstrap() {
	kv.rf.Bootstrap()
}
