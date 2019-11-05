package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "math/rand"

//import "runtime/debug" 

import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    // Persistent state on all servers
    currentTerm int
    votedFor map[int]int //key = term, value = serverId
    log []int

    // Volatile state on all servers
   commitIndex int
   lastApplied int

   // Volatile state on leaders
   nodeType string
   nextIndex []int
   matchIndex []int

   //New variables to control the laders
   timeoutLeader time.Time
}

func (rf *Raft) Print(Msg string, Msg2 int) {
    fmt.Println(Msg, Msg2, "[Me:", rf.me, "Type:", rf.nodeType, "Term:", rf.currentTerm, "]")
}

func (rf *Raft) CheckTimeoutLeader() {

    for true {

        switch rf.nodeType {
        case "Follower":
            now := time.Now()
            if now.After(rf.timeoutLeader) {
                rf.currentTerm += 1
                rf.nodeType = "Candidate"
                rf.ResetTime()
            }
        case "Candidate":
            votes := rf.startVotation()
            rf.Print("Votes", votes)
            if float32(votes) >= float32(len(rf.peers))/2.0 {
                if rf.nodeType == "Candidate" {
                    rf.Print("Became Leader", -1)
                    rf.nodeType = "Leader"
                }
            } else {
                rf.nodeType = "Follower"
            }
        case "Leader": 
            alive := rf.Heartbeat()
            if alive == 0 {
                rf.Print("Anybody response", -1)
                rf.nodeType = "Follower"
                rf.ResetTime()
            }
        }

    }
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    fmt.Println("GetState", rf.me, rf.nodeType, rf.currentTerm)

    if rf.nodeType == "Leader" {
        return rf.currentTerm, true
    } else {
        return rf.currentTerm, false
    }
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
}

//
// AppendEntries RPC arguments structure
//

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []int
    LeaderCommit int
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
    Term int
    Success bool
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        rf.Print("No success", -1)
        reply.Success = false

    } else {

        rf.nodeType = "Follower"
        rf.ResetTime()
        rf.currentTerm = args.Term

        //TODO thingswith the logs

        reply.Success = true
    }

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.MakeCall(server, "Raft.AppendEntries", args, reply)
}

func (rf *Raft) Heartbeat() int {

    count := 0

    //fmt.Println("HB", rf.nodeType, rf.me, rf.currentTerm)

    //Vote for self or requests votes
    for peerIdx, _ := range rf.peers {
        if peerIdx != rf.me {

            args := AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, 0}
            reply := AppendEntriesReply{}
            ok := rf.sendAppendEntries(peerIdx, args, &reply)
            if ok {
                count += 1
            }
        }
    }

    return count
}


//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {

        if rf.nodeType == "Candidate" && rf.currentTerm == args.Term {
            rf.votedFor[args.Term] = args.CandidateId
            reply.VoteGranted = false
        } else {

            _, exist := rf.votedFor[args.Term]
            if ! exist && args.CandidateId != -1 {
                rf.votedFor[args.Term] = args.CandidateId
                reply.VoteGranted = true
            } else {
                reply.VoteGranted = false
            }
        }
    }
}



//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
    return rf.MakeCall(server, "Raft.RequestVote", args, reply)
}

func (rf *Raft) CallWrapper(server int, endpoint string, args interface{}, reply interface{}, suc chan bool) {
    ok := rf.peers[server].Call(endpoint, args, reply)
    suc <- ok
}

func (rf *Raft) MakeCall(server int, endpoint string, args interface{}, reply interface{}) bool {

    channel := make(chan bool)
    go rf.CallWrapper(server, endpoint, args, reply, channel)

    timeout := time.Now().Add(time.Duration(500) * time.Millisecond)

    counter := 0
    for true {
        now := time.Now()
        counter += 1
        /*if endpoint != "Raft.AppendEntries" {
            fmt.Println("CurrentTime", now, timeout)
            counter = 0
        }*/
        if now.After(timeout) {
            rf.Print("Network Issue (" + endpoint + ") send it to", server)
            return false
        } else {
            select {
            case v := <-channel:
                return v
            default:
                //pass
            }
        }
    }
    return false
}

func (rf *Raft) startVotation() int {

    votes := 0

    //Vote for self or requests votes
    for peerIdx, _ := range rf.peers {
        if peerIdx == rf.me {
            votes += 1
        } else {

            args := RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
            reply := RequestVoteReply{}
            rf.Print("Send RPCVote", peerIdx)
            ok := rf.sendRequestVote(peerIdx, args, &reply)
            if ok {
                if reply.VoteGranted {
                    votes += 1
               }
           } else {
                rf.Print("RPC Fail", peerIdx)
           }
       }
   }

   return votes
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//rf.currentTerm = -1
//	rf.isLeader = true

//	return index, rf.currentTerm, rf.isLeader
return 0, 0, false
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ResetTime() {
   startTimeout := time.Duration(rand.Intn(500)+500) * time.Millisecond
   //startTimeout := time.Duration(2) * time.Second
   rf.timeoutLeader = time.Now().Add(startTimeout)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

    //Creation of the pair
    //fmt.Println("*** START")
    //debug.PrintStack()

    // Persistent state on all servers
    rf.currentTerm = 0
    rf.votedFor = make(map[int]int)
    rf.log = make([]int, 0)

    // Volatile state on all servers
   rf.commitIndex = 0
   rf.lastApplied = 0

   // Volatile state on leaders
   rf.nodeType = "Follower"
   rf.nextIndex = make([]int, 0)
   rf.matchIndex = make([]int, 0)

   rf.ResetTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go rf.CheckTimeoutLeader()


	return rf
}
