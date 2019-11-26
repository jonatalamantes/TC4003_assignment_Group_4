package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new.history entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the.history, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "strconv"
//import "math/rand"

//import "runtime/debug" 

import "fmt"

//
// as each Raft peer becomes aware that successive.history entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
    Term int
    Command int
    Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
    applyCh chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    // Persistent state on all servers
    currentTerm int
    votedFor map[int]int //key = term, value = serverId
    history []Entry
    leaderId int

    // Volatile state on all servers
   commitIndex int
   lastApplied int

   // Volatile state on leaders
   nodeType string
   nextIndex map[int]int //key = serverId, value = retries
   matchIndex map[int]int //key = serverId

   //New variables to control the laders
   timeoutLeader time.Time
   avgPackage time.Duration
   disconnected bool

   kill bool
   aliveServices int
   pingSend int
   heartbeatsCount int
}

func (rf *Raft) Print(Msg string, Msg2 interface{}) {
    fmt.Println(Msg, Msg2, "[Me:", rf.me,
                            "LeaderTimeout:", rf.timeoutLeader.Sub(time.Now()),
                            "Type:", rf.nodeType,
                            "CommitIndex:", rf.commitIndex,
                            "Term:", rf.currentTerm,
                            "Logs", rf.history, "]")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    rf.Print("GetState, LeaderId:", rf.leaderId)

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
    e.Encode(rf.history)
    e.Encode(rf.commitIndex)
    e.Encode(rf.lastApplied)
    e.Encode(rf.nodeType)
    e.Encode(rf.nextIndex)
    e.Encode(rf.matchIndex)
    e.Encode(rf.avgPackage)
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
    d.Decode(&rf.history)
    d.Decode(&rf.commitIndex)
    d.Decode(&rf.lastApplied)
    d.Decode(&rf.nodeType)
    d.Decode(&rf.nextIndex)
    d.Decode(&rf.matchIndex)
    d.Decode(&rf.avgPackage)

    if rf.nodeType == "Leader" {
        rf.nodeType = "Candidate"
    }

    rf.ResetTime()
    go rf.CheckTimeoutLeader()
    go rf.UpdateStateMachine()
    go rf.HandleNextAppendEntries()

    rf.Print("ReadPersister", nil)
}

/*
  Dynamic history interaction functions
*/
func (rf *Raft) FindCommand(cmd int) *Entry {
    for i := len(rf.history)-1; i >= 0; i-- {
        entry := rf.history[i]
        if cmd == entry.Command {
            return &entry
        }
    }
    return nil
}

func (rf *Raft) AppendCommand(cmd int) *Entry{
    add := true
    if len(rf.history) > 0 {
        if rf.history[len(rf.history)-1].Command == cmd {
            add = false
        }
    }
    if add {
        entry := Entry{rf.currentTerm, cmd, len(rf.history)}
        rf.history = append(rf.history, entry)
        return &entry
    } else {
        return &rf.history[len(rf.history)-1]
    }
}

func (rf *Raft) PopCommand() *Entry{
    if len(rf.history) > 0 {
        entry := rf.history[len(rf.history)-1]
        rf.history = rf.history[:len(rf.history)-1]
        return &entry
    }
    return nil
}

func (rf *Raft) MapCommands(from int) []int {
    cmds := make([]int, 0)
    if from > 0 {
        for i := from; i < len(rf.history); i++ {
            cmds = append(cmds, rf.history[i].Command)
        }
    }
    return cmds
}

func (rf *Raft) Clear() {
    rf.history = make([]Entry, 0)
}

//
// AppendEntries RPC arguments structure
//

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevEntry Entry
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
        rf.Print("AppendEntries, Incorrect Term", args)
        reply.Success = false

    } else {

        if rf.me != args.LeaderId {
            rf.nodeType = "Follower"
            rf.ResetTime()
            rf.currentTerm = args.Term
        } 

        rf.leaderId = args.LeaderId

        // Heartbeat package
        if len(args.Entries) == 0 {
            reply.Success = true
            return
        }

        rf.Print("Call AppendEntries", args)

        if args.PrevEntry.Index == -1 {
            rf.Clear()
            //rf.AppendCommand(args.PrevEntry.Command)
            for _, cmd := range args.Entries {
                rf.AppendCommand(cmd)
            }
            if args.LeaderCommit > rf.commitIndex {
                rf.commitIndex = args.LeaderCommit
            }
            rf.Print("Call AppendEntries Complete 1", args)
            reply.Success = true
            return
        }

        if args.PrevEntry.Index > len(rf.history)-1 {
            reply.Success = false
            rf.Print("AppendEntries PrevEntry not exists", args)
            return
        }

        prevEntry := rf.history[args.PrevEntry.Index]

        if prevEntry.Command != args.PrevEntry.Command {
            rf.Print("AppendEntries PrevEntry Term conflict", args)
            reply.Success = false
            return
        }

        //Override logs after prevEntry
        removed := rf.PopCommand()
        for removed.Index != args.PrevEntry.Index {
           removed = rf.PopCommand()
        }
        rf.AppendCommand(args.PrevEntry.Command)

        //Apply new commands
        for _, cmd := range args.Entries {
            rf.AppendCommand(cmd)
        }
        if args.LeaderCommit > rf.commitIndex {
            if len(rf.history)-1 > args.LeaderCommit {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = len(rf.history)-1
            }
        }

        rf.Print("Call AppendEntries Complete 2", args)
        reply.Success = true
    }
}

func (rf *Raft) Heartbeat() int {
    count := 0
    for peerIdx, _ := range rf.peers {
        if peerIdx != rf.me {
            entry := Entry{}
            args := AppendEntriesArgs{rf.currentTerm, rf.me, entry, nil, -1}
            reply := AppendEntriesReply{}
            reason := rf.SendAppendEntries(peerIdx, args, &reply)
            if reason == 0 {
                count += 1
            }
        }

        rf.heartbeatsCount += 1

        if rf.heartbeatsCount > 50 {
            if len(rf.history) > 1 {
                rf.Schedule(3)
            }
            rf.heartbeatsCount = 0
        }

    }
    return count
}

//
// Return reason of failure (0 = success, 1 = failure, 2 = network issue)
//
func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) int {
    if server == rf.me {
        rf.AppendEntries(args, reply)
        if reply.Success {
            return 0
        } else {
            return 1
        }
    } else {
        response := rf.MakeCall(server, "Raft.AppendEntries", args, reply, false)
        if ! response {
            return 2
        } else {
            if reply.Success {
                return 0
            } else {
                return 1
            }
        }
    }
}
//
// Ping structures
//
type PingArgs struct {
    Sleep int
}

type PingReply struct {
    Ok bool
}

func (rf *Raft) PingRPC (args PingArgs, reply *PingReply) {
    if args.Sleep != 0 {
        time.Sleep(time.Duration(args.Sleep) * time.Millisecond)
    }
    reply.Ok = true
}

func (rf *Raft) TestPing(serverId int, sleep int) {
    args := PingArgs{sleep}
    reply := PingReply{false}
    rf.MakeCall(serverId, "Raft.PingRPC", args, &reply, true)
    rf.pingSend += 1
}

func (rf *Raft) PeersAlives(retries int) int {
    count := 0
    for peerIdx, _ := range rf.peers {
        if peerIdx != rf.me {
            if rf.DoPing(peerIdx, retries) {
                count += 1
            }
        }
    }
    return count
}

func (rf *Raft) GetPingCount() int {
    return rf.pingSend
}

func (rf *Raft) DoPing(serverId int, retries int) bool {
    if rf.leaderId == -1 && serverId == rf.leaderId{
        return false
    }
    for i := 0; i < retries; i++ {
        args := PingArgs{0}
        reply := PingReply{false}
        ok := rf.MakeCall(serverId, "Raft.PingRPC", args, &reply, false)
        rf.pingSend += 1
        if ok && reply.Ok {
            return true
        }
    }
    return false
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastEntry Entry
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

    rf.Print("RequestVote", args)

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        rf.Print("RequestVote Incorrect Terms", args)
        return
    }

    votedFor, exist := rf.votedFor[args.Term]
    if ! exist && args.CandidateId != -1 {

        if args.LastEntry.Index >= len(rf.history) {
            rf.votedFor[args.Term] = args.CandidateId
            reply.VoteGranted = true
            rf.Print("RequestVote New Vote received and Upper Logs", reply)
            return
        }

        entry := rf.history[args.LastEntry.Index]

        if entry.Index < rf.commitIndex {
            reply.VoteGranted = false
            rf.Print("RequestVote No Vote for commit leaser", entry)
            return
        }

        if entry.Command == args.LastEntry.Command && entry.Term == args.LastEntry.Term {

            rf.votedFor[args.Term] = args.CandidateId
            reply.VoteGranted = true
            rf.Print("RequestVote New Vote received and Entry OK", entry)
            return
        }

        if args.LastEntry.Term > entry.Term {
            rf.votedFor[args.Term] = args.CandidateId
            reply.VoteGranted = true
            rf.Print("RequestVote New Vote with most updated entry", entry)
            return
        }

        rf.Print("RequestVote Denial, Incosistence Log", entry)
        reply.VoteGranted = false
        return
    }

    if votedFor == args.CandidateId {
        reply.VoteGranted = true
        rf.Print("RequestVote Already voted by args node", reply)
        return
    }

    if rf.leaderId != -1 {
        rf.Print("RequestVote Denial, Voted for", votedFor)
        reply.VoteGranted = false
        return
    }

    rf.votedFor[args.Term] = args.CandidateId
    reply.VoteGranted = true
    rf.Print("RequestVote Revoted since there is not leader", -1)
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
    return rf.MakeCall(server, "Raft.RequestVote", args, reply, false)
}


func (rf *Raft) Votation(retries int) int {

    votes := 0

    //Vote for self or requests votes
    for peerIdx := len(rf.peers)-1; peerIdx >= 0; peerIdx-- {

            lastEntry := rf.history[len(rf.history)-1]
            args := RequestVoteArgs{rf.currentTerm, rf.me, lastEntry}
            reply := RequestVoteReply{}
            ok := false
            for i := 0; i < retries && ! ok; i++ {
                ok = rf.sendRequestVote(peerIdx, args, &reply)
                if ok {
                    if reply.VoteGranted {
                        votes += 1
                   }
               }
           }
   }

   return votes
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's.history. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft.history, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command int) (int, int, bool) {

    if rf.nodeType == "Leader" {
        entry := rf.AppendCommand(command)
        rf.Schedule(3)
        return entry.Index, rf.currentTerm, true
    } else {
        return -1, rf.currentTerm, false
    }
}

func (rf *Raft) Schedule(retries int) {
    for peerIdx, _ := range rf.peers {

        rf.mu.Lock()
        retries, ok := rf.nextIndex[peerIdx]
        if ! ok {
            retries = 3
        }
        rf.nextIndex[peerIdx] = retries
        rf.mu.Unlock()
    }
    rf.Print("Schedule", rf.nextIndex)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {

    rf.kill = true
    for rf.aliveServices > 0 {
        time.Sleep(50 * time.Millisecond)
    }
    rf.Print("Kill", nil)
}

func (rf *Raft) ResetTime() {

    if rf.disconnected {
        rf.disconnected = false
    }

    rf.timeoutLeader = time.Now()
    ratio := time.Duration((rf.me+1)*150) * time.Duration(len(rf.peers)) * rf.avgPackage
    rf.timeoutLeader = rf.timeoutLeader.Add(ratio)
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
    rf.applyCh = applyCh

    // Persistent state on all servers
    rf.votedFor = make(map[int]int)
    rf.history = make([]Entry, 0)

    // Volatile state on all servers
   rf.commitIndex = -1
   rf.lastApplied = -1
   rf.currentTerm = 0

   // Volatile state on leaders
   if rf.me == 0 {
       rf.nodeType = "Candidate"
       rf.currentTerm = 1
   } else {
       rf.nodeType = "Follower"
   }
   rf.AppendCommand(-7)
   rf.leaderId = -1
   rf.nextIndex = make(map[int]int)

   rf.matchIndex = make(map[int]int)
   for peerIdx, _ := range(rf.peers) {
        rf.matchIndex[peerIdx] = -1
   }

   rf.disconnected = true
   rf.ResetTime()

   rf.kill = false
   rf.aliveServices = 0
   rf.pingSend = 0
   rf.avgPackage = time.Duration(500)  * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	return rf
}

func (rf *Raft) VolatilRefresh() {
   rf.nextIndex = make(map[int]int)

   rf.matchIndex = make(map[int]int)
   for peerIdx, _ := range(rf.peers) {
        rf.matchIndex[peerIdx] = -1
   }
}

func (rf *Raft) Bootstrap() {
    rf.CalculateBestTimeout(10)
}

/*
 Async functions
*/

func (rf *Raft) CheckTimeoutLeader() {

    rf.aliveServices += 1

    for ! rf.kill {

        time.Sleep(100 * time.Millisecond)

        if rf.disconnected {
            alives := rf.PeersAlives(3)
            if alives > 0 {
                rf.Print("ReConnected", nil)
                rf.nodeType = "Follower"
                rf.ResetTime()
            } else {
                rf.Print("I am disconnected", nil)
                continue
            }
        }

        switch rf.nodeType {
        case "Follower":
            now := time.Now()
            if now.After(rf.timeoutLeader) {
                alives := rf.PeersAlives(3)
                if alives > 0 {
                    leaderAlive := rf.DoPing(rf.leaderId, 3)
                    if ! leaderAlive {
                        rf.currentTerm += 1
                        rf.nodeType = "Candidate"
                    }
                } else {
                    rf.Print("Disconnected (Follower)", nil)
                    rf.disconnected = true
                    rf.leaderId = -1
                }
            }
        case "Candidate":
            votes := rf.Votation(3)
            rf.Print("Votes", votes)
            if float32(votes) >= float32(len(rf.peers))/2.0 {
                if rf.nodeType == "Candidate" {
                    rf.Print("Became Leader", nil)
                    rf.VolatilRefresh()
                    rf.nodeType = "Leader"
                } else {
                    rf.nodeType = "Follower"
                    rf.ResetTime()
                }
            } else {
                alives := rf.PeersAlives(3)
                if alives <= 0 {
                    rf.Print("Disconnected (Candidate)", nil)
                    rf.nodeType = "Follower"
                    rf.leaderId = -1
                    rf.disconnected = true
                }
            }
        case "Leader":
            alive := rf.Heartbeat()
            if alive == 0 {
                alives := rf.PeersAlives(3)
                if alives <= 0 {
                    rf.Print("Disconnected (Leader)", nil)
                    rf.nodeType = "Follower"
                    rf.leaderId = -1
                    rf.disconnected = true
                }
            }
        }
    }

    rf.aliveServices -= 1
}

func (rf *Raft) UpdateStateMachine() {

    rf.aliveServices += 1

    for ! rf.kill {

        time.Sleep(100 * time.Millisecond)

        //Save Persister
        rf.persist()

        // Apply command
        for rf.commitIndex > rf.lastApplied {
            rf.lastApplied += 1
            entry := rf.history[rf.lastApplied]
            apply := ApplyMsg{rf.lastApplied, entry.Command, false, nil}
            rf.applyCh <- apply
            rf.Print("ApplyCh", apply)
        }

        // Update Commit Index
        if rf.nodeType == "Leader" {
            for n := len(rf.history)-1; n > rf.commitIndex && n >= 0; n-- {
                matched := 0
                for _, matchIdx := range rf.matchIndex {
                    if matchIdx >= n && rf.history[n].Term == rf.currentTerm {
                        matched += 1
                    }
                }
                if float64(matched) > float64(len(rf.peers))/float64(2) {
                    rf.commitIndex = n
                    rf.Print("Set Commit Index", rf.commitIndex)
                    //Propagate commit
                    rf.Schedule(3)
                    break
                }
            }
        }
    }

    rf.aliveServices -= 1
}

func (rf *Raft) HandleNextAppendEntries() {

    rf.aliveServices += 1
    nextServer := 0

    for ! rf.kill {

        time.Sleep(100 * time.Millisecond)

        // Only leader must use this function
        if rf.nodeType != "Leader" {
            rf.nextIndex = make(map[int]int)
        }

        // No Entries for the moment
        if len(rf.nextIndex) == 0 {
            continue
        }

        // Fetch next pendint request
        retries, exists := rf.nextIndex[nextServer]
        lastMatched := rf.matchIndex[nextServer]

        // No Entry for this server yet
        if ! exists {
            continue
        }

        if retries == 0 {
            rf.Print("Too Many Network Issues", nil)
            delete(rf.nextIndex, nextServer)
            continue
        }

        // Apply the AppendEntries of the schedule
        entries := rf.MapCommands(1)
        currEntry := rf.history[0]

        if lastMatched >= 0 {
            entries = rf.MapCommands(lastMatched)
            currEntry = rf.history[lastMatched-1]
        }

        args := AppendEntriesArgs{rf.currentTerm, rf.me, currEntry, entries, rf.commitIndex}
        reply := AppendEntriesReply{}
        reason := rf.SendAppendEntries(nextServer, args, &reply)

        // Log inconsistency
        if reason == 1 {
            rf.matchIndex[nextServer] -= 1
        }
        // Network Issue
        if reason == 2 {
            rf.nextIndex[nextServer] = retries-1
        }
        // Success
        if reason == 0 {
            delete(rf.nextIndex, nextServer)
            if len(rf.history) > 0 {
                lastEntry := rf.history[len(rf.history)-1]

                match, ok := rf.matchIndex[nextServer]
                if !ok || match < lastEntry.Index {
                    rf.matchIndex[nextServer] = lastEntry.Index
                    rf.Print("Set Match Index", lastEntry.Index)
                }
            }
        }

        nextServer = (nextServer+1) % len(rf.peers)
    }

    rf.aliveServices -= 1
}


/*
 Network Functions
*/
func (rf *Raft) CallWrapper(server int, endpoint string, args interface{}, reply interface{}, suc chan bool) {
    if ! rf.kill {
        ok := rf.peers[server].Call(endpoint, args, reply)
        suc <- ok
    }
    suc <- false
}

func (rf *Raft) MakeCall(server int, endpoint string, args interface{}, reply interface{}, unlimited bool) bool {

    if rf.kill {
        return false
    }

    channel := make(chan bool)
    go rf.CallWrapper(server, endpoint, args, reply, channel)

    tolerance := time.Duration(150*len(rf.peers)) * rf.avgPackage
    timeout := time.Now().Add(tolerance)

    counter := 0
    for true {
        now := time.Now()
        counter += 1
        if now.After(timeout) && ! unlimited {
            if endpoint != "Raft.PingRPC" {
                rf.Print("Network Issue (" + endpoint + ") send it to (" + strconv.Itoa(server) + ")", args)
            }
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

func (rf *Raft) CalculateBestTimeout(retries int) {
    avg := time.Duration(0)
    for i := 0; i < retries; i++ {
        for peerIdx, _ := range rf.peers {
            starttime := time.Now()
            rf.TestPing(peerIdx, 0)
            endtime := time.Now()
            avg += endtime.Sub(starttime)
        }
    }
    rf.avgPackage = (avg/time.Duration(len(rf.peers)))/time.Duration(retries)
    rf.Print("AVG Time", rf.avgPackage)
}
