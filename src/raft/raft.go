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

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"




//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       NodeState
	currentTerm int
	votedFor    int
	NONE int

	appendEntriesSignal chan int

	*DemotionHelper
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	// TODO: Don't think this is the right equality
	_, isLeader := rf.state.(Leader)
	if isLeader {
		fmt.Println("--------")
		fmt.Println(rf.me, " is leader")
	}
	return rf.currentTerm, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm int
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.Demotion()
	} else {
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == rf.NONE || rf.votedFor == args.CandidateId{
		fmt.Println(rf.me, " is voting for ", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		// TODO: Verify this
		if state, ok := rf.state.(*Follower); ok {
			state.gotValidMessage = true
		}
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int
	Success bool
}
//
// AppendEntries RPC Handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.Demotion()
	} else {
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.appendEntriesSignal <- 1
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) gatherVotes() <- chan RequestVoteReply{
	fanIn := make(chan RequestVoteReply)
	for i := 0; i <len(rf.peers); i += 1 {
		if i != rf.me {
			go sendVoteHelper(i, rf, fanIn)
		}
	}
	return fanIn
}

func sendVoteHelper(i int, rf *Raft, fanIn chan RequestVoteReply) {
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
	}
	reply := RequestVoteReply{}
	rf.sendRequestVote(i, &args, &reply)
	fanIn <- reply
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
	rf.NONE = len(peers)
	rf.votedFor = rf.NONE
	dh := newDemotionHelper()
	rf.appendEntriesSignal = make(chan int)
	rf.DemotionHelper = &dh

	// Your initialization code here (2A, 2B, 2C).
	// FSM YO!!
	go func() {
		rf.state = newFollower()
		for {
			fmt.Println("================NEW STATE=====================")
			fmt.Println(rf.me)
			fmt.Println(reflect.TypeOf(rf.state))
			fmt.Println("==============================================")
			rf.state = rf.state.ProcessState(rf)
			rf.ResetDemotionState(rf)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

type NodeState interface {
	ProcessState(raft *Raft) NodeState
}
type DemotionHelper struct {
	 demoteChannel chan int
	 isInProcessOfDemoting bool
	 doneDemotion chan int
}

func newDemotionHelper() DemotionHelper{
	return DemotionHelper{
		demoteChannel:         make(chan int),
		isInProcessOfDemoting: false,
		doneDemotion:make(chan int),
	}
}

func (dh *DemotionHelper) Demotion(){
	// Sends to channel for Node to process
	dh.demoteChannel <- 1
	// Waits until demotion is complete before continuing
	dh.isInProcessOfDemoting = true
	<- dh.doneDemotion
}

func (dh *DemotionHelper) ResetDemotionState(raft *Raft) {
	if dh.isInProcessOfDemoting {
		dh.isInProcessOfDemoting = false
		dh.doneDemotion <- 1
		raft.votedFor = raft.NONE
	}
}

/// STATES

type Candidate struct {
}
func newCandidate() Candidate {
	return Candidate{}
}

type Leader struct {
}

type Follower struct {
	// Received either appendMessage or grantedVote
	gotValidMessage bool
}

func newFollower() *Follower {
	return &Follower{
		gotValidMessage: false,
	}
}
func getRandomElectionTimeout() time.Duration {
	dur := time.Duration(rand.Intn(150) + 300)
	return dur * time.Millisecond
}

func (follower *Follower) ProcessState(raft *Raft) NodeState {
	timeout := time.After(getRandomElectionTimeout())
	for {
		select {
			case <- raft.appendEntriesSignal:
				follower.gotValidMessage = true
			case <- raft.demoteChannel:
				fmt.Println(raft.me, " is demoted because it received an rpc request of a higher term")
				return newFollower()
			case <- timeout:
				if follower.gotValidMessage {
					fmt.Println(raft.me, " is demoted because it received a hearbeat")
					return newFollower()
				} else {
					fmt.Println(raft.me, "did not get the right message")
					return newCandidate()
				}
		}
	}
	return nil
}


func (candidate Candidate) ProcessState(raft *Raft) NodeState {
	raft.mu.Lock()
	raft.currentTerm += 1
	raft.votedFor = raft.me
	raft.mu.Unlock()
	timeout := time.After(getRandomElectionTimeout())
	count := 1
	receives := raft.gatherVotes()

	for {
		select {
			case vote := <-receives:
				raft.mu.Lock()
				currentTerm := raft.currentTerm
				raft.mu.Unlock()
				if vote.Term > currentTerm {
					return newFollower()
				}
				if vote.VoteGranted {
					count += 1
					if count >= len(raft.peers)/2 {
						fmt.Println(raft.me, " has ", count, " votes")
						return Leader{}
					}
				}
			case <-timeout:

				fmt.Println(raft.me, " is Tan Cheng Bok and is reelecting. Term: ", raft.currentTerm)
				// Restarts election
				return newCandidate()
			case <- raft.demoteChannel:
				return newFollower()
			case <- raft.appendEntriesSignal:
				return newFollower()
		}
	}
	return nil
}

func (leader Leader) ProcessState(raft *Raft) NodeState{
	timeout := time.After(getRandomElectionTimeout())
	for {
		select {
			case <- raft.demoteChannel:
				fmt.Println("Leader ", raft.me, " demoted")
				return newFollower()
			case <- timeout:
				raft.sendHeartBeat()
				timeout = time.After(getRandomElectionTimeout())
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for i := 0; i <len(rf.peers); i += 1 {
		if i != rf.me {
			go sendAppendEntriesHelper(i, rf)
		}
	}
}

func sendAppendEntriesHelper(i int, rf *Raft) {
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(i, &args, &reply)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.Demotion()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
