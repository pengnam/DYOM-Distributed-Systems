package raft

//
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
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"reflect"

	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"


// NOTE: Should have used the defer statement



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
	UseSnapshot bool
	Snapshot []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	state       NodeState
	currentTerm int
	votedFor    int
	NONE int
	log	[]LogEntry
	Die chan int

	commitIndex int
	lastApplied int

	appendEntriesSignal chan int
	applyCh             chan ApplyMsg

	*DemotionHelper
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	// TODO: Don't think this is the right equality
	_, isLeader := rf.state.(*Leader)
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
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil{
		log.Fatal("There is a problem")

	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = logEntries
	  fmt.Printf("Something is working %+v\n", rf)
	}
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// PERSIST
	defer rf.persist()
	fmt.Println("--------------------------------------")
	fmt.Println("Request vote RPC from : ", args.CandidateId ," to ", rf.me)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		fmt.Println(args.CandidateId, rf.me, "Process: ", args.CandidateId, " caused ", rf.me, " to be demoted")
		rf.Demotion()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		fmt.Println(args.CandidateId, rf.me, " Candidate has a poor lower term: ", args.Term, " vs ", rf.currentTerm)
		return
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if (rf.votedFor == rf.NONE || rf.votedFor == args.CandidateId) && upToDate(args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm){
		fmt.Println(rf.me, " is voting for ", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		fmt.Println(rf.me, " voted for ", args.CandidateId)

		if state, ok := rf.state.(*Follower); ok {
			state.gotValidMessage = true
		}
	} else {
		reply.VoteGranted = false
	}
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	fmt.Println("Not supposed to happen")
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.Demotion()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		// send snapshot to kv server
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.applyCh<- msg
	}
}
//TODO
func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	fmt.Println("Yikes")
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

/// True if a is more up to date (at least)
func upToDate(lastLogIndexA int, lastLogTermA int, lastLogIndexB int, lastLogTermB int) bool{
	fmt.Println("Comparing: ", lastLogIndexA, lastLogTermA, lastLogIndexB, lastLogTermB)
	if lastLogTermA != lastLogTermB {

		return lastLogTermA > lastLogTermB
	}
	return lastLogIndexA >= lastLogIndexB
}
//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int
	Success bool
}

type LogEntry struct {
	Index int
	Item interface{}
	Term int
}
//
// AppendEntries RPC Handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// PERSIST
	rf.persist()
	fmt.Println("----------------------------------")
	fmt.Println("Append entries from ", args.LeaderId, " to ", rf.me)
	fmt.Printf("%d %d %+v\n", args.LeaderId, rf.me, args)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		fmt.Println("Process: ", args.LeaderId, " caused ", rf.me, " to be demoted")
		rf.Demotion()
	} else {
	}
	// 1.
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		fmt.Println(args.LeaderId, " ", rf.me, "leader has a lower term than me")
		reply.Success = false
		return
	}
	if ele, ok := rf.getElementAtPosition(args.PrevLogIndex); ok {
		// 2. term does not match
		if ele.Term != args.PrevLogTerm {
			fmt.Println(args.LeaderId, " ", rf.me, "leader and I don't have matching log terms")
			reply.Success = false
			return
		}
	} else if (args.PrevLogIndex != -1){
		// No element at position
		fmt.Println(args.LeaderId, " ", rf.me, "No element at position")
		reply.Success = false
		return
	}

	// INVARIANT: args.PrevLogTerm should match
	if ele, ok := rf.getElementAtPosition(args.PrevLogIndex); ok {
		if ele.Term != args.PrevLogTerm {
			panic("THIS SHOULD NOT WORK")
		}
	}

	fmt.Println(args.LeaderId, rf.me, "Previous log for :", rf.me)
	fmt.Println(args.LeaderId, rf.me, rf.log)
	// 3 + 4.
	fmt.Println("There are ", len(args.Entries), "entries ")
	for i := 0; i < len(args.Entries); i++ {
		logNumber := args.PrevLogIndex + 1 + i
		fmt.Println("Log number", logNumber)
		if logNumber < rf.getLastLogIndex() + 1 {
			if rf.log[logNumber - rf.getBaseIndex()].Term != args.Entries[i].Term {
				rf.log = rf.log[:logNumber - rf.getBaseIndex()]
				rf.log = append(rf.log, args.Entries[i])
			}
		} else {
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	fmt.Println(args.LeaderId, rf.me, "Outcome of log for ", rf.me)
	fmt.Println(args.LeaderId, rf.me, rf.log)

	// 5.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex() )
	}
	reply.Success = true

	fmt.Printf("%d %d %+v\n", args.LeaderId, rf.me, reply)
	rf.appendEntriesSignal <- 1
	fmt.Printf("%d %d after append entries\n", args.LeaderId, rf.me)
}

func (rf *Raft) getElementAtPosition(index int) (LogEntry, bool){
	if index >  rf.getLastLogIndex() || index < 0{
		return LogEntry{}, false
	}
	return rf.log[index - rf.getBaseIndex()], true
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
	if _, ok := rf.state.(*Leader); !ok {
		// TODO: Return -1?
		return rf.getLastLogIndex() + 1, rf.currentTerm, false
	}
	fmt.Println("CALLED START FOR ", rf.me, "WITH", command)
	fmt.Println(rf.me, rf.log)
	newIndex := rf.getLastLogIndex() + 1

	rf.log = append(rf.log, LogEntry{
		Item: command,
		Term: rf.currentTerm,
		Index: newIndex,
	})
	fmt.Println("HERE ", newIndex, rf.log)
	return newIndex + 1 , rf.currentTerm, true

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
	rf.Die <- 1
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
			go rf.sendVoteHelper(i, fanIn)
		}
	}
	return fanIn
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) sendVoteHelper(i int, fanIn chan RequestVoteReply) {
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIndex,
		lastLogTerm,
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
	dh := newDemotionHelper(rf.me)
	rf.appendEntriesSignal = make(chan int)
	rf.DemotionHelper = &dh
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)
	rf.Die = make(chan int)
	rf.commitIndex = -1
	rf.lastApplied = -1

	// Your initialization code here (2A, 2B, 2C).
	// FSM YO!!
	go func() {
		rf.state = newFollower()
		for {
			fmt.Println("================NEW STATE=====================")
			fmt.Println("Id:",rf.me)
			fmt.Println("Term: ", rf.currentTerm)
			fmt.Println(reflect.TypeOf(rf.state))
			fmt.Println(rf.log)
			fmt.Println("Voting for: ", rf.votedFor)
			fmt.Println("==============================================")
			rf.state = rf.state.ProcessState(rf)
			rf.ResetDemotionState(rf)
			if rf.state == nil {
				return
			}
		}
	}()

	// initialize from state persisted before a crash
	fmt.Println("READING FROM PREVIOUS STATE")
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnapshot(persister.ReadSnapshot())
	rf.persist()
	return rf
}

type NodeState interface {
	ProcessState(raft *Raft) NodeState
}
type DemotionHelper struct {
	 demoteChannel chan int
	 isInProcessOfDemoting bool
	 doneDemotion chan int
	 id int
}

func newDemotionHelper(id int) DemotionHelper{
	return DemotionHelper{
		demoteChannel:         make(chan int),
		isInProcessOfDemoting: false,
		doneDemotion:make(chan int),
		id: id,
	}
}

func (dh *DemotionHelper) Demotion(){
	// Sends to channel for Node to process
	fmt.Printf("%d [demotion] Sending to demotion channel\n", dh.id)
	dh.demoteChannel <- 1
	fmt.Printf("%d [demotion] Sending to done demotion\n", dh.id)
	// Waits until demotion is complete before continuing
	dh.isInProcessOfDemoting = true
	<- dh.doneDemotion
	fmt.Printf("%d [demotion] After done demotion\n", dh.id)
}

func (dh *DemotionHelper) ResetDemotionState(raft *Raft) {
	if dh.isInProcessOfDemoting {
		dh.isInProcessOfDemoting = false
		dh.doneDemotion <- 1
		raft.votedFor = raft.NONE
		fmt.Printf("%d [demotion] resetted \n", dh.id)
	}
}

/// STATES

type Candidate struct {
}
func newCandidate() Candidate {
	return Candidate{}
}

type Leader struct {
	nextIndex []int
	matchIndex []int
}

func newLeader(numServers int, logLength int) *Leader {
	nextIndexes := 	make([]int, numServers)
	for i := 0; i < numServers; i++ {
		nextIndexes[i] = logLength
	}
	matchIndexes := make([]int, numServers)
	for i := 0; i < numServers; i++ {
		matchIndexes[i] = -1
	}

	return &Leader{
		nextIndex: nextIndexes,
		matchIndex: matchIndexes,
	}
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

func getRandomFollowerTimeout() time.Duration {
	dur := time.Duration(rand.Intn(100) + 50)
	return dur * time.Millisecond
}
func getRandomElectionTimeout() time.Duration {
	dur := time.Duration(rand.Intn(100) + 0)
	return dur * time.Millisecond
}

func (follower *Follower) ProcessState(raft *Raft) NodeState {
	timeout := time.After(getRandomFollowerTimeout())
	for {
		if raft.lastApplied < raft.commitIndex {
			applyLogs(raft)
		}
		select {
			case <- raft.appendEntriesSignal:
				follower.gotValidMessage = true
			case <- raft.demoteChannel:
				fmt.Println(raft.me, " is demoted because it received an rpc request of a higher term")
				return newFollower()
			case <- timeout:
				if follower.gotValidMessage {
					fmt.Println(raft.me, " is demoted because it received a hearbeat")
					// Note: There is no reset of voting here because we want the follower to still be voting for lead
					return newFollower()
				} else {
					fmt.Println(raft.me, "did not get the right message")
					return newCandidate()
				}
			case <- raft.Die:
				fmt.Println("Dying", raft.me)
				return nil
		}
	}
	return nil
}


func (candidate Candidate) ProcessState(raft *Raft) NodeState {
	raft.currentTerm += 1
	raft.votedFor = raft.me
	timeout := time.After(getRandomFollowerTimeout())
	count := 1
	receives := raft.gatherVotes()

	for {
		if raft.lastApplied < raft.commitIndex {
			applyLogs(raft)
		}
		select {
			case vote := <-receives:
				if vote.Term > raft.currentTerm {
					fmt.Printf("%d [candidate] is demoted from candidate cause it received vote of higher term\n", raft.me)
					raft.votedFor = raft.NONE
					return newFollower()
				}
				if vote.VoteGranted {
					count += 1
					fmt.Println(raft.me, " has ", count, " votes")
					if float64(count) >= float64(len(raft.peers))/2 {
						fmt.Printf("%d [candidate] is becoming leader\n", raft.me)
						return newLeader(len(raft.peers), len(raft.log))
					}
				}
			case <-timeout:
				fmt.Println(raft.me, "[candidate] is reelecting. Term: ", raft.currentTerm)
				// Restarts election
				return newCandidate()
			case <- raft.demoteChannel:
				fmt.Printf("%d [candidate] is demoted from candidate\n", raft.me)
				raft.votedFor = raft.NONE
				return newFollower()
			case <- raft.appendEntriesSignal:
				fmt.Printf("%d [candidate] is demoted from candidate cause of append entries\n", raft.me)
				raft.votedFor = raft.NONE
				return newFollower()
			case <- raft.Die:
				fmt.Println("Dying", raft.me)
				return nil
		}
	}
	return nil
}

func (leader Leader) ProcessState(raft *Raft) NodeState{
	// TODO: This timeout should be shorter
	raft.updateLogEntries(&leader)
	timeout := time.After(40*time.Millisecond)
	for {
		if raft.lastApplied < raft.commitIndex {
			applyLogs(raft)
		}
		select {
			case <- raft.demoteChannel:
				fmt.Println("Leader ", raft.me, " demoted")
				return newFollower()
			case <- timeout:
				raft.updateLogEntries(&leader)
				timeout = time.After(40*time.Millisecond)
			case <- raft.Die:
				fmt.Println("Dying", raft.me)
				return nil
		}
	}
}
// CURRENT: Heartbeat with append entries
func (rf *Raft) updateLogEntries(leader *Leader) {
	rf.persist()
	for i := 0; i <len(rf.peers); i += 1 {
		if i != rf.me {
			// 3. TODO: Missing leader number 3
			go sendAppendEntriesHelper(i, rf, leader)
		}
	}
}

func (rf *Raft) getBaseIndex() int{
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[0].Index
}

func sendAppendEntriesHelper(i int, rf *Raft, leader *Leader) {
	for {
		lastIndex := leader.nextIndex[i]
		// TODO: When starting from log 0, what is this?
		// TODO: When there is nothing to update... what happens?
		prevLogIndex := lastIndex - 1
		prevLogTerm := -1
		//bi := rf.getBaseIndex()
		//li := rf.getLastLogIndex()
		if rf.getBaseIndex() <= prevLogIndex && prevLogIndex <= rf.getLastLogIndex() {
			prevLogTerm = rf.log[prevLogIndex - rf.getBaseIndex()].Term
		}
		entries := []LogEntry{}
		if rf.getBaseIndex() <= lastIndex && lastIndex <= rf.getLastLogIndex() {
			entries = rf.log[lastIndex-rf.getBaseIndex():]
		}
		//fmt.Println("Log index and term")
		//fmt.Println(prevLogIndex)
		//fmt.Println(prevLogTerm)
		//fmt.Println(bi)
		//fmt.Println(li)
		//fmt.Println(rf.log)
		//fmt.Println(entries)
		//fmt.Println(leader.nextIndex)
		//fmt.Println(leader.matchIndex)
		logLength := rf.getLastLogIndex() + 1
		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			prevLogTerm,
			entries,
			rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(i, &args, &reply)
		fmt.Printf("%d %d reply: %+v\n", rf.me, i, reply)

		if !ok {
			fmt.Println(rf.me, i, "FAILED RPC")
		}


		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			fmt.Println("Leader ", rf.me, " was demoted because of ", i)
			rf.Demotion()
			return
		}
		// 3a.
		//fmt.Println(rf.me, " sent ", i, " status:")
		//fmt.Printf("%+v\n", args)
		//fmt.Printf("%+v\n", reply)
		//fmt.Println(reply.Success)
		if reply.Success {
			leader.nextIndex[i] = logLength
			leader.matchIndex[i] = logLength - 1
			incrementCommitCheck(leader, rf)
			fmt.Println(rf.me, i, "commit index: ", rf.commitIndex)
			fmt.Println(rf.me, i, "NEXT", leader.nextIndex)
			fmt.Println(rf.me, i, "MATCH", leader.matchIndex)
			return
		} else {
			if leader.nextIndex[i] > 0 {
				leader.nextIndex[i] -= 1
			} else {
				return
			}
		}
	}
}
func incrementCommitCheck(leader *Leader, raft *Raft) {
	for i := 0; i < len(leader.matchIndex); i++ {
		val := leader.matchIndex[i]
		found := false
		if entry, ok := raft.getElementAtPosition(val); ok {
			if entry.Term == raft.currentTerm {
				found = true
			}
		}
		if !found {
			continue
		}

		currentCount := 1
		for j := 0; j < len(leader.nextIndex); j++ {
			if leader.matchIndex[j] >= val {
				currentCount += 1
			}
		}
		if float64(currentCount) >= float64(len(leader.nextIndex))/2 {
			if val > raft.commitIndex {
				fmt.Println(raft.me, "incremented commit index")
				raft.commitIndex = val
			}
		}
	}
}

func applyLogs(raft *Raft) {
	for raft.lastApplied < raft.commitIndex {
		raft.lastApplied += 1
		fmt.Println("Applying ",raft.me, raft.lastApplied, raft.log)
		fmt.Println(raft.log[raft.lastApplied].Item)
		raft.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      raft.log[raft.lastApplied].Item,
			CommandIndex: raft.lastApplied + 1,
		}
		fmt.Println("after")
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	//rf.trimLog(lastIncludedIndex, lastIncludedTerm)
}
