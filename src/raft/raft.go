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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const Follower  int = 0
const Candidate int = 1
const Leader    int = 2
const ElectionTimeGapLeft int = 300
const ElectionTimeGap     int = 300
const HeartbeatGap        int = 120
const FakeSleepSpinGap    int = 20
const CheckCommitmentGap  int = 40
const LCheckCommitmentGap int = 40

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
// log entry, contains command and term
//
type LogEntry struct {
	Command interface{}
	Term    int
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
	// Persist State
	currentTerm      int
	votedFor         int // -1 if none
	log              []LogEntry
	lastLogIndex     int
	lastLogTerm      int
	// Volatile State
	commitIndex      int
	lastApplied      int
	state            int
	receiveHeartbeat bool
	kill             bool
	// Volatile State for Leader
	nextIndex        []int
	matchIndex       []int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//DPrintf("persist, begin...", rf)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	var err error
	err = e.Encode(rf.currentTerm)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("persist, end...", rf)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var err error
	err = d.Decode(&rf.currentTerm)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	err = d.Decode(&rf.votedFor)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	err = d.Decode(&rf.log)
	if err != nil {
		PlainDPrintf("rf.persist shutdown: %v\n", err)
	}
	rf.lastLogIndex = len(rf.log) - 1
	rf.lastLogTerm = rf.log[rf.lastLogIndex].Term
}

//
// AppendEntries RPC arguments and reply structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Idx          int
	Ok           bool
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("receives AppendEntries from %d: args.Term = %d, currentTerm = %d\n",
		rf, args.LeaderId, args.Term, rf.currentTerm)
	DPrintf("receives AppendEntries from %d: args.PrevLogIndex = %d, args.PrevLogTerm = %d\n",
		rf, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
	DPrintf("receives AppendEntries from %d: args.Entries = %v\n", rf, args.LeaderId, args.Entries)
	DPrintf("receives AppendEntries from %d: args.LeaderCommit = %d, rf.lastApplied = %d, rf.commitIndex = %d\n",
		rf, args.LeaderId, args.LeaderCommit, rf.lastApplied, rf.commitIndex)
	persistFlag := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		persistFlag = true
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		if rf.state == Follower {
			rf.receiveHeartbeat = true    // Heartbeat
		} else if rf.state == Candidate {
			rf.state = Follower
		}
		if args.PrevLogIndex > rf.lastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // Conflict term
			if args.PrevLogIndex > rf.lastLogIndex {  // out of bound
				reply.ConflictTerm = -1
				reply.FirstIndex = rf.lastLogIndex + 1  // allow leader to change the nextIndex quickly into bound
			} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {  // simple conflict term
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				firstIndex := args.PrevLogIndex
				for ; firstIndex >= 0 && rf.log[firstIndex].Term == reply.ConflictTerm; firstIndex-- {}
				reply.FirstIndex = firstIndex + 1
			}
			reply.Success = false
		} else {
			lengthEntries := len(args.Entries)
			index := 0
			j := args.PrevLogIndex + 1
			for ; index < lengthEntries && j <= rf.lastLogIndex;
			      index, j = index + 1, j + 1 {
				if rf.log[j].Term != args.Entries[index].Term {
					break
				}
			}
			if index < lengthEntries {    // index == lengthEntries means no conflict, no change!
				rf.log = append(rf.log[:j], args.Entries[index:]...)
				rf.lastLogIndex = len(rf.log) - 1
				rf.lastLogTerm = rf.log[rf.lastLogIndex].Term
				persistFlag = true
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex)
			}
			reply.FirstIndex = j + lengthEntries - index
			reply.Success = true
		}
	}
	if persistFlag {
		rf.persist()
	}
	DPrintf("receives AppendEntries from %d: rf.log = %v\n", rf, args.LeaderId, rf.log)
	DPrintf("receives AppendEntries from %d: args.LeaderCommit = %d, rf.lastApplied = %d, rf.commitIndex = %d\n",
		rf, args.LeaderId, args.LeaderCommit, rf.lastApplied, rf.commitIndex)
	DPrintf("receives AppendEntries from %d: reply.Success = %t\n", rf, args.LeaderId, reply.Success)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	DPrintf("sends AppendEntries to %d: currentTerm = %d\n", rf, server, args.Term)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	}
	return ok
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// return true iff 1 is at least as up-to-date as 2
	atLeastAsUpToDateAs := func(index1 int, term1 int, index2 int, term2 int) bool {
		return term1 > term2 || (term1 == term2 && index1 >= index2)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("receives RequestVote from %d (begin): args.Term = %d, currentTerm = %d\n",
		rf, args.CandidateId, args.Term, rf.currentTerm)
	persistFlag := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		persistFlag = true
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else {  // rf.votedFor == -1 || rf.votedFor == args.CandidateId
		index1, term1 := args.LastLogIndex, args.LastLogTerm
		index2, term2 := rf.lastLogIndex, rf.lastLogTerm
		DPrintf("RequestVote compare: argId = %d, rf.me = %d, index1, term1, index2, term2 = %d, %d, %d, %d",
			rf, args.CandidateId, rf.me, index1, term1, index2, term2)
		if atLeastAsUpToDateAs(index1, term1, index2, term2) {
			reply.VoteGranted = true
			if rf.votedFor != args.CandidateId {
				persistFlag = true
			}
			rf.votedFor = args.CandidateId
			if rf.state == Follower {
				rf.receiveHeartbeat = true
			}
		} else {  // not atLeastAsUpToDateAs(index1, term1, index2, term2)
			reply.VoteGranted = false
		}
	}
	if persistFlag {
		rf.persist()
	}
	DPrintf("receives RequestVote from %d (end): args.Term = %d, currentTerm = %d, reply.VoteGranted = %t\n",
		rf, args.CandidateId, args.Term, rf.currentTerm, reply.VoteGranted)
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
	rf.mu.Lock()
	DPrintf("sends RequestVote to %d: currentTerm = %d\n", rf, server, rf.currentTerm)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	DPrintf("sends RequestVote to %d: currentTerm = %d, ok = %t\n", rf, server, rf.currentTerm,  ok)
	rf.mu.Unlock()
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
		}
		rf.mu.Unlock()
	}
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == Leader
	if !isLeader {
		return index, term, isLeader
	} else {
		rf.lastLogIndex++
		rf.lastLogTerm = rf.currentTerm
		index = rf.lastLogIndex
		term = rf.currentTerm
		newLogEntry := LogEntry{ Command: command, Term: term }
		if index < len(rf.log) {
			rf.log[index] = newLogEntry
		} else {
			rf.log = append(rf.log, newLogEntry)
		}
		rf.persist()
		DPrintf("after Start: rf.lastApplied = %d, rf.commitIndex = %d, rf.log = %v",
			rf, rf.lastApplied, rf.commitIndex, rf.log)
		return index, term, isLeader
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.kill = true
	rf.mu.Unlock()
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

	// Your initialization code here.
	data := rf.persister.ReadRaftState()
	if len(data) > 0 {
		rf.readPersist(data)
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 0)
		// to ensure rf.log starts from index 1
		rf.log = append(rf.log, LogEntry{ Command: -1, Term: 0 })
		rf.lastLogIndex = 0
		rf.lastLogTerm = 0
		rf.persist()
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.receiveHeartbeat = false
	rf.kill = false

	totPeers := len(rf.peers)
	rf.nextIndex = make([]int, totPeers)
	rf.matchIndex = make([]int, totPeers)

	//seeds := [5]int64{
	//	1639396707338395000,
	//	1639396707338904100,
	//	1639396707339264200,
	//	1639396707339585300,
	//	1639396707339843400,
	//}

	// start go routine as raft server's main procedure
	go func() {
		seed := time.Now().UTC().UnixNano() + int64(rf.me) * 100
		//seed := 20 + int64(rf.me) * 667
		//seed := seeds[rf.me]
		rand.Seed(seed)
		PlainDPrintf("server %d has seed %d", rf.me, seed)

		Apply := func(rf *Raft, index int) {
			command := rf.log[index].Command
			applyCh <- ApplyMsg{
				Index:       index,
				Command:     command,
				UseSnapshot: false,
				Snapshot:    nil,
			}
		}

		go func() {    // Check commitment periodically
			for {
				rf.mu.Lock()
				//DPrintf("Check applying, begin of one turn, rf.lastApplied = %d, rf.commitIndex = %d",
				//	rf, rf.lastApplied, rf.commitIndex)
				for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
					Apply(rf, index)
				}
				rf.lastApplied = rf.commitIndex
				//DPrintf("Check applying, begin of one turn, rf.lastApplied = %d, rf.commitIndex = %d",
				//	rf, rf.lastApplied, rf.commitIndex)
				rf.mu.Unlock()
				time.Sleep(time.Duration(CheckCommitmentGap) * time.Millisecond)
			}
		}()

		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.kill {
				rf.mu.Unlock()
				break
			}
			if rf.state == Follower {
				rf.receiveHeartbeat = false
				rf.mu.Unlock()

				electionTimeoutMs := rand.Intn(ElectionTimeGap) + ElectionTimeGapLeft
				for electionTimeoutMs > 0 {    // fake sleeping loop
					currentGap := Min(FakeSleepSpinGap, electionTimeoutMs)

					time.Sleep(time.Duration(currentGap) * time.Millisecond)
					electionTimeoutMs -= currentGap

					rf.mu.Lock()
					if rf.receiveHeartbeat {
						//DPrintf("receives heartbeat from leader, eletionTimeout old value: %d", rf, electionTimeoutMs)
						electionTimeoutMs = rand.Intn(ElectionTimeGap) + ElectionTimeGapLeft
						//DPrintf("receives heartbeat from leader, eletionTimeout new value: %d", rf, electionTimeoutMs)
						rf.receiveHeartbeat = false
					}
					rf.mu.Unlock()
				}
				rf.mu.Lock()
				DPrintf("I am going to be a candidate!", rf)
				rf.state = Candidate
				rf.mu.Unlock()
			} else if rf.state == Candidate {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()

				term := rf.currentTerm

				args := make([]RequestVoteArgs, totPeers)
				replies := make([]RequestVoteReply, totPeers)
				oks := make([]bool, totPeers)
				messages := make(chan int)
				for idx := 0; idx < totPeers; idx++ {
					if idx != rf.me {
						args[idx].Term = rf.currentTerm
						args[idx].CandidateId = rf.me
						args[idx].LastLogIndex = rf.lastLogIndex
						args[idx].LastLogTerm = rf.lastLogTerm
						go func(idx int) {
							oks[idx] = rf.sendRequestVote(idx, args[idx], &replies[idx])
							messages <- idx
						}(idx)
					}
				}
				rf.mu.Unlock()

				go func(term int) { // count the votes
					count := 1
					for i := 0; i < totPeers - 1; i++ {
						idx := <-messages
						rf.mu.Lock()
						DPrintf("receives requestVote reply from %d, reply.VoteGranted = %t",
							rf, idx, replies[idx].VoteGranted)
						DPrintf("%d votes return", rf, i + 1)
						if rf.currentTerm > term {
							rf.mu.Unlock()
							break
						}
						if oks[idx] && replies[idx].VoteGranted {
							DPrintf("%d votes for %d\n", rf, idx, rf.me)
							count++
						}
						if 2 * count > totPeers {
							rf.state = Leader
							DPrintf("I am the leader!!\n", rf)
							rf.mu.Unlock()
							break
						}
						rf.mu.Unlock()
					}
				}(term)

				electionTimeoutMs := rand.Intn(ElectionTimeGap) + ElectionTimeGapLeft
				for electionTimeoutMs > 0 {  // fake sleeping loop
					currentGap := Min(FakeSleepSpinGap, electionTimeoutMs)
					time.Sleep(time.Duration(currentGap) * time.Millisecond)
					electionTimeoutMs -= currentGap

					rf.mu.Lock()
					if rf.state == Follower {  // follower: stop the sleeping and be ready to vote
						DPrintf("detect state change in candidate fake sleeping loop: become follower", rf)
						rf.mu.Unlock()
						break
					} else if rf.state == Leader {  // leader: send heartbeat in no time!
						DPrintf("detect state change in candidate fake sleeping loop: become leader", rf)
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
				}
			} else if rf.state == Leader {
				for idx := 0; idx < totPeers; idx++ {
					if idx != rf.me {
						rf.nextIndex[idx] = rf.lastLogIndex + 1
						rf.matchIndex[idx] = 0
					}
				}

				term := rf.currentTerm

				rf.mu.Unlock()

				go func(term int) {    // leader's check commitIndex periodically
					rf.mu.Lock()
					DPrintf("leader's check commitIndex periodically", rf)
					rf.mu.Unlock()

					findProperN := func(rf *Raft) int {
						totPeers := len(rf.peers)
						for N := rf.lastLogIndex; N > rf.commitIndex; N-- {
							count := 1
							for idx := 0; idx < totPeers; idx++ {
								if idx != rf.me && rf.matchIndex[idx] >= N {
									count++
								}
							}
							if 2 * count > totPeers && rf.log[N].Term == rf.currentTerm { // TestFigure8
								return N
							}
						}
						return -1
					}

					for {
						rf.mu.Lock()
						//DPrintf("leader's check commitIndex periodically, begin of one turn, rf.commitIndex = %d",
						//	rf, rf.commitIndex)
						if rf.currentTerm > term {
							rf.mu.Unlock()
							break
						}
						N := findProperN(rf)
						if N != -1 {
							rf.commitIndex = N
						}
						//DPrintf("leader's check commitIndex periodically, end of one turn, rf.commitIndex = %d",
						//	rf, rf.commitIndex)
						rf.mu.Unlock()

						time.Sleep(time.Duration(LCheckCommitmentGap) * time.Millisecond)
					}
				}(term)

				messages := make(chan AppendEntriesReply)

				go func(term int) {
					rf.mu.Lock()
					DPrintf("leader's check AppendEntriesReply periodically", rf)
					rf.mu.Unlock()

					for {
						reply := <-messages
						rf.mu.Lock()
						DPrintf("AppendEntries reply return, from %d\n", rf, reply.Idx)
						if rf.currentTerm > term {
							rf.mu.Unlock()
							break
						}
						if reply.Ok {
							idx := reply.Idx
							if reply.Success {
								DPrintf("%d verifies the new log: OK\n", rf, idx)
								DPrintf("%d verifies the new log, OK: old rf.nextIndex[idx] = %d, rf.matchIndex[idx] = %d",
									rf, idx, rf.nextIndex[idx], rf.matchIndex[idx])
								rf.nextIndex[idx] = reply.FirstIndex
								rf.matchIndex[idx] = rf.nextIndex[idx] - 1
								DPrintf("%d verifies the new log, OK: new rf.nextIndex[idx] = %d, rf.matchIndex[idx] = %d",
									rf, idx, rf.nextIndex[idx], rf.matchIndex[idx])
							} else {
								DPrintf("%d verifies the new log: NOT OK\n", rf, idx)
								DPrintf("%d verifies the new log, NOT OK: ConcflictTerm = %d, FirstIndex = %d",
									rf, idx, reply.ConflictTerm, reply.FirstIndex)
								DPrintf("%d verifies the new log, NOT OK: old, rf.nextIndex[idx] = %d, rf.matchIndex[idx] = %d",
									rf, idx, rf.nextIndex[idx], rf.matchIndex[idx])
								if reply.ConflictTerm == -1 && reply.FirstIndex != -1 {
									// rf.nextIndex[idx] is out of bound
									rf.nextIndex[idx] = reply.FirstIndex
								} else if reply.ConflictTerm != -1 {
									// not out of bound, but conflict in term
									nextIndex := rf.nextIndex[idx] - 1
									for ; nextIndex >= reply.FirstIndex &&
										rf.log[nextIndex].Term != reply.ConflictTerm; nextIndex-- {}
									rf.nextIndex[idx] = nextIndex + 1
								}
								DPrintf("%d verifies the new log, not OK: new, rf.nextIndex[idx] = %d, rf.matchIndex[idx] = %d",
									rf, idx, rf.nextIndex[idx], rf.matchIndex[idx])
							}
						}
						rf.mu.Unlock()
					}
				}(term)

				for {
					rf.mu.Lock()
					if rf.currentTerm > term || rf.kill {
						rf.mu.Unlock()
						break
					}
					args := make([]AppendEntriesArgs, totPeers)
					replies := make([]AppendEntriesReply, totPeers)
					for idx := 0; idx < totPeers; idx++ {
						if idx != rf.me {
							args[idx].Term = rf.currentTerm
							args[idx].LeaderId = rf.me
							args[idx].PrevLogIndex = rf.nextIndex[idx] - 1
							args[idx].PrevLogTerm = rf.log[rf.nextIndex[idx] - 1].Term
							args[idx].Entries = make([]LogEntry, rf.lastLogIndex + 1 - rf.nextIndex[idx])
							copy(args[idx].Entries, rf.log[rf.nextIndex[idx]:])
							args[idx].LeaderCommit = rf.commitIndex
							go func(idx int) {
								replies[idx].Idx = idx
								replies[idx].Ok = rf.sendAppendEntries(idx, args[idx], &replies[idx])
								messages <- replies[idx]
							}(idx)
						}
					}
					rf.mu.Unlock()

					heartbeatTimeoutMs := HeartbeatGap
					for heartbeatTimeoutMs > 0 { // fake sleeping loop
						currentGap := Min(FakeSleepSpinGap, heartbeatTimeoutMs)
						time.Sleep(time.Duration(currentGap) * time.Millisecond)
						heartbeatTimeoutMs -= currentGap

						rf.mu.Lock()
						if rf.currentTerm > term {
							rf.mu.Unlock()
							break
						}
						rf.mu.Unlock()
					}
				}
			}
		}
	}()

	return rf
}
