package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	// "fmt"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	Leader = iota
	Candidate
	Follower
)

const (
	heartBearInterval  = 100 * time.Millisecond
	minElectionTimeout = 300 * time.Millisecond
	maxElectionTimeout = 500 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	voteFor         int
	state           int
	lastHeartBeat   time.Time
	electionTimeout time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan raftapi.ApplyMsg
	//only for leader
	nextIndex  []int
	matchIndex []int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// holding lock to use this function
func (rf *Raft) resetElectionTimeout() {
	rf.lastHeartBeat = time.Now()
	rf.electionTimeout = minElectionTimeout + time.Duration(rand.Int63()%int64(maxElectionTimeout-minElectionTimeout))
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.LastLogTerm < rf.log[rf.lastLogIndex()].Term || (args.LastLogTerm == rf.log[rf.lastLogIndex()].Term && args.LastLogIndex < rf.lastLogIndex()) {
		reply.VoteGranted = false
		rf.currentTerm = max(rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm

		return
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if rf.voteFor == -1 {
			rf.resetElectionTimeout()
			rf.voteFor = args.CandidateId
			rf.state = Follower
			reply.VoteGranted = true
			return
		}
		reply.VoteGranted = false
		return
	}
	rf.currentTerm = args.Term
	rf.voteFor = args.CandidateId
	rf.state = Follower
	reply.VoteGranted = true
	rf.resetElectionTimeout()
}
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// fmt.Printf("server %v term %v\n", rf.me, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	rf.resetElectionTimeout()
	rf.state = Follower
	rf.voteFor = -1
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XIndex = rf.lastLogIndex()
		reply.XTerm = -1
		// log.Println("different")
		return
	}
	// log.Printf("prevLogIndex %v args %v\n", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		for index := args.PrevLogIndex; index > 0; index -= 1 {
			if rf.log[index-1].Term == reply.Term {
				continue
			}
			reply.XIndex = index
			break
		}
		// log.Println("different")
		return
	}
	reply.Success = true
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	} else {
		rf.commitIndex = args.LeaderCommit
	}
}

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}
func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	for index := rf.lastLogIndex(); index > rf.commitIndex; index -= 1 {
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= index {
				count += 1
			}
		}
		// log.Printf("undateCommit: %v\n", count)
		if count > len(rf.peers)/2 {
			rf.commitIndex = index
			// log.Println("signal")
			rf.applyCond.Signal()
			break
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) replicateTo(server int) {
	rf.mu.Lock()
	startTerm := rf.currentTerm
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		// log.Printf("len of nextIndex : %v\n", len(rf.nextIndex))
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		// log.Printf("args.PrevLogTerm %v\n", args.PrevLogTerm)
		args.Entries = rf.log[rf.nextIndex[server]:]
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		var reply AppendEntriesReply
		ok := rf.sendRequestAppendEntries(server, &args, &reply)
		rf.mu.Lock()
		if rf.currentTerm != startTerm {
			rf.mu.Unlock()
			break
		}

		// fmt.Printf("replyTerm %v rfCurrentTerm %v\n", reply.Term, rf.currentTerm)
		if reply.Term > rf.currentTerm {
			// fmt.Println("break")
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
			rf.mu.Unlock()
			break
		}
		if !ok {
			rf.mu.Unlock()
			break
		}
		if reply.Success == false {
			if reply.XTerm == rf.currentTerm {
				rf.nextIndex[server] -= 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
			// fmt.Printf("leader : %v server %v\n", rf.me, server)
			// fmt.Println("nextIndex--")
			// if rf.nextIndex[server] <= 1 {
			// 	log.Printf("nextIndex %v\n", rf.nextIndex[server])
			// }
			rf.mu.Unlock()
			continue
		} else {
			rf.nextIndex[server] = rf.lastLogIndex() + 1
			rf.matchIndex[server] = rf.lastLogIndex()
			rf.mu.Unlock()
			rf.updateCommit()
			break
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		rf.applyCond.Wait()
		// log.Printf("doing applier\n")
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i += 1 {
			msg := raftapi.ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.log[i].Command}
			rf.applyCh <- msg
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	index = rf.lastLogIndex()
	term = rf.currentTerm
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateTo(i)
	}
	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	rf.resetElectionTimeout()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateTo(i)
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	voteCnt := 1
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.currentTerm += 1
	startTerm := rf.currentTerm
	rf.resetElectionTimeout()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.log[rf.lastLogIndex()].Term}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			if rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted == true && startTerm == rf.currentTerm {
				voteCnt += 1
			}
			if voteCnt > len(rf.peers)/2 && rf.state == Candidate {
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for index := range rf.peers {
					rf.nextIndex[index] = rf.lastLogIndex() + 1
					rf.matchIndex[index] = 0
				}
				rf.matchIndex[rf.me] = rf.lastLogIndex()
				rf.mu.Unlock()
				rf.sendHeartBeat()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.sendHeartBeat()
			time.Sleep(heartBearInterval)
			continue
		}
		interval := time.Since(rf.lastHeartBeat)
		if interval > rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	//3A
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.log = make([]LogEntry, 1)
	// log.Printf("inital len : %v\n", len(rf.log))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
