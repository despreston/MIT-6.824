package raft

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "../labgob"

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

const (
	ElectionInterval  int = 300
	HeartBeatInterval     = 100
)

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
	Dead            = "dead"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       State
	lastReceive time.Time

	// persistent
	currentTerm int // latest term server has seen
	votedFor    int // candidate ID that received vote in current term
	log         []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term  int // the term when the entry was added to the log
	Index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term         int // candidates's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntryArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceeding new ones
	PrevLogTerm  int        // term of previous log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commit index
}

type AppendEntryReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained every matching prevLogIndex and prevLogTerm
}

func (rf *Raft) getLastLogEntry() LogEntry {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return LogEntry{
			Term:  -1,
			Index: -1,
		}
	}
}

func (rf *Raft) convert(to func()) {
	rf.lastReceive = time.Now()
	to()
}

func (rf *Raft) toCandidate() {
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
}

func (rf *Raft) toFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) toLeader() {
	rf.state = Leader
}

func (rf *Raft) sendAppendEntry(pid int) {
	rf.mu.Lock()
	lastLogEntry := rf.getLastLogEntry()

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastLogEntry.Index,
		PrevLogTerm:  lastLogEntry.Term,
		Entries:      nil,
		LeaderCommit: -1,
	}

	reply := AppendEntryReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntries(pid, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.convert(func() { rf.toFollower(reply.Term) })
		}
	}
}

/**
 * As long as this server is Leader, ping the others with heartbeat messages.
 */
func (rf *Raft) performLeaderState() {
	for {
		rf.mu.Lock()
		notLeader := rf.state != Leader
		rf.mu.Unlock()
		if notLeader {
			return
		}

		for pid := 0; pid < len(rf.peers); pid++ {
			if pid != rf.me {
				DPrintf("%d sending heartbeat to %d", rf.me, pid)
				go rf.sendAppendEntry(pid)
				time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("%d starting election", rf.me)
	rf.mu.Lock()

	rf.convert(rf.toCandidate)
	lastLogTerm := rf.getLastLogEntry().Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, lastLogTerm}

	rf.mu.Unlock()

	votes := 1

	// Request vote from each peer.
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(p int) {
			reply := RequestVoteReply{}

			if !rf.sendRequestVote(p, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.convert(func() { rf.toFollower(reply.Term) })
			}

			if reply.VoteGranted {
				DPrintf("%d received vote from %d", rf.me, p)
				votes++

				// Majority check
				if votes > len(rf.peers)/2 && rf.state == Candidate {
					DPrintf("%d won the election", rf.me)
					rf.convert(rf.toLeader)
					go rf.performLeaderState()
				}
			}
		}(i)
	}
}

/**
 * Tracks the time between messages received from the Leader. If the Leader
 * doesn't send a message soon enough it'll attempt a new election.
 */
func (rf *Raft) MonitorLeader() {
	for {
		// Randomize to ensure split votes are rare and resolved quickly.
		electionTimeout := ElectionInterval + rand.Intn(200)

		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.mu.Lock()

		if rf.state == Dead {
			rf.mu.Unlock()
			return
		}

		// Haven't heard any heartbeats, send a request to become new leader.
		if rf.lastReceive.Before(startTime) && rf.state != Leader {
			go rf.startElection()
		}

		rf.mu.Unlock()
	}
}

/**
 * RequestVote RPC Handler
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d received RequestVote from %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// The leader is ahead of this server, so become a follower.
	if args.Term >= rf.currentTerm {
		rf.convert(func() { rf.toFollower(args.Term) })
	}

	hasVoted := rf.votedFor != -1
	latestEntry := rf.getLastLogEntry()

	// Already voted
	if rf.votedFor == args.CandidateId || hasVoted {
		return
	}

	/*
		Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date.
	*/
	if args.LastLogTerm > latestEntry.Term {
		reply.VoteGranted = true
	} else if args.LastLogTerm < latestEntry.Term {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = args.LastLogIndex >= len(rf.log)
	}

	if reply.VoteGranted {
		reply.Term = args.LastLogTerm
	} else {
		reply.Term = rf.currentTerm
	}
}

/**
 * AppendEntries RPC Handler
 */
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastReceive = time.Now()
	reply.Term = rf.currentTerm
	DPrintf("%d received a heartbeat from %d", rf.me, args.LeaderId)

	// Leader's log is not up-to-date so ignore.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	/*
		If two entries in different logs have the same index
		and term, then the logs are identical in all preceding
		entries.

		The two conditionals below perform that check.
	*/

	if len(rf.log)-1 < args.PrevLogIndex+1 {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex+1].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true
}

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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := false

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.currentTerm = 0
	rf.state = Follower
	rf.lastReceive = time.Now()
	rf.votedFor = -1

	go rf.MonitorLeader()
	DPrintf("%d initialized", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
