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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824-golabs-2020/src/labgob"
	"6.824-golabs-2020/src/labrpc"
)

// import "../labrpc"

// import "bytes"
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

// log，每条日志包含其提交给leader时的term和命令
type LogEntry struct {
	Term    int
	Command interface{}
}

type ROLE string

const (
	Leader    ROLE = "leader"
	Candidate ROLE = "candidate"
	Follower  ROLE = "follower"
)

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
	role         ROLE
	currentTerm  int
	leader       int
	votedFor     int
	commitIndex  int
	lastApplied  int
	logs         []LogEntry
	operatortime time.Time
	outtime      int
	hbtime       int

	nextIndex  []int
	matchIndex []int
	applymsg   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.role == Leader
	return term, isleader
}

func (rf *Raft) setouttime() {
	rf.operatortime = time.Now()
	rf.outtime = 800 + rand.Intn(1000)
	// DPrintf("%d 设置超时时间为:%d", rf.me, rf.outtime)
}

func (rf *Raft) setrole(role ROLE) {
	rf.role = role
	rf.setouttime()
}

func (rf *Raft) Init() {
	rf.mu.Lock()
	DPrintf("%v 转为leader", rf.me)
	index := rf.lastlogindex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = 0
	}
	rf.setrole(Leader)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go rf.CommandSync(i)
		}
	}
}

func (rf *Raft) setcommmitindex(index int) {
	if index < rf.commitIndex {
		return
	}
	old := rf.commitIndex
	rf.commitIndex = index
	for i := old + 1; i <= rf.commitIndex; i++ {
		// DPrintf("%v 回复index为 %v 的日志被应用", rf.me, i)
		rf.applymsg <- ApplyMsg{true, rf.logs[i].Command, i}
	}
	// DPrintf("%v %v 更新commit index 为: %v", rf.role, rf.me, rf.commitIndex)
}

func (rf *Raft) UpdateCommitindex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	commit := rf.commitIndex + 1
	for ; commit <= rf.lastlogindex(); commit += 1 {
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= commit {
				count += 1
			}
		}
		if count < len(rf.peers)/2+1 {
			break
		}
	}
	rf.setcommmitindex(commit - 1)
}

func (rf *Raft) setcurrentTerm(term int) {
	if term < rf.currentTerm {
		return
	}
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) setlogs(index int, logs []LogEntry) {
	rf.logs = append(rf.logs[:index], logs...)
	rf.persist()
}

func (rf *Raft) setvoteFor(peernum int) {
	rf.votedFor = peernum
	rf.persist()
}

func (rf *Raft) lastlogindex() int {
	return len(rf.logs) - 1
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("开始读取持久化数据")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var term int
	var votefor int
	if d.Decode(&logs) != nil || d.Decode(&term) != nil || d.Decode(&votefor) != nil {
		DPrintf("%v 读取错误", rf.me)
	} else {
		rf.mu.Lock()
		rf.logs = logs
		rf.currentTerm = term
		rf.votedFor = votefor
		DPrintf("%v 读取持久化数据，logs:%v，currentterm:%v, votefor:%v", rf.me, logs, term, votefor)
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	Lastindex int
}

//
// example RequestVote RPC handler.
//
// 一个term内只能投一票
//候选人会增加其term，所以接到请求投票的话，应该投给比自己term高的
//lastterm和lastindex用来保证候选人的数据应该比自己新或一样新
//需同时满足两个条件才能给票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("%v 收到 %v 的投票请求, 自身状态为:%v，请求者term:%v，自身term:%v", rf.me, args.CandidateId, rf.role, args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.setcurrentTerm(args.Term)
		rf.setvoteFor(-1)
		rf.setrole(Follower)
	}
	index := rf.lastlogindex()
	lastlogterm := rf.logs[index].Term
	// 要注意到选举限制条约虽然能限制到不能成为leader，但是会将别人的term带起来
	if (args.LastLogTerm == lastlogterm && args.LastLogIndex < index) || args.LastLogTerm < lastlogterm {
		DPrintf("触发选举限制条约，%v 拒绝给 %v 投票", rf.me, args.CandidateId)
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.setvoteFor(args.CandidateId)
		rf.setrole(Follower)
		reply.VoteGranted = true
		// DPrintf("%v 投票给 %v ，term:%v", rf.me, args.CandidateId, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		DPrintf("%v %v 收到来自 %v 的添加日志请求，日志内容为:%v, 自身term:%v, 请求者term: %v, leadercommitindex: %v", rf.role, rf.me, args.LeaderId, args.Entries, rf.currentTerm, args.Term, args.LeaderCommit)
	}

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v %v 拒绝来自 %v 的添加日志请求，日志内容为:%v, 自身term:%v，%v term:%v", rf.role, rf.me, args.LeaderId, args.Entries, rf.currentTerm, args.LeaderId, args.Term)
		}
		return
	}
	if rf.role != Follower {
		rf.setrole(Follower)
	}
	rf.setouttime()
	if args.Term > rf.currentTerm {
		rf.setcurrentTerm(args.Term)
		reply.Term = rf.currentTerm
	}
	if rf.lastlogindex() < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //  (len(rf.logs) > 0 && args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		if rf.lastlogindex() < args.PrevLogIndex {
			reply.Lastindex = rf.lastlogindex() + 1
		} else {
			for i := 1; i <= args.PrevLogIndex; i += 1 {
				if rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term {
					reply.Lastindex = i
					break
				}
			}
		}
		DPrintf("来自 %v 的日志有冲突，%v拒绝, 自身lastindex:%v, 请求者lastindex:%v, replyindex:%v", args.LeaderId, rf.me, rf.lastlogindex(), args.PrevLogIndex, reply.Lastindex)
		return
	}
	rf.setlogs(args.PrevLogIndex+1, args.Entries)
	if len(args.Entries) > 0 {
		DPrintf("%v 同步来自 %v 的日志，同步后日志为:%v", rf.me, args.LeaderId, rf.logs)
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastlogindex() {
			rf.setcommmitindex(args.LeaderCommit)
		} else {
			rf.setcommmitindex(rf.lastlogindex())
		}
	}
	reply.Success = true
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) jugletimeout() bool {
	if rf.role != Leader && time.Since(rf.operatortime) >= time.Duration(rf.outtime)*time.Millisecond {
		return true
	}
	return false
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.setrole(Candidate)
	rf.setcurrentTerm(rf.currentTerm + 1)
	// DPrintf("%v 增加term为 %v", rf.me, rf.currentTerm)
	rf.setvoteFor(rf.me)
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastlogindex(), rf.logs[rf.lastlogindex()].Term}
	rf.mu.Unlock()

	result := make(chan bool)
	defer close(result)

	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go func(index int) {
				reply := RequestVoteReply{}
				vote := false
				if ok := rf.sendRequestVote(index, &args, &reply); ok {
					rf.mu.Lock()
					// DPrintf("%v 收到 %v 的投票请求回应", rf.me, index)
					if reply.Term > rf.currentTerm {
						rf.setcurrentTerm(reply.Term)
						rf.setrole(Follower)
					} else if reply.VoteGranted && reply.Term == rf.currentTerm && rf.role == Candidate {
						vote = true
						DPrintf("%v 收到 %v 的投票, term:%v", rf.me, index, rf.currentTerm)
					}
					rf.mu.Unlock()
				}
				result <- vote
			}(i)
		}
	}
	num := 1
	for i := 0; i < len(rf.peers)-1; i += 1 {
		if r := <-result; r {
			num += 1
			if num == len(rf.peers)/2+1 {
				rf.Init()
			}
		}
	}
	// DPrintf("候选人:%v 的 term 为 %v 轮投票结果为 %v 票", rf.me, rf.currentTerm, num)
}

// func (rf *Raft) Sync() {
// 	for atomic.LoadInt32(&rf.dead) == 0 {
// 		if rf.role == Leader {
// 			go rf.CommandSync()
// 		}
// 		time.Sleep(time.Duration(rf.hbtime) * time.Millisecond)
// 	}
// }

func (rf *Raft) CommandSync(peernum int) {
	sendtime := time.Now()
	for atomic.LoadInt32(&rf.dead) == 0 {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			break
		}
		sendlastindex := rf.lastlogindex()
		matchindex := rf.matchIndex[peernum]
		preindex := rf.nextIndex[peernum] - 1
		leadercommit := rf.commitIndex
		entries := rf.logs[preindex+1 : sendlastindex+1] // preLogIndex是new ones的前一个，因此取出的日志是从preLogIndex+1到最后一个
		preterm := rf.logs[preindex].Term
		args := AppendEntriesArgs{rf.currentTerm, rf.me, preindex, preterm, entries, leadercommit}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		//	需要同步log或者到了心跳的时间
		if matchindex != sendlastindex || time.Since(sendtime) > time.Duration(rf.hbtime)*time.Millisecond {
			DPrintf("%v 给 %v发送同步请求, logs: %v, time:%v", rf.me, peernum, entries, time.Now())
			sendtime = time.Now()
			if ok := rf.sendAppendEntry(peernum, &args, &reply); ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("%v 的term比当前大，%v转为follower", peernum, rf.me)
					rf.setcurrentTerm(reply.Term)
					rf.setrole(Follower)
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[peernum] == preindex+1 && rf.matchIndex[peernum] == matchindex {
					if !reply.Success {
						// rf.nextIndex[peernum] -= 1
						rf.nextIndex[peernum] = reply.Lastindex
						DPrintf("收到冲突拒绝，更新%v的nextindex为:%v", peernum, rf.nextIndex[peernum])
					} else if len(entries) > 0 { //过滤掉心跳
						rf.nextIndex[peernum] = sendlastindex + 1
						rf.matchIndex[peernum] = sendlastindex
						DPrintf("更新%v的nextindex为:%v，matchindex为:%v", peernum, rf.nextIndex[peernum], rf.matchIndex[peernum])
						go rf.UpdateCommitindex()
						DPrintf("%v 同步成功， 当前 %v 的commitindex为: %v", peernum, rf.me, rf.commitIndex)
					}
				}
				rf.mu.Unlock()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) Server() {
	// go rf.Sync()
	for atomic.LoadInt32(&rf.dead) == 0 {
		if rf.jugletimeout() {
			DPrintf("%v 发生超时, time: %v", rf.me, time.Now())
			go rf.election()
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	index := 0
	term := 0
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if isLeader = rf.role == Leader; isLeader {
		// DPrintf("%v收到命令%v提交, 此时身份为: %v", rf.me, command, rf.role)
		log := LogEntry{rf.currentTerm, command}
		rf.setlogs(rf.lastlogindex()+1, []LogEntry{log})
		index = rf.lastlogindex()
		term = rf.currentTerm
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.hbtime = 150
	rf.applymsg = applyCh
	rf.logs = []LogEntry{{0, 0}}
	rf.setcommmitindex(0)
	rf.setrole(Follower)
	rf.setouttime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Server()
	return rf
}
