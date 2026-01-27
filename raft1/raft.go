package raft

// 文件 raftapi/raft.go 定义了 Raft 必须向服务器（或测试器）暴露的接口。
// 具体细节请参阅下方每个函数的注释。
//
// Make() 用于创建一个新的 Raft 节点实例，该实例实现了 Raft 接口。

import (
	"bytes"
	//"fmt"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int

const (
	Leader ServerState = iota
	Candidate
	Follower
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// 一个实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.Mutex          // 用于保护该节点共享状态访问的锁。
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 通信端点。
	persister *tester.Persister   // 用于保存该节点持久化状态的对象。
	me        int                 // 该节点在 peers[] 数组中的索引。
	dead      int32               // set by Kill()

	cond    *sync.Cond
	applyCh chan raftapi.ApplyMsg

	// 在此处存放你的数据（3A、3B、3C）。
	// 查看论文的图 2，以了解 Raft 服务器必须维护的状态。
	state            ServerState // raft节点的状态
	currentTerm      int         // raft节点的任期
	votedFor         int         // raft节点的投票信息
	log              []LogEntry  // 日志
	lastIncludeTerm  int         // 最后一条快照的任期
	lastIncludeIndex int         // 最后一条快照的索引

	commitIndex int // raft集群已提交的日志索引
	lastApplied int // raft节点已应用到上层的日志

	nextIndex  []int // leader对下一条应发送给follower的日志索引
	matchIndex []int // 各个follower已接受的日志索引

	lastHeartBeatTime time.Time     // 最后一次接受心跳的时间
	heartBeatTimeout  time.Duration // 心跳超时时间

	pendingSnap      bool
	pendingSnapData  []byte
	pendingSnapIndex int
	pendingSnapTerm  int
}

// 返回当前任期（currentTerm）以及该服务器
// 是否认为自己是领导者。
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

// 将 Raft 的持久化状态保存到稳定存储中，
// 以便在崩溃重启后可以恢复。
// 具体哪些状态需要持久化，请参阅论文的图 2。
// 在实现快照功能之前，应将 persister.Save() 的第二个参数设为 nil。
// 实现快照功能后，应传入当前快照（如果尚无快照，则传入 nil）。
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludeIndex) != nil ||
		e.Encode(rf.lastIncludeTerm) != nil {
		return
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		if rf.lastIncludeIndex > rf.commitIndex {
			rf.commitIndex = rf.lastIncludeIndex
		}
		rf.mu.Unlock()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = Follower
	rf.lastHeartBeatTime = time.Now()
	reply.Term = rf.currentTerm

	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		return
	}

	if args.LastIncludeIndex < rf.lastIncludeIndex+len(rf.log) {
		cut := args.LastIncludeIndex - rf.lastIncludeIndex
		if rf.log[cut].Term == args.LastIncludeTerm {
			rf.log = rf.log[cut:]
		} else {
			rf.log = make([]LogEntry, 1)
		}
	} else {
		rf.log = make([]LogEntry, 1)
	}
	rf.log[0] = LogEntry{Term: args.LastIncludeTerm, Command: nil}

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludeIndex)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludeIndex) != nil ||
		e.Encode(rf.lastIncludeTerm) != nil {
		return
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	snap := make([]byte, len(args.Data))
	copy(snap, args.Data)
	rf.pendingSnap = true
	rf.pendingSnapData = snap
	rf.pendingSnapIndex = args.LastIncludeIndex
	rf.pendingSnapTerm = args.LastIncludeTerm

	rf.cond.Broadcast()
}

// 服务（上层应用）表示它已经创建了一个快照（snapshot），
// 该快照包含了**直到并且包括 index 在内的所有信息**。
// 这意味着服务已经不再需要 index 以及之前（包含 index）的日志。
// Raft 此时应该**尽可能多地裁剪（trim）自己的日志**，
// 删除 index（含）之前的日志条目。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		return
	}

	lastLogIndex := rf.lastIncludeIndex + len(rf.log) - 1
	if index > lastLogIndex {
		return
	}

	cut := index - rf.lastIncludeIndex
	newTerm := rf.log[cut].Term

	newlog := make([]LogEntry, len(rf.log)-cut)
	copy(newlog, rf.log[cut:])

	newlog[0] = LogEntry{Term: newTerm, Command: nil}

	rf.log = newlog
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = newTerm

	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludeIndex) != nil ||
		e.Encode(rf.lastIncludeTerm) != nil {
		return
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// 示例 RequestVote RPC 参数结构体。
// 字段名必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// 示例 RequestVote RPC 回复结构体。
// 字段名必须以大写字母开头！
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	CTerm := args.Term
	CLIndex := args.LastLogIndex
	CLTerm := args.LastLogTerm
	CID := args.CandidateID
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if CTerm < rf.currentTerm {
		// fmt.Printf("[服务器%d]:候选者服务器%d的任期：%d < %d,拒绝投票\n", rf.me, CID, CLTerm, rf.currentTerm)
		return
	}
	if CTerm > rf.currentTerm {
		// fmt.Printf("[服务器%d]:候选者服务器%d的任期：%d > %d,更新任期和投票信息\n", rf.me, CID, CTerm, rf.currentTerm)
		rf.currentTerm = CTerm
		rf.votedFor = -1
		rf.state = Follower
		reply.Term = rf.currentTerm
	}
	if rf.votedFor != -1 && rf.votedFor != CID {
		// fmt.Printf("[服务器%d]:在任期%d已经投票了,拒绝投票给服务器%d\n", rf.me, rf.currentTerm, CID)
		return
	}
	if !rf.isLogUp(CLIndex, CLTerm) {
		// fmt.Printf("[服务器%d]:服务器%d的日志过于陈旧,拒绝投票,任期：%d\n", rf.me, CID, rf.currentTerm)
		return
	}
	//fmt.Printf("[服务器%d]:服务器%d的日志较新,投票,任期：%d\n", rf.me, CID, rf.currentTerm)
	rf.votedFor = CID
	rf.state = Follower
	reply.VoteGranted = true
	rf.lastHeartBeatTime = time.Now()
}

func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.log) == 1 {
		return rf.lastIncludeTerm, rf.lastIncludeIndex
	}
	slicesIndex := len(rf.log) - 1
	lastLogIndex := rf.lastIncludeIndex + slicesIndex
	lastLogTerm := rf.log[len(rf.log)-1].Term
	return lastLogTerm, lastLogIndex
}

func (rf *Raft) isLogUp(CLIndex int, CLTerm int) bool {
	lastLogTerm, lastLogIndex := rf.getLastLogInfo()
	if lastLogTerm > CLTerm {
		return false
	}
	if lastLogTerm < CLTerm {
		return true
	}
	return lastLogIndex <= CLIndex
}

// 示例代码，用于向服务器发送 RequestVote RPC。
// server 是目标服务器在 rf.peers[] 数组中的索引。
// args 是 RPC 的参数。
// 调用者应传入 &reply，Call() 会填充 reply。
// args 和 reply 的类型必须与 handler 函数中声明的参数类型相同（包括是否为指针）。
//
// labrpc 包模拟了一个丢包的网络，其中服务器可能无法访问，
// 请求和回复可能会丢失。
// Call() 会发送请求并等待回复。如果回复在超时区间内到达，
// Call() 返回 true；否则返回 false。Call() 可能会长时间阻塞。
// 返回 false 可能是由于服务器宕机、无法访问的活跃服务器、请求丢失或回复丢失导致的。
//
// Call() 保证最终会返回（可能会有延迟），除非服务器端的 handler 函数没有返回。
// 因此，不需要为 Call() 实现自己的超时机制。
//
// 如果你遇到 RPC 问题，检查传递的结构体中的字段名是否都已大写，
// 并且调用者传递的是 reply 结构体的地址（&），而不是结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 使用 Raft 的服务（例如一个键值服务器）希望开始
// 对下一个要添加到 Raft 日志中的命令达成一致。如果该
// 服务器不是领导者，返回 false；否则，开始达成一致并立即返回。
// 不能保证这个命令会被提交到 Raft 日志，因为领导者可能会崩溃或失去选举。
// 即使 Raft 实例已经被终止，该函数也应当优雅地返回。
//
// 第一个返回值是命令将在日志中提交时出现的索引。
// 第二个返回值是当前的任期。
// 第三个返回值是如果该服务器认为自己是领导者，则返回 true
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		newLog := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, newLog)
		rf.persist()
		index = len(rf.log) + rf.lastIncludeIndex - 1
		term = rf.currentTerm
		isLeader = true
		for i := range rf.peers {
			if i != rf.me {
				go rf.replicateToPeer(i)
			}
		}
	}

	return index, term, isLeader
}

// 测试器在每个测试之后不会停止 Raft 创建的 goroutines，
// 但是会调用 Kill() 方法。你的代码可以使用 killed() 来
// 检查 Kill() 是否已被调用。使用 atomic 可以避免使用锁。
//
// 问题在于，长时间运行的 goroutines 会占用内存并消耗 CPU 时间，
// 可能会导致后续测试失败并产生混乱的调试输出。
// 任何长时间运行的 goroutine 都应该调用 killed() 来检查
// 是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	CTerm := args.Term
	CPIndex := args.PrevLogIndex
	CPTerm := args.PrevLogTerm
	//CLID := args.LeaderID
	CLCommit := args.LeaderCommit
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if CTerm < rf.currentTerm {
		// fmt.Printf("[服务器%d]:领导者%d的任期：%d < %d,过于陈旧，拒绝日志\n", rf.me, CLID, CTerm,
		// 	rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if CTerm > rf.currentTerm {
		// fmt.Printf("[服务器%d]:领导者%d的任期：%d > %d,更新自身信息\n", rf.me, CLID, CTerm,
		// 	rf.currentTerm)
		rf.currentTerm = CTerm
		rf.votedFor = -1
		rf.persist()
	}
	// fmt.Printf("[服务器%d]:收到领导者%d的信息,更新心跳信息\n", rf.me, CLID)

	rf.state = Follower
	rf.lastHeartBeatTime = time.Now()
	if CPIndex < rf.lastIncludeIndex {
		// fmt.Printf("[服务器%d]:领导者%d发送的日志已经被快照保存\n", rf.me, CLID)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.lastIncludeIndex + 1
		return
	}
	lastIndex := rf.lastIncludeIndex + len(rf.log) - 1
	if CPIndex > lastIndex {
		// fmt.Printf("[服务器%d]:领导者%d发送的日志过于超前\n", rf.me, CLID)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = lastIndex + 1
		reply.ConflictTerm = -1
		return
	}
	phyIndex := CPIndex - rf.lastIncludeIndex
	if rf.log[phyIndex].Term != CPTerm {
		// fmt.Printf("[服务器%d]:任期%d日志中存在被错误的日志信息，进行快速回退\n", rf.me, rf.log[phyIndex].Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[phyIndex].Term
		tmpIdx := phyIndex
		for tmpIdx > 0 && rf.log[tmpIdx-1].Term == reply.ConflictTerm {
			tmpIdx--
		}
		reply.ConflictIndex = rf.lastIncludeIndex + tmpIdx
		return
	}
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		phyIdx := idx - rf.lastIncludeIndex
		if phyIdx < len(rf.log) {
			if rf.log[phyIdx].Term != entry.Term {
				rf.log = rf.log[:phyIdx]
				rf.log = append(rf.log, args.Entries[i:]...) // 一次性追加后续所有
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...) // 越界了，直接追加剩余全部
			rf.persist()
			break
		}
	}
	if CLCommit > rf.commitIndex {
		newlastidx := CPIndex + len(args.Entries)
		rf.commitIndex = min(newlastidx, CLCommit)
		rf.cond.Broadcast()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) replicateToPeer(peer int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.lastIncludeIndex {
		args := &InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LeaderID:         rf.me,
			LastIncludeIndex: rf.lastIncludeIndex,
			LastIncludeTerm:  rf.lastIncludeTerm,
			Data:             rf.persister.ReadSnapshot(),
		}

		rf.mu.Unlock()
		reply := &InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(peer, args, reply); ok {
			rf.handleInstallSnapshot(peer, args, reply)
		}
		return
	}

	prevIdx := rf.nextIndex[peer] - 1
	phyPrevIdx := prevIdx - rf.lastIncludeIndex
	prevTerm := rf.log[phyPrevIdx].Term
	entries := make([]LogEntry, 0)
	entries = append(entries, rf.log[rf.nextIndex[peer]-rf.lastIncludeIndex:]...)

	args := &AppendEntriesArgs{
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		LeaderID:     rf.me,
		Term:         rf.currentTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(peer, args, reply); ok {
		rf.handleAppendEntries(peer, args, reply)
	}
}

func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if args.LastIncludeIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludeIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
	rf.advanceCommitIndex()
}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
			rf.nextIndex[peer] = newmatch + 1
			rf.advanceCommitIndex()
		}
	} else {
		if rf.nextIndex[peer] != args.PrevLogIndex+1 {
			return
		}
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			foundIdx := -1
			for n := len(rf.log) - 1; n >= 0; n-- {
				if rf.log[n].Term == reply.ConflictTerm {
					foundIdx = rf.lastIncludeIndex + n
					break
				}
			}
			if foundIdx == -1 {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				rf.nextIndex[peer] = foundIdx + 1
			}
		}
	}
}

func (rf *Raft) advanceCommitIndex() {
	if rf.state != Leader {
		return
	}
	for n := rf.lastIncludeIndex + len(rf.log) - 1; n > rf.commitIndex; n-- {
		count := 1
		if rf.log[n-rf.lastIncludeIndex].Term < rf.currentTerm {
			break
		}
		if rf.log[n-rf.lastIncludeIndex].Term == rf.currentTerm {
			for pe := range rf.peers {
				if pe != rf.me {
					if rf.matchIndex[pe] >= n {
						count++
					}
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.cond.Broadcast()
				break
			}
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// 你的代码在这里（3A）
		// 检查是否应该开始领导者选举。

		rf.mu.Lock()
		state := rf.state
		if state == Leader {
			for i := range rf.peers {
				if i != rf.me {
					//go rf.sendHeartbeat(i)
					go rf.replicateToPeer(i)
				}
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			if rf.state != Leader && time.Since(rf.lastHeartBeatTime) > rf.heartBeatTimeout {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = Candidate
				rf.persist()
				//fmt.Printf("[服务器%d]:超时，开始选举,任期：%d\n", rf.me, rf.currentTerm)
				rf.startElection()
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) startElection() {
	term, index := rf.getLastLogInfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  term,
	}
	num := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(id int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(id, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Candidate || rf.currentTerm != args.Term {
						return
					}
					if reply.VoteGranted {
						num++
						if num > len(rf.peers)/2 && rf.state == Candidate {
							rf.state = Leader
							rf.votedFor = rf.me
							_, lastLogIdx := rf.getLastLogInfo()
							for i := range rf.nextIndex {
								rf.nextIndex[i] = lastLogIdx + 1 // nextIndex 应该是最后一条日志索引 + 1
								rf.matchIndex[i] = 0
							}
							rf.persist()
							//fmt.Printf("[服务器%d]:赢得选举了,任期：%d\n", rf.me, rf.currentTerm)
							return
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							//fmt.Printf("[服务器%d]:节点中有更大的任期：%d,退化为follower\n", rf.me, reply.Term)
						}
					}
				}
			}(i)
		}
	}
}


func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied && !rf.pendingSnap {
			rf.cond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		if rf.pendingSnap && rf.pendingSnapIndex <= rf.lastApplied {
			rf.pendingSnap = false
		}

		if rf.pendingSnap && rf.pendingSnapIndex > rf.lastApplied {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapData,
				SnapshotTerm:  rf.pendingSnapTerm,
				SnapshotIndex: rf.pendingSnapIndex,
			}
			rf.pendingSnap = false
			rf.lastApplied = rf.pendingSnapIndex

			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		if !rf.pendingSnap && rf.lastApplied < rf.lastIncludeIndex {
			rf.lastApplied = rf.lastIncludeIndex
		}

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}

		low := rf.lastApplied + 1
		high := rf.commitIndex

		start := low - rf.lastIncludeIndex
		end := high - rf.lastIncludeIndex
		entries := make([]LogEntry, end-start+1)
		copy(entries, rf.log[start:end+1])

		rf.lastApplied = high
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: low + i,
			}
		}
	}
}

// 服务或测试器希望创建一个 Raft 服务器。
// 所有 Raft 服务器的端口（包括当前服务器）都存储在 peers[] 中。
// 当前服务器的端口是 peers[me]。
// 所有服务器的 peers[] 数组顺序一致。
// persister 是用来保存该服务器持久化状态的地方，
// 它也会初始存储最新的保存状态（如果有的话）。
// applyCh 是一个通道，测试器或服务期望 Raft 在该通道上发送 ApplyMsg 消息。
// Make() 必须快速返回，因此应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rand.Seed(time.Now().UnixNano() + int64(me))
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.readPersist(persister.ReadRaftState())
	//rf.log[0] = LogEntry{Term: rf.lastIncludeTerm}

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludeIndex)

	if snap := rf.persister.ReadSnapshot(); len(snap) > 0 {
		rf.pendingSnap = true
		rf.pendingSnapData = snap
		rf.pendingSnapIndex = rf.lastIncludeIndex
		rf.pendingSnapTerm = rf.lastIncludeTerm
		rf.cond.Broadcast()
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastHeartBeatTime = time.Now()
	rf.heartBeatTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

	// 从崩溃前持久化的状态初始化
	//fmt.Printf("[服务器%d]:已初始化完成\n", rf.me)
	// 启动 ticker goroutine 以启动选举

	go rf.ticker()
	go rf.applier()

	return rf
}
