package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// 当每个 Raft 节点发现有新的连续日志条目被提交（committed）时，
// 该节点应该通过传给 Make() 的 applyCh，向上层服务（或测试器）
// 发送一个 ApplyMsg。
// 将 CommandValid 设为 true，用来表示这个 ApplyMsg
// 包含的是一个新提交的日志条目。
//
// 在 Lab 3 中，你还需要通过 applyCh 发送其他类型的消息
//（例如快照 snapshots）；到那时你可以在 ApplyMsg 中添加新的字段，
// 但对于这些其他用途，需要将 CommandValid 设为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
