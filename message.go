package quorum

// MessageType discriminates the RPCs defined by the Raft paper. Election
// (Part 1) uses VoteRequest/VoteResponse; AppendEntries/AppendResponse
// are already wired up here since heartbeats are just AppendEntries with
// no entries — Part 2 fills in the entries themselves.
type MessageType int

const (
	MsgVoteRequest MessageType = iota
	MsgVoteResponse
	MsgAppendEntries
	MsgAppendResponse
	MsgInstallSnapshot
	MsgInstallSnapshotResponse
	MsgPreVoteRequest
	MsgPreVoteResponse
	MsgTimeoutNow
)

func (t MessageType) String() string {
	switch t {
	case MsgVoteRequest:
		return "VoteRequest"
	case MsgVoteResponse:
		return "VoteResponse"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendResponse:
		return "AppendResponse"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResponse:
		return "InstallSnapshotResponse"
	case MsgPreVoteRequest:
		return "PreVoteRequest"
	case MsgPreVoteResponse:
		return "PreVoteResponse"
	case MsgTimeoutNow:
		return "TimeoutNow"
	default:
		return "unknown"
	}
}

// Message unifies every RPC exchanged between nodes. Only the fields
// relevant to m.Type are meaningful; the rest are zero-valued.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// MsgVoteRequest
	LastLogIndex LogIndex
	LastLogTerm  Term

	// MsgVoteResponse
	VoteGranted bool

	// MsgAppendEntries
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex

	// MsgAppendResponse / MsgInstallSnapshotResponse (shared: "success"
	// plus the resulting matched position)
	Success       bool
	Index         LogIndex // on success: the follower's new last-matched index
	ConflictIndex LogIndex // AppendResponse failure: where the leader should retry from
	ConflictTerm  Term     // AppendResponse failure: the conflicting term, for fast backtracking

	// MsgAppendEntries / MsgAppendResponse (shared): ReadSeq correlates a
	// heartbeat round-trip with a pending ReadIndex request. The leader
	// stamps its current readSeq on every outgoing AppendEntries; a
	// follower echoes back exactly what it received. A monotonic,
	// exact-echo correlation token, not an inference from unrelated
	// fields — see Node.pendingReads' doc comment for why that
	// distinction specifically matters here.
	ReadSeq uint64

	// MsgInstallSnapshot
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	SnapshotData      []byte
}
