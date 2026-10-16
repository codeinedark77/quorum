package quorum

// Term is a Raft election term: a monotonically increasing logical clock.
// Any message carrying a term older than a node's current term is stale
// and gets rejected; any message carrying a newer term forces the
// receiving node back to Follower (Raft paper, Rules for Servers).
type Term uint64

// LogIndex is a 1-based position in the replicated log. 0 is the sentinel
// "no entry" value — an empty log has LastIndex() == 0.
type LogIndex uint64

// NodeID identifies a participant in the cluster.
type NodeID string

// EntryType distinguishes ordinary client data from cluster membership
// changes, which Node itself must act on once committed (updating its
// own notion of who the peers are), not just hand to the state machine.
type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfigChange
)

// LogEntry is a single replicated log entry. Part 1 (leader election)
// only needs Term/Index to implement the §5.4.1 "up-to-date log" check;
// Data starts getting appended once Part 2 (replication) lands.
type LogEntry struct {
	Term  Term
	Index LogIndex
	Type  EntryType
	Data  []byte
}

// Role is one of the three states a Raft node can be in (paper, Figure 4).
type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
	// Removed is terminal: once a node processes a config change that
	// no longer lists it, it must stop participating entirely — not
	// just report a different role. A node that merely fell back to
	// Follower would keep ticking its own election timer, and once the
	// real leader (correctly) stops replicating to something it no
	// longer considers a peer, that timer would eventually elapse and
	// the retired node would start elections against a cluster it isn't
	// part of anymore, using a peer list nobody else agrees with.
	Removed
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Removed:
		return "removed"
	default:
		return "unknown"
	}
}
