package quorum

// HardState is the minimal durable state Election Safety depends on. If
// a restarted server forgets votedFor, it can grant a second vote in a
// term it already voted in — the entire "one vote per term" argument
// that guarantees at most one leader per term stops holding. It must be
// saved before any RPC response that depends on it (a granted vote, an
// accepted AppendEntries) is allowed to leave the process.
type HardState struct {
	Term     Term
	VotedFor NodeID
}

// Storage is the seam between Node's pure in-memory state and whatever
// durably persists it — a plain file today (FileStorage), eventually
// flowforge's own WAL once this integrates with it directly.
//
// Ordering contract: when a single Ready() reports both a TruncateFrom
// and overlapping EntriesToSave (a follower's log conflicted with its
// leader's and got corrected), the caller MUST call TruncateFrom before
// AppendEntries. This is an append-only log, not a database — replay
// reconstructs state by replaying records in the order they were
// written, so writing the replacement entries before the truncation
// that's supposed to clear the way for them means replay would delete
// the very entries it just replayed. Within a single Ready(), Node
// always computes them in a way that's correct provided the caller
// preserves this order.
//
// Storage deliberately does not hold snapshot bytes — only the boundary
// (index, term) that Compact records. Bulk snapshot data is the state
// machine's own concern (it produced the bytes; it can decide how to
// keep them durable), mirroring how real systems keep a small-record WAL
// separate from bulk snapshot storage rather than interleaving them.
type Storage interface {
	// SaveHardState durably persists currentTerm and votedFor.
	SaveHardState(term Term, votedFor NodeID) error

	// AppendEntries durably persists new log entries.
	AppendEntries(entries []LogEntry) error

	// TruncateFrom durably discards every persisted entry at or after
	// index — the durable-storage side of the §5.3 conflict resolution
	// that already happens to Node's in-memory log.
	TruncateFrom(index LogIndex) error

	// Compact durably records that the log up to and including index (at
	// the given term) has been folded into a snapshot the caller
	// maintains elsewhere; discrete entries at or before it are forgotten.
	Compact(index LogIndex, term Term) error

	// Load reconstructs everything needed to resume after a restart: the
	// last-saved term and vote, the most recent compaction boundary
	// (index 0 if nothing has ever been compacted), and every real log
	// entry strictly after that boundary, in index order. commitIndex/
	// lastApplied are deliberately not part of this — Raft never
	// requires persisting them (see NewNodeFromStorage).
	Load() (term Term, votedFor NodeID, snapshotIndex LogIndex, snapshotTerm Term, log []LogEntry, err error)
}
