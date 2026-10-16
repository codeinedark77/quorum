package quorum

import "testing"

func newTestNode(id NodeID, peers []NodeID, seed int64) *Node {
	return NewNode(id, peers, DefaultConfig(), seed)
}

// withLog builds a log the way Node itself represents one internally:
// entries[0] is always the sentinel/compaction-boundary slot, so tests
// constructing a log by hand for a node with no compaction need the
// {0,0} "nothing compacted yet" sentinel explicitly, the same way
// NewNode itself starts every fresh node.
func withLog(entries ...LogEntry) []LogEntry {
	return append([]LogEntry{{Term: 0, Index: 0}}, entries...)
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	if n.Role() != Follower {
		t.Fatalf("new node should start as Follower, got %s", n.Role())
	}
	if n.Term() != 0 {
		t.Fatalf("new node should start at term 0, got %d", n.Term())
	}
}

func TestElectionTimeoutStartsCandidacy(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
	}
	if n.Role() != Candidate {
		t.Fatalf("expected Candidate after election timeout, got %s", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("starting an election should bump term to 1, got %d", n.Term())
	}
	msgs := n.Ready().Messages
	if len(msgs) != 2 {
		t.Fatalf("expected 2 vote requests (one per peer), got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgVoteRequest {
			t.Fatalf("expected MsgVoteRequest, got %s", m.Type)
		}
	}
}

func TestGrantsVoteToUpToDateCandidate(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.Step(Message{Type: MsgVoteRequest, From: "b", Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Type != MsgVoteResponse || !msgs[0].VoteGranted {
		t.Fatalf("expected a granted vote response, got %+v", msgs)
	}
	if n.Term() != 1 {
		t.Fatalf("node should adopt the candidate's term, got %d", n.Term())
	}
}

func TestRefusesSecondVoteInSameTerm(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.Step(Message{Type: MsgVoteRequest, From: "b", Term: 1})
	n.Ready() // drain
	n.Step(Message{Type: MsgVoteRequest, From: "c", Term: 1})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected vote refused for a second candidate in the same term, got %+v", msgs)
	}
}

func TestRegrantsSameCandidateIdempotently(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.Step(Message{Type: MsgVoteRequest, From: "b", Term: 1})
	n.Ready()
	n.Step(Message{Type: MsgVoteRequest, From: "b", Term: 1}) // retried RPC
	msgs := n.Ready().Messages
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected idempotent re-grant to the same candidate, got %+v", msgs)
	}
}

func TestRefusesVoteForStaleLog(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	// Node "a" already has an entry at term 5 — more up-to-date than a
	// candidate whose log tops out at term 3.
	n.log = append(n.log, LogEntry{Term: 5, Index: 1})
	n.Step(Message{Type: MsgVoteRequest, From: "b", Term: 5, LastLogIndex: 1, LastLogTerm: 3})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected vote refused for a candidate with a stale log, got %+v", msgs)
	}
}

func TestHigherTermStepsDownLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.term = 3
	n.Step(Message{Type: MsgAppendEntries, From: "b", Term: 5})
	if n.Role() != Follower {
		t.Fatalf("expected step-down to Follower on higher term, got %s", n.Role())
	}
	if n.Term() != 5 {
		t.Fatalf("expected term to adopt the higher value, got %d", n.Term())
	}
}

func TestCandidateBecomesLeaderOnMajority(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1) // 5-node cluster
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
	}
	n.Ready() // drain vote requests
	n.Step(Message{Type: MsgVoteResponse, From: "b", Term: n.Term(), VoteGranted: true})
	if n.Role() != Candidate {
		t.Fatalf("2 of 5 votes (including self) shouldn't be a majority yet, got %s", n.Role())
	}
	n.Step(Message{Type: MsgVoteResponse, From: "c", Term: n.Term(), VoteGranted: true})
	if n.Role() != Leader {
		t.Fatalf("3 of 5 votes should be a majority, got %s", n.Role())
	}
}

func TestLeaderSendsHeartbeatOnSchedule(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{}
	n.matchIndex = map[NodeID]LogIndex{}
	n.Ready() // clear anything pending

	for i := 0; i < n.cfg.HeartbeatTicks; i++ {
		n.Tick()
	}
	msgs := n.Ready().Messages
	if len(msgs) != 2 {
		t.Fatalf("expected 2 heartbeats (one per peer), got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgAppendEntries {
			t.Fatalf("expected heartbeats to be MsgAppendEntries, got %s", m.Type)
		}
	}
}

// --- Part 2: log replication ---

func TestProposeAppendsToLeaderLog(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	idx, term, err := n.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 1 || term != 0 {
		t.Fatalf("expected index=1 term=0, got index=%d term=%d", idx, term)
	}
	if n.LastIndex() != 1 {
		t.Fatalf("expected log to have 1 entry, LastIndex=%d", n.LastIndex())
	}
}

func TestProposeFailsWhenNotLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1) // starts as Follower
	_, _, err := n.Propose([]byte("hello"))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestSingleNodeClusterCommitsImmediately(t *testing.T) {
	n := newTestNode("solo", nil, 1) // no peers
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{}
	n.matchIndex = map[NodeID]LogIndex{}
	_, _, err := n.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.commitIndex != 1 {
		t.Fatalf("a single-node cluster should commit its own writes immediately, got commitIndex=%d", n.commitIndex)
	}
	ready := n.Ready()
	if len(ready.CommittedEntries) != 1 || string(ready.CommittedEntries[0].Data) != "hello" {
		t.Fatalf("expected the committed entry to surface via Ready(), got %+v", ready.CommittedEntries)
	}
}

func TestLeaderDoesNotCommitOldTermEntryOnReplicationAlone(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1) // 5-node cluster
	n.role = Leader
	n.term = 4
	n.log = withLog(LogEntry{Term: 2, Index: 1}) // an entry from an OLD term
	n.nextIndex = map[NodeID]LogIndex{}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1} // 3 of 5 (self+b+c): a majority
	n.advanceCommitIndex()
	if n.commitIndex != 0 {
		t.Fatalf("must not commit an old-term entry just because a majority replicated it, got commitIndex=%d", n.commitIndex)
	}
}

func TestLeaderCommitsOldEntryOnceCurrentTermEntryCommits(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1)
	n.role = Leader
	n.term = 4
	n.log = withLog(LogEntry{Term: 2, Index: 1})
	n.nextIndex = map[NodeID]LogIndex{}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.advanceCommitIndex()
	if n.commitIndex != 0 {
		t.Fatalf("sanity check failed: old entry shouldn't be committed yet, got %d", n.commitIndex)
	}

	// A current-term entry gets appended and replicated to the same
	// majority — this should commit index 2 (current term) AND, as a
	// side effect, retroactively commit index 1 too.
	idx, term, err := n.Propose([]byte("current-term-write"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if idx != 2 || term != 4 {
		t.Fatalf("expected proposal at index 2 term 4, got index=%d term=%d", idx, term)
	}
	n.Ready() // drain the replication messages Propose() queued
	n.matchIndex["b"] = 2
	n.matchIndex["c"] = 2
	n.advanceCommitIndex()
	if n.commitIndex != 2 {
		t.Fatalf("expected commitIndex to advance to 2 (retroactively committing index 1 too), got %d", n.commitIndex)
	}
}

func TestFollowerRejectsAppendEntriesOnLogMismatch(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.Step(Message{
		Type: MsgAppendEntries, From: "b", Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 2, // claims index 1 is term 2; we have term 1 there
	})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected AppendEntries to be rejected on log mismatch, got %+v", msgs)
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2}, LogEntry{Term: 2, Index: 3})
	n.Step(Message{
		Type: MsgAppendEntries, From: "b", Term: 3,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Index: 2}},
	})
	if n.LastIndex() != 2 {
		t.Fatalf("expected the conflicting suffix (old indices 2,3) to be discarded, LastIndex=%d", n.LastIndex())
	}
	e, ok := n.entryAt(2)
	if !ok || e.Term != 3 {
		t.Fatalf("expected index 2 to now be term 3, got %+v ok=%v", e, ok)
	}
}

func TestHandleAppendResponseUpdatesMatchAndNextIndex(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 1
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2})
	n.nextIndex = map[NodeID]LogIndex{"b": 1}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: 1, Success: true, Index: 2})

	if n.matchIndex["b"] != 2 {
		t.Fatalf("expected matchIndex[b]=2, got %d", n.matchIndex["b"])
	}
	if n.nextIndex["b"] != 3 {
		t.Fatalf("expected nextIndex[b]=3, got %d", n.nextIndex["b"])
	}
}

func TestHandleAppendResponseBacktracksToOwnLastEntryOfConflictTerm(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 5
	// Our log has two term-2 entries (indices 2,3); the follower reports
	// a conflict somewhere in term 2. Since we DO have term-2 entries,
	// the fast path should retry right after our own last one (index 4),
	// not just accept the follower's raw ConflictIndex.
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 2, Index: 2}, LogEntry{Term: 2, Index: 3}, LogEntry{Term: 4, Index: 4})
	n.nextIndex = map[NodeID]LogIndex{"b": 5}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	n.Step(Message{
		Type: MsgAppendResponse, From: "b", Term: 5, Success: false,
		ConflictTerm: 2, ConflictIndex: 2,
	})

	if n.nextIndex["b"] != 4 {
		t.Fatalf("expected fast-path backtrack to 4 (just past our own last term-2 entry), got %d", n.nextIndex["b"])
	}
}

func TestHandleAppendResponseFallsBackToConflictIndexWhenTermUnknown(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 5
	// We have no term-3 entries at all, so there's nothing to fast-path
	// past — fall back to exactly what the follower told us.
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 4, Index: 2})
	n.nextIndex = map[NodeID]LogIndex{"b": 3}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	n.Step(Message{
		Type: MsgAppendResponse, From: "b", Term: 5, Success: false,
		ConflictTerm: 3, ConflictIndex: 2,
	})

	if n.nextIndex["b"] != 2 {
		t.Fatalf("expected fallback to ConflictIndex=2 when we hold no entries at ConflictTerm, got %d", n.nextIndex["b"])
	}
}

func TestHandleAppendResponseUsesConflictIndexWhenFollowerMissingEntry(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 1
	n.nextIndex = map[NodeID]LogIndex{"b": 10}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	// ConflictTerm==0 (its zero value) signals "the follower had nothing
	// at all at PrevLogIndex" rather than a term mismatch.
	n.Step(Message{
		Type: MsgAppendResponse, From: "b", Term: 1, Success: false,
		ConflictIndex: 3,
	})

	if n.nextIndex["b"] != 3 {
		t.Fatalf("expected nextIndex[b]=3 from the bare ConflictIndex hint, got %d", n.nextIndex["b"])
	}
}

func TestHandleAppendResponseDecrementsWithoutConflictHints(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 1
	n.nextIndex = map[NodeID]LogIndex{"b": 5}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: 1, Success: false})

	if n.nextIndex["b"] != 4 {
		t.Fatalf("expected plain decrement to 4 when neither conflict hint is set, got %d", n.nextIndex["b"])
	}
}

func TestHandleAppendEntriesReportsMissingEntry(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.Step(Message{
		Type: MsgAppendEntries, From: "b", Term: 1,
		PrevLogIndex: 5, PrevLogTerm: 1, // we don't have anything at index 5
	})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected rejection when PrevLogIndex is beyond our log, got %+v", msgs)
	}
	if msgs[0].ConflictIndex != 2 || msgs[0].ConflictTerm != 0 {
		t.Fatalf("expected ConflictIndex=2 (LastIndex+1), ConflictTerm=0, got %+v", msgs[0])
	}
}

func TestSingleNodeClusterElectsSelfViaTick(t *testing.T) {
	n := newTestNode("solo", nil, 1)
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
		if n.Role() == Leader {
			break
		}
	}
	if n.Role() != Leader {
		t.Fatalf("a node with no peers should elect itself leader on timeout, got %s", n.Role())
	}
}

func TestAdvanceCommitIndexNoOpForNonLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1) // starts Follower
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.advanceCommitIndex()
	if n.commitIndex != 0 {
		t.Fatalf("advanceCommitIndex should be a no-op for a non-leader, got commitIndex=%d", n.commitIndex)
	}
}

// --- Part 3: persistence ---

func TestReadyReportsHardStateOnlyWhenChanged(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	ready := n.Ready() // nothing has happened yet
	if ready.HardState != nil {
		t.Fatalf("expected no HardState on an untouched node, got %+v", ready.HardState)
	}

	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick() // becomes a candidate: term and votedFor both change
	}
	ready = n.Ready()
	if ready.HardState == nil {
		t.Fatal("expected HardState to be reported after an election started")
	}
	if ready.HardState.Term != 1 || ready.HardState.VotedFor != "a" {
		t.Fatalf("expected term=1 votedFor=a, got %+v", ready.HardState)
	}

	// Nothing changed since — should not be re-reported.
	ready = n.Ready()
	if ready.HardState != nil {
		t.Fatalf("expected no HardState when nothing changed since the last Ready(), got %+v", ready.HardState)
	}
}

func TestReadyReportsNewEntriesToSave(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	n.Ready() // drain whatever Leader setup produced

	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	ready := n.Ready()
	if len(ready.EntriesToSave) != 1 || string(ready.EntriesToSave[0].Data) != "x" {
		t.Fatalf("expected exactly the new entry to be reported for saving, got %+v", ready.EntriesToSave)
	}

	// Calling Ready() again with nothing new shouldn't re-report it.
	ready = n.Ready()
	if len(ready.EntriesToSave) != 0 {
		t.Fatalf("expected no EntriesToSave when nothing changed since the last Ready(), got %+v", ready.EntriesToSave)
	}
}

func TestReadyReportsTruncateFromOnConflict(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2})
	n.Ready() // establish persistedLogIndex=2, as if both entries were already durable

	n.Step(Message{
		Type: MsgAppendEntries, From: "b", Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 2, Index: 2}},
	})
	ready := n.Ready()
	if ready.TruncateFrom != 2 {
		t.Fatalf("expected TruncateFrom=2, got %d", ready.TruncateFrom)
	}
	if len(ready.EntriesToSave) != 1 || ready.EntriesToSave[0].Term != 2 {
		t.Fatalf("expected the replacement entry to be reported for saving, got %+v", ready.EntriesToSave)
	}
}

func TestNewNodeFromStorageResumesCorrectly(t *testing.T) {
	log := []LogEntry{{Term: 1, Index: 1, Data: []byte("a")}, {Term: 2, Index: 2, Data: []byte("b")}}
	n := NewNodeFromStorage("a", []NodeID{"b", "c"}, DefaultConfig(), 1, 2, "peer-x", 0, 0, log, nil, 1)

	if n.Term() != 2 {
		t.Fatalf("expected restored term=2, got %d", n.Term())
	}
	if n.Role() != Follower {
		t.Fatalf("a restarted node must resume as Follower regardless of its role before crashing, got %s", n.Role())
	}
	if n.LastIndex() != 2 {
		t.Fatalf("expected restored log to have LastIndex=2, got %d", n.LastIndex())
	}
	if n.votedFor != "peer-x" {
		t.Fatalf("expected restored votedFor=peer-x, got %s", n.votedFor)
	}

	// appliedIndex=1 was passed in, so only index 2 should replay.
	ready := n.Ready()
	if len(ready.CommittedEntries) != 0 {
		t.Fatalf("commitIndex hasn't been established yet post-restore, shouldn't report anything committed, got %+v", ready.CommittedEntries)
	}
	// Nothing should be reported as needing persistence — it's already durable.
	if ready.HardState != nil || len(ready.EntriesToSave) != 0 {
		t.Fatalf("a freshly-restored node's loaded state is already durable, shouldn't be re-reported: hardstate=%+v entries=%+v",
			ready.HardState, ready.EntriesToSave)
	}
}

func TestBecomeLeaderAppendsNoOpAndFastTracksOldEntryCommit(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1) // 5-node cluster
	// An entry inherited from a previous term, already on a majority
	// (simulating what a restarted-then-re-elected leader's log would
	// look like) but not yet committable per §5.4.2's own-term rule.
	n.term = 2
	n.log = withLog(LogEntry{Term: 2, Index: 1})
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
	}
	n.Ready() // drain vote requests
	n.Step(Message{Type: MsgVoteResponse, From: "b", Term: n.Term(), VoteGranted: true})
	n.Step(Message{Type: MsgVoteResponse, From: "c", Term: n.Term(), VoteGranted: true})
	if n.Role() != Leader {
		t.Fatalf("expected to win the election, got %s", n.Role())
	}
	if n.LastIndex() != 2 {
		t.Fatalf("expected becomeLeader to append a no-op at index 2, LastIndex=%d", n.LastIndex())
	}
	noop, ok := n.entryAt(2)
	if !ok || len(noop.Data) != 0 || noop.Term != n.Term() {
		t.Fatalf("expected an empty-Data no-op at index 2 in the new term, got %+v ok=%v", noop, ok)
	}

	// Once a majority also replicates the no-op, both it AND the
	// inherited index-1 entry should commit together.
	n.matchIndex["b"] = 2
	n.matchIndex["c"] = 2
	n.advanceCommitIndex()
	if n.commitIndex != 2 {
		t.Fatalf("expected the no-op to fast-track committing the inherited entry too, got commitIndex=%d", n.commitIndex)
	}
}

// --- Part 4: snapshotting ---

func TestCompactLogDiscardsOldEntries(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2}, LogEntry{Term: 2, Index: 3})
	n.commitIndex = 3

	term, err := n.CompactLog(2, []byte("snap-as-of-2"))
	if err != nil {
		t.Fatalf("CompactLog failed: %v", err)
	}
	if term != 1 {
		t.Fatalf("expected term=1 at the compaction boundary, got %d", term)
	}
	if n.LastIndex() != 3 {
		t.Fatalf("compacting shouldn't change LastIndex, got %d", n.LastIndex())
	}
	if _, ok := n.entryAt(1); ok {
		t.Fatal("expected index 1 to be gone after compacting through it")
	}
	if _, ok := n.entryAt(2); ok {
		t.Fatal("expected index 2 (the boundary itself) to not be a retrievable entry")
	}
	e, ok := n.entryAt(3)
	if !ok || e.Term != 2 {
		t.Fatalf("expected index 3 to survive compaction, got %+v ok=%v", e, ok)
	}
}

func TestCompactLogRefusesUncommittedIndex(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2})
	n.commitIndex = 1 // index 2 is NOT committed yet

	if _, err := n.CompactLog(2, nil); err != ErrCompactUncommitted {
		t.Fatalf("expected ErrCompactUncommitted, got %v", err)
	}
	if n.snapshotIndex() != 0 {
		t.Fatalf("a refused compaction shouldn't change anything, got snapshotIndex=%d", n.snapshotIndex())
	}
}

func TestCompactLogIsNoOpIfAlreadyCompactedFurther(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 2, Index: 2})
	n.commitIndex = 2
	if _, err := n.CompactLog(2, []byte("snap")); err != nil {
		t.Fatalf("CompactLog failed: %v", err)
	}
	term, err := n.CompactLog(1, nil) // asking to compact LESS than we already have
	if err != nil {
		t.Fatalf("expected a no-op, not an error, got %v", err)
	}
	if term != 2 {
		t.Fatalf("expected the existing boundary's term (2) to be returned unchanged, got %d", term)
	}
	if n.snapshotIndex() != 2 {
		t.Fatalf("expected snapshotIndex to remain 2, got %d", n.snapshotIndex())
	}
}

func TestCompactLogRefusesUnknownIndex(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	// Artificially inconsistent with the log (commitIndex should never
	// outrun LastIndex in real operation) specifically to exercise this
	// defensive guard directly, independent of the uncommitted-index check.
	n.commitIndex = 50
	if _, err := n.CompactLog(50, nil); err != ErrCompactUnknownIndex {
		t.Fatalf("expected ErrCompactUnknownIndex for an index beyond what our log actually has, got %v", err)
	}
}

func TestReplicateToSendsSnapshotWhenFollowerIsBehindCompaction(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2}, LogEntry{Term: 2, Index: 3})
	n.commitIndex = 3
	if _, err := n.CompactLog(2, []byte("snap-data")); err != nil {
		t.Fatalf("CompactLog failed: %v", err)
	}
	n.nextIndex = map[NodeID]LogIndex{"b": 1} // follower needs index 1, which is long gone
	n.matchIndex = map[NodeID]LogIndex{"b": 0}
	n.Ready() // drain the compaction/no-op noise

	n.replicateTo("b")
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Type != MsgInstallSnapshot {
		t.Fatalf("expected an InstallSnapshot, got %+v", msgs)
	}
	if msgs[0].LastIncludedIndex != 2 || msgs[0].LastIncludedTerm != 1 {
		t.Fatalf("expected snapshot boundary (2,1), got (%d,%d)", msgs[0].LastIncludedIndex, msgs[0].LastIncludedTerm)
	}
	if string(msgs[0].SnapshotData) != "snap-data" {
		t.Fatalf("expected the actual snapshot bytes to be attached, got %q", msgs[0].SnapshotData)
	}
}

func TestHandleInstallSnapshotOnFreshFollower(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1) // empty log, never seen anything
	n.Step(Message{
		Type: MsgInstallSnapshot, From: "b", Term: 3,
		LastIncludedIndex: 5, LastIncludedTerm: 2, SnapshotData: []byte("snap"),
	})
	ready := n.Ready()
	if len(ready.Messages) != 1 || !ready.Messages[0].Success || ready.Messages[0].Index != 5 {
		t.Fatalf("expected a successful ack at index 5, got %+v", ready.Messages)
	}
	if n.LastIndex() != 5 || n.snapshotIndex() != 5 || n.snapshotTerm() != 2 {
		t.Fatalf("expected the snapshot boundary adopted wholesale, got last=%d snapIdx=%d snapTerm=%d",
			n.LastIndex(), n.snapshotIndex(), n.snapshotTerm())
	}
	if n.commitIndex != 5 {
		t.Fatalf("expected commitIndex to advance to the snapshot boundary, got %d", n.commitIndex)
	}
	if ready.InstalledSnapshot == nil || ready.InstalledSnapshot.Index != 5 || string(ready.InstalledSnapshot.Data) != "snap" {
		t.Fatalf("expected Ready to surface the installed snapshot for the state machine, got %+v", ready.InstalledSnapshot)
	}
}

func TestHandleInstallSnapshotKeepsAgreeingTail(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2}, LogEntry{Term: 1, Index: 3})
	n.Step(Message{
		Type: MsgInstallSnapshot, From: "b", Term: 1,
		LastIncludedIndex: 2, LastIncludedTerm: 1, SnapshotData: []byte("snap"),
	})
	if n.LastIndex() != 3 {
		t.Fatalf("expected index 3 (which agreed at the boundary) to survive, LastIndex=%d", n.LastIndex())
	}
	e, ok := n.entryAt(3)
	if !ok || e.Term != 1 {
		t.Fatalf("expected index 3 preserved with its original term, got %+v ok=%v", e, ok)
	}
}

func TestHandleAppendEntriesRespondsCaughtUpWhenPrevLogIndexBeforeOwnSnapshot(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 2, Index: 2})
	n.commitIndex = 2
	if _, err := n.CompactLog(2, []byte("snap")); err != nil {
		t.Fatalf("CompactLog failed: %v", err)
	}
	n.Ready() // drain

	// A leader with stale knowledge thinks we only have up to index 1.
	n.Step(Message{
		Type: MsgAppendEntries, From: "b", Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 1, Index: 2}},
	})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || !msgs[0].Success || msgs[0].Index != 2 {
		t.Fatalf("expected a success response reporting our real position (2), got %+v", msgs)
	}
}

func TestHandleInstallSnapshotResponseUpdatesMatchAndNextIndex(t *testing.T) {
	n := newTestNode("a", []NodeID{"b"}, 1)
	n.role = Leader
	n.term = 1
	n.nextIndex = map[NodeID]LogIndex{"b": 1}
	n.matchIndex = map[NodeID]LogIndex{"b": 0}

	n.Step(Message{Type: MsgInstallSnapshotResponse, From: "b", Term: 1, Success: true, Index: 5})

	if n.matchIndex["b"] != 5 || n.nextIndex["b"] != 6 {
		t.Fatalf("expected matchIndex[b]=5 nextIndex[b]=6, got matchIndex=%d nextIndex=%d",
			n.matchIndex["b"], n.nextIndex["b"])
	}
}

// --- PreVote ---

func newPreVoteTestNode(id NodeID, peers []NodeID, seed int64) *Node {
	cfg := DefaultConfig()
	cfg.PreVote = true
	return NewNode(id, peers, cfg, seed)
}

func TestPreVoteGrantedWhenNoLeaderTrustedAndLogCurrent(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b"}, 1)
	n.Step(Message{Type: MsgPreVoteRequest, From: "b", Term: n.Term() + 1})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Type != MsgPreVoteResponse || !msgs[0].VoteGranted {
		t.Fatalf("expected a granted prevote, got %+v", msgs)
	}
	if n.Term() != 0 {
		t.Fatalf("a prevote grant must never change our real term, got %d", n.Term())
	}
}

func TestPreVoteRefusedWhenLeaderTrusted(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b", "c"}, 1)
	// Recognize a leader first (heartbeat), so leaderAlive becomes true.
	n.Step(Message{Type: MsgAppendEntries, From: "b", Term: 1})
	n.Ready()

	n.Step(Message{Type: MsgPreVoteRequest, From: "c", Term: n.Term() + 1})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected the prevote to be refused while a leader is trusted, got %+v", msgs)
	}
}

func TestElectionTimeoutStartsPreVoteNotRealCandidacy(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b", "c"}, 1)
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
	}
	if n.Role() != PreCandidate {
		t.Fatalf("expected PreCandidate after election timeout with PreVote enabled, got %s", n.Role())
	}
	if n.Term() != 0 {
		t.Fatalf("entering PreCandidate must not bump the real term, got %d", n.Term())
	}
	msgs := n.Ready().Messages
	if len(msgs) != 2 {
		t.Fatalf("expected 2 prevote requests, got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgPreVoteRequest || m.Term != 1 {
			t.Fatalf("expected MsgPreVoteRequest carrying prospective term 1, got %+v", m)
		}
	}
}

func TestPreVoteMajorityTriggersRealElection(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b", "c", "d", "e"}, 1)
	for i := 0; i < n.cfg.ElectionTicksMax+1; i++ {
		n.Tick()
	}
	n.Ready() // drain prevote requests
	n.Step(Message{Type: MsgPreVoteResponse, From: "b", Term: 0, VoteGranted: true})
	if n.Role() != PreCandidate || n.Term() != 0 {
		t.Fatalf("2 of 5 prevotes shouldn't be enough yet, or shouldn't touch term: role=%s term=%d", n.Role(), n.Term())
	}
	n.Step(Message{Type: MsgPreVoteResponse, From: "c", Term: 0, VoteGranted: true})
	if n.Role() != Candidate {
		t.Fatalf("expected a majority of prevotes to trigger a real candidacy, got %s", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("the real election that follows should bump the term for real, got %d", n.Term())
	}
}

func TestIsolatedPreVoteNeverBumpsRealTermAcrossManyRetries(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b", "c"}, 1)
	// No peers ever respond — simulating total isolation — across many
	// election-timeout cycles worth of ticks.
	for i := 0; i < 500; i++ {
		n.Tick()
		n.Ready() // drain and discard; nothing ever answers
	}
	if n.Term() != 0 {
		t.Fatalf("an isolated node should never bump its real term while it can't reach a prevote majority, got term=%d", n.Term())
	}
	if n.Role() != PreCandidate {
		t.Fatalf("expected it to still be retrying prevotes, got %s", n.Role())
	}
}

// --- Part 5: membership changes ---

func TestIsSingleNodeDiff(t *testing.T) {
	cases := []struct {
		name     string
		current  []NodeID
		proposed []NodeID
		want     bool
	}{
		{"add one", []NodeID{"a", "b"}, []NodeID{"a", "b", "c"}, true},
		{"remove one", []NodeID{"a", "b", "c"}, []NodeID{"a", "b"}, true},
		{"add two", []NodeID{"a"}, []NodeID{"a", "b", "c"}, false},
		{"remove and add", []NodeID{"a", "b"}, []NodeID{"a", "c"}, false},
		{"identical", []NodeID{"a", "b"}, []NodeID{"a", "b"}, false},
	}
	for _, tc := range cases {
		if got := isSingleNodeDiff(tc.current, tc.proposed); got != tc.want {
			t.Errorf("%s: isSingleNodeDiff(%v, %v) = %v, want %v", tc.name, tc.current, tc.proposed, got, tc.want)
		}
	}
}

func TestMembershipEncodeDecodeRoundTrips(t *testing.T) {
	ids := []NodeID{"alpha", "b", "gamma-3"}
	got := decodeMembership(encodeMembership(ids))
	if len(got) != len(ids) {
		t.Fatalf("expected %d ids back, got %d: %v", len(ids), len(got), got)
	}
	for i := range ids {
		if got[i] != ids[i] {
			t.Fatalf("id %d: got %q want %q", i, got[i], ids[i])
		}
	}
}

func TestProposeConfigChangeFailsWhenNotLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	if _, _, err := n.ProposeConfigChange([]NodeID{"a", "b", "c", "d"}); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestProposeConfigChangeRejectsMultiNodeDiff(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	if _, _, err := n.ProposeConfigChange([]NodeID{"a", "b", "c", "d", "e"}); err != ErrConfigChangeNotSingleNode {
		t.Fatalf("expected ErrConfigChangeNotSingleNode, got %v", err)
	}
}

func TestProposeConfigChangeRejectsOverlappingChange(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	if _, _, err := n.ProposeConfigChange([]NodeID{"a", "b", "c", "d"}); err != nil {
		t.Fatalf("first config change failed: %v", err)
	}
	if _, _, err := n.ProposeConfigChange([]NodeID{"a", "b", "c", "e"}); err != ErrConfigChangeInProgress {
		t.Fatalf("expected ErrConfigChangeInProgress while the first is still uncommitted, got %v", err)
	}
}

func TestApplyMembershipAddsPeerWithFreshNextIndex(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1, Data: []byte("x")})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1}

	n.applyMembership([]NodeID{"a", "b", "c", "d"}, 2)

	if len(n.peers) != 3 {
		t.Fatalf("expected 3 peers after adding one, got %v", n.peers)
	}
	if n.nextIndex["d"] != n.LastIndex()+1 {
		t.Fatalf("expected the new peer's nextIndex to start at LastIndex+1, got %d", n.nextIndex["d"])
	}
	if n.nextIndex["b"] != 2 || n.matchIndex["b"] != 1 {
		t.Fatalf("expected existing peer b's replication state preserved, got nextIndex=%d matchIndex=%d",
			n.nextIndex["b"], n.matchIndex["b"])
	}
}

func TestApplyMembershipRemovesPeer(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1, "d": 1}
	n.matchIndex = map[NodeID]LogIndex{"b": 0, "c": 0, "d": 0}

	n.applyMembership([]NodeID{"a", "b", "c"}, 1)

	if len(n.peers) != 2 {
		t.Fatalf("expected 2 peers after removing one, got %v", n.peers)
	}
	// "d"'s replication state must be PRESERVED, not dropped — it still
	// needs a final heartbeat carrying the commit that removed it, or
	// it never learns to retire and eventually disrupts the cluster via
	// its own election timeout. See TestDepartingPeerGetsNotifiedThenDropped.
	if _, ok := n.nextIndex["d"]; !ok {
		t.Fatal("expected the removed peer's replication state preserved for final notification, not dropped immediately")
	}
	if info, ok := n.departing["d"]; !ok || info.targetIndex != 1 {
		t.Fatalf("expected d tracked as departing with target index 1, got %+v ok=%v", info, ok)
	}
}

func TestApplyMembershipRetiresWhenSelfRemoved(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}

	n.applyMembership([]NodeID{"b", "c"}, 1) // "a" is no longer listed

	if n.Role() != Removed {
		t.Fatalf("expected the terminal Removed state after being removed from the cluster, got %s", n.Role())
	}
	if len(n.peers) != 0 {
		t.Fatalf("expected peers cleared after removal, got %v", n.peers)
	}

	// A retired node must not do ANYTHING — not tick toward an election,
	// not respond to messages. This is what actually prevents it from
	// disrupting the cluster it left, unlike merely reporting Follower
	// (which would still tick its own election timer toward nothing).
	for i := 0; i < 100; i++ {
		n.Tick()
	}
	if n.Role() != Removed || n.Term() != 0 {
		t.Fatalf("a retired node must never start an election on its own, got role=%s term=%d", n.Role(), n.Term())
	}
	n.Step(Message{Type: MsgVoteRequest, From: "x", Term: 99})
	if len(n.Ready().Messages) != 0 {
		t.Fatal("a retired node must not respond to messages at all")
	}
}

// TestDepartingPeerGetsNotifiedThenDropped is the direct proof for the
// bug this mechanism exists to fix: dropping a removed peer from
// replication the INSTANT the leader's own bookkeeping updates would
// strand it — it has the config-change entry in its log, but never
// learns it actually committed, so it never retires and eventually
// disrupts the cluster via its own election timeout. The leader must
// keep replicating to it until it confirms.
func TestDepartingPeerGetsNotifiedThenDropped(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1, Type: EntryConfigChange})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2, "d": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1, "d": 1}

	n.applyMembership([]NodeID{"a", "b", "c"}, 1) // removes "d" via change at index 1

	if len(n.peers) != 2 {
		t.Fatalf("expected 2 peers, got %v", n.peers)
	}

	// The leader must still be willing to replicate to "d" even though
	// it's no longer in n.peers.
	n.Ready() // drain anything pending
	n.replicateToAll()
	msgs := n.Ready().Messages
	sentToD := false
	for _, m := range msgs {
		if m.To == "d" {
			sentToD = true
		}
	}
	if !sentToD {
		t.Fatal("expected the leader to still replicate to a departing peer until it confirms")
	}

	// "d" acks with an index that proves it now has (at least) the
	// commit that removed it — the leader can stop notifying it.
	n.Step(Message{Type: MsgAppendResponse, From: "d", Term: n.Term(), Success: true, Index: 1})

	if _, stillDeparting := n.departing["d"]; stillDeparting {
		t.Fatal("expected d to be dropped from departing once it confirmed the removal commit")
	}
	if _, ok := n.nextIndex["d"]; ok {
		t.Fatal("expected d's replication state fully cleaned up after confirmation")
	}
}

// TestStaleAckDoesNotPrematurelyConfirmDeparture is the direct
// regression test for the actual bug this mechanism hit: an ack proving
// only that a peer's LOG matches up to the target index — which can be
// true from BEFORE it was ever marked departing, just from ordinary
// replication — must not be mistaken for proof it learned the entry
// committed. Only a fresh, post-departure round trip counts.
func TestStaleAckDoesNotPrematurelyConfirmDeparture(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1, Type: EntryConfigChange})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2, "d": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1, "d": 1}

	n.applyMembership([]NodeID{"a", "b", "c"}, 1) // marks "d" departing, target index 1

	// A stale ack arrives — "d" already had index 1 in its log from
	// BEFORE it was ever marked departing (ordinary replication, nothing
	// to do with learning about its removal). notifiedFresh is still
	// false at this point: we haven't sent it anything since departure
	// began.
	n.Step(Message{Type: MsgAppendResponse, From: "d", Term: n.Term(), Success: true, Index: 1})

	if _, stillDeparting := n.departing["d"]; !stillDeparting {
		t.Fatal("a stale ack (from before departure began) must not prematurely confirm the departure")
	}

	// Only once the leader actually sends "d" something post-departure...
	n.replicateToAll()
	n.Ready() // drain

	// ...does a genuine fresh ack correctly confirm and clean up.
	n.Step(Message{Type: MsgAppendResponse, From: "d", Term: n.Term(), Success: true, Index: 1})
	if _, stillDeparting := n.departing["d"]; stillDeparting {
		t.Fatal("expected a genuine post-departure ack to confirm and clean up")
	}
}

func TestBecomeLeaderSetsUpReplicationForPendingDepartures(t *testing.T) {
	// Simulates a leadership handoff while a departure is still
	// unresolved: this node processed the removal as a FOLLOWER (so it
	// never had leader-side nextIndex/matchIndex for "d" to carry
	// forward), and now wins its own election.
	n := newTestNode("a", []NodeID{"b", "c", "d"}, 1)
	n.log = withLog(LogEntry{Term: 1, Index: 1, Type: EntryConfigChange})
	n.applyMembership([]NodeID{"a", "b", "c"}, 1) // marks "d" departing, as a non-leader

	if _, ok := n.departing["d"]; !ok {
		t.Fatal("sanity check: expected d tracked as departing")
	}

	n.becomeLeader()

	if _, ok := n.nextIndex["d"]; !ok {
		t.Fatal("expected becomeLeader to set up replication state for a pending departure inherited from before it was leader")
	}
	n.Ready() // drain
	n.replicateToAll()
	msgs := n.Ready().Messages
	sentToD := false
	for _, m := range msgs {
		if m.To == "d" {
			sentToD = true
		}
	}
	if !sentToD {
		t.Fatal("expected the new leader to continue notifying the still-pending departing peer")
	}
}

func TestDecodeMembershipStopsAtTruncatedData(t *testing.T) {
	full := encodeMembership([]NodeID{"alpha", "beta"})
	truncated := full[:len(full)-2] // cut off mid-way through the last id
	got := decodeMembership(truncated)
	if len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("expected only the complete first id to decode, got %v", got)
	}
}

// --- ReadIndex ---

func TestRequestReadIndexFailsWhenNotLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	if _, err := n.RequestReadIndex(); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestReadConfirmsOnceMajorityAcksAndApplyCatchesUp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1) // 5-node cluster
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1, "d": 1, "e": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	n.commitIndex = 3
	n.lastApplied = 3 // already caught up

	id, err := n.RequestReadIndex()
	if err != nil {
		t.Fatalf("RequestReadIndex failed: %v", err)
	}
	n.Ready() // drain the confirmation-soliciting heartbeats

	// Only self so far (1 of 5) — nowhere near a majority.
	if ready := n.checkPendingReads(); len(ready) != 0 {
		t.Fatalf("expected no ready reads with only 1 of 5 confirmed, got %+v", ready)
	}

	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: n.Term(), Success: true, Index: 0, ReadSeq: 1})
	if ready := n.checkPendingReads(); len(ready) != 0 {
		t.Fatalf("expected no ready reads with only 2 of 5 confirmed, got %+v", ready)
	}

	n.Step(Message{Type: MsgAppendResponse, From: "c", Term: n.Term(), Success: true, Index: 0, ReadSeq: 1})
	ready := n.checkPendingReads()
	if len(ready) != 1 || ready[0].ID != id || ready[0].Index != 3 {
		t.Fatalf("expected the read to confirm at 3 of 5 with lastApplied already caught up, got %+v", ready)
	}
}

func TestReadDoesNotConfirmUntilLastAppliedCatchesUp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1) // 3-node cluster, majority 2
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	n.commitIndex = 5
	n.lastApplied = 2 // NOT caught up yet — a real state machine consuming CommittedEntries would still be behind

	if _, err := n.RequestReadIndex(); err != nil {
		t.Fatalf("RequestReadIndex failed: %v", err)
	}
	// Deliberately not calling Ready() here — it unconditionally sets
	// lastApplied = commitIndex, which would erase the very lag this
	// test needs to hold in place to check the gate actually works.
	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: n.Term(), Success: true, Index: 0, ReadSeq: 1})

	// Majority (2 of 3, including self) IS confirmed, but lastApplied
	// hasn't caught up to targetIndex(5) yet — must not surface.
	if ready := n.checkPendingReads(); len(ready) != 0 {
		t.Fatalf("expected no ready reads while lastApplied lags the target index, got %+v", ready)
	}

	n.lastApplied = 5 // now caught up
	if ready := n.checkPendingReads(); len(ready) != 1 {
		t.Fatalf("expected the read to confirm once lastApplied caught up, got %+v", ready)
	}
}

// TestStaleReadSeqEchoDoesNotConfirmRead is the direct analog of the
// departing-peer bug, applied preemptively here rather than discovered
// the hard way again: an ack echoing an OLDER seq — proof of a message
// sent BEFORE this read was requested, still in flight — must not count
// as confirming it. Only an echo at or after the read's own seq can.
func TestStaleReadSeqEchoDoesNotConfirmRead(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	n.commitIndex = 1
	n.lastApplied = 1

	// Some ordinary replication happens BEFORE any read is requested —
	// readSeq is still 0 at this point.
	if n.readSeq != 0 {
		t.Fatalf("sanity check: expected readSeq=0 before any read request, got %d", n.readSeq)
	}

	if _, err := n.RequestReadIndex(); err != nil { // bumps readSeq to 1
		t.Fatalf("RequestReadIndex failed: %v", err)
	}
	n.Ready()

	// A stale ack arrives, echoing seq=0 — proof of a message sent
	// before the read was ever requested.
	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: n.Term(), Success: true, Index: 0, ReadSeq: 0})
	if ready := n.checkPendingReads(); len(ready) != 0 {
		t.Fatalf("a stale seq=0 echo must not confirm a read requested at seq=1, got %+v", ready)
	}

	// A genuine fresh ack, echoing seq=1, correctly does confirm.
	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: n.Term(), Success: true, Index: 0, ReadSeq: 1})
	if ready := n.checkPendingReads(); len(ready) != 1 {
		t.Fatalf("expected a fresh seq=1 echo to confirm, got %+v", ready)
	}
}

func TestPendingReadsClearedOnStepDown(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}

	if _, err := n.RequestReadIndex(); err != nil {
		t.Fatalf("RequestReadIndex failed: %v", err)
	}
	if len(n.pendingReads) != 1 {
		t.Fatalf("sanity check: expected 1 pending read, got %d", len(n.pendingReads))
	}

	n.Step(Message{Type: MsgAppendEntries, From: "b", Term: n.Term() + 1}) // a higher-term leader supersedes us

	if len(n.pendingReads) != 0 {
		t.Fatal("expected pending reads to be cleared on step-down — a former leader can't safely confirm anything")
	}
}

// --- Leadership transfer ---

func TestTransferLeadershipFailsWhenNotLeader(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	if err := n.TransferLeadership("b"); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestTransferLeadershipFailsForUnknownTarget(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	if err := n.TransferLeadership("z"); err != ErrTransferUnknownTarget {
		t.Fatalf("expected ErrTransferUnknownTarget, got %v", err)
	}
}

func TestTransferToSelfIsANoOp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1}
	n.matchIndex = map[NodeID]LogIndex{}
	if err := n.TransferLeadership("a"); err != nil {
		t.Fatalf("expected transferring to self to be a harmless no-op, got %v", err)
	}
	if n.transferTarget != "" {
		t.Fatal("expected no transfer to actually be pending")
	}
}

func TestTransferLeadershipSendsTimeoutNowImmediatelyWhenCaughtUp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 1} // "b" already fully caught up

	if err := n.TransferLeadership("b"); err != nil {
		t.Fatalf("TransferLeadership failed: %v", err)
	}
	if n.transferTarget != "" {
		t.Fatal("expected the transfer to complete immediately, not stay pending, since b was already caught up")
	}
	msgs := n.Ready().Messages
	found := false
	for _, m := range msgs {
		if m.Type == MsgTimeoutNow && m.To == "b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an immediate TimeoutNow to b, got %+v", msgs)
	}
}

func TestTransferLeadershipWaitsForTargetToCatchUp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 3}
	n.matchIndex = map[NodeID]LogIndex{"b": 1, "c": 2} // "b" is one entry behind

	if err := n.TransferLeadership("b"); err != nil {
		t.Fatalf("TransferLeadership failed: %v", err)
	}
	if n.transferTarget != "b" {
		t.Fatal("expected the transfer to stay pending since b isn't caught up yet")
	}
	n.Ready() // drain — must not contain TimeoutNow yet
	msgs := n.Ready().Messages
	for _, m := range msgs {
		if m.Type == MsgTimeoutNow {
			t.Fatal("must not send TimeoutNow before the target has caught up")
		}
	}

	// "b" acks catching up to index 2 — now fully matched.
	n.Step(Message{Type: MsgAppendResponse, From: "b", Term: n.Term(), Success: true, Index: 2})

	if n.transferTarget != "" {
		t.Fatal("expected the transfer to complete once b caught up")
	}
	msgs = n.Ready().Messages
	found := false
	for _, m := range msgs {
		if m.Type == MsgTimeoutNow && m.To == "b" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected TimeoutNow to fire the instant the target caught up")
	}
}

func TestProposeRejectedDuringTransfer(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 0, "c": 0} // behind, so the transfer stays pending

	if err := n.TransferLeadership("b"); err != nil {
		t.Fatalf("TransferLeadership failed: %v", err)
	}
	if _, _, err := n.Propose([]byte("x")); err != ErrLeadershipTransferInProgress {
		t.Fatalf("expected ErrLeadershipTransferInProgress, got %v", err)
	}
}

func TestTransferLeadershipRejectsOverlappingTransfer(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 0, "c": 0}

	if err := n.TransferLeadership("b"); err != nil {
		t.Fatalf("first TransferLeadership failed: %v", err)
	}
	if err := n.TransferLeadership("c"); err != ErrTransferInProgress {
		t.Fatalf("expected ErrTransferInProgress, got %v", err)
	}
}

func TestTransferAbandonedIfTargetNeverCatchesUp(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.role = Leader
	n.log = withLog(LogEntry{Term: 1, Index: 1})
	n.nextIndex = map[NodeID]LogIndex{"b": 2, "c": 2}
	n.matchIndex = map[NodeID]LogIndex{"b": 0, "c": 0}

	if err := n.TransferLeadership("b"); err != nil {
		t.Fatalf("TransferLeadership failed: %v", err)
	}
	for i := 0; i < n.cfg.ElectionTicksMax*2+1; i++ {
		n.Tick() // "b" never acks, never catches up
	}
	if n.transferTarget != "" {
		t.Fatal("expected the abandoned transfer to clear itself after a generous timeout")
	}
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatalf("expected Propose to work again after the transfer timed out, got %v", err)
	}
}

func TestHandleTimeoutNowBypassesElectionTimeoutAndPreVote(t *testing.T) {
	n := newPreVoteTestNode("a", []NodeID{"b", "c"}, 1) // PreVote enabled
	n.Step(Message{Type: MsgTimeoutNow, From: "b", Term: n.Term()})

	if n.Role() != Candidate {
		t.Fatalf("expected an immediate real candidacy, bypassing PreCandidate entirely, got %s", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("expected a real election (term bumped), got term=%d", n.Term())
	}
}

func TestHandleTimeoutNowIgnoresStaleTerm(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"}, 1)
	n.term = 5
	n.Step(Message{Type: MsgTimeoutNow, From: "b", Term: 2}) // from a leader we've already moved past

	if n.Role() != Follower {
		t.Fatalf("expected a stale TimeoutNow to be ignored, got role=%s", n.Role())
	}
	if n.Term() != 5 {
		t.Fatalf("expected term to stay at 5, got %d", n.Term())
	}
}

func TestConfigChangeAppliesOnCommitNotAppend(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c", "d", "e"}, 1) // 5-node cluster
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{"b": 1, "c": 1, "d": 1, "e": 1}
	n.matchIndex = map[NodeID]LogIndex{}

	if _, _, err := n.ProposeConfigChange([]NodeID{"a", "b", "c", "d", "e", "f"}); err != nil {
		t.Fatalf("ProposeConfigChange failed: %v", err)
	}
	if len(n.peers) != 4 {
		t.Fatalf("appending a config change must not apply it yet — peers should still be the OLD 4, got %d: %v",
			len(n.peers), n.peers)
	}

	// Replicate to a majority of the OLD config (3 of 5, including
	// self) and let it commit.
	n.matchIndex["b"] = n.LastIndex()
	n.matchIndex["c"] = n.LastIndex()
	n.advanceCommitIndex()
	n.Ready() // drains CommittedEntries, which is where applyMembership actually runs

	if len(n.peers) != 5 {
		t.Fatalf("expected peers to include the new node once the change committed, got %d: %v", len(n.peers), n.peers)
	}
}
