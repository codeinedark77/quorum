package quorum

import (
	"errors"
	"math/rand"
	"sort"
)

// ErrNotLeader is returned by Propose when called on a node that isn't
// currently the leader.
var ErrNotLeader = errors.New("quorum: not leader")

// ErrCompactUncommitted is returned by CompactLog when asked to discard
// entries that haven't been proven committed yet — doing so would risk
// permanently losing data that a future leader election could still
// legitimately overwrite.
var ErrCompactUncommitted = errors.New("quorum: cannot compact past commitIndex")

// ErrCompactUnknownIndex is returned by CompactLog when asked to compact
// to an index this node has no record of at all.
var ErrCompactUnknownIndex = errors.New("quorum: no such log index to compact to")

// Config controls timing, expressed in ticks rather than wall-clock time.
// The caller decides what a tick means — 10ms in production, one
// simulated step in a test. Raft's safety never depends on the actual
// values, only on ElectionTicksMax > HeartbeatTicks by a comfortable
// margin, so a single delayed heartbeat doesn't trigger a spurious
// election.
type Config struct {
	ElectionTicksMin int
	ElectionTicksMax int
	HeartbeatTicks   int

	// PreVote adds a phase before a real election: a node that suspects
	// the leader is gone first checks, without incrementing its own
	// term, whether it could actually win — peers refuse if they still
	// believe a leader is alive. Without this, an isolated node keeps
	// bumping its term forever while cut off, and reconnecting forces
	// the real leader to step down for no real reason (it's the
	// isolated node that was wrong, not the cluster). Defaults to false
	// so existing behavior and every earlier test are unaffected;
	// enabling it only changes *how* an election gets triggered, never
	// the safety of the election itself.
	PreVote bool
}

func DefaultConfig() Config {
	return Config{
		ElectionTicksMin: 10,
		ElectionTicksMax: 20,
		HeartbeatTicks:   2,
	}
}

// Snapshot is an opaque state-machine snapshot Node carries around
// without interpreting, exactly like LogEntry.Data. It only ever appears
// in Ready.InstalledSnapshot, when a peer's InstallSnapshot RPC replaced
// this node's log wholesale — never for a node's own CompactLog call,
// since that caller already knows what it just did.
type Snapshot struct {
	Index LogIndex
	Term  Term
	Data  []byte
}

// SnapshotBoundary reports the (index, term) a caller should hand to
// Storage.Compact — set on Ready() whenever the compaction point moved
// since the last call, whether from this node's own CompactLog or from
// installing a peer's snapshot.
type SnapshotBoundary struct {
	Index LogIndex
	Term  Term
}

// Ready bundles everything a driver needs to act on after ticking or
// stepping a Node: messages to send, log entries that just became
// committed, and (rarely) a freshly-installed snapshot the state machine
// must load wholesale. Mirrors etcd/raft's Ready() design for the same
// reason — it keeps Node itself free of any notion of "what do I do with
// committed data or snapshots," which is the caller's business
// (flowforge's, eventually).
type Ready struct {
	Messages          []Message
	CommittedEntries  []LogEntry
	InstalledSnapshot *Snapshot    // non-nil iff this node just adopted a peer's snapshot
	ReadyReads        []ReadResult // read requests now confirmed safe to serve

	// Persistence: everything below must be durably saved via Storage
	// BEFORE Messages is sent. Sending "I granted your vote" or "I
	// appended your entries" before that's actually durable is exactly
	// the crash-then-lie-on-restart scenario persistence exists to
	// prevent. See Storage's doc comment for the required call order
	// when both TruncateFrom and EntriesToSave are non-empty together.
	HardState        *HardState        // nil if term/votedFor didn't change since the last Ready()
	EntriesToSave    []LogEntry        // newly appended entries not yet known to be durable
	TruncateFrom     LogIndex          // >0 if a conflict discarded persisted entries from this index on
	SnapshotBoundary *SnapshotBoundary // non-nil if the compaction point moved since the last Ready()
}

// Node is the pure, deterministic core of one Raft participant. It owns
// no goroutines, sockets, or timers: callers drive it with Tick and Step
// and collect its effects with Ready. That separation is what makes the
// whole thing simulation-testable — the entire state machine is a
// function of (current state, ticks elapsed, messages received), so a
// test can compress a partition and a recovery into a few hundred Tick()
// calls with zero wall-clock time involved and byte-identical results run
// to run.
//
// Node is not safe for concurrent use. A single caller (or a driver that
// serializes access, e.g. one event-loop goroutine) owns it — same
// contract as etcd/raft.
type Node struct {
	id    NodeID
	peers []NodeID // does not include id
	cfg   Config
	rng   *rand.Rand

	role Role
	term Term

	votedFor NodeID
	haveVote bool

	// log[0] is always a sentinel representing the most recent
	// compaction point: {Term: snapshotTerm, Index: snapshotIndex}, or
	// {0, 0} if nothing has ever been compacted. Real entries (if any)
	// occupy log[1:], with log[i] holding Index == log[0].Index + i. Use
	// posOf/entryAt/entriesFrom rather than indexing log[] directly.
	log          []LogEntry
	snapshotData []byte

	commitIndex LogIndex
	lastApplied LogIndex

	// leader-only; reinitialized on every accession to leadership
	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex

	// departing tracks peers just dropped from n.peers by a committed
	// config change, mapped to info about that departure. A dropped peer
	// only has the ENTRY at this point, not proof it committed — without
	// this, it would never learn to retire and would eventually disrupt
	// the cluster via its own election timeout. Cleanup can't just check
	// "did their last ack's Index reach the target" — a follower's log
	// can match that index long before it KNOWS the entry is committed
	// (that only happens via LeaderCommit in a message sent after
	// departure began), so a stale ack from before it was marked
	// departing could satisfy that check by coincidence without the
	// peer having learned anything. notifiedFresh distinguishes a
	// genuine post-departure round trip from that stale-ack false
	// positive. Tracked regardless of current role, since leadership can
	// change while a departure is still unresolved.
	departing map[NodeID]*departureInfo

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	votesReceived map[NodeID]bool

	// leaderAlive is what makes PreVote actually work: true whenever
	// we've heard from a recognized leader recently enough to still
	// trust one exists, false once our own election timeout elapses
	// without hearing anything. A peer refuses a PreVote while this is
	// true, regardless of the requester's term or log — from where it's
	// standing, nothing is actually wrong.
	leaderAlive bool

	msgs []Message // outbound messages queued since the last Ready()

	// readSeq/pendingReads implement ReadIndex: the mechanism that makes
	// a plain read against the leader actually safe. Without it, a
	// leader that's been silently superseded (partitioned from the
	// majority, with a new leader already elected elsewhere) could serve
	// a read from stale local state — nothing about log replication
	// prevents that, since a read never touches the log at all. Every
	// RequestReadIndex call bumps readSeq and stamps it on the next
	// heartbeat round; a majority of followers echoing it back proves
	// this leader was still recognized, in this term, after the read was
	// requested — deliberately an exact, monotonic echo rather than an
	// inference from some other field's value, which is the mistake
	// departing-peer notification made and had to be corrected for.
	readSeq      uint64
	pendingReads []*pendingRead

	// transferTarget is non-empty while a leadership transfer is under
	// way (see TransferLeadership). Propose is refused during this
	// window — accepting new writes would only make the target's catch
	// up chase a moving finish line.
	transferTarget  NodeID
	transferElapsed int

	// Persistence bookkeeping: what the caller still needs to durably
	// save, in terms Ready() can report without Node touching IO itself.
	hardStateDirty        bool     // term or votedFor changed since the last Ready()
	persistedLogIndex     LogIndex // highest index the caller has already been told to persist
	truncatedSince        LogIndex // >0: entries at/after this index were discarded since the last Ready()
	reportedSnapshotIndex LogIndex // the snapshot boundary the caller was last told about
	pendingInstalled      *Snapshot
}

func NewNode(id NodeID, peers []NodeID, cfg Config, seed int64) *Node {
	n := &Node{
		id:    id,
		peers: peers,
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(seed)),
		role:  Follower,
		log:   []LogEntry{{Term: 0, Index: 0}}, // sentinel: nothing compacted yet
	}
	n.resetElectionTimeout()
	return n
}

// NewNodeFromStorage reconstructs a Node from previously-persisted state
// — the path a real process takes on restart, as opposed to NewNode's
// fresh-cluster-member path. snapshotData should be whatever the state
// machine itself durably holds for the snapshot at (snapshotIndex,
// snapshotTerm) — Storage's WAL never stores the bytes themselves, only
// the boundary, so re-supplying them here is what lets a restarted node
// still serve InstallSnapshot to a lagging peer immediately. commitIndex
// is initialized to snapshotIndex (compaction is proof that far was
// committed); anything beyond it is safely rediscovered from the current
// leader's next heartbeat, or from this node's own majority-replication
// bookkeeping if it re-becomes leader (see becomeLeader's no-op).
// appliedIndex lets the caller skip re-delivering entries its state
// machine already durably applied before the crash; pass snapshotIndex
// to resume exactly where a snapshot-aware state machine left off, or 0
// to replay everything the restored log has to offer.
func NewNodeFromStorage(id NodeID, peers []NodeID, cfg Config, seed int64, term Term, votedFor NodeID, snapshotIndex LogIndex, snapshotTerm Term, log []LogEntry, snapshotData []byte, appliedIndex LogIndex) *Node {
	n := NewNode(id, peers, cfg, seed)
	n.term = term
	n.votedFor = votedFor
	n.haveVote = votedFor != ""
	n.log = append([]LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}, log...)
	n.snapshotData = snapshotData
	n.commitIndex = snapshotIndex
	n.lastApplied = appliedIndex
	n.persistedLogIndex = n.LastIndex() // everything we just loaded is already durable
	n.hardStateDirty = false            // ditto — what we loaded IS what's on disk
	n.reportedSnapshotIndex = snapshotIndex
	return n
}

func (n *Node) ID() NodeID { return n.id }
func (n *Node) Role() Role { return n.role }
func (n *Node) Term() Term { return n.term }

func (n *Node) LastIndex() LogIndex     { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() Term          { return n.log[len(n.log)-1].Term }
func (n *Node) snapshotIndex() LogIndex { return n.log[0].Index }
func (n *Node) snapshotTerm() Term      { return n.log[0].Term }

func (n *Node) posOf(idx LogIndex) int { return int(idx - n.log[0].Index) }

// entryAt returns the real entry at idx. The sentinel itself (idx ==
// snapshotIndex) is deliberately not "found" here — callers that need to
// treat the boundary as a match point check snapshotIndex/snapshotTerm
// directly, since the sentinel isn't a real entry with real Data.
func (n *Node) entryAt(idx LogIndex) (LogEntry, bool) {
	pos := n.posOf(idx)
	if pos <= 0 || pos >= len(n.log) {
		return LogEntry{}, false
	}
	return n.log[pos], true
}

func (n *Node) entriesFrom(idx LogIndex) []LogEntry {
	pos := n.posOf(idx)
	if pos <= 0 || pos > len(n.log) {
		return nil
	}
	return n.log[pos:]
}

// CompactLog discards every log entry at or before lastIncludedIndex,
// keeping only a sentinel with its index and term. The caller must
// already have durably saved its own state-machine snapshot covering
// everything through lastIncludedIndex before calling this — Node has no
// way to verify that and trusts the caller here exactly as it trusts the
// caller's Storage writes to have actually completed before Messages are
// sent. Returns the term at lastIncludedIndex (which the caller then
// hands to Storage.Compact) so the caller never has to separately track
// it.
func (n *Node) CompactLog(lastIncludedIndex LogIndex, snapshotData []byte) (Term, error) {
	if lastIncludedIndex <= n.snapshotIndex() {
		return n.snapshotTerm(), nil // already compacted at least this far — a no-op
	}
	if lastIncludedIndex > n.commitIndex {
		return 0, ErrCompactUncommitted
	}
	pos := n.posOf(lastIncludedIndex)
	if pos <= 0 || pos >= len(n.log) {
		return 0, ErrCompactUnknownIndex
	}
	term := n.log[pos].Term
	n.log = append([]LogEntry{{Term: term, Index: lastIncludedIndex}}, n.log[pos+1:]...)
	n.snapshotData = snapshotData
	return term, nil
}

// Ready drains and returns messages queued for delivery, plus any log
// entries that committed, or a snapshot that got installed, since the
// last call. The caller — a real network driver, or the deterministic
// simulator in sim/ — is responsible for actually routing messages,
// applying entries/snapshots, and persisting what's reported.
func (n *Node) Ready() Ready {
	var committed []LogEntry
	for idx := n.lastApplied + 1; idx <= n.commitIndex; idx++ {
		if e, ok := n.entryAt(idx); ok {
			committed = append(committed, e)
			if e.Type == EntryConfigChange {
				n.applyMembership(decodeMembership(e.Data), e.Index)
			}
		}
	}
	n.lastApplied = n.commitIndex

	readyReads := n.checkPendingReads()

	installed := n.pendingInstalled
	n.pendingInstalled = nil

	var hs *HardState
	if n.hardStateDirty {
		hs = &HardState{Term: n.term, VotedFor: n.votedFor}
		n.hardStateDirty = false
	}

	var truncateFrom LogIndex
	if n.truncatedSince > 0 {
		truncateFrom = n.truncatedSince
		if n.persistedLogIndex >= n.truncatedSince {
			n.persistedLogIndex = n.truncatedSince - 1
		}
		n.truncatedSince = 0
	}

	var toSave []LogEntry
	if n.persistedLogIndex < n.LastIndex() {
		toSave = n.entriesFrom(n.persistedLogIndex + 1)
		n.persistedLogIndex = n.LastIndex()
	}

	var boundary *SnapshotBoundary
	if n.snapshotIndex() != n.reportedSnapshotIndex {
		boundary = &SnapshotBoundary{Index: n.snapshotIndex(), Term: n.snapshotTerm()}
		n.reportedSnapshotIndex = n.snapshotIndex()
	}

	msgs := n.msgs
	n.msgs = nil
	return Ready{
		Messages:          msgs,
		CommittedEntries:  committed,
		InstalledSnapshot: installed,
		ReadyReads:        readyReads,
		HardState:         hs,
		EntriesToSave:     toSave,
		TruncateFrom:      truncateFrom,
		SnapshotBoundary:  boundary,
	}
}

func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) resetElectionTimeout() {
	spread := n.cfg.ElectionTicksMax - n.cfg.ElectionTicksMin
	n.electionTimeout = n.cfg.ElectionTicksMin
	if spread > 0 {
		n.electionTimeout += n.rng.Intn(spread)
	}
	n.electionElapsed = 0
}

// Tick advances logical time by one unit. Every node in a cluster is
// ticked at the same logical rate; the randomized election timeout is
// what keeps two nodes from perpetually tying (§5.2).
func (n *Node) Tick() {
	switch n.role {
	case Removed:
		return // permanently retired: no elections, no heartbeats, nothing
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.replicateToAll()
		}
		if n.transferTarget != "" {
			n.transferElapsed++
			if n.transferElapsed >= n.cfg.ElectionTicksMax*2 {
				// The target never caught up in a reasonable window —
				// abort rather than block writes indefinitely.
				n.transferTarget = ""
				n.transferElapsed = 0
			}
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.leaderAlive = false
			if n.cfg.PreVote && n.role != Candidate {
				n.startPreVote()
			} else {
				n.startElection()
			}
		}
	}
}

// startPreVote checks whether an election would actually be winnable
// without committing to one — no term increment, no real candidacy yet.
// Peers grant a prevote using the same log up-to-date rule as a real
// vote, but only if they don't currently believe a leader is alive. An
// isolated node's prevote requests just get refused (or never reach
// anyone), so it keeps retrying this phase forever without its real
// term ever moving — which is the entire point.
func (n *Node) startPreVote() {
	n.role = PreCandidate
	n.resetElectionTimeout()
	n.votesReceived = map[NodeID]bool{n.id: true}

	if len(n.peers) == 0 {
		n.startElection() // nothing to prevote against; a real election resolves instantly anyway
		return
	}
	prospectiveTerm := n.term + 1
	for _, p := range n.peers {
		// Deliberately not going through send(), which stamps our
		// current term onto every message — a prevote must carry the
		// term we'd move to if we proceed, without us adopting it now.
		n.msgs = append(n.msgs, Message{
			Type: MsgPreVoteRequest, From: n.id, To: p, Term: prospectiveTerm,
			LastLogIndex: n.LastIndex(), LastLogTerm: n.lastTerm(),
		})
	}
}

// Propose appends data to the log if this node is currently the leader
// and kicks off replication immediately rather than waiting for the next
// heartbeat tick. It returns the (index, term) the entry was proposed
// at — the caller can later confirm whether it actually committed by
// watching CommittedEntries, since leadership (and therefore the fate of
// unreplicated entries) can still change before that happens.
func (n *Node) Propose(data []byte) (LogIndex, Term, error) {
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if n.transferTarget != "" {
		return 0, 0, ErrLeadershipTransferInProgress
	}
	entry := LogEntry{Term: n.term, Index: n.LastIndex() + 1, Data: data}
	n.log = append(n.log, entry)
	n.advanceCommitIndex() // covers the single-node-cluster immediate-commit case
	n.replicateToAll()
	return entry.Index, entry.Term, nil
}

func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.haveVote = true
	n.hardStateDirty = true
	n.votesReceived = map[NodeID]bool{n.id: true}
	n.resetElectionTimeout()

	if len(n.peers) == 0 {
		// Single-node cluster: a majority of one is already met.
		n.becomeLeader()
		return
	}

	for _, p := range n.peers {
		n.send(Message{
			Type:         MsgVoteRequest,
			To:           p,
			LastLogIndex: n.LastIndex(),
			LastLogTerm:  n.lastTerm(),
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.nextIndex = map[NodeID]LogIndex{}
	n.matchIndex = map[NodeID]LogIndex{}
	for _, p := range n.peers {
		n.nextIndex[p] = n.LastIndex() + 1
		n.matchIndex[p] = 0
	}
	for p := range n.departing {
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = n.LastIndex() + 1
			n.matchIndex[p] = 0
		}
	}
	n.heartbeatElapsed = 0

	// §5.4.2's term-restriction on committing is correct, but it has a
	// consequence worth handling deliberately: entries inherited from a
	// previous leader/term can't be confirmed committed by this leader
	// until SOME entry from its own current term commits — and if no
	// client ever writes anything new, that might be never. A no-op
	// entry (empty Data) gives that a term-current entry to piggyback on
	// immediately, at the cost of one harmless log slot the state
	// machine should treat as a skip.
	n.log = append(n.log, LogEntry{Term: n.term, Index: n.LastIndex() + 1})
	n.advanceCommitIndex()
	n.replicateToAll()
}

// replicateTo sends peer whatever entries it's missing, based on our
// current belief of its nextIndex. With no new entries (peer already
// caught up) this degenerates into a plain heartbeat. If the peer needs
// entries we no longer have as discrete records — they've been folded
// into our snapshot — a normal AppendEntries can't express that, so a
// snapshot goes instead.
func (n *Node) replicateTo(peer NodeID) {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	if next <= n.snapshotIndex() {
		n.sendSnapshot(peer)
		return
	}

	prevIndex := next - 1
	var prevTerm Term
	if prevIndex == n.snapshotIndex() {
		prevTerm = n.snapshotTerm()
	} else if e, ok := n.entryAt(prevIndex); ok {
		prevTerm = e.Term
	}
	n.send(Message{
		Type:         MsgAppendEntries,
		To:           peer,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      n.entriesFrom(next),
		LeaderCommit: n.commitIndex,
		ReadSeq:      n.readSeq,
	})
}

func (n *Node) sendSnapshot(peer NodeID) {
	n.send(Message{
		Type:              MsgInstallSnapshot,
		To:                peer,
		LastIncludedIndex: n.snapshotIndex(),
		LastIncludedTerm:  n.snapshotTerm(),
		SnapshotData:      n.snapshotData,
	})
}

func (n *Node) replicateToAll() {
	for _, p := range n.peers {
		n.replicateTo(p)
	}
	for p, info := range n.departing {
		n.replicateTo(p)
		info.notifiedFresh = true
	}
}

// advanceCommitIndex re-checks, after any matchIndex update, whether a
// new log position has been replicated to a majority of the cluster.
// §5.4.2's critical restriction: a leader may only commit an entry this
// way if it's from its OWN current term. Directly committing an entry
// from an older term just because a majority now happens to have it is
// the exact unsafety the paper's Figure 8 warns about — a later leader,
// elected fair and square, can still legitimately overwrite it. Once a
// current-term entry does commit, everything at or before it commits too
// as a side effect (log entries commit as a prefix, never piecemeal).
func (n *Node) advanceCommitIndex() {
	if n.role != Leader {
		return
	}
	matches := make([]LogIndex, 0, len(n.peers)+1)
	matches = append(matches, n.LastIndex()) // the leader always matches itself
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[len(matches)/2] // highest index a majority has reached

	if candidate > n.commitIndex {
		if entry, ok := n.entryAt(candidate); ok && entry.Term == n.term {
			n.commitTo(candidate)
		}
	}
}

func (n *Node) commitTo(idx LogIndex) {
	if idx > n.commitIndex {
		n.commitIndex = idx
	}
}

func (n *Node) becomeFollower(term Term) {
	n.role = Follower
	n.term = term
	n.votedFor = ""
	n.haveVote = false
	n.nextIndex = nil
	n.matchIndex = nil
	n.pendingReads = nil
	n.transferTarget = ""
	n.transferElapsed = 0
	n.hardStateDirty = true
	n.resetElectionTimeout()
}

// Step processes one incoming message and updates state accordingly.
func (n *Node) Step(m Message) {
	if n.role == Removed {
		return // retired: doesn't vote, doesn't ack, doesn't do anything
	}

	// Rules for Servers (all servers): any RPC carrying a newer term
	// forces an immediate, unconditional step-down before the specific
	// handler runs — except PreVote messages, which deliberately carry a
	// prospective term nobody has actually adopted yet. Applying the
	// normal rule to them would mean merely being asked bumps our term,
	// which defeats the entire purpose of asking first.
	if m.Term > n.term && m.Type != MsgPreVoteRequest && m.Type != MsgPreVoteResponse {
		n.becomeFollower(m.Term)
	}

	switch m.Type {
	case MsgVoteRequest:
		n.handleVoteRequest(m)
	case MsgVoteResponse:
		n.handleVoteResponse(m)
	case MsgAppendEntries:
		n.handleAppendEntries(m)
	case MsgAppendResponse:
		n.handleAppendResponse(m)
	case MsgInstallSnapshot:
		n.handleInstallSnapshot(m)
	case MsgInstallSnapshotResponse:
		n.handleInstallSnapshotResponse(m)
	case MsgPreVoteRequest:
		n.handlePreVoteRequest(m)
	case MsgPreVoteResponse:
		n.handlePreVoteResponse(m)
	case MsgTimeoutNow:
		n.handleTimeoutNow(m)
	}
}

func (n *Node) handleVoteRequest(m Message) {
	grant := false
	switch {
	case m.Term < n.term:
		grant = false
	case n.haveVote && n.votedFor != m.From:
		grant = false
	default:
		// §5.4.1: grant only if the candidate's log is at least as
		// up-to-date as ours — higher last-log term wins outright; a tie
		// goes to whoever has the longer log.
		upToDate := m.LastLogTerm > n.lastTerm() ||
			(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.LastIndex())
		if upToDate {
			grant = true
			n.votedFor = m.From
			n.haveVote = true
			n.hardStateDirty = true
			// Granting a vote counts as hearing from a live peer.
			n.resetElectionTimeout()
		}
	}
	n.send(Message{Type: MsgVoteResponse, To: m.From, VoteGranted: grant})
}

func (n *Node) handleVoteResponse(m Message) {
	if n.role != Candidate || m.Term != n.term || !m.VoteGranted {
		return
	}
	n.votesReceived[m.From] = true
	if len(n.votesReceived) > (len(n.peers)+1)/2 {
		n.becomeLeader()
	}
}

// handlePreVoteRequest grants only if we don't currently trust a leader
// is alive AND the requester's log is at least as up-to-date as ours —
// the same §5.4.1 rule a real vote uses. Refusing whenever leaderAlive
// is true regardless of log currency is what actually stops an isolated
// node from ever winning a majority of prevotes while the rest of the
// cluster is healthy.
func (n *Node) handlePreVoteRequest(m Message) {
	grant := false
	if !n.leaderAlive && m.Term > n.term {
		grant = m.LastLogTerm > n.lastTerm() ||
			(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.LastIndex())
	}
	// Deliberately not going through send(): the response should carry
	// our real current term (informational, so a stale requester can
	// tell), not have our term stamped to something we haven't adopted.
	n.msgs = append(n.msgs, Message{
		Type: MsgPreVoteResponse, From: n.id, To: m.From, Term: n.term, VoteGranted: grant,
	})
}

func (n *Node) handlePreVoteResponse(m Message) {
	if n.role != PreCandidate || !m.VoteGranted {
		return
	}
	n.votesReceived[m.From] = true
	if len(n.votesReceived) > (len(n.peers)+1)/2 {
		n.startElection() // confirmed a majority would actually back us — now do it for real
	}
}

func (n *Node) handleAppendEntries(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResponse, To: m.From, Success: false})
		return
	}
	// A valid leader for our term (or higher, already handled above):
	// recognize it and reset our election clock. Covers both the
	// Candidate->Follower demotion (§5.2) and the plain Follower
	// heartbeat case.
	n.role = Follower
	n.leaderAlive = true
	n.resetElectionTimeout()

	switch {
	case m.PrevLogIndex < n.snapshotIndex():
		// We've already compacted past what the leader thinks we have —
		// tell it we're actually caught up to our snapshot boundary
		// instead of incorrectly reporting a conflict for data that was
		// never wrong, just already folded away.
		n.send(Message{Type: MsgAppendResponse, To: m.From, Success: true, Index: n.snapshotIndex(), ReadSeq: m.ReadSeq})
		return
	case m.PrevLogIndex > n.snapshotIndex():
		entry, ok := n.entryAt(m.PrevLogIndex)
		if !ok {
			n.send(Message{
				Type: MsgAppendResponse, To: m.From, Success: false,
				ConflictIndex: n.LastIndex() + 1,
			})
			return
		}
		if entry.Term != m.PrevLogTerm {
			// We have something there, but from a different term — hand
			// back the whole conflicting term's range so the leader can
			// skip it in one round trip instead of decrementing by one.
			conflictTerm := entry.Term
			firstIdx := m.PrevLogIndex
			for firstIdx > n.snapshotIndex()+1 {
				if e, ok := n.entryAt(firstIdx - 1); ok && e.Term == conflictTerm {
					firstIdx--
				} else {
					break
				}
			}
			n.send(Message{
				Type: MsgAppendResponse, To: m.From, Success: false,
				ConflictIndex: firstIdx, ConflictTerm: conflictTerm,
			})
			return
		}
		// case m.PrevLogIndex == n.snapshotIndex(): the boundary is
		// definitionally where we and the leader agree — fall through.
	}

	// §5.3: reconcile our log with the leader's. An entry already
	// present and matching is left alone (this is what makes the RPC
	// idempotent under retries); a conflicting entry (same index,
	// different term) and everything after it gets discarded; anything
	// genuinely new gets appended.
	insertAt := m.PrevLogIndex + 1
	for i, e := range m.Entries {
		idx := insertAt + LogIndex(i)
		if existing, ok := n.entryAt(idx); ok {
			if existing.Term == e.Term {
				continue
			}
			if n.truncatedSince == 0 || idx < n.truncatedSince {
				n.truncatedSince = idx
			}
			n.log = n.log[:n.posOf(idx)]
			n.log = append(n.log, m.Entries[i:]...)
			break
		}
		n.log = append(n.log, m.Entries[i:]...)
		break
	}

	if m.LeaderCommit > n.commitIndex {
		newCommit := m.LeaderCommit
		if lastNew := m.PrevLogIndex + LogIndex(len(m.Entries)); lastNew < newCommit {
			newCommit = lastNew
		}
		n.commitTo(newCommit)
	}

	n.send(Message{
		Type: MsgAppendResponse, To: m.From, Success: true,
		Index: m.PrevLogIndex + LogIndex(len(m.Entries)), ReadSeq: m.ReadSeq,
	})
}

func (n *Node) handleAppendResponse(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if m.Success {
		if m.Index > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.Index
		}
		n.nextIndex[m.From] = m.Index + 1
		n.confirmPendingReads(m.From, m.ReadSeq)
		if n.transferTarget == m.From && n.matchIndex[m.From] >= n.LastIndex() {
			n.sendTimeoutNow(m.From)
			n.transferTarget = ""
			n.transferElapsed = 0
		}
		if info, ok := n.departing[m.From]; ok && info.notifiedFresh && m.Index >= info.targetIndex {
			// Only a genuine post-departure round trip counts — an ack
			// merely proving old log-matching, from before we ever sent
			// them anything carrying the commit, would satisfy the index
			// check by coincidence without them having learned anything.
			delete(n.departing, m.From)
			delete(n.nextIndex, m.From)
			delete(n.matchIndex, m.From)
		}
		n.advanceCommitIndex()
		return
	}
	// Failure: back up nextIndex using the follower's conflict hint so a
	// long divergence resolves in one round trip rather than one index
	// at a time.
	switch {
	case m.ConflictTerm != 0:
		newNext := m.ConflictIndex
		for i := len(n.log) - 1; i >= 0; i-- {
			if n.log[i].Term == m.ConflictTerm {
				newNext = n.log[i].Index + 1
				break
			}
		}
		n.nextIndex[m.From] = newNext
	case m.ConflictIndex > 0:
		n.nextIndex[m.From] = m.ConflictIndex
	default:
		if n.nextIndex[m.From] > 1 {
			n.nextIndex[m.From]--
		}
	}
	n.replicateTo(m.From) // retry immediately rather than waiting for the next heartbeat
}

func (n *Node) handleInstallSnapshot(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgInstallSnapshotResponse, To: m.From, Success: false})
		return
	}
	n.role = Follower
	n.leaderAlive = true
	n.resetElectionTimeout()

	if m.LastIncludedIndex <= n.snapshotIndex() {
		// We're already at least this far along — just ack where we are.
		n.send(Message{Type: MsgInstallSnapshotResponse, To: m.From, Success: true, Index: n.snapshotIndex()})
		return
	}

	// If we already have an entry at the boundary that agrees on term,
	// keep whatever we have beyond it — no need to discard log we've
	// already correctly replicated just because a snapshot arrived.
	// Otherwise the snapshot replaces our entire log wholesale.
	var tail []LogEntry
	if e, ok := n.entryAt(m.LastIncludedIndex); ok && e.Term == m.LastIncludedTerm {
		tail = append([]LogEntry(nil), n.entriesFrom(m.LastIncludedIndex+1)...)
	}
	n.log = append([]LogEntry{{Term: m.LastIncludedTerm, Index: m.LastIncludedIndex}}, tail...)
	n.snapshotData = m.SnapshotData
	n.commitTo(m.LastIncludedIndex)
	if m.LastIncludedIndex > n.lastApplied {
		// The snapshot itself represents everything through here as
		// already applied by whoever produced it — installing it means
		// this node's state machine adopts it wholesale rather than
		// replaying individual entries that no longer exist.
		n.lastApplied = m.LastIncludedIndex
	}
	n.pendingInstalled = &Snapshot{Index: m.LastIncludedIndex, Term: m.LastIncludedTerm, Data: m.SnapshotData}

	n.send(Message{Type: MsgInstallSnapshotResponse, To: m.From, Success: true, Index: m.LastIncludedIndex})
}

func (n *Node) handleInstallSnapshotResponse(m Message) {
	if n.role != Leader || m.Term != n.term || !m.Success {
		return
	}
	if m.Index > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.Index
	}
	n.nextIndex[m.From] = m.Index + 1
	if info, ok := n.departing[m.From]; ok && info.notifiedFresh && m.Index >= info.targetIndex {
		delete(n.departing, m.From)
		delete(n.nextIndex, m.From)
		delete(n.matchIndex, m.From)
	}
	n.advanceCommitIndex()
}
