package quorum

import (
	"encoding/binary"
	"errors"
)

// departureInfo tracks one peer removed from the cluster that hasn't yet
// been confirmed to know it. See Node.departing's doc comment for why
// notifiedFresh (not just the ack's Index) is what gates cleanup.
type departureInfo struct {
	targetIndex   LogIndex
	notifiedFresh bool
}

// ErrConfigChangeInProgress is returned by ProposeConfigChange when a
// previous config change hasn't committed yet. Changes must be
// serialized — proposing a second one before the first commits is
// exactly the multi-step scenario joint consensus exists to handle, and
// the single-node-at-a-time restriction is what lets this implementation
// skip that complexity entirely.
var ErrConfigChangeInProgress = errors.New("quorum: a configuration change is already in flight")

// ErrConfigChangeNotSingleNode is returned when the proposed membership
// differs from the current one by more than one added or removed server.
// This restriction — never joint consensus's two-phase protocol — is
// what guarantees the old and new majorities always overlap by at least
// one server: majorities of cluster sizes differing by at most one
// always intersect, so there's never a moment where two disjoint
// majorities could both elect a leader or commit conflicting entries.
var ErrConfigChangeNotSingleNode = errors.New("quorum: configuration changes must add or remove exactly one server at a time")

// ProposeConfigChange replicates a new cluster membership the same way
// Propose replicates client data — it only takes effect once committed
// (not merely appended), which keeps the safety argument simple: every
// node still uses its old membership for every majority calculation
// right up until its own commitIndex crosses this entry, at which point
// Ready() applies the switch. There's no window where two different
// notions of "the cluster" are both live for majority-counting purposes.
//
// fullMembership must list every server that should be in the cluster
// after the change, including this node itself if it's staying. It must
// differ from the current membership by exactly one server.
func (n *Node) ProposeConfigChange(fullMembership []NodeID) (LogIndex, Term, error) {
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if n.configChangeInFlight() {
		return 0, 0, ErrConfigChangeInProgress
	}
	if !isSingleNodeDiff(n.currentMembership(), fullMembership) {
		return 0, 0, ErrConfigChangeNotSingleNode
	}
	entry := LogEntry{
		Term: n.term, Index: n.LastIndex() + 1,
		Type: EntryConfigChange, Data: encodeMembership(fullMembership),
	}
	n.log = append(n.log, entry)
	n.advanceCommitIndex() // single-node-cluster edge case, same as Propose
	n.replicateToAll()
	return entry.Index, entry.Term, nil
}

// currentMembership is n.peers plus this node itself — the inverse of
// what applyMembership computes when a config change commits.
func (n *Node) currentMembership() []NodeID {
	return append([]NodeID{n.id}, n.peers...)
}

// configChangeInFlight scans the uncommitted tail of the log for a
// config-change entry that hasn't resolved yet.
func (n *Node) configChangeInFlight() bool {
	for idx := n.commitIndex + 1; idx <= n.LastIndex(); idx++ {
		if e, ok := n.entryAt(idx); ok && e.Type == EntryConfigChange {
			return true
		}
	}
	return false
}

// applyMembership is invoked from Ready() the moment a config-change
// entry crosses into committed territory — never on mere append, which
// is what keeps every node's majority math unambiguous throughout.
// changeIndex is that entry's own index — what a departing peer needs to
// have matched before the leader can stop notifying it (see the
// departing field's doc comment on Node for why this exists at all).
func (n *Node) applyMembership(fullMembership []NodeID, changeIndex LogIndex) {
	newMemberSet := map[NodeID]bool{}
	var newPeers []NodeID
	stillMember := false
	for _, id := range fullMembership {
		if id == n.id {
			stillMember = true
			continue
		}
		newPeers = append(newPeers, id)
		newMemberSet[id] = true
	}

	// Anyone in our OLD peers list who isn't in the new membership is
	// being removed. Tracked regardless of current role: if we become
	// leader later while this is still unresolved, we need to remember
	// there's a peer that hasn't yet learned it's been removed.
	for _, oldPeer := range n.peers {
		if !newMemberSet[oldPeer] {
			if n.departing == nil {
				n.departing = map[NodeID]*departureInfo{}
			}
			if _, already := n.departing[oldPeer]; !already {
				n.departing[oldPeer] = &departureInfo{targetIndex: changeIndex}
			}
		}
	}

	if n.role == Leader {
		// Carry forward what we already know about surviving peers and
		// anyone still pending departure notification; give a
		// newly-added peer a fresh, correct starting point rather than
		// relying on a map's zero value to happen to be right.
		newNextIndex := map[NodeID]LogIndex{}
		newMatchIndex := map[NodeID]LogIndex{}
		for _, p := range newPeers {
			if v, ok := n.nextIndex[p]; ok {
				newNextIndex[p] = v
			} else {
				newNextIndex[p] = n.LastIndex() + 1
			}
			newMatchIndex[p] = n.matchIndex[p]
		}
		for p := range n.departing {
			if _, ok := newNextIndex[p]; !ok {
				newNextIndex[p] = n.nextIndex[p]
				newMatchIndex[p] = n.matchIndex[p]
			}
		}
		n.nextIndex, n.matchIndex = newNextIndex, newMatchIndex
	}

	n.peers = newPeers

	if !stillMember {
		// Removed ourselves: nothing left to lead or vote in, and
		// critically, nothing left to ever elect ourselves for either —
		// Removed (not Follower) is what actually stops the election
		// timer from mattering. Clearing peers and departing too, since
		// there's nothing stale left to act on even if something got
		// this wrong. (Self-removal is deliberately not the focus of
		// this implementation's test coverage — the common,
		// load-bearing cases are adding a server and removing some
		// *other* server, which is what's exercised end-to-end.)
		n.role = Removed
		n.peers = nil
		n.departing = nil
	}
}

func isSingleNodeDiff(current, proposed []NodeID) bool {
	currentSet := map[NodeID]bool{}
	for _, id := range current {
		currentSet[id] = true
	}
	proposedSet := map[NodeID]bool{}
	for _, id := range proposed {
		proposedSet[id] = true
	}
	diff := 0
	for id := range currentSet {
		if !proposedSet[id] {
			diff++
		}
	}
	for id := range proposedSet {
		if !currentSet[id] {
			diff++
		}
	}
	return diff == 1
}

func encodeMembership(ids []NodeID) []byte {
	var buf []byte
	for _, id := range ids {
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(id)))
		buf = append(buf, lenBuf...)
		buf = append(buf, []byte(id)...)
	}
	return buf
}

func decodeMembership(b []byte) []NodeID {
	var ids []NodeID
	for len(b) >= 4 {
		n := binary.LittleEndian.Uint32(b[0:4])
		b = b[4:]
		if uint32(len(b)) < n {
			break // corrupt/truncated — stop rather than panic
		}
		ids = append(ids, NodeID(b[:n]))
		b = b[n:]
	}
	return ids
}
