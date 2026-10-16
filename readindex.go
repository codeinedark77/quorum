package quorum

// pendingRead is one outstanding RequestReadIndex call, waiting on a
// majority of followers to prove — via echoing seq back in a fresh
// AppendResponse — that this leader was still recognized, in this term,
// at some point after the read was requested.
type pendingRead struct {
	id          uint64
	seq         uint64
	targetIndex LogIndex
	confirmed   map[NodeID]bool
}

// ReadResult reports one read request that's now safe to serve: a
// majority has confirmed this leader is still legitimate as of after the
// request, AND this node's own state machine has been handed everything
// through Index (via CommittedEntries) — so reading local state right
// now correctly reflects at least everything committed as of the request.
type ReadResult struct {
	ID    uint64
	Index LogIndex
}

// RequestReadIndex begins confirming that a linearizable read is safe to
// serve right now. It does not perform the read itself — Node has no
// state machine to read from — it only tells the caller *when* it's
// safe to read its own local state and trust the answer. The result
// surfaces asynchronously via Ready().ReadyReads once confirmed; there's
// no explicit failure signal if this leader steps down before that
// happens (pending reads are simply dropped — see becomeFollower), so a
// caller should apply its own timeout and retry against whichever leader
// is current if one doesn't resolve promptly.
func (n *Node) RequestReadIndex() (uint64, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	n.readSeq++
	id := n.readSeq
	n.pendingReads = append(n.pendingReads, &pendingRead{
		id: id, seq: n.readSeq, targetIndex: n.commitIndex,
		confirmed: map[NodeID]bool{n.id: true},
	})
	n.replicateToAll() // solicit fresh confirmations now rather than waiting for the next scheduled heartbeat
	return id, nil
}

// checkPendingReads is called from Ready(), after CommittedEntries has
// already advanced lastApplied for this call, to see which pending reads
// have now cleared both bars: a real majority confirmed, and this node's
// own state genuinely caught up to what it needs to.
func (n *Node) checkPendingReads() []ReadResult {
	if len(n.pendingReads) == 0 {
		return nil
	}
	majority := (len(n.peers)+1)/2 + 1
	var ready []ReadResult
	var stillPending []*pendingRead
	for _, pr := range n.pendingReads {
		if len(pr.confirmed) >= majority && n.lastApplied >= pr.targetIndex {
			ready = append(ready, ReadResult{ID: pr.id, Index: pr.targetIndex})
		} else {
			stillPending = append(stillPending, pr)
		}
	}
	n.pendingReads = stillPending
	return ready
}

// confirmPendingReads marks peer as having proven, via a fresh echo of
// seq, that it still recognizes this leader. Any pending read whose own
// seq is at or before this one is provably confirmed by this ack: seq is
// monotonic and stamped fresh on every outgoing message at send time, so
// an echo of seq >= pr.seq could only have come from a message sent at or
// after pr.seq was assigned — which happened at or after that read was
// requested. An echo of a lower seq (a genuinely older, in-flight
// message) correctly does not count.
func (n *Node) confirmPendingReads(peer NodeID, seq uint64) {
	for _, pr := range n.pendingReads {
		if pr.seq <= seq {
			pr.confirmed[peer] = true
		}
	}
}
