package quorum

import "errors"

// ErrTransferUnknownTarget is returned by TransferLeadership when the
// named target isn't a current peer.
var ErrTransferUnknownTarget = errors.New("quorum: transfer target is not a current peer")

// ErrTransferInProgress is returned by TransferLeadership when another
// transfer hasn't finished (or timed out) yet.
var ErrTransferInProgress = errors.New("quorum: a leadership transfer is already in progress")

// ErrLeadershipTransferInProgress is returned by Propose while a
// transfer is under way — see Node.transferTarget's doc comment.
var ErrLeadershipTransferInProgress = errors.New("quorum: cannot propose while a leadership transfer is in progress")

// TransferLeadership begins gracefully handing leadership to target —
// useful for planned maintenance or rebalancing, where forcing an
// election via a crash or partition is unnecessarily disruptive. It's
// only safe to install a new leader whose log is fully caught up (§5.4.1
// would refuse to elect one that isn't anyway, but attempting a doomed
// election is pure waste); if target isn't caught up yet, Propose is
// refused in the meantime so the target isn't chasing a moving target,
// and TimeoutNow fires the instant it catches up. If it never does
// within a generous window, the attempt aborts and normal operation
// resumes — see the transferElapsed check in Tick.
func (n *Node) TransferLeadership(target NodeID) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	if target == n.id {
		return nil
	}
	found := false
	for _, p := range n.peers {
		if p == target {
			found = true
			break
		}
	}
	if !found {
		return ErrTransferUnknownTarget
	}
	if n.transferTarget != "" {
		return ErrTransferInProgress
	}

	n.transferTarget = target
	n.transferElapsed = 0
	if n.matchIndex[target] >= n.LastIndex() {
		n.sendTimeoutNow(target)
		n.transferTarget = ""
		return nil
	}
	n.replicateToAll() // don't wait for the next scheduled heartbeat to start closing the gap
	return nil
}

func (n *Node) sendTimeoutNow(target NodeID) {
	n.send(Message{Type: MsgTimeoutNow, To: target})
}

// handleTimeoutNow starts a real election immediately, bypassing both
// the normal election-timeout wait and PreVote even if enabled — there's
// no ambiguity to resolve here about whether a leader is alive; the
// current one, which we just heard from, explicitly invited this.
func (n *Node) handleTimeoutNow(m Message) {
	if m.Term < n.term {
		return // stale, from a leader we've already moved past
	}
	n.startElection()
}
