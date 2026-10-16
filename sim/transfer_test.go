package sim_test

import (
	"testing"

	"quorum"
)

// TestLeadershipTransferHandsOffToSpecificTarget is the actual point:
// request a transfer to a named follower and confirm THAT follower —
// not some other node, not the same leader via re-election — becomes
// the new leader, with the old leader correctly recognizing and
// stepping down rather than needing a disruptive election to force it.
func TestLeadershipTransferHandsOffToSpecificTarget(t *testing.T) {
	net, ids := newCluster(5, 5000)
	settle(t, net, 200)
	oldLeader := requireExactlyOneLeader(t, net)
	oldLeaderTermBefore := oldLeader.Term()

	// A few committed writes first, so the target has real work to be
	// caught up on, not just the initial no-op.
	for i := 0; i < 3; i++ {
		if _, _, err := net.Node(oldLeader.ID()).Propose([]byte("w")); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
	}
	net.TickN(30)

	var target quorum.NodeID
	for _, id := range ids {
		if id != oldLeader.ID() {
			target = id
			break
		}
	}

	if err := net.Node(oldLeader.ID()).TransferLeadership(target); err != nil {
		t.Fatalf("TransferLeadership failed: %v", err)
	}
	net.TickN(50)

	newLeader := requireExactlyOneLeader(t, net)
	if newLeader.ID() != target {
		t.Fatalf("expected leadership to transfer specifically to %s, got %s", target, newLeader.ID())
	}
	if newLeader.Term() <= oldLeaderTermBefore {
		t.Fatalf("expected the new leader's term to have advanced past the pre-transfer term, got %d (was %d)",
			newLeader.Term(), oldLeaderTermBefore)
	}

	// Confirm the new leader is fully functional: further writes commit
	// and apply everywhere.
	if _, _, err := net.Node(target).Propose([]byte("after-transfer")); err != nil {
		t.Fatalf("Propose on the new leader failed: %v", err)
	}
	net.TickN(30)
	for _, id := range ids {
		applied := nonNoop(net.Applied(id))
		if len(applied) == 0 || string(applied[len(applied)-1].Data) != "after-transfer" {
			t.Fatalf("node %s: expected the post-transfer write applied, got %+v", id, applied)
		}
	}
}
