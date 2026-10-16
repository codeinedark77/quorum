package sim_test

import (
	"bytes"
	"fmt"
	"testing"

	"quorum"
)

// TestAddingNodeChangesTheRequiredMajority is the actual safety-relevant
// claim of Part 5: after a config change commits, the cluster must use
// the NEW membership for majority counting, not the old one. Start at 3
// nodes (majority 2), add a 4th (majority becomes 3), then split the
// resulting 4 nodes into two pairs. If the majority math were still
// using the old cluster size, a pair would wrongly be enough. It isn't
// enough for 4 — neither side should ever elect a leader.
func TestAddingNodeChangesTheRequiredMajority(t *testing.T) {
	net, ids := newCluster(3, 3000)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	// The joining node's peers must be everyone else in the post-change
	// cluster — it needs to know who to talk to once it's added.
	var newNodePeers []quorum.NodeID
	newNodePeers = append(newNodePeers, ids...)
	net.Add(quorum.NewNode("n3", newNodePeers, quorum.DefaultConfig(), 9999))

	var fullMembership []quorum.NodeID
	fullMembership = append(fullMembership, ids...)
	fullMembership = append(fullMembership, "n3")
	if _, _, err := net.Node(leader.ID()).ProposeConfigChange(fullMembership); err != nil {
		t.Fatalf("ProposeConfigChange failed: %v", err)
	}
	net.TickN(200) // commit the change, and let n3 catch up from an empty log via the normal backoff path

	if net.Node("n3").LastIndex() == 0 {
		t.Fatal("expected n3 to have caught up via normal replication after joining, LastIndex is still 0")
	}

	// Confirm it's a real, participating member: a fresh write reaches it.
	currentLeader := requireExactlyOneLeader(t, net)
	if _, _, err := net.Node(currentLeader.ID()).Propose([]byte("post-join-write")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(30)
	for _, id := range fullMembership {
		applied := nonNoop(net.Applied(id))
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1].Data, []byte("post-join-write")) {
			t.Fatalf("node %s: expected the post-join write applied, got %+v", id, applied)
		}
	}

	// The actual safety check: split the 4 nodes into two pairs. Neither
	// pair is a majority of 4. Note this does NOT mean no node will
	// self-report as Leader — the pre-split leader has no way to detect
	// it's lost quorum (baseline Raft has no lease/quorum-check
	// mechanism for that) and will happily keep believing it's in
	// charge. The actual safety property is narrower and is the one
	// that matters: nothing NEW can get committed without a real
	// majority, on either side.
	net.Split(fullMembership[0:2], fullMembership[2:4])
	preSplitApplied := map[quorum.NodeID]int{}
	for _, id := range fullMembership {
		preSplitApplied[id] = len(net.Applied(id))
	}

	for i := 0; i < 200; i++ {
		net.Tick()
		for _, l := range net.Leaders() {
			net.Node(l.ID()).Propose([]byte(fmt.Sprintf("split-write-%d", i)))
		}
	}

	for _, id := range fullMembership {
		if len(net.Applied(id)) != preSplitApplied[id] {
			t.Fatalf("node %s: a write committed during a 2-2 split of a 4-node cluster — neither half has a real majority, this must never happen",
				id)
		}
	}
}

// TestRemovingNodeStillOperatesCorrectly confirms the other direction:
// remove a node from a running cluster, and the remaining members keep
// operating correctly without it — including that the removed node
// itself is no longer able to act as leader for the cluster it left.
func TestRemovingNodeStillOperatesCorrectly(t *testing.T) {
	net, ids := newCluster(5, 3100)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	var toRemove quorum.NodeID
	for _, id := range ids {
		if id != leader.ID() {
			toRemove = id
			break
		}
	}
	var remaining []quorum.NodeID
	for _, id := range ids {
		if id != toRemove {
			remaining = append(remaining, id)
		}
	}

	if _, _, err := net.Node(leader.ID()).ProposeConfigChange(remaining); err != nil {
		t.Fatalf("ProposeConfigChange failed: %v", err)
	}
	net.TickN(50)

	// Isolate the removed node entirely (standing in for it being
	// decommissioned) and confirm the remaining 4 keep operating fine —
	// majority of 4 is 3, and all 4 are still reachable.
	net.Isolate(toRemove)
	currentLeader := requireExactlyOneLeader(t, net)
	if currentLeader.ID() == toRemove {
		t.Fatal("the removed node should not still be able to act as leader")
	}
	if _, _, err := net.Node(currentLeader.ID()).Propose([]byte("after-removal")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(30)
	for _, id := range remaining {
		applied := nonNoop(net.Applied(id))
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1].Data, []byte("after-removal")) {
			t.Fatalf("node %s: expected the post-removal write applied, got %+v", id, applied)
		}
	}
}

// TestRemovedNodeStaysConnectedButNeverDisrupts is the direct proof for
// the bug this part's implementation actually hit: a removed node that
// is NOT isolated — still fully reachable, no network trickery masking
// anything — must still never disrupt the cluster it left. A node that
// only fell back to Follower (rather than a genuine terminal Removed
// state) would keep ticking its own election timer, and once the real
// leader correctly stopped replicating to it, that timer would elapse
// and it would start elections against a cluster it isn't part of.
func TestRemovedNodeStaysConnectedButNeverDisrupts(t *testing.T) {
	net, ids := newCluster(5, 3200)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)
	leaderTermBefore := leader.Term()

	var toRemove quorum.NodeID
	for _, id := range ids {
		if id != leader.ID() {
			toRemove = id
			break
		}
	}
	var remaining []quorum.NodeID
	for _, id := range ids {
		if id != toRemove {
			remaining = append(remaining, id)
		}
	}

	if _, _, err := net.Node(leader.ID()).ProposeConfigChange(remaining); err != nil {
		t.Fatalf("ProposeConfigChange failed: %v", err)
	}

	// Deliberately no Isolate call — the removed node stays fully
	// network-reachable for the rest of this test. If it can still
	// disrupt anything, this is where it would show up.
	net.TickN(500)

	if net.Node(toRemove).Role() != quorum.Removed {
		t.Fatalf("expected the removed node to have reached the terminal Removed state, got %s", net.Node(toRemove).Role())
	}

	stillLeaders := net.Leaders()
	if len(stillLeaders) != 1 {
		t.Fatalf("expected exactly 1 leader among the remaining cluster after 500 ticks, got %d", len(stillLeaders))
	}
	if stillLeaders[0].ID() != leader.ID() || stillLeaders[0].Term() != leaderTermBefore {
		t.Fatalf("expected zero disruption to the original leader (same id, same term), got %s at term %d (was %s at term %d)",
			stillLeaders[0].ID(), stillLeaders[0].Term(), leader.ID(), leaderTermBefore)
	}

	if _, _, err := net.Node(leader.ID()).Propose([]byte("still-fine")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(30)
	for _, id := range remaining {
		applied := nonNoop(net.Applied(id))
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1].Data, []byte("still-fine")) {
			t.Fatalf("node %s: expected normal operation to continue, got %+v", id, applied)
		}
	}
}

// TestRemovedNodeNeverDisruptsAcrossSeeds sweeps 40 seeds of the same
// scenario — remove a node, leave it fully connected, never isolate it —
// since the bug this guards against was purely a timing artifact
// (whether a departing peer's notification lands before its own election
// timeout) and one lucky seed proved nothing about the general case.
func TestRemovedNodeNeverDisruptsAcrossSeeds(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		net, ids := newCluster(5, seed*513+7)
		settle(t, net, 200)
		leader := requireExactlyOneLeader(t, net)
		leaderTermBefore := leader.Term()

		var toRemove quorum.NodeID
		for _, id := range ids {
			if id != leader.ID() {
				toRemove = id
				break
			}
		}
		var remaining []quorum.NodeID
		for _, id := range ids {
			if id != toRemove {
				remaining = append(remaining, id)
			}
		}

		if _, _, err := net.Node(leader.ID()).ProposeConfigChange(remaining); err != nil {
			t.Fatalf("seed %d: ProposeConfigChange failed: %v", seed, err)
		}
		net.TickN(500)

		if net.Node(toRemove).Role() != quorum.Removed {
			t.Fatalf("seed %d: expected the removed node to reach the terminal Removed state, got %s",
				seed, net.Node(toRemove).Role())
		}
		stillLeaders := net.Leaders()
		if len(stillLeaders) != 1 || stillLeaders[0].ID() != leader.ID() || stillLeaders[0].Term() != leaderTermBefore {
			t.Fatalf("seed %d: expected zero disruption (same leader %s, same term %d), got %v",
				seed, leader.ID(), leaderTermBefore, stillLeaders)
		}
	}
}
