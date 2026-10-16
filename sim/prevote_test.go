package sim_test

import (
	"fmt"
	"testing"

	"quorum"
	"quorum/sim"
)

func newClusterWithConfig(n int, seed int64, cfg quorum.Config) (*sim.Network, []quorum.NodeID) {
	ids := make([]quorum.NodeID, n)
	for i := range ids {
		ids[i] = quorum.NodeID(fmt.Sprintf("n%d", i))
	}
	net := sim.NewNetwork(seed)
	for i, id := range ids {
		var peers []quorum.NodeID
		for j, other := range ids {
			if j != i {
				peers = append(peers, other)
			}
		}
		net.Add(quorum.NewNode(id, peers, cfg, seed+int64(i)+1))
	}
	return net, ids
}

// TestPreVotePreventsDisruptionFromIsolatedNode is the actual point:
// without PreVote (Part 1, Part 4), an isolated node's real term climbs
// forever while cut off, and reconnecting forces the legitimate leader
// to step down for no real reason. With PreVote enabled, the isolated
// node's prevote requests get refused by everyone still hearing from the
// real leader — its real term never moves — so reconnecting causes zero
// disruption: same leader, same term, the whole time.
func TestPreVotePreventsDisruptionFromIsolatedNode(t *testing.T) {
	cfg := quorum.DefaultConfig()
	cfg.PreVote = true
	net, ids := newClusterWithConfig(5, 2000, cfg)

	settled := false
	for i := 0; i < 200; i++ {
		net.Tick()
		if len(net.Leaders()) >= 1 {
			settled = true
			break
		}
	}
	if !settled {
		t.Fatal("no leader elected within 200 ticks")
	}
	leaders := net.Leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", len(leaders))
	}
	leader := leaders[0]
	leaderTermBefore := leader.Term()

	var victim quorum.NodeID
	for _, id := range ids {
		if id != leader.ID() {
			victim = id
			break
		}
	}

	net.Isolate(victim)
	net.TickN(300) // plenty of time for many prevote-retry cycles while cut off

	if net.Node(victim).Term() != leaderTermBefore {
		t.Fatalf("an isolated node under PreVote should never bump its real term, got %d (was %d)",
			net.Node(victim).Term(), leaderTermBefore)
	}
	if net.Node(victim).Role() != quorum.PreCandidate {
		t.Fatalf("expected the isolated node to be stuck retrying prevotes, got %s", net.Node(victim).Role())
	}

	net.Heal()
	net.TickN(50)

	stillLeaders := net.Leaders()
	if len(stillLeaders) != 1 {
		t.Fatalf("expected exactly 1 leader after healing, got %d", len(stillLeaders))
	}
	if stillLeaders[0].ID() != leader.ID() {
		t.Fatalf("expected the SAME leader to still be in charge (zero disruption), got a different one: %s vs %s",
			stillLeaders[0].ID(), leader.ID())
	}
	if stillLeaders[0].Term() != leaderTermBefore {
		t.Fatalf("expected the leader's term to be completely unaffected by the reconnection, got %d (was %d)",
			stillLeaders[0].Term(), leaderTermBefore)
	}
}
