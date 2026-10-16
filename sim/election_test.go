package sim_test

import (
	"fmt"
	"testing"

	"quorum"
	"quorum/sim"
)

func newCluster(n int, seed int64) (*sim.Network, []quorum.NodeID) {
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
		net.Add(quorum.NewNode(id, peers, quorum.DefaultConfig(), seed+int64(i)+1))
	}
	return net, ids
}

func settle(t *testing.T, net *sim.Network, maxTicks int) {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		net.Tick()
		if len(net.Leaders()) >= 1 {
			return
		}
	}
	t.Fatalf("no leader elected within %d ticks", maxTicks)
}

func requireExactlyOneLeader(t *testing.T, net *sim.Network) *quorum.Node {
	t.Helper()
	leaders := net.Leaders()
	if len(leaders) != 1 {
		ids := make([]quorum.NodeID, len(leaders))
		for i, l := range leaders {
			ids[i] = l.ID()
		}
		t.Fatalf("expected exactly 1 leader, got %d: %v", len(leaders), ids)
	}
	return leaders[0]
}

// nonNoop filters out the empty-Data no-op entries becomeLeader appends
// on every election — recognizing and skipping them is the consuming
// state machine's job, not something the log hides, so tests that stand
// in for a toy state machine do the same filtering a real one would.
func nonNoop(entries []quorum.LogEntry) []quorum.LogEntry {
	var out []quorum.LogEntry
	for _, e := range entries {
		if len(e.Data) > 0 {
			out = append(out, e)
		}
	}
	return out
}

func TestThreeNodeClusterElectsOneLeader(t *testing.T) {
	net, _ := newCluster(3, 100)
	settle(t, net, 200)
	requireExactlyOneLeader(t, net)
}

func TestFiveNodeClusterElectsOneLeader(t *testing.T) {
	net, _ := newCluster(5, 200)
	settle(t, net, 200)
	requireExactlyOneLeader(t, net)
}

func TestReElectionAfterLeaderIsolationThenHeal(t *testing.T) {
	net, _ := newCluster(5, 300)
	settle(t, net, 200)
	oldLeader := requireExactlyOneLeader(t, net)

	net.Isolate(oldLeader.ID())
	// The isolated ex-leader keeps ticking on its own — it'll bump its
	// term forever without ever reaching a majority — but the remaining
	// 4 nodes should still settle on a replacement among themselves.
	newLeaderFound := false
	for i := 0; i < 200; i++ {
		net.Tick()
		for _, l := range net.Leaders() {
			if l.ID() != oldLeader.ID() {
				newLeaderFound = true
			}
		}
		if newLeaderFound {
			break
		}
	}
	if !newLeaderFound {
		t.Fatal("majority partition never elected a replacement leader")
	}

	// Healing lets the isolated ex-leader back in. It's been bumping its
	// term the whole time it was alone, so its stale-but-higher term will
	// force the legitimate leader to step down for a beat before a fresh
	// election settles things — expected behavior for baseline Raft (no
	// PreVote extension yet, that's a natural Part-1.5 hardening item).
	// The cluster should still reconverge to exactly one leader.
	net.Heal()
	settle(t, net, 200)
	requireExactlyOneLeader(t, net)
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	net, ids := newCluster(4, 400)
	// Clean 2-2 split: neither half can reach 3 of 4 votes, so no leader
	// should ever emerge as long as the split holds.
	net.Split(ids[0:2], ids[2:4])
	for i := 0; i < 200; i++ {
		net.Tick()
		if leaders := net.Leaders(); len(leaders) > 0 {
			t.Fatalf("tick %d: neither side of a 2-2 split should elect a leader, got %v",
				i, leaders[0].ID())
		}
	}
}

func TestAtMostOneLeaderPerTermUnderChurn(t *testing.T) {
	net, ids := newCluster(5, 500)
	net.SetDropRate(0.1)
	leaderOf := map[quorum.Term]quorum.NodeID{}

	for i := 0; i < 2000; i++ {
		net.Tick()
		if i%137 == 0 { // periodically reshuffle the partition topology
			switch (i / 137) % 3 {
			case 0:
				net.Heal()
			case 1:
				net.Isolate(ids[i%len(ids)])
			case 2:
				net.Split(ids[0:2], ids[2:5])
			}
		}
		for _, l := range net.Leaders() {
			if existing, ok := leaderOf[l.Term()]; ok && existing != l.ID() {
				t.Fatalf("tick %d: two different leaders (%s, %s) claim term %d",
					i, existing, l.ID(), l.Term())
			}
			leaderOf[l.Term()] = l.ID()
		}
	}
	if len(leaderOf) == 0 {
		t.Fatal("no leader was ever elected across 2000 ticks of churn")
	}
}

// TestElectionSafetyAcrossManySeeds sweeps 200 independent seeds, each
// under partition churn and message loss, and checks the core safety
// property continuously: two nodes can never both be Leader for the same
// term (§5.2, Election Safety). Five hand-picked seeds passing proves
// much less than this — it's the difference between "I read the proof"
// and "I tried to break it 200 different ways and couldn't."
func TestElectionSafetyAcrossManySeeds(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		net, ids := newCluster(5, seed*1000)
		net.SetDropRate(0.15)
		leaderOf := map[quorum.Term]quorum.NodeID{}

		for i := 0; i < 500; i++ {
			net.Tick()
			if i%97 == 0 {
				switch (i / 97) % 3 {
				case 0:
					net.Heal()
				case 1:
					net.Isolate(ids[i%len(ids)])
				case 2:
					net.Split(ids[0:2], ids[2:5])
				}
			}
			for _, l := range net.Leaders() {
				if existing, ok := leaderOf[l.Term()]; ok && existing != l.ID() {
					t.Fatalf("seed %d, tick %d: two different leaders (%s, %s) claim term %d",
						seed, i, existing, l.ID(), l.Term())
				}
				leaderOf[l.Term()] = l.ID()
			}
		}
	}
}
