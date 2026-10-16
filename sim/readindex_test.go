package sim_test

import (
	"testing"

	"quorum"
)

// TestStaleLeaderCanNeverConfirmARead is the actual point of ReadIndex:
// a leader that's been silently superseded — partitioned away from the
// majority, with a replacement already elected elsewhere — must never
// be able to confirm a read is safe, since serving one from its own
// (possibly stale) local state would violate linearizability. Isolate
// the leader, request a read, and confirm it never resolves no matter
// how long we wait; meanwhile the healthy majority elects its own leader
// and a read against *that* one resolves normally.
func TestStaleLeaderCanNeverConfirmARead(t *testing.T) {
	net, ids := newCluster(5, 4000)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	net.Isolate(leader.ID())

	readID, err := net.Node(leader.ID()).RequestReadIndex()
	if err != nil {
		t.Fatalf("RequestReadIndex failed: %v", err)
	}

	net.TickN(300)
	for _, r := range net.ReadyReads(leader.ID()) {
		if r.ID == readID {
			t.Fatal("an isolated, superseded leader confirmed a read — this must never happen")
		}
	}

	// Meanwhile, the healthy majority should have moved on and elected
	// its own leader, and a read against that one resolves normally.
	var newLeaderID quorum.NodeID
	for _, id := range ids {
		if id != leader.ID() && net.Node(id).Role() == quorum.Leader {
			newLeaderID = id
		}
	}
	if newLeaderID == "" {
		t.Fatal("expected the healthy majority to have elected a replacement leader")
	}

	goodReadID, err := net.Node(newLeaderID).RequestReadIndex()
	if err != nil {
		t.Fatalf("RequestReadIndex on the healthy leader failed: %v", err)
	}
	net.TickN(30)

	confirmed := false
	for _, r := range net.ReadyReads(newLeaderID) {
		if r.ID == goodReadID {
			confirmed = true
		}
	}
	if !confirmed {
		t.Fatal("expected a read against the healthy, majority-connected leader to confirm promptly")
	}
}

// TestStaleLeaderCanNeverConfirmAReadAcrossSeeds sweeps 30 seeds of the
// same scenario — the exact number of ticks before a replacement leader
// emerges varies by seed, and the core safety claim (a superseded leader
// never confirms) needs to hold regardless, not just for one timing.
func TestStaleLeaderCanNeverConfirmAReadAcrossSeeds(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		net, ids := newCluster(5, seed*911+3)
		settle(t, net, 200)
		leader := requireExactlyOneLeader(t, net)

		net.Isolate(leader.ID())
		readID, err := net.Node(leader.ID()).RequestReadIndex()
		if err != nil {
			t.Fatalf("seed %d: RequestReadIndex failed: %v", seed, err)
		}
		net.TickN(300)
		for _, r := range net.ReadyReads(leader.ID()) {
			if r.ID == readID {
				t.Fatalf("seed %d: an isolated, superseded leader confirmed a read", seed)
			}
		}

		var newLeaderID quorum.NodeID
		for _, id := range ids {
			if id != leader.ID() && net.Node(id).Role() == quorum.Leader {
				newLeaderID = id
			}
		}
		if newLeaderID == "" {
			t.Fatalf("seed %d: expected the healthy majority to have elected a replacement", seed)
		}
	}
}
