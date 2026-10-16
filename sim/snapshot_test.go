package sim_test

import (
	"bytes"
	"fmt"
	"testing"

	"quorum"
)

// TestLaggingFollowerCatchesUpViaSnapshot is the actual point of Part 4:
// without snapshotting, a follower missing entries the leader has
// already discarded is permanently stuck — there's no AppendEntries that
// can express "here's everything before you" once the discrete entries
// are gone. Isolate a follower, commit and then compact past everything
// it's missing, heal, and confirm it still catches up correctly via
// InstallSnapshot rather than getting stuck waiting for data that no
// longer exists anywhere.
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	net, ids := newCluster(5, 1200)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	var follower quorum.NodeID
	for _, id := range ids {
		if id != leader.ID() {
			follower = id
			break
		}
	}

	net.Isolate(follower)

	// Commit several entries while the follower can't hear any of them —
	// the remaining 4 nodes are still a majority.
	for i := 0; i < 5; i++ {
		if _, _, err := net.Node(leader.ID()).Propose([]byte(fmt.Sprintf("w%d", i))); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
		net.TickN(10)
	}

	leaderLast := net.Node(leader.ID()).LastIndex()
	// Compact right up to the leader's current tip — every entry the
	// isolated follower is missing is now gone as a discrete record.
	if err := net.CompactLog(leader.ID(), leaderLast, []byte("snapshot-as-of-tip")); err != nil {
		t.Fatalf("CompactLog failed: %v", err)
	}
	net.TickN(5)

	net.Heal()
	net.TickN(200) // rejoin, get InstallSnapshot instead of a doomed AppendEntries, adopt it

	if net.Node(follower).LastIndex() < leaderLast {
		t.Fatalf("expected the follower to catch up to at least LastIndex=%d via snapshot, got %d",
			leaderLast, net.Node(follower).LastIndex())
	}

	// The isolated follower spent the whole time bumping its own term
	// (same dynamic as an isolated leader in baseline Raft), so
	// leadership may well have moved during reconnection — re-check
	// rather than assume the original leader is still in charge.
	currentLeader := requireExactlyOneLeader(t, net)

	// Prove it's not just caught up in name — a new write after the
	// snapshot still replicates and applies correctly everywhere,
	// including on the node that needed the snapshot to get there.
	if _, _, err := net.Node(currentLeader.ID()).Propose([]byte("after-snapshot")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(30)

	for _, id := range ids {
		applied := nonNoop(net.Applied(id))
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1].Data, []byte("after-snapshot")) {
			t.Fatalf("node %s: expected the post-snapshot write to be the most recent applied entry, got %+v", id, applied)
		}
	}
}

// TestLaggingFollowerCatchesUpViaSnapshotAcrossSeeds sweeps the same
// scenario across many seeds — the exact timing of the isolated
// follower's own election attempts versus the leader's compaction is
// seed-sensitive, and one lucky pass isn't evidence the mechanism is
// actually robust to it.
func TestLaggingFollowerCatchesUpViaSnapshotAcrossSeeds(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		net, ids := newCluster(5, seed*777+1)
		settle(t, net, 200)
		leader := requireExactlyOneLeader(t, net)

		var follower quorum.NodeID
		for _, id := range ids {
			if id != leader.ID() {
				follower = id
				break
			}
		}

		net.Isolate(follower)
		for i := 0; i < 5; i++ {
			if _, _, err := net.Node(leader.ID()).Propose([]byte(fmt.Sprintf("s%d-w%d", seed, i))); err != nil {
				t.Fatalf("seed %d: Propose failed: %v", seed, err)
			}
			net.TickN(10)
		}

		leaderLast := net.Node(leader.ID()).LastIndex()
		if err := net.CompactLog(leader.ID(), leaderLast, []byte("snap")); err != nil {
			t.Fatalf("seed %d: CompactLog failed: %v", seed, err)
		}
		net.TickN(5)
		net.Heal()
		net.TickN(300)

		if net.Node(follower).LastIndex() < leaderLast {
			t.Fatalf("seed %d: follower never caught up to at least %d, got %d",
				seed, leaderLast, net.Node(follower).LastIndex())
		}

		currentLeader := requireExactlyOneLeader(t, net)
		marker := []byte(fmt.Sprintf("seed%d-after", seed))
		if _, _, err := net.Node(currentLeader.ID()).Propose(marker); err != nil {
			t.Fatalf("seed %d: Propose failed: %v", seed, err)
		}
		net.TickN(30)

		for _, id := range ids {
			applied := nonNoop(net.Applied(id))
			if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1].Data, marker) {
				t.Fatalf("seed %d, node %s: expected the post-snapshot write as the most recent applied entry, got %+v",
					seed, id, applied)
			}
		}
	}
}
