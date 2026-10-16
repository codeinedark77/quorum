package sim_test

import (
	"bytes"
	"fmt"
	"testing"

	"quorum"
)

func TestReplicatedEntriesApplyInOrderOnAllNodes(t *testing.T) {
	net, ids := newCluster(5, 700)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	writes := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, w := range writes {
		if _, _, err := net.Node(leader.ID()).Propose(w); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
		net.TickN(10) // let each write fully replicate + commit before the next
	}
	net.TickN(20)

	for _, id := range ids {
		applied := nonNoop(net.Applied(id))
		if len(applied) != len(writes) {
			t.Fatalf("node %s: expected %d applied entries, got %d", id, len(writes), len(applied))
		}
		for i, e := range applied {
			if !bytes.Equal(e.Data, writes[i]) {
				t.Fatalf("node %s: entry %d mismatch: got %q want %q", id, i, e.Data, writes[i])
			}
		}
	}
}

func TestIsolatedFollowerCatchesUpAfterHeal(t *testing.T) {
	net, ids := newCluster(5, 800)
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
	for i := 0; i < 3; i++ {
		if _, _, err := net.Node(leader.ID()).Propose([]byte(fmt.Sprintf("w%d", i))); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
		net.TickN(10)
	}
	if len(nonNoop(net.Applied(follower))) != 0 {
		t.Fatalf("isolated follower shouldn't have applied any client entries yet, got %d", len(nonNoop(net.Applied(follower))))
	}

	net.Heal()
	net.TickN(50)

	want := nonNoop(net.Applied(leader.ID()))
	if len(want) != 3 {
		t.Fatalf("sanity check: expected leader to have applied 3 entries, got %d", len(want))
	}
	got := nonNoop(net.Applied(follower))
	if len(got) != len(want) {
		t.Fatalf("previously isolated follower didn't catch up: got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d mismatch after catch-up: got %q want %q", i, got[i].Data, want[i].Data)
		}
	}
}

// TestOldLeadersUncommittedEntryDiscardedAfterNewLeaderCommits is the
// Figure-8-flavored test at the cluster level: an old leader appends an
// entry that can never replicate anywhere (it's isolated the instant
// after appending it), a new leader wins an election in the majority
// partition and commits a *different* entry at that same log index, and
// once everyone reconnects, the orphaned entry must be gone everywhere —
// never applied by anyone, including the node that originally wrote it.
func TestOldLeadersUncommittedEntryDiscardedAfterNewLeaderCommits(t *testing.T) {
	net, ids := newCluster(5, 900)
	settle(t, net, 200)
	oldLeader := requireExactlyOneLeader(t, net)

	// A durable write everyone gets first, so there's a known-good prefix.
	if _, _, err := net.Node(oldLeader.ID()).Propose([]byte("base")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(20)

	net.Isolate(oldLeader.ID())
	// This entry can never replicate anywhere — the old leader is alone
	// from the instant this call returns.
	if _, _, err := net.Node(oldLeader.ID()).Propose([]byte("orphaned")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(5)

	// The remaining 4 (a majority) elect a replacement and commit a
	// different entry at the same log index the orphaned write occupies.
	var newLeader quorum.NodeID
	for i := 0; i < 200 && newLeader == ""; i++ {
		net.Tick()
		for _, l := range net.Leaders() {
			if l.ID() != oldLeader.ID() {
				newLeader = l.ID()
			}
		}
	}
	if newLeader == "" {
		t.Fatal("majority partition never elected a replacement leader")
	}
	if _, _, err := net.Node(newLeader).Propose([]byte("real")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	net.TickN(30)

	net.Heal()
	net.TickN(50)

	for _, id := range ids {
		applied := nonNoop(net.Applied(id))
		if len(applied) != 2 {
			t.Fatalf("node %s: expected 2 applied entries (base, real), got %d: %+v", id, len(applied), applied)
		}
		if !bytes.Equal(applied[0].Data, []byte("base")) {
			t.Fatalf("node %s: entry 0 should be 'base', got %q", id, applied[0].Data)
		}
		if !bytes.Equal(applied[1].Data, []byte("real")) {
			t.Fatalf("node %s: entry 1 should be 'real', got %q — the orphaned write leaked through", id, applied[1].Data)
		}
	}
}

// TestLogMatchesAcrossAllNodesUnderChurn sweeps 50 seeds of continuous
// random writes against whatever the current (possibly minority, possibly
// illusory) leader is, under constant partition churn and message loss,
// and checks State Machine Safety directly (§5.4.3): if two nodes have
// both applied a given index, they must agree on exactly what's there.
// This is the bottom-line guarantee a replicated log exists to provide —
// broader and less scripted than the two hand-built scenarios above.
func TestLogMatchesAcrossAllNodesUnderChurn(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		net, ids := newCluster(5, seed*1000+seed)
		net.SetDropRate(0.1)
		writeCount := 0

		for i := 0; i < 1500; i++ {
			net.Tick()

			if i%150 == 0 {
				switch (i / 150) % 3 {
				case 0:
					net.Heal()
				case 1:
					net.Isolate(ids[i%len(ids)])
				case 2:
					net.Split(ids[0:2], ids[2:5])
				}
			}

			// Try a write against every node that currently believes
			// it's leader — including an isolated ex-leader still
			// ticking away alone, which will succeed locally but should
			// never actually commit anywhere.
			if i%13 == 0 {
				for _, l := range net.Leaders() {
					data := []byte(fmt.Sprintf("seed%d-write%d", seed, writeCount))
					if _, _, err := net.Node(l.ID()).Propose(data); err == nil {
						writeCount++
					}
				}
			}
		}

		byIndex := map[quorum.LogIndex]quorum.LogEntry{}
		for _, id := range ids {
			for i, e := range net.Applied(id) {
				idx := quorum.LogIndex(i + 1)
				if existing, ok := byIndex[idx]; ok {
					if !bytes.Equal(existing.Data, e.Data) || existing.Term != e.Term {
						t.Fatalf("seed %d: nodes disagree on applied index %d: %+v vs %+v",
							seed, idx, existing, e)
					}
				} else {
					byIndex[idx] = e
				}
			}
		}
	}
}
