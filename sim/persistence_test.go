package sim_test

import (
	"bytes"
	"fmt"
	"testing"

	"quorum"
)

// TestNodeResumesAfterSimulatedRestart is the actual point of Part 3:
// commit some entries, crash a node (discard its in-memory Node and
// rebuild one purely from what its Storage durably saved), let it rejoin,
// and confirm the data is genuinely still there — not just "the log
// slice survived" but "the whole cluster still agrees on it and the
// restarted node's own state machine sees it applied again."
func TestNodeResumesAfterSimulatedRestart(t *testing.T) {
	net, ids := newCluster(5, 1000)
	settle(t, net, 200)
	leader := requireExactlyOneLeader(t, net)

	for i := 0; i < 3; i++ {
		if _, _, err := net.Node(leader.ID()).Propose([]byte(fmt.Sprintf("w%d", i))); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
	}
	net.TickN(30)

	before := nonNoop(net.Applied(leader.ID()))
	if len(before) != 3 {
		t.Fatalf("sanity check: expected 3 applied entries before restart, got %d", len(before))
	}

	net.SimulateRestart(leader.ID())
	net.TickN(300) // let it rejoin, possibly sit out a re-election, and reconfirm its log

	for _, id := range ids {
		applied := nonNoop(net.Applied(id))
		if len(applied) != 3 {
			t.Fatalf("node %s: expected 3 applied entries after restart, got %d: %+v", id, len(applied), applied)
		}
		for i := range before {
			if !bytes.Equal(applied[i].Data, before[i].Data) {
				t.Fatalf("node %s: entry %d changed across the restart: got %q want %q",
					id, i, applied[i].Data, before[i].Data)
			}
		}
	}
}

// TestRestartedNodeNeverGrantsASecondVoteInATermItAlreadyVotedIn is the
// actual safety property persistence exists to protect. A node grants a
// vote — durably saving votedFor is what Network.Tick does before that
// grant is allowed to count as "sent" — then the process is simulated as
// crashing and restarting immediately after, before hearing anything
// else. If votedFor hadn't actually survived that, the restarted
// instance would be willing to grant a second, conflicting vote in the
// very term it already committed to — which is exactly the crack
// Election Safety's proof depends on not existing.
func TestRestartedNodeNeverGrantsASecondVoteInATermItAlreadyVotedIn(t *testing.T) {
	net, _ := newCluster(3, 1100)

	net.Node("n0").Step(quorum.Message{
		Type: quorum.MsgVoteRequest, From: "n1", Term: 1,
	})
	net.Tick() // drains n0's Ready(), persisting the vote grant's HardState

	if net.Node("n0").Term() != 1 {
		t.Fatalf("sanity check: expected n0 to have adopted term 1, got %d", net.Node("n0").Term())
	}

	net.SimulateRestart("n0")

	net.Node("n0").Step(quorum.Message{
		Type: quorum.MsgVoteRequest, From: "n2", Term: 1, // same term, a different candidate
	})
	ready := net.Node("n0").Ready()
	if len(ready.Messages) != 1 {
		t.Fatalf("expected exactly one vote response, got %+v", ready.Messages)
	}
	if ready.Messages[0].VoteGranted {
		t.Fatal("restarted node granted a second, conflicting vote in a term it already voted in — Election Safety is broken")
	}
}
