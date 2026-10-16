package transport

import (
	"bytes"
	"testing"
	"time"

	"quorum"
)

func waitConnected(t *testing.T, tr *TCPTransport, peer quorum.NodeID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		tr.mu.Lock()
		_, ok := tr.senders[peer]
		tr.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection to %s never established within %v", peer, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTCPTransportRegisterPeerConnectsDynamically(t *testing.T) {
	addrA, addrB := "127.0.0.1:19011", "127.0.0.1:19012"

	a, err := NewTCPTransport("a", addrA, nil) // no peers configured at construction
	if err != nil {
		t.Fatalf("NewTCPTransport(a) failed: %v", err)
	}
	defer a.Close()
	b, err := NewTCPTransport("b", addrB, nil)
	if err != nil {
		t.Fatalf("NewTCPTransport(b) failed: %v", err)
	}
	defer b.Close()

	a.RegisterPeer("b", addrB)
	waitConnected(t, a, "b", 3*time.Second)

	msg := quorum.Message{Type: quorum.MsgAppendEntries, From: "a", To: "b", Term: 3}
	a.Send(msg)
	select {
	case got := <-b.Incoming():
		if got.Term != 3 {
			t.Fatalf("unexpected message: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived after RegisterPeer")
	}
}

func TestTCPTransportSendReceive(t *testing.T) {
	addrA, addrB := "127.0.0.1:19001", "127.0.0.1:19002"

	a, err := NewTCPTransport("a", addrA, map[quorum.NodeID]string{"b": addrB})
	if err != nil {
		t.Fatalf("NewTCPTransport(a) failed: %v", err)
	}
	defer a.Close()
	b, err := NewTCPTransport("b", addrB, map[quorum.NodeID]string{"a": addrA})
	if err != nil {
		t.Fatalf("NewTCPTransport(b) failed: %v", err)
	}
	defer b.Close()

	waitConnected(t, a, "b", 3*time.Second)

	msg := quorum.Message{Type: quorum.MsgAppendEntries, From: "a", To: "b", Term: 7, LeaderCommit: 3}
	a.Send(msg)

	select {
	case got := <-b.Incoming():
		if got.Type != msg.Type || got.From != msg.From || got.Term != msg.Term || got.LeaderCommit != msg.LeaderCommit {
			t.Fatalf("received message doesn't match sent: got %+v want %+v", got, msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived")
	}
}

func TestTCPTransportFullMessageFidelity(t *testing.T) {
	addrA, addrB := "127.0.0.1:19003", "127.0.0.1:19004"

	a, err := NewTCPTransport("a", addrA, map[quorum.NodeID]string{"b": addrB})
	if err != nil {
		t.Fatalf("NewTCPTransport(a) failed: %v", err)
	}
	defer a.Close()
	b, err := NewTCPTransport("b", addrB, map[quorum.NodeID]string{"a": addrA})
	if err != nil {
		t.Fatalf("NewTCPTransport(b) failed: %v", err)
	}
	defer b.Close()

	waitConnected(t, a, "b", 3*time.Second)

	original := quorum.Message{
		Type: quorum.MsgInstallSnapshot, From: "a", To: "b", Term: 12,
		Entries: []quorum.LogEntry{
			{Term: 3, Index: 5, Type: quorum.EntryConfigChange, Data: []byte("membership-blob")},
			{Term: 4, Index: 6, Data: []byte("ordinary data")},
		},
		LastIncludedIndex: 9, LastIncludedTerm: 8,
		SnapshotData: []byte("a reasonably large fake snapshot blob, just bytes quorum never interprets"),
		ReadSeq:      42,
	}
	a.Send(original)

	select {
	case got := <-b.Incoming():
		if got.Type != original.Type || got.Term != original.Term {
			t.Fatalf("basic fields mismatch: got %+v", got)
		}
		if len(got.Entries) != 2 || string(got.Entries[0].Data) != "membership-blob" || got.Entries[0].Type != quorum.EntryConfigChange {
			t.Fatalf("entries didn't round-trip correctly: got %+v", got.Entries)
		}
		if got.LastIncludedIndex != 9 || got.LastIncludedTerm != 8 {
			t.Fatalf("snapshot boundary didn't round-trip: got %+v", got)
		}
		if !bytes.Equal(got.SnapshotData, original.SnapshotData) {
			t.Fatalf("snapshot data didn't round-trip: got %q", got.SnapshotData)
		}
		if got.ReadSeq != 42 {
			t.Fatalf("ReadSeq didn't round-trip: got %d", got.ReadSeq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived")
	}
}

func TestTCPTransportSendToUnconnectedPeerIsANoOp(t *testing.T) {
	a, err := NewTCPTransport("a", "127.0.0.1:19007", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport failed: %v", err)
	}
	defer a.Close()

	// No peer configured at all — Send must not panic or block, just
	// silently drop, matching the transport's best-effort contract.
	a.Send(quorum.Message{Type: quorum.MsgAppendEntries, To: "nobody"})
}

func TestTCPTransportAddrReturnsActualListenAddress(t *testing.T) {
	a, err := NewTCPTransport("a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport failed: %v", err)
	}
	defer a.Close()
	if a.Addr() == "127.0.0.1:0" || a.Addr() == "" {
		t.Fatalf("expected Addr() to report the OS-assigned port, got %q", a.Addr())
	}
}
func TestTCPTransportReconnectsAfterConnectionDrop(t *testing.T) {
	addrA, addrB := "127.0.0.1:19005", "127.0.0.1:19006"

	a, err := NewTCPTransport("a", addrA, map[quorum.NodeID]string{"b": addrB})
	if err != nil {
		t.Fatalf("NewTCPTransport(a) failed: %v", err)
	}
	defer a.Close()
	b, err := NewTCPTransport("b", addrB, map[quorum.NodeID]string{"a": addrA})
	if err != nil {
		t.Fatalf("NewTCPTransport(b) failed: %v", err)
	}
	defer b.Close()

	waitConnected(t, a, "b", 3*time.Second)

	// Simulate a dropped connection from a's side.
	a.mu.Lock()
	conn := a.conns["b"]
	delete(a.senders, "b")
	delete(a.conns, "b")
	a.mu.Unlock()
	conn.Close()

	// The background maintainer should redial without any help.
	waitConnected(t, a, "b", 3*time.Second)

	msg := quorum.Message{Type: quorum.MsgAppendEntries, From: "a", To: "b", Term: 1}
	a.Send(msg)
	select {
	case got := <-b.Incoming():
		if got.Term != 1 {
			t.Fatalf("unexpected message after reconnect: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived after reconnection")
	}
}
