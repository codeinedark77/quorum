package transport

import (
	"sync"
	"testing"
	"time"

	"quorum"
)

type testCluster struct {
	drivers map[quorum.NodeID]*Driver
	trs     map[quorum.NodeID]*TCPTransport

	mu      sync.Mutex
	applied map[quorum.NodeID][]quorum.LogEntry
}

func newTestCluster(t *testing.T, ids []quorum.NodeID, basePort int) *testCluster {
	t.Helper()
	addrs := map[quorum.NodeID]string{}
	for i, id := range ids {
		addrs[id] = "127.0.0.1:" + itoa(basePort+i)
	}

	tc := &testCluster{
		drivers: map[quorum.NodeID]*Driver{},
		trs:     map[quorum.NodeID]*TCPTransport{},
		applied: map[quorum.NodeID][]quorum.LogEntry{},
	}

	for i, id := range ids {
		var peers []quorum.NodeID
		peerAddrs := map[quorum.NodeID]string{}
		for j, other := range ids {
			if j != i {
				peers = append(peers, other)
				peerAddrs[other] = addrs[other]
			}
		}

		tr, err := NewTCPTransport(id, addrs[id], peerAddrs)
		if err != nil {
			t.Fatalf("NewTCPTransport(%s) failed: %v", id, err)
		}
		tc.trs[id] = tr

		cfg := quorum.DefaultConfig()
		cfg.ElectionTicksMin, cfg.ElectionTicksMax, cfg.HeartbeatTicks = 10, 20, 2
		node := quorum.NewNode(id, peers, cfg, int64(i)+1)
		storage := quorum.NewMemStorage()

		d := NewDriver(node, storage, tr, 20*time.Millisecond)
		nodeID := id
		d.OnCommitted = func(e quorum.LogEntry) {
			tc.mu.Lock()
			tc.applied[nodeID] = append(tc.applied[nodeID], e)
			tc.mu.Unlock()
		}
		tc.drivers[id] = d
		go d.Run()
	}
	return tc
}

func (tc *testCluster) stop() {
	for _, d := range tc.drivers {
		d.Stop()
	}
	for _, tr := range tc.trs {
		tr.Close()
	}
}

func (tc *testCluster) appliedFor(id quorum.NodeID) []quorum.LogEntry {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	out := make([]quorum.LogEntry, len(tc.applied[id]))
	copy(out, tc.applied[id])
	return out
}

func (tc *testCluster) findLeader(timeout time.Duration) (quorum.NodeID, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []quorum.NodeID
		for id, d := range tc.drivers {
			if d.State().Role == quorum.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0], true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// alwaysFailStorage is a quorum.Storage whose every method fails, used
// specifically to exercise the driver's failure-handling path
// deterministically rather than trying to provoke a real disk error.
type alwaysFailStorage struct{ err error }

func (s alwaysFailStorage) SaveHardState(quorum.Term, quorum.NodeID) error { return s.err }
func (s alwaysFailStorage) AppendEntries([]quorum.LogEntry) error          { return s.err }
func (s alwaysFailStorage) TruncateFrom(quorum.LogIndex) error             { return s.err }
func (s alwaysFailStorage) Compact(quorum.LogIndex, quorum.Term) error     { return s.err }
func (s alwaysFailStorage) Load() (quorum.Term, quorum.NodeID, quorum.LogIndex, quorum.Term, []quorum.LogEntry, error) {
	return 0, "", 0, 0, nil, s.err
}

func TestDriverPanicsOnStorageFailureWithNoHandlerSet(t *testing.T) {
	tr, err := NewTCPTransport("solo", "127.0.0.1:19008", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport failed: %v", err)
	}
	defer tr.Close()

	node := quorum.NewNode("solo", nil, quorum.DefaultConfig(), 1)
	storage := alwaysFailStorage{err: errFake}
	d := NewDriver(node, storage, tr, 10*time.Millisecond)
	// OnStorageError deliberately left nil.

	panicked := make(chan interface{}, 1)
	go func() {
		defer func() { panicked <- recover() }()
		d.Run()
	}()

	// A zero-peer node becomes its own leader as soon as its election
	// timeout elapses — which itself is a term/votedFor change that
	// tries (and fails) to persist, so the panic actually fires well
	// before any explicit Propose call is needed.
	select {
	case r := <-panicked:
		if r == nil {
			t.Fatal("expected the driver's Run loop to panic on an unhandled storage failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("driver never panicked within the timeout")
	}
}

func TestDriverRoutesStorageFailureToHandlerWhenSet(t *testing.T) {
	tr, err := NewTCPTransport("solo", "127.0.0.1:19009", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport failed: %v", err)
	}
	defer tr.Close()

	node := quorum.NewNode("solo", nil, quorum.DefaultConfig(), 1)
	storage := alwaysFailStorage{err: errFake}
	d := NewDriver(node, storage, tr, 10*time.Millisecond)

	errCh := make(chan error, 1)
	d.OnStorageError = func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go d.Run()
	defer d.Stop()

	// Same trigger as the panic test: a zero-peer node's own election
	// timeout fires a term/votedFor change that tries to persist and
	// fails, well within a couple of election-timeout windows.
	select {
	case gotErr := <-errCh:
		if gotErr != errFake {
			t.Fatalf("expected OnStorageError to receive the storage error, got %v", gotErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnStorageError was never called within the timeout")
	}
}

var errFake = fakeErr("fake storage failure")

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

// TestRealClusterElectsLeaderAndReplicatesOverTCP is the full-stack
// proof: three real Node instances, each with real MemStorage and a real
// TCPTransport, running as real goroutines on a real time.Ticker,
// communicating over actual localhost TCP sockets — not sim.Network's
// deterministic, in-memory routing. If this passes, the whole stack
// (algorithm, persistence interface, and now a real transport) genuinely
// works together, not just in a controlled simulation.
func TestDriverStopIsIdempotent(t *testing.T) {
	tr, err := NewTCPTransport("solo", "127.0.0.1:19010", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport failed: %v", err)
	}
	defer tr.Close()

	node := quorum.NewNode("solo", nil, quorum.DefaultConfig(), 1)
	d := NewDriver(node, quorum.NewMemStorage(), tr, 10*time.Millisecond)
	go d.Run()

	// A real caller can plausibly invoke Stop() more than once — a
	// shutdown handler firing twice, cleanup code that doesn't perfectly
	// track whether it already stopped something. This must not panic.
	d.Stop()
	d.Stop()
	d.Stop()
}

func TestRealClusterElectsLeaderAndReplicatesOverTCP(t *testing.T) {
	ids := []quorum.NodeID{"n0", "n1", "n2"}
	tc := newTestCluster(t, ids, 20101)
	defer tc.stop()

	leaderID, ok := tc.findLeader(5 * time.Second)
	if !ok {
		t.Fatal("no leader elected within 5 seconds over real TCP")
	}

	idx, term, err := tc.drivers[leaderID].Propose([]byte("hello over real tcp"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if idx == 0 {
		t.Fatalf("expected a nonzero index, got %d (term %d)", idx, term)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		allApplied := true
		for _, id := range ids {
			found := false
			for _, e := range tc.appliedFor(id) {
				if string(e.Data) == "hello over real tcp" {
					found = true
				}
			}
			if !found {
				allApplied = false
			}
		}
		if allApplied {
			break
		}
		if time.Now().After(deadline) {
			for _, id := range ids {
				t.Logf("node %s applied: %+v", id, tc.appliedFor(id))
			}
			t.Fatal("the write never replicated to all 3 real nodes within 5 seconds")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
