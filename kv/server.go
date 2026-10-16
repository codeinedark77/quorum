package kv

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"quorum"
	"quorum/transport"
)

// ErrProposalSuperseded means the entry actually committed at the index
// this write landed on is a DIFFERENT entry than the one proposed — a
// leadership change overwrote it before it committed. The write did not
// take effect; the caller should retry.
var ErrProposalSuperseded = errors.New("kv: proposal was superseded before it committed")

// ErrTimeout means Put or Get gave up waiting rather than that anything
// is known to have failed — the operation may still complete later.
var ErrTimeout = errors.New("kv: operation timed out waiting for confirmation")

type pendingWrite struct {
	data []byte
	done chan error
}

// Server is what actually turns a *transport.Driver into a usable
// replicated key-value store: Put and Get are real blocking calls a
// caller can use directly, each one exercising the exact quorum
// mechanism built to make it safe — Propose plus watching
// CommittedEntries for writes, RequestReadIndex plus watching ReadyReads
// for reads.
type Server struct {
	store  *KVStore
	driver *transport.Driver

	mu               sync.Mutex
	pendingWrite     map[quorum.LogIndex]*pendingWrite
	pendingRead      map[uint64]chan struct{}
	lastAppliedIndex quorum.LogIndex
}

// NewServer wires itself into driver's callbacks. driver must not have
// Run() called on it yet, and its OnCommitted/OnSnapshot/OnReadReady
// must still be unset — Server takes ownership of all three.
func NewServer(store *KVStore, driver *transport.Driver) *Server {
	s := &Server{
		store:        store,
		driver:       driver,
		pendingWrite: map[quorum.LogIndex]*pendingWrite{},
		pendingRead:  map[uint64]chan struct{}{},
	}
	driver.OnCommitted = s.onCommitted
	driver.OnSnapshot = s.onSnapshot
	driver.OnReadReady = s.onReadReady
	return s
}

// Put proposes a write and blocks until it's either confirmed committed
// (nil), confirmed superseded (ErrProposalSuperseded — safe to retry),
// or the timeout elapses (ErrTimeout — unknown outcome, also safe to
// retry, since Propose is not itself applied twice for the same
// attempt).
func (s *Server) Put(key, value string, timeout time.Duration) error {
	return s.propose(Command{Op: OpPut, Key: key, Value: value}, timeout)
}

func (s *Server) Delete(key string, timeout time.Duration) error {
	return s.propose(Command{Op: OpDelete, Key: key}, timeout)
}

func (s *Server) propose(cmd Command, timeout time.Duration) error {
	data, err := encodeCommand(cmd)
	if err != nil {
		return err
	}

	var idx quorum.LogIndex
	var proposeErr error
	s.driver.Do(func(n *quorum.Node) {
		idx, _, proposeErr = n.Propose(data)
	})
	if proposeErr != nil {
		return proposeErr
	}

	pw := &pendingWrite{data: data, done: make(chan error, 1)}
	s.mu.Lock()
	s.pendingWrite[idx] = pw
	s.mu.Unlock()

	select {
	case err := <-pw.done:
		return err
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.pendingWrite, idx)
		s.mu.Unlock()
		return ErrTimeout
	}
}

// Get performs a linearizable read: it waits for ReadIndex confirmation
// — proof a majority still recognizes this node as leader as of after
// the call, and that this node's own state has caught up to everything
// committed as of that point — before reading local state at all.
func (s *Server) Get(key string, timeout time.Duration) (string, bool, error) {
	var id uint64
	var err error
	s.driver.Do(func(n *quorum.Node) {
		id, err = n.RequestReadIndex()
	})
	if err != nil {
		return "", false, err
	}

	done := make(chan struct{})
	s.mu.Lock()
	s.pendingRead[id] = done
	s.mu.Unlock()

	select {
	case <-done:
		v, ok := s.store.Get(key)
		return v, ok, nil
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.pendingRead, id)
		s.mu.Unlock()
		return "", false, ErrTimeout
	}
}

// Compact snapshots current state and asks quorum to fold the log up
// through everything applied so far. Safe to call at any time —
// lastAppliedIndex is, by construction, never ahead of what's actually
// committed, so this can never accidentally compact something that
// could still be rolled back by a future leader change.
func (s *Server) Compact() error {
	data, err := s.store.Snapshot()
	if err != nil {
		return err
	}
	s.mu.Lock()
	idx := s.lastAppliedIndex
	s.mu.Unlock()
	if idx == 0 {
		return nil
	}
	var compactErr error
	s.driver.Do(func(n *quorum.Node) {
		_, compactErr = n.CompactLog(idx, data)
	})
	return compactErr
}

func (s *Server) onCommitted(e quorum.LogEntry) {
	if e.Type == quorum.EntryNormal && len(e.Data) > 0 {
		if cmd, err := decodeCommand(e.Data); err == nil {
			s.store.Apply(cmd)
		}
		// A decode failure here would mean something wrote a
		// non-Command entry through this Node — not possible through
		// Server's own API, so silently skipping is fine; this isn't
		// meant to be a defensive boundary against a misused Node.
	}
	// EntryConfigChange entries and empty-Data no-ops are correctly
	// skipped — there's nothing for the KV state machine to apply.

	s.mu.Lock()
	if e.Index > s.lastAppliedIndex {
		s.lastAppliedIndex = e.Index
	}
	pw, ok := s.pendingWrite[e.Index]
	if ok {
		delete(s.pendingWrite, e.Index)
	}
	s.mu.Unlock()

	if !ok {
		return
	}
	if bytes.Equal(pw.data, e.Data) {
		pw.done <- nil
	} else {
		pw.done <- ErrProposalSuperseded
	}
}

func (s *Server) onSnapshot(snap *quorum.Snapshot) {
	_ = s.store.Restore(snap.Data)
	s.mu.Lock()
	if snap.Index > s.lastAppliedIndex {
		s.lastAppliedIndex = snap.Index
	}
	s.mu.Unlock()
}

func (s *Server) onReadReady(r quorum.ReadResult) {
	s.mu.Lock()
	done, ok := s.pendingRead[r.ID]
	if ok {
		delete(s.pendingRead, r.ID)
	}
	s.mu.Unlock()
	if ok {
		close(done)
	}
}
