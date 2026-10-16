package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// KVStore is a plain in-memory map with the two operations a Raft-backed
// state machine needs beyond ordinary reads and writes: Snapshot (a
// compact, wholesale representation of current state) and Restore
// (loading one back, whether from this node's own CompactLog call or
// from an InstallSnapshot received from a leader). Everything about
// linearizability, replication, and durability lives in quorum itself —
// this is deliberately as plain as a state machine can be, to keep the
// integration with quorum.Node.Ready() the thing on display.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: map[string]string{}}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Apply mutates local state per cmd. Called only from Server.onCommitted,
// which is only ever invoked on the driver's single goroutine — no
// separate locking discipline needed there, but Get can still be called
// from any goroutine, hence the RWMutex.
func (s *KVStore) Apply(cmd Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
}

// Snapshot returns a compact, wholesale encoding of current state — the
// bytes that become quorum.Node.CompactLog's snapshotData, and later get
// handed to a lagging peer via InstallSnapshot.
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore replaces all local state with what's encoded in data — used
// both to re-supply a snapshot to a restarted Node (via
// NewNodeFromStorage) and to adopt one received via InstallSnapshot
// (via Driver.OnSnapshot).
func (s *KVStore) Restore(data []byte) error {
	var m map[string]string
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return err
	}
	s.mu.Lock()
	s.data = m
	s.mu.Unlock()
	return nil
}
