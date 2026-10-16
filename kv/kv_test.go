package kv

import "testing"

func TestKVStorePutGetDelete(t *testing.T) {
	s := NewKVStore()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected a missing key to report not-found")
	}
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("expected a=1, got %q ok=%v", v, ok)
	}
	s.Apply(Command{Op: OpPut, Key: "a", Value: "2"})
	v, ok = s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("expected a Put to overwrite, got %q ok=%v", v, ok)
	}
	s.Apply(Command{Op: OpDelete, Key: "a"})
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected a to be gone after delete")
	}
}

func TestKVStoreSnapshotRestoreRoundTrips(t *testing.T) {
	s := NewKVStore()
	s.Apply(Command{Op: OpPut, Key: "x", Value: "1"})
	s.Apply(Command{Op: OpPut, Key: "y", Value: "2"})

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	restored := NewKVStore()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if restored.Len() != 2 {
		t.Fatalf("expected 2 entries after restore, got %d", restored.Len())
	}
	if v, ok := restored.Get("x"); !ok || v != "1" {
		t.Fatalf("expected x=1, got %q ok=%v", v, ok)
	}
	if v, ok := restored.Get("y"); !ok || v != "2" {
		t.Fatalf("expected y=2, got %q ok=%v", v, ok)
	}
}

func TestKVStoreRestoreReplacesRatherThanMerges(t *testing.T) {
	s := NewKVStore()
	s.Apply(Command{Op: OpPut, Key: "old", Value: "stale"})

	empty := NewKVStore()
	data, _ := empty.Snapshot()

	if err := s.Restore(data); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("expected Restore to wholesale-replace state, got %d leftover entries", s.Len())
	}
}

func TestCommandEncodeDecodeRoundTrips(t *testing.T) {
	cmd := Command{Op: OpPut, Key: "hello", Value: "world"}
	data, err := encodeCommand(cmd)
	if err != nil {
		t.Fatalf("encodeCommand failed: %v", err)
	}
	got, err := decodeCommand(data)
	if err != nil {
		t.Fatalf("decodeCommand failed: %v", err)
	}
	if got != cmd {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, cmd)
	}
}
