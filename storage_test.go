package quorum

import (
	"os"
	"testing"
)

func testStorageConformance(t *testing.T, s Storage) {
	t.Helper()

	if err := s.SaveHardState(5, "peer-b"); err != nil {
		t.Fatalf("SaveHardState failed: %v", err)
	}
	if err := s.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Data: []byte("one")},
		{Term: 1, Index: 2, Data: []byte("two")},
		{Term: 2, Index: 3, Data: []byte("three")},
	}); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	term, votedFor, snapIdx, snapTerm, log, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if term != 5 || votedFor != "peer-b" {
		t.Fatalf("expected term=5 votedFor=peer-b, got term=%d votedFor=%s", term, votedFor)
	}
	if snapIdx != 0 || snapTerm != 0 {
		t.Fatalf("expected no compaction yet, got snapIdx=%d snapTerm=%d", snapIdx, snapTerm)
	}
	if len(log) != 3 {
		t.Fatalf("expected 3 entries, got %d: %+v", len(log), log)
	}
	for i, want := range []string{"one", "two", "three"} {
		if string(log[i].Data) != want {
			t.Fatalf("entry %d: got %q want %q", i, log[i].Data, want)
		}
	}

	// Truncate from index 2 on, then persist a different index 2 — the
	// exact durable-storage side of §5.3 conflict resolution.
	if err := s.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom failed: %v", err)
	}
	if err := s.AppendEntries([]LogEntry{{Term: 3, Index: 2, Data: []byte("replacement")}}); err != nil {
		t.Fatalf("AppendEntries after truncate failed: %v", err)
	}

	_, _, _, _, log, err = s.Load()
	if err != nil {
		t.Fatalf("Load after truncate failed: %v", err)
	}
	if len(log) != 2 {
		t.Fatalf("expected 2 entries after truncate+replace, got %d: %+v", len(log), log)
	}
	if string(log[0].Data) != "one" {
		t.Fatalf("expected index 1 untouched, got %+v", log[0])
	}
	if log[1].Term != 3 || string(log[1].Data) != "replacement" {
		t.Fatalf("expected index 2 replaced with the term-3 entry, got %+v", log[1])
	}

	// Compact through index 2 — index 1 should disappear entirely, only
	// the boundary metadata should remain for it.
	if err := s.Compact(2, 3); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	_, _, snapIdx, snapTerm, log, err = s.Load()
	if err != nil {
		t.Fatalf("Load after compact failed: %v", err)
	}
	if snapIdx != 2 || snapTerm != 3 {
		t.Fatalf("expected compaction boundary (2,3), got (%d,%d)", snapIdx, snapTerm)
	}
	if len(log) != 0 {
		t.Fatalf("expected no discrete entries left after compacting through the last one, got %+v", log)
	}
}

func TestMemStorageConformance(t *testing.T) {
	testStorageConformance(t, NewMemStorage())
}

func TestMemStorageCompactIgnoresRegression(t *testing.T) {
	m := NewMemStorage()
	if err := m.AppendEntries([]LogEntry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if err := m.Compact(2, 2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if err := m.Compact(1, 1); err != nil { // a regression — should be ignored, not applied
		t.Fatalf("Compact failed: %v", err)
	}
	_, _, snapIdx, snapTerm, _, err := m.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if snapIdx != 2 || snapTerm != 2 {
		t.Fatalf("expected the boundary to stay at (2,2), a regressive Compact call shouldn't move it backward, got (%d,%d)", snapIdx, snapTerm)
	}
}

func TestFileStorageConformance(t *testing.T) {
	fs, err := OpenFileStorage(t.TempDir() + "/wal.log")
	if err != nil {
		t.Fatalf("OpenFileStorage failed: %v", err)
	}
	defer fs.Close()
	testStorageConformance(t, fs)
}

func TestFileStorageReturnsErrorsAfterClose(t *testing.T) {
	fs, err := OpenFileStorage(t.TempDir() + "/wal.log")
	if err != nil {
		t.Fatalf("OpenFileStorage failed: %v", err)
	}
	if err := fs.SaveHardState(1, "x"); err != nil {
		t.Fatalf("SaveHardState failed: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := fs.SaveHardState(2, "y"); err == nil {
		t.Fatal("expected SaveHardState to fail on a closed file")
	}
	if err := fs.AppendEntries([]LogEntry{{Term: 1, Index: 1}}); err == nil {
		t.Fatal("expected AppendEntries to fail on a closed file")
	}
	if err := fs.TruncateFrom(1); err == nil {
		t.Fatal("expected TruncateFrom to fail on a closed file")
	}
	if err := fs.Compact(1, 1); err == nil {
		t.Fatal("expected Compact to fail on a closed file")
	}
	if _, _, _, _, _, err := fs.Load(); err == nil {
		t.Fatal("expected Load to fail on a closed file")
	}
}

func TestOpenFileStorageFailsOnInvalidPath(t *testing.T) {
	if _, err := OpenFileStorage("/nonexistent-dir-xyz/wal.log"); err == nil {
		t.Fatal("expected an error opening a file in a nonexistent directory")
	}
}

func TestFileStorageRecoversFromTornWrite(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	fs, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("OpenFileStorage failed: %v", err)
	}
	if err := fs.SaveHardState(3, "x"); err != nil {
		t.Fatalf("SaveHardState failed: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{{Term: 1, Index: 1, Data: []byte("safe")}}); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate a crash mid-write: a length prefix so large it's
	// obviously corrupt, followed by a few bytes that never form a
	// complete record.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("failed to reopen for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("failed to write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close corrupted file: %v", err)
	}

	fs2, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer fs2.Close()

	term, votedFor, _, _, log, err := fs2.Load()
	if err != nil {
		t.Fatalf("Load should recover past the torn tail, not error: %v", err)
	}
	if term != 3 || votedFor != "x" {
		t.Fatalf("expected the last good hard state to survive, got term=%d votedFor=%s", term, votedFor)
	}
	if len(log) != 1 || string(log[0].Data) != "safe" {
		t.Fatalf("expected the one good entry to survive, got %+v", log)
	}

	// The corrupt tail must have been trimmed — writing something new
	// now shouldn't leave garbage stranded mid-file, which would corrupt
	// every future replay the same way.
	if err := fs2.AppendEntries([]LogEntry{{Term: 3, Index: 2, Data: []byte("after-recovery")}}); err != nil {
		t.Fatalf("AppendEntries after recovery failed: %v", err)
	}
	if err := fs2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	fs3, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("final reopen failed: %v", err)
	}
	defer fs3.Close()
	_, _, _, _, log, err = fs3.Load()
	if err != nil {
		t.Fatalf("final Load failed: %v", err)
	}
	if len(log) != 2 || string(log[1].Data) != "after-recovery" {
		t.Fatalf("expected both entries to survive a second reopen, got %+v", log)
	}
}

func TestFileStorageCompactionSurvivesReopen(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	fs, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("OpenFileStorage failed: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 2, Index: 3, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if err := fs.Compact(2, 1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	fs2, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer fs2.Close()

	_, _, snapIdx, snapTerm, log, err := fs2.Load()
	if err != nil {
		t.Fatalf("Load after reopen failed: %v", err)
	}
	if snapIdx != 2 || snapTerm != 1 {
		t.Fatalf("expected compaction boundary (2,1) to survive reopen, got (%d,%d)", snapIdx, snapTerm)
	}
	if len(log) != 1 || string(log[0].Data) != "c" {
		t.Fatalf("expected only index 3 to survive compaction+reopen, got %+v", log)
	}
}
