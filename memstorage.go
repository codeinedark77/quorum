package quorum

import "sort"

// MemStorage is a Storage with no crash-safety story at all — data
// vanishes exactly like Node's own in-memory log would. That's the
// point: it exercises the interface and Node's persist-then-send
// discipline without the cost or complexity of real IO. Durability
// itself is FileStorage's job.
type MemStorage struct {
	term          Term
	votedFor      NodeID
	snapshotIndex LogIndex
	snapshotTerm  Term
	log           map[LogIndex]LogEntry
}

func NewMemStorage() *MemStorage {
	return &MemStorage{log: map[LogIndex]LogEntry{}}
}

func (m *MemStorage) SaveHardState(term Term, votedFor NodeID) error {
	m.term, m.votedFor = term, votedFor
	return nil
}

func (m *MemStorage) AppendEntries(entries []LogEntry) error {
	for _, e := range entries {
		m.log[e.Index] = e
	}
	return nil
}

func (m *MemStorage) TruncateFrom(index LogIndex) error {
	for idx := range m.log {
		if idx >= index {
			delete(m.log, idx)
		}
	}
	return nil
}

func (m *MemStorage) Compact(index LogIndex, term Term) error {
	if index <= m.snapshotIndex {
		return nil
	}
	m.snapshotIndex, m.snapshotTerm = index, term
	for idx := range m.log {
		if idx <= index {
			delete(m.log, idx)
		}
	}
	return nil
}

func (m *MemStorage) Load() (Term, NodeID, LogIndex, Term, []LogEntry, error) {
	log := make([]LogEntry, 0, len(m.log))
	for _, e := range m.log {
		log = append(log, e)
	}
	sort.Slice(log, func(i, j int) bool { return log[i].Index < log[j].Index })
	return m.term, m.votedFor, m.snapshotIndex, m.snapshotTerm, log, nil
}
