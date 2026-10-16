package quorum

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
)

// FileStorage is a durable Storage backed by a single append-only file:
// a sequence of length-prefixed, CRC32-checksummed records. Truncation
// is recorded as its own record rather than actually rewriting the file —
// replay reconstructs the final state by processing records in the order
// they were written and keeping whatever a later record says wins.
//
// A crash mid-write can leave a torn, unreadable record at the tail.
// Load recovers everything up to that point and silently discards the
// torn remainder — the same crash-safe-replay policy as any WAL that
// has to survive a process dying mid-fsync.
type FileStorage struct {
	mu   sync.Mutex
	file *os.File
}

func OpenFileStorage(path string) (*FileStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStorage{file: f}, nil
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.file.Close()
}

func (fs *FileStorage) SaveHardState(term Term, votedFor NodeID) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeRecord(fs.file, recHardState, encodeHardState(term, votedFor)); err != nil {
		return err
	}
	return fs.file.Sync()
}

func (fs *FileStorage) AppendEntries(entries []LogEntry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, e := range entries {
		if err := writeRecord(fs.file, recAppend, encodeEntry(e)); err != nil {
			return err
		}
	}
	return fs.file.Sync()
}

func (fs *FileStorage) TruncateFrom(index LogIndex) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeRecord(fs.file, recTruncate, encodeTruncate(index)); err != nil {
		return err
	}
	return fs.file.Sync()
}

func (fs *FileStorage) Compact(index LogIndex, term Term) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeRecord(fs.file, recCompact, encodeCompact(index, term)); err != nil {
		return err
	}
	return fs.file.Sync()
}

func (fs *FileStorage) Load() (Term, NodeID, LogIndex, Term, []LogEntry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, err := fs.file.Seek(0, io.SeekStart); err != nil {
		return 0, "", 0, 0, nil, err
	}
	r := bufio.NewReader(fs.file)

	var term Term
	var votedFor NodeID
	var snapshotIndex LogIndex
	var snapshotTerm Term
	byIndex := map[LogIndex]LogEntry{}

	var goodOffset int64
	for {
		recType, payload, bodyLen, ok := readRecord(r)
		if !ok {
			break
		}
		switch recType {
		case recHardState:
			term, votedFor = decodeHardState(payload)
		case recAppend:
			e := decodeEntry(payload)
			byIndex[e.Index] = e
		case recTruncate:
			from := decodeTruncate(payload)
			for idx := range byIndex {
				if idx >= from {
					delete(byIndex, idx)
				}
			}
		case recCompact:
			idx, t := decodeCompact(payload)
			if idx > snapshotIndex {
				snapshotIndex, snapshotTerm = idx, t
			}
			for idx := range byIndex {
				if idx <= snapshotIndex {
					delete(byIndex, idx)
				}
			}
		}
		goodOffset += 8 + int64(bodyLen) // 8-byte header + body
	}

	// Trim a torn tail so it doesn't sit as permanent garbage in the
	// middle of the file, corrupting every future replay the same way.
	if err := fs.file.Truncate(goodOffset); err != nil {
		return 0, "", 0, 0, nil, err
	}
	if _, err := fs.file.Seek(0, io.SeekEnd); err != nil {
		return 0, "", 0, 0, nil, err
	}

	log := make([]LogEntry, 0, len(byIndex))
	for _, e := range byIndex {
		log = append(log, e)
	}
	sort.Slice(log, func(i, j int) bool { return log[i].Index < log[j].Index })

	return term, votedFor, snapshotIndex, snapshotTerm, log, nil
}

// --- on-disk record format ---
//
// [4 bytes length (of type+payload)] [4 bytes CRC32(type+payload)] [1 byte type] [payload]

const (
	recHardState byte = 1
	recAppend    byte = 2
	recTruncate  byte = 3
	recCompact   byte = 4

	maxRecordLen = 64 * 1024 * 1024 // sanity bound against a corrupt length field driving a huge allocation
)

func writeRecord(w io.Writer, recType byte, payload []byte) error {
	body := make([]byte, 1+len(payload))
	body[0] = recType
	copy(body[1:], payload)

	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(body))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// readRecord reads one record. ok=false covers both a clean end of file
// and a torn/corrupt trailing record — from the caller's perspective
// both mean the same thing: nothing more can be reliably recovered past
// this point.
func readRecord(r *bufio.Reader) (recType byte, payload []byte, bodyLen int, ok bool) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, 0, false
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])
	if length < 1 || length > maxRecordLen {
		return 0, nil, 0, false
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, 0, false
	}
	if crc32.ChecksumIEEE(body) != wantCRC {
		return 0, nil, 0, false
	}
	return body[0], body[1:], len(body), true
}

func encodeHardState(term Term, votedFor NodeID) []byte {
	buf := make([]byte, 8+4+len(votedFor))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(term))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(votedFor)))
	copy(buf[12:], votedFor)
	return buf
}

func decodeHardState(b []byte) (Term, NodeID) {
	term := Term(binary.LittleEndian.Uint64(b[0:8]))
	n := binary.LittleEndian.Uint32(b[8:12])
	return term, NodeID(b[12 : 12+n])
}

func encodeEntry(e LogEntry) []byte {
	buf := make([]byte, 8+8+1+4+len(e.Data))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(e.Index))
	buf[16] = byte(e.Type)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(e.Data)))
	copy(buf[21:], e.Data)
	return buf
}

func decodeEntry(b []byte) LogEntry {
	term := Term(binary.LittleEndian.Uint64(b[0:8]))
	index := LogIndex(binary.LittleEndian.Uint64(b[8:16]))
	typ := EntryType(b[16])
	n := binary.LittleEndian.Uint32(b[17:21])
	data := append([]byte(nil), b[21:21+n]...) // copy: b may share a reused buffer
	return LogEntry{Term: term, Index: index, Type: typ, Data: data}
}

func encodeTruncate(from LogIndex) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(from))
	return buf
}

func decodeTruncate(b []byte) LogIndex {
	return LogIndex(binary.LittleEndian.Uint64(b))
}

func encodeCompact(index LogIndex, term Term) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(index))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(term))
	return buf
}

func decodeCompact(b []byte) (LogIndex, Term) {
	index := LogIndex(binary.LittleEndian.Uint64(b[0:8]))
	term := Term(binary.LittleEndian.Uint64(b[8:16]))
	return index, term
}
