package kv

import (
	"bytes"
	"encoding/gob"
)

// OpType is the kind of mutation a Command represents.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
)

// Command is what Server.propose actually hands to quorum.Node.Propose —
// the log entry's Data, gob-encoded for the same reason the wire
// transport uses gob: a handful of simple fields, no crash-safety
// framing to earn here the way the WAL's format does.
type Command struct {
	Op    OpType
	Key   string
	Value string
}

func encodeCommand(c Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (Command, error) {
	var c Command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c)
	return c, err
}
