// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm (Extended Version)"
// by Ongaro and Ousterhout: leader election (§5.2), log replication (§5.3)
// and the safety rules of §5.4.
//
// The package has two layers:
//
//   - raft (unexported) is the protocol state machine. It is synchronous and
//     single-threaded: tick, step and propose are plain method calls. Tests
//     drive it directly with a deterministic fake network.
//   - Node wraps it in one goroutine (Run) that owns all Raft state and is
//     fed through channels: incoming messages, proposals and clock ticks.
//
// Raft talks to the outside world only through LogStore, StableStore,
// Transport and Applier.
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// EntryType tells normal entries apart from the no-op a new leader writes.
type EntryType uint8

const (
	// EntryNormal carries data proposed by the application.
	EntryNormal EntryType = 1
	// EntryNoop is appended by a leader at the start of its term (§8) so that
	// entries from earlier terms can be committed.
	EntryNoop EntryType = 2
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoop:
		return "noop"
	default:
		return fmt.Sprintf("EntryType(%d)", uint8(t))
	}
}

// Entry is one entry of the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// ErrCorruptEntry is returned when a stored entry cannot be decoded.
var ErrCorruptEntry = errors.New("raft: corrupt log entry")

// entryHeaderSize is the size of the fixed part of an encoded entry.
const entryHeaderSize = 9

// encodeEntry returns the on-disk form of e:
//
//	| term uint64 LE | type uint8 | data |
//
// The index is not stored; it is the position of the record in the LogStore.
func encodeEntry(e Entry) []byte {
	b := make([]byte, entryHeaderSize+len(e.Data))
	binary.LittleEndian.PutUint64(b[0:8], e.Term)
	b[8] = byte(e.Type)
	copy(b[entryHeaderSize:], e.Data)
	return b
}

// decodeEntry is the inverse of encodeEntry. Data aliases b.
func decodeEntry(index uint64, b []byte) (Entry, error) {
	if len(b) < entryHeaderSize {
		return Entry{}, fmt.Errorf("%w: index %d: %d bytes", ErrCorruptEntry, index, len(b))
	}
	e := Entry{
		Index: index,
		Term:  binary.LittleEndian.Uint64(b[0:8]),
		Type:  EntryType(b[8]),
		Data:  b[entryHeaderSize:],
	}
	if e.Type != EntryNormal && e.Type != EntryNoop {
		return Entry{}, fmt.Errorf("%w: index %d: unknown type %d", ErrCorruptEntry, index, b[8])
	}
	if e.Term == 0 {
		return Entry{}, fmt.Errorf("%w: index %d: term 0", ErrCorruptEntry, index)
	}
	return e, nil
}
