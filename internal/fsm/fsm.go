// Package fsm is curntValv's replicated state machine. It receives committed
// Raft entries and keeps the cluster metadata (design D1, D5): for now a
// key-value map; later the source LSN, sink offsets, pipeline config and the
// schema registry.
//
// Every normal Raft entry carries an envelope:
//
//	| kind uint8 | body |
//
// KindMeta bodies are metadata commands. KindTxBatch bodies are protobuf
// pb.TxBatch messages: the row changes of one Postgres transaction (or a
// part of it) together with the source position they advance to.
package fsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Kind is the type of a log entry payload.
type Kind uint8

const (
	KindMeta    Kind = 1
	KindTxBatch Kind = 2
)

type op uint8

const (
	opPut    op = 1
	opDelete op = 2
)

const (
	// MaxKeySize is the longest allowed key in bytes.
	MaxKeySize = 1 << 10
	// MaxValueSize is the largest allowed value in bytes.
	MaxValueSize = 1 << 20

	snapshotVersion = 2
)

var (
	// ErrInvalid is returned for a key or value that may not be stored.
	ErrInvalid = errors.New("fsm: invalid command")
	// ErrCorrupt is returned for an entry or snapshot that cannot be decoded.
	// Only our own code writes them, so this means a bug or bad storage.
	ErrCorrupt = errors.New("fsm: corrupt data")
)

// EncodePut returns the log payload that sets key to value.
func EncodePut(key string, value []byte) ([]byte, error) {
	if err := validate(key, value); err != nil {
		return nil, err
	}
	b := []byte{byte(KindMeta), byte(opPut)}
	b = appendBytes(b, []byte(key))
	return append(b, value...), nil
}

// EncodeDelete returns the log payload that removes key.
func EncodeDelete(key string) ([]byte, error) {
	if err := validate(key, nil); err != nil {
		return nil, err
	}
	b := []byte{byte(KindMeta), byte(opDelete)}
	return appendBytes(b, []byte(key)), nil
}

func validate(key string, value []byte) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: empty key", ErrInvalid)
	case len(key) > MaxKeySize:
		return fmt.Errorf("%w: key longer than %d bytes", ErrInvalid, MaxKeySize)
	case len(value) > MaxValueSize:
		return fmt.Errorf("%w: value larger than %d bytes", ErrInvalid, MaxValueSize)
	}
	return nil
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func readBytes(b []byte) (v, rest []byte, ok bool) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return nil, nil, false
	}
	return b[k : k+int(n)], b[k+int(n):], true
}

// FSM is the metadata state machine. It implements raft.Applier. Apply,
// Snapshot and Restore are called by the Raft goroutine; Get and the other
// readers may be called concurrently from anywhere.
type FSM struct {
	mu      sync.RWMutex
	kv      map[string][]byte
	src     SourceState
	schemas map[uint64]*pb.Schema
	applied uint64
}

// New returns an empty state machine.
func New() *FSM {
	return &FSM{kv: map[string][]byte{}, schemas: map[uint64]*pb.Schema{}}
}

// Apply applies one committed entry.
func (f *FSM) Apply(e raft.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e.Type == raft.EntryNormal {
		if err := f.applyPayload(e.Data); err != nil {
			return fmt.Errorf("fsm: entry %d: %w", e.Index, err)
		}
	}
	f.applied = e.Index
	return nil
}

func (f *FSM) applyPayload(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("%w: empty payload", ErrCorrupt)
	}
	switch Kind(b[0]) {
	case KindMeta:
		return f.applyMeta(b[1:])
	case KindTxBatch:
		return f.applyTxBatch(b[1:])
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrCorrupt, b[0])
	}
}

func (f *FSM) applyMeta(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("%w: empty meta command", ErrCorrupt)
	}
	key, rest, ok := readBytes(b[1:])
	if !ok || len(key) == 0 {
		return fmt.Errorf("%w: bad key", ErrCorrupt)
	}
	switch op(b[0]) {
	case opPut:
		f.kv[string(key)] = slices.Clone(rest)
	case opDelete:
		if len(rest) != 0 {
			return fmt.Errorf("%w: trailing bytes in delete", ErrCorrupt)
		}
		delete(f.kv, string(key))
	default:
		return fmt.Errorf("%w: unknown meta op %d", ErrCorrupt, b[0])
	}
	return nil
}

// Get returns the value stored under key.
func (f *FSM) Get(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.kv[key]
	return slices.Clone(v), ok
}

// Keys returns all keys in sorted order.
func (f *FSM) Keys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	keys := make([]string, 0, len(f.kv))
	for k := range f.kv {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// AppliedIndex returns the index of the last applied entry.
func (f *FSM) AppliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Snapshot encodes the whole state. The encoding is deterministic: the same
// state always gives the same bytes.
//
//	| version u8 | applied u64 | count uvarint | (key, value)* |
//	| commit_lsn u64 | end_lsn u64 | partial_lsn u64 | partial_next_seq u32 |
//	| anomalies u64 | schema count uvarint | schema* |
//
// with keys in sorted order, schemas in id order, and each key, value and
// schema (protobuf) length-prefixed.
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	keys := make([]string, 0, len(f.kv))
	for k := range f.kv {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	b := []byte{snapshotVersion}
	b = binary.LittleEndian.AppendUint64(b, f.applied)
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, f.kv[k])
	}
	b = binary.LittleEndian.AppendUint64(b, f.src.CommitLSN)
	b = binary.LittleEndian.AppendUint64(b, f.src.EndLSN)
	b = binary.LittleEndian.AppendUint64(b, f.src.PartialLSN)
	b = binary.LittleEndian.AppendUint32(b, f.src.PartialNextSeq)
	b = binary.LittleEndian.AppendUint64(b, f.src.Anomalies)
	ids := make([]uint64, 0, len(f.schemas))
	for id := range f.schemas {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		sb, err := marshalOpts.Marshal(f.schemas[id])
		if err != nil {
			return nil, fmt.Errorf("fsm: snapshot schema %d: %w", id, err)
		}
		b = appendBytes(b, sb)
	}
	return b, nil
}

// Restore replaces the whole state with a snapshot.
func (f *FSM) Restore(b []byte) error {
	if len(b) < 9 || b[0] != snapshotVersion {
		return fmt.Errorf("%w: bad snapshot header", ErrCorrupt)
	}
	applied := binary.LittleEndian.Uint64(b[1:9])
	n, k := binary.Uvarint(b[9:])
	if k <= 0 {
		return fmt.Errorf("%w: bad snapshot count", ErrCorrupt)
	}
	rest := b[9+k:]
	kv := make(map[string][]byte, min(n, 1<<16))
	for range n {
		key, r, ok := readBytes(rest)
		if !ok {
			return fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
		}
		val, r, ok := readBytes(r)
		if !ok {
			return fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
		}
		kv[string(key)] = slices.Clone(val)
		rest = r
	}
	if len(rest) < 36 {
		return fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
	}
	src := SourceState{
		CommitLSN:      binary.LittleEndian.Uint64(rest[0:8]),
		EndLSN:         binary.LittleEndian.Uint64(rest[8:16]),
		PartialLSN:     binary.LittleEndian.Uint64(rest[16:24]),
		PartialNextSeq: binary.LittleEndian.Uint32(rest[24:28]),
		Anomalies:      binary.LittleEndian.Uint64(rest[28:36]),
	}
	rest = rest[36:]
	ns, k := binary.Uvarint(rest)
	if k <= 0 {
		return fmt.Errorf("%w: bad schema count", ErrCorrupt)
	}
	rest = rest[k:]
	schemas := make(map[uint64]*pb.Schema, min(ns, 1<<16))
	for range ns {
		sb, r, ok := readBytes(rest)
		if !ok {
			return fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
		}
		sc := &pb.Schema{}
		if err := proto.Unmarshal(sb, sc); err != nil {
			return fmt.Errorf("%w: snapshot schema: %v", ErrCorrupt, err)
		}
		schemas[sc.GetId()] = sc
		rest = r
	}
	if len(rest) != 0 {
		return fmt.Errorf("%w: trailing bytes in snapshot", ErrCorrupt)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv, f.applied, f.src, f.schemas = kv, applied, src, schemas
	return nil
}
