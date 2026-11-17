package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// LogStore is durable storage for the Raft log. Entries are opaque byte
// slices addressed by a contiguous index starting at 1; raft encodes and
// decodes them itself. *wal.Log satisfies this interface.
//
// Append must not return before the entries are on stable storage.
// CompactBefore may keep some entries below index (FirstIndex tells what is
// left). Reset(next) empties the log so the next entry gets index next; if
// the process crashes during Reset the log may reopen as a prefix of its old
// content, which raft detects and repairs on start.
type LogStore interface {
	Append(entries ...[]byte) (uint64, error)
	Read(index uint64) ([]byte, error)
	FirstIndex() uint64
	LastIndex() uint64
	TruncateAfter(index uint64) error
	CompactBefore(index uint64) error
	Reset(next uint64) error
}

// SnapshotMeta identifies a snapshot: the state machine after applying every
// entry up to and including Index, whose term is Term.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// SnapshotStore durably stores state machine snapshots. Save must not return
// before the snapshot is on stable storage and must be atomic: after a crash
// a snapshot is either fully there or not listed.
type SnapshotStore interface {
	// List returns the stored snapshots in ascending index order.
	List() ([]SnapshotMeta, error)
	Load(index uint64) ([]byte, error)
	Save(meta SnapshotMeta, data []byte) error
	// DeleteBefore removes every snapshot with an index lower than index.
	DeleteBefore(index uint64) error
}

// HardState is the state Raft must persist before it answers an RPC
// (Figure 2, "persistent state on all servers"; the log is stored separately).
type HardState struct {
	Term     uint64
	VotedFor uint64 // 0 means no vote in Term
}

// StableStore durably stores the HardState. Save must not return before the
// state is on stable storage.
type StableStore interface {
	Load() (HardState, error)
	Save(hs HardState) error
}

// ErrCorruptState is returned when the stored HardState cannot be read.
var ErrCorruptState = errors.New("raft: corrupt hard state")

const hardStateSize = 20 // term u64 | votedFor u64 | crc32c u32

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// FileStableStore stores the HardState in a single small file. Save writes a
// temporary file, syncs it and renames it over the old one, so the file on
// disk is always either the old or the new state, never a mix.
type FileStableStore struct {
	path   string
	noSync bool
}

// NewFileStableStore returns a store that keeps its state in path. The
// directory must exist. If noSync is true fsync is skipped; only for tests.
func NewFileStableStore(path string, noSync bool) *FileStableStore {
	return &FileStableStore{path: path, noSync: noSync}
}

// Load returns the stored state, or the zero HardState if none was saved.
func (s *FileStableStore) Load() (HardState, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, fmt.Errorf("raft: load hard state: %w", err)
	}
	if len(b) != hardStateSize {
		return HardState{}, fmt.Errorf("%w: %d bytes", ErrCorruptState, len(b))
	}
	if crc32.Checksum(b[:16], castagnoli) != binary.LittleEndian.Uint32(b[16:20]) {
		return HardState{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptState)
	}
	return HardState{
		Term:     binary.LittleEndian.Uint64(b[0:8]),
		VotedFor: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}

// Save durably replaces the stored state with hs.
func (s *FileStableStore) Save(hs HardState) error {
	var b [hardStateSize]byte
	binary.LittleEndian.PutUint64(b[0:8], hs.Term)
	binary.LittleEndian.PutUint64(b[8:16], hs.VotedFor)
	binary.LittleEndian.PutUint32(b[16:20], crc32.Checksum(b[:16], castagnoli))

	tmp := s.path + ".tmp"
	if err := writeFileSync(tmp, b[:], !s.noSync); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	if s.noSync {
		return nil
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	return nil
}
