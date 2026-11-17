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
type LogStore interface {
	Append(entries ...[]byte) (uint64, error)
	Read(index uint64) ([]byte, error)
	LastIndex() uint64
	TruncateAfter(index uint64) error
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
	if err := s.writeFile(tmp, b[:]); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	if s.noSync {
		return nil
	}
	d, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("raft: save hard state: sync dir: %w", err)
	}
	return nil
}

func (s *FileStableStore) writeFile(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if !s.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}
