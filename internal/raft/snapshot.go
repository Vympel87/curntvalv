package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// ErrCorruptSnapshot is returned when a stored snapshot fails its checksum.
var ErrCorruptSnapshot = errors.New("raft: corrupt snapshot")

const snapSuffix = ".snap"

// FileSnapshotStore keeps each snapshot in its own file in a directory,
// named <index>-<term>.snap. The file holds the data followed by a CRC-32C of
// it. Files are written to a temporary name, synced and renamed, so a
// crash never leaves a partial snapshot under a real name.
type FileSnapshotStore struct {
	dir    string
	noSync bool
}

// NewFileSnapshotStore returns a store in dir, creating the directory if
// needed. If noSync is true fsync is skipped; only for tests.
func NewFileSnapshotStore(dir string, noSync bool) (*FileSnapshotStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: snapshot dir: %w", err)
	}
	return &FileSnapshotStore{dir: dir, noSync: noSync}, nil
}

func (s *FileSnapshotStore) path(m SnapshotMeta) string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d-%020d%s", m.Index, m.Term, snapSuffix))
}

// List returns the stored snapshots in ascending index order. Leftover
// temporary files are ignored.
func (s *FileSnapshotStore) List() ([]SnapshotMeta, error) {
	des, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("raft: list snapshots: %w", err)
	}
	var out []SnapshotMeta
	for _, de := range des {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, snapSuffix) {
			continue
		}
		idx, term, ok := strings.Cut(strings.TrimSuffix(name, snapSuffix), "-")
		if !ok {
			return nil, fmt.Errorf("%w: unexpected file %q", ErrCorruptSnapshot, name)
		}
		i, err1 := strconv.ParseUint(idx, 10, 64)
		t, err2 := strconv.ParseUint(term, 10, 64)
		if err1 != nil || err2 != nil || i == 0 || t == 0 {
			return nil, fmt.Errorf("%w: unexpected file %q", ErrCorruptSnapshot, name)
		}
		out = append(out, SnapshotMeta{Index: i, Term: t})
	}
	slices.SortFunc(out, func(a, b SnapshotMeta) int { return compareU64(a.Index, b.Index) })
	return out, nil
}

func compareU64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Load returns the data of the snapshot with the given index.
func (s *FileSnapshotStore) Load(index uint64) ([]byte, error) {
	metas, err := s.List()
	if err != nil {
		return nil, err
	}
	for _, m := range metas {
		if m.Index != index {
			continue
		}
		b, err := os.ReadFile(s.path(m))
		if err != nil {
			return nil, fmt.Errorf("raft: load snapshot %d: %w", index, err)
		}
		if len(b) < 4 {
			return nil, fmt.Errorf("%w: snapshot %d too short", ErrCorruptSnapshot, index)
		}
		data, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
		if crc32.Checksum(data, castagnoli) != sum {
			return nil, fmt.Errorf("%w: snapshot %d checksum mismatch", ErrCorruptSnapshot, index)
		}
		return data, nil
	}
	return nil, fmt.Errorf("raft: snapshot %d not found", index)
}

// Save durably stores a snapshot. Saving an index that exists replaces it.
func (s *FileSnapshotStore) Save(m SnapshotMeta, data []byte) error {
	b := make([]byte, len(data)+4)
	copy(b, data)
	binary.LittleEndian.PutUint32(b[len(data):], crc32.Checksum(data, castagnoli))

	tmp := s.path(m) + ".tmp"
	if err := writeFileSync(tmp, b, !s.noSync); err != nil {
		return fmt.Errorf("raft: save snapshot %d: %w", m.Index, err)
	}
	if err := os.Rename(tmp, s.path(m)); err != nil {
		return fmt.Errorf("raft: save snapshot %d: %w", m.Index, err)
	}
	return s.syncDir()
}

// DeleteBefore removes every snapshot with an index lower than index.
func (s *FileSnapshotStore) DeleteBefore(index uint64) error {
	metas, err := s.List()
	if err != nil {
		return err
	}
	for _, m := range metas {
		if m.Index >= index {
			break
		}
		if err := os.Remove(s.path(m)); err != nil {
			return fmt.Errorf("raft: delete snapshot %d: %w", m.Index, err)
		}
	}
	return s.syncDir()
}

func (s *FileSnapshotStore) syncDir() error {
	if s.noSync {
		return nil
	}
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return nil
}

func writeFileSync(path string, b []byte, sync bool) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}
