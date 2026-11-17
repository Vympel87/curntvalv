// Package wal implements the segmented, append-only log that curntValv stores
// everything in. Entries are opaque byte slices addressed by a contiguous
// uint64 index (the Raft index). The first entry of a fresh log has index 1.
//
// On-disk layout: a directory of segment files named after the index of
// their first entry (00000000000000000001.seg). Each record is
//
//	| length uint32 LE | crc32c uint32 LE | payload |
//
// where the checksum covers the length bytes and the payload.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	headerSize    = 8
	segmentSuffix = ".seg"

	// DefaultSegmentSize is the size at which the active segment is rotated.
	DefaultSegmentSize = 64 << 20
	// MaxEntrySize is the largest payload a single entry may have.
	MaxEntrySize = 256 << 20
)

var (
	ErrClosed    = errors.New("wal: log is closed")
	ErrNotFound  = errors.New("wal: entry not found")
	ErrCompacted = errors.New("wal: entry has been compacted")
	ErrCorrupt   = errors.New("wal: log is corrupt")
	ErrTooLarge  = errors.New("wal: entry too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checksum(lenBytes, payload []byte) uint32 {
	return crc32.Update(crc32.Update(0, castagnoli, lenBytes), castagnoli, payload)
}

// Options configures a Log. The zero value is usable.
type Options struct {
	// SegmentSize is the size in bytes at which a new segment is started.
	// Defaults to DefaultSegmentSize.
	SegmentSize int64
	// NoSync disables fsync. Only for tests and benchmarks: with NoSync a
	// crash can lose acknowledged entries.
	NoSync bool
}

type segment struct {
	first   uint64 // index of the first entry in this segment
	path    string
	f       *os.File
	offsets []int64 // offsets[i] is the file offset of entry first+i
	size    int64   // bytes of valid data in the file
}

// last returns the index of the last entry, or first-1 if the segment is empty.
func (s *segment) last() uint64 { return s.first + uint64(len(s.offsets)) - 1 }

// Log is a durable, segmented, append-only log. It is safe for concurrent use.
type Log struct {
	mu     sync.RWMutex
	dir    string
	opts   Options
	segs   []*segment // ordered by first index; never empty; last one is active
	closed bool
}

// Open opens the log in dir, creating it if needed. A torn or corrupt tail in
// the newest segment (the result of a crash during a write) is cut off.
// Corruption anywhere else returns ErrCorrupt.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var firsts []uint64
	for _, de := range names {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
		if err != nil || first == 0 {
			return nil, fmt.Errorf("%w: unexpected file %q", ErrCorrupt, name)
		}
		firsts = append(firsts, first)
	}
	sort.Slice(firsts, func(i, j int) bool { return firsts[i] < firsts[j] })

	l := &Log{dir: dir, opts: opts}
	for i, first := range firsts {
		isLast := i == len(firsts)-1
		s, err := openSegment(l.segmentPath(first), first, isLast, !opts.NoSync)
		if err != nil {
			l.closeFiles()
			return nil, err
		}
		if i > 0 && l.segs[i-1].last()+1 != first {
			s.f.Close()
			l.closeFiles()
			return nil, fmt.Errorf("%w: gap before segment %d", ErrCorrupt, first)
		}
		l.segs = append(l.segs, s)
	}
	if len(l.segs) == 0 {
		if err := l.addSegment(1); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Log) segmentPath(first uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d%s", first, segmentSuffix))
}

// openSegment opens a segment file and rebuilds its offset index by scanning
// it. If repair is true an invalid tail is truncated away, otherwise it is
// reported as ErrCorrupt.
func openSegment(path string, first uint64, repair, sync bool) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := st.Size()
	s := &segment{first: first, path: path, f: f}

	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [headerSize]byte
	var buf []byte
	var off int64
	for off < fileSize {
		if off+headerSize > fileSize {
			break // torn header
		}
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			f.Close()
			return nil, err
		}
		n := int64(binary.LittleEndian.Uint32(hdr[0:4]))
		if n > MaxEntrySize || off+headerSize+n > fileSize {
			break // garbage length or torn payload
		}
		if int64(cap(buf)) < n {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(r, buf); err != nil {
			f.Close()
			return nil, err
		}
		if checksum(hdr[0:4], buf) != binary.LittleEndian.Uint32(hdr[4:8]) {
			break
		}
		s.offsets = append(s.offsets, off)
		off += headerSize + n
	}
	if off < fileSize {
		if !repair {
			f.Close()
			return nil, fmt.Errorf("%w: segment %s at offset %d", ErrCorrupt, filepath.Base(path), off)
		}
		if err := f.Truncate(off); err != nil {
			f.Close()
			return nil, err
		}
		if sync {
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, err
			}
		}
	}
	s.size = off
	return s, nil
}

// addSegment creates a new, empty active segment starting at first.
func (l *Log) addSegment(first uint64) error {
	s, err := openSegment(l.segmentPath(first), first, true, !l.opts.NoSync)
	if err != nil {
		return err
	}
	l.segs = append(l.segs, s)
	return l.syncDir()
}

func (l *Log) syncDir() error {
	if l.opts.NoSync {
		return nil
	}
	d, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (l *Log) active() *segment   { return l.segs[len(l.segs)-1] }
func (l *Log) lastIndex() uint64  { return l.active().last() }
func (l *Log) firstIndex() uint64 { return l.segs[0].first }

func (l *Log) closeFiles() {
	for _, s := range l.segs {
		s.f.Close()
	}
}

// FirstIndex returns the index of the oldest entry still in the log. For an
// empty log it equals LastIndex()+1.
func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex()
}

// LastIndex returns the index of the newest entry, or FirstIndex()-1 if the
// log is empty (0 for a fresh log).
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex()
}

// Append writes entries to the end of the log and returns the index of the
// last one. Unless NoSync is set, the entries are on stable storage when
// Append returns. An error from Append means the log may contain only some
// of the entries; the caller should treat it as fatal.
func (l *Log) Append(entries ...[]byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	for _, e := range entries {
		if len(e) > MaxEntrySize {
			return 0, ErrTooLarge
		}
	}

	var buf []byte
	var offs []int64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		a := l.active()
		if _, err := a.f.WriteAt(buf, a.size); err != nil {
			_ = a.f.Truncate(a.size) // drop a partial write
			return err
		}
		a.offsets = append(a.offsets, offs...)
		a.size += int64(len(buf))
		buf, offs = buf[:0], offs[:0]
		return nil
	}

	for _, e := range entries {
		a := l.active()
		pending := a.size + int64(len(buf))
		if pending >= l.opts.SegmentSize && len(a.offsets)+len(offs) > 0 {
			if err := flush(); err != nil {
				return 0, err
			}
			if !l.opts.NoSync {
				if err := a.f.Sync(); err != nil {
					return 0, err
				}
			}
			if err := l.addSegment(a.last() + 1); err != nil {
				return 0, err
			}
			pending = 0
		}
		offs = append(offs, pending)
		var hdr [headerSize]byte
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(e)))
		binary.LittleEndian.PutUint32(hdr[4:8], checksum(hdr[0:4], e))
		buf = append(buf, hdr[:]...)
		buf = append(buf, e...)
	}
	if err := flush(); err != nil {
		return 0, err
	}
	if !l.opts.NoSync {
		if err := l.active().f.Sync(); err != nil {
			return 0, err
		}
	}
	return l.lastIndex(), nil
}

// Read returns the entry at index. It returns ErrCompacted if the entry was
// removed by CompactBefore or Reset, and ErrNotFound if it is past the end.
func (l *Log) Read(index uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if index < l.firstIndex() {
		return nil, ErrCompacted
	}
	if index > l.lastIndex() {
		return nil, ErrNotFound
	}
	s := l.segs[l.segmentFor(index)]
	k := index - s.first
	start, end := s.offsets[k], s.size
	if int(k)+1 < len(s.offsets) {
		end = s.offsets[k+1]
	}
	rec := make([]byte, end-start)
	if _, err := s.f.ReadAt(rec, start); err != nil {
		return nil, err
	}
	payload := rec[headerSize:]
	if int(binary.LittleEndian.Uint32(rec[0:4])) != len(payload) ||
		checksum(rec[0:4], payload) != binary.LittleEndian.Uint32(rec[4:8]) {
		return nil, fmt.Errorf("%w: entry %d", ErrCorrupt, index)
	}
	return payload, nil
}

// segmentFor returns the position of the last segment whose first index is
// <= index. The caller guarantees firstIndex() <= index.
func (l *Log) segmentFor(index uint64) int {
	return sort.Search(len(l.segs), func(i int) bool { return l.segs[i].first > index }) - 1
}

// TruncateAfter removes every entry with an index greater than index. Raft
// uses it to drop entries that conflict with the leader's log.
func (l *Log) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if index >= l.lastIndex() {
		return nil
	}
	if index+1 < l.firstIndex() {
		return ErrCompacted
	}
	i := l.segmentFor(index + 1)
	// Delete newest first so a crash never leaves a gap.
	for j := len(l.segs) - 1; j > i; j-- {
		s := l.segs[j]
		s.f.Close()
		if err := os.Remove(s.path); err != nil {
			return err
		}
		l.segs = l.segs[:j]
	}
	s := l.segs[i]
	k := index + 1 - s.first
	off := s.offsets[k]
	if err := s.f.Truncate(off); err != nil {
		return err
	}
	s.offsets = s.offsets[:k]
	s.size = off
	if !l.opts.NoSync {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	return l.syncDir()
}

// CompactBefore deletes whole segments that only contain entries with an
// index lower than index. It never splits a segment, so FirstIndex() may
// remain lower than index afterwards. The active segment is never deleted.
func (l *Log) CompactBefore(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	// Delete oldest first so a crash never leaves a gap.
	for len(l.segs) > 1 && l.segs[0].last() < index {
		s := l.segs[0]
		s.f.Close()
		if err := os.Remove(s.path); err != nil {
			return err
		}
		l.segs = l.segs[1:]
	}
	return l.syncDir()
}

// Reset deletes all entries and restarts the log so that the next appended
// entry gets index next. Raft uses it after installing a snapshot. If the
// process crashes during Reset the log reopens as a prefix of its old
// content; the caller must then call Reset again.
func (l *Log) Reset(next uint64) error {
	if next == 0 {
		return errors.New("wal: next index must be >= 1")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	for j := len(l.segs) - 1; j >= 0; j-- {
		s := l.segs[j]
		s.f.Close()
		if err := os.Remove(s.path); err != nil {
			return err
		}
		l.segs = l.segs[:j]
	}
	return l.addSegment(next)
}

// Close syncs and closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if !l.opts.NoSync {
		err = l.active().f.Sync()
	}
	for _, s := range l.segs {
		if cerr := s.f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
