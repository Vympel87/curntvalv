package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func entry(i uint64) []byte { return []byte(fmt.Sprintf("entry-%06d", i)) }

func mustOpen(t *testing.T, dir string, segSize int64) *Log {
	t.Helper()
	l, err := Open(dir, Options{SegmentSize: segSize, NoSync: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func appendN(t *testing.T, l *Log, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		got, err := l.Append(entry(i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if got != i {
			t.Fatalf("append returned index %d, want %d", got, i)
		}
	}
}

func checkRange(t *testing.T, l *Log, first, last uint64) {
	t.Helper()
	if got := l.FirstIndex(); got != first {
		t.Fatalf("FirstIndex = %d, want %d", got, first)
	}
	if got := l.LastIndex(); got != last {
		t.Fatalf("LastIndex = %d, want %d", got, last)
	}
	for i := first; i <= last; i++ {
		b, err := l.Read(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(b) != string(entry(i)) {
			t.Fatalf("read %d = %q, want %q", i, b, entry(i))
		}
	}
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEmptyLog(t *testing.T) {
	l := mustOpen(t, t.TempDir(), 0)
	checkRange(t, l, 1, 0)
	if _, err := l.Read(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read on empty log: %v, want ErrNotFound", err)
	}
}

func TestAppendReadReopen(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200) // tiny segments to force rotation
	appendN(t, l, 1, 100)
	checkRange(t, l, 1, 100)
	if n := len(segmentFiles(t, dir)); n < 5 {
		t.Fatalf("expected many segments, got %d", n)
	}
	l.Close()

	l = mustOpen(t, dir, 200)
	checkRange(t, l, 1, 100)
	appendN(t, l, 101, 150)
	checkRange(t, l, 1, 150)
}

func TestBatchAppendAcrossSegments(t *testing.T) {
	l := mustOpen(t, t.TempDir(), 100)
	var batch [][]byte
	for i := uint64(1); i <= 50; i++ {
		batch = append(batch, entry(i))
	}
	last, err := l.Append(batch...)
	if err != nil || last != 50 {
		t.Fatalf("append batch: last=%d err=%v", last, err)
	}
	checkRange(t, l, 1, 50)
}

func TestEmptyPayload(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 0)
	if _, err := l.Append([]byte{}, []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l = mustOpen(t, dir, 0)
	if l.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", l.LastIndex())
	}
	if b, err := l.Read(1); err != nil || len(b) != 0 {
		t.Fatalf("read 1: %q %v", b, err)
	}
}

func TestTornTailIsRepaired(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 0)
	appendN(t, l, 1, 10)
	l.Close()

	// Simulate a crash in the middle of writing entry 11: a full header
	// that promises more payload than was written.
	files := segmentFiles(t, dir)
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{50, 0, 0, 0, 1, 2, 3, 4, 'h', 'a', 'l', 'f'})
	f.Close()

	l = mustOpen(t, dir, 0)
	checkRange(t, l, 1, 10)
	appendN(t, l, 11, 20)
	l.Close()
	l = mustOpen(t, dir, 0)
	checkRange(t, l, 1, 20)
}

func TestZeroFilledTailIsRepaired(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 0)
	appendN(t, l, 1, 5)
	l.Close()
	files := segmentFiles(t, dir)
	f, _ := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	f.Write(make([]byte, 64))
	f.Close()

	l = mustOpen(t, dir, 0)
	checkRange(t, l, 1, 5)
}

func TestCorruptionInOldSegmentIsAnError(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	appendN(t, l, 1, 100)
	l.Close()

	files := segmentFiles(t, dir)
	f, _ := os.OpenFile(files[0], os.O_WRONLY, 0)
	f.WriteAt([]byte("XX"), headerSize+2) // flip payload bytes of the first entry
	f.Close()

	if _, err := Open(dir, Options{SegmentSize: 200, NoSync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v, want ErrCorrupt", err)
	}
}

func TestMissingSegmentIsAnError(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	appendN(t, l, 1, 100)
	l.Close()
	files := segmentFiles(t, dir)
	os.Remove(files[len(files)/2])
	if _, err := Open(dir, Options{SegmentSize: 200, NoSync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v, want ErrCorrupt", err)
	}
}

func TestTruncateAfter(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	appendN(t, l, 1, 100)

	if err := l.TruncateAfter(500); err != nil { // no-op
		t.Fatal(err)
	}
	if err := l.TruncateAfter(42); err != nil {
		t.Fatal(err)
	}
	checkRange(t, l, 1, 42)
	if _, err := l.Read(43); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read 43: %v, want ErrNotFound", err)
	}
	appendN(t, l, 43, 60) // new entries reuse the indexes
	l.Close()
	l = mustOpen(t, dir, 200)
	checkRange(t, l, 1, 60)

	// Truncate at every possible point, including segment boundaries.
	for i := uint64(59); ; i-- {
		if err := l.TruncateAfter(i); err != nil {
			t.Fatalf("truncate after %d: %v", i, err)
		}
		checkRange(t, l, 1, i)
		if i == 0 {
			break
		}
	}
	appendN(t, l, 1, 5)
	checkRange(t, l, 1, 5)
}

func TestCompactBefore(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	appendN(t, l, 1, 100)

	if err := l.CompactBefore(50); err != nil {
		t.Fatal(err)
	}
	first := l.FirstIndex()
	if first <= 1 || first > 50 {
		t.Fatalf("FirstIndex after compaction = %d, want in (1, 50]", first)
	}
	checkRange(t, l, first, 100)
	if _, err := l.Read(first - 1); !errors.Is(err, ErrCompacted) {
		t.Fatalf("read compacted: %v, want ErrCompacted", err)
	}
	if err := l.TruncateAfter(first - 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("truncate into compacted range: %v, want ErrCompacted", err)
	}

	// Compacting everything keeps the active segment.
	if err := l.CompactBefore(1000); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 100 {
		t.Fatalf("LastIndex = %d, want 100", l.LastIndex())
	}
	l.Close()
	l = mustOpen(t, dir, 200)
	checkRange(t, l, l.FirstIndex(), 100)
	appendN(t, l, 101, 110)
}

func TestReset(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	appendN(t, l, 1, 100)
	if err := l.Reset(501); err != nil {
		t.Fatal(err)
	}
	checkRange(t, l, 501, 500)
	if n := len(segmentFiles(t, dir)); n != 1 {
		t.Fatalf("segments after reset = %d, want 1", n)
	}
	appendN(t, l, 501, 520)
	l.Close()
	l = mustOpen(t, dir, 200)
	checkRange(t, l, 501, 520)
}

func TestClosed(t *testing.T) {
	l := mustOpen(t, t.TempDir(), 0)
	l.Close()
	if _, err := l.Append([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append: %v, want ErrClosed", err)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("read: %v, want ErrClosed", err)
	}
}

func TestWithFsync(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 1, 30)
	if err := l.TruncateAfter(20); err != nil {
		t.Fatal(err)
	}
	if err := l.CompactBefore(10); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = mustOpen(t, dir, 200)
	checkRange(t, l, l.FirstIndex(), 20)
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	l := mustOpen(t, t.TempDir(), 500)
	const total = 2000
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				last := l.LastIndex()
				for i := uint64(1); i <= last; i += 7 {
					b, err := l.Read(i)
					if err != nil || string(b) != string(entry(i)) {
						t.Errorf("read %d: %q %v", i, b, err)
						return
					}
				}
				if last == total {
					return
				}
			}
		}()
	}
	appendN(t, l, 1, total)
	wg.Wait()
}

func TestSegments(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, 200)
	before := time.Now().Add(-time.Second)
	appendN(t, l, 1, 40)
	segs := l.Segments()
	if len(segs) < 3 || segs[0].First != 1 || segs[len(segs)-1].Last != 40 {
		t.Fatalf("segments %+v", segs)
	}
	var total int64
	for i, s := range segs {
		if i > 0 && s.First != segs[i-1].Last+1 {
			t.Fatalf("gap in %+v", segs)
		}
		if s.Bytes <= 0 || s.Written.Before(before) {
			t.Fatalf("segment %+v", s)
		}
		total += s.Bytes
	}
	files := segmentFiles(t, dir)
	var onDisk int64
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		onDisk += st.Size()
	}
	if total != onDisk {
		t.Fatalf("bytes %d, on disk %d", total, onDisk)
	}

	// After reopening, the write time comes from the file.
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(files[0], old, old); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l = mustOpen(t, dir, 200)
	if got := l.Segments()[0].Written; !got.Equal(old) {
		t.Fatalf("written %v, want %v", got, old)
	}
	if err := l.CompactBefore(segs[1].First); err != nil {
		t.Fatal(err)
	}
	if got := l.Segments(); len(got) != len(segs)-1 || got[0].First != segs[1].First {
		t.Fatalf("after compaction %+v", got)
	}
}
