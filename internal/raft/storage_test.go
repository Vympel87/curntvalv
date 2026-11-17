package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Vympel87/curntvalv/internal/wal"
)

var _ LogStore = (*wal.Log)(nil)

func TestEntryEncoding(t *testing.T) {
	for _, e := range []Entry{
		{Index: 1, Term: 1, Type: EntryNoop},
		{Index: 7, Term: 1 << 40, Type: EntryNormal, Data: []byte("hello")},
		{Index: 9, Term: 3, Type: EntryNormal, Data: []byte{}},
	} {
		got, err := decodeEntry(e.Index, encodeEntry(e))
		if err != nil {
			t.Fatal(err)
		}
		if got.Index != e.Index || got.Term != e.Term || got.Type != e.Type || !bytes.Equal(got.Data, e.Data) {
			t.Fatalf("round trip: got %+v, want %+v", got, e)
		}
	}
}

func TestEntryDecodeRejectsGarbage(t *testing.T) {
	good := encodeEntry(Entry{Term: 2, Type: EntryNormal})
	badType := bytes.Clone(good)
	badType[8] = 99
	zeroTerm := encodeEntry(Entry{Term: 0, Type: EntryNormal})
	for name, b := range map[string][]byte{"short": good[:5], "type": badType, "term": zeroTerm} {
		if _, err := decodeEntry(1, b); !errors.Is(err, ErrCorruptEntry) {
			t.Errorf("%s: got %v, want ErrCorruptEntry", name, err)
		}
	}
}

func TestFileStableStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := NewFileStableStore(path, false)
	hs, err := s.Load()
	if err != nil || hs != (HardState{}) {
		t.Fatalf("empty load: %+v %v", hs, err)
	}
	for _, want := range []HardState{{Term: 1, VotedFor: 3}, {Term: 1 << 50, VotedFor: 0}} {
		if err := s.Save(want); err != nil {
			t.Fatal(err)
		}
		got, err := NewFileStableStore(path, false).Load()
		if err != nil || got != want {
			t.Fatalf("got %+v %v, want %+v", got, err, want)
		}
	}
}

func TestFileStableStoreCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := NewFileStableStore(path, true)
	if err := s.Save(HardState{Term: 4, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	b[3] ^= 0xff
	os.WriteFile(path, b, 0o644)
	if _, err := s.Load(); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("flipped bit: got %v", err)
	}
	os.WriteFile(path, b[:7], 0o644)
	if _, err := s.Load(); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("short file: got %v", err)
	}
}

// A crash between writing the temp file and renaming it leaves the old state.
func TestFileStableStoreLeftoverTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := NewFileStableStore(path, true)
	if err := s.Save(HardState{Term: 4, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(path+".tmp", []byte("torn"), 0o644)
	if hs, err := s.Load(); err != nil || hs != (HardState{Term: 4, VotedFor: 2}) {
		t.Fatalf("got %+v %v", hs, err)
	}
	if err := s.Save(HardState{Term: 5}); err != nil {
		t.Fatal(err)
	}
	if hs, _ := s.Load(); hs != (HardState{Term: 5}) {
		t.Fatalf("got %+v", hs)
	}
}
