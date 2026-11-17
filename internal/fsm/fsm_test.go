package fsm

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Vympel87/curntvalv/internal/raft"
)

var _ raft.Applier = (*FSM)(nil)

func put(t *testing.T, f *FSM, idx uint64, k, v string) {
	t.Helper()
	b, err := EncodePut(k, []byte(v))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func del(t *testing.T, f *FSM, idx uint64, k string) {
	t.Helper()
	b, err := EncodeDelete(k)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func TestPutGetDelete(t *testing.T) {
	f := New()
	put(t, f, 1, "a", "1")
	put(t, f, 2, "b", "")
	put(t, f, 3, "a", "2")
	if err := f.Apply(raft.Entry{Index: 4, Term: 2, Type: raft.EntryNoop}); err != nil {
		t.Fatal(err)
	}
	del(t, f, 5, "b")
	del(t, f, 6, "missing")

	if v, ok := f.Get("a"); !ok || string(v) != "2" {
		t.Fatalf("a = %q %v", v, ok)
	}
	if _, ok := f.Get("b"); ok {
		t.Fatal("b still present")
	}
	if f.AppliedIndex() != 6 || strings.Join(f.Keys(), ",") != "a" {
		t.Fatalf("applied %d keys %v", f.AppliedIndex(), f.Keys())
	}
	// Returned values are copies.
	v, _ := f.Get("a")
	v[0] = 'x'
	if v2, _ := f.Get("a"); string(v2) != "2" {
		t.Fatal("Get leaked internal storage")
	}
}

func TestValidation(t *testing.T) {
	if _, err := EncodePut("", nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("empty key: %v", err)
	}
	if _, err := EncodePut(strings.Repeat("k", MaxKeySize+1), nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("long key: %v", err)
	}
	if _, err := EncodePut("k", make([]byte, MaxValueSize+1)); !errors.Is(err, ErrInvalid) {
		t.Errorf("big value: %v", err)
	}
	if _, err := EncodeDelete(""); !errors.Is(err, ErrInvalid) {
		t.Errorf("delete empty key: %v", err)
	}
}

func TestCorruptEntriesAreErrors(t *testing.T) {
	good, _ := EncodePut("key", []byte("v"))
	badDelete, _ := EncodeDelete("key")
	badDelete = append(badDelete, 'x')
	cases := map[string][]byte{
		"empty":         {},
		"unknown kind":  {99},
		"txbatch in M3": {byte(KindTxBatch)},
		"empty meta":    {byte(KindMeta)},
		"unknown op":    {byte(KindMeta), 9, 1, 'k'},
		"bad length":    good[:3],
		"delete+junk":   badDelete,
	}
	for name, b := range cases {
		f := New()
		err := f.Apply(raft.Entry{Index: 1, Term: 1, Type: raft.EntryNormal, Data: b})
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	f := New()
	put(t, f, 1, "z", "last")
	put(t, f, 2, "a", "first")
	put(t, f, 3, "empty", "")
	put(t, f, 4, "bin", "\x00\xff")
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	g := New()
	put(t, g, 1, "stale", "x")
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.Get("stale"); ok {
		t.Fatal("Restore kept old state")
	}
	if g.AppliedIndex() != 4 || strings.Join(g.Keys(), ",") != "a,bin,empty,z" {
		t.Fatalf("applied %d keys %v", g.AppliedIndex(), g.Keys())
	}
	if v, _ := g.Get("bin"); string(v) != "\x00\xff" {
		t.Fatalf("bin = %q", v)
	}
	// Deterministic: restoring and snapshotting again gives the same bytes.
	again, _ := g.Snapshot()
	if !bytes.Equal(snap, again) {
		t.Fatal("snapshot is not deterministic")
	}
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	f := New()
	put(t, f, 1, "k", "v")
	snap, _ := f.Snapshot()
	for name, b := range map[string][]byte{
		"empty":     nil,
		"version":   append([]byte{9}, snap[1:]...),
		"truncated": snap[:len(snap)-1],
		"trailing":  append(append([]byte{}, snap...), 0),
	} {
		g := New()
		put(t, g, 1, "keep", "me")
		if err := g.Restore(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: got %v", name, err)
		}
		if _, ok := g.Get("keep"); !ok {
			t.Errorf("%s: failed Restore changed state", name)
		}
	}
}

func TestConcurrentReadsDuringApply(t *testing.T) {
	f := New()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					f.Get("k")
					f.Keys()
					f.AppliedIndex()
				}
			}
		}()
	}
	for i := uint64(1); i <= 2000; i++ {
		put(t, f, i, "k", "v")
	}
	close(stop)
	wg.Wait()
}
