package fsm

import (
	"errors"
	"testing"
	"time"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func setOffset(t *testing.T, f *FSM, idx uint64, name string, off uint64) {
	t.Helper()
	b, err := EncodeSinkOffset(name, off)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func TestSinkOffsetsOnlyMoveForward(t *testing.T) {
	f := New()
	if _, ok := f.SinkOffset("ch"); ok {
		t.Fatal("offset of unknown sink")
	}
	setOffset(t, f, 1, "ch", 10)
	setOffset(t, f, 2, "ch", 7) // late command from an old leader
	setOffset(t, f, 3, "hook", 2)
	if off, ok := f.SinkOffset("ch"); !ok || off != 10 {
		t.Fatalf("ch = %d %v", off, ok)
	}
	setOffset(t, f, 4, "ch", 12)
	got := f.SinkOffsets()
	if len(got) != 2 || got["ch"] != 12 || got["hook"] != 2 {
		t.Fatalf("offsets %v", got)
	}
	got["ch"] = 0 // a copy
	if off, _ := f.SinkOffset("ch"); off != 12 {
		t.Fatal("SinkOffsets returned the internal map")
	}
}

func TestSinkOffsetValidation(t *testing.T) {
	for _, name := range []string{"", "Upper", "-dash", "has space", string(make([]byte, 64))} {
		if _, err := EncodeSinkOffset(name, 1); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: got %v", name, err)
		}
	}
	for _, name := range []string{"a", "clickhouse_1", "web-hook"} {
		if !ValidSinkName(name) {
			t.Errorf("%q rejected", name)
		}
	}
	f := New()
	b, _ := EncodeSinkOffset("ok", 1)
	err := f.Apply(raft.Entry{Index: 1, Type: raft.EntryNormal, Data: b[:len(b)-1]})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short offset: %v", err)
	}
}

func TestSnapshotKeepsSinkOffsets(t *testing.T) {
	f := New()
	put(t, f, 1, "k", "v")
	setOffset(t, f, 2, "b", 5)
	setOffset(t, f, 3, "a", 1<<40)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := New()
	setOffset(t, g, 1, "stale", 3)
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	got := g.SinkOffsets()
	if len(got) != 2 || got["a"] != 1<<40 || got["b"] != 5 {
		t.Fatalf("restored offsets %v", got)
	}
}

// A version 2 snapshot (M4, no sink section) still restores.
func TestRestoreVersion2Snapshot(t *testing.T) {
	f := New()
	put(t, f, 1, "k", "v")
	v3, _ := f.Snapshot()
	v2 := append([]byte{2}, v3[1:len(v3)-9]...) // drop the empty sink section and last change
	g := New()
	if err := g.Restore(v2); err != nil {
		t.Fatal(err)
	}
	if v, ok := g.Get("k"); !ok || string(v) != "v" || g.AppliedIndex() != 1 {
		t.Fatalf("k = %q %v, applied %d", v, ok, g.AppliedIndex())
	}
}

func TestAppliedNotifiesWaiters(t *testing.T) {
	f := New()
	idx, ch := f.Applied()
	if idx != 0 {
		t.Fatalf("applied %d", idx)
	}
	select {
	case <-ch:
		t.Fatal("closed before any apply")
	default:
	}
	b, _ := EncodePut("k", []byte("v"))
	go f.Apply(raft.Entry{Index: 1, Term: 1, Type: raft.EntryNormal, Data: b})
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("not notified on Apply")
	}
	idx, ch = f.Applied()
	if idx != 1 {
		t.Fatalf("applied %d", idx)
	}
	snap, _ := f.Snapshot()
	if err := New().Restore(snap); err != nil {
		t.Fatal(err)
	}
	if err := f.Restore(snap); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("not notified on Restore")
	}
}

func TestLastChange(t *testing.T) {
	f := New()
	applyBatch(t, f, 1, &pb.TxBatch{CommitLsn: 10, Final: true, Events: events(2)})
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 20, EndLsn: 21, Final: true}) // idle position, no changes
	setOffset(t, f, 3, "ch", 1)
	if f.LastChange() != 1 {
		t.Fatalf("last change %d", f.LastChange())
	}
	applyBatch(t, f, 4, &pb.TxBatch{CommitLsn: 5, Final: true, Events: events(1)}) // anomaly: ignored
	if f.LastChange() != 1 {
		t.Fatalf("anomaly counted: %d", f.LastChange())
	}
	snap, _ := f.Snapshot()
	g := New()
	if err := g.Restore(snap); err != nil || g.LastChange() != 1 {
		t.Fatalf("restored last change %d %v", g.LastChange(), err)
	}
}
