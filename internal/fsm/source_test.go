package fsm

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func applyBatch(t *testing.T, f *FSM, idx uint64, tb *pb.TxBatch) {
	t.Helper()
	b, err := EncodeTxBatch(tb)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func events(n int) []*pb.Event {
	ev := make([]*pb.Event, n)
	for i := range ev {
		ev[i] = &pb.Event{SchemaId: 7, Op: pb.Op_OP_INSERT}
	}
	return ev
}

func TestTxBatchAdvancesSource(t *testing.T) {
	f := New()
	applyBatch(t, f, 1, &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true, Events: events(2)})
	if got := f.Source(); got != (SourceState{CommitLSN: 100, EndLSN: 108}) {
		t.Fatalf("after one tx: %+v", got)
	}

	// A large transaction in three parts.
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 200, EndLsn: 208, FirstSeq: 0, Events: events(3)})
	if got := f.Source(); got.PartialLSN != 200 || got.PartialNextSeq != 3 || got.CommitLSN != 100 {
		t.Fatalf("after part 1: %+v", got)
	}
	applyBatch(t, f, 3, &pb.TxBatch{CommitLsn: 200, EndLsn: 208, FirstSeq: 3, Events: events(3)})
	applyBatch(t, f, 4, &pb.TxBatch{CommitLsn: 200, EndLsn: 208, FirstSeq: 6, Final: true, Events: events(1)})
	if got := f.Source(); got != (SourceState{CommitLSN: 200, EndLSN: 208}) {
		t.Fatalf("after final part: %+v", got)
	}
}

func TestTxBatchAnomaliesAreIgnored(t *testing.T) {
	f := New()
	applyBatch(t, f, 1, &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true})
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 300, EndLsn: 308, Events: events(2)}) // partial

	bad := []*pb.TxBatch{
		{CommitLsn: 100, Final: true},              // duplicate of a stored tx
		{CommitLsn: 50, Final: true},               // older tx
		{CommitLsn: 400, Final: true},              // other tx while 300 is partial
		{CommitLsn: 300, FirstSeq: 0, Final: true}, // replays part 1
		{CommitLsn: 300, FirstSeq: 5, Final: true}, // skips events
	}
	for i, tb := range bad {
		applyBatch(t, f, uint64(3+i), tb)
	}
	want := SourceState{CommitLSN: 100, EndLSN: 108, PartialLSN: 300, PartialNextSeq: 2, Anomalies: uint64(len(bad))}
	if got := f.Source(); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	// A fresh transaction must start at sequence 0.
	g := New()
	applyBatch(t, g, 1, &pb.TxBatch{CommitLsn: 10, FirstSeq: 3, Final: true})
	if g.Source().Anomalies != 1 || g.Source().CommitLSN != 0 {
		t.Fatalf("%+v", g.Source())
	}
}

func testSchema() *pb.Schema {
	s := &pb.Schema{RelationOid: 16384, Namespace: "public", Name: "orders", ReplicaIdentity: 'd',
		Columns: []*pb.Column{{Name: "id", TypeOid: 23, TypeModifier: -1, Key: true}, {Name: "note", TypeOid: 25, TypeModifier: -1}}}
	s.Id = SchemaID(s)
	return s
}

func TestSchemaID(t *testing.T) {
	a, b := testSchema(), testSchema()
	if a.Id == 0 || a.Id != b.Id {
		t.Fatalf("ids %d %d", a.Id, b.Id)
	}
	if SchemaID(a) != a.Id {
		t.Fatal("id depends on the id field")
	}
	b.Columns[1].Name = "comment"
	if SchemaID(b) == a.Id {
		t.Fatal("different schemas share an id")
	}
}

func TestSchemaRegistryAndSnapshot(t *testing.T) {
	f := New()
	sc := testSchema()
	put(t, f, 1, "k", "v")
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true, Schemas: []*pb.Schema{sc}, Events: events(1)})
	applyBatch(t, f, 3, &pb.TxBatch{CommitLsn: 200, EndLsn: 208, Events: events(4)})
	got, ok := f.Schema(sc.Id)
	if !ok || !proto.Equal(got, sc) || !f.HasSchema(sc.Id) || f.HasSchema(sc.Id+1) {
		t.Fatalf("schema lookup: %v %v", got, ok)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if g.Source() != f.Source() || !g.HasSchema(sc.Id) || g.AppliedIndex() != 3 {
		t.Fatalf("restored source %+v schema %v", g.Source(), g.HasSchema(sc.Id))
	}
	if v, _ := g.Get("k"); string(v) != "v" {
		t.Fatal("kv lost in snapshot")
	}
	again, _ := g.Snapshot()
	if !bytes.Equal(snap, again) {
		t.Fatal("snapshot is not deterministic")
	}
}

func TestDecodeTxBatch(t *testing.T) {
	tb := &pb.TxBatch{CommitLsn: 5, Final: true, Events: events(2)}
	b, _ := EncodeTxBatch(tb)
	got, ok, err := DecodeTxBatch(b)
	if err != nil || !ok || !proto.Equal(got, tb) {
		t.Fatalf("%v %v %v", got, ok, err)
	}
	meta, _ := EncodePut("k", nil)
	if _, ok, err := DecodeTxBatch(meta); ok || err != nil {
		t.Fatalf("meta decoded as tx batch: %v %v", ok, err)
	}
	if _, ok, err := DecodeTxBatch([]byte{byte(KindTxBatch), 0xff}); !ok || err == nil {
		t.Fatalf("corrupt: %v %v", ok, err)
	}
}

func TestSourceIdentity(t *testing.T) {
	f := New()
	// A v0.2 batch carries no identity; the first one that does sets it.
	applyBatch(t, f, 1, &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true})
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 200, EndLsn: 208, Final: true, SystemId: 42, Timeline: 1})
	if got := f.Source(); got.SystemID != 42 || got.Timeline != 1 || got.CommitLSN != 200 {
		t.Fatalf("%+v", got)
	}
	// A new timeline of the same cluster continues (the source checked it).
	applyBatch(t, f, 3, &pb.TxBatch{CommitLsn: 300, EndLsn: 308, Final: true, SystemId: 42, Timeline: 2})
	// Another cluster is an anomaly, also without identity-less batches
	// changing anything.
	applyBatch(t, f, 4, &pb.TxBatch{CommitLsn: 400, EndLsn: 408, Final: true, SystemId: 7, Timeline: 1})
	applyBatch(t, f, 5, &pb.TxBatch{CommitLsn: 500, EndLsn: 508, Final: true})
	got := f.Source()
	if got.Anomalies != 1 || got.SystemID != 42 || got.Timeline != 2 || got.CommitLSN != 500 {
		t.Fatalf("%+v", got)
	}

	// The identity survives a snapshot.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if g.Source() != got {
		t.Fatalf("restored %+v, want %+v", g.Source(), got)
	}
	// A version 4 snapshot restores without one.
	h := New()
	if err := h.Restore(asVersion4(t, snap)); err != nil {
		t.Fatal(err)
	}
	want := got
	want.SystemID, want.Timeline = 0, 0
	if h.Source() != want {
		t.Fatalf("v4 restored %+v, want %+v", h.Source(), want)
	}
}

func resetBatch(r *pb.SourceReset) *pb.TxBatch {
	return &pb.TxBatch{CommitLsn: max(r.GetLsn(), 1) - 1, EndLsn: r.GetLsn(), Final: true, SystemId: r.GetSystemId(),
		Timeline: r.GetTimeline(), Events: []*pb.Event{{Op: pb.Op_OP_SOURCE_RESET, SourceReset: r}}}
}

func TestSourceResetReplacesThePosition(t *testing.T) {
	f := New()
	applyBatch(t, f, 1, &pb.TxBatch{CommitLsn: 500, EndLsn: 508, Final: true, SystemId: 42, Timeline: 1, Events: events(1)})
	// Part of a large transaction, then a reset backwards to another cluster.
	applyBatch(t, f, 2, &pb.TxBatch{CommitLsn: 600, EndLsn: 608, Events: events(2), SystemId: 42, Timeline: 1})
	applyBatch(t, f, 3, resetBatch(&pb.SourceReset{PreviousLsn: 508, PreviousSystemId: 42, PreviousTimeline: 1,
		Lsn: 300, SystemId: 7, Timeline: 3, Reason: "diverged"}))
	if got := f.Source(); got != (SourceState{CommitLSN: 299, EndLSN: 300, SystemID: 7, Timeline: 3}) {
		t.Fatalf("after reset: %+v", got)
	}
	if f.LastChange() != 3 {
		t.Fatalf("reset is a change for sinks; last change %d", f.LastChange())
	}
	// Transactions of the new cluster after the reset are taken, also one
	// committing exactly at the reset position.
	applyBatch(t, f, 4, &pb.TxBatch{CommitLsn: 300, EndLsn: 340, Final: true, SystemId: 7, Timeline: 3, Events: events(1)})
	if got := f.Source(); got.CommitLSN != 300 || got.Anomalies != 0 {
		t.Fatalf("after reset: %+v", got)
	}
	// An OP_SOURCE_RESET event among others is not a reset.
	tb := &pb.TxBatch{CommitLsn: 1, Events: append(events(1), &pb.Event{Op: pb.Op_OP_SOURCE_RESET, SourceReset: &pb.SourceReset{}})}
	if SourceReset(tb) != nil {
		t.Fatal("mixed batch taken as a reset")
	}
}
