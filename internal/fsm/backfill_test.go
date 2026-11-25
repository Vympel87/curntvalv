package fsm

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func backfillCmd(t *testing.T, f *FSM, idx uint64, cmd *pb.BackfillCommand) {
	t.Helper()
	b, err := EncodeBackfill(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func key(vs ...string) [][]byte {
	var k [][]byte
	for _, v := range vs {
		k = append(k, []byte(v))
	}
	return k
}

func chunk(lsn uint64, table string, id uint64, after, last [][]byte, done bool, rows int) *pb.TxBatch {
	evs := events(rows)
	for _, e := range evs {
		e.Op = pb.Op_OP_BACKFILL
	}
	return &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 8, Final: true, Events: evs,
		Backfill: &pb.BackfillChunk{Table: table, Id: id, AfterKey: after, LastKey: last, Done: done}}
}

func backfill(t *testing.T, f *FSM, table string) *pb.Backfill {
	t.Helper()
	for _, bf := range f.Backfills() {
		if bf.GetTable() == table {
			return bf
		}
	}
	t.Fatalf("no backfill of %s", table)
	return nil
}

func TestBackfillProgress(t *testing.T) {
	f := New()
	backfillCmd(t, f, 1, &pb.BackfillCommand{Start: []string{"public.orders", "public.users"}})
	applyBatch(t, f, 2, chunk(100, "public.orders", 1, nil, key("10"), false, 10))
	// A chunk that does not continue the stored key (an old leader's, or a
	// repeated one) stores its rows but no progress.
	applyBatch(t, f, 3, chunk(200, "public.orders", 1, nil, key("10"), false, 10))
	applyBatch(t, f, 4, chunk(300, "public.orders", 1, key("10"), key("15"), false, 5))
	if bf := backfill(t, f, "public.orders"); bf.GetRows() != 15 || string(bf.GetLastKey()[0]) != "15" ||
		bf.GetState() != pb.BackfillState_BACKFILL_STATE_PENDING {
		t.Fatalf("%v", bf)
	}
	if f.LastChange() != 4 {
		t.Fatalf("last change %d", f.LastChange())
	}
	// Starting a pending backfill again keeps it.
	backfillCmd(t, f, 5, &pb.BackfillCommand{Start: []string{"public.orders"}})
	// The last chunk has no rows; the backfill is done.
	applyBatch(t, f, 6, chunk(400, "public.orders", 1, key("15"), nil, true, 0))
	bf := backfill(t, f, "public.orders")
	if bf.GetState() != pb.BackfillState_BACKFILL_STATE_DONE || bf.GetEndIndex() != 6 || bf.GetRows() != 15 || string(bf.GetLastKey()[0]) != "15" {
		t.Fatalf("%v", bf)
	}
	if f.LastChange() != 4 {
		t.Fatalf("an empty chunk is not a change; last change %d", f.LastChange())
	}
	// A done backfill starts again from the beginning.
	backfillCmd(t, f, 7, &pb.BackfillCommand{Start: []string{"public.orders"}})
	if bf := backfill(t, f, "public.orders"); bf.GetId() != 7 || bf.GetRows() != 0 || len(bf.GetLastKey()) != 0 {
		t.Fatalf("restart: %v", bf)
	}
	// A chunk of the previous run counts no more.
	applyBatch(t, f, 8, chunk(500, "public.orders", 1, nil, key("3"), false, 3))
	if bf := backfill(t, f, "public.orders"); bf.GetRows() != 0 {
		t.Fatalf("old chunk counted: %v", bf)
	}
	// Ordered by start.
	if bfs := f.Backfills(); len(bfs) != 2 || bfs[0].GetTable() != "public.users" || bfs[1].GetTable() != "public.orders" {
		t.Fatalf("%v", bfs)
	}
}

func TestBackfillCancelAndFail(t *testing.T) {
	f := New()
	backfillCmd(t, f, 1, &pb.BackfillCommand{Start: []string{"public.a", "public.b"}})
	backfillCmd(t, f, 2, &pb.BackfillCommand{Cancel: []string{"public.a", "public.nope"}})
	if bf := backfill(t, f, "public.a"); bf.GetState() != pb.BackfillState_BACKFILL_STATE_CANCELLED || bf.GetEndIndex() != 2 {
		t.Fatalf("%v", bf)
	}
	applyBatch(t, f, 3, chunk(100, "public.a", 1, nil, key("1"), true, 1))
	if bf := backfill(t, f, "public.a"); bf.GetState() != pb.BackfillState_BACKFILL_STATE_CANCELLED || bf.GetRows() != 0 {
		t.Fatalf("chunk after cancel: %v", bf)
	}
	// A failure for another run of the table is ignored.
	backfillCmd(t, f, 4, &pb.BackfillCommand{FailTable: "public.b", FailId: 99, FailError: "x"})
	backfillCmd(t, f, 5, &pb.BackfillCommand{FailTable: "public.b", FailId: 1, FailError: "table dropped"})
	if bf := backfill(t, f, "public.b"); bf.GetState() != pb.BackfillState_BACKFILL_STATE_FAILED || bf.GetError() != "table dropped" || bf.GetEndIndex() != 5 {
		t.Fatalf("%v", bf)
	}
	for _, name := range []string{"orders", "", ".x", "x.", string(make([]byte, 200)) + ".x"} {
		if _, err := EncodeBackfill(&pb.BackfillCommand{Start: []string{name}}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: %v", name, err)
		}
	}
	// A backfill command under another key is corrupt.
	b := append([]byte{byte(KindMeta), byte(opBackfill)}, appendBytes(nil, []byte("other"))...)
	if err := f.Apply(raft.Entry{Index: 6, Type: raft.EntryNormal, Data: b}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("other key: %v", err)
	}
}

func TestSnapshotKeepsBackfills(t *testing.T) {
	f := New()
	backfillCmd(t, f, 1, &pb.BackfillCommand{Start: []string{"public.orders", "s.t"}})
	applyBatch(t, f, 2, chunk(100, "public.orders", 1, nil, key("7", "a"), false, 4))
	backfillCmd(t, f, 3, &pb.BackfillCommand{Cancel: []string{"s.t"}})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	want, got := f.Backfills(), g.Backfills()
	if len(got) != 2 || !proto.Equal(got[0], want[0]) || !proto.Equal(got[1], want[1]) {
		t.Fatalf("restored %v, want %v", got, want)
	}
	again, _ := g.Snapshot()
	if string(again) != string(snap) {
		t.Fatal("snapshot not deterministic")
	}
	// A version 5 snapshot has none.
	h := New()
	if err := h.Restore(asVersion5(t, mustSnapshot(t, New()))); err != nil || len(h.Backfills()) != 0 {
		t.Fatalf("v5: %v %v", err, h.Backfills())
	}
	// Returned backfills are copies.
	got[0].Rows = 1000
	if backfill(t, g, "public.orders").GetRows() != 4 {
		t.Fatal("Backfills returned shared state")
	}
}

func mustSnapshot(t *testing.T, f *FSM) []byte {
	t.Helper()
	b, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
