package fsm

import (
	"fmt"
	"testing"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func skip(t *testing.T, f *FSM, idx uint64, s *pb.SinkSkip) {
	t.Helper()
	b, err := EncodeSinkSkip(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func TestSinkSkips(t *testing.T) {
	f := New()
	// Without skips the snapshot stays at version 7, which v0.6 reads.
	if snap := mustSnapshot(t, f); snap[0] != 7 {
		t.Fatalf("version %d without skips", snap[0])
	}
	for i := range MaxSkipsKept + 5 {
		skip(t, f, uint64(i+1), &pb.SinkSkip{Sink: "hook", Index: uint64(100 + i), Id: fmt.Sprintf("0/1:%d", i), Error: "rejected"})
	}
	// A retried proposal of the last one counts once.
	skip(t, f, 500, &pb.SinkSkip{Sink: "hook", Index: 100 + MaxSkipsKept + 4, Id: fmt.Sprintf("0/1:%d", MaxSkipsKept+4)})
	sk := f.SinkSkips("hook")
	if sk.Total != MaxSkipsKept+5 || len(sk.Recent) != MaxSkipsKept || sk.Recent[0].GetIndex() != 105 {
		t.Fatalf("total %d, %d kept, first %v", sk.Total, len(sk.Recent), sk.Recent[0])
	}
	if f.SinkSkips("other").Total != 0 {
		t.Fatal("skips of another sink")
	}
	snap := mustSnapshot(t, f)
	if snap[0] != 8 {
		t.Fatalf("version %d with skips", snap[0])
	}
	g := New()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if got := g.SinkSkips("hook"); got.Total != sk.Total || len(got.Recent) != MaxSkipsKept || got.Recent[MaxSkipsKept-1].GetId() != sk.Recent[MaxSkipsKept-1].GetId() {
		t.Fatalf("restored %d %d", got.Total, len(got.Recent))
	}
	if again := mustSnapshot(t, g); string(again) != string(snap) {
		t.Fatal("snapshot not deterministic")
	}
	// Copies.
	sk.Recent[0].Error = "changed"
	if f.SinkSkips("hook").Recent[0].GetError() != "rejected" {
		t.Fatal("shared state")
	}
	// A skip whose body names another sink is corrupt.
	b, _ := EncodeSinkSkip(&pb.SinkSkip{Sink: "hook", Index: 1})
	b2 := append([]byte{byte(KindMeta), byte(opSinkSkip)}, appendBytes(nil, []byte("other"))...)
	b2 = append(b2, b[len(b)-len(b)+2+1+len("hook"):]...)
	if err := f.Apply(raft.Entry{Index: 600, Type: raft.EntryNormal, Data: b2}); err == nil {
		t.Fatal("mismatched sink accepted")
	}
	if _, err := EncodeSinkSkip(&pb.SinkSkip{Sink: "Bad"}); err == nil {
		t.Fatal("bad name accepted")
	}
}
