package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

var (
	_ RaftNode  = (*raft.Node)(nil)
	_ LogReader = (*raft.Node)(nil)
	_ Meta      = (*fsm.FSM)(nil)
)

// fakeNode applies proposals straight to an FSM and keeps them as its log,
// or fails with err.
type fakeNode struct {
	f       *fsm.FSM
	err     error
	readErr error
	index   uint64
	log     []raft.Entry
	first   uint64 // simulated compaction: entries below are gone
}

func (n *fakeNode) Propose(_ context.Context, data []byte) (uint64, error) {
	if n.err != nil {
		return 0, n.err
	}
	n.index++
	e := raft.Entry{Index: n.index, Term: 1, Type: raft.EntryNormal, Data: data}
	n.log = append(n.log, e)
	return n.index, n.f.Apply(e)
}

func (n *fakeNode) ReadCommitted(_ context.Context, from uint64, max int) ([]raft.Entry, error) {
	if from == 0 {
		from = max1(n.first)
	}
	if from < n.first {
		return nil, raft.ErrCompacted
	}
	var out []raft.Entry
	for _, e := range n.log {
		if e.Index >= from && len(out) < max {
			out = append(out, e)
		}
	}
	return out, nil
}

func max1(v uint64) uint64 { return max(v, 1) }

func (n *fakeNode) ReadIndex(context.Context) (uint64, error) {
	if n.readErr != nil {
		return 0, n.readErr
	}
	return n.index, nil
}

func (n *fakeNode) Status(context.Context) (raft.Status, error) {
	return raft.Status{ID: 1, Term: 4, State: raft.StateLeader, Leader: 1, LastIndex: n.index, CommitIndex: n.index, LastApplied: n.index, FirstIndex: max1(n.first)}, nil
}

func newAdmin() (*Admin, *fakeNode) {
	f := fsm.New()
	n := &fakeNode{f: f}
	return NewAdmin(n, n, f, nil, map[uint64]string{1: "a:1", 2: "b:2"}), n
}

func TestPutGetDelete(t *testing.T) {
	a, _ := newAdmin()
	ctx := context.Background()
	pr, err := a.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v")})
	if err != nil || pr.GetIndex() != 1 {
		t.Fatalf("put: %v %v", pr, err)
	}
	gr, err := a.Get(ctx, &pb.GetRequest{Key: "k"})
	if err != nil || !gr.GetFound() || string(gr.GetValue()) != "v" || gr.GetReadIndex() != 1 {
		t.Fatalf("get: %v %v", gr, err)
	}
	if _, err := a.Delete(ctx, &pb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatal(err)
	}
	gr, _ = a.Get(ctx, &pb.GetRequest{Key: "k"})
	if gr.GetFound() {
		t.Fatal("found after delete")
	}
	st, err := a.Status(ctx, &pb.StatusRequest{})
	if err != nil || st.GetState() != "leader" || st.GetLeaderAddr() != "a:1" || st.GetTerm() != 4 {
		t.Fatalf("status: %v %v", st, err)
	}
}

type fakeHealth struct{}

func (fakeHealth) Source() (string, string) { return "retrying", "pg: connect: refused" }
func (fakeHealth) Sinks() []*pb.SinkStatus {
	return []*pb.SinkStatus{{Name: "ch", Type: "clickhouse", State: "running"}, {Name: "new", Type: "webhook", State: "running"}}
}

func TestStatusReportsHealth(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f}
	a := NewAdmin(n, n, f, fakeHealth{}, nil)
	ctx := context.Background()
	for i := range 5 { // indexes 1-5 hold changes
		tb, _ := fsm.EncodeTxBatch(&pb.TxBatch{CommitLsn: uint64(i + 1), Final: true, Events: []*pb.Event{{Op: pb.Op_OP_INSERT}}})
		n.Propose(ctx, tb)
	}
	b, _ := fsm.EncodeSinkOffset("ch", 3)
	n.Propose(ctx, b) // index 6
	a.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v")})
	st, err := a.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.SourceState != "retrying" || st.SourceError != "pg: connect: refused" || len(st.Sinks) != 2 {
		t.Fatalf("status %v", st)
	}
	if s := st.Sinks[0]; s.Offset != 3 || s.Lag != 2 || s.Type != "clickhouse" {
		t.Fatalf("ch %v", s)
	}
	if s := st.Sinks[1]; s.Offset != 0 || s.Lag != 5 {
		t.Fatalf("new %v", s)
	}
}

func TestErrorCodes(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		err  error
		code codes.Code
		msg  string
	}{
		{"follower", &raft.NotLeaderError{Leader: 2}, codes.FailedPrecondition, "node 2 at b:2"},
		{"no leader", &raft.NotLeaderError{}, codes.FailedPrecondition, "unknown"},
		{"lost", raft.ErrLeadershipLost, codes.Unavailable, "may or may not"},
		{"stopped", raft.ErrStopped, codes.Unavailable, ""},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded, ""},
		{"other", errors.New("disk on fire"), codes.Internal, "disk on fire"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, n := newAdmin()
			n.err, n.readErr = tc.err, tc.err
			_, perr := a.Put(ctx, &pb.PutRequest{Key: "k"})
			_, gerr := a.Get(ctx, &pb.GetRequest{Key: "k"})
			for _, err := range []error{perr, gerr} {
				s, _ := status.FromError(err)
				if s.Code() != tc.code || !strings.Contains(s.Message(), tc.msg) {
					t.Fatalf("got %v %q, want %v containing %q", s.Code(), s.Message(), tc.code, tc.msg)
				}
			}
		})
	}
}

func TestInvalidArguments(t *testing.T) {
	a, _ := newAdmin()
	ctx := context.Background()
	_, err1 := a.Put(ctx, &pb.PutRequest{Key: ""})
	_, err2 := a.Get(ctx, &pb.GetRequest{Key: ""})
	_, err3 := a.Delete(ctx, &pb.DeleteRequest{Key: ""})
	_, err4 := a.Put(ctx, &pb.PutRequest{Key: strings.Repeat("x", fsm.MaxKeySize+1)})
	for i, err := range []error{err1, err2, err3, err4} {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

func TestReadLog(t *testing.T) {
	a, n := newAdmin()
	ctx := context.Background()
	a.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v")})
	sc := &pb.Schema{RelationOid: 1, Namespace: "public", Name: "t", Columns: []*pb.Column{{Name: "id", TypeOid: 23}}}
	sc.Id = fsm.SchemaID(sc)
	tb := &pb.TxBatch{CommitLsn: 10, EndLsn: 20, Final: true, Schemas: []*pb.Schema{sc},
		Events: []*pb.Event{{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")}}}}}}
	data, _ := fsm.EncodeTxBatch(tb)
	n.Propose(ctx, data)
	n.log = append(n.log, raft.Entry{Index: 3, Term: 2, Type: raft.EntryNoop})
	n.index = 3

	resp, err := a.ReadLog(ctx, &pb.ReadLogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	kinds := []pb.EntryKind{pb.EntryKind_ENTRY_KIND_META, pb.EntryKind_ENTRY_KIND_TX_BATCH, pb.EntryKind_ENTRY_KIND_NOOP}
	if len(resp.GetEntries()) != 3 || resp.GetCommitIndex() != 3 {
		t.Fatalf("%v", resp)
	}
	for i, e := range resp.GetEntries() {
		if e.GetKind() != kinds[i] || e.GetIndex() != uint64(i+1) {
			t.Fatalf("entry %d: %v", i, e)
		}
	}
	if !proto.Equal(resp.GetEntries()[1].GetTxBatch(), tb) || !proto.Equal(resp.GetSchemas()[sc.Id], sc) {
		t.Fatalf("batch or schema mismatch: %v", resp)
	}

	page, _ := a.ReadLog(ctx, &pb.ReadLogRequest{FromIndex: 2, Limit: 1})
	if len(page.GetEntries()) != 1 || page.GetEntries()[0].GetIndex() != 2 {
		t.Fatalf("page: %v", page)
	}
	n.first = 2
	if _, err := a.ReadLog(ctx, &pb.ReadLogRequest{FromIndex: 1}); status.Code(err) != codes.OutOfRange {
		t.Fatalf("compacted: %v", err)
	}
}
