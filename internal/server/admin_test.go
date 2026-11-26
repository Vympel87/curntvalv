package server

import (
	"context"
	"errors"
	"fmt"
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
	conf    raft.Configuration
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
	return raft.Status{ID: 1, Term: 4, State: raft.StateLeader, Leader: 1, LastIndex: n.index, CommitIndex: n.index, LastApplied: n.index, FirstIndex: max1(n.first),
		Config: n.conf, ConfigIndex: 3, Match: map[uint64]uint64{2: 1}}, nil
}

func newAdmin() (*Admin, *fakeNode) {
	f := fsm.New()
	n := &fakeNode{f: f}
	return NewAdmin(n, n, f, nil, nil, nil, map[uint64]string{1: "a:1", 2: "b:2"}), n
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
func (fakeHealth) Retention() *pb.RetentionStatus {
	return &pb.RetentionStatus{MaxBytes: 100, CompactLimit: 3, BlockedBy: "ch"}
}

func TestStatusReportsHealth(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f}
	a := NewAdmin(n, n, f, fakeHealth{}, nil, nil, nil)
	ctx := context.Background()
	for i := range 5 { // indexes 1-5 hold changes
		tb, _ := fsm.EncodeTxBatch(&pb.TxBatch{CommitLsn: uint64(i + 1), Final: true, Events: []*pb.Event{{Op: pb.Op_OP_INSERT}}})
		n.Propose(ctx, tb)
	}
	b, _ := fsm.EncodeSinkOffset("ch", 0, 3)
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
	if st.Retention.GetBlockedBy() != "ch" {
		t.Fatalf("retention %v", st.Retention)
	}

	// Resets: the offset can move backwards and the generation grows.
	for _, tc := range []struct {
		req    *pb.ResetSinkRequest
		code   codes.Code
		offset uint64
		gen    uint64
	}{
		{&pb.ResetSinkRequest{Name: "ch", Position: pb.ResetPosition_RESET_POSITION_EARLIEST}, codes.OK, 0, 1},
		{&pb.ResetSinkRequest{Name: "ch", Position: pb.ResetPosition_RESET_POSITION_INDEX, Index: 4}, codes.OK, 4, 2},
		{&pb.ResetSinkRequest{Name: "new", Position: pb.ResetPosition_RESET_POSITION_LATEST}, codes.OK, 9, 1},
		{&pb.ResetSinkRequest{Name: "ch", Position: pb.ResetPosition_RESET_POSITION_INDEX, Index: 99}, codes.InvalidArgument, 0, 0},
		{&pb.ResetSinkRequest{Name: "ch"}, codes.InvalidArgument, 0, 0},
		{&pb.ResetSinkRequest{Name: "typo", Position: pb.ResetPosition_RESET_POSITION_EARLIEST}, codes.NotFound, 0, 0},
	} {
		resp, err := a.ResetSink(ctx, tc.req)
		if status.Code(err) != tc.code {
			t.Fatalf("%v: %v", tc.req, err)
		}
		if err == nil && (resp.Offset != tc.offset || resp.Generation != tc.gen) {
			t.Fatalf("%v: %v", tc.req, resp)
		}
	}
	n.first = 6 // entries before 6 compacted
	_, err = a.ResetSink(ctx, &pb.ResetSinkRequest{Name: "ch", Position: pb.ResetPosition_RESET_POSITION_INDEX, Index: 3})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("reset into compacted region: %v", err)
	}
	st, _ = a.Status(ctx, &pb.StatusRequest{})
	if s := st.Sinks[0]; s.Offset != 4 || s.Generation != 2 {
		t.Fatalf("ch after resets %v", s)
	}
	n.readErr = &raft.NotLeaderError{Leader: 2}
	_, err = a.ResetSink(ctx, &pb.ResetSinkRequest{Name: "ch", Position: pb.ResetPosition_RESET_POSITION_LATEST})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower: %v", err)
	}
	if _, err := newAdminNoHealth().ResetSink(ctx, &pb.ResetSinkRequest{Name: "ch", Position: 1}); status.Code(err) != codes.NotFound {
		t.Fatalf("without health: %v", err)
	}
}

func newAdminNoHealth() *Admin { a, _ := newAdmin(); return a }

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

type fakeResetter struct {
	idx    uint64
	r      *pb.SourceReset
	err    error
	tables []string
}

func (f fakeResetter) Reset(context.Context) (uint64, *pb.SourceReset, error) {
	return f.idx, f.r, f.err
}

func (f fakeResetter) StartBackfill(_ context.Context, tables []string) (uint64, []string, error) {
	return f.idx, append([]string{"start"}, tables...), f.err
}

func (f fakeResetter) CancelBackfill(_ context.Context, tables []string) (uint64, []string, error) {
	return f.idx, append([]string{"cancel"}, tables...), f.err
}

func TestResetSource(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f}
	ctx := context.Background()
	if _, err := NewAdmin(n, n, f, nil, nil, nil, nil).ResetSource(ctx, &pb.ResetSourceRequest{}); status.Code(err) != codes.Aborted {
		t.Fatalf("without a source: %v", err)
	}
	r := &pb.SourceReset{Lsn: 100}
	resp, err := NewAdmin(n, n, f, nil, fakeResetter{idx: 7, r: r}, nil, nil).ResetSource(ctx, &pb.ResetSourceRequest{})
	if err != nil || resp.GetIndex() != 7 || resp.GetSourceReset().GetLsn() != 100 {
		t.Fatalf("%v %v", resp, err)
	}
	peers := map[uint64]string{2: "b:2"}
	for _, tc := range []struct {
		err  error
		code codes.Code
		msg  string
	}{
		{&raft.NotLeaderError{Leader: 2}, codes.FailedPrecondition, "b:2"},
		{raft.ErrLeadershipLost, codes.Unavailable, ""},
		{errors.New("pg: ingest has not stopped on this node"), codes.Aborted, "has not stopped"},
	} {
		_, err := NewAdmin(n, n, f, nil, fakeResetter{err: tc.err}, nil, peers).ResetSource(ctx, &pb.ResetSourceRequest{})
		if status.Code(err) != tc.code || !strings.Contains(status.Convert(err).Message(), tc.msg) {
			t.Errorf("%v: got %v", tc.err, err)
		}
	}
}

func TestBackfillRPCs(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f}
	ctx := context.Background()
	if _, err := NewAdmin(n, n, f, nil, nil, nil, nil).StartBackfill(ctx, &pb.StartBackfillRequest{}); status.Code(err) != codes.Aborted {
		t.Fatalf("without a source: %v", err)
	}
	a := NewAdmin(n, n, f, nil, fakeResetter{idx: 9}, nil, nil)
	if resp, err := a.StartBackfill(ctx, &pb.StartBackfillRequest{Tables: []string{"orders"}}); err != nil || resp.GetIndex() != 9 ||
		strings.Join(resp.GetTables(), ",") != "start,orders" {
		t.Fatalf("%v %v", resp, err)
	}
	if resp, err := a.CancelBackfill(ctx, &pb.CancelBackfillRequest{Tables: []string{"orders"}}); err != nil ||
		strings.Join(resp.GetTables(), ",") != "cancel,orders" {
		t.Fatalf("%v %v", resp, err)
	}
	a = NewAdmin(n, n, f, nil, fakeResetter{err: errors.New("pg: cannot backfill x: no primary key")}, nil, nil)
	if _, err := a.StartBackfill(ctx, &pb.StartBackfillRequest{Tables: []string{"x"}}); status.Code(err) != codes.Aborted ||
		!strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("%v", err)
	}
	a = NewAdmin(n, n, f, nil, fakeResetter{err: &raft.NotLeaderError{}}, nil, nil)
	if _, err := a.CancelBackfill(ctx, &pb.CancelBackfillRequest{Tables: []string{"x"}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
}

type fakeMembers struct {
	err   error
	calls []string
}

func (m *fakeMembers) AddMember(_ context.Context, id uint64, addr string) (uint64, error) {
	m.calls = append(m.calls, fmt.Sprintf("add %d %s", id, addr))
	return 11, m.err
}

func (m *fakeMembers) RemoveMember(_ context.Context, id uint64) (uint64, error) {
	m.calls = append(m.calls, fmt.Sprintf("remove %d", id))
	return 12, m.err
}

func TestMembers(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f, conf: raft.Configuration{Members: []raft.Member{{ID: 1, Addr: "a:1", Voter: true}, {ID: 2, Addr: "b:2"}}}}
	ctx := context.Background()
	if _, err := NewAdmin(n, n, f, nil, nil, nil, nil).AddMember(ctx, &pb.AddMemberRequest{Id: 2, Addr: "b"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("without membership: %v", err)
	}
	m := &fakeMembers{}
	a := NewAdmin(n, n, f, nil, nil, m, nil)
	if _, err := a.AddMember(ctx, &pb.AddMemberRequest{Id: 3}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no address: %v", err)
	}
	if r, err := a.AddMember(ctx, &pb.AddMemberRequest{Id: 3, Addr: "c:3"}); err != nil || r.GetIndex() != 11 {
		t.Fatalf("%v %v", r, err)
	}
	if r, err := a.RemoveMember(ctx, &pb.RemoveMemberRequest{Id: 2}); err != nil || r.GetIndex() != 12 {
		t.Fatalf("%v %v", r, err)
	}
	if fmt.Sprint(m.calls) != "[add 3 c:3 remove 2]" {
		t.Fatalf("%v", m.calls)
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("%w: server 2 is the last voter", raft.ErrInvalidConfChange), codes.InvalidArgument},
		{raft.ErrConfChangeInProgress, codes.Unavailable},
		{&raft.NotLeaderError{Leader: 2}, codes.FailedPrecondition},
	} {
		m.err = tc.err
		if _, err := a.RemoveMember(ctx, &pb.RemoveMemberRequest{Id: 2}); status.Code(err) != tc.code {
			t.Errorf("%v: %v", tc.err, err)
		}
	}
	// The leader's address comes from the configuration.
	m.err = &raft.NotLeaderError{Leader: 2}
	if _, err := a.RemoveMember(ctx, &pb.RemoveMemberRequest{Id: 2}); !strings.Contains(err.Error(), "b:2") {
		t.Fatalf("%v", err)
	}
	st, err := a.Status(ctx, &pb.StatusRequest{})
	if err != nil || len(st.GetMembers()) != 2 || st.GetConfigIndex() != 3 || st.GetLeaderAddr() != "a:1" {
		t.Fatalf("%v %v", st, err)
	}
	if mb := st.GetMembers()[1]; mb.GetVoter() || mb.GetMatchIndex() != 1 || mb.GetAddr() != "b:2" {
		t.Fatalf("%v", mb)
	}
}

func TestNamedSubscriptions(t *testing.T) {
	f := fsm.New()
	n := &fakeNode{f: f}
	ctx := context.Background()
	s := NewSubscribe(nil, n, f)
	if _, err := NewSubscribe(nil, nil, nil).JoinSubscription(ctx, &pb.JoinSubscriptionRequest{Name: "a"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("without node: %v", err)
	}
	if _, err := s.JoinSubscription(ctx, &pb.JoinSubscriptionRequest{Name: "Bad"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := s.JoinSubscription(ctx, &pb.JoinSubscriptionRequest{Name: "billing", MustExist: true}); status.Code(err) != codes.NotFound {
		t.Fatalf("must exist: %v", err)
	}
	// Some entries, then a subscription from the latest one.
	for range 3 {
		b, _ := fsm.EncodePut("k", []byte("v"))
		n.Propose(ctx, b)
	}
	j, err := s.JoinSubscription(ctx, &pb.JoinSubscriptionRequest{Name: "billing", Start: pb.SubscriptionStart_SUBSCRIPTION_START_LATEST})
	if err != nil || !j.GetCreated() || j.GetSubscription().GetOffset() != 3 {
		t.Fatalf("%v %v", j, err)
	}
	// Joining again changes nothing, whatever it asks for.
	j, err = s.JoinSubscription(ctx, &pb.JoinSubscriptionRequest{Name: "billing", Drop: true})
	if err != nil || j.GetCreated() || j.GetSubscription().GetOffset() != 3 || j.GetSubscription().GetDrop() {
		t.Fatalf("again: %v %v", j, err)
	}
	if _, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: "billing", Offset: 9}); err != nil {
		t.Fatal(err)
	}
	before := n.index
	if _, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: "billing", Offset: 5}); err != nil || n.index != before {
		t.Fatalf("a commit backwards must not write: %v", err)
	}
	if _, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: "nope", Offset: 5}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown: %v", err)
	}

	a := NewAdmin(n, n, f, fakeHealth{}, nil, nil, nil)
	r, err := a.ResetSubscription(ctx, &pb.ResetSubscriptionRequest{Name: "billing", Position: pb.ResetPosition_RESET_POSITION_EARLIEST})
	if err != nil || r.GetSubscription().GetOffset() != 0 || r.GetSubscription().GetGeneration() != 1 {
		t.Fatalf("reset %v %v", r, err)
	}
	if _, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: "billing", Generation: 0, Offset: 12}); status.Code(err) != codes.Aborted {
		t.Fatalf("commit after a reset: %v", err)
	}
	if _, err := a.ResetSubscription(ctx, &pb.ResetSubscriptionRequest{Name: "billing", Position: pb.ResetPosition_RESET_POSITION_INDEX, Index: 2}); err != nil {
		t.Fatal(err)
	}
	st, err := a.Status(ctx, &pb.StatusRequest{})
	if err != nil || len(st.GetSubscriptions()) != 1 {
		t.Fatalf("%v %v", st, err)
	}
	if ss := st.GetSubscriptions()[0]; ss.GetName() != "billing" || ss.GetOffset() != 2 || ss.GetGeneration() != 2 || ss.GetPolicy() != "block" {
		t.Fatalf("%v", ss)
	}
	if _, err := a.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Name: "billing"}); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		func() error {
			_, err := a.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Name: "billing"})
			return err
		}(),
		func() error {
			_, err := a.ResetSubscription(ctx, &pb.ResetSubscriptionRequest{Name: "billing", Position: pb.ResetPosition_RESET_POSITION_LATEST})
			return err
		}(),
		func() error {
			_, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: "billing", Generation: 2, Offset: 20})
			return err
		}(),
	} {
		if status.Code(err) != codes.NotFound {
			t.Errorf("after delete: %v", err)
		}
	}
}
