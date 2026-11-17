package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

var (
	_ RaftNode = (*raft.Node)(nil)
	_ KV       = (*fsm.FSM)(nil)
)

// fakeNode applies proposals straight to an FSM, or fails with err.
type fakeNode struct {
	f       *fsm.FSM
	err     error
	readErr error
	index   uint64
}

func (n *fakeNode) Propose(_ context.Context, data []byte) (uint64, error) {
	if n.err != nil {
		return 0, n.err
	}
	n.index++
	return n.index, n.f.Apply(raft.Entry{Index: n.index, Term: 1, Type: raft.EntryNormal, Data: data})
}

func (n *fakeNode) ReadIndex(context.Context) (uint64, error) {
	if n.readErr != nil {
		return 0, n.readErr
	}
	return n.index, nil
}

func (n *fakeNode) Status(context.Context) (raft.Status, error) {
	return raft.Status{ID: 1, Term: 4, State: raft.StateLeader, Leader: 1, LastIndex: n.index}, nil
}

func newAdmin() (*Admin, *fakeNode) {
	f := fsm.New()
	n := &fakeNode{f: f}
	return NewAdmin(n, f, map[uint64]string{1: "a:1", 2: "b:2"}), n
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
