package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/server"
)

// metaNode is a one-node "cluster" that applies proposals to an FSM at
// once; leader says whether it answers as leader.
type metaNode struct {
	mu     sync.Mutex
	f      *fsm.FSM
	index  uint64
	leader bool
}

func (n *metaNode) Propose(_ context.Context, data []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return 0, &raft.NotLeaderError{}
	}
	n.index++
	return n.index, n.f.Apply(raft.Entry{Index: n.index, Term: 1, Type: raft.EntryNormal, Data: data})
}

func (n *metaNode) ReadIndex(context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return 0, &raft.NotLeaderError{}
	}
	return max(n.index, 10), nil
}

func (n *metaNode) Status(context.Context) (raft.Status, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := raft.Status{State: raft.StateFollower}
	if n.leader {
		st.State = raft.StateLeader
	}
	return st, nil
}

// serveNamed starts a subscribe server with named subscriptions.
func serveNamed(t *testing.T, l *log, n *metaNode) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterSubscribeServiceServer(gs, server.NewSubscribe(l, n, n.f))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestNamedSubscription(t *testing.T) {
	l := newLog()
	for i := uint64(1); i <= 5; i++ {
		l.add(change(i, orders, string(rune('a'+i-1))))
	}
	f := fsm.New()
	leader := &metaNode{f: f, leader: true}
	follower := &metaNode{f: f}
	// The first node is not the leader: joins and commits go to the second.
	c, _ := New([]string{serveNamed(t, l, follower), serveNamed(t, l, leader)})
	ctx := ctxT(t)

	var got []string
	for ch, err := range c.Subscribe(ctx, Options{Name: "billing"}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, *ch.After["id"])
		if len(got) == 3 {
			break // processed a, b; c is in progress
		}
	}
	if s, _ := f.Subscription("billing"); s.GetOffset() != 2 {
		t.Fatalf("stored %v after breaking at entry 3", s)
	}
	// Continues with the change in progress.
	got = nil
	for ch, err := range c.Subscribe(ctx, Options{Name: "billing"}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, *ch.After["id"])
		if len(got) == 3 {
			break
		}
	}
	if len(got) != 3 || got[0] != "c" || got[2] != "e" {
		t.Fatalf("resumed with %v", got)
	}

	// A reset while running: the subscriber continues from the new position.
	b, _ := fsm.EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_RESET, Name: "billing", Offset: 0})
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	got = nil
	for ch, err := range c.Subscribe(sctx, Options{Name: "billing"}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, *ch.After["id"])
		if len(got) == 1 {
			leader.Propose(ctx, b) // reset to the beginning
			time.Sleep(commitEvery + 100*time.Millisecond)
			l.add(change(6, orders, "f")) // makes the client commit, and notice
		}
		if len(got) > 1 && got[len(got)-1] == "a" {
			break
		}
	}
	if got[len(got)-1] != "a" {
		t.Fatalf("no restart after the reset: %v", got)
	}

	// Options that cannot work.
	for _, err := range c.Subscribe(ctx, Options{Name: "billing", From: 3}) {
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("name and from: %v", err)
		}
	}
	for _, err := range c.Subscribe(ctx, Options{Name: "Bad Name"}) {
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad name: %v", err)
		}
	}
}

func TestNamedSubscriptionLatestAndManual(t *testing.T) {
	l := newLog(change(1, orders, "a"), change(2, orders, "b"))
	f := fsm.New()
	n := &metaNode{f: f, leader: true}
	c, _ := New([]string{serveNamed(t, l, n)})
	ctx := ctxT(t)
	go func() {
		time.Sleep(300 * time.Millisecond)
		l.add(change(11, orders, "new"))
	}()
	// The test node's latest index is 10.
	for ch, err := range c.Subscribe(ctx, Options{Name: "tail", StartLatest: true, Drop: true, ManualCommit: true}) {
		if err != nil || *ch.After["id"] != "new" {
			t.Fatalf("%+v %v", ch, err)
		}
		break
	}
	if s, _ := f.Subscription("tail"); s.GetOffset() != 10 || !s.GetDrop() {
		t.Fatalf("manual: nothing may be stored, got %v", s)
	}
	if err := c.Commit(ctx, "tail", 11); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, "tail", 4); err != nil {
		t.Fatal(err)
	}
	if s, _ := f.Subscription("tail"); s.GetOffset() != 11 {
		t.Fatalf("after commit %v", s)
	}
	if err := c.Commit(ctx, "typo", 4); !errors.Is(err, ErrSubscriptionDeleted) {
		t.Fatalf("commit of an unknown name: %v", err)
	}
	if _, ok := f.Subscription("typo"); ok {
		t.Fatal("commit created a subscription")
	}
	// Deleted while running.
	b, _ := fsm.EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_DELETE, Name: "gone"})
	var last error
	for _, err := range c.Subscribe(ctx, Options{Name: "gone"}) {
		if err != nil {
			last = err
			break
		}
		n.Propose(ctx, b)
		time.Sleep(commitEvery + 100*time.Millisecond)
	}
	if !errors.Is(last, ErrSubscriptionDeleted) {
		t.Fatalf("deleted: %v", last)
	}
}
