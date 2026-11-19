package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/security"
	"github.com/Vympel87/curntvalv/internal/server"
)

// log is a fake log: one change per entry, shared by all test servers like
// the replicated log of a cluster.
type log struct {
	mu        sync.Mutex
	changes   []event.Change
	compacted uint64 // entries below are gone
	added     chan struct{}
}

func newLog(changes ...event.Change) *log {
	return &log{changes: changes, added: make(chan struct{})}
}

func (l *log) add(c event.Change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.changes = append(l.changes, c)
	close(l.added)
	l.added = make(chan struct{})
}

func (l *log) Read(ctx context.Context, from uint64, _ int, filter event.Filter) (event.Result, error) {
	for {
		l.mu.Lock()
		if from != 0 && from < l.compacted {
			l.mu.Unlock()
			return event.Result{}, raft.ErrCompacted
		}
		for _, c := range l.changes {
			if c.Index >= from {
				l.mu.Unlock()
				res := event.Result{Next: c.Index + 1, TxEntries: 1}
				if filter.Match(c.Schema) {
					res.Changes = []event.Change{c}
				}
				return res, nil
			}
		}
		added := l.added
		l.mu.Unlock()
		select {
		case <-added:
		case <-ctx.Done():
			return event.Result{}, ctx.Err()
		}
	}
}

var (
	orders = &pb.Schema{Id: 1, Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}, {Name: "note"}}}
	items  = &pb.Schema{Id: 2, Namespace: "public", Name: "items", Columns: []*pb.Column{{Name: "id"}, {Name: "note"}}}
)

func change(index uint64, sc *pb.Schema, id string) event.Change {
	return event.Change{Index: index, CommitLSN: index << 8, Xid: uint32(index), CommitTimeUs: 1_700_000_000_000_000 + int64(index),
		Schema: sc, Event: &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{
			{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(id)}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}}}
}

// serve starts a subscribe server on r and returns its address and a stop
// function.
func serve(t *testing.T, r server.ChangeReader) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterSubscribeServiceServer(gs, server.NewSubscribe(r))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String(), gs.Stop
}

func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSubscribeFiltersAndSkipsDeadNode(t *testing.T) {
	l := newLog(change(3, orders, "a"), change(4, items, "x"), change(6, orders, "b"), change(9, orders, "c"))
	addr, _ := serve(t, l)
	dead := deadAddr(t)
	c, err := New([]string{dead + ", " + addr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var retries []uint64
	var got []string
	opts := Options{From: 4, Tables: []string{"orders"}, OnRetry: func(a string, _ error, next uint64) {
		if a != dead {
			t.Errorf("retry on %s", a)
		}
		retries = append(retries, next)
	}}
	for ch, err := range c.Subscribe(ctxT(t), opts) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, *ch.After["id"])
		if ch.Table != "public.orders" || ch.Op != OpInsert || ch.After["note"] != nil {
			t.Fatalf("%+v", ch)
		}
		if len(got) == 2 {
			break
		}
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("got %v", got)
	}
	if len(retries) != 1 || retries[0] != 4 {
		t.Fatalf("retries %v", retries)
	}
}

// When the serving node goes away, the stream continues on another node
// right after the last entry it received.
func TestSubscribeContinuesOnAnotherNode(t *testing.T) {
	l := newLog(change(1, orders, "a"), change(2, orders, "b"))
	a, stopA := serve(t, l)
	b, _ := serve(t, l)
	c, _ := New([]string{a, b})
	var resumed []uint64
	opts := Options{OnRetry: func(_ string, _ error, next uint64) { resumed = append(resumed, next) }}
	var got []string
	for ch, err := range c.Subscribe(ctxT(t), opts) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, *ch.After["id"])
		if len(got) == 2 {
			stopA()
			l.add(change(3, orders, "c"))
		}
		if len(got) == 3 {
			break
		}
	}
	if len(got) != 3 || got[2] != "c" {
		t.Fatalf("got %v", got)
	}
	if len(resumed) == 0 || resumed[0] != 3 {
		t.Fatalf("resumed from %v, want 3", resumed)
	}
}

func TestSubscribeCompacted(t *testing.T) {
	l := newLog(change(10, orders, "a"))
	l.compacted = 10
	addr, _ := serve(t, l)
	c, _ := New([]string{addr})
	n := 0
	for _, err := range c.Subscribe(ctxT(t), Options{From: 5}) {
		n++
		if !errors.Is(err, ErrCompacted) {
			t.Fatalf("got %v, want ErrCompacted", err)
		}
	}
	if n != 1 {
		t.Fatalf("%d values", n)
	}
	for _, err := range c.Subscribe(ctxT(t), Options{Tables: []string{""}}) {
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("empty table: got %v", err)
		}
	}
}

func TestSubscribeEndsWithContext(t *testing.T) {
	addr, _ := serve(t, newLog())
	c, _ := New([]string{addr})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range c.Subscribe(ctx, Options{}) {
			t.Errorf("unexpected value, err %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not end with its context")
	}
}

// A Change marshals to the shared JSON form of a change.
func TestChangeJSONMatchesSharedForm(t *testing.T) {
	ec := change(7, orders, "a")
	ec.Event.Op = pb.Op_OP_UPDATE
	ec.Event.Before = &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("a")}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}
	ec.Event.After.Values[1] = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED}
	ce := &pb.ChangeEvent{Index: ec.Index, CommitLsn: ec.CommitLSN, Seq: ec.Seq, Xid: ec.Xid, CommitTimeUs: ec.CommitTimeUs, Event: ec.Event}
	got, err := json.Marshal(convert(ce, map[uint64]*pb.Schema{1: orders}))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(ec.JSON())
	if string(got) != string(want) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestNewNeedsAnAddress(t *testing.T) {
	if _, err := New([]string{" , "}); err == nil {
		t.Fatal("no error")
	}
}

func TestSubscribeWithToken(t *testing.T) {
	l := newLog(change(1, orders, "a"))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(security.Guard{Token: "s3cret"}.ServerOptions(nil)...)
	pb.RegisterSubscribeServiceServer(gs, server.NewSubscribe(l))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	addr := lis.Addr().String()

	c, _ := New([]string{addr})
	for _, err := range c.Subscribe(ctxT(t), Options{}) {
		if !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("without token: %v", err)
		}
	}
	c, _ = New([]string{addr}, WithToken("s3cret"))
	for ch, err := range c.Subscribe(ctxT(t), Options{}) {
		if err != nil || *ch.After["id"] != "a" {
			t.Fatalf("with token: %+v %v", ch, err)
		}
		break
	}
}
