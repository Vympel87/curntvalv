package transport

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/wal"
)

var (
	_ raft.Transport = (*Transport)(nil)
	_ Stepper        = (*raft.Node)(nil)
)

func TestConversionRoundTrip(t *testing.T) {
	msgs := []raft.Message{
		{Type: raft.MsgVote, From: 1, To: 2, Term: 3, LastLogIndex: 4, LastLogTerm: 5},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 3, VoteGranted: true},
		{Type: raft.MsgApp, From: 1, To: 2, Term: 3, PrevLogIndex: 7, PrevLogTerm: 2, LeaderCommit: 6, ReadID: 9,
			Entries: []raft.Entry{{Index: 8, Term: 3, Type: raft.EntryNormal, Data: []byte("x")}, {Index: 9, Term: 3, Type: raft.EntryNoop}}},
		{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, Success: true, MatchIndex: 9, PrevLogIndex: 7, LastLogIndex: 9, ReadID: 9},
		{Type: raft.MsgSnap, From: 1, To: 2, Term: 3, ReadID: 1,
			Snapshot: &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 100, Term: 2}, Data: []byte("state")}},
	}
	for _, m := range msgs {
		got, err := FromPB(ToPB(m))
		if err != nil {
			t.Fatal(err)
		}
		// Empty and nil Data are the same on the wire.
		for i := range got.Entries {
			if len(got.Entries[i].Data) == 0 {
				got.Entries[i].Data = nil
			}
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip:\n got %+v\nwant %+v", got, m)
		}
	}
}

func TestFromPBRejectsUnknownTypes(t *testing.T) {
	bad := []*pb.RaftMessage{
		nil,
		{Type: 0},
		{Type: 99},
		{Type: uint32(raft.MsgApp), Entries: []*pb.Entry{{Type: 7}}},
	}
	for i, p := range bad {
		if _, err := FromPB(p); err == nil {
			t.Errorf("case %d: accepted", i)
		}
	}
}

// recorder is a Stepper that records what it receives.
type recorder struct {
	mu   sync.Mutex
	msgs []raft.Message
}

func (r *recorder) Step(_ context.Context, m raft.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *recorder) terms() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uint64
	for _, m := range r.msgs {
		out = append(out, m.Term)
	}
	return out
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	var lis net.Listener
	var err error
	for range 50 { // the port may take a moment to free up after a restart
		if lis, err = net.Listen("tcp", addr); err == nil {
			return lis
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

func serve(t *testing.T, lis net.Listener, srv pb.RaftServiceServer) *grpc.Server {
	t.Helper()
	gs := grpc.NewServer()
	pb.RegisterRaftServiceServer(gs, srv)
	go gs.Serve(lis)
	return gs
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDeliveryAndReconnect(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	addr := lis.Addr().String()
	peers := map[uint64]string{1: "unused", 2: addr}
	rec := &recorder{}
	gs := serve(t, lis, NewServer(2, peers, rec))

	tr := New(1, peers, Options{MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- tr.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	for i := uint64(1); i <= 10; i++ {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: i})
	}
	waitFor(t, "10 messages", func() bool { return len(rec.terms()) == 10 })
	if fmt.Sprint(rec.terms()) != "[1 2 3 4 5 6 7 8 9 10]" {
		t.Fatalf("order: %v", rec.terms())
	}

	// The peer restarts on the same address; sending resumes.
	gs.Stop()
	rec2 := &recorder{}
	gs2 := serve(t, listen(t, addr), NewServer(2, peers, rec2))
	defer gs2.Stop()
	waitFor(t, "delivery after restart", func() bool {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 99})
		time.Sleep(10 * time.Millisecond)
		return len(rec2.terms()) > 0
	})
}

// After a long outage the peer must be reachable again quickly, not after
// gRPC's default reconnect backoff (up to two minutes).
func TestReconnectAfterLongOutage(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	addr := lis.Addr().String()
	peers := map[uint64]string{1: "unused", 2: addr}
	gs := serve(t, lis, NewServer(2, peers, &recorder{}))

	tr := New(1, peers, Options{MinBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2})

	gs.Stop()
	// Keep sending during a 3s outage, so both our and gRPC's backoff grow.
	outage := time.Now().Add(3 * time.Second)
	for time.Now().Before(outage) {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2})
		time.Sleep(20 * time.Millisecond)
	}
	rec := &recorder{}
	gs2 := serve(t, listen(t, addr), NewServer(2, peers, rec))
	defer gs2.Stop()
	back := time.Now()
	waitFor(t, "delivery after outage", func() bool {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 7})
		time.Sleep(10 * time.Millisecond)
		return len(rec.terms()) > 0
	})
	if d := time.Since(back); d > time.Second {
		t.Fatalf("took %v to reach the peer again", d)
	}
}

// A message type from a newer version is dropped; the stream stays up and
// the messages after it arrive.
func TestUnknownMessageTypeIsDropped(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	addr := lis.Addr().String()
	peers := map[uint64]string{1: "unused", 2: addr}
	rec := &recorder{}
	gs := serve(t, lis, NewServer(2, peers, rec))
	defer gs.Stop()
	tr := New(1, peers, Options{MinBackoff: time.Second, MaxBackoff: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1})
	waitFor(t, "first message", func() bool { return len(rec.terms()) == 1 })
	tr.Send(raft.Message{Type: raft.MessageType(200), From: 1, To: 2, Term: 2})
	for i := uint64(3); i <= 6; i++ {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: i})
	}
	// A closed stream would lose these, and reconnecting takes a second.
	waitFor(t, "messages after the unknown one", func() bool { return len(rec.terms()) == 5 })
	if fmt.Sprint(rec.terms()) != "[1 3 4 5 6]" {
		t.Fatalf("got %v", rec.terms())
	}
}

func TestSendNeverBlocks(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	addr := lis.Addr().String()
	lis.Close() // nobody listens here
	tr := New(1, map[uint64]string{2: addr}, Options{QueueSize: 4})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- tr.Run(ctx) }()

	start := time.Now()
	for range 1000 {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2})
	}
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 7}) // unknown peer
	if time.Since(start) > time.Second {
		t.Fatalf("Send blocked for %v", time.Since(start))
	}
	if tr.Dropped() < 1000-4 {
		t.Fatalf("dropped %d, want at least %d", tr.Dropped(), 1000-4)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestServerDropsForeignMessages(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	peers := map[uint64]string{1: "x", 2: lis.Addr().String()}
	rec := &recorder{}
	gs := serve(t, lis, NewServer(2, peers, rec))
	defer gs.Stop()

	all := map[uint64]string{1: "x", 2: lis.Addr().String(), 3: lis.Addr().String()}
	tr := New(1, all, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 3, Term: 1}) // wrong recipient
	tr.Send(raft.Message{Type: raft.MsgApp, From: 9, To: 2, Term: 2}) // unknown sender
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 3})
	waitFor(t, "valid message", func() bool { return len(rec.terms()) > 0 })
	time.Sleep(50 * time.Millisecond)
	if fmt.Sprint(rec.terms()) != "[3]" {
		t.Fatalf("got %v", rec.terms())
	}
}

// Peers can be added, moved and removed while the transport runs; the server
// accepts only the current ones.
func TestSetPeers(t *testing.T) {
	lisA, lisB := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
	recA, recB := &recorder{}, &recorder{}
	srvB := NewServer(3, map[uint64]string{}, recB)
	defer serve(t, lisA, NewServer(2, map[uint64]string{1: "x"}, recA)).Stop()
	defer serve(t, lisB, srvB).Stop()

	tr := New(1, map[uint64]string{1: "self"}, Options{MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- tr.Run(ctx) }()
	defer func() { cancel(); <-done }()

	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1}) // unknown yet: dropped
	tr.SetPeers(map[uint64]string{1: "self", 2: lisA.Addr().String()})
	waitFor(t, "added peer", func() bool {
		tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 2})
		time.Sleep(5 * time.Millisecond)
		return len(recA.terms()) > 0
	})
	if fmt.Sprint(recA.terms()[0]) != "2" || tr.Dropped() == 0 {
		t.Fatalf("got %v, dropped %d", recA.terms(), tr.Dropped())
	}
	// Node 2 moves to B's address; B does not know node 1 yet.
	tr.SetPeers(map[uint64]string{2: lisB.Addr().String()})
	if fmt.Sprint(tr.Peers()) != fmt.Sprintf("map[2:%s]", lisB.Addr()) {
		t.Fatalf("peers %v", tr.Peers())
	}
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 3})
	time.Sleep(100 * time.Millisecond)
	if len(recB.terms()) != 0 {
		t.Fatalf("server accepted a message for another node: %v", recB.terms())
	}
	// Removed: messages are dropped.
	tr.SetPeers(nil)
	before := tr.Dropped()
	tr.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 4})
	if tr.Dropped() != before+1 {
		t.Fatal("message to a removed peer not dropped")
	}
}

// A server accepts messages from a peer added later.
func TestServerSetPeers(t *testing.T) {
	lis := listen(t, "127.0.0.1:0")
	rec := &recorder{}
	srv := NewServer(2, map[uint64]string{1: "x"}, rec)
	defer serve(t, lis, srv).Stop()
	tr := New(4, map[uint64]string{2: lis.Addr().String()}, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)
	tr.Send(raft.Message{Type: raft.MsgApp, From: 4, To: 2, Term: 1})
	time.Sleep(100 * time.Millisecond)
	srv.SetPeers(map[uint64]string{1: "x", 4: "y"})
	tr.Send(raft.Message{Type: raft.MsgApp, From: 4, To: 2, Term: 2})
	waitFor(t, "message from the added peer", func() bool { return len(rec.terms()) > 0 })
	if fmt.Sprint(rec.terms()) != "[2]" {
		t.Fatalf("got %v", rec.terms())
	}
}

// --- a real Raft cluster over gRPC --------------------------------------------

type grpcNode struct {
	node   *raft.Node
	fsm    *fsm.FSM
	log    *wal.Log
	gs     *grpc.Server
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type grpcCluster struct {
	t     *testing.T
	dir   string
	peers map[uint64]string
	nodes map[uint64]*grpcNode
}

func newGRPCCluster(t *testing.T, size int) *grpcCluster {
	c := &grpcCluster{t: t, dir: t.TempDir(), peers: map[uint64]string{}, nodes: map[uint64]*grpcNode{}}
	lis := map[uint64]net.Listener{}
	for id := uint64(1); id <= uint64(size); id++ {
		lis[id] = listen(t, "127.0.0.1:0")
		c.peers[id] = lis[id].Addr().String()
	}
	for id := range c.peers {
		c.start(id, lis[id])
	}
	t.Cleanup(func() {
		for id := range c.peers {
			c.stop(id)
		}
	})
	return c
}

func (c *grpcCluster) start(id uint64, lis net.Listener) {
	t := c.t
	t.Helper()
	if lis == nil {
		lis = listen(t, c.peers[id])
	}
	dir := filepath.Join(c.dir, fmt.Sprint(id))
	log, err := wal.Open(filepath.Join(dir, "wal"), wal.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dir, "snapshots"), true)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uint64, 0, len(c.peers))
	for p := range c.peers {
		ids = append(ids, p)
	}
	f := fsm.New()
	tr := New(id, c.peers, Options{MinBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond})
	node, err := raft.NewNode(raft.Config{ID: id, Peers: ids, TickInterval: 10 * time.Millisecond, SnapshotEntries: 20},
		raft.Storage{Log: log, Stable: raft.NewFileStableStore(filepath.Join(dir, "state"), true), Snapshots: snaps}, tr, f)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessageSize))
	pb.RegisterRaftServiceServer(gs, NewServer(id, c.peers, node))
	ctx, cancel := context.WithCancel(context.Background())
	n := &grpcNode{node: node, fsm: f, log: log, gs: gs, cancel: cancel}
	n.wg.Add(3)
	go func() { defer n.wg.Done(); gs.Serve(lis) }()
	go func() { defer n.wg.Done(); tr.Run(ctx) }()
	go func() {
		defer n.wg.Done()
		if err := node.Run(ctx); err != nil {
			t.Errorf("node %d: %v", id, err)
		}
	}()
	c.nodes[id] = n
}

func (c *grpcCluster) stop(id uint64) {
	n := c.nodes[id]
	if n == nil {
		return
	}
	n.cancel()
	n.gs.Stop()
	n.wg.Wait()
	n.log.Close()
	c.nodes[id] = nil
}

func (c *grpcCluster) put(key, value string) {
	c.t.Helper()
	data, err := fsm.EncodePut(key, []byte(value))
	if err != nil {
		c.t.Fatal(err)
	}
	waitFor(c.t, "put "+key, func() bool {
		for _, n := range c.nodes {
			if n == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := n.node.Propose(ctx, data)
			cancel()
			if err == nil {
				return true
			}
		}
		return false
	})
}

func TestRaftClusterOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	for i := range 30 {
		c.put(fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	for id, n := range c.nodes {
		waitFor(t, fmt.Sprintf("node %d to apply", id), func() bool { return len(n.fsm.Keys()) == 30 })
	}

	// Kill a node, keep writing, bring it back: it catches up from disk + peers.
	c.stop(3)
	for i := 30; i < 60; i++ {
		c.put(fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	c.start(3, nil)
	waitFor(t, "node 3 to catch up", func() bool { return len(c.nodes[3].fsm.Keys()) == 60 })
	if v, ok := c.nodes[3].fsm.Get("k59"); !ok || string(v) != "59" {
		t.Fatalf("k59 = %q %v", v, ok)
	}
}
