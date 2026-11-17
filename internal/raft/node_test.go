package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/wal"
)

// chanNet connects Nodes in one process. Send never blocks: if the receiver's
// inbox is full or the receiver is down the message is dropped.
type chanNet struct {
	mu    sync.Mutex
	nodes map[uint64]*Node
}

func (n *chanNet) Send(m Message) {
	n.mu.Lock()
	dst := n.nodes[m.To]
	n.mu.Unlock()
	if dst == nil {
		return
	}
	select {
	case dst.recvc <- m:
	default:
	}
}

func (n *chanNet) set(id uint64, node *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

// syncApplier is an Applier that can be read from the test goroutine.
type syncApplier struct {
	mu      sync.Mutex
	entries []Entry
}

func (a *syncApplier) Apply(e Entry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if want := uint64(len(a.entries)) + 1; e.Index != want {
		return fmt.Errorf("applied index %d, want %d", e.Index, want)
	}
	a.entries = append(a.entries, Entry{Index: e.Index, Term: e.Term, Type: e.Type, Data: append([]byte(nil), e.Data...)})
	return nil
}

func (a *syncApplier) data() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, e := range a.entries {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

type liveNode struct {
	node   *Node
	app    *syncApplier
	cancel context.CancelFunc
	log    *wal.Log
}

type liveCluster struct {
	t     *testing.T
	ids   []uint64
	dir   string
	net   *chanNet
	nodes map[uint64]*liveNode
}

func newLiveCluster(t *testing.T, size int) *liveCluster {
	c := &liveCluster{t: t, dir: t.TempDir(), net: &chanNet{nodes: map[uint64]*Node{}}, nodes: map[uint64]*liveNode{}}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

// start runs node id on a wal.Log and FileStableStore in its own directory.
func (c *liveCluster) start(id uint64) {
	c.t.Helper()
	dir := filepath.Join(c.dir, fmt.Sprint(id))
	log, err := wal.Open(dir, wal.Options{NoSync: true})
	if err != nil {
		c.t.Fatal(err)
	}
	app := &syncApplier{}
	cfg := Config{ID: id, Peers: c.ids, ElectionTicks: 10, HeartbeatTicks: 1, TickInterval: 2 * time.Millisecond}
	node, err := NewNode(cfg, log, NewFileStableStore(filepath.Join(dir, "state"), true), c.net, app)
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.nodes[id] = &liveNode{node: node, app: app, cancel: cancel, log: log}
	c.net.set(id, node)
	go func() {
		if err := node.Run(ctx); err != nil {
			c.t.Errorf("node %d: %v", id, err)
		}
	}()
}

func (c *liveCluster) stop(id uint64) {
	ln := c.nodes[id]
	if ln == nil {
		return
	}
	c.net.set(id, nil)
	ln.cancel()
	<-ln.node.Done()
	ln.log.Close()
	c.nodes[id] = nil
}

// propose retries on every live node until one accepts and commits data.
func (c *liveCluster) propose(data string) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			ln := c.nodes[id]
			if ln == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			idx, err := ln.node.Propose(ctx, []byte(data))
			cancel()
			if err == nil {
				return idx
			}
			if errors.Is(err, ErrLeadershipLost) {
				// Outcome unknown; this test proposes unique values and
				// accepts at most one copy, so it is safe to retry.
				continue
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("could not commit %q", data)
	return 0
}

func (c *liveCluster) waitApplied(id uint64, n int) []string {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		d := c.nodes[id].app.data()
		if len(d) >= n || time.Now().After(deadline) {
			return d
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNodeClusterWithWALSurvivesRestarts(t *testing.T) {
	c := newLiveCluster(t, 3)
	var want []string
	for i := range 20 {
		d := fmt.Sprintf("a%d", i)
		c.propose(d)
		want = append(want, d)
	}

	// Restart one node, then the whole cluster, from what is on disk.
	c.stop(2)
	for i := range 10 {
		d := fmt.Sprintf("b%d", i)
		c.propose(d)
		want = append(want, d)
	}
	c.start(2)
	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.propose("c")
	want = append(want, "c")

	for _, id := range c.ids {
		got := c.waitApplied(id, len(want))
		if fmt.Sprint(dedupe(got)) != fmt.Sprint(want) {
			t.Fatalf("node %d applied %v, want %v", id, got, want)
		}
	}
}

// dedupe drops repeated values (from retries after ErrLeadershipLost) while
// keeping order; every value must still appear.
func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func TestNodeProposeOnFollower(t *testing.T) {
	c := newLiveCluster(t, 3)
	c.propose("x") // waits for a leader
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	followers := 0
	for _, id := range c.ids {
		st, err := c.nodes[id].node.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.State == StateLeader {
			continue
		}
		_, err = c.nodes[id].node.Propose(ctx, []byte("y"))
		var nl *NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("follower %d: got %v", id, err)
		}
		followers++
	}
	if followers < 2 {
		t.Fatalf("only %d followers", followers)
	}
}

func TestNodeStopFailsWaiters(t *testing.T) {
	// A lone node in a 3-node cluster can never commit.
	net := &chanNet{nodes: map[uint64]*Node{}}
	cfg := Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTicks: 2, HeartbeatTicks: 1, TickInterval: time.Millisecond}
	n, err := NewNode(cfg, &syncLog{}, &syncStable{}, net, &syncApplier{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	if _, err := n.Status(ctx); err != nil { // wait until Run owns the node
		t.Fatal(err)
	}
	if err := n.Run(ctx); err == nil {
		t.Fatal("second Run succeeded")
	}
	cancel()
	<-n.Done()
	if _, err := n.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrStopped) {
		t.Fatalf("propose after stop: %v", err)
	}
	if _, err := n.Status(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("status after stop: %v", err)
	}
	if n.Err() != nil {
		t.Fatalf("Err() = %v after clean stop", n.Err())
	}
}

// A leader that lost its followers cannot commit; stopping it must release
// the blocked Propose with ErrStopped.
func TestNodePendingProposeFailsOnStop(t *testing.T) {
	c := newLiveCluster(t, 3)
	c.propose("x")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var leader uint64
	for _, id := range c.ids {
		st, err := c.nodes[id].node.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.State == StateLeader {
			leader = id
		}
	}
	if leader == 0 {
		t.Skip("leadership moved during the test")
	}
	for _, id := range c.ids {
		if id != leader {
			c.stop(id)
		}
	}
	res := make(chan error, 1)
	go func() {
		_, err := c.nodes[leader].node.Propose(ctx, []byte("never"))
		res <- err
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-res:
		t.Fatalf("propose returned early: %v", err)
	default:
	}
	c.stop(leader)
	if err := <-res; !errors.Is(err, ErrStopped) {
		t.Fatalf("pending propose: got %v, want ErrStopped", err)
	}
}

func TestNodeFatalStorageError(t *testing.T) {
	net := &chanNet{nodes: map[uint64]*Node{}}
	cfg := Config{ID: 1, Peers: []uint64{1}, TickInterval: time.Millisecond}
	boom := errors.New("disk on fire")
	n, err := NewNode(cfg, &syncLog{}, &syncStable{err: boom}, net, &syncApplier{})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run returned %v", err)
	}
	if !errors.Is(n.Err(), boom) {
		t.Fatalf("Err() = %v", n.Err())
	}
}

// syncLog and syncStable are memLog/memStable for use with a Node (whose
// stores are touched only by the Run goroutine, but the race detector wants
// no shared state with the test either; these are never read by the test).
type syncLog struct{ memLog }
type syncStable struct {
	memStable
	err error
}

func (s *syncStable) Save(hs HardState) error {
	if s.err != nil {
		return s.err
	}
	return s.memStable.Save(hs)
}
