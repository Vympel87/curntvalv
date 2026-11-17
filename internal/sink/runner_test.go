package sink

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

var (
	_ Node    = (*raft.Node)(nil)
	_ Reader  = (*event.Reader)(nil)
	_ Offsets = (*fsm.FSM)(nil)
)

// fakeNode is a one-node "cluster": proposals are appended to a log and
// applied to a real state machine at once. Leadership can be switched.
type fakeNode struct {
	mu        sync.Mutex
	f         *fsm.FSM
	first     uint64
	log       []raft.Entry
	leader    bool
	term      uint64
	proposals int
}

func newFakeNode() *fakeNode {
	return &fakeNode{f: fsm.New(), first: 1, leader: true, term: 1}
}

func (n *fakeNode) appendLocked(data []byte) (uint64, error) {
	e := raft.Entry{Index: n.first + uint64(len(n.log)), Term: n.term, Type: raft.EntryNormal, Data: data}
	n.log = append(n.log, e)
	return e.Index, n.f.Apply(e)
}

func (n *fakeNode) Propose(_ context.Context, data []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return 0, &raft.NotLeaderError{}
	}
	n.proposals++
	return n.appendLocked(data)
}

func (n *fakeNode) ReadIndex(context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return 0, &raft.NotLeaderError{}
	}
	return n.first + uint64(len(n.log)) - 1, nil
}

func (n *fakeNode) Status(context.Context) (raft.Status, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := raft.Status{Term: n.term, State: raft.StateFollower}
	if n.leader {
		st.State = raft.StateLeader
	}
	return st, nil
}

func (n *fakeNode) ReadCommitted(_ context.Context, from uint64, max int) ([]raft.Entry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if from == 0 {
		from = n.first
	}
	if from < n.first {
		return nil, raft.ErrCompacted
	}
	i := int(from - n.first)
	if i >= len(n.log) {
		return nil, nil
	}
	return slices.Clone(n.log[i:min(len(n.log), i+max)]), nil
}

func (n *fakeNode) setLeader(leader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leader = leader
	n.term++
}

func (n *fakeNode) entries() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

func testSchema(table string) *pb.Schema {
	sc := &pb.Schema{Namespace: "public", Name: table}
	sc.Id = fsm.SchemaID(sc)
	return sc
}

// ingest appends a transaction with one insert per id into table.
func (n *fakeNode) ingest(t *testing.T, lsn uint64, table string, ids ...int) {
	t.Helper()
	sc := testSchema(table)
	tb := &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 1, Final: true, Schemas: []*pb.Schema{sc}}
	for _, id := range ids {
		tb.Events = append(tb.Events, &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT,
			After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(fmt.Sprint(id))}}}})
	}
	b, err := fsm.EncodeTxBatch(tb)
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, err := n.appendLocked(b); err != nil {
		t.Fatal(err)
	}
}

// memSink records delivered rows and can be made to fail.
type memSink struct {
	mu       sync.Mutex
	rows     []string
	calls    int
	failNext int
	sizes    []int
}

func (s *memSink) Deliver(_ context.Context, changes []event.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failNext > 0 {
		s.failNext--
		return errors.New("sink down")
	}
	s.sizes = append(s.sizes, len(changes))
	for _, c := range changes {
		s.rows = append(s.rows, c.Schema.GetName()+":"+string(c.Event.GetAfter().GetValues()[0].GetData()))
	}
	return nil
}

func (s *memSink) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.rows)
}

type harness struct {
	t    *testing.T
	node *fakeNode
	sink *memSink
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, node: newFakeNode(), sink: &memSink{}}
}

// run starts a runner and returns it with a function that stops it.
func (h *harness) run(cfg Config) (*Runner, func()) {
	h.t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test"
	}
	if cfg.RetryMin == 0 {
		cfg.RetryMin = time.Millisecond
	}
	if cfg.SkipCommitInterval == 0 {
		cfg.SkipCommitInterval = 50 * time.Millisecond
	}
	r, err := NewRunner(cfg, h.sink, h.node, event.NewReader(h.node, h.node.f), h.node.f)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.Run(ctx); err != nil {
			h.t.Error(err)
		}
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	h.t.Cleanup(stop)
	return r, stop
}

func (h *harness) waitRows(want ...string) {
	h.t.Helper()
	waitFor(h.t, fmt.Sprintf("rows %v", want), func() bool { return slices.Equal(h.sink.got(), want) })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *harness) offset(name string) uint64 {
	off, _ := h.node.f.SinkOffset(name)
	return off
}

func TestRunnerDeliversAndResumes(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1, 2)
	h.node.ingest(t, 200, "orders", 3)
	_, stop := h.run(Config{})
	h.waitRows("orders:1", "orders:2", "orders:3")
	waitFor(t, "offset", func() bool { return h.offset("test") == 2 })

	h.node.ingest(t, 300, "orders", 4)
	h.waitRows("orders:1", "orders:2", "orders:3", "orders:4")
	waitFor(t, "offset", func() bool { return h.offset("test") >= 4 })
	stop()

	// A new runner (another leader, a restart) continues after the offset.
	h.node.ingest(t, 400, "orders", 5)
	h.run(Config{})
	h.waitRows("orders:1", "orders:2", "orders:3", "orders:4", "orders:5")
}

func TestRunnerRetriesFailedDeliveries(t *testing.T) {
	h := newHarness(t)
	h.sink.failNext = 3
	h.node.ingest(t, 100, "orders", 1)
	r, _ := h.run(Config{})
	h.waitRows("orders:1")
	h.sink.mu.Lock()
	if h.sink.calls != 4 {
		t.Fatalf("%d calls, want 4", h.sink.calls)
	}
	h.sink.mu.Unlock()
	waitFor(t, "running", func() bool { return r.Status().State == StateRunning })
	if st := r.Status(); st.LastError != "" || st.Delivered != 1 {
		t.Fatalf("status %+v", st)
	}

	h.sink.mu.Lock()
	h.sink.failNext = 1 << 30
	h.sink.mu.Unlock()
	h.node.ingest(t, 200, "orders", 2)
	waitFor(t, "retrying", func() bool { return r.Status().State == StateRetrying })
	if st := r.Status(); st.LastError != "sink down" {
		t.Fatalf("status %+v", st)
	}
	if h.offset("test") >= 2 {
		t.Fatal("offset moved past an undelivered entry")
	}
}

func TestRunnerOnlyOnLeader(t *testing.T) {
	h := newHarness(t)
	h.node.setLeader(false)
	h.node.ingest(t, 100, "orders", 1)
	r, _ := h.run(Config{})
	time.Sleep(300 * time.Millisecond)
	if len(h.sink.got()) != 0 || r.Status().State != StateStandby {
		t.Fatalf("follower delivered %v (%s)", h.sink.got(), r.Status().State)
	}
	h.node.setLeader(true)
	h.waitRows("orders:1")

	// Losing leadership while a delivery keeps failing stops the session.
	h.sink.mu.Lock()
	h.sink.failNext = 1 << 30
	h.sink.mu.Unlock()
	h.node.ingest(t, 200, "orders", 2)
	waitFor(t, "retrying", func() bool { return r.Status().State == StateRetrying })
	h.node.setLeader(false)
	waitFor(t, "standby", func() bool { return r.Status().State == StateStandby })
	h.sink.mu.Lock()
	calls := h.sink.calls
	h.sink.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	h.sink.mu.Lock()
	defer h.sink.mu.Unlock()
	if h.sink.calls != calls {
		t.Fatal("still delivering after losing leadership")
	}
}

func TestRunnerStartLatest(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1)
	h.run(Config{StartLatest: true})
	waitFor(t, "offset", func() bool { return h.offset("test") > 0 })
	h.node.ingest(t, 200, "orders", 2)
	h.waitRows("orders:2")
}

func TestRunnerFilterAndSkippedOffsets(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1)
	h.node.ingest(t, 200, "items", 9)
	h.run(Config{Tables: event.Filter{"orders"}})
	h.waitRows("orders:1")
	// Entry 2 (items only) is skipped; its offset is stored after a while.
	waitFor(t, "skipped offset", func() bool { return h.offset("test") >= 2 })

	// Offset entries must not cause more offset entries.
	time.Sleep(200 * time.Millisecond)
	n := h.node.entries()
	time.Sleep(300 * time.Millisecond)
	if h.node.entries() != n {
		t.Fatalf("log grows while idle: %d -> %d entries", n, h.node.entries())
	}
	h.node.ingest(t, 300, "items", 10)
	h.node.ingest(t, 400, "orders", 2)
	h.waitRows("orders:1", "orders:2")
}

func TestRunnerSplitsBatches(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1, 2, 3, 4, 5)
	h.node.ingest(t, 200, "orders", 6, 7)
	h.run(Config{MaxBatch: 3})
	h.waitRows("orders:1", "orders:2", "orders:3", "orders:4", "orders:5", "orders:6", "orders:7")
	h.sink.mu.Lock()
	defer h.sink.mu.Unlock()
	if fmt.Sprint(h.sink.sizes) != "[3 3 1]" {
		t.Fatalf("batch sizes %v", h.sink.sizes)
	}
}

func TestRunnerFailsWhenBehindCompaction(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1)
	h.node.ingest(t, 200, "orders", 2)
	b, _ := fsm.EncodeSinkOffset("test", 1) // delivered entry 1 only
	h.node.Propose(context.Background(), b)
	h.node.mu.Lock()
	h.node.first, h.node.log = 3, h.node.log[2:] // entry 2 compacted away
	h.node.mu.Unlock()
	r, _ := h.run(Config{})
	waitFor(t, "failed", func() bool { return r.Status().State == StateFailed })
	if !strings.Contains(r.Status().LastError, "compaction") {
		t.Fatalf("status %+v", r.Status())
	}
	if len(h.sink.got()) != 0 {
		t.Fatalf("delivered %v", h.sink.got())
	}
}

func TestNewRunnerValidatesName(t *testing.T) {
	if _, err := NewRunner(Config{Name: "Bad Name"}, &memSink{}, newFakeNode(), nil, nil); err == nil {
		t.Fatal("bad name accepted")
	}
}
