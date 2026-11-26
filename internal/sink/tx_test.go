package sink

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// part is one stored batch of a transaction: rows of one table.
type part struct {
	table string
	ids   []int
}

// ingestParts appends a transaction stored in several batches; the last is
// final unless open is set. It returns the index of the first batch.
func (n *fakeNode) ingestParts(t *testing.T, lsn uint64, firstSeq uint32, open bool, parts ...part) uint64 {
	t.Helper()
	var first uint64
	seq := firstSeq
	for i, p := range parts {
		sc := testSchema(p.table)
		tb := &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 1, FirstSeq: seq, Schemas: []*pb.Schema{sc},
			Final: i == len(parts)-1 && !open}
		for _, id := range p.ids {
			tb.Events = append(tb.Events, &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT,
				After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(fmt.Sprint(id))}}}})
		}
		seq += uint32(len(p.ids))
		b, err := fsm.EncodeTxBatch(tb)
		if err != nil {
			t.Fatal(err)
		}
		n.mu.Lock()
		idx, err := n.appendLocked(b)
		n.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = idx
		}
	}
	return first
}

// noise appends an entry that is not a transaction batch.
func (n *fakeNode) noise(t *testing.T) {
	t.Helper()
	b, err := fsm.EncodeSinkOffset("other", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, err := n.appendLocked(b); err != nil {
		t.Fatal(err)
	}
}

// callSink records each Deliver call as "table:id/txchanges ...".
type callSink struct {
	mu     sync.Mutex
	calls  []string
	refuse string // a row id refused while in the call
}

func (s *callSink) Deliver(_ context.Context, changes []event.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var parts []string
	for _, c := range changes {
		id := string(c.Event.GetAfter().GetValues()[0].GetData())
		if s.refuse != "" && id == s.refuse {
			return fmt.Errorf("row %s refused", id)
		}
		parts = append(parts, fmt.Sprintf("%s:%s/%d", c.Schema.GetName(), id, c.TxChanges))
	}
	s.calls = append(s.calls, strings.Join(parts, " "))
	return nil
}

func (s *callSink) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

// rows returns every delivered row without repeats, in first order.
func (s *callSink) rows() []string {
	var out []string
	for _, c := range s.got() {
		for _, r := range strings.Fields(c) {
			if !slices.Contains(out, r) {
				out = append(out, r)
			}
		}
	}
	return out
}

func runSink(t *testing.T, h *harness, s Sink, cfg Config) func() {
	t.Helper()
	return runSinkNamed(t, h, "test", s, cfg)
}

func runSinkNamed(t *testing.T, h *harness, name string, s Sink, cfg Config) func() {
	t.Helper()
	cfg.Name, cfg.RetryMin, cfg.RetryMax = name, time.Millisecond, 20*time.Millisecond
	cfg.SkipCommitInterval = 20 * time.Millisecond
	r, err := NewRunner(cfg, s, h.node, event.NewReader(h.node, h.node.f), h.node.f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitCalls(t *testing.T, s *callSink, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Equal(s.got(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("calls %q, want %q", s.got(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunnerDeliversWholeTransactions(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1, 2}}, part{"orders", []int{3}}, part{"orders", []int{4, 5}})
	h.node.noise(t)
	h.node.ingestParts(t, 200, 0, false, part{"orders", []int{6}})
	// Changes of other tables are not counted.
	h.node.ingestParts(t, 300, 0, false, part{"orders", []int{7}}, part{"items", []int{70, 71}}, part{"orders", []int{8}})
	s := &callSink{}
	// One entry per read cuts through the transactions; MaxBatch 3 still
	// never splits one.
	runSink(t, h, s, Config{Transactions: true, MaxEntries: 1, MaxBatch: 3, Tables: event.Filter{"orders"}})
	waitCalls(t, s,
		"orders:1/5 orders:2/5 orders:3/5 orders:4/5 orders:5/5",
		"orders:6/1", "orders:7/2 orders:8/2")
	waitFor(t, "offset at the end", func() bool { return h.offset("test") >= 8 })
}

func TestRunnerPacksTransactionsIntoBatches(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1}})
	h.node.ingestParts(t, 200, 0, false, part{"orders", []int{2, 3}})
	h.node.ingestParts(t, 300, 0, false, part{"orders", []int{4, 5, 6, 7}})
	h.node.ingestParts(t, 400, 0, false, part{"orders", []int{8}})
	s := &callSink{}
	runSink(t, h, s, Config{Transactions: true, MaxBatch: 3})
	waitCalls(t, s, "orders:1/1 orders:2/2 orders:3/2", "orders:4/4 orders:5/4 orders:6/4 orders:7/4", "orders:8/1")
}

func TestRunnerWithoutTransactionsIsUnchanged(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1, 2}}, part{"orders", []int{3}})
	s := &callSink{}
	runSink(t, h, s, Config{MaxEntries: 1})
	waitCalls(t, s, "orders:1/0 orders:2/0", "orders:3/0")
}

func TestRunnerHoldsOpenTransaction(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1}})
	first := h.node.ingestParts(t, 200, 0, true, part{"orders", []int{2}}, part{"orders", []int{3}})
	h.node.noise(t)
	s := &callSink{}
	stop := runSink(t, h, s, Config{Transactions: true})
	waitCalls(t, s, "orders:1/1")
	// The offset stays before the open transaction however long it waits.
	waitFor(t, "offset", func() bool { return h.offset("test") == first-1 })
	time.Sleep(100 * time.Millisecond)
	if got := h.offset("test"); got != first-1 {
		t.Fatalf("offset %d moved past the open transaction at %d", got, first)
	}
	// Another leader reads it whole again.
	stop()
	h.node.ingestParts(t, 200, 2, false, part{"orders", []int{4}})
	runSink(t, h, s, Config{Transactions: true})
	waitCalls(t, s, "orders:1/1", "orders:2/3 orders:3/3 orders:4/3")
}

func TestRunnerSplitsHugeTransaction(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1, 2}}, part{"orders", []int{3, 4}}, part{"orders", []int{5}})
	h.node.ingestParts(t, 200, 0, false, part{"orders", []int{6}})
	s := &callSink{}
	runSink(t, h, s, Config{Transactions: true, MaxEntries: 1, MaxTxChanges: 3})
	// Past 3 held changes it goes out in parts, without TxChanges.
	waitFor(t, "all rows", func() bool { return len(s.rows()) == 6 })
	want := []string{"orders:1/0", "orders:2/0", "orders:3/0", "orders:4/0", "orders:5/0", "orders:6/1"}
	if got := s.rows(); !slices.Equal(got, want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
}

func TestRunnerTransactionWithoutEnd(t *testing.T) {
	h := newHarness(t)
	// Part of a transaction, then another one: the first never ends (a
	// Postgres failover, design D7). Its changes still go out, uncounted.
	h.node.ingestParts(t, 100, 0, true, part{"orders", []int{1, 2}})
	h.node.ingestParts(t, 200, 0, false, part{"orders", []int{3}})
	// A transaction resumed after a source restart: its first batch is
	// missing here, so it is not known to be whole.
	h.node.ingestParts(t, 300, 4, false, part{"orders", []int{4}})
	s := &callSink{}
	runSink(t, h, s, Config{Transactions: true, MaxEntries: 1})
	waitFor(t, "all rows", func() bool { return len(s.rows()) == 4 })
	want := []string{"orders:1/0", "orders:2/0", "orders:3/1", "orders:4/0"}
	if got := s.rows(); !slices.Equal(got, want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
}

func TestRunnerSkipsPoisonTransaction(t *testing.T) {
	h := newHarness(t)
	h.node.ingestParts(t, 100, 0, false, part{"orders", []int{1}})
	h.node.ingestParts(t, 200, 0, false, part{"orders", []int{2, 3}}, part{"orders", []int{4}})
	h.node.ingestParts(t, 300, 0, false, part{"orders", []int{5}})
	s := &callSink{refuse: "3"}
	runSink(t, h, s, Config{Transactions: true, SkipPoison: true, MaxAttempts: 2})
	// The whole transaction with the refused row is skipped, not just the
	// row: delivering 2 and 4 alone would split it.
	waitFor(t, "skip recorded", func() bool { return h.node.f.SinkSkips("test").Total == 1 })
	waitFor(t, "rows", func() bool { return len(s.rows()) == 2 })
	if got := s.rows(); !slices.Equal(got, []string{"orders:1/1", "orders:5/1"}) {
		t.Fatalf("rows %v", got)
	}
	sk := h.node.f.SinkSkips("test")
	if r := sk.Recent[0]; r.GetId() != event.ID(200, 0) || !strings.Contains(r.GetError(), "transaction of 3 changes") {
		t.Fatalf("skip %+v", r)
	}
}
