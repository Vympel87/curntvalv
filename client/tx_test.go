package client

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func txChange(lsn string, seq, n uint32) Change {
	return Change{ID: fmt.Sprintf("%s:%d", lsn, seq), LSN: lsn, Seq: seq, TxChanges: n}
}

func seqOf(items ...any) iter.Seq2[Change, error] {
	return func(yield func(Change, error) bool) {
		for _, it := range items {
			var ok bool
			switch v := it.(type) {
			case Change:
				ok = yield(v, nil)
			case error:
				ok = yield(Change{}, v)
			}
			if !ok {
				return
			}
		}
	}
}

func ids(tx []Change) string {
	var out []string
	for _, c := range tx {
		out = append(out, c.ID)
	}
	return strings.Join(out, " ")
}

func TestGroupTransactions(t *testing.T) {
	boom := errors.New("boom")
	in := seqOf(
		txChange("A", 0, 2), txChange("A", 1, 2), // whole
		txChange("B", 0, 3), txChange("B", 1, 3), // the stream broke here
		txChange("B", 0, 3), txChange("B", 1, 3), txChange("B", 2, 3), // resent from its start
		txChange("C", 0, 0), // alone
		txChange("D", 4, 2), // part of D, then the stream resumed at E
		txChange("E", 0, 1),
		boom)
	var got []string
	var gotErr error
	for tx, err := range groupTransactions(in) {
		if err != nil {
			gotErr = err
			continue
		}
		got = append(got, ids(tx))
	}
	want := []string{"A:0 A:1", "B:0 B:1 B:2", "C:0", "E:0"}
	if !slices.Equal(got, want) || !errors.Is(gotErr, boom) {
		t.Fatalf("got %q %v, want %q", got, gotErr, want)
	}
	// Stopping early stops the stream.
	n := 0
	for range groupTransactions(in) {
		n++
		break
	}
	if n != 1 {
		t.Fatal("break ignored")
	}
}

// raftLog is a committed log over a real state machine, read by a real
// event.Reader.
type raftLog struct {
	mu      sync.Mutex
	entries []raft.Entry
	f       *fsm.FSM
}

func (l *raftLog) ReadCommitted(_ context.Context, from uint64, max int) ([]raft.Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from == 0 {
		from = 1
	}
	i := int(from - 1)
	if i >= len(l.entries) {
		return nil, nil
	}
	return slices.Clone(l.entries[i:min(len(l.entries), i+max)]), nil
}

// batch appends one batch of a transaction with an insert per id.
func (l *raftLog) batch(t *testing.T, lsn uint64, firstSeq uint32, final bool, ids ...string) {
	t.Helper()
	l.batchOf(t, orders, lsn, firstSeq, final, ids...)
}

func (l *raftLog) batchOf(t *testing.T, sc *pb.Schema, lsn uint64, firstSeq uint32, final bool, ids ...string) {
	t.Helper()
	tb := &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 1, FirstSeq: firstSeq, Final: final}
	if _, ok := l.f.Schema(sc.GetId()); !ok { // as the source does: only new ones
		tb.Schemas = []*pb.Schema{sc}
	}
	for _, id := range ids {
		tb.Events = append(tb.Events, &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT,
			After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(id)}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}})
	}
	data, err := fsm.EncodeTxBatch(tb)
	if err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := raft.Entry{Index: uint64(len(l.entries)) + 1, Term: 1, Type: raft.EntryNormal, Data: data}
	l.entries = append(l.entries, e)
	if err := l.f.Apply(e); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionsThroughServer(t *testing.T) {
	l := &raftLog{f: fsm.New()}
	l.batch(t, 0x100, 0, false, "1", "2")
	l.batch(t, 0x100, 2, true, "3")
	l.batch(t, 0x200, 0, true, "4")
	l.batch(t, 0x300, 0, false, "5") // still open
	addr, _ := serve(t, event.NewReader(l, l.f))
	c, _ := New([]string{addr})
	ctx := ctxT(t)
	var got []string
	for tx, err := range c.Transactions(ctx, Options{}) {
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for _, ch := range tx {
			rows = append(rows, fmt.Sprintf("%s/%d", *ch.After["id"], ch.TxChanges))
		}
		got = append(got, strings.Join(rows, " "))
		if len(got) == 2 {
			// The open transaction is not sent until it ends.
			l.batch(t, 0x300, 1, true, "6")
		}
		if len(got) == 3 {
			break
		}
	}
	want := []string{"1/3 2/3 3/3", "4/1", "5/2 6/2"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Without Transactions nothing changes.
	for ch, err := range c.Subscribe(ctx, Options{}) {
		if err != nil || ch.TxChanges != 0 {
			t.Fatalf("%+v %v", ch, err)
		}
		break
	}
}

// TestNamedTransactionsCommitBetweenTransactions: a named subscription
// never stores a position inside a transaction, so one left in the middle
// is received whole again, even when its first batch had nothing for the
// subscriber.
func TestNamedTransactionsCommitBetweenTransactions(t *testing.T) {
	l := &raftLog{f: fsm.New()}
	l.batch(t, 0x100, 0, true, "1", "2")      // 1: A
	l.batchOf(t, items, 0x200, 0, false, "x") // 2: B, nothing for orders
	l.batch(t, 0x200, 1, true, "3", "4")      // 3: rest of B
	node := &metaNode{f: fsm.New(), leader: true, index: 100}
	c, _ := New([]string{serveNamed(t, event.NewReader(l, l.f), node)})
	ctx := ctxT(t)
	read := func() []string {
		var got []string
		for tx, err := range c.Transactions(ctx, Options{Name: "t", Tables: []string{"orders"}}) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s:%d", *tx[0].After["id"], len(tx)))
			if len(got) == 2 {
				break // stopped while processing B
			}
		}
		return got
	}
	if got := read(); !slices.Equal(got, []string{"1:2", "3:2"}) {
		t.Fatalf("first run %v", got)
	}
	// Never stored past B's first batch (entry 2): A may come again, as
	// delivery is at least once, but B comes whole.
	if s, _ := node.f.Subscription("t"); s.GetOffset() > 1 {
		t.Fatalf("stored offset %d, past the start of B", s.GetOffset())
	}
	var again []string
	for tx, err := range c.Transactions(ctx, Options{Name: "t", Tables: []string{"orders"}}) {
		if err != nil {
			t.Fatal(err)
		}
		again = append(again, fmt.Sprintf("%s:%d/%d", *tx[0].After["id"], len(tx), tx[0].TxChanges))
		if *tx[0].After["id"] == "3" {
			break
		}
	}
	if again[len(again)-1] != "3:2/2" {
		t.Fatalf("resumed with %v", again)
	}
}

func TestSchemaChanges(t *testing.T) {
	l := &raftLog{f: fsm.New()}
	l.batch(t, 0x100, 0, true, "1")
	l.batch(t, 0x200, 0, true, "2") // orders is known by now
	addr, _ := serve(t, event.NewReader(l, l.f))
	c, _ := New([]string{addr})
	ctx := ctxT(t)
	var got []string
	for ch, err := range c.Subscribe(ctx, Options{SchemaChanges: true}) {
		if err != nil {
			t.Fatal(err)
		}
		if ch.Op == OpSchema {
			var cols []string
			for _, col := range ch.Columns {
				cols = append(cols, col.Name)
			}
			got = append(got, fmt.Sprintf("schema %s %s %v %d", ch.ID, ch.Table, cols, len(ch.After)))
		} else {
			got = append(got, fmt.Sprintf("%s %s", ch.Op, *ch.After["id"]))
		}
		if len(got) == 3 {
			break
		}
	}
	want := []string{"schema 0/100:0:schema public.orders [id note] 0", "insert 1", "insert 2"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
