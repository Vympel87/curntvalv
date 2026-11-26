package sink

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// jsonSink records each change's op, table and columns or row.
type jsonSink struct {
	mu  sync.Mutex
	got []string
}

func (s *jsonSink) Deliver(_ context.Context, changes []event.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range changes {
		j := c.JSON()
		var cols []string
		for _, col := range j.Columns {
			cols = append(cols, col.Name)
		}
		s.got = append(s.got, fmt.Sprintf("%s %s %v %v /%d", j.Op, j.Table, cols, j.After, j.TxChanges))
	}
	return nil
}

func (s *jsonSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

// ingestSchema appends a one-row transaction of a table with columns id
// (key) and the given others; the batch carries the schema when it is new.
func (n *fakeNode) ingestSchema(t *testing.T, lsn uint64, table string, cols ...string) {
	t.Helper()
	sc := &pb.Schema{Namespace: "public", Name: table, Columns: []*pb.Column{{Name: "id", TypeOid: 23, Key: true}}}
	vals := []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")}}
	for _, c := range cols {
		sc.Columns = append(sc.Columns, &pb.Column{Name: c, TypeOid: 25})
		vals = append(vals, &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(c + "-value")})
	}
	sc.Id = fsm.SchemaID(sc)
	tb := &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 1, Final: true,
		Events: []*pb.Event{{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: vals}}}}
	if _, ok := n.f.Schema(sc.Id); !ok {
		tb.Schemas = []*pb.Schema{sc}
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

func TestRunnerSchemaChanges(t *testing.T) {
	h := newHarness(t)
	h.node.ingestSchema(t, 100, "orders", "note")
	h.node.ingestSchema(t, 200, "orders", "note")          // same definition
	h.node.ingestSchema(t, 300, "orders", "note", "email") // a column added
	with, without := &jsonSink{}, &jsonSink{}
	runSink(t, h, with, Config{SchemaChanges: true, Transactions: true, Shape: Shape{Drop: []string{"email"}, Rename: map[string]string{"note": "comment"}}})
	h2 := &harness{t: t, node: h.node}
	runSinkNamed(t, h2, "plain", without, Config{})
	want := []string{
		"schema public.orders [id comment] map[] /2",
		"insert public.orders [] map[comment:note-value id:1] /2",
		"insert public.orders [] map[comment:note-value id:1] /1",
		// The new definition is announced as the sink sees it.
		"schema public.orders [id comment] map[] /2",
		"insert public.orders [] map[comment:note-value id:1] /2",
	}
	waitFor(t, "schema changes", func() bool { return len(with.all()) == len(want) })
	if got := with.all(); !slices.Equal(got, want) {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	waitFor(t, "rows without schema changes", func() bool { return len(without.all()) == 3 })
	for _, g := range without.all() {
		if strings.HasPrefix(g, "schema") {
			t.Fatalf("schema change delivered without the option: %s", g)
		}
	}
}
