package event

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func text(s string) *pb.Value { return &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(s)} }

func TestChangeJSON(t *testing.T) {
	sc := &pb.Schema{Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}, {Name: "note"}, {Name: "blob"}, {Name: "bin"}}}
	c := Change{
		Index: 42, CommitLSN: 0x1_0000_00A0, Seq: 4, Xid: 7, CommitTimeUs: 1_700_000_000_000_000, Schema: sc,
		Event: &pb.Event{Op: pb.Op_OP_UPDATE,
			Before: &pb.Row{Values: []*pb.Value{text("1"), nil, {Kind: pb.ValueKind_VALUE_KIND_UNCHANGED}}},
			After: &pb.Row{Values: []*pb.Value{
				text("1"),
				{Kind: pb.ValueKind_VALUE_KIND_NULL},
				{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED},
				{Kind: pb.ValueKind_VALUE_KIND_BINARY, Data: []byte{0xff}},
			}}},
	}
	b, err := json.Marshal(c.JSON())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"1/A0:4","index":42,"lsn":"1/A0","seq":4,"xid":7,"commit_time":"2023-11-14T22:13:20Z","table":"public.orders","op":"update",` +
		`"before":{"id":"1"},"after":{"bin":"/w==","id":"1","note":null},"unchanged":["blob"]}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	// Unknown schema: still printable.
	c.Schema = nil
	if j := c.JSON(); j.Table != "schema#0" || j.After["col0"] != "1" {
		t.Fatalf("without schema: %+v", j)
	}
}

func TestFilter(t *testing.T) {
	orders := &pb.Schema{Namespace: "public", Name: "orders"}
	shopOrders := &pb.Schema{Namespace: "shop", Name: "orders"}
	items := &pb.Schema{Namespace: "public", Name: "items"}
	for _, tc := range []struct {
		f    Filter
		sc   *pb.Schema
		want bool
	}{
		{nil, orders, true},
		{nil, nil, true},
		{Filter{"orders"}, orders, true},
		{Filter{"orders"}, shopOrders, true},
		{Filter{"public.orders"}, shopOrders, false},
		{Filter{"public.orders"}, orders, true},
		{Filter{"orders", "items"}, items, true},
		{Filter{"orders"}, items, false},
		{Filter{"orders"}, nil, false},
	} {
		if got := tc.f.Match(tc.sc); got != tc.want {
			t.Errorf("%v.Match(%v) = %v", tc.f, tc.sc, got)
		}
	}
}

// batch returns a final transaction batch with one insert per schema id.
func batch(lsn uint64, firstSeq uint32, schemas ...uint64) *pb.TxBatch {
	tb := &pb.TxBatch{CommitLsn: lsn, EndLsn: lsn + 1, FirstSeq: firstSeq, Final: true}
	for _, id := range schemas {
		tb.Events = append(tb.Events, &pb.Event{SchemaId: id, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{text("x")}}})
	}
	return tb
}

func TestChangesUsesBatchSchemas(t *testing.T) {
	orders := &pb.Schema{Id: 1, Namespace: "public", Name: "orders"}
	items := &pb.Schema{Id: 2, Namespace: "public", Name: "items"}
	tb := batch(100, 5, 1, 2, 1)
	tb.Schemas = []*pb.Schema{items}
	lookup := func(id uint64) (*pb.Schema, bool) { return orders, id == 1 }
	got := Changes(9, tb, lookup, Filter{"orders"})
	if len(got) != 2 || got[0].Seq != 5 || got[1].Seq != 7 || got[1].Index != 9 || got[0].Table() != "public.orders" {
		t.Fatalf("changes %+v", got)
	}
	// items is new to the log: its definition is announced before its
	// first change, with an id of its own.
	all := Changes(9, tb, lookup, nil)
	var got2 []string
	for _, c := range all {
		got2 = append(got2, c.ID()+" "+c.Op()+" "+c.Table())
	}
	want := []string{"0/64:5 insert public.orders", "0/64:6:schema schema public.items", "0/64:6 insert public.items", "0/64:7 insert public.orders"}
	if !slices.Equal(got2, want) {
		t.Fatalf("all changes %q, want %q", got2, want)
	}
	if rest := WithoutSchemaChanges(all); len(rest) != 3 || rest[1].ID() != "0/64:6" {
		t.Fatalf("without schema changes %+v", rest)
	}
}

func TestSchemaChangeJSON(t *testing.T) {
	sc := &pb.Schema{Id: 3, Namespace: "public", Name: "orders", Columns: []*pb.Column{
		{Name: "id", TypeOid: 23, TypeModifier: -1, Key: true}, {Name: "code", TypeOid: 1043, TypeModifier: 24}, {Name: "geo", TypeOid: 99999, TypeModifier: -1}}}
	tb := batch(100, 0, 3)
	tb.Schemas = []*pb.Schema{sc}
	b, err := json.Marshal(Changes(4, tb, nil, nil)[0].JSON())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"op":"schema"`, `"table":"public.orders"`, `"id":"0/64:0:schema"`,
		`"columns":[{"name":"id","type":"integer","type_oid":23,"type_modifier":-1,"key":true},` +
			`{"name":"code","type":"character varying","type_oid":1043,"type_modifier":24},{"name":"geo","type_oid":99999,"type_modifier":-1}]`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("%s lacks %s", b, want)
		}
	}
	if strings.Contains(string(b), `"after"`) {
		t.Fatalf("schema change with a row: %s", b)
	}
}

// memLog is a committed log over a real state machine: entries are applied
// to the fsm when appended.
type memLog struct {
	first   uint64
	entries []raft.Entry
	f       *fsm.FSM
}

func (m *memLog) appendData(t *testing.T, data []byte, typ raft.EntryType) {
	t.Helper()
	e := raft.Entry{Index: m.first + uint64(len(m.entries)), Term: 1, Type: typ, Data: data}
	m.entries = append(m.entries, e)
	if err := m.f.Apply(e); err != nil {
		t.Fatal(err)
	}
}

func (m *memLog) appendBatch(t *testing.T, tb *pb.TxBatch) {
	t.Helper()
	b, err := fsm.EncodeTxBatch(tb)
	if err != nil {
		t.Fatal(err)
	}
	m.appendData(t, b, raft.EntryNormal)
}

func (m *memLog) ReadCommitted(_ context.Context, from uint64, max int) ([]raft.Entry, error) {
	if from == 0 {
		from = m.first
	}
	if from < m.first {
		return nil, raft.ErrCompacted
	}
	i := int(from - m.first)
	if i >= len(m.entries) {
		return nil, nil
	}
	return m.entries[i:min(len(m.entries), i+max)], nil
}

func newLog(t *testing.T) (*memLog, *Reader) {
	m := &memLog{first: 1, f: fsm.New()}
	return m, NewReader(m, m.f)
}

func TestReaderReadsAndSkips(t *testing.T) {
	m, r := newLog(t)
	m.appendData(t, nil, raft.EntryNoop) // 1
	tb := batch(100, 0, 1, 1)
	tb.Schemas = []*pb.Schema{{Id: 1, Namespace: "public", Name: "orders"}}
	m.appendBatch(t, tb) // 2
	put, _ := fsm.EncodePut("k", []byte("v"))
	m.appendData(t, put, raft.EntryNormal) // 3
	m.appendBatch(t, batch(200, 0, 1))     // 4

	ctx := context.Background()
	res, err := r.Read(ctx, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Entry 2 brings the orders schema: one schema change, then the rows.
	if len(res.Changes) != 4 || !res.Changes[0].IsSchemaChange() || res.Next != 5 || res.TxEntries != 2 {
		t.Fatalf("read all: %d changes, next %d, tx %d", len(res.Changes), res.Next, res.TxEntries)
	}
	// The schema stored by entry 2 is used for entry 4, not announced again.
	if res.Changes[3].Table() != "public.orders" || res.Changes[3].Index != 4 || res.Changes[3].IsSchemaChange() {
		t.Fatalf("last change %+v", res.Changes[3])
	}
	// max limits entries, not changes.
	res, err = r.Read(ctx, 2, 2, nil)
	if err != nil || len(res.Changes) != 3 || res.Next != 4 {
		t.Fatalf("max 2 from 2: %+v %v", res, err)
	}
	// A filter that matches nothing still advances.
	res, err = r.Read(ctx, 1, 100, Filter{"items"})
	if err != nil || len(res.Changes) != 0 || res.Next != 5 || res.TxEntries != 2 {
		t.Fatalf("filtered: %+v %v", res, err)
	}
}

func TestReaderReportsTransactions(t *testing.T) {
	m, r := newLog(t)
	orders := []*pb.Schema{{Id: 1, Namespace: "public", Name: "orders"}}
	open := func(tb *pb.TxBatch) *pb.TxBatch { tb.Final = false; return tb }
	first := open(batch(100, 0, 1, 1))
	first.Schemas = orders
	m.appendBatch(t, first) // 1: begins 100
	put, _ := fsm.EncodePut("k", []byte("v"))
	m.appendData(t, put, raft.EntryNormal)   // 2
	m.appendBatch(t, batch(100, 2, 1))       // 3: ends 100
	m.appendBatch(t, batch(200, 0))          // 4: no changes (a position)
	m.appendBatch(t, open(batch(300, 4, 1))) // 5: resumes 300, open
	ctx := context.Background()
	res, err := r.Read(ctx, 0, 100, Filter{"items"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Tx{{CommitLSN: 100, First: 1, Begin: true, End: true}, {CommitLSN: 200, First: 4, End: true}, {CommitLSN: 300, First: 5}}
	if !slices.Equal(res.Txs, want) {
		t.Fatalf("txs %+v, want %+v", res.Txs, want)
	}
	// A read starting inside a transaction does not see its beginning.
	res, err = r.Read(ctx, 3, 1, nil)
	if err != nil || !slices.Equal(res.Txs, []Tx{{CommitLSN: 100, First: 3, End: true}}) {
		t.Fatalf("from 3: %+v %v", res.Txs, err)
	}
}

func TestReaderWaitsForNewEntries(t *testing.T) {
	m, r := newLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := make(chan Result, 1)
	go func() {
		res, err := r.Read(ctx, 0, 10, nil)
		if err != nil {
			t.Error(err)
		}
		got <- res
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case res := <-got:
		t.Fatalf("returned before any entry: %+v", res)
	default:
	}
	m.appendBatch(t, batch(100, 0, 1))
	if res := <-got; res.Next != 2 || len(res.Changes) != 1 {
		t.Fatalf("after append: %+v", res)
	}

	// Waiting past the end, then cancelled.
	short, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel2()
	if _, err := r.Read(short, 2, 10, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read past end: %v", err)
	}
}

func TestReaderCompacted(t *testing.T) {
	m, r := newLog(t)
	m.appendBatch(t, batch(100, 0, 1))
	m.appendBatch(t, batch(200, 0, 1))
	m.first, m.entries = 2, m.entries[1:]
	if _, err := r.Read(context.Background(), 1, 10, nil); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted read: %v", err)
	}
	res, err := r.Read(context.Background(), 0, 10, nil)
	if err != nil || res.Next != 3 || len(res.Changes) != 1 {
		t.Fatalf("read from oldest: %+v %v", res, err)
	}
}

func TestSourceResetChange(t *testing.T) {
	r := &pb.SourceReset{PreviousLsn: 0x1500000, PreviousSystemId: 7394150873, PreviousTimeline: 1,
		Lsn: 0x3000000, SystemId: 7394150873000000001, Timeline: 2, Reason: "diverged"}
	tb := &pb.TxBatch{CommitLsn: 0x3000000 - 1, EndLsn: 0x3000000, Final: true,
		Events: []*pb.Event{{Op: pb.Op_OP_SOURCE_RESET, SourceReset: r}}}
	// It passes a table filter: every consumer must see the gap.
	cs := Changes(9, tb, nil, Filter{"orders"})
	if len(cs) != 1 || !cs[0].IsSourceReset() || cs[0].Table() != "" || cs[0].Op() != "source_reset" {
		t.Fatalf("%+v", cs)
	}
	b, err := json.Marshal(cs[0].JSON())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"op":"source_reset"`, `"table":""`, `"previous_lsn":"0/1500000"`, `"lsn":"0/3000000"`,
		`"system_id":"7394150873000000001"`, `"timeline":2`, `"reason":"diverged"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
	// Row changes do not carry it.
	if b, _ := json.Marshal(Change{Event: &pb.Event{Op: pb.Op_OP_INSERT}}.JSON()); strings.Contains(string(b), "reset") {
		t.Errorf("%s", b)
	}
}
