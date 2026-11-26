package sink

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func TestParseShape(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Condition
	}{
		{"status=paid", Condition{Column: "status", Value: "paid"}},
		{"region!=eu", Condition{Column: "region", Value: "eu", Not: true}},
		{"note=", Condition{Column: "note"}},
		{"a=b=c", Condition{Column: "a", Value: "b=c"}},
	} {
		got, err := ParseCondition(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("%q: %+v %v", tc.in, got, err)
		}
	}
	for _, bad := range []string{"status", "=x", "!=x"} {
		if _, err := ParseCondition(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if op, err := ParseOp("backfill"); err != nil || op != pb.Op_OP_BACKFILL {
		t.Fatalf("%v %v", op, err)
	}
	for _, bad := range []string{"upsert", "source_reset", "unspecified"} {
		if _, err := ParseOp(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// ids returns "op:id:note" of each change, "reset" for a source reset.
func shapeIDs(cs []event.Change) []string {
	var out []string
	for _, c := range cs {
		if c.IsSourceReset() {
			out = append(out, "reset")
			continue
		}
		j := c.JSON()
		r := j.After
		if r == nil {
			r = j.Before
		}
		out = append(out, fmt.Sprintf("%s:%v:%v", j.Op, r["id"], r["note"]))
	}
	return out
}

func TestShapeFilters(t *testing.T) {
	changes := changesOf(1,
		op{op: pb.Op_OP_INSERT, id: "1", note: "paid"}, op{op: pb.Op_OP_INSERT, id: "2", note: "open"},
		op{op: pb.Op_OP_UPDATE, id: "2", note: "paid"}, op{op: pb.Op_OP_DELETE, id: "1"},
		op{op: pb.Op_OP_TRUNCATE}, op{op: pb.Op_OP_BACKFILL, id: "3", note: "paid"})
	changes = append(changes, event.Change{Index: 9, Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET, SourceReset: &pb.SourceReset{}}})

	for _, tc := range []struct {
		shape Shape
		want  string
	}{
		{Shape{}, "[insert:1:paid insert:2:open update:2:paid delete:1:<nil> truncate backfill:3:paid reset]"},
		{Shape{Ops: []pb.Op{pb.Op_OP_INSERT, pb.Op_OP_BACKFILL}}, "[insert:1:paid insert:2:open backfill:3:paid reset]"},
		// The new row decides; a delete is judged by its old row (here
		// only the key is known, so note=paid fails), a truncate passes.
		{Shape{Where: []Condition{{Column: "note", Value: "paid"}}}, "[insert:1:paid update:2:paid truncate backfill:3:paid reset]"},
		{Shape{Where: []Condition{{Column: "id", Value: "1"}}}, "[insert:1:paid delete:1:<nil> truncate reset]"},
		{Shape{Where: []Condition{{Column: "note", Value: "paid", Not: true}}}, "[insert:2:open delete:1:<nil> truncate reset]"},
		{Shape{Where: []Condition{{Column: "missing", Value: "x", Not: true}}}, "[truncate reset]"},
		{Shape{Ops: []pb.Op{pb.Op_OP_UPDATE}, Where: []Condition{{Column: "id", Value: "2"}}}, "[update:2:paid reset]"},
	} {
		got := normalize(shapeIDs(newShaper(tc.shape).Apply(changes)))
		if fmt.Sprint(got) != tc.want {
			t.Errorf("%+v\n got %v\nwant %s", tc.shape, got, tc.want)
		}
	}
}

func normalize(ids []string) []string {
	out := make([]string, len(ids))
	for i, s := range ids {
		if s == "truncate:<nil>:<nil>" {
			s = "truncate"
		}
		out[i] = s
	}
	return out
}

func TestShapeColumns(t *testing.T) {
	in := changesOf(1, op{op: pb.Op_OP_UPDATE, id: "7", note: "x", oldID: "6"})
	orig := in[0].Event
	for _, tc := range []struct {
		shape Shape
		want  string
	}{
		{Shape{Columns: []string{"note"}}, `{"id":"7","note":"x"}`}, // the key stays
		{Shape{Drop: []string{"meta", "amount", "id"}}, `{"id":"7","note":"x","paid":"t"}`},
		{Shape{Columns: []string{"note", "paid"}, Drop: []string{"paid"}}, `{"id":"7","note":"x"}`},
		{Shape{Drop: []string{"nope"}}, `{"amount":"12.50","id":"7","meta":"{\"a\": [1]}","note":"x","paid":"t"}`},
	} {
		s := newShaper(tc.shape)
		out := s.Apply(in)
		if len(out) != 1 {
			t.Fatalf("%v", out)
		}
		b, _ := json.Marshal(out[0].JSON().After)
		if string(b) != tc.want {
			t.Errorf("%+v: %s, want %s", tc.shape, b, tc.want)
		}
		if before := out[0].Event.GetBefore(); len(before.GetValues()) != len(out[0].Schema.GetColumns()) {
			t.Errorf("%+v: before has %d values for %d columns", tc.shape, len(before.GetValues()), len(out[0].Schema.GetColumns()))
		}
		if sc := out[0].Schema; sc != ordersSchema && sc.GetId() != fsm.SchemaID(sc) {
			t.Errorf("schema id %d is not its content id", sc.GetId())
		}
		// Same schema next time; the input is untouched.
		if again := s.Apply(in); again[0].Schema != out[0].Schema {
			t.Error("projection not reused")
		}
	}
	if in[0].Event != orig || len(orig.GetAfter().GetValues()) != 5 || in[0].Schema != ordersSchema {
		t.Fatal("input modified")
	}
	// Meilisearch still finds the key of a projected change.
	out := newShaper(Shape{Columns: []string{"note"}}).Apply(in)
	if k, err := documentKey(out[0].Schema, out[0].Event.GetAfter()); err != nil || k != "7" {
		t.Fatalf("key %q %v", k, err)
	}
}

// Changes a shape removes are skipped like other tables' changes: the
// offset moves past them.
func TestRunnerShapeSkips(t *testing.T) {
	h := newHarness(t)
	sc := &pb.Schema{Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id", Key: true}}}
	sc.Id = fsm.SchemaID(sc)
	for i, ids := range [][]string{{"1", "2"}, {"3"}, {"4"}} {
		tb := &pb.TxBatch{CommitLsn: uint64(100 * (i + 1)), EndLsn: uint64(100*(i+1) + 1), Final: true, Schemas: []*pb.Schema{sc}}
		for _, id := range ids {
			tb.Events = append(tb.Events, &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{text(id)}}})
		}
		b, _ := fsm.EncodeTxBatch(tb)
		h.node.mu.Lock()
		h.node.appendLocked(b)
		h.node.mu.Unlock()
	}
	h.run(Config{Shape: Shape{Where: []Condition{{Column: "id", Value: "3", Not: true}}}})
	h.waitRows("orders:1", "orders:2", "orders:4")
	waitFor(t, "offset past the filtered entry", func() bool { return h.offset("test") >= 3 })
}
