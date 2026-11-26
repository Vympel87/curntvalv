package sink

import (
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// where parses a where option or fails the test.
func where(t *testing.T, src string) *Expr {
	t.Helper()
	e, err := ParseWhere(src)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestParseShape(t *testing.T) {
	// The first form keeps working, with values taken as they are.
	row := &pb.Row{Values: []*pb.Value{text("John Smith"), text(""), text("b=c")}}
	sc := &pb.Schema{Columns: []*pb.Column{{Name: "name"}, {Name: "note"}, {Name: "a"}}}
	for _, src := range []string{"name=John Smith", "note=", "a=b=c", "name!=Bob", "note!=x"} {
		if !where(t, src).Eval(sc, row) {
			t.Errorf("%q does not match", src)
		}
	}
	for _, bad := range []string{"status", "=x", "!=x", "a = (", "a in (1", "(a = 1", "a is nul", "not", "a = 1 or"} {
		if _, err := ParseWhere(bad); err == nil {
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
		{Shape{Where: []*Expr{where(t, "note=paid")}}, "[insert:1:paid update:2:paid truncate backfill:3:paid reset]"},
		{Shape{Where: []*Expr{where(t, "id=1")}}, "[insert:1:paid delete:1:<nil> truncate reset]"},
		// A delete's old row holds only the key (default replica identity):
		// note is unknown, so the delete is skipped (on_leave=delete takes it).
		{Shape{Where: []*Expr{where(t, "note!=paid")}}, "[insert:2:open truncate reset]"},
		{Shape{Where: []*Expr{where(t, "missing!=x")}}, "[truncate reset]"},
		{Shape{Ops: []pb.Op{pb.Op_OP_UPDATE}, Where: []*Expr{where(t, "id=2")}}, "[update:2:paid reset]"},
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
	h.run(Config{Shape: Shape{Where: []*Expr{where(t, "id != 3")}}})
	h.waitRows("orders:1", "orders:2", "orders:4")
	waitFor(t, "offset past the filtered entry", func() bool { return h.offset("test") >= 3 })
}

func TestShapeLeaveAsDelete(t *testing.T) {
	full := func(id, note string) *pb.Row { return orderRow(id, note) } // REPLICA IDENTITY FULL
	keyRow := func(id string) *pb.Row {                                 // the default identity: key only
		return &pb.Row{Values: []*pb.Value{text(id), {Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL},
			{Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}
	}
	fullSchema := proto.Clone(ordersSchema).(*pb.Schema)
	fullSchema.ReplicaIdentity = identityFull
	ch := func(op pb.Op, before, after *pb.Row) event.Change {
		sc := ordersSchema // the default identity: old rows carry the key only
		if before != nil && before.GetValues()[1].GetKind() == pb.ValueKind_VALUE_KIND_TEXT {
			sc = fullSchema
		}
		return event.Change{Index: 1, Schema: sc, Event: &pb.Event{SchemaId: 7, Op: op, Before: before, After: after}}
	}
	paid := where(t, "note = paid")
	for _, tc := range []struct {
		name string
		c    event.Change
		skip string // result without on_leave
		del  string // with on_leave=delete
	}{
		{"update stays in", ch(pb.Op_OP_UPDATE, nil, full("1", "paid")), "update:1:paid", "update:1:paid"},
		{"update leaves, old row known", ch(pb.Op_OP_UPDATE, full("1", "paid"), full("1", "refunded")), "-", "delete:1:paid"},
		{"update leaves, old row unknown", ch(pb.Op_OP_UPDATE, nil, full("1", "refunded")), "-", "delete:1:<nil>"},
		{"update never was in", ch(pb.Op_OP_UPDATE, full("1", "open"), full("1", "refunded")), "-", "-"},
		{"key change out of the filter", ch(pb.Op_OP_UPDATE, keyRow("1"), full("2", "open")), "-", "delete:1:<nil>"},
		{"delete, old row unknown", ch(pb.Op_OP_DELETE, keyRow("1"), nil), "-", "delete:1:<nil>"},
		// The NULLs of a key-only old row are not values: != does not hold.
		{"delete, old row unknown, !=", event.Change{Index: 1, Schema: ordersSchema, Event: &pb.Event{SchemaId: 7, Op: pb.Op_OP_DELETE,
			Before: keyRow("1")}}, "-", "delete:1:<nil>"},
		{"delete, old row known in", ch(pb.Op_OP_DELETE, full("1", "paid"), nil), "delete:1:paid", "delete:1:paid"},
		{"delete, old row known out", ch(pb.Op_OP_DELETE, full("1", "open"), nil), "-", "-"},
		{"insert out", ch(pb.Op_OP_INSERT, nil, full("1", "open")), "-", "-"},
	} {
		for _, leave := range []bool{false, true} {
			out := newShaper(Shape{Where: []*Expr{paid}, LeaveAsDelete: leave}).Apply([]event.Change{tc.c})
			got := "-"
			if len(out) == 1 {
				got = shapeIDs(out)[0]
			}
			want := tc.skip
			if leave {
				want = tc.del
			}
			if got != want {
				t.Errorf("%s, on_leave=%v: %s, want %s", tc.name, leave, got, want)
			}
		}
	}
	// A delete made from an update carries the key: mirrors can drop it.
	out := newShaper(Shape{Where: []*Expr{paid}, LeaveAsDelete: true}).Apply([]event.Change{ch(pb.Op_OP_UPDATE, nil, full("9", "refunded"))})
	if k, err := documentKey(ordersSchema, out[0].Event.GetBefore()); err != nil || k != "9" || out[0].Event.GetAfter() != nil {
		t.Fatalf("delete %v: %q %v", out[0].Event, k, err)
	}
}
