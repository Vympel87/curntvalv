package pg

import (
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func mark(lsn uint64, content string) []pglogrepl.Message {
	return []pglogrepl.Message{begin(lsn),
		&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: watermarkPrefix, Content: []byte(content)}, commit(lsn)}
}

// testChunk is a chunk of orders with rows id 1..n, as the reader fills it.
func testChunk(w *window, nonce string, n int, done bool) *chunk {
	c := &chunk{nonce: nonce, table: "public.orders", id: 7, oid: 16384, keyCols: []string{"id"}, afterKey: [][]byte{[]byte("0")}}
	w.begin(c)
	schema := &pb.Schema{RelationOid: 16384, Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id", TypeOid: 23}, {Name: "note", TypeOid: 25}}}
	schema.Id = fsm.SchemaID(schema)
	var rows []*pb.Row
	var keys []string
	var last [][]byte
	for i := 1; i <= n; i++ {
		id := []byte(fmt.Sprint(i))
		rows = append(rows, &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: id}, {Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("old")}}})
		last = [][]byte{id}
		keys = append(keys, encodeKey(last))
	}
	w.fill(c, schema, rows, keys, last, done)
	return c
}

func ids(evs []*pb.Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, string(e.GetAfter().GetValues()[0].GetData()))
	}
	return out
}

func outcome(t *testing.T, c *chunk) string {
	t.Helper()
	select {
	case o := <-c.outcome:
		return o
	default:
		t.Fatal("no outcome")
		return ""
	}
}

func TestWindowDropsChangedRows(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	d.bf = &window{}
	c := testChunk(d.bf, "n1", 5, false)
	msgs := []pglogrepl.Message{orders,
		// Before the low watermark: the read already sees it.
		begin(90), &pglogrepl.UpdateMessage{RelationID: 16384, NewTuple: text("1", "new")}, commit(90)}
	msgs = append(msgs, mark(100, "low n1")...)
	msgs = append(msgs, begin(110),
		&pglogrepl.UpdateMessage{RelationID: 16384, NewTuple: text("2", "new")},
		&pglogrepl.DeleteMessage{RelationID: 16384, OldTuple: text("3", "<null>")},
		// The key of row 4 changes to 40: both keys count.
		&pglogrepl.UpdateMessage{RelationID: 16384, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: text("4", "<null>"), NewTuple: text("40", "x")},
		commit(110))
	msgs = append(msgs, mark(120, "high other")...) // not ours
	msgs = append(msgs, mark(130, "high n1")...)
	got := feed(t, d, msgs...)
	if outcome(t, c) != "" {
		t.Fatal("chunk not stored")
	}
	b := got[len(got)-1]
	if b.CommitLsn != 130 || !b.Final || b.FirstSeq != 0 || len(b.Schemas) != 1 {
		t.Fatalf("batch %v", b)
	}
	if fmt.Sprint(ids(b.Events)) != "[1 5]" || b.Events[0].Op != pb.Op_OP_BACKFILL || b.Events[0].SchemaId != b.Schemas[0].Id {
		t.Fatalf("events %v", b.Events)
	}
	ch := b.Backfill
	if ch.GetTable() != "public.orders" || ch.GetId() != 7 || string(ch.GetAfterKey()[0]) != "0" || string(ch.GetLastKey()[0]) != "5" || ch.GetDone() {
		t.Fatalf("progress %v", ch)
	}
	// The window is closed: later watermarks and changes do nothing.
	if len(feed(t, d, mark(140, "high n1")...)) != 0 {
		t.Fatal("second high watermark stored something")
	}
}

func TestWindowGivesUp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change pglogrepl.Message
		want   string
	}{
		{"truncate", &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{16384}}, "table truncated"},
		{"delete without key", &pglogrepl.DeleteMessage{RelationID: 16384}, "delete without its primary key"},
		{"unchanged TOAST key", &pglogrepl.UpdateMessage{RelationID: 16384, NewTuple: text("<toast>", "x")}, "change without its primary key"},
	} {
		d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
		d.bf = &window{}
		c := testChunk(d.bf, "n", 3, false)
		msgs := append([]pglogrepl.Message{orders}, mark(100, "low n")...)
		msgs = append(msgs, begin(110), tc.change, commit(110))
		msgs = append(msgs, mark(120, "high n")...)
		got := feed(t, d, msgs...)
		if o := outcome(t, c); o != tc.want {
			t.Errorf("%s: outcome %q", tc.name, o)
		}
		if last := got[len(got)-1]; last.CommitLsn == 120 {
			t.Errorf("%s: stored %v", tc.name, last)
		}
	}

	// A high watermark without the low one (the window opened in an
	// earlier session).
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	d.bf = &window{}
	c := testChunk(d.bf, "n", 3, false)
	feed(t, d, mark(120, "high n")...)
	if o := outcome(t, c); o != "high watermark before the low one" {
		t.Fatalf("outcome %q", o)
	}
	// Changes of other tables do not matter.
	c = testChunk(d.bf, "m", 3, false)
	users := &pglogrepl.RelationMessage{RelationID: 99, Namespace: "public", RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{{Flags: 1, Name: "id", DataType: 23}}}
	got := feed(t, d, append(append(mark(130, "low m"), users, begin(140), &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{99}}, commit(140)),
		mark(150, "high m")...)...)
	if o := outcome(t, c); o != "" || len(got[len(got)-1].Events) != 3 {
		t.Fatalf("outcome %q, %v", o, got)
	}
}

// The last chunk may have no rows; its batch is stored all the same, to
// record that the backfill is done.
func TestWindowEmptyLastChunk(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	d.bf = &window{}
	c := testChunk(d.bf, "n", 0, true)
	got := feed(t, d, append(mark(100, "low n"), mark(110, "high n")...)...)
	if outcome(t, c) != "" || len(got) != 1 || got[0].CommitLsn != 110 || !got[0].Backfill.GetDone() || len(got[0].Events) != 0 {
		t.Fatalf("%v", got)
	}
	// A watermark of an unknown chunk is an empty transaction, as without
	// a backfill.
	if got := feed(t, d, mark(120, "low x")...); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	if e := d.takeEmpty(); e == nil || e.CommitLsn != 120 {
		t.Fatalf("empty %v", e)
	}
}
