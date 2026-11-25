package pg

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

type fakeRegistry map[uint64]bool

func (r fakeRegistry) HasSchema(id uint64) bool { return r[id] }

var orders = &pglogrepl.RelationMessage{
	RelationID: 16384, Namespace: "public", RelationName: "orders", ReplicaIdentity: 'd',
	Columns: []*pglogrepl.RelationMessageColumn{
		{Flags: 1, Name: "id", DataType: 23, TypeModifier: -1},
		{Name: "note", DataType: 25, TypeModifier: -1},
	},
}

func text(vals ...string) *pglogrepl.TupleData {
	t := &pglogrepl.TupleData{}
	for _, v := range vals {
		switch v {
		case "<null>":
			t.Columns = append(t.Columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull})
		case "<toast>":
			t.Columns = append(t.Columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast})
		default:
			t.Columns = append(t.Columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)})
		}
	}
	t.ColumnNum = uint16(len(t.Columns))
	return t
}

func begin(lsn uint64) *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{FinalLSN: pglogrepl.LSN(lsn), CommitTime: time.Unix(1700000000, 0), Xid: uint32(lsn)}
}

func commit(lsn uint64) *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{CommitLSN: pglogrepl.LSN(lsn), TransactionEndLSN: pglogrepl.LSN(lsn + 8)}
}

func insert(id int) *pglogrepl.InsertMessage {
	return &pglogrepl.InsertMessage{RelationID: 16384, Tuple: text(fmt.Sprint(id), "x")}
}

// feed runs messages through d and returns all batches produced.
func feed(t *testing.T, d *decoder, msgs ...pglogrepl.Message) []*pb.TxBatch {
	t.Helper()
	var out []*pb.TxBatch
	for _, m := range msgs {
		bs, err := d.handle(m)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, bs...)
	}
	return out
}

// tx builds the messages of one transaction with n inserts.
func tx(lsn uint64, n int) []pglogrepl.Message {
	msgs := []pglogrepl.Message{begin(lsn)}
	for i := range n {
		msgs = append(msgs, insert(i))
	}
	return append(msgs, commit(lsn))
}

func TestDecodeChanges(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	got := feed(t, d,
		orders,
		begin(100),
		insert(1),
		&pglogrepl.UpdateMessage{RelationID: 16384, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: text("1", "<null>"), NewTuple: text("2", "<toast>")},
		&pglogrepl.DeleteMessage{RelationID: 16384, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: text("2", "<null>")},
		&pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{16384}},
		commit(100),
	)
	if len(got) != 1 {
		t.Fatalf("got %d batches", len(got))
	}
	b := got[0]
	if !b.Final || b.CommitLsn != 100 || b.EndLsn != 108 || b.FirstSeq != 0 || b.Xid != 100 || b.CommitTimeUs != 1700000000_000000 {
		t.Fatalf("batch header %+v", b)
	}
	if len(b.Schemas) != 1 || b.Schemas[0].Name != "orders" || !b.Schemas[0].Columns[0].Key || b.Schemas[0].Columns[1].Key {
		t.Fatalf("schemas %v", b.Schemas)
	}
	ops := []pb.Op{pb.Op_OP_INSERT, pb.Op_OP_UPDATE, pb.Op_OP_DELETE, pb.Op_OP_TRUNCATE}
	if len(b.Events) != 4 {
		t.Fatalf("events %v", b.Events)
	}
	for i, ev := range b.Events {
		if ev.Op != ops[i] || ev.SchemaId != b.Schemas[0].Id {
			t.Fatalf("event %d: %v", i, ev)
		}
	}
	upd := b.Events[1]
	if upd.Before.Values[1].Kind != pb.ValueKind_VALUE_KIND_NULL || upd.After.Values[1].Kind != pb.ValueKind_VALUE_KIND_UNCHANGED ||
		string(upd.After.Values[0].Data) != "2" {
		t.Fatalf("update %v", upd)
	}
	if b.Events[2].After != nil || b.Events[2].Before == nil || b.Events[3].Before != nil || b.Events[3].After != nil {
		t.Fatalf("row images: delete %v truncate %v", b.Events[2], b.Events[3])
	}
}

func TestSchemaSentOnce(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	msgs := append([]pglogrepl.Message{orders}, tx(100, 1)...)
	msgs = append(msgs, tx(200, 1)...)
	got := feed(t, d, msgs...)
	if len(got[0].Schemas) != 1 || len(got[1].Schemas) != 0 {
		t.Fatalf("schemas per batch: %d, %d", len(got[0].Schemas), len(got[1].Schemas))
	}
	// Already registered in the cluster: never sent.
	reg := fakeRegistry{got[0].Schemas[0].Id: true}
	d2 := newDecoder(reg, Limits{}, fsm.SourceState{})
	got2 := feed(t, d2, append([]pglogrepl.Message{orders}, tx(100, 1)...)...)
	if len(got2[0].Schemas) != 0 {
		t.Fatal("registered schema sent again")
	}
	// A changed definition is a new schema.
	changed := *orders
	changed.Columns = append(changed.Columns, &pglogrepl.RelationMessageColumn{Name: "extra", DataType: 25})
	got3 := feed(t, d2, append([]pglogrepl.Message{&changed}, tx(300, 1)...)...)
	if len(got3[0].Schemas) != 1 || got3[0].Events[0].SchemaId == got2[0].Events[0].SchemaId {
		t.Fatal("changed schema not sent")
	}
}

func TestLargeTransactionIsSplit(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{MaxEvents: 4}, fsm.SourceState{})
	got := feed(t, d, append([]pglogrepl.Message{orders}, tx(100, 10)...)...)
	want := []struct {
		first  uint32
		events int
		final  bool
	}{{0, 4, false}, {4, 4, false}, {8, 2, true}}
	if len(got) != len(want) {
		t.Fatalf("got %d batches", len(got))
	}
	for i, w := range want {
		b := got[i]
		if b.FirstSeq != w.first || len(b.Events) != w.events || b.Final != w.final || b.CommitLsn != 100 {
			t.Fatalf("batch %d: first %d events %d final %v", i, b.FirstSeq, len(b.Events), b.Final)
		}
	}
	if len(got[0].Schemas) != 1 || len(got[1].Schemas)+len(got[2].Schemas) != 0 {
		t.Fatal("schema should travel with the first batch only")
	}
	// Exactly at the limit: the final batch is empty but still sent.
	d2 := newDecoder(fakeRegistry{}, Limits{MaxEvents: 4}, fsm.SourceState{})
	got2 := feed(t, d2, append([]pglogrepl.Message{orders}, tx(100, 8)...)...)
	if len(got2) != 3 || !got2[2].Final || len(got2[2].Events) != 0 || got2[2].FirstSeq != 8 {
		t.Fatalf("final empty batch: %v", got2)
	}
}

// Every event lands in the state machine exactly once when the decoder
// restarts from what the state machine stored.
func TestResumeSkipsStoredEvents(t *testing.T) {
	msgs := []pglogrepl.Message{orders}
	msgs = append(msgs, tx(100, 3)...)
	msgs = append(msgs, tx(200, 10)...)
	msgs = append(msgs, tx(300, 2)...)

	// 3 + (4+4+2) + 2 events = 5 batches.
	for stop := 1; stop <= 5; stop++ { // number of batches stored before "the crash"
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			f := fsm.New()
			var idx uint64
			store := func(bs []*pb.TxBatch) {
				for _, b := range bs {
					data, _ := fsm.EncodeTxBatch(b)
					idx++
					if err := f.Apply(raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: data}); err != nil {
						t.Fatal(err)
					}
				}
			}
			all := feed(t, newDecoder(f, Limits{MaxEvents: 4}, fsm.SourceState{}), msgs...)
			if len(all) != 5 {
				t.Fatalf("%d batches", len(all))
			}
			store(all[:stop])
			// Postgres sends everything again (we start at the ack position).
			rest := feed(t, newDecoder(f, Limits{MaxEvents: 4}, f.Source()), msgs...)
			store(rest)
			if f.Source().Anomalies != 0 || f.Source().CommitLSN != 300 || f.Source().PartialLSN != 0 {
				t.Fatalf("source %+v", f.Source())
			}
			events := 0
			for _, b := range append(all[:stop:stop], rest...) {
				events += len(b.Events)
			}
			if events != 15 {
				t.Fatalf("stored %d events, want 15", events)
			}
		})
	}
}

func TestEmptyTransactions(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
	if got := feed(t, d, tx(100, 0)...); len(got) != 0 {
		t.Fatalf("empty tx produced %v", got)
	}
	e := d.takeEmpty()
	if e == nil || e.CommitLsn != 100 || e.EndLsn != 108 || !e.Final {
		t.Fatalf("takeEmpty %v", e)
	}
	if d.takeEmpty() != nil {
		t.Fatal("takeEmpty returned the same tx twice")
	}
	feed(t, d, tx(200, 0)...)
	feed(t, d, append([]pglogrepl.Message{orders}, tx(300, 1)...)...)
	if d.takeEmpty() != nil {
		t.Fatal("a newer stored tx supersedes the empty one")
	}
}

func TestIdlePosition(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{CommitLSN: 100, EndLSN: 108})
	d.idle(90) // behind what is stored
	if d.takeEmpty() != nil {
		t.Fatal("idle position behind the stored one")
	}
	d.idle(500)
	d.idle(400) // older keepalive: keep the newest
	e := d.takeEmpty()
	if e == nil || e.CommitLsn != 499 || e.EndLsn != 500 || !e.Final || len(e.Events) != 0 {
		t.Fatalf("got %v", e)
	}
	// A transaction older than the stored idle position is now skipped.
	if got := feed(t, d, append([]pglogrepl.Message{orders}, tx(450, 1)...)...); len(got) != 0 {
		t.Fatalf("tx before stored position: %v", got)
	}
	// One committing exactly at it was still open at the keepalive: taken.
	if got := feed(t, d, tx(500, 1)...); len(got) != 1 || got[0].CommitLsn != 500 {
		t.Fatalf("tx at the idle position: %v", got)
	}
	// No idle position while a transaction is open or partly stored.
	feed(t, d, begin(600))
	d.idle(700)
	feed(t, d, commit(600))
	if e := d.takeEmpty(); e != nil && e.EndLsn == 700 {
		t.Fatal("idle recorded inside a transaction")
	}
	p := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{CommitLSN: 100, PartialLSN: 200, PartialNextSeq: 1})
	p.idle(900)
	if p.takeEmpty() != nil {
		t.Fatal("idle recorded while a transaction is partly stored")
	}
}

func TestSkipsAlreadyStoredTransactions(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{CommitLSN: 200, EndLSN: 208})
	got := feed(t, d, append(append(append([]pglogrepl.Message{orders}, tx(100, 1)...), tx(200, 1)...), tx(300, 1)...)...)
	if len(got) != 1 || got[0].CommitLsn != 300 {
		t.Fatalf("got %v", got)
	}
}

func TestPartialTransactionNotResent(t *testing.T) {
	d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{CommitLSN: 100, PartialLSN: 200, PartialNextSeq: 4})
	_, err := d.handle(begin(300))
	if !errors.Is(err, ErrPartialLost) {
		t.Fatalf("got %v", err)
	}
	// Resent with fewer changes than already stored.
	d = newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{CommitLSN: 100, PartialLSN: 200, PartialNextSeq: 4})
	var last error
	for _, m := range append([]pglogrepl.Message{orders}, tx(200, 2)...) {
		if _, err := d.handle(m); err != nil {
			last = err
		}
	}
	if !errors.Is(last, ErrPartialLost) {
		t.Fatalf("got %v", last)
	}
}

func TestProtocolErrors(t *testing.T) {
	cases := map[string][]pglogrepl.Message{
		"change outside tx": {orders, insert(1)},
		"nested begin":      {begin(1), begin(2)},
		"commit outside tx": {commit(1)},
		"commit mismatch":   {begin(1), commit(2)},
		"unknown relation":  {begin(1), insert(1)},
	}
	for name, msgs := range cases {
		d := newDecoder(fakeRegistry{}, Limits{}, fsm.SourceState{})
		var last error
		for _, m := range msgs {
			if _, err := d.handle(m); err != nil {
				last = err
				break
			}
		}
		if !errors.Is(last, ErrProtocol) {
			t.Errorf("%s: got %v", name, last)
		}
	}
}
