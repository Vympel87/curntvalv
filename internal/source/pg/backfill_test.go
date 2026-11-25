package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// loggedEvent is a stored event with its schema.
type loggedEvent struct {
	ev     *pb.Event
	schema *pb.Schema
}

func (n *testNode) logged() []loggedEvent {
	n.t.Helper()
	var out []loggedEvent
	from := uint64(0)
	for {
		es, err := n.node.ReadCommitted(context.Background(), from, 500)
		if err != nil {
			n.t.Fatal(err)
		}
		if len(es) == 0 {
			return out
		}
		for _, e := range es {
			from = e.Index + 1
			tb, ok, err := fsm.DecodeTxBatch(e.Data)
			if err != nil {
				n.t.Fatal(err)
			}
			for _, ev := range tb.GetEvents() {
				if ok {
					sc, _ := n.fsm.Schema(ev.GetSchemaId())
					out = append(out, loggedEvent{ev: ev, schema: sc})
				}
			}
		}
	}
}

// replay applies the stored events of table in log order, the way a
// consumer keyed by the first column would, and returns id → note.
func replay(evs []loggedEvent, table string) map[string]string {
	rows := map[string]string{}
	val := func(r *pb.Row, i int) string { return string(r.GetValues()[i].GetData()) }
	for _, e := range evs {
		if e.schema.GetName() != table {
			continue
		}
		switch e.ev.GetOp() {
		case pb.Op_OP_INSERT, pb.Op_OP_UPDATE, pb.Op_OP_BACKFILL:
			if b := e.ev.GetBefore(); b != nil {
				delete(rows, val(b, 0))
			}
			rows[val(e.ev.GetAfter(), 0)] = val(e.ev.GetAfter(), 1)
		case pb.Op_OP_DELETE:
			delete(rows, val(e.ev.GetBefore(), 0))
		case pb.Op_OP_TRUNCATE:
			clear(rows)
		}
	}
	return rows
}

func (e *pgEnv) tableRows() map[string]string {
	e.t.Helper()
	rows, err := e.db.Query(context.Background(), fmt.Sprintf("SELECT id::text, note FROM %s", e.table))
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, note string
		if err := rows.Scan(&id, &note); err != nil {
			e.t.Fatal(err)
		}
		out[id] = note
	}
	return out
}

func backfillOf(n *testNode, table string) *pb.Backfill {
	return findBackfill(n.fsm.Backfills(), "public."+table)
}

func waitBackfill(t *testing.T, n *testNode, table string, state pb.BackfillState) *pb.Backfill {
	t.Helper()
	waitFor(t, "backfill "+state.String(), 60*time.Second, func() bool { return backfillOf(n, table).GetState() == state })
	return backfillOf(n, table)
}

// backfilled counts the stored backfill events per id.
func backfilled(n *testNode) map[string]int {
	got := map[string]int{}
	for _, ev := range n.events() {
		if ev.op == pb.Op_OP_BACKFILL {
			got[ev.id]++
		}
	}
	return got
}

func TestBackfillCopiesExistingRows(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'n' || g FROM generate_series(1, 2500) g", e.table))
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.config(Limits{MaxEvents: 300}), n)
	e.waitSlot()
	ctx := context.Background()
	idx, names, err := src.StartBackfill(ctx, []string{e.table})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "public."+e.table || idx == 0 {
		t.Fatalf("%d %v", idx, names)
	}
	bf := waitBackfill(t, n, e.table, pb.BackfillState_BACKFILL_STATE_DONE)
	if bf.GetRows() != 2500 || string(bf.GetLastKey()[0]) != "2500" || bf.GetId() != idx {
		t.Fatalf("%v", bf)
	}
	got := backfilled(n)
	for i := 1; i <= 2500; i++ {
		if got[fmt.Sprint(i)] != 1 {
			t.Fatalf("row %d backfilled %d times", i, got[fmt.Sprint(i)])
		}
	}
	// Chunks of 300 rows, each in its own entry; ingest goes on.
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (9999, 'after')", e.table))
	waitFor(t, "insert after the backfill", 10*time.Second, func() bool { return replay(n.logged(), e.table)["9999"] == "after" })
	if got, want := replay(n.logged(), e.table), e.tableRows(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("replayed %d rows, table has %d", len(got), len(want))
	}
}

// Rows written while the backfill reads the table: a consumer replaying the
// log ends up with exactly the table.
func TestBackfillWithConcurrentWrites(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'n' || g FROM generate_series(1, 3000) g", e.table))
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.config(Limits{MaxEvents: 100}), n)
	e.waitSlot()

	// Three writers, mostly on the keys just ahead of the backfill, so that
	// many changes fall into chunk windows.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	for w := range 3 {
		db := connect(t, e.url) // e.db is not safe for concurrent use
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(1, uint64(w)))
			for i := 0; ctx.Err() == nil; i++ {
				id := r.IntN(3500) + 1
				if k := backfillOf(n, e.table).GetLastKey(); len(k) == 1 && r.IntN(4) > 0 {
					fmt.Sscan(string(k[0]), &id)
					id += r.IntN(150) + 1
				}
				var sql string
				switch r.IntN(4) {
				case 0:
					sql = fmt.Sprintf("DELETE FROM %s WHERE id = %d", e.table, id)
				case 1:
					sql = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'w%d.%d') ON CONFLICT (id) DO UPDATE SET note = excluded.note", e.table, id, w, i)
				case 2: // move a row to a new key
					sql = fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", e.table, 10_000+w*1_000_000+i, id)
				default:
					sql = fmt.Sprintf("UPDATE %s SET note = 'u%d.%d' WHERE id = %d", e.table, w, i, id)
				}
				if _, err := db.Exec(context.Background(), sql); err != nil {
					var pe *pgconn.PgError
					if errors.As(err, &pe) && (pe.Code == "40P01" || pe.Code == "23505") {
						continue // writers collided; nothing was written
					}
					t.Errorf("%s: %v", sql, err)
					return
				}
			}
		}()
	}
	if _, _, err := src.StartBackfill(context.Background(), []string{e.table}); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, n, e.table, pb.BackfillState_BACKFILL_STATE_DONE)
	cancel()
	wg.Wait()
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (-1, 'end')", e.table))
	waitFor(t, "stream caught up", 30*time.Second, func() bool { return replay(n.logged(), e.table)["-1"] == "end" })
	got, want := replay(n.logged(), e.table), e.tableRows()
	for id, note := range want {
		if got[id] != note {
			t.Errorf("row %s: replayed %q, table %q", id, got[id], note)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("row %s replayed but not in the table", id)
		}
	}
	for id, c := range backfilled(n) {
		if c > 1 {
			t.Errorf("row %s backfilled %d times", id, c)
		}
	}
}

// A backfill continues after the source restarts, from the stored key, and
// stores every row once.
func TestBackfillResumesAfterRestart(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'n' || g FROM generate_series(1, 5000) g", e.table))
	n := startNode(t, t.TempDir())
	src, stop := runSourceGet(t, e.config(Limits{MaxEvents: 50}), n)
	e.waitSlot()
	if _, _, err := src.StartBackfill(context.Background(), []string{e.table}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "some progress", 30*time.Second, func() bool { return backfillOf(n, e.table).GetRows() >= 500 })
	stop()
	if bf := backfillOf(n, e.table); bf.GetState() != pb.BackfillState_BACKFILL_STATE_PENDING {
		t.Fatalf("finished before the restart: %v", bf)
	}
	runSource(t, e.config(Limits{MaxEvents: 50}), n)
	bf := waitBackfill(t, n, e.table, pb.BackfillState_BACKFILL_STATE_DONE)
	got := backfilled(n)
	for i := 1; i <= 5000; i++ {
		if got[fmt.Sprint(i)] != 1 {
			t.Fatalf("row %d backfilled %d times", i, got[fmt.Sprint(i)])
		}
	}
	if bf.GetRows() != 5000 {
		t.Fatalf("%v", bf)
	}
}

func TestBackfillStartAndCancel(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'n' FROM generate_series(1, 100000) g", e.table))
	e.exec(fmt.Sprintf("CREATE TABLE %s_nopk (id int)", e.table))
	e.exec(fmt.Sprintf("CREATE TABLE %s_unpub (id int PRIMARY KEY)", e.table))
	t.Cleanup(func() {
		e.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %[1]s_nopk, %[1]s_unpub", e.table))
	})
	e.exec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s_nopk", e.pub, e.table))
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.config(Limits{MaxEvents: 10}), n)
	e.waitSlot()
	ctx := context.Background()
	for _, tc := range []struct{ table, msg string }{
		{e.table + "_nopk", "no primary key"},
		{e.table + "_unpub", "not in publication"},
		{"nope_" + e.table, "no such table"},
		{"pg_class", "not in publication"},
	} {
		if _, _, err := src.StartBackfill(ctx, []string{e.table, tc.table}); !errors.Is(err, ErrBackfillTable) || !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("%s: %v", tc.table, err)
		}
	}
	if len(n.fsm.Backfills()) != 0 {
		t.Fatal("a failed start stored a backfill")
	}
	if _, _, err := src.CancelBackfill(ctx, []string{e.table}); !errors.Is(err, ErrNoBackfill) {
		t.Fatalf("cancel without backfill: %v", err)
	}
	// Names resolve like in SQL, also with the schema.
	if _, names, err := src.StartBackfill(ctx, []string{"public." + e.table, e.table}); err != nil || len(names) != 1 {
		t.Fatalf("%v %v", names, err)
	}
	waitFor(t, "some progress", 30*time.Second, func() bool { return backfillOf(n, e.table).GetRows() > 0 })
	if _, names, err := src.CancelBackfill(ctx, []string{e.table}); err != nil || names[0] != "public."+e.table {
		t.Fatalf("cancel: %v %v", names, err)
	}
	bf := backfillOf(n, e.table)
	if bf.GetState() != pb.BackfillState_BACKFILL_STATE_CANCELLED {
		t.Fatalf("%v", bf)
	}
	time.Sleep(500 * time.Millisecond)
	if got := backfillOf(n, e.table); got.GetRows() != bf.GetRows() {
		t.Fatalf("progress after the cancel: %v", got)
	}
}

// With BackfillInitial a new cluster backfills every published table with a
// primary key.
func TestBackfillInitial(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'n' FROM generate_series(1, 30) g", e.table))
	e.exec(fmt.Sprintf("CREATE TABLE %s_nopk (id int)", e.table))
	t.Cleanup(func() { e.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s_nopk", e.table)) })
	e.exec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s_nopk", e.pub, e.table))
	n := startNode(t, t.TempDir())
	cfg := e.config(Limits{})
	cfg.BackfillInitial = true
	runSource(t, cfg, n)
	bf := waitBackfill(t, n, e.table, pb.BackfillState_BACKFILL_STATE_DONE)
	if bf.GetRows() != 30 || len(n.fsm.Backfills()) != 1 {
		t.Fatalf("%v", n.fsm.Backfills())
	}
}

// A backfilled row looks exactly like the same row from the stream: the
// same schema and the same text values.
func TestBackfillRowsMatchTheStream(t *testing.T) {
	e := newPGEnv(t)
	e.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN ts timestamptz, ADD COLUMN num numeric(10,3), ADD COLUMN f float8, "+
		"ADD COLUMN b bytea, ADD COLUMN j jsonb, ADD COLUMN arr int[], ADD COLUMN flag bool, ADD COLUMN d date, ADD COLUMN gen int GENERATED ALWAYS AS (id * 2) STORED", e.table))
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.config(Limits{}), n)
	e.waitSlot()
	e.exec(fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a''b', '2026-11-25 10:11:12.345678+07', 12.5, 0.1, '\x00ff', '{"k": [1, null]}', '{1,2}', true, '2026-01-02')`, e.table))
	waitFor(t, "insert", 10*time.Second, func() bool { return len(n.events()) == 1 })
	if _, _, err := src.StartBackfill(context.Background(), []string{e.table}); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, n, e.table, pb.BackfillState_BACKFILL_STATE_DONE)
	evs := n.logged()
	if len(evs) != 2 || evs[1].ev.GetOp() != pb.Op_OP_BACKFILL {
		t.Fatalf("%d events", len(evs))
	}
	ins, bf := evs[0].ev, evs[1].ev
	if ins.GetSchemaId() != bf.GetSchemaId() {
		t.Fatalf("schema from the catalog %v differs from the stream's %v", evs[1].schema, evs[0].schema)
	}
	for i, v := range ins.GetAfter().GetValues() {
		if w := bf.GetAfter().GetValues()[i]; w.GetKind() != v.GetKind() || string(w.GetData()) != string(v.GetData()) {
			t.Errorf("column %s: backfill %v, stream %v", evs[0].schema.GetColumns()[i].GetName(), w, v)
		}
	}
}

func connect(t *testing.T, url string) *pgx.Conn {
	t.Helper()
	db, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(context.Background()) })
	return db
}
