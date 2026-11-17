package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/wal"
)

var (
	_ Node  = (*raft.Node)(nil)
	_ State = (*fsm.FSM)(nil)
)

// pgURL returns the test database URL or skips the test.
func pgURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("CURNTVALV_TEST_PG_URL")
	if u == "" {
		t.Skip("CURNTVALV_TEST_PG_URL not set; run `make test-pg` to test against a real Postgres")
	}
	return u
}

var nameSeq atomic.Uint64

// pgEnv is one test's slice of the database: its own table, publication and
// slot, removed when the test ends.
type pgEnv struct {
	t     *testing.T
	url   string
	db    *pgx.Conn
	table string
	pub   string
	slot  string
}

func newPGEnv(t *testing.T) *pgEnv {
	url := pgURL(t)
	ctx := context.Background()
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	n := fmt.Sprintf("t%d_%d", time.Now().UnixNano()%1_000_000_000, nameSeq.Add(1))
	e := &pgEnv{t: t, url: url, db: db, table: "orders_" + n, pub: "pub_" + n, slot: "slot_" + n}
	e.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, note text)", e.table))
	e.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", e.pub, e.table))
	t.Cleanup(func() {
		ctx := context.Background()
		// The slot may still be held for a moment by a stopping reader.
		for range 50 {
			db.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", e.slot)
			if _, err := db.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", e.slot); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		db.Exec(ctx, "DROP PUBLICATION IF EXISTS "+e.pub)
		db.Exec(ctx, "DROP TABLE IF EXISTS "+e.table)
		db.Close(ctx)
	})
	return e
}

func (e *pgEnv) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

func (e *pgEnv) config(limits Limits) Config {
	return Config{URL: e.url, Slot: e.slot, Publication: e.pub, StatusInterval: 200 * time.Millisecond, Limits: limits}
}

// testNode is a single-node Raft cluster on disk with the metadata FSM.
type testNode struct {
	t      *testing.T
	dir    string
	node   *raft.Node
	fsm    *fsm.FSM
	log    *wal.Log
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func startNode(t *testing.T, dir string) *testNode {
	t.Helper()
	log, err := wal.Open(filepath.Join(dir, "wal"), wal.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dir, "snaps"), true)
	if err != nil {
		t.Fatal(err)
	}
	f := fsm.New()
	node, err := raft.NewNode(raft.Config{ID: 1, Peers: []uint64{1}, TickInterval: 5 * time.Millisecond, SnapshotEntries: 50},
		raft.Storage{Log: log, Stable: raft.NewFileStableStore(filepath.Join(dir, "state"), true), Snapshots: snaps},
		nopTransport{}, f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &testNode{t: t, dir: dir, node: node, fsm: f, log: log, cancel: cancel}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := node.Run(ctx); err != nil {
			t.Errorf("raft: %v", err)
		}
	}()
	t.Cleanup(n.stop)
	return n
}

func (n *testNode) stop() {
	if n.cancel == nil {
		return
	}
	n.cancel()
	n.wg.Wait()
	n.log.Close()
	n.cancel = nil
}

type nopTransport struct{}

func (nopTransport) Send(raft.Message) {}

// runSource runs a Source until the returned stop function is called.
func runSource(t *testing.T, cfg Config, n *testNode) (stop func()) {
	t.Helper()
	src, err := New(cfg, n.node, n.fsm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); src.Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

type storedEvent struct {
	lsn   uint64
	seq   uint32
	op    pb.Op
	table string
	id    string
	note  string
}

// events reads every stored event from the node's log.
func (n *testNode) events() []storedEvent {
	n.t.Helper()
	ctx := context.Background()
	var out []storedEvent
	from := uint64(0)
	for {
		es, err := n.node.ReadCommitted(ctx, from, 500)
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
			if !ok {
				continue
			}
			for i, ev := range tb.GetEvents() {
				se := storedEvent{lsn: tb.GetCommitLsn(), seq: tb.GetFirstSeq() + uint32(i), op: ev.GetOp()}
				if sc, ok := n.fsm.Schema(ev.GetSchemaId()); ok {
					se.table = sc.GetName()
				}
				r := ev.GetAfter()
				if r == nil {
					r = ev.GetBefore()
				}
				if r != nil && len(r.GetValues()) == 2 {
					se.id, se.note = string(r.GetValues()[0].GetData()), string(r.GetValues()[1].GetData())
				}
				out = append(out, se)
			}
		}
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestIngestChanges(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	runSource(t, e.config(Limits{}), n)
	// Wait until the slot exists, so the changes below are captured.
	waitFor(t, "slot", 10*time.Second, func() bool {
		var ok bool
		e.db.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", e.slot).Scan(&ok)
		return ok
	})

	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", e.table))
	e.exec(fmt.Sprintf("UPDATE %s SET note = 'c' WHERE id = 1", e.table))
	e.exec(fmt.Sprintf("DELETE FROM %s WHERE id = 2", e.table))
	e.exec(fmt.Sprintf("TRUNCATE %s", e.table))

	waitFor(t, "5 events", 10*time.Second, func() bool { return len(n.events()) == 5 })
	evs := n.events()
	want := []struct {
		op   pb.Op
		id   string
		note string
	}{{pb.Op_OP_INSERT, "1", "a"}, {pb.Op_OP_INSERT, "2", "b"}, {pb.Op_OP_UPDATE, "1", "c"}, {pb.Op_OP_DELETE, "2", ""}, {pb.Op_OP_TRUNCATE, "", ""}}
	for i, w := range want {
		g := evs[i]
		if g.op != w.op || g.id != w.id || g.note != w.note || g.table != e.table {
			t.Fatalf("event %d: got %+v, want %+v", i, g, w)
		}
	}
	// Each statement was its own transaction: seq restarts, LSNs grow.
	if evs[0].seq != 0 || evs[1].seq != 1 || evs[2].seq != 0 || !(evs[0].lsn < evs[2].lsn && evs[2].lsn < evs[3].lsn) {
		t.Fatalf("keys: %+v", evs)
	}
	if n.fsm.Source().CommitLSN != evs[4].lsn {
		t.Fatalf("source %+v", n.fsm.Source())
	}
}

func (e *pgEnv) waitSlot() {
	e.t.Helper()
	waitFor(e.t, "slot", 10*time.Second, func() bool {
		var ok bool
		e.db.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", e.slot).Scan(&ok)
		return ok
	})
}

func TestLargeTransactionIsSplitInLog(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	runSource(t, e.config(Limits{MaxEvents: 100}), n)
	e.waitSlot()
	e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'x' FROM generate_series(1, 250) g", e.table))
	waitFor(t, "250 events", 10*time.Second, func() bool { return len(n.events()) == 250 })
	for i, ev := range n.events() {
		if ev.seq != uint32(i) || ev.id != fmt.Sprint(i+1) {
			t.Fatalf("event %d: %+v", i, ev)
		}
	}
	if s := n.fsm.Source(); s.PartialLSN != 0 || s.Anomalies != 0 {
		t.Fatalf("source %+v", s)
	}
}

// The source and the node are stopped and restarted from disk many times
// while a writer runs; every row must be stored exactly once.
func TestRestartsStoreEveryRowOnce(t *testing.T) {
	e := newPGEnv(t)
	dir := t.TempDir()
	n := startNode(t, dir)
	stop := runSource(t, e.config(Limits{MaxEvents: 7}), n)
	e.waitSlot()

	const rows = 600
	writerDone := make(chan error, 1)
	go func() {
		ctx := context.Background()
		w, err := pgx.Connect(ctx, e.url)
		if err != nil {
			writerDone <- err
			return
		}
		defer w.Close(ctx)
		for i := 1; i <= rows; i += 20 {
			// 20 rows per transaction, so many batches get split.
			if _, err := w.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT g, 'r' FROM generate_series(%d, %d) g", e.table, i, i+19)); err != nil {
				writerDone <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		writerDone <- nil
	}()

	for range 6 {
		time.Sleep(150 * time.Millisecond)
		stop()
		n.stop()
		n = startNode(t, dir)
		stop = runSource(t, e.config(Limits{MaxEvents: 7}), n)
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	waitFor(t, "all rows", 20*time.Second, func() bool { return len(n.events()) >= rows })
	seen := map[string]int{}
	for _, ev := range n.events() {
		seen[ev.id]++
	}
	for i := 1; i <= rows; i++ {
		if seen[fmt.Sprint(i)] != 1 {
			t.Fatalf("row %d stored %d times", i, seen[fmt.Sprint(i)])
		}
	}
	if len(seen) != rows || n.fsm.Source().Anomalies != 0 {
		t.Fatalf("%d distinct rows, anomalies %d", len(seen), n.fsm.Source().Anomalies)
	}
}

// sessionErr runs one session directly and returns its error.
func sessionErr(t *testing.T, cfg Config, n *testNode) error {
	t.Helper()
	src, err := New(cfg, n.node, n.fsm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitFor(t, "leader", 5*time.Second, func() bool {
		st, _ := n.node.Status(ctx)
		return st.State == raft.StateLeader
	})
	return src.session(ctx)
}

func TestMissingPublicationIsAConfigError(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	cfg := e.config(Limits{})
	cfg.Publication = "does_not_exist"
	if err := sessionErr(t, cfg, n); !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), "CREATE PUBLICATION") {
		t.Fatalf("got %v", err)
	}
}

// Design D7: if the slot disappears after changes were stored, refuse to
// create a new one silently.
func TestLostSlotIsFatal(t *testing.T) {
	e := newPGEnv(t)
	dir := t.TempDir()
	n := startNode(t, dir)
	stop := runSource(t, e.config(Limits{}), n)
	e.waitSlot()
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", e.table))
	waitFor(t, "event", 10*time.Second, func() bool { return len(n.events()) == 1 })
	stop()
	waitFor(t, "slot released", 10*time.Second, func() bool {
		_, err := e.db.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", e.slot)
		return err == nil
	})
	if err := sessionErr(t, e.config(Limits{}), n); !errors.Is(err, ErrSlotLost) || !isFatal(err) {
		t.Fatalf("got %v", err)
	}
}

func TestSlotAheadIsFatal(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	stop := runSource(t, e.config(Limits{}), n)
	e.waitSlot()
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", e.table))
	waitFor(t, "event", 10*time.Second, func() bool { return len(n.events()) == 1 })
	stop()
	// Someone else consumes changes the cluster never stored.
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'b')", e.table))
	waitFor(t, "slot advanced", 10*time.Second, func() bool {
		_, err := e.db.Exec(context.Background(), "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())", e.slot)
		return err == nil
	})
	if err := sessionErr(t, e.config(Limits{}), n); !errors.Is(err, ErrSlotAhead) {
		t.Fatalf("got %v", err)
	}
}

// Design D3 fencing: a new leader takes the slot from a reader that still
// holds it.
func TestFencingTakesOverTheSlot(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	stop := runSource(t, e.config(Limits{}), n)
	e.waitSlot()
	stop()
	waitFor(t, "slot released", 10*time.Second, func() bool {
		var active bool
		e.db.QueryRow(context.Background(), "SELECT active FROM pg_replication_slots WHERE slot_name=$1", e.slot).Scan(&active)
		return !active
	})

	// An "old leader" holding the slot.
	ctx := context.Background()
	cfg, _ := pgconn.ParseConfig(e.url)
	cfg.RuntimeParams["replication"] = "database"
	waitFor(t, "old reader holds the slot", 10*time.Second, func() bool {
		old, err := pgconn.ConnectConfig(ctx, cfg)
		if err != nil {
			return false
		}
		err = pglogrepl.StartReplication(ctx, old, e.slot, 0, pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication, PluginArgs: []string{"proto_version '1'", "publication_names '" + e.pub + "'"}})
		if err != nil {
			t.Logf("old reader: %v", err)
			old.Close(ctx)
			return false
		}
		t.Cleanup(func() { old.Close(ctx) })
		return true
	})

	runSource(t, e.config(Limits{}), n)
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", e.table))
	waitFor(t, "event after fencing", 15*time.Second, func() bool { return len(n.events()) == 1 })
}

// Transactions on unpublished tables must still let Postgres free WAL.
func TestEmptyTransactionsAdvanceTheSlot(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	runSource(t, e.config(Limits{}), n)
	e.waitSlot()
	other := e.table + "_unpublished"
	e.exec(fmt.Sprintf("CREATE TABLE %s (id int)", other))
	t.Cleanup(func() { e.db.Exec(context.Background(), "DROP TABLE IF EXISTS "+other) })
	var before string
	e.db.QueryRow(context.Background(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", e.slot).Scan(&before)
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1)", other))
	var target string
	e.db.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&target)
	tl, _ := pglogrepl.ParseLSN(target)
	waitFor(t, "slot to move past the unpublished change", 15*time.Second, func() bool {
		var cur string
		e.db.QueryRow(context.Background(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", e.slot).Scan(&cur)
		l, _ := pglogrepl.ParseLSN(cur)
		return l >= tl
	})
	if len(n.events()) != 0 {
		t.Fatalf("unpublished change stored: %v", n.events())
	}
	if n.fsm.Source().EndLSN == 0 {
		t.Fatal("source position did not advance")
	}
}
