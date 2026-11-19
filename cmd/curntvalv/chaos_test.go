package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// slotReady is true when slot $1 exists and no walsender is still creating
// it.
const slotReady = `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1) AND NOT EXISTS (
	SELECT 1 FROM pg_stat_activity WHERE backend_type = 'walsender' AND state = 'active'
	AND query LIKE 'CREATE_REPLICATION_SLOT ' || $1 || ' %')`

// dropSlot removes a test slot. A walsender of a stopped node may still hold
// it, or still be creating it (it does not notice that its client is gone
// while it waits for a consistent point), so those are terminated too.
func dropSlot(ctx context.Context, db *pgx.Conn, slot string) {
	for range 50 {
		db.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'walsender'
			AND (query LIKE 'CREATE_REPLICATION_SLOT ' || $1 || ' %' OR query LIKE 'START_REPLICATION SLOT ' || $1 || ' %')`, slot)
		db.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
		var left bool
		if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1) OR EXISTS (
			SELECT 1 FROM pg_stat_activity WHERE backend_type = 'walsender' AND query LIKE 'CREATE_REPLICATION_SLOT ' || $1 || ' %')`,
			slot).Scan(&left); err == nil && !left {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// pgFixture is a table with a publication and the name of a slot (created by
// the cluster) in the test Postgres, all removed at the end of the test.
type pgFixture struct {
	t                *testing.T
	url              string
	db               *pgx.Conn
	table, slot, pub string
}

func newPGFixture(t *testing.T) *pgFixture {
	t.Helper()
	pgURL := os.Getenv("CURNTVALV_TEST_PG_URL")
	if pgURL == "" {
		t.Skip("CURNTVALV_TEST_PG_URL not set; run `make test-pg`")
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	n := fmt.Sprintf("e2e_%d", time.Now().UnixNano()%1_000_000_000)
	f := &pgFixture{t: t, url: pgURL, db: db, table: "rows_" + n, slot: "slot_" + n, pub: "pub_" + n}
	f.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", f.table))
	f.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", f.pub, f.table))
	t.Cleanup(func() {
		// Runs after the cluster's cleanup (registered later, runs first).
		dropSlot(ctx, db, f.slot)
		db.Exec(ctx, "DROP PUBLICATION "+f.pub)
		db.Exec(ctx, "DROP TABLE "+f.table)
		db.Close(ctx)
	})
	return f
}

func (f *pgFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.db.Exec(context.Background(), sql, args...); err != nil {
		f.t.Fatalf("%s: %v", sql, err)
	}
}

// args are the server flags that capture the fixture's table.
func (f *pgFixture) args() []string {
	return []string{"--pg-slot", f.slot, "--pg-publication", f.pub}
}

func (f *pgFixture) env() []string { return []string{"CURNTVALV_PG_URL=" + f.url} }

func (f *pgFixture) waitSlot() {
	f.t.Helper()
	waitE2E(f.t, "slot", 30*time.Second, func() bool {
		var ok bool
		// A logical slot is listed while it is still being created.
		f.db.QueryRow(context.Background(), slotReady, f.slot).Scan(&ok)
		return ok
	})
}

// insert adds rows from..to in transactions of size rows.
func (f *pgFixture) insert(from, to, size int) {
	f.t.Helper()
	for next := from; next <= to; next += size {
		f.exec(fmt.Sprintf("INSERT INTO %s SELECT generate_series(%d, %d)", f.table, next, min(next+size-1, to)))
	}
}

// rowRange checks that a consumer holds exactly the rows lo..hi for some
// lo >= minLo, one event id per row, and returns lo.
func rowRange(t *testing.T, name string, ids map[string]string, minLo, hi int) int {
	t.Helper()
	rows := map[int]bool{}
	for id, r := range ids {
		n, err := strconv.Atoi(r)
		if err != nil {
			t.Fatalf("%s: event %s has row %q", name, id, r)
		}
		rows[n] = true
	}
	if len(rows) != len(ids) {
		t.Fatalf("%s: %d event ids for %d distinct rows", name, len(ids), len(rows))
	}
	lo := hi + 1
	for r := range rows {
		lo = min(lo, r)
	}
	for r := lo; r <= hi; r++ {
		if !rows[r] {
			t.Fatalf("%s: has rows %d..%d but not row %d", name, lo, hi, r)
		}
	}
	if len(rows) != hi-lo+1 || lo < minLo {
		t.Fatalf("%s: %d rows from %d, want exactly rows %d..%d with the start at or after %d", name, len(rows), lo, lo, hi, minLo)
	}
	return lo
}

// waitRows waits until a consumer has row hi and every row between its
// first row and hi. It fails when the consumer makes no progress for a
// minute, not after a fixed time: a backlog may take a while.
func waitRows(t *testing.T, name string, get func() (map[string]string, int), hi int) map[string]string {
	t.Helper()
	var ids map[string]string
	seen, stalled := -1, time.Now()
	waitE2E(t, fmt.Sprintf("%s to reach row %d", name, hi), time.Hour, func() bool {
		ids, _ = get()
		if len(ids) > seen {
			seen, stalled = len(ids), time.Now()
		} else if time.Since(stalled) > time.Minute {
			t.Fatalf("%s: stuck at %d events, waiting for row %d", name, len(ids), hi)
		}
		rows := map[string]bool{}
		for _, r := range ids {
			rows[r] = true
		}
		if !rows[fmt.Sprint(hi)] {
			return false
		}
		lo := hi
		for lo > 1 && rows[fmt.Sprint(lo-1)] {
			lo--
		}
		return len(rows) == hi-lo+1
	})
	return ids
}

// nodeRow is one line of the node table of `curntvalv status`.
type nodeRow struct {
	addr           string
	state          string
	first, applied uint64
}

func parseNodes(status string) []nodeRow {
	var out []nodeRow
	for _, line := range strings.Split(status, "\n")[1:] {
		f := strings.Fields(line)
		if len(f) < 10 {
			break
		}
		r := nodeRow{addr: f[0], state: f[2]}
		r.first, _ = strconv.ParseUint(f[5], 10, 64)
		r.applied, _ = strconv.ParseUint(f[8], 10, 64)
		out = append(out, r)
	}
	return out
}

// sinkLine returns the fields of a sink's line in `curntvalv status`:
// SINK TYPE POLICY STATE OFFSET LAG DELIVERED ERROR...
func sinkLine(status, name string) []string {
	for _, line := range strings.Split(status, "\n") {
		if f := strings.Fields(line); len(f) >= 8 && f[0] == name && (f[1] == "webhook" || f[1] == "clickhouse") {
			return f
		}
	}
	return nil
}

func leaderFirst(status string) uint64 {
	for _, n := range parseNodes(status) {
		if n.state == "leader" {
			return n.first
		}
	}
	return 0
}

// TestEndToEndRetentionAndReplay covers design D5 and D12 with a real
// cluster and Postgres: the log is cut by size, a block sink holds the cut
// back, a drop sink that falls behind fails and is recovered with a reset,
// a sink added later gets the retained history, and a reset replays it.
func TestEndToEndRetentionAndReplay(t *testing.T) {
	f := newPGFixture(t)
	a := newHookReceiverWith(t, 0) // block (the default)
	b := newHookReceiverWith(t, 0) // drop
	b.setDown(true)
	hookArg := func(name string, h *hookReceiver, extra string) []string {
		return []string{"--sink", "name=" + name + ",type=webhook,url=" + h.url + ",table=" + f.table + extra}
	}
	args := append(f.args(), "--snapshot-entries", "10", "--retention-bytes", "3KB", "--wal-segment-bytes", "1KB")
	args = append(args, hookArg("a", a, "")...)
	args = append(args, hookArg("b", b, ",policy=drop")...)
	c := newE2EWith(t, f.env(), args...)
	f.waitSlot()

	// 1. History is cut while a keeps up; b (down, drop) falls behind.
	f.insert(1, 300, 5)
	waitRows(t, "a", a.ids, 300)
	waitE2E(t, "log compaction", 30*time.Second, func() bool { return leaderFirst(c.mustCLI("status")) > 1 })
	b.setDown(false)
	waitE2E(t, "sink b to fail", 60*time.Second, func() bool {
		f := sinkLine(c.mustCLI("status"), "b")
		return f != nil && f[3] == "failed" && strings.Contains(strings.Join(f[7:], " "), "compaction")
	})
	rowRange(t, "a", func() map[string]string { m, _ := a.ids(); return m }(), 1, 300)

	// 2. A reset to the oldest retained entry recovers b, without the lost
	// history.
	b.forget()
	if out := c.mustCLI("sink", "reset", "b", "--to", "earliest"); !strings.Contains(out, "generation 1") {
		t.Fatalf("reset: %s", out)
	}
	bIDs := waitRows(t, "b", b.ids, 300)
	if lo := rowRange(t, "b", bIDs, 2, 300); lo == 1 {
		t.Fatal("b got the whole history although it was compacted")
	}

	// 3. A sink added later (rolling restart) gets the retained history and
	// everything written during the restart.
	late := newHookReceiverWith(t, 0)
	c.args = append(c.args, "--sink", "name=late,type=webhook,url="+late.url+",table="+f.table)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 301; i <= 400; i += 5 {
			if _, err := f.db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s SELECT generate_series(%d, %d)", f.table, i, i+4)); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	for i := 1; i <= 3; i++ {
		c.stop(i)
		c.start(i)
		c.leader()
		time.Sleep(300 * time.Millisecond)
	}
	<-done
	lateIDs := waitRows(t, "late", late.ids, 400)
	lateLo := rowRange(t, "late", lateIDs, 2, 400)
	rowRange(t, "a", waitRows(t, "a", a.ids, 400), 1, 400)
	rowRange(t, "b", waitRows(t, "b", b.ids, 400), 2, 400)

	// 4. Replay: a reset to earliest delivers the retained history again.
	a.forget()
	c.mustCLI("sink", "reset", "a", "--to", "earliest")
	if lo := rowRange(t, "a replay", waitRows(t, "a replay", a.ids, 400), 2, 400); lo < lateLo {
		t.Fatalf("replay starts at row %d, before the oldest row (%d) the late sink could get", lo, lateLo)
	}

	// 5. Block: while a is down the log is not cut past a's offset.
	a.setDown(true)
	f.insert(401, 600, 5)
	waitRows(t, "late", late.ids, 600)
	var aOffset uint64
	waitE2E(t, "retention held back by a", 30*time.Second, func() bool {
		st := c.mustCLI("status")
		if fl := sinkLine(st, "a"); fl != nil {
			aOffset, _ = strconv.ParseUint(fl[4], 10, 64)
		}
		return strings.Contains(st, "held back by sink a")
	})
	time.Sleep(time.Second) // the periodic compaction check runs meanwhile
	st := c.mustCLI("status")
	for _, n := range parseNodes(st) {
		if n.first > aOffset+1 {
			t.Fatalf("node %s cut its log at %d past the offset %d of block sink a:\n%s", n.addr, n.first, aOffset, st)
		}
	}
	a.setDown(false)
	waitRows(t, "a", a.ids, 600)
	waitE2E(t, "compaction past a's old offset", 60*time.Second, func() bool { return leaderFirst(c.mustCLI("status")) > aOffset+1 })
	// b was failed, so only a's running session had to notice its reset.
	if n := c.logCount("sink reset; restarting"); n < 1 {
		t.Fatalf("runners noticed %d resets", n)
	}
}

// TestChaos is the end-to-end checker of design §5 under random faults. It
// runs for CURNTVALV_CHAOS (e.g. 2m; skipped if unset) with Postgres
// (CURNTVALV_TEST_PG_URL): a writer inserts numbered rows while nodes are
// killed, paused (SIGSTOP, a stand-in for a partition), restarted together,
// the replication connection is cut and the webhook goes down. Retention is
// small, so the log is cut and followers install snapshots meanwhile. At the
// end every consumer must hold exactly rows 1..N after de-duplication, and
// every node the same log. CURNTVALV_CHAOS_SEED repeats a run.
func TestChaos(t *testing.T) {
	dur, err := time.ParseDuration(os.Getenv("CURNTVALV_CHAOS"))
	if err != nil || dur <= 0 {
		t.Skip("CURNTVALV_CHAOS not set (e.g. 2m); run `make chaos`")
	}
	f := newPGFixture(t)
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("CURNTVALV_CHAOS_SEED"); s != "" {
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("chaos seed %d (CURNTVALV_CHAOS_SEED=%d repeats the fault schedule)", seed, seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	hook := newHookReceiver(t) // also fails every fifth request
	args := append(f.args(), "--snapshot-entries", "50", "--retention-bytes", "64KB", "--wal-segment-bytes", "16KB",
		"--sink", "name=hook,type=webhook,url="+hook.url+",table="+f.table)
	c := newE2EWith(t, f.env(), args...)
	sub := c.subscribe("--table", f.table)
	f.waitSlot()

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	type writeResult struct{ rows, txs int }
	written := make(chan writeResult, 1)
	go func() {
		w, err := pgx.Connect(context.Background(), f.url)
		if err != nil {
			t.Error(err)
			written <- writeResult{}
			return
		}
		defer w.Close(context.Background())
		wr := rand.New(rand.NewPCG(seed+1, seed+1))
		last, txs := 0, 0
		// As fast as one connection can commit: ingest must keep up, apart
		// from the backlog that builds up during faults (design D11).
		for ctx.Err() == nil {
			size := 1 + wr.IntN(40)
			if _, err := w.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s SELECT generate_series(%d, %d)",
				f.table, last+1, last+size)); err != nil {
				t.Error(err)
				break
			}
			last += size
			txs++
		}
		written <- writeResult{last, txs}
	}()

	faults := map[string]int{}
	pick := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.Int64N(int64(hi-lo))) }
	sleep := func(d time.Duration) {
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
	for ctx.Err() == nil {
		sleep(pick(time.Second, 3*time.Second))
		if ctx.Err() != nil {
			break
		}
		switch k := rng.IntN(100); {
		case k < 25:
			faults["kill leader"]++
			l := c.leader()
			c.kill(l)
			sleep(pick(100*time.Millisecond, 2*time.Second))
			c.start(l)
		case k < 45:
			faults["kill random node"]++
			i := 1 + rng.IntN(3)
			c.kill(i)
			sleep(pick(100*time.Millisecond, 3*time.Second))
			c.start(i)
		case k < 65:
			faults["pause leader"]++
			l := c.leader()
			p := c.procs[l].cmd.Process
			p.Signal(syscall.SIGSTOP)
			sleep(pick(500*time.Millisecond, 3*time.Second))
			p.Signal(syscall.SIGCONT)
		case k < 80:
			faults["cut replication"]++
			f.db.Exec(context.Background(),
				"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active_pid IS NOT NULL", f.slot)
		case k < 93:
			faults["webhook down"]++
			hook.setDown(true)
			sleep(pick(time.Second, 5*time.Second))
			hook.setDown(false)
		default:
			faults["restart all"]++
			for i := 1; i <= 3; i++ {
				c.kill(i)
			}
			sleep(pick(100*time.Millisecond, time.Second))
			for i := 1; i <= 3; i++ {
				c.start(i)
			}
		}
	}
	hook.setDown(false)
	wres := <-written
	rows := wres.rows
	stopped := time.Now()
	t.Logf("faults: %v; %d rows in %d transactions written (%.0f transactions/s)",
		faults, rows, wres.txs, float64(wres.txs)/dur.Seconds())
	if rows == 0 || t.Failed() {
		t.Fatal("writer failed")
	}

	for name, get := range map[string]func() (map[string]string, int){"webhook": hook.ids, "subscriber": sub.ids} {
		ids := waitRows(t, name, get, rows)
		rowRange(t, name, ids, 1, rows)
		_, n := get()
		t.Logf("%s: %d deliveries for %d rows, complete %s after the writer stopped",
			name, n, rows, time.Since(stopped).Round(time.Second))
	}

	// Every node holds the same log. Compaction can move while the logs are
	// read, so a failed attempt is repeated.
	var st, why string
	var from uint64
	var events int
	compacted := false
	defer func() {
		if t.Failed() {
			t.Log("last attempt:", why)
		}
	}()
	waitE2E(t, "nodes to hold the same log", 60*time.Second, func() bool {
		st = c.mustCLI("status")
		nodes := parseNodes(st)
		why = "a node is unreachable or behind"
		if len(nodes) != 3 {
			return false
		}
		from = 0
		for _, n := range nodes {
			if n.state == "unreachable" || n.applied != nodes[0].applied {
				return false
			}
			from = max(from, n.first)
			compacted = compacted || n.first > 1
		}
		var logs []string
		for _, a := range c.addrs {
			out, err := exec.Command(c.bin, "log", "--addr", a, "--from", fmt.Sprint(from)).Output()
			if err != nil {
				why = fmt.Sprintf("log from %s: %v", a, err)
				return false
			}
			logs = append(logs, string(out))
		}
		why = fmt.Sprintf("nodes hold different logs from index %d", from)
		events = strings.Count(logs[0], "\n")
		return logs[0] != "" && slices.Equal(logs, []string{logs[0], logs[0], logs[0]})
	})
	if !compacted {
		t.Fatalf("no node compacted its log:\n%s", st)
	}
	t.Logf("log from index %d identical on all nodes (%d events)", from, events)
}
