package pg

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Failover tests (design D7) need a Postgres 17 primary and a streaming
// standby with sync_replication_slots; `make test-failover` creates a fresh
// pair for each test, since a promotion cannot be undone. The source reaches
// Postgres through a proxy that plays the virtual IP: it is moved to the
// standby after its promotion.

// proxy forwards TCP connections to a target that can be changed; changing
// it closes the open connections, as a moved virtual IP would.
type proxy struct {
	ln     net.Listener
	mu     sync.Mutex
	target string
	conns  []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close(); p.moveTo("") })
	return p
}

func (p *proxy) serve(c net.Conn) {
	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	if target == "" {
		c.Close()
		return
	}
	up, err := net.Dial("tcp", target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, c, up)
	p.mu.Unlock()
	go func() { io.Copy(up, c); up.Close() }()
	io.Copy(c, up)
	c.Close()
}

func (p *proxy) moveTo(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

type foEnv struct {
	t                *testing.T
	primary, standby *pgx.Conn
	standbyAddr      string
	proxy            *proxy
	cfg              Config
	table            string
}

func newFailoverEnv(t *testing.T) *foEnv {
	t.Helper()
	pURL, sURL := os.Getenv("CURNTVALV_TEST_PG_PRIMARY_URL"), os.Getenv("CURNTVALV_TEST_PG_STANDBY_URL")
	if pURL == "" || sURL == "" {
		t.Skip("CURNTVALV_TEST_PG_PRIMARY_URL / _STANDBY_URL not set; run `make test-failover`")
	}
	ctx := context.Background()
	connect := func(u string) *pgx.Conn {
		c, err := pgx.Connect(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		return c
	}
	pu, _ := url.Parse(pURL)
	su, _ := url.Parse(sURL)
	e := &foEnv{t: t, primary: connect(pURL), standby: connect(sURL), standbyAddr: su.Host, table: "orders"}
	e.proxy = newProxy(t, pu.Host)
	viaProxy := *pu
	viaProxy.Host = e.proxy.ln.Addr().String()
	e.cfg = Config{URL: viaProxy.String(), Slot: "curntvalv", Publication: "curntvalv", StatusInterval: 200 * time.Millisecond}
	e.exec(e.primary, "CREATE TABLE orders (id int PRIMARY KEY, note text)")
	e.exec(e.primary, "CREATE PUBLICATION curntvalv FOR TABLE orders")
	return e
}

func (e *foEnv) exec(c *pgx.Conn, sql string, args ...any) {
	e.t.Helper()
	if _, err := c.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

func (e *foEnv) insert(c *pgx.Conn, from, to int) {
	e.t.Helper()
	for i := from; i <= to; i++ {
		e.exec(c, fmt.Sprintf("INSERT INTO orders VALUES (%d, 'row')", i))
	}
}

// synchronizeStandby makes the primary hold logical changes back until the
// standby has them (synchronized_standby_slots).
func (e *foEnv) synchronizeStandby() {
	e.exec(e.primary, "ALTER SYSTEM SET synchronized_standby_slots = 'standby1'")
	e.exec(e.primary, "SELECT pg_reload_conf()")
}

// waitSyncedSlot waits until the standby holds a persistent copy of the
// slot, at least at the primary's current WAL position.
func (e *foEnv) waitSyncedSlot() {
	e.t.Helper()
	ctx := context.Background()
	var target string
	e.primary.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&target)
	waitFor(e.t, "slot synced to the standby", 90*time.Second, func() bool {
		var ok bool
		e.standby.QueryRow(ctx, `SELECT synced AND NOT temporary AND pg_last_wal_replay_lsn() >= $2::pg_lsn
			FROM pg_replication_slots WHERE slot_name = $1`, e.cfg.Slot, target).Scan(&ok)
		return ok
	})
}

// cutStandby stops the standby's replication, so the primary's next WAL
// never reaches it.
func (e *foEnv) cutStandby() {
	e.t.Helper()
	e.exec(e.standby, "ALTER SYSTEM SET primary_conninfo = ''")
	e.exec(e.standby, "SELECT pg_reload_conf()")
	waitFor(e.t, "standby disconnected", 30*time.Second, func() bool {
		var n int
		e.standby.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_wal_receiver").Scan(&n)
		return n == 0
	})
}

// failover promotes the standby and moves the proxy to it.
func (e *foEnv) failover() {
	e.t.Helper()
	var ok bool
	if err := e.standby.QueryRow(context.Background(), "SELECT pg_promote(true, 60)").Scan(&ok); err != nil || !ok {
		e.t.Fatalf("promote: %v %v", ok, err)
	}
	e.proxy.moveTo(e.standbyAddr)
}

// rows counts the stored insert events per row id and the resets.
func rows(n *testNode) (map[string]int, int) {
	got, resets := map[string]int{}, 0
	for _, ev := range n.events() {
		switch ev.op {
		case pb.Op_OP_INSERT:
			got[ev.id]++
		case pb.Op_OP_SOURCE_RESET:
			resets++
		}
	}
	return got, resets
}

func exactly(t *testing.T, got map[string]int, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		if c := got[fmt.Sprint(i)]; c != 1 {
			t.Errorf("row %d stored %d times", i, c)
		}
	}
}

// With synchronized_standby_slots the source never reads past the standby,
// so after a failover it continues on the new timeline: every row once.
func TestFailoverSafe(t *testing.T) {
	e := newFailoverEnv(t)
	e.synchronizeStandby()
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.cfg, n)
	waitFor(t, "ingest", 30*time.Second, func() bool { h, _ := src.Health(); return h == "running" })
	e.insert(e.primary, 1, 20)
	waitFor(t, "rows 1-20", 30*time.Second, func() bool { got, _ := rows(n); return len(got) == 20 })
	e.waitSyncedSlot()
	e.insert(e.primary, 21, 30) // read or not, the standby has them first
	time.Sleep(500 * time.Millisecond)

	e.failover()
	e.insert(e.standby, 31, 40)
	waitFor(t, "rows 1-40", 60*time.Second, func() bool { got, _ := rows(n); return len(got) == 40 })
	time.Sleep(time.Second) // a duplicate would show up now
	got, resets := rows(n)
	exactly(t, got, 1, 40)
	if h, msg := src.Health(); h != "running" || resets != 0 {
		t.Fatalf("health %s %q, resets %d", h, msg, resets)
	}
	if st := n.fsm.Source(); st.Timeline != 2 {
		t.Fatalf("source %+v, want timeline 2", st)
	}
}

// Without it the source can read changes the standby never got. After the
// failover ingest stops with ErrDiverged instead of skipping the new
// primary's transactions; a reset continues from the slot.
func TestFailoverDiverged(t *testing.T) {
	e := newFailoverEnv(t)
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.cfg, n)
	waitFor(t, "ingest", 30*time.Second, func() bool { h, _ := src.Health(); return h == "running" })
	e.insert(e.primary, 1, 10)
	waitFor(t, "rows 1-10", 30*time.Second, func() bool { got, _ := rows(n); return len(got) == 10 })
	e.waitSyncedSlot()
	e.cutStandby()
	e.insert(e.primary, 11, 20) // stored, but lost with the old primary
	waitFor(t, "rows 11-20", 30*time.Second, func() bool { got, _ := rows(n); return len(got) == 20 })

	e.failover()
	waitFor(t, "ingest stopped", 60*time.Second, func() bool { h, _ := src.Health(); return h == "failed" })
	if _, msg := src.Health(); !strings.Contains(msg, "lost in the failover") {
		t.Fatalf("health error %q", msg)
	}
	// The new primary's transactions are not skipped: nothing is stored
	// until an operator decides.
	e.insert(e.standby, 21, 30)
	time.Sleep(time.Second)
	if got, _ := rows(n); len(got) != 20 {
		t.Fatalf("stored while stopped: %v", got)
	}

	_, r, err := src.Reset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.GetTimeline() != 2 || r.GetPreviousTimeline() != 1 || r.GetLsn() >= r.GetPreviousLsn() {
		t.Fatalf("reset %v", r)
	}
	waitFor(t, "rows 21-30 after the reset", 60*time.Second, func() bool {
		got, _ := rows(n)
		for i := 21; i <= 30; i++ {
			if got[fmt.Sprint(i)] == 0 {
				return false
			}
		}
		return true
	})
	got, resets := rows(n)
	exactly(t, got, 21, 30)
	if resets != 1 {
		t.Fatalf("%d resets", resets)
	}
}

// With synchronized_standby_slots and the standby gone, the primary holds
// changes back from the source; after the failover nothing diverged.
func TestFailoverHeldBack(t *testing.T) {
	e := newFailoverEnv(t)
	e.synchronizeStandby()
	n := startNode(t, t.TempDir())
	src, _ := runSourceGet(t, e.cfg, n)
	waitFor(t, "ingest", 30*time.Second, func() bool { h, _ := src.Health(); return h == "running" })
	e.insert(e.primary, 1, 10)
	waitFor(t, "rows 1-10", 30*time.Second, func() bool { got, _ := rows(n); return len(got) == 10 })
	e.waitSyncedSlot()
	e.cutStandby()
	e.insert(e.primary, 11, 20) // never reaches the standby, so not read
	time.Sleep(2 * time.Second)
	if got, _ := rows(n); len(got) != 10 {
		t.Fatalf("read past the standby: %d rows", len(got))
	}

	e.failover()
	e.insert(e.standby, 21, 30)
	waitFor(t, "rows 21-30", 60*time.Second, func() bool { got, _ := rows(n); return len(got) == 20 })
	time.Sleep(time.Second)
	got, resets := rows(n)
	exactly(t, got, 1, 10)
	exactly(t, got, 21, 30)
	if h, msg := src.Health(); h != "running" || resets != 0 || got["11"] != 0 {
		t.Fatalf("health %s %q, resets %d, rows %v", h, msg, resets, got)
	}
}
