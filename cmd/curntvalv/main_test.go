package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func TestParsePeers(t *testing.T) {
	p, err := parsePeers(" 1=a:1, 2=b:2,,3=c:3 ")
	if err != nil || len(p) != 3 || p[2] != "b:2" {
		t.Fatalf("%v %v", p, err)
	}
	for _, bad := range []string{"", "1", "x=a:1", "0=a:1", "1=", "1=a:1,1=b:2"} {
		if _, err := parsePeers(bad); !errors.Is(err, errUsage) {
			t.Errorf("%q: got %v", bad, err)
		}
	}
}

func TestParseServerFlags(t *testing.T) {
	cfg, err := parseServerFlags([]string{"--id", "2", "--peers", "1=a:1,2=b:2,3=c:3", "--data-dir", "/d", "--trailing-entries", "50"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.raft.ID != 2 || fmt.Sprint(cfg.raft.Peers) != "[1 2 3]" || cfg.listen != "b:2" || cfg.raft.TrailingEntries != 50 {
		t.Fatalf("%+v", cfg)
	}
	for _, args := range [][]string{
		{"--peers", "1=a:1", "--data-dir", "/d"},              // no id
		{"--id", "5", "--peers", "1=a:1", "--data-dir", "/d"}, // id not in peers
		{"--id", "1", "--peers", "1=a:1"},                     // no data dir
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "--log-level", "loud"},
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "extra"},
	} {
		if _, err := parseServerFlags(args, io.Discard); !errors.Is(err, errUsage) {
			t.Errorf("%v: got %v", args, err)
		}
	}
	if _, err := parseServerFlags([]string{"-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: %v", err)
	}
}

func TestParseClientFlagsAfterArgs(t *testing.T) {
	cf, pos, err := parseClientFlags("kv", []string{"get", "key", "--addr", "x:1,y:2", "--timeout", "1s"}, io.Discard)
	if err != nil || fmt.Sprint(pos) != "[get key]" || fmt.Sprint(cf.addrs) != "[x:1 y:2]" || cf.timeout != time.Second {
		t.Fatalf("%+v %v %v", cf, pos, err)
	}
}

func TestRunUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), nil, &out, &errOut); code != 2 {
		t.Fatalf("no args: code %d", code)
	}
	if code := run(context.Background(), []string{"bogus"}, &out, &errOut); code != 2 {
		t.Fatalf("bogus: code %d", code)
	}
	out.Reset()
	if code := run(context.Background(), []string{"version"}, &out, &errOut); code != 0 || !strings.HasPrefix(out.String(), "curntvalv ") {
		t.Fatalf("version: %d %q", code, out.String())
	}
}

// --- end-to-end: real processes ------------------------------------------------

type proc struct {
	cmd  *exec.Cmd
	logs *syncBuffer
}

type e2eCluster struct {
	t     *testing.T
	bin   string
	dir   string
	addrs []string
	peers string
	procs map[int]*proc
	args  []string            // server flags besides id, peers, data dir
	env   []string            // extra environment for servers
	logs  map[int]*syncBuffer // kept across restarts
}

// syncBuffer is a bytes.Buffer that can be read while a process writes it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// logCount counts the lines of all node logs that contain text.
func (c *e2eCluster) logCount(text string) int {
	n := 0
	for _, l := range c.logs {
		n += strings.Count(l.String(), text)
	}
	return n
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newE2E(t *testing.T) *e2eCluster {
	return newE2EWith(t, nil, "--snapshot-entries", "10", "--trailing-entries", "15")
}

func newE2EWith(t *testing.T, env []string, args ...string) *e2eCluster {
	if testing.Short() {
		t.Skip("end-to-end test skipped in -short mode")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "curntvalv")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	c := &e2eCluster{t: t, bin: bin, dir: dir, procs: map[int]*proc{}, args: args, env: env, logs: map[int]*syncBuffer{}}
	var peers []string
	for i := 1; i <= 3; i++ {
		addr := freePort(t)
		c.addrs = append(c.addrs, addr)
		peers = append(peers, fmt.Sprintf("%d=%s", i, addr))
	}
	c.peers = strings.Join(peers, ",")
	for i := 1; i <= 3; i++ {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			if p != nil {
				p.cmd.Process.Signal(os.Interrupt)
				p.cmd.Wait()
			}
		}
		if t.Failed() {
			for i, l := range c.logs {
				t.Logf("node %d log:\n%s", i, l)
			}
		}
	})
	return c
}

func (c *e2eCluster) start(i int) {
	c.t.Helper()
	logs := c.logs[i]
	if logs == nil {
		logs = &syncBuffer{}
		c.logs[i] = logs
	}
	fmt.Fprintf(logs, "--- start ---\n")
	args := []string{"server", "--id", fmt.Sprint(i), "--peers", c.peers,
		"--data-dir", filepath.Join(c.dir, fmt.Sprintf("node%d", i)), "--tick", "20ms", "--log-level", "debug"}
	cmd := exec.Command(c.bin, append(args, c.args...)...)
	cmd.Env = append(os.Environ(), c.env...)
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.procs[i] = &proc{cmd: cmd, logs: logs}
}

// kill stops node i with SIGKILL: no clean shutdown.
func (c *e2eCluster) kill(i int) {
	p := c.procs[i]
	p.cmd.Process.Kill()
	p.cmd.Wait()
	c.procs[i] = nil
}

// cli runs a client command and returns stdout.
func (c *e2eCluster) cli(args ...string) (string, error) {
	args = append(args, "--addr", strings.Join(c.addrs, ","), "--timeout", "15s")
	out, err := exec.Command(c.bin, args...).Output()
	if ee, ok := err.(*exec.ExitError); ok {
		err = fmt.Errorf("%v: %s", err, ee.Stderr)
	}
	return string(out), err
}

func (c *e2eCluster) mustCLI(args ...string) string {
	c.t.Helper()
	out, err := c.cli(args...)
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	return out
}

// leader returns the 1-based index of the node that reports itself leader.
func (c *e2eCluster) leader() int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := c.cli("status")
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) > 2 && f[2] == "leader" {
				for i, a := range c.addrs {
					if a == f[0] {
						return i + 1
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

func TestEndToEndKillLeader(t *testing.T) {
	c := newE2E(t)
	for i := range 40 {
		c.mustCLI("kv", "put", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	if got := strings.TrimSpace(c.mustCLI("kv", "get", "k7")); got != "v7" {
		t.Fatalf("get k7 = %q", got)
	}

	// Kill the leader hard. The other two elect a new one and keep serving.
	old := c.leader()
	c.kill(old)
	c.mustCLI("kv", "put", "after", "kill")
	for i := 40; i < 60; i++ {
		c.mustCLI("kv", "put", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	if got := strings.TrimSpace(c.mustCLI("kv", "get", "k0")); got != "v0" {
		t.Fatalf("get k0 after failover = %q", got)
	}
	c.mustCLI("kv", "del", "k1")
	if _, err := c.cli("kv", "get", "k1"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("get deleted key: %v", err)
	}

	// Restart the killed node: it recovers from its disk and catches up.
	c.start(old)
	addr := c.addrs[old-1]
	deadline := time.Now().Add(15 * time.Second)
	for {
		out, _ := exec.Command(c.bin, "status", "--addr", addr).Output()
		f := strings.Fields(strings.Split(string(out), "\n")[1] + " ")
		leaderOut := c.mustCLI("status")
		if len(f) >= 9 && f[2] != "unreachable" && sameApplied(leaderOut, f[8]) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up:\n%s\n%s", out, leaderOut)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// sameApplied reports whether every reachable node in a status table has
// applied exactly want.
func sameApplied(table, want string) bool {
	for _, line := range strings.Split(table, "\n")[1:] {
		f := strings.Fields(line)
		if len(f) < 9 || f[2] == "unreachable" {
			continue
		}
		if f[8] != want {
			return false
		}
	}
	return true
}

func TestEventJSON(t *testing.T) {
	sc := &pb.Schema{Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}, {Name: "note"}, {Name: "blob"}}}
	tb := &pb.TxBatch{CommitLsn: 0x1_0000_00A0, Xid: 7, FirstSeq: 3, CommitTimeUs: 1_700_000_000_000_000}
	ev := &pb.Event{Op: pb.Op_OP_UPDATE,
		Before: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")}}},
		After: &pb.Row{Values: []*pb.Value{
			{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")},
			{Kind: pb.ValueKind_VALUE_KIND_NULL},
			{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED},
		}}}
	b, err := json.Marshal(eventJSON(42, tb, 1, ev, sc))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"index":42,"lsn":"1/A0","seq":4,"xid":7,"commit_time":"2023-11-14T22:13:20Z","table":"public.orders","op":"update",` +
		`"before":{"id":"1"},"after":{"id":"1","note":null},"unchanged":["blob"]}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}

// TestEndToEndPostgresNoLossNoDuplicates is the M4 checker (design §5): a
// writer inserts numbered rows in transactions of varying size while the
// leader is killed with SIGKILL several times. Afterwards the log must hold
// every row exactly once, with every event key unique and transactions in
// commit order.
func TestEndToEndPostgresNoLossNoDuplicates(t *testing.T) {
	pgURL := os.Getenv("CURNTVALV_TEST_PG_URL")
	if pgURL == "" {
		t.Skip("CURNTVALV_TEST_PG_URL not set; run `make test-pg`")
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	n := fmt.Sprintf("e2e_%d", time.Now().UnixNano()%1_000_000_000)
	table, slot := "rows_"+n, "slot_"+n
	mustExec := func(sql string) {
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	mustExec(fmt.Sprintf("CREATE PUBLICATION pub_%s FOR TABLE %s", n, table))
	t.Cleanup(func() {
		// Runs after the cluster's cleanup (registered later, runs first).
		db.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
		db.Exec(ctx, "DROP PUBLICATION pub_"+n)
		db.Exec(ctx, "DROP TABLE "+table)
	})

	c := newE2EWith(t, []string{"CURNTVALV_PG_URL=" + pgURL},
		"--pg-slot", slot, "--pg-publication", "pub_"+n, "--snapshot-entries", "50")

	// Wait for the slot so the first rows are captured.
	waitE2E(t, "slot", 30*time.Second, func() bool {
		var ok bool
		db.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot).Scan(&ok)
		return ok
	})

	const rows = 3000
	writer := make(chan error, 1)
	go func() {
		w, err := pgx.Connect(ctx, pgURL)
		if err != nil {
			writer <- err
			return
		}
		defer w.Close(ctx)
		for next, size := 1, 1; next <= rows; next += size {
			size = 1 + next%37 // 1..37 rows per transaction
			last := min(next+size-1, rows)
			if _, err := w.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(%d, %d)", table, next, last)); err != nil {
				writer <- err
				return
			}
			size = last - next + 1
			time.Sleep(2 * time.Millisecond)
		}
		writer <- nil
	}()

	// Kill the leader three times while the writer runs.
	for range 3 {
		time.Sleep(700 * time.Millisecond)
		l := c.leader()
		c.kill(l)
		time.Sleep(500 * time.Millisecond)
		c.start(l)
	}
	if err := <-writer; err != nil {
		t.Fatal(err)
	}

	var events []jsonEvent
	waitE2E(t, "all rows in the log", 60*time.Second, func() bool {
		events = c.readLog()
		return len(events) >= rows
	})
	time.Sleep(time.Second) // anything extra (a duplicate) would show up now
	events = c.readLog()

	seen := map[string]int{}
	keys := map[string]bool{}
	lastLSN := ""
	var lastIdx uint64
	for _, e := range events {
		id, _ := e.After["id"].(string)
		seen[id]++
		key := fmt.Sprintf("%s/%d", e.LSN, e.Seq)
		if keys[key] {
			t.Fatalf("event key %s stored twice", key)
		}
		keys[key] = true
		if e.Index < lastIdx || (e.LSN != lastLSN && lsnLess(e.LSN, lastLSN)) {
			t.Fatalf("transactions out of commit order at index %d (%s after %s)", e.Index, e.LSN, lastLSN)
		}
		lastIdx, lastLSN = e.Index, e.LSN
	}
	for i := 1; i <= rows; i++ {
		if seen[fmt.Sprint(i)] != 1 {
			t.Fatalf("row %d stored %d times", i, seen[fmt.Sprint(i)])
		}
	}
	if len(events) != rows {
		t.Fatalf("%d events for %d rows", len(events), rows)
	}
	// The test is only meaningful if ingest really moved between leaders.
	if n := c.logCount("ingest started"); n < 4 {
		t.Fatalf("ingest started %d times; expected a takeover after every kill", n)
	}
	t.Logf("ingest sessions: %d, slot takeovers: %d", c.logCount("ingest started"), c.logCount("terminating previous reader"))
}

func waitE2E(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// readLog returns every event in the log, read from any live node.
func (c *e2eCluster) readLog() []jsonEvent {
	c.t.Helper()
	var out []jsonEvent
	for i, a := range c.addrs {
		if c.procs[i+1] == nil {
			continue
		}
		raw, err := exec.Command(c.bin, "log", "--addr", a).Output()
		if err != nil {
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if line == "" {
				continue
			}
			var e jsonEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				c.t.Fatalf("bad log line %q: %v", line, err)
			}
			out = append(out, e)
		}
		return out
	}
	return nil
}

func lsnLess(a, b string) bool {
	pa, _ := pglogrepl.ParseLSN(a)
	pb, _ := pglogrepl.ParseLSN(b)
	return pa < pb
}
