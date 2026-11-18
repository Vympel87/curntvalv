package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/sink"
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

func TestParseSizes(t *testing.T) {
	for in, want := range map[string]time.Duration{"0": 0, "7d": 7 * 24 * time.Hour, "1d12h": 36 * time.Hour, "90m": 90 * time.Minute} {
		if got, err := parseAge(in); err != nil || got != want {
			t.Errorf("parseAge(%q) = %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"d", "7x", "-1h", "1.5d", "7d7d"} {
		if _, err := parseAge(bad); err == nil {
			t.Errorf("parseAge(%q) accepted", bad)
		}
	}
	for in, want := range map[string]int64{"0": 0, "123": 123, "1KB": 1024, "512mb": 512 << 20, "10GB": 10 << 30, "2 TB": 2 << 40, "7B": 7} {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Errorf("parseBytes(%q) = %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "MB", "-1", "1.5GB", "9999999999TB"} {
		if _, err := parseBytes(bad); err == nil {
			t.Errorf("parseBytes(%q) accepted", bad)
		}
	}
}

func TestParseServerFlags(t *testing.T) {
	cfg, err := parseServerFlags([]string{"--id", "2", "--peers", "1=a:1,2=b:2,3=c:3", "--data-dir", "/d",
		"--retention-age", "1d12h", "--retention-bytes", "2GB", "--wal-segment-bytes", "1MB"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.raft.ID != 2 || fmt.Sprint(cfg.raft.Peers) != "[1 2 3]" || cfg.listen != "b:2" ||
		cfg.retention.MaxAge != 36*time.Hour || cfg.retention.MaxBytes != 2<<30 || cfg.segmentBytes != 1<<20 {
		t.Fatalf("%+v", cfg)
	}
	for _, args := range [][]string{
		{"--peers", "1=a:1", "--data-dir", "/d"},              // no id
		{"--id", "5", "--peers", "1=a:1", "--data-dir", "/d"}, // id not in peers
		{"--id", "1", "--peers", "1=a:1"},                     // no data dir
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "--log-level", "loud"},
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "extra"},
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "--retention-age", "a week"},
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "--retention-bytes", "lots"},
		{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d", "--wal-segment-bytes", "0"},
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
	https []string // HTTP addresses (--http-listen)
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
	return newE2EWith(t, nil, "--snapshot-entries", "10", "--retention-bytes", "512", "--wal-segment-bytes", "200")
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
		c.https = append(c.https, freePort(t))
		peers = append(peers, fmt.Sprintf("%d=%s", i, addr))
	}
	c.peers = strings.Join(peers, ",")
	for i := 1; i <= 3; i++ {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			if p != nil {
				p.cmd.Process.Signal(syscall.SIGCONT) // in case a chaos test paused it
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
		"--data-dir", filepath.Join(c.dir, fmt.Sprintf("node%d", i)), "--tick", "20ms", "--log-level", "debug",
		"--http-listen", c.https[i-1]}
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

// stop shuts node i down cleanly.
func (c *e2eCluster) stop(i int) {
	p := c.procs[i]
	p.cmd.Process.Signal(os.Interrupt)
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
	checkHTTP(t, c)

	// --retention-bytes 512 with 200 byte segments: every log was cut.
	out := c.mustCLI("status")
	for _, line := range strings.Split(out, "\n")[1:4] {
		if f := strings.Fields(line); len(f) < 6 || f[5] == "1" {
			t.Fatalf("log not compacted:\n%s", out)
		}
	}
	if !strings.Contains(out, "RETENTION max size 512 B") {
		t.Fatalf("status without retention:\n%s", out)
	}
}

// feed reads /api/events from the first live node, from the start of the
// log, until it has want distinct events (or 30s pass).
func (c *e2eCluster) feed(t *testing.T, table string, want int) (map[string]string, int) {
	t.Helper()
	ids, n := map[string]string{}, 0
	for i, h := range c.https {
		if c.procs[i+1] == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+h+"/api/events?from=1&table="+table, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for len(ids) < want && sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}
			var e event.JSON
			if err := json.Unmarshal([]byte(data), &e); err != nil {
				t.Fatalf("feed event %q: %v", data, err)
			}
			row, _ := e.After["id"].(string)
			ids[e.ID] = row
			n++
		}
		return ids, n
	}
	return ids, n
}

// checkHTTP checks the HTTP endpoints of a running cluster (design D13)
// after a leader change: every node is ready, shows the whole cluster with
// one leader, and exports metrics.
func checkHTTP(t *testing.T, c *e2eCluster) {
	t.Helper()
	httpGet := func(url string) (int, string) {
		resp, err := http.Get(url)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	for i, h := range c.https {
		base := "http://" + h
		waitE2E(t, fmt.Sprintf("node %d ready", i+1), 15*time.Second, func() bool { code, _ := httpGet(base + "/readyz"); return code == 200 })
		var view struct {
			Nodes []struct {
				Addr  string `json:"addr"`
				State string `json:"state"`
			} `json:"nodes"`
		}
		waitE2E(t, fmt.Sprintf("node %d to see one leader", i+1), 15*time.Second, func() bool {
			code, body := httpGet(base + "/api/status")
			if code != 200 || json.Unmarshal([]byte(body), &view) != nil || len(view.Nodes) != 3 {
				return false
			}
			leaders := 0
			for _, n := range view.Nodes {
				if n.State == "unreachable" {
					return false
				}
				if n.State == "leader" {
					leaders++
				}
			}
			return leaders == 1
		})
		code, metrics := httpGet(base + "/metrics")
		if code != 200 || !strings.Contains(metrics, "curntvalv_raft_commit_index ") ||
			!strings.Contains(metrics, fmt.Sprintf(`curntvalv_info{version="0.0.0-dev",node="%d"} 1`, i+1)) {
			t.Fatalf("node %d metrics: %d\n%.500s", i+1, code, metrics)
		}
		if code, page := httpGet(base + "/"); code != 200 || !strings.Contains(page, "curntValv") {
			t.Fatalf("node %d dashboard: %d", i+1, code)
		}
	}
	// The cluster lost a leader, so somebody won an election since.
	won := 0
	for _, h := range c.https {
		_, m := httpGet("http://" + h + "/metrics")
		for _, line := range strings.Split(m, "\n") {
			if v, ok := strings.CutPrefix(line, "curntvalv_raft_became_leader_total "); ok && v != "0" {
				won++
			}
		}
	}
	if won == 0 {
		t.Fatal("no node reports a won election")
	}
}

// sameApplied reports whether every reachable node in a status table has
// applied exactly want.
func sameApplied(table, want string) bool {
	for _, line := range strings.Split(table, "\n")[1:] {
		if line == "" {
			break // end of the node table
		}
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

// TestEndToEndPostgresNoLossNoDuplicates is the M4/M5 checker (design §5):
// a writer inserts numbered rows in transactions of varying size while the
// leader is killed with SIGKILL several times. Afterwards the log must hold
// every row exactly once, with every event key unique and transactions in
// commit order. The consumers of the log (a webhook that fails one request
// in five, a subscriber, and ClickHouse if CURNTVALV_TEST_CH_URL is set) must
// each have every row, and after de-duplication by event id exactly once.
func TestEndToEndPostgresNoLossNoDuplicates(t *testing.T) {
	f := newPGFixture(t)
	ctx, table := context.Background(), f.table
	var err error

	hook := newHookReceiver(t)
	args := append(f.args(), "--snapshot-entries", "50",
		"--sink", "name=hook,type=webhook,url="+hook.url+",table="+table)
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	var ch *sink.ClickHouse
	if chURL != "" {
		args = append(args, "--sink", "name=ch,type=clickhouse,url="+chURL+",into="+table)
		if ch, err = sink.NewClickHouse(sink.ClickHouseConfig{URL: chURL, Table: table}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ch.Query(context.Background(), "DROP TABLE IF EXISTS "+table) })
	}
	c := newE2EWith(t, f.env(), args...)
	sub := c.subscribe("--table", table)
	// Wait for the slot so the first rows are captured.
	f.waitSlot()

	const rows = 3000
	writer := make(chan error, 1)
	go func() {
		w, err := pgx.Connect(ctx, f.url)
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

	var events []event.JSON
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

	// Consumers: every row, once after de-duplication by event id.
	checkConsumer := func(name string, get func() (ids map[string]string, deliveries int)) {
		t.Helper()
		var ids map[string]string
		var deliveries int
		waitE2E(t, name+" to have every row", 60*time.Second, func() bool {
			ids, deliveries = get()
			return len(ids) >= rows
		})
		rowsSeen := map[string]bool{}
		for _, row := range ids {
			rowsSeen[row] = true
		}
		if len(ids) != rows || len(rowsSeen) != rows {
			t.Fatalf("%s: %d event ids for %d distinct rows; want %d", name, len(ids), len(rowsSeen), rows)
		}
		t.Logf("%s: %d deliveries for %d events", name, deliveries, rows)
	}
	checkConsumer("webhook", hook.ids)
	checkConsumer("subscriber", sub.ids)
	if ch != nil {
		checkConsumer("clickhouse", func() (map[string]string, int) {
			out, err := ch.Query(ctx, "SELECT id, JSONExtractString(after, 'id') FROM "+table+" FINAL FORMAT TSV")
			if err != nil {
				t.Fatal(err)
			}
			ids := map[string]string{}
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, l := range lines {
				if id, row, ok := strings.Cut(l, "\t"); ok {
					ids[id] = row
				}
			}
			return ids, len(lines)
		})
	}
	checkConsumer("dashboard feed", func() (map[string]string, int) { return c.feed(t, table, rows) })
	if n := c.logCount("sink started"); n < 4 {
		t.Fatalf("sinks started %d times; expected them to move with the leader", n)
	}
	status := c.mustCLI("status")
	if !strings.Contains(status, "SOURCE postgres: running") || !strings.Contains(status, "hook") {
		t.Fatalf("status without pipeline:\n%s", status)
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
func (c *e2eCluster) readLog() []event.JSON {
	c.t.Helper()
	var out []event.JSON
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
			var e event.JSON
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

// hookReceiver is a webhook endpoint that fails every failEvery-th request
// (0: never) and all requests while it is down.
type hookReceiver struct {
	url        string
	failEvery  int
	mu         sync.Mutex
	down       bool
	got        map[string]string // event id -> row id
	requests   int
	deliveries int
}

func newHookReceiver(t *testing.T) *hookReceiver { return newHookReceiverWith(t, 5) }

func newHookReceiverWith(t *testing.T, failEvery int) *hookReceiver {
	h := &hookReceiver{got: map[string]string{}, failEvery: failEvery}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.requests++
		if h.down || (h.failEvery > 0 && h.requests%h.failEvery == 0) {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		}
		var body struct {
			Events []event.JSON `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, e := range body.Events {
			row, _ := e.After["id"].(string)
			h.got[e.ID] = row
			h.deliveries++
		}
	}))
	t.Cleanup(srv.Close)
	h.url = srv.URL
	return h
}

func (h *hookReceiver) ids() (map[string]string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.got), h.deliveries
}

func (h *hookReceiver) setDown(down bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.down = down
}

// forget drops what was received so far.
func (h *hookReceiver) forget() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got, h.deliveries = map[string]string{}, 0
}

// subscriber is a running `curntvalv subscribe` process.
type subscriber struct {
	out *syncBuffer
}

func (c *e2eCluster) subscribe(args ...string) *subscriber {
	c.t.Helper()
	s := &subscriber{out: &syncBuffer{}}
	cmd := exec.Command(c.bin, append([]string{"subscribe", "--addr", strings.Join(c.addrs, ",")}, args...)...)
	cmd.Stdout = s.out
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	})
	return s
}

func (s *subscriber) ids() (map[string]string, int) {
	ids := map[string]string{}
	n := 0
	for _, line := range strings.Split(s.out.String(), "\n") {
		var e event.JSON
		if json.Unmarshal([]byte(line), &e) != nil {
			continue // empty or partly written line
		}
		row, _ := e.After["id"].(string)
		ids[e.ID] = row
		n++
	}
	return ids, n
}
