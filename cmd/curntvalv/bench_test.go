package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Vympel87/curntvalv/internal/event"
)

// arrivals records when each row first reached a webhook.
type arrivals struct {
	mu   sync.Mutex
	at   map[string]time.Time
	reqs int
}

func newArrivals(t *testing.T) (*arrivals, string) {
	a := &arrivals{at: map[string]time.Time{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []event.JSON `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now()
		a.mu.Lock()
		defer a.mu.Unlock()
		a.reqs++
		for _, e := range body.Events {
			id, _ := e.After["id"].(string)
			if _, ok := a.at[id]; !ok {
				a.at[id] = now
			}
		}
	}))
	t.Cleanup(srv.Close)
	return a, srv.URL
}

func (a *arrivals) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.at)
}

func (a *arrivals) last() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	var last time.Time
	for _, t := range a.at {
		if t.After(last) {
			last = t
		}
	}
	return last
}

func envInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return def
}

// leaderMetric polls a metric on whichever node leads.
func (c *e2eCluster) leaderMetric(t *testing.T, name string) float64 {
	if l := c.leader(); l != 0 {
		return c.metric(t, l, name)
	}
	return -1
}

// TestBenchmark measures a 3-node cluster with fsync on, reading from a
// real Postgres and delivering to a local webhook (make bench). It reports
// the throughput of ingest (Postgres to the replicated log) and of
// delivery (to the webhook), for large and for one-row transactions, and
// the latency from a commit returning in Postgres to the row arriving.
func TestBenchmark(t *testing.T) {
	if os.Getenv("CURNTVALV_BENCH") != "1" {
		t.Skip("CURNTVALV_BENCH not set; run `make bench`")
	}
	f := newPGFixture(t)
	f.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN note text, ADD COLUMN amount numeric(12,2), ADD COLUMN at timestamptz", f.table))
	hook, url := newArrivals(t)
	c := newE2EWith(t, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+url,
		"--tick", "100ms", "--log-level", "info")...)
	f.waitSlot()
	var report []string
	add := func(format string, a ...any) {
		line := fmt.Sprintf(format, a...)
		report = append(report, line)
		t.Log(line)
	}
	rows := envInt("CURNTVALV_BENCH_ROWS", 200_000)
	txRows := envInt("CURNTVALV_BENCH_TX_ROWS", 1000)
	single := envInt("CURNTVALV_BENCH_SINGLE", 5000)
	conns := envInt("CURNTVALV_BENCH_CONNS", 8)

	// 1. Large transactions: one INSERT ... generate_series per txRows rows.
	before := c.leaderMetric(t, "curntvalv_source_events_total")
	logBefore := c.leaderMetric(t, "curntvalv_log_bytes")
	start := time.Now()
	var ingested time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		for time.Since(start) < 10*time.Minute {
			if ingested.IsZero() && c.leaderMetric(t, "curntvalv_source_events_total")-before >= float64(rows) {
				ingested = time.Now()
			}
			if hook.count() >= rows {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	for next := 1; next <= rows; next += txRows {
		f.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'note ' || g, g * 1.25, now() FROM generate_series(%d, %d) g",
			f.table, next, min(next+txRows-1, rows)))
	}
	committed := time.Since(start)
	<-done
	if hook.count() < rows {
		t.Fatalf("only %d of %d rows delivered", hook.count(), rows)
	}
	delivered := hook.last().Sub(start)
	add("| Large transactions (%d rows each), %d rows | Postgres commits | %.0f rows/s |", txRows, rows, float64(rows)/committed.Seconds())
	if !ingested.IsZero() {
		add("| | ingest into the log (3 nodes, fsync) | %.0f rows/s |", float64(rows)/ingested.Sub(start).Seconds())
	}
	add("| | delivered to the webhook | %.0f rows/s |", float64(rows)/delivered.Seconds())
	var size int
	if err := f.db.QueryRow(context.Background(), fmt.Sprintf("SELECT avg(pg_column_size(t.*))::int FROM %s t", f.table)).Scan(&size); err != nil {
		t.Fatal(err)
	}
	add("| | log bytes per row (row %d bytes in Postgres) | %.0f |", size, (c.leaderMetric(t, "curntvalv_log_bytes")-logBefore)/float64(rows))

	// 2. One-row transactions from several connections.
	base := rows + 1
	start = time.Now()
	var wg sync.WaitGroup
	for w := range conns {
		db := connect(t, f.url)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < single; i += conns {
				if _, err := db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (%d, 'single', 1, now())", f.table, base+i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	committed = time.Since(start)
	waitE2E(t, "one-row transactions delivered", 5*time.Minute, func() bool { return hook.count() >= rows+single })
	add("| One-row transactions, %d connections, %d transactions | Postgres commits | %.0f tx/s |", conns, single, float64(single)/committed.Seconds())
	add("| | delivered to the webhook | %.0f tx/s |", float64(single)/hook.last().Sub(start).Seconds())

	// 3. Latency at a steady 20 transactions per second.
	db := connect(t, f.url)
	base += single
	const n = 300
	commits := make([]time.Time, n)
	tick := time.NewTicker(50 * time.Millisecond)
	for i := range n {
		<-tick.C
		if _, err := db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (%d, 'latency', 1, now())", f.table, base+i)); err != nil {
			t.Fatal(err)
		}
		commits[i] = time.Now()
	}
	tick.Stop()
	waitE2E(t, "latency rows delivered", time.Minute, func() bool { return hook.count() >= rows+single+n })
	var lat []time.Duration
	hook.mu.Lock()
	for i := range n {
		lat = append(lat, hook.at[strconv.Itoa(base+i)].Sub(commits[i]))
	}
	hook.mu.Unlock()
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))].Round(100 * time.Microsecond) }
	add("| Latency, commit to webhook, %d one-row transactions at 20/s | p50 / p95 / p99 / max | %v / %v / %v / %v |", n, pct(0.5), pct(0.95), pct(0.99), lat[len(lat)-1].Round(100*time.Microsecond))

	var pgVersion string
	if err := f.db.QueryRow(context.Background(), "SHOW server_version").Scan(&pgVersion); err != nil {
		t.Fatal(err)
	}
	header := []string{
		fmt.Sprintf("Measured %s on %s/%s, %d CPUs, %s, Postgres %s.", time.Now().UTC().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH,
			runtime.NumCPU(), runtime.Version(), pgVersion),
		"",
		"| Workload | Measure | Result |",
		"|---|---|---|",
	}
	out := strings.Join(append(header, report...), "\n") + "\n"
	fmt.Print("\n" + out)
	if path := os.Getenv("CURNTVALV_BENCH_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// connect opens another Postgres connection, closed with the test.
func connect(t *testing.T, url string) *pgx.Conn {
	t.Helper()
	db, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(context.Background()) })
	return db
}
