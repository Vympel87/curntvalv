package web

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

var (
	_ Peers        = (*GRPCPeers)(nil)
	_ ChangeReader = (*event.Reader)(nil)
)

type fakeLocal struct {
	mu  sync.Mutex
	st  *pb.StatusResponse
	err error
}

func (f *fakeLocal) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.st, f.err
}

type fakePeers map[string]*pb.StatusResponse

func (f fakePeers) NodeStatus(_ context.Context, addr string) (*pb.StatusResponse, error) {
	if st, ok := f[addr]; ok {
		return st, nil
	}
	return nil, errors.New("connection refused")
}

// feedReader hands out scripted results, then blocks until ctx ends.
type feedReader struct {
	mu      sync.Mutex
	froms   []uint64
	results []event.Result
	errs    []error
}

func (r *feedReader) Read(ctx context.Context, from uint64, _ int, _ event.Filter) (event.Result, error) {
	r.mu.Lock()
	r.froms = append(r.froms, from)
	if len(r.results) > 0 {
		res, err := r.results[0], r.errs[0]
		r.results, r.errs = r.results[1:], r.errs[1:]
		r.mu.Unlock()
		return res, err
	}
	r.mu.Unlock()
	<-ctx.Done()
	return event.Result{}, ctx.Err()
}

func (r *feedReader) push(res event.Result, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results, r.errs = append(r.results, res), append(r.errs, err)
}

func (r *feedReader) starts() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.froms...)
}

func leaderStatus() *pb.StatusResponse {
	return &pb.StatusResponse{Id: 1, Term: 3, State: "leader", Leader: 1, FirstIndex: 5, LastIndex: 40, CommitIndex: 40, LastApplied: 40,
		SourceState: "running",
		Sinks: []*pb.SinkStatus{
			{Name: "hook", Type: "webhook", Policy: "drop", State: "retrying", Offset: 30, Lag: 10, Delivered: 7, Failures: 2, LastError: `bad "quote"`},
			{Name: "ch", Type: "clickhouse", Policy: "block", State: "running", Offset: 40, Generation: 1},
		},
		Retention: &pb.RetentionStatus{MaxBytes: 1 << 20, LogBytes: 4096, CompactLimit: 30, BlockedBy: "ch"},
		Backfills: []*pb.Backfill{
			{Table: "public.users", Id: 3, State: pb.BackfillState_BACKFILL_STATE_DONE, Rows: 12},
			{Table: "public.orders", Id: 8, State: pb.BackfillState_BACKFILL_STATE_PENDING, Rows: 500, LastKey: [][]byte{[]byte("17"), []byte("a")}},
			{Table: "s.t", Id: 9, State: pb.BackfillState_BACKFILL_STATE_PENDING},
		}}
}

func TestMetrics(t *testing.T) {
	out := string(writeMetrics(leaderStatus(), Counters{Elections: 2, BecameLeader: 1, SourceEvents: 99, SubscribeStreams: 3}, `1.0 "x"`))
	for _, want := range []string{
		`curntvalv_info{version="1.0 \"x\"",node="1"} 1`,
		`curntvalv_raft_state{state="leader"} 1`,
		`curntvalv_raft_state{state="follower"} 0`,
		`curntvalv_raft_first_index 5`,
		`curntvalv_raft_elections_total 2`,
		`curntvalv_retention_blocked{sink="ch"} 1`,
		`curntvalv_retention_max_bytes 1.048576e+06`,
		`curntvalv_source_state{state="running"} 1`,
		`curntvalv_source_events_total 99`,
		`curntvalv_sink_lag_entries{sink="hook",type="webhook"} 10`,
		`curntvalv_sink_failures_total{sink="hook",type="webhook"} 2`,
		`curntvalv_sink_state{sink="hook",type="webhook",state="retrying"} 1`,
		`curntvalv_sink_generation{sink="ch",type="clickhouse"} 1`,
		`curntvalv_subscribe_streams 3`,
		`curntvalv_backfill_rows{table="public.orders"} 500`,
		`curntvalv_backfill_state{table="public.orders",state="running"} 1`,
		`curntvalv_backfill_state{table="s.t",state="pending"} 1`,
		`curntvalv_backfill_state{table="public.users",state="done"} 1`,
		"# TYPE curntvalv_sink_delivered_total counter",
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q", want)
		}
	}
	// Sinks are sorted by name, and every family is declared once, before
	// its samples.
	if strings.Index(out, `sink="ch"`) > strings.Index(out, `sink="hook"`) {
		t.Error("sinks not sorted")
	}
	sample := regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([a-zA-Z_][a-zA-Z0-9_]*="(\\.|[^"\\])*",?)*\})? [-+0-9.e]+$`)
	declared := map[string]int{}
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		if f := strings.Fields(line); len(f) >= 3 && f[0] == "#" {
			if f[1] == "TYPE" {
				declared[f[2]]++
			}
			continue
		}
		m := sample.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("bad sample line %q", line)
		}
		if declared[m[1]] != 1 {
			t.Fatalf("sample %q before or without exactly one TYPE line", line)
		}
	}

	// A node without source and sinks has no such metrics.
	out = string(writeMetrics(&pb.StatusResponse{Id: 2, State: "follower"}, Counters{}, "dev"))
	if strings.Contains(out, "curntvalv_source_") || strings.Contains(out, "curntvalv_sink_") || strings.Contains(out, "retention") {
		t.Fatalf("unexpected metrics:\n%s", out)
	}
}

func newTestServer(t *testing.T, local *fakeLocal, peers fakePeers, reader ChangeReader, maxFeeds int) *httptest.Server {
	t.Helper()
	s := New(Config{Version: "1.2.3", Peers: []string{"a:1", "b:2", "c:3"}, Self: "a:1", MaxFeeds: maxFeeds,
		Counters: func(context.Context) Counters { return Counters{Elections: 4} }}, local, peers, reader)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHealthReadyAndMetrics(t *testing.T) {
	local := &fakeLocal{st: &pb.StatusResponse{Id: 1, State: "follower"}}
	srv := newTestServer(t, local, nil, &feedReader{}, 0)
	if code, _ := get(t, srv.URL+"/healthz"); code != 200 {
		t.Fatalf("healthz %d", code)
	}
	if code, body := get(t, srv.URL+"/readyz"); code != 503 || !strings.Contains(body, "no leader") {
		t.Fatalf("readyz without leader: %d %q", code, body)
	}
	local.mu.Lock()
	local.st.Leader = 2
	local.mu.Unlock()
	if code, _ := get(t, srv.URL+"/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") ||
		!strings.Contains(string(b), "curntvalv_raft_elections_total 4\n") {
		t.Fatalf("metrics %s\n%s", resp.Header.Get("Content-Type"), b)
	}

	local.mu.Lock()
	local.err = raft.ErrStopped
	local.mu.Unlock()
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		if code, _ := get(t, srv.URL+p); code != 503 {
			t.Errorf("%s with a stopped node: %d", p, code)
		}
	}
}

func TestAPIStatus(t *testing.T) {
	local := &fakeLocal{st: &pb.StatusResponse{Id: 1, State: "follower", Term: 3, Leader: 2}}
	srv := newTestServer(t, local, fakePeers{"b:2": func() *pb.StatusResponse { s := leaderStatus(); s.Id = 2; return s }()}, &feedReader{}, 0)
	code, body := get(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	var v clusterView
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != "1.2.3" || v.Self != "a:1" || len(v.Nodes) != 3 {
		t.Fatalf("%+v", v)
	}
	if n := v.Nodes[0]; n.Addr != "a:1" || n.ID != 1 || n.State != "follower" {
		t.Fatalf("self %+v", n)
	}
	if n := v.Nodes[1]; n.ID != 2 || n.State != "leader" || n.First != 5 || n.Applied != 40 {
		t.Fatalf("leader %+v", n)
	}
	if n := v.Nodes[2]; n.State != "unreachable" || !strings.Contains(n.Error, "refused") {
		t.Fatalf("down node %+v", n)
	}
	p := v.Pipeline
	if p == nil || p.Node != 2 || p.SourceState != "running" || len(p.Sinks) != 2 || p.Sinks[0].Failures != 2 ||
		p.Retention == nil || p.Retention.BlockedBy != "ch" {
		t.Fatalf("pipeline %+v", p)
	}
	if b := p.Backfills; len(b) != 3 || b[1] != (backfillView{Table: "public.orders", State: "running", Rows: 500, LastKey: "17,a", Started: 8}) ||
		b[0].State != "done" || b[2].State != "pending" {
		t.Fatalf("backfills %+v", b)
	}

	// Without a leader there is no pipeline.
	srv = newTestServer(t, local, fakePeers{}, &feedReader{}, 0)
	_, body = get(t, srv.URL+"/api/status")
	if !strings.Contains(body, `"pipeline":null`) {
		t.Fatalf("pipeline without leader: %s", body)
	}
}

var ordersSchema = &pb.Schema{Id: 7, Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}}}

func change(index, lsn uint64, seq uint32) event.Change {
	return event.Change{Index: index, CommitLSN: lsn, Seq: seq, Schema: ordersSchema,
		Event: &pb.Event{SchemaId: 7, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")}}}}}
}

// readEvents reads n SSE data events from body.
func readEvents(t *testing.T, body io.Reader, n int) (ids []string, data []event.JSON) {
	t.Helper()
	sc := bufio.NewScanner(body)
	for len(data) < n && sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			ids = append(ids, strings.TrimPrefix(line, "id: "))
		case strings.HasPrefix(line, "data: "):
			var j event.JSON
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &j); err != nil {
				t.Fatalf("data %q: %v", line, err)
			}
			data = append(data, j)
		}
	}
	return ids, data
}

func TestAPIEvents(t *testing.T) {
	local := &fakeLocal{st: &pb.StatusResponse{Id: 1, State: "leader", Leader: 1, LastApplied: 41}}
	reader := &feedReader{}
	reader.push(event.Result{Changes: []event.Change{change(42, 0x100, 0), change(42, 0x100, 1)}, Next: 43}, nil)
	reader.push(event.Result{Next: 45}, nil) // entries without matching changes
	reader.push(event.Result{}, raft.ErrCompacted)
	reader.push(event.Result{Changes: []event.Change{change(60, 0x200, 0)}, Next: 61}, nil)
	srv := newTestServer(t, local, nil, reader, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events?table=orders", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	ids, data := readEvents(t, resp.Body, 3)
	if strings.Join(ids, " ") != "42 42 60" || data[1].ID != "0/100:1" || data[2].Table != "public.orders" {
		t.Fatalf("ids %v data %+v", ids, data)
	}
	// Started after the applied index, continued at next, restarted at the
	// oldest entry after compaction.
	if got := reader.starts(); len(got) < 4 || got[0] != 42 || got[1] != 43 || got[2] != 45 || got[3] != 0 {
		t.Fatalf("reads from %v", got)
	}

	// One feed at a time (MaxFeeds 1).
	if code, _ := get(t, srv.URL+"/api/events"); code != 503 {
		t.Fatalf("second feed: %d", code)
	}
	cancel()
	resp.Body.Close()

	// Bad requests; Last-Event-ID resumes at that entry.
	srv2 := newTestServer(t, local, nil, &feedReader{}, 0)
	for _, q := range []string{"?from=x", "?from=0", "?table="} {
		if code, _ := get(t, srv2.URL+"/api/events"+q); code != 400 {
			t.Errorf("%s: %d", q, code)
		}
	}
	r2 := &feedReader{}
	r2.push(event.Result{Changes: []event.Change{change(50, 0x300, 0)}, Next: 51}, nil)
	srv3 := newTestServer(t, local, nil, r2, 0)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()
	req, _ = http.NewRequestWithContext(ctx3, http.MethodGet, srv3.URL+"/api/events", nil)
	req.Header.Set("Last-Event-ID", "50")
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if ids, _ := readEvents(t, resp3.Body, 1); len(ids) != 1 || r2.starts()[0] != 50 {
		t.Fatalf("resume: ids %v reads %v", ids, r2.starts())
	}
}

func TestDashboardIsServed(t *testing.T) {
	srv := newTestServer(t, &fakeLocal{st: &pb.StatusResponse{}}, nil, &feedReader{}, 0)
	code, html := get(t, srv.URL+"/")
	if code != 200 || !strings.Contains(html, `<script src="app.js">`) || !strings.Contains(html, `href="style.css"`) {
		t.Fatalf("index: %d %.200s", code, html)
	}
	code, js := get(t, srv.URL+"/app.js")
	if code != 200 {
		t.Fatalf("app.js %d", code)
	}
	if c, _ := get(t, srv.URL+"/style.css"); c != 200 {
		t.Fatalf("style.css %d", c)
	}
	// Every endpoint the page uses exists, and it loads nothing from
	// elsewhere (the dashboard works offline).
	for _, path := range regexp.MustCompile(`"(api/[a-z]+)`).FindAllStringSubmatch(js, -1) {
		if path[1] != "api/status" && path[1] != "api/events" {
			t.Errorf("app.js uses unknown endpoint %s", path[1])
		}
	}
	if !strings.Contains(js, `"api/status"`) || !strings.Contains(js, `"api/events"`) {
		t.Error("app.js does not use the API")
	}
	if regexp.MustCompile(`(src|href)="(https?:)?//`).MatchString(html) || strings.Contains(js, "http://") || strings.Contains(js, "https://") {
		t.Error("dashboard loads external resources")
	}
}
