package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClickHouseFake(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	var bodies []string
	var users []string
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		b, _ := io.ReadAll(r.Body)
		queries = append(queries, r.URL.Query().Get("query"))
		bodies = append(bodies, string(b))
		users = append(users, r.Header.Get("X-ClickHouse-User")+"/"+r.Header.Get("X-ClickHouse-Key"))
		if fail {
			http.Error(w, "Code: 60. DB::Exception: Table does not exist", http.StatusNotFound)
		}
	}))
	defer srv.Close()
	u := strings.Replace(srv.URL, "http://", "http://bob:pw@", 1)
	c, err := NewClickHouse(ClickHouseConfig{URL: u, Database: "db", Table: "events"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Deliver(ctx, testChanges()); err != nil {
		t.Fatal(err)
	}
	if err := c.Deliver(ctx, testChanges()[:1]); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(queries) != 3 || !strings.HasPrefix(queries[0], "CREATE TABLE IF NOT EXISTS `db`.`events`") ||
		queries[1] != "INSERT INTO `db`.`events` FORMAT JSONEachRow" {
		t.Fatalf("queries %q", queries)
	}
	if users[0] != "bob/pw" {
		t.Fatalf("auth %q", users[0])
	}
	lines := strings.Split(strings.TrimSpace(bodies[1]), "\n")
	var row clickHouseRow
	if len(lines) != 2 || json.Unmarshal([]byte(lines[1]), &row) != nil {
		t.Fatalf("body %q", bodies[1])
	}
	if row.ID != "0/10:1" || row.LSN != 0x10 || row.Seq != 1 || row.SourceTable != "public.orders" || row.After != `{"id":"2"}` ||
		row.Before != "" || row.CommitTime != "1970-01-01 00:00:00.000000" || row.Unchanged == nil {
		t.Fatalf("row %+v", row)
	}
	fail = true
	mu.Unlock()
	err = c.Deliver(ctx, testChanges())
	if err == nil || !strings.Contains(err.Error(), "Code: 60") || strings.Contains(err.Error(), "pw") {
		t.Fatalf("error %v", err)
	}

	for _, cfg := range []ClickHouseConfig{{URL: "x"}, {URL: srv.URL, Table: "a;b"}, {URL: srv.URL, Database: "1x"}} {
		if _, err := NewClickHouse(cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

// TestClickHouseReal needs a ClickHouse server: run `make test-ch`.
func TestClickHouseReal(t *testing.T) {
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	if chURL == "" {
		t.Skip("CURNTVALV_TEST_CH_URL not set; run `make test-ch`")
	}
	table := fmt.Sprintf("test_%d", time.Now().UnixNano())
	c, err := NewClickHouse(ClickHouseConfig{URL: chURL, Table: table})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() { c.exec(context.Background(), "DROP TABLE IF EXISTS "+c.table, nil) })

	changes := testChanges()
	changes[0].CommitTimeUs = 1_700_000_000_123_456
	// Delivered twice (a redelivery after a leader change).
	for range 2 {
		if err := c.Deliver(ctx, changes); err != nil {
			t.Fatal(err)
		}
	}
	q := "SELECT id, lsn, seq, toString(commit_time), source_table, op, JSONExtractString(after, 'id') FROM " + c.table +
		" FINAL ORDER BY seq FORMAT TSV"
	out, err := c.exec(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "0/10:0\t16\t0\t2023-11-14 22:13:20.123456\tpublic.orders\tinsert\t1\n" +
		"0/10:1\t16\t1\t1970-01-01 00:00:00.000000\tpublic.orders\tinsert\t2\n"
	if string(out) != want {
		t.Fatalf("got\n%s\nwant\n%s", out, want)
	}

	// A wrong password is an error, without the password in it.
	u, _ := url.Parse(chURL)
	u.User = url.UserPassword("default", "wrong-secret")
	bad, _ := NewClickHouse(ClickHouseConfig{URL: u.String(), Table: table})
	if err := bad.Deliver(ctx, changes); err == nil || strings.Contains(err.Error(), "wrong-secret") {
		t.Fatalf("bad password: %v", err)
	}
}
