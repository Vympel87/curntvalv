package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// TestEndToEndSchemaChanges: ALTER TABLE in Postgres reaches a webhook with
// schema_changes=true as a schema event before the first row under the new
// definition, and a ClickHouse mirror follows an added column and a type
// change.
func TestEndToEndSchemaChanges(t *testing.T) {
	f := newPGFixture(t)
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []event.JSON `json:"events"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		for _, e := range body.Events {
			if e.Op == "schema" {
				var cols []string
				for _, c := range e.Columns {
					cols = append(cols, c.Name+":"+c.Type)
				}
				got = append(got, "schema "+strings.Join(cols, ","))
			} else {
				got = append(got, fmt.Sprintf("%s %v", e.Op, e.After["id"]))
			}
		}
	}))
	defer srv.Close()
	args := append(f.args(), "--sink", "name=hook,type=webhook,url="+srv.URL+",schema_changes=true")
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	prefix := "s" + strings.TrimPrefix(f.table, "rows_") + "_"
	mirror := "default.`" + prefix + f.table + "`"
	if chURL != "" {
		args = append(args, "--sink", "name=mirror,type=clickhouse,url="+chURL+",mode=mirror,into="+prefix+"{table}")
		t.Cleanup(func() { http.Post(chURL+"/?query="+url.QueryEscape("DROP TABLE IF EXISTS "+mirror), "text/plain", nil) })
	}
	newE2EWith(t, f.env(), args...)
	f.waitSlot()

	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (1)", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (2)", f.table))
	f.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN qty integer", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (3, 7)", f.table))
	f.exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN qty TYPE bigint", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (4, 5000000000)", f.table))

	want := []string{"schema id:integer", "insert 1", "insert 2",
		"schema id:integer,qty:integer", "insert 3",
		"schema id:integer,qty:bigint", "insert 4"}
	waitE2E(t, "every change at the webhook", 60*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= len(want)
	})
	mu.Lock()
	if !slices.Equal(got, want) {
		t.Fatalf("webhook got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	mu.Unlock()
	if chURL == "" {
		return
	}
	waitE2E(t, "mirror rows", 60*time.Second, func() bool {
		resp, err := http.Post(chURL+"/?query="+url.QueryEscape("SELECT count() FROM "+mirror+" FINAL"), "text/plain", nil)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var n int
		fmt.Fscan(resp.Body, &n)
		return n == 4
	})
	// Rows 1 and 2 have qty NULL, which groupArray skips.
	if out := chQuery(t, chURL, "SELECT toTypeName(qty), groupArray(qty) FROM (SELECT qty FROM "+mirror+" FINAL ORDER BY id) GROUP BY 1"); out != "Nullable(Int64)\t[7,5000000000]" {
		t.Fatalf("mirror: %q", out)
	}
}
