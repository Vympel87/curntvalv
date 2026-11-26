package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// TestEndToEndSinkShape: a webhook sink with op, where and columns gets
// only the matching changes, without the dropped columns, while a second
// sink without them gets everything; the filtered sink's offset still
// reaches the end of the log.
func TestEndToEndSinkShape(t *testing.T) {
	f := newPGFixture(t)
	f.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN status text, ADD COLUMN secret text", f.table))
	var mu sync.Mutex
	var got, got2 []string
	receiver := func(into *[]string, field string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Events []event.JSON `json:"events"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			defer mu.Unlock()
			for _, e := range body.Events {
				keys := make([]string, 0, len(e.After))
				for k := range e.After {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				*into = append(*into, fmt.Sprintf("%s:%v:%v:%v:%v", e.Op, e.After["id"], e.After[field], e.After["secret"], keys))
			}
		}))
	}
	srv, srv2 := receiver(&got, "status"), receiver(&got2, "state")
	defer srv.Close()
	defer srv2.Close()
	all := newHookReceiverWith(t, 0)
	c := newE2EWith(t, f.env(), append(f.args(),
		"--sink", "name=paid,type=webhook,url="+srv.URL+",op=insert,op=update,where=status=paid,drop=secret",
		"--sink", "name=all,type=webhook,url="+all.url,
		"--sink", "name=expr,type=webhook,url="+srv2.URL+",where=status in (paid, shipped) and id > 1,mask=secret,rename=status:state")...)
	f.waitSlot()
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'paid', 's1'), (2, 'open', 's2')", f.table))
	f.exec(fmt.Sprintf("UPDATE %s SET status = 'paid' WHERE id = 2", f.table))
	f.exec(fmt.Sprintf("DELETE FROM %s WHERE id = 1", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'open', 's3')", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (10, 'shipped', 's10')", f.table))

	waitE2E(t, "unfiltered sink has every change", 30*time.Second, func() bool { _, n := all.ids(); return n >= 6 })
	waitE2E(t, "filtered sink caught up", 30*time.Second, func() bool {
		out, _ := c.cli("status")
		var paid, last string
		for _, line := range strings.Split(out, "\n") {
			if f := strings.Fields(line); len(f) > 5 && f[0] == "paid" {
				paid, last = f[4], f[5]
			}
		}
		return paid != "" && last == "0" // lag 0: the offset passed the filtered entries
	})
	mu.Lock()
	first := fmt.Sprint(got)
	mu.Unlock()
	if first != "[insert:1:paid:<nil>:[id status] update:2:paid:<nil>:[id status]]" {
		t.Fatalf("filtered sink got %v", first)
	}
	// id 10 compares as a number (10 > 1), the secret is masked, status
	// arrives as state; the delete (only its key is known) does not match.
	waitE2E(t, "expression sink", 30*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got2) >= 2 })
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	second := fmt.Sprint(got2)
	mu.Unlock()
	if second != "[update:2:paid:***:[id secret state] insert:10:shipped:***:[id secret state]]" {
		t.Fatalf("expression sink got %v", second)
	}
}
