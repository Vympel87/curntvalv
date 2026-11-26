package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// TestEndToEndPoisonChange: a webhook that refuses one row stops a sink by
// default; with on_error=skip the row is passed over, recorded, and every
// other row arrives.
func TestEndToEndPoisonChange(t *testing.T) {
	f := newPGFixture(t)
	var mu sync.Mutex
	got := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []event.JSON `json:"events"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		for _, e := range body.Events {
			if e.After["id"] == "13" {
				http.Error(w, "row 13: invalid", http.StatusUnprocessableEntity)
				return
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for _, e := range body.Events {
			got[fmt.Sprint(e.After["id"])] = true
		}
	}))
	defer srv.Close()
	c := newE2EWith(t, f.env(), append(f.args(),
		"--sink", "name=strict,type=webhook,url="+srv.URL,
		"--sink", "name=lenient,type=webhook,url="+srv.URL+",on_error=skip")...)
	f.waitSlot()
	f.insert(1, 30, 10)

	waitE2E(t, "lenient sink past the poison row", 60*time.Second, func() bool {
		out, _ := c.cli("status")
		for _, line := range strings.Split(out, "\n") {
			if fl := strings.Fields(line); len(fl) > 7 && fl[0] == "lenient" {
				return fl[5] == "0" && fl[7] == "1" // lag 0, 1 skipped
			}
		}
		return false
	})
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 29 {
		t.Fatalf("receiver got %d distinct rows, want 29", n)
	}
	out := c.mustCLI("sink", "skipped", "lenient")
	if !strings.Contains(out, "1 change(s) skipped by sink lenient") || !strings.Contains(out, "row 13: invalid") {
		t.Fatalf("skipped:\n%s", out)
	}
	// The strict sink is stuck at the poison row, as before.
	st := c.mustCLI("status")
	for _, line := range strings.Split(st, "\n") {
		if fl := strings.Fields(line); len(fl) > 7 && fl[0] == "strict" && (fl[3] != "retrying" || fl[7] != "0") {
			t.Fatalf("strict sink: %s", line)
		}
	}
}
