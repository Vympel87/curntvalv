package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// TestEndToEndTransactions: with transactions=true a Postgres transaction
// larger than a source batch and than a delivery batch still reaches a
// webhook in one request, each change counting the transaction's changes.
func TestEndToEndTransactions(t *testing.T) {
	f := newPGFixture(t)
	var mu sync.Mutex
	var problems []string
	rows := map[string]uint32{} // row id -> tx_changes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []event.JSON `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		perTx := map[string]uint32{}
		for _, e := range body.Events {
			perTx[e.LSN]++
		}
		mu.Lock()
		defer mu.Unlock()
		for _, e := range body.Events {
			if e.TxChanges != perTx[e.LSN] {
				problems = append(problems, fmt.Sprintf("row %v: tx_changes %d, %d of its transaction in the request", e.After["id"], e.TxChanges, perTx[e.LSN]))
			}
			rows[fmt.Sprint(e.After["id"])] = e.TxChanges
		}
	}))
	defer srv.Close()
	newE2EWith(t, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+srv.URL+",transactions=true")...)
	f.waitSlot()
	f.insert(1, 2500, 2500) // three source batches, more than one delivery batch
	f.insert(2501, 2510, 5)

	waitE2E(t, "every row delivered", 60*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(rows) == 2510
	})
	mu.Lock()
	defer mu.Unlock()
	if len(problems) > 0 {
		t.Fatalf("%d problems, first: %s", len(problems), problems[0])
	}
	if rows["1"] != 2500 || rows["2500"] != 2500 || rows["2501"] != 5 || rows["2510"] != 5 {
		t.Fatalf("tx_changes: row 1 %d, 2500 %d, 2501 %d, 2510 %d", rows["1"], rows["2500"], rows["2501"], rows["2510"])
	}
}
