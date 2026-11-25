package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// backfillRe matches the backfill line of `curntvalv status`.
var backfillRe = regexp.MustCompile(`(?m)^public\.(\S+)\s+(\S+)\s+(\d+)\s`)

// backfillStatus returns the state and row count of the table's backfill.
func (c *e2eCluster) backfillStatus(table string) (string, int) {
	out, _ := c.cli("status")
	for _, m := range backfillRe.FindAllStringSubmatch(out, -1) {
		if m[1] == table {
			n, _ := strconv.Atoi(m[3])
			return m[2], n
		}
	}
	return "", 0
}

// TestEndToEndBackfill: rows that existed before the cluster reach every
// consumer through `curntvalv backfill start`, also when the leader is
// killed halfway; each row is stored once, and new rows keep flowing.
func TestEndToEndBackfill(t *testing.T) {
	f := newPGFixture(t)
	const before = 40_000
	f.exec(fmt.Sprintf("INSERT INTO %s SELECT generate_series(1, %d)", f.table, before))
	hook := newHookReceiverWith(t, 0)
	c := newE2EWith(t, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+hook.url)...)
	sub := c.subscribe("--table", f.table)
	f.waitSlot()

	if _, err := c.cli("backfill", "start", "nope_"+f.table); err == nil || !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("unknown table: %v", err)
	}
	out := c.mustCLI("backfill", "start", f.table)
	if !strings.Contains(out, "backfill started") || !strings.Contains(out, "public."+f.table) {
		t.Fatalf("start output: %s", out)
	}
	f.insert(before+1, before+200, 10) // while the backfill runs

	waitE2E(t, "backfill progress", 30*time.Second, func() bool { _, n := c.backfillStatus(f.table); return n >= 2000 })
	state, n := c.backfillStatus(f.table)
	if state != "running" {
		t.Fatalf("backfill %s with %d rows before the kill; make the table larger", state, n)
	}
	c.kill(c.leader())
	f.insert(before+201, before+400, 10)

	waitE2E(t, "backfill done", 90*time.Second, func() bool { s, _ := c.backfillStatus(f.table); return s == "done" })
	// Rows inserted meanwhile ahead of the backfill are read too (and also
	// arrive as inserts).
	_, stored := c.backfillStatus(f.table)
	if stored < before || stored > before+400 {
		t.Fatalf("backfill stored %d rows, want %d to %d", stored, before, before+400)
	}
	waitE2E(t, "every row delivered", 60*time.Second, func() bool {
		got, _ := hook.ids()
		rows := map[string]bool{}
		for _, r := range got {
			rows[r] = true
		}
		return len(rows) == before+400
	})
	// Each row was backfilled at most once, across the leader change, and
	// every existing row was.
	waitE2E(t, "subscriber has the backfill", 60*time.Second, func() bool {
		return strings.Count(sub.out.String(), `"op":"backfill"`) >= stored
	})
	seen := map[int]bool{}
	for _, m := range backfillRowRe.FindAllStringSubmatch(sub.out.String(), -1) {
		id, _ := strconv.Atoi(m[1])
		if seen[id] {
			t.Fatalf("row %d backfilled twice", id)
		}
		seen[id] = true
	}
	if len(seen) != stored {
		t.Fatalf("subscriber got %d backfilled rows, status says %d", len(seen), stored)
	}
	for id := 1; id <= before; id++ {
		if !seen[id] {
			t.Fatalf("row %d not backfilled", id)
		}
	}
}

var backfillRowRe = regexp.MustCompile(`"op":"backfill","after":\{"id":"(\d+)"\}`)
