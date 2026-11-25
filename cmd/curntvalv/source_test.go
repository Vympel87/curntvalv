package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestEndToEndSourceReset: a lost slot stops ingest loudly; `curntvalv
// source reset --to-slot` continues from a new slot, and every consumer,
// also one filtering by table, gets the source_reset event.
func TestEndToEndSourceReset(t *testing.T) {
	f := newPGFixture(t)
	hook := newHookReceiverWith(t, 0)
	c := newE2EWith(t, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+hook.url+",table="+f.table)...)
	sub := c.subscribe("--table", f.table)
	f.waitSlot()
	f.insert(1, 10, 5)
	waitE2E(t, "rows 1-10 delivered", 30*time.Second, func() bool { _, n := hook.ids(); return n >= 10 })

	if out, err := c.cli("source", "reset", "--to-slot"); err == nil || !strings.Contains(err.Error(), "has not stopped") {
		t.Fatalf("reset of a running source: %v %s", err, out)
	}

	// The slot disappears (say, a failover to a standby without it).
	ctx := context.Background()
	waitE2E(t, "slot dropped", 30*time.Second, func() bool {
		f.db.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", f.slot)
		_, err := f.db.Exec(ctx, "SELECT pg_drop_replication_slot($1)", f.slot)
		return err == nil
	})
	waitE2E(t, "source failed", 30*time.Second, func() bool {
		out, _ := c.cli("status")
		return strings.Contains(out, "failed") && strings.Contains(out, "slot")
	})
	if c.logCount("source reset --to-slot") == 0 {
		t.Fatal("the log does not say how to continue")
	}

	out := c.mustCLI("source", "reset", "--to-slot")
	if !strings.Contains(out, "source reset (index") || !strings.Contains(out, "source_reset event") {
		t.Fatalf("reset output: %s", out)
	}
	waitE2E(t, "source running", 30*time.Second, func() bool {
		out, _ := c.cli("status")
		return strings.Contains(out, "SOURCE postgres: running")
	})
	f.insert(11, 20, 5)

	waitE2E(t, "rows 11-20 and the reset delivered", 30*time.Second, func() bool { _, n := hook.ids(); return n >= 21 })
	got, _ := hook.ids()
	resets := 0
	for _, row := range got {
		if row == "" {
			resets++
		}
	}
	if len(got) != 21 || resets != 1 {
		t.Fatalf("webhook got %d events, %d resets: %v", len(got), resets, got)
	}
	waitE2E(t, "subscriber sees the reset", 30*time.Second, func() bool {
		return strings.Contains(sub.out.String(), `"op":"source_reset"`) && strings.Count(sub.out.String(), `"op":"insert"`) == 20
	})
}
