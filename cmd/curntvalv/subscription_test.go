package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// subscribeIDs runs `curntvalv subscribe` with args until it printed the
// row id last (then it is interrupted), and returns the printed ids.
func (c *e2eCluster) subscribeIDs(t *testing.T, last string, args ...string) []string {
	t.Helper()
	out := &syncBuffer{}
	cmd := exec.Command(c.bin, append([]string{"subscribe", "--addr", strings.Join(c.addrs, ",")}, args...)...)
	cmd.Env = append(os.Environ(), c.cliEnv...)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ids := func() []string {
		var ids []string
		for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
			var ch struct {
				After map[string]*string `json:"after"`
			}
			if json.Unmarshal([]byte(line), &ch) == nil && ch.After["id"] != nil {
				ids = append(ids, *ch.After["id"])
			}
		}
		return ids
	}
	waitE2E(t, "subscriber printed row "+last, 60*time.Second, func() bool {
		got := ids()
		return len(got) > 0 && got[len(got)-1] == last
	})
	cmd.Process.Signal(os.Interrupt) // stores the position on the way out
	cmd.Wait()
	return ids()
}

// TestEndToEndNamedSubscription: a named subscription continues where it
// stopped, across a leader kill, and can be reset and deleted.
func TestEndToEndNamedSubscription(t *testing.T) {
	f := newPGFixture(t)
	c := newE2EWith(t, f.env(), f.args()...)
	f.waitSlot()
	f.insert(1, 50, 10)

	first := c.subscribeIDs(t, "50", "--name", "billing", "--table", f.table)
	if len(first) != 50 || first[0] != "1" {
		t.Fatalf("first run: %v", first)
	}
	waitE2E(t, "subscription listed", 15*time.Second, func() bool {
		out, _ := c.cli("subscription", "list")
		return strings.Contains(out, "billing") && strings.Contains(out, "block")
	})

	c.kill(c.leader())
	f.insert(51, 100, 10)
	second := c.subscribeIDs(t, "100", "--name", "billing", "--table", f.table)
	// At least once: nothing before the stored position comes again.
	if n, _ := strconv.Atoi(second[0]); n != 51 {
		t.Fatalf("second run started at %s, want 51: %v", second[0], second)
	}
	if len(second) != 50 {
		t.Fatalf("second run got %d rows", len(second))
	}
	out, _ := c.cli("status")
	if !strings.Contains(out, "SUBSCRIPTION") || !strings.Contains(out, "billing") {
		t.Fatalf("status without the subscription:\n%s", out)
	}

	// Replay from the start.
	if out := c.mustCLI("subscription", "reset", "billing", "--to", "earliest"); !strings.Contains(out, "generation 1") {
		t.Fatalf("reset: %s", out)
	}
	if again := c.subscribeIDs(t, "100", "--name", "billing", "--table", f.table); again[0] != "1" || len(again) != 100 {
		t.Fatalf("after reset: %d rows from %s", len(again), again[0])
	}
	// A new subscription from the latest position sees only new rows.
	inserted := make(chan struct{})
	go func() { // f.db is not shared: the main goroutine waits for this one below
		defer close(inserted)
		time.Sleep(2 * time.Second)
		f.insert(101, 110, 10)
	}()
	tail := c.subscribeIDs(t, "110", "--name", "tail", "--start", "latest", "--policy", "drop", "--table", f.table)
	<-inserted
	if tail[0] != "101" {
		t.Fatalf("latest: %v", tail)
	}
	c.mustCLI("subscription", "delete", "billing")
	if out, _ := c.cli("subscription", "list"); strings.Contains(out, "billing") || !strings.Contains(out, "drop") {
		t.Fatalf("after delete:\n%s", out)
	}
	if _, err := c.cli("subscription", "delete", "billing"); err == nil || !strings.Contains(err.Error(), "no subscription") {
		t.Fatalf("second delete: %v", err)
	}
}
