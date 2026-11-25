package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// join starts a new node i with --join and returns its address.
func (c *e2eCluster) join(i int) string {
	c.t.Helper()
	addr := freePort(c.t)
	c.addrs = append(c.addrs, addr)
	c.https = append(c.https, freePort(c.t))
	if c.extra == nil {
		c.extra = map[int][]string{}
	}
	c.extra[i] = []string{"--join", "--listen", addr}
	c.start(i)
	return addr
}

// members returns `curntvalv member list` output.
func (c *e2eCluster) members() string {
	out, _ := c.cli("member", "list")
	return out
}

// TestEndToEndMembership: while Postgres changes flow in, a node joins, the
// leader is killed, an old node is removed and the leader removes itself;
// every row reaches the sink, and the remaining nodes agree on the log.
func TestEndToEndMembership(t *testing.T) {
	f := newPGFixture(t)
	hook := newHookReceiverWith(t, 0)
	c := newE2EWith(t, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+hook.url)...)
	f.waitSlot()
	next := 1
	insert := func(n int) {
		f.insert(next, next+n-1, 10)
		next += n
	}
	delivered := func(what string) {
		t.Helper()
		want := next - 1
		waitE2E(t, what, 60*time.Second, func() bool {
			got, _ := hook.ids()
			rows := map[string]bool{}
			for _, r := range got {
				rows[r] = true
			}
			return len(rows) == want
		})
	}
	insert(100)
	delivered("rows before the join")

	// A fourth node joins: learner, then voter.
	addr4 := c.join(4)
	if _, err := c.cli("member", "add", "4="); err == nil {
		t.Fatal("member add without an address accepted")
	}
	out := c.mustCLI("member", "add", "4="+addr4)
	if !strings.Contains(out, "node 4 ("+addr4+") is a voting member") {
		t.Fatalf("add output: %s", out)
	}
	if out := c.mustCLI("member", "add", "4="+addr4); !strings.Contains(out, "voting member") {
		t.Fatalf("second add: %s", out)
	}
	if m := c.members(); strings.Count(m, " voter") != 4 {
		t.Fatalf("members:\n%s", m)
	}
	insert(100)

	// With four voters, losing the leader still leaves a majority.
	killed := c.leader()
	c.kill(killed)
	insert(100)
	delivered("rows after the leader was killed")
	c.start(killed) // rejoins with its stored membership

	// Remove one of the original nodes that is not the leader.
	l := c.leader()
	victim := 1
	for victim == l || c.procs[victim] == nil {
		victim++
	}
	out = c.mustCLI("member", "remove", fmt.Sprint(victim))
	if !strings.Contains(out, fmt.Sprintf("node %d removed", victim)) {
		t.Fatalf("remove output: %s", out)
	}
	c.stop(victim)
	insert(100)

	// The leader removes itself and steps down; another takes over.
	l = c.leader()
	c.mustCLI("member", "remove", fmt.Sprint(l))
	waitE2E(t, "a new leader", 30*time.Second, func() bool { return c.leader() != l })
	c.stop(l)
	insert(100)
	delivered("every row after the membership changes")

	// Two members remain: neither the removed node nor the old leader.
	m := c.members()
	for _, gone := range []int{victim, l} {
		if strings.Contains(m, "\n"+fmt.Sprint(gone)+" ") {
			t.Fatalf("node %d still a member:\n%s", gone, m)
		}
	}
	if c.logCount("cluster members changed") == 0 {
		t.Fatal("no node logged the membership change")
	}
}
