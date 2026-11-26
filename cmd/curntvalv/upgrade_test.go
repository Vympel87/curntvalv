package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// metric returns the value of a metric line in a node's /metrics.
func (c *e2eCluster) metric(t *testing.T, i int, name string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + c.https[i-1] + "/metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + ` (\S+)$`).FindSubmatch(b)
	if m == nil {
		return -1
	}
	v, _ := strconv.ParseFloat(string(m[1]), 64)
	return v
}

// applied returns node i's applied index from `curntvalv status`.
func (c *e2eCluster) applied(t *testing.T, i int) uint64 {
	t.Helper()
	for _, n := range parseNodes(c.mustCLI("status")) {
		if n.addr == c.addrs[i-1] {
			return n.applied
		}
	}
	return 0
}

// TestRollingUpgrade upgrades a running cluster from the release in
// CURNTVALV_UPGRADE_FROM_BIN (make test-upgrade) to this build, one node at
// a time, while Postgres changes flow in. A node of the old release that
// fell behind installs a snapshot from a new leader. Every row reaches the
// sink, and the nodes end with the same log.
func TestRollingUpgrade(t *testing.T) {
	old := os.Getenv("CURNTVALV_UPGRADE_FROM_BIN")
	if old == "" {
		t.Skip("CURNTVALV_UPGRADE_FROM_BIN not set; run `make test-upgrade`")
	}
	f := newPGFixture(t)
	hook := newHookReceiverWith(t, 0)
	c := newE2EBins(t, old, f.env(), append(f.args(), "--sink", "name=hook,type=webhook,url="+hook.url,
		"--snapshot-entries", "20", "--wal-segment-bytes", "32KB", "--retention-bytes", "64KB")...)
	f.waitSlot()
	next := 1
	insert := func(n int) {
		f.insert(next, next+n-1, 10)
		next += n
	}
	delivered := func(what string) {
		t.Helper()
		waitRows(t, what, func() (map[string]string, int) { return hook.ids() }, next-1)
	}
	insert(200)
	delivered("rows on the old release")

	// Node 3 (old) stops; nodes 1 and 2 move to the new build, so the
	// leader is new.
	c.stop(3)
	for _, i := range []int{1, 2} {
		c.stop(i)
		c.bins[i] = c.bin
		c.start(i)
		waitE2E(t, fmt.Sprintf("node %d back", i), 60*time.Second, func() bool { return c.leader() != 0 && c.applied(t, i) > 0 })
		insert(100)
	}
	delivered("rows with two new nodes")
	// Enough entries that the log is cut past where node 3 stopped.
	stopped := c.applied(t, 3) // unreachable: 0, so take the leader's
	for _, n := range parseNodes(c.mustCLI("status")) {
		if n.state == "leader" {
			stopped = n.applied
		}
	}
	waitE2E(t, "log cut past node 3", 120*time.Second, func() bool {
		insert(100)
		return leaderFirst(c.mustCLI("status")) > stopped+1
	})
	// Node 3, still old, comes back and must install a new leader's
	// snapshot.
	c.start(3)
	waitE2E(t, "old node installs a snapshot from the new leader", 60*time.Second, func() bool {
		return c.metric(t, 3, "curntvalv_raft_snapshots_installed_total") >= 1
	})
	insert(100)
	delivered("rows with an old node back")

	// The last node moves too.
	c.stop(3)
	c.bins[3] = c.bin
	c.start(3)
	insert(100)
	delivered("rows after the upgrade")
	waitE2E(t, "every node applied the same log", 60*time.Second, func() bool {
		st := c.mustCLI("status")
		lines := strings.Split(st, "\n")
		for _, l := range lines[1:] {
			if f := strings.Fields(l); len(f) >= 9 && f[2] == "leader" {
				return sameApplied(st, f[8]) && len(parseNodes(st)) == 3
			}
		}
		return false
	})
	if n := c.logCount("panic"); n > 0 {
		t.Fatalf("%d panics in the node logs", n)
	}
}
