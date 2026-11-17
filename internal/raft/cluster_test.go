package raft

import (
	"fmt"
	"testing"
)

func TestInitialElection(t *testing.T) {
	for _, size := range []int{1, 3, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := newCluster(t, size, 1)
			l := c.waitLeader()
			// Leadership is stable while nothing fails.
			term := c.rafts[l].term
			c.run(200)
			if c.stableLeader() != l || c.rafts[l].term != term {
				t.Fatalf("leader changed without faults:\n%s", c.dump())
			}
		})
	}
}

func TestReplicateAndApply(t *testing.T) {
	c := newCluster(t, 5, 2)
	l := c.waitLeader()
	var last uint64
	for i := range 50 {
		last = c.propose(l, fmt.Sprintf("v%d", i))
	}
	c.waitApplied(last)
	for _, id := range c.ids {
		if got := len(c.apps[id].entries); uint64(got) != last {
			t.Fatalf("node %d applied %d, want %d", id, got, last)
		}
	}
}

func TestLeaderCrashElectsNewLeader(t *testing.T) {
	c := newCluster(t, 3, 3)
	l := c.waitLeader()
	idx := c.propose(l, "before")
	c.waitApplied(idx)

	c.crash(l)
	l2 := c.waitLeader()
	if l2 == l {
		t.Fatal("crashed node is leader")
	}
	idx = c.propose(l2, "after")
	c.waitApplied(idx)

	// The old leader restarts, catches up and re-applies everything.
	c.start(l)
	c.waitApplied(idx)
	if c.rafts[l].state != StateFollower {
		t.Fatalf("restarted node: %+v", c.rafts[l].status())
	}
}

func TestFollowerCatchesUpAfterDowntime(t *testing.T) {
	c := newCluster(t, 3, 4)
	l := c.waitLeader()
	var down uint64
	for _, id := range c.ids {
		if id != l {
			down = id
			break
		}
	}
	c.crash(down)
	var last uint64
	for i := range 300 { // many more than MaxEntriesPerMsg
		last = c.propose(l, fmt.Sprintf("v%d", i))
		if i%10 == 0 {
			c.step()
		}
	}
	c.waitApplied(last)
	c.start(down)
	c.waitApplied(last)
}

// §5.3/§5.4: a leader cut off in a minority cannot commit; when the
// partition heals its uncommitted entries are replaced by the majority's.
func TestLeaderInMinorityPartition(t *testing.T) {
	c := newCluster(t, 5, 5)
	old := c.waitLeader()
	base := c.propose(old, "committed")
	c.waitApplied(base)

	var minority, majority []uint64
	minority = append(minority, old)
	for _, id := range c.ids {
		if id == old {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.partition(minority, majority)

	lost := c.propose(old, "lost")
	c.run(100)
	if c.rafts[old].commitIndex >= lost {
		t.Fatalf("minority leader committed index %d", lost)
	}

	var newLeader uint64
	c.runUntil(500, "majority leader", func() bool {
		for _, id := range majority {
			if r := c.rafts[id]; r.state == StateLeader {
				newLeader = id
				return true
			}
		}
		return false
	})
	kept := c.propose(newLeader, "kept")
	c.runUntil(500, "majority commit", func() bool { return c.rafts[newLeader].commitIndex >= kept })

	c.heal()
	c.waitApplied(kept)
	for _, id := range c.ids {
		for _, e := range c.apps[id].entries {
			if string(e.Data) == "lost" {
				t.Fatalf("node %d applied the minority leader's entry", id)
			}
		}
	}
	if c.rafts[old].state == StateLeader && c.rafts[old].term <= c.rafts[newLeader].term {
		t.Fatalf("old leader did not step down:\n%s", c.dump())
	}
}

func TestNoProgressWithoutMajority(t *testing.T) {
	c := newCluster(t, 3, 6)
	l := c.waitLeader()
	for _, id := range c.ids {
		if id != l {
			c.crash(id)
		}
	}
	idx := c.propose(l, "x")
	c.run(300)
	for _, id := range c.ids {
		if r := c.rafts[id]; r != nil && r.commitIndex >= idx {
			t.Fatalf("node %d committed without a majority", id)
		}
	}
	for _, id := range c.ids {
		if !c.up(id) {
			c.start(id)
		}
	}
	l = c.waitLeader()
	idx = c.propose(l, "y")
	c.waitApplied(idx)
}

func TestAllNodesRestart(t *testing.T) {
	c := newCluster(t, 3, 7)
	l := c.waitLeader()
	var last uint64
	for i := range 20 {
		last = c.propose(l, fmt.Sprintf("v%d", i))
	}
	c.waitApplied(last)
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	l = c.waitLeader()
	idx := c.propose(l, "after restart")
	c.waitApplied(idx)
	for _, id := range c.ids {
		if string(c.apps[id].entries[last-1].Data) != "v19" {
			t.Fatalf("node %d lost data after restart", id)
		}
	}
}

func TestLossyNetwork(t *testing.T) {
	c := newCluster(t, 5, 8)
	c.net.dropRate = 0.2
	c.net.dupRate = 0.1
	c.net.delayRate = 0.2
	c.net.reorder = true
	var last uint64
	for i := 0; i < 100; {
		if l := c.leader(); l != 0 {
			if idx := c.propose(l, fmt.Sprintf("v%d", i)); idx != 0 {
				last = idx
				i++
			}
		}
		c.step()
	}
	c.net.dropRate, c.net.dupRate, c.net.delayRate = 0, 0, 0
	c.runUntil(1000, "final leader", func() bool { return c.stableLeader() != 0 })
	idx := c.propose(c.stableLeader(), "final")
	c.waitApplied(max(idx, last))
}

// TestRandomFaults runs a cluster under a random mix of lost, duplicated,
// delayed and reordered messages, partitions, crashes and restarts, checking
// the Raft safety invariants after every round. At the end the faults stop
// and the cluster must make progress again.
func TestRandomFaults(t *testing.T) {
	seeds := 20
	rounds := 3000
	if testing.Short() {
		seeds, rounds = 4, 1000
	}
	modes := map[string]func(*Config){
		"plain":   nil,
		"compact": snapCfg(7, 12), // frequent snapshots, aggressive log cuts
	}
	for mode, tweak := range modes {
		for _, size := range []int{3, 5} {
			for seed := 1; seed <= seeds; seed++ {
				t.Run(fmt.Sprintf("%s/n%d/seed%d", mode, size, seed), func(t *testing.T) {
					t.Parallel()
					runRandomFaults(t, size, uint64(seed), rounds, tweak)
				})
			}
		}
	}
}

func runRandomFaults(t *testing.T, size int, seed uint64, rounds int, tweak func(*Config)) *cluster {
	c := newClusterCfg(t, size, seed, tweak)
	rng := c.net.rng
	c.net.dropRate = 0.05
	c.net.dupRate = 0.02
	c.net.delayRate = 0.1
	c.net.reorder = true

	proposed := map[string]bool{}
	for i := range rounds {
		switch x := rng.Float64(); {
		case x < 0.20:
			if l := c.leader(); l != 0 {
				d := fmt.Sprintf("s%d-%d", seed, i)
				if c.propose(l, d) != 0 {
					proposed[d] = true
				}
			}
		case x < 0.21:
			id := c.ids[rng.IntN(size)]
			if c.up(id) {
				c.crash(id)
			}
		case x < 0.24:
			id := c.ids[rng.IntN(size)]
			if !c.up(id) {
				c.start(id)
			}
		case x < 0.25:
			perm := rng.Perm(size)
			cut := 1 + rng.IntN(size-1)
			var a, b []uint64
			for k, p := range perm {
				if k < cut {
					a = append(a, c.ids[p])
				} else {
					b = append(b, c.ids[p])
				}
			}
			c.partition(a, b)
		case x < 0.27:
			c.heal()
		}
		c.step()
	}

	// Stop all faults: the cluster must recover and commit a final entry.
	c.heal()
	c.net.dropRate, c.net.dupRate, c.net.delayRate = 0, 0, 0
	for _, id := range c.ids {
		if !c.up(id) {
			c.start(id)
		}
	}
	l := c.waitLeader()
	idx := c.propose(l, "final")
	c.waitApplied(idx)
	c.checkLogMatching()

	// Every committed entry was proposed exactly once.
	seen := map[string]bool{}
	for _, e := range c.committed {
		if e.Type != EntryNormal {
			continue
		}
		d := string(e.Data)
		if d != "final" && !proposed[d] {
			t.Fatalf("committed unknown entry %q", d)
		}
		if seen[d] {
			t.Fatalf("entry %q committed twice", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatalf("only %d entries committed; the fault mix is too harsh to be useful", len(seen))
	}
	if c.cfg.TrailingEntries > 0 {
		compacted := false
		for _, id := range c.ids {
			compacted = compacted || c.logs[id].FirstIndex() > 1
		}
		if !compacted {
			t.Fatal("compaction is enabled but no log was compacted")
		}
	}
	return c
}
