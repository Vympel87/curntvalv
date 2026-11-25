package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestConfigEncoding(t *testing.T) {
	c := Configuration{Members: []Member{{ID: 1, Addr: "a:1", Voter: true}, {ID: 4, Addr: "d:4"}, {ID: 9, Addr: "", Voter: true}}}
	got, err := decodeConfig(encodeConfig(c))
	if err != nil || got.String() != c.String() {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range [][]byte{nil, {2}, {1, 5}, append(encodeConfig(c), 0)} {
		if _, err := decodeConfig(bad); !errors.Is(err, ErrCorruptEntry) {
			t.Errorf("%x: %v", bad, err)
		}
	}
	// Snapshots carry it only once there is one.
	app := []byte{6, 1, 2, 3}
	if b := wrapSnapshot(nil, app); string(b) != string(app) {
		t.Fatal("bootstrap snapshot changed")
	}
	conf, data, err := unwrapSnapshot(wrapSnapshot(&c, app))
	if err != nil || conf.String() != c.String() || string(data) != string(app) {
		t.Fatalf("%v %x %v", conf, data, err)
	}
	if conf, data, _ := unwrapSnapshot(app); conf != nil || string(data) != string(app) {
		t.Fatal("plain snapshot")
	}
	if _, _, err := unwrapSnapshot(append(slices.Clone(snapshotMagic), 0xff)); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("corrupt: %v", err)
	}
}

func TestConfChangeValidation(t *testing.T) {
	c := Configuration{Members: []Member{{ID: 1, Addr: "a", Voter: true}, {ID: 2, Addr: "b"}}}
	for _, cc := range []ConfChange{
		{Type: ConfAddLearner, ID: 1, Addr: "x"}, // a member
		{Type: ConfAddLearner, ID: 3},            // no address
		{Type: ConfAddLearner, ID: 3, Addr: "b"}, // address in use
		{Type: ConfPromote, ID: 1},               // a voter
		{Type: ConfPromote, ID: 3},               // not a member
		{Type: ConfRemove, ID: 1},                // the last voter
		{Type: ConfRemove, ID: 3},
		{Type: ConfAddLearner, Addr: "z"},
		{Type: 9, ID: 3},
	} {
		if _, err := c.apply(cc); !errors.Is(err, ErrInvalidConfChange) {
			t.Errorf("%+v: %v", cc, err)
		}
	}
	got, err := c.apply(ConfChange{Type: ConfAddLearner, ID: 0x10, Addr: "p"})
	if err != nil || got.String() != "[1=a(voter) 2=b(learner) 16=p(learner)]" || c.String() != "[1=a(voter) 2=b(learner)]" {
		t.Fatalf("%v %v (original %v)", got, err, c)
	}
}

// change proposes cc on the leader, retrying while another change is in
// progress, and waits until every live member applied it.
func (c *cluster) change(cc ConfChange) uint64 {
	c.t.Helper()
	var idx uint64
	c.runUntil(500, fmt.Sprintf("change %+v accepted", cc), func() bool {
		l := c.stableLeader()
		if l == 0 {
			return false
		}
		i, err := c.rafts[l].proposeConf(cc)
		switch {
		case err == nil:
			idx = i
			return true
		case errors.Is(err, ErrConfChangeInProgress):
			return false
		default:
			c.t.Fatalf("change %+v: %v", cc, err)
			return false
		}
	})
	c.runUntil(1000, fmt.Sprintf("change %+v applied", cc), func() bool {
		l := c.leader()
		if l == 0 || c.rafts[l].commitIndex < idx {
			return false
		}
		for _, m := range c.rafts[l].conf.Members {
			if r := c.rafts[m.ID]; r != nil && r.lastApplied < idx {
				return false
			}
		}
		return true
	})
	return idx
}

// waitReady waits until the leader committed an entry of its term, so it
// may change the membership.
func (c *cluster) waitReady() uint64 {
	c.t.Helper()
	var l uint64
	c.runUntil(500, "leader ready for changes", func() bool {
		l = c.stableLeader()
		if l == 0 {
			return false
		}
		t, _ := c.rafts[l].termAt(c.rafts[l].commitIndex)
		return t == c.rafts[l].term
	})
	return l
}

func voters(r *raft) string { return fmt.Sprint(r.conf.Voters()) }

func TestAddServer(t *testing.T) {
	c := newCluster(t, 3, 1)
	l := c.waitLeader()
	for i := range 30 {
		c.propose(l, fmt.Sprint("a", i))
	}
	c.waitApplied(30)
	c.addNode(4)
	c.run(30)
	if r := c.rafts[4]; r.state != StateFollower || r.term != 0 || len(r.conf.Members) != 0 {
		t.Fatalf("a joining node acted on its own: %+v", r.status())
	}

	idx := c.change(ConfChange{Type: ConfAddLearner, ID: 4, Addr: "n4"})
	c.waitApplied(idx)
	if r := c.rafts[4]; r.conf.IsVoter(4) || len(c.apps[4].entries) < 30 {
		t.Fatalf("learner %+v", r.status())
	}
	// A learner does not count: with two voters down nothing commits.
	l = c.leader()
	var down []uint64
	for _, id := range []uint64{1, 2, 3} {
		if id != l && len(down) < 2 {
			down = append(down, id)
		}
	}
	if len(down) != 2 {
		down = []uint64{1, 2, 3}[:2]
	}
	c.crash(down[0])
	c.crash(down[1])
	if l = c.leader(); l != 0 {
		i := c.propose(l, "stuck")
		c.run(50)
		if c.rafts[l] != nil && c.rafts[l].commitIndex >= i {
			t.Fatal("committed with one voter and a learner")
		}
	}
	c.start(down[0])
	c.start(down[1])
	c.waitLeader()

	idx = c.change(ConfChange{Type: ConfPromote, ID: 4})
	c.waitApplied(idx)
	for _, id := range c.ids {
		if got := voters(c.rafts[id]); got != "[1 2 3 4]" {
			t.Fatalf("node %d voters %s", id, got)
		}
	}
	// Four voters: one may be down.
	l = c.waitLeader()
	other := c.ids[(slices.Index(c.ids, l)+1)%4]
	c.crash(other)
	i := c.propose(c.waitLeader(), "with three")
	c.runUntil(300, "commit with 3 of 4", func() bool { return c.leader() != 0 && c.rafts[c.leader()].commitIndex >= i })
}

// A removed server that is still running cannot disturb the cluster, even
// without PreVote.
func TestRemoveFollower(t *testing.T) {
	c := newCluster(t, 3, 2)
	l := c.waitLeader()
	victim := c.ids[(slices.Index(c.ids, l)+1)%3]
	idx := c.change(ConfChange{Type: ConfRemove, ID: victim})
	c.waitApplied(idx)
	l = c.stableLeader()
	term := c.rafts[l].term
	c.run(400) // the victim times out again and again
	if c.leader() != l || c.rafts[l].term != term {
		t.Fatalf("removed node %d disturbed the cluster: leader %d term %d, was %d term %d", victim, c.leader(), c.rafts[c.leader()].term, l, term)
	}
	// Two voters remain: both are needed.
	i := c.propose(l, "x")
	c.waitApplied(i)
	for _, id := range c.ids {
		if id != victim && voters(c.rafts[id]) != fmt.Sprint(otherIDs(c.ids, victim)) {
			t.Fatalf("node %d voters %s", id, voters(c.rafts[id]))
		}
	}
}

func otherIDs(ids []uint64, not uint64) []uint64 {
	return slices.DeleteFunc(slices.Clone(ids), func(id uint64) bool { return id == not })
}

// A leader that removes itself steps down after the commit and never
// campaigns again; the others elect a new leader.
func TestRemoveLeader(t *testing.T) {
	c := newClusterCfg(t, 3, 3, func(cfg *Config) { cfg.PreVote = true })
	l := c.waitLeader()
	elections := c.rafts[l].counters.Elections
	idx := c.change(ConfChange{Type: ConfRemove, ID: l})
	if r := c.rafts[l]; r.state == StateLeader || r.commitIndex < idx {
		c.runUntil(100, "old leader steps down", func() bool { return c.rafts[l].state != StateLeader })
	}
	c.runUntil(500, "new leader", func() bool { n := c.leader(); return n != 0 && n != l })
	n := c.leader()
	i := c.propose(n, "after")
	c.runUntil(300, "commit after the removal", func() bool { return c.rafts[n].commitIndex >= i })
	c.run(300)
	if c.rafts[l].state != StateFollower || c.rafts[l].counters.Elections != elections {
		t.Fatalf("removed leader %d: %+v", l, c.rafts[l].status())
	}
}

func TestConfChangeOneAtATime(t *testing.T) {
	c := newCluster(t, 3, 4)
	l := c.waitReady()
	c.addNode(4)
	c.addNode(5)
	r := c.rafts[l]
	if _, err := r.proposeConf(ConfChange{Type: ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.proposeConf(ConfChange{Type: ConfAddLearner, ID: 5, Addr: "n5"}); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("second change while the first is uncommitted: %v", err)
	}
	if _, err := c.rafts[c.ids[(slices.Index(c.ids, l)+1)%3]].proposeConf(ConfChange{Type: ConfRemove, ID: 4}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("on a follower: %v", err)
	}
	c.change(ConfChange{Type: ConfAddLearner, ID: 5, Addr: "n5"})

	// A new leader must commit an entry of its term first.
	c.crash(l)
	c.runUntil(500, "new leader", func() bool { return c.leader() != 0 && c.leader() != l })
	n := c.rafts[c.waitReady()]
	n.commitIndex-- // pretend its no-op is not committed yet
	if _, err := n.proposeConf(ConfChange{Type: ConfRemove, ID: 5}); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("before committing in its term: %v", err)
	}
	n.commitIndex++
}

// An uncommitted configuration entry that is overwritten no longer applies.
func TestUncommittedConfigIsRolledBack(t *testing.T) {
	c := newCluster(t, 3, 5)
	l := c.waitReady()
	c.addNode(4)
	others := otherIDs([]uint64{1, 2, 3}, l)
	c.partition([]uint64{l, 4}, others)
	if _, err := c.rafts[l].proposeConf(ConfChange{Type: ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.rafts[l].conf.Member(4); !ok {
		t.Fatal("not in effect at once")
	}
	c.runUntil(500, "majority elects a leader", func() bool {
		n := c.leader()
		return n != 0 && n != l
	})
	n := c.leader()
	c.waitApplied(0)
	i := c.propose(n, "majority")
	c.runUntil(300, "commit", func() bool { return c.rafts[n].commitIndex >= i })
	c.heal()
	c.runUntil(500, "old leader catches up", func() bool { return c.rafts[l].commitIndex >= i })
	if _, ok := c.rafts[l].conf.Member(4); ok || c.rafts[l].confIndex != 0 {
		t.Fatalf("overwritten configuration still applies: %v", c.rafts[l].conf)
	}
}

// The configuration survives restarts: from the log, and from a snapshot
// once the log is compacted (and is sent with it to a new server).
func TestConfigSurvivesRestartAndSnapshot(t *testing.T) {
	c := newClusterCfg(t, 3, 6, func(cfg *Config) { cfg.SnapshotEntries = 10; cfg.TrailingEntries = 5 })
	l := c.waitLeader()
	c.addNode(4)
	c.change(ConfChange{Type: ConfAddLearner, ID: 4, Addr: "n4"})
	c.change(ConfChange{Type: ConfPromote, ID: 4})
	// From the log.
	c.crash(2)
	c.start(2)
	if voters(c.rafts[2]) != "[1 2 3 4]" {
		t.Fatalf("after restart: %v", c.rafts[2].conf)
	}
	l = c.waitLeader()
	for i := range 60 {
		c.propose(c.waitLeader(), fmt.Sprint("b", i))
		c.step()
	}
	c.waitApplied(c.rafts[c.waitLeader()].lastIndex)
	// From a snapshot: the configuration entries are compacted away.
	for _, id := range c.ids {
		if c.logs[id].FirstIndex() <= 10 {
			t.Fatalf("node %d log not compacted: %d", id, c.logs[id].FirstIndex())
		}
	}
	c.crash(3)
	c.start(3)
	if voters(c.rafts[3]) != "[1 2 3 4]" || c.rafts[3].confIndex != 0 {
		t.Fatalf("after restart from a snapshot: %v", c.rafts[3].conf)
	}
	// A new server gets it with the snapshot.
	c.addNode(5)
	idx := c.change(ConfChange{Type: ConfAddLearner, ID: 5, Addr: "n5"})
	c.waitApplied(idx)
	if m, ok := c.rafts[5].conf.Member(5); !ok || m.Voter || voters(c.rafts[5]) != "[1 2 3 4]" {
		t.Fatalf("new server: %v", c.rafts[5].conf)
	}
	if c.rafts[5].counters.SnapshotsInstalled == 0 {
		t.Fatal("the new server did not get a snapshot")
	}
	_ = l
}

// Membership changes under message loss, reordering, partitions and
// crashes; the invariants of Figure 3 are checked after every round.
func TestMembershipChaos(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			c := newClusterCfg(t, 3, seed, func(cfg *Config) { cfg.SnapshotEntries = 16; cfg.TrailingEntries = 8; cfg.PreVote = seed%2 == 0 })
			c.net.dropRate, c.net.dupRate, c.net.delayRate, c.net.reorder = 0.05, 0.02, 0.05, true
			rng := rand.New(rand.NewPCG(seed, 99))
			next := uint64(4)
			proposed := 0
			for round := 0; round < 3000; round++ {
				l := c.leader()
				switch x := rng.IntN(100); {
				case x < 30 && l != 0:
					c.propose(l, fmt.Sprint("e", proposed))
					proposed++
				case x < 33 && l != 0:
					r := c.rafts[l]
					var cc ConfChange
					var learners, vs []uint64
					for _, m := range r.conf.Members {
						if m.Voter {
							vs = append(vs, m.ID)
						} else {
							learners = append(learners, m.ID)
						}
					}
					switch {
					case len(learners) > 0:
						cc = ConfChange{Type: ConfPromote, ID: learners[0]}
					case len(vs) < 5 || rng.IntN(2) == 0 && len(vs) < 6:
						c.addNode(next)
						cc = ConfChange{Type: ConfAddLearner, ID: next, Addr: fmt.Sprint("n", next)}
						next++
					default:
						cc = ConfChange{Type: ConfRemove, ID: vs[rng.IntN(len(vs))]}
					}
					if _, err := r.proposeConf(cc); err != nil && !errors.Is(err, ErrConfChangeInProgress) && !errors.Is(err, ErrInvalidConfChange) {
						t.Fatal(err)
					}
				case x < 35:
					id := c.ids[rng.IntN(len(c.ids))]
					if c.up(id) {
						c.crash(id)
					}
				case x < 45:
					for _, id := range c.ids {
						if !c.up(id) {
							c.start(id)
						}
					}
				case x < 46:
					a := c.ids[rng.IntN(len(c.ids))]
					c.partition([]uint64{a}, otherIDs(c.ids, a))
				case x < 50:
					c.heal()
				}
				c.step()
			}
			// Heal everything: the cluster settles on one configuration and
			// keeps committing.
			c.heal()
			c.net.dropRate, c.net.dupRate, c.net.delayRate = 0, 0, 0
			for _, id := range c.ids {
				if !c.up(id) {
					c.start(id)
				}
			}
			l := c.waitLeader()
			i := c.propose(l, "final")
			c.runUntil(2000, "final commit", func() bool { n := c.leader(); return n != 0 && c.rafts[n].commitIndex >= i })
			n := c.leader()
			want := c.rafts[n].conf.String()
			c.runUntil(2000, "every member has the configuration", func() bool {
				for _, m := range c.rafts[c.leader()].conf.Members {
					if r := c.rafts[m.ID]; r == nil || r.conf.String() != want {
						return false
					}
				}
				return true
			})
			c.checkLogMatching()
			if len(c.rafts[n].conf.Voters()) < 1 {
				t.Fatal("no voters")
			}
			t.Logf("final configuration %s after %d nodes", want, next-1)
		})
	}
}
