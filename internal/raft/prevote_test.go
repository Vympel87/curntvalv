package raft

import (
	"testing"
)

func preVote(c *Config) { c.PreVote = true }

// isolate cuts id off from every other node, then runs the cluster.
func (c *cluster) isolate(id uint64, rounds int) {
	var rest []uint64
	for _, o := range c.ids {
		if o != id {
			rest = append(rest, o)
		}
	}
	c.partition([]uint64{id}, rest)
	c.run(rounds)
}

// A follower that was cut off does not raise its term while alone, and its
// return does not depose the leader (thesis §9.6). Without PreVote it does
// both, which shows that the test can tell.
func TestPreVoteRejoiningNodeDoesNotDisrupt(t *testing.T) {
	for _, pv := range []bool{true, false} {
		c := newClusterCfg(t, 3, 11, func(cfg *Config) { cfg.PreVote = pv })
		leader := c.waitLeader()
		term := c.rafts[leader].term
		var follower uint64
		for _, id := range c.ids {
			if id != leader {
				follower = id
				break
			}
		}
		c.isolate(follower, 300)
		c.heal()
		c.run(100)
		c.check()
		after := c.stableLeader()
		raised := c.rafts[follower].term > term
		changed := after != leader || c.rafts[after].term != term
		if pv && (raised || changed) {
			t.Fatalf("with PreVote: leader %d term %d -> leader %d, terms:\n%s", leader, term, after, c.dump())
		}
		if !pv && !raised {
			t.Fatalf("without PreVote the isolated node kept term %d; the test proves nothing", term)
		}
	}
}

// With PreVote a crashed leader is still replaced, after one pre-vote round.
func TestPreVoteElectsAfterLeaderCrash(t *testing.T) {
	c := newClusterCfg(t, 5, 3, preVote)
	old := c.waitLeader()
	term := c.rafts[old].term
	idx := c.propose(old, "before")
	c.waitApplied(idx)
	c.crash(old)
	c.runUntil(500, "new leader", func() bool { l := c.stableLeader(); return l != 0 && l != old })
	l := c.stableLeader()
	if c.rafts[l].term <= term {
		t.Fatalf("new leader in term %d, old term %d", c.rafts[l].term, term)
	}
	c.waitApplied(c.propose(l, "after"))
	c.check()
}

// A candidate whose election failed asks for pre-votes again instead of
// raising its term while cut off.
func TestPreVoteCandidateCutOffKeepsTerm(t *testing.T) {
	for _, pv := range []bool{true, false} {
		c := newClusterCfg(t, 3, 7, func(cfg *Config) { cfg.PreVote = pv })
		leader := c.waitLeader()
		var f uint64
		for _, id := range c.ids {
			if id != leader {
				f = id
				break
			}
		}
		c.isolate(f, 0)
		r := c.rafts[f]
		if err := r.campaign(); err != nil { // as if a pre-vote had just passed
			t.Fatal(err)
		}
		term := r.term
		c.run(300)
		if pv && r.term != term {
			t.Fatalf("with PreVote the cut-off candidate went from term %d to %d", term, r.term)
		}
		if !pv && r.term == term {
			t.Fatal("without PreVote the candidate kept its term; the test proves nothing")
		}
	}
}

func TestPreVoteRequestChangesNothing(t *testing.T) {
	r, tr, _, st := newTestRaft(t, 1, []uint64{1, 2, 3}, 5, 1, 5)
	r.cfg.PreVote = true
	// Granted: term ahead, log up to date, no leader heard.
	mustStep(t, r, Message{Type: MsgPreVote, From: 2, To: 1, Term: 6, LastLogIndex: 2, LastLogTerm: 5})
	msgs := tr.take()
	if len(msgs) != 1 || msgs[0].Type != MsgPreVoteResp || !msgs[0].VoteGranted || msgs[0].Term != 6 {
		t.Fatalf("got %+v", msgs)
	}
	if r.term != 5 || r.votedFor != 0 || st.hs.Term != 5 || r.state != StateFollower {
		t.Fatalf("pre-vote changed state: term %d vote %d stored %+v", r.term, r.votedFor, st.hs)
	}
	// Refused: the candidate's log is behind.
	mustStep(t, r, Message{Type: MsgPreVote, From: 3, To: 1, Term: 6, LastLogIndex: 1, LastLogTerm: 1})
	if msgs := tr.take(); len(msgs) != 1 || msgs[0].VoteGranted || msgs[0].Term != 5 {
		t.Fatalf("stale log: %+v", msgs)
	}
	// Refused: a leader was heard from just now.
	mustStep(t, r, Message{Type: MsgApp, From: 3, To: 1, Term: 5, PrevLogIndex: 2, PrevLogTerm: 5})
	tr.take()
	mustStep(t, r, Message{Type: MsgPreVote, From: 2, To: 1, Term: 6, LastLogIndex: 2, LastLogTerm: 5})
	if msgs := tr.take(); len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("recent leader: %+v", msgs)
	}
	// Refused: the term is not ahead.
	mustStep(t, r, Message{Type: MsgPreVote, From: 2, To: 1, Term: 5, LastLogIndex: 2, LastLogTerm: 5})
	if msgs := tr.take(); len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("same term: %+v", msgs)
	}
	if r.term != 5 {
		t.Fatalf("term %d", r.term)
	}
}

func TestPreVoteRefusalWithHigherTermStepsDown(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 5, 1, 5)
	r.cfg.PreVote = true
	if err := r.preCampaign(); err != nil {
		t.Fatal(err)
	}
	msgs := tr.take()
	if len(msgs) != 2 || msgs[0].Type != MsgPreVote || msgs[0].Term != 6 || r.term != 5 {
		t.Fatalf("pre-campaign sent %+v, term %d", msgs, r.term)
	}
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 9})
	if r.term != 9 || r.preVotes != nil {
		t.Fatalf("term %d preVotes %v", r.term, r.preVotes)
	}
	// A late grant for the old pre-vote term is ignored.
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 3, To: 1, Term: 6, VoteGranted: true})
	if r.state != StateFollower || r.term != 9 {
		t.Fatalf("state %v term %d", r.state, r.term)
	}
	// Two grants for term+1 start the real election.
	if err := r.preCampaign(); err != nil {
		t.Fatal(err)
	}
	tr.take()
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 10, VoteGranted: true})
	if r.state != StateCandidate || r.term != 10 || r.votedFor != 1 {
		t.Fatalf("state %v term %d vote %d", r.state, r.term, r.votedFor)
	}
}
