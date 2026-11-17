package raft

import (
	"errors"
	"testing"
)

// newTestRaft builds a single raft instance whose log holds entries with the
// given terms, for unit tests that drive messages by hand.
func newTestRaft(t *testing.T, id uint64, peers []uint64, term uint64, logTerms ...uint64) (*raft, *sinkTransport, *memLog, *memStable) {
	t.Helper()
	log := &memLog{}
	for i, lt := range logTerms {
		log.Append(encodeEntry(Entry{Index: uint64(i + 1), Term: lt, Type: EntryNormal}))
	}
	st := &memStable{hs: HardState{Term: term}}
	tr := &sinkTransport{}
	r, err := newRaft(Config{ID: id, Peers: peers, Seed: 1}, log, st, tr, &recApplier{})
	if err != nil {
		t.Fatal(err)
	}
	return r, tr, log, st
}

func logTerms(t *testing.T, r *raft) []uint64 {
	t.Helper()
	var out []uint64
	for i := uint64(1); i <= r.lastIndex; i++ {
		tm, err := r.termAt(i)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, tm)
	}
	return out
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustStep(t *testing.T, r *raft, m Message) {
	t.Helper()
	if err := r.step(m); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{ID: 0, Peers: []uint64{1}},
		{ID: 1, Peers: []uint64{2, 3}},
		{ID: 1, Peers: []uint64{1, 1}},
		{ID: 1, Peers: []uint64{1, 0}},
		{ID: 1, Peers: []uint64{1}, ElectionTicks: 3, HeartbeatTicks: 3},
	}
	for i, c := range bad {
		if err := c.setDefaults(); err == nil {
			t.Errorf("config %d: expected error", i)
		}
	}
}

func TestRestoreFromStorage(t *testing.T) {
	r, _, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 5, 1, 1, 3)
	if r.term != 5 || r.lastIndex != 3 || r.lastTerm != 3 || r.state != StateFollower || r.commitIndex != 0 {
		t.Fatalf("restored %+v", r.status())
	}
}

func TestLogTermAboveCurrentTermIsCorruption(t *testing.T) {
	log := &memLog{}
	log.Append(encodeEntry(Entry{Index: 1, Term: 4, Type: EntryNormal}))
	_, err := newRaft(Config{ID: 1, Peers: []uint64{1}, Seed: 1}, log, &memStable{hs: HardState{Term: 2}}, &sinkTransport{}, &recApplier{})
	if !errors.Is(err, errSafety) {
		t.Fatalf("got %v, want errSafety", err)
	}
}

func TestSingleNodeCommitsImmediately(t *testing.T) {
	r, _, _, st := newTestRaft(t, 1, []uint64{1}, 0)
	for r.state != StateLeader {
		if err := r.tick(); err != nil {
			t.Fatal(err)
		}
	}
	if st.hs.Term != 1 || st.hs.VotedFor != 1 {
		t.Fatalf("hard state %+v", st.hs)
	}
	idx, err := r.propose([][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if idx != 2 || r.commitIndex != 3 || r.lastApplied != 3 {
		t.Fatalf("idx=%d status=%+v", idx, r.status())
	}
}

func TestProposeOnFollower(t *testing.T) {
	r, _, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 1)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 1})
	_, err := r.propose([][]byte{[]byte("x")})
	var nl *NotLeaderError
	if !errors.As(err, &nl) || nl.Leader != 2 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

// §5.2: one vote per term, persisted before the reply.
func TestVoteOncePerTerm(t *testing.T) {
	r, tr, log, st := newTestRaft(t, 1, []uint64{1, 2, 3}, 1)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 2})
	if got := tr.take(); len(got) != 1 || !got[0].VoteGranted {
		t.Fatalf("vote for 2: %+v", got)
	}
	if st.hs != (HardState{Term: 2, VotedFor: 2}) {
		t.Fatalf("hard state %+v", st.hs)
	}
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 2})
	if got := tr.take(); len(got) != 1 || got[0].VoteGranted {
		t.Fatalf("second vote in term 2 granted: %+v", got)
	}
	// Duplicate request from the same candidate is granted again.
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 2})
	if got := tr.take(); len(got) != 1 || !got[0].VoteGranted {
		t.Fatalf("repeat vote for 2: %+v", got)
	}

	// After a restart the vote is still remembered.
	r2, err := newRaft(Config{ID: 1, Peers: []uint64{1, 2, 3}, Seed: 2}, log, st, tr, &recApplier{})
	if err != nil {
		t.Fatal(err)
	}
	mustStep(t, r2, Message{Type: MsgVote, From: 3, To: 1, Term: 2})
	if got := tr.take(); len(got) != 1 || got[0].VoteGranted {
		t.Fatalf("vote after restart granted: %+v", got)
	}
}

// §5.4.1: deny votes to candidates whose log is less up to date.
func TestVoteElectionRestriction(t *testing.T) {
	cases := []struct {
		name               string
		lastIndex, lastTrm uint64
		grant              bool
	}{
		{"older last term", 5, 1, false},
		{"same term shorter log", 2, 2, false},
		{"same term same length", 3, 2, true},
		{"same term longer log", 4, 2, true},
		{"newer last term shorter log", 1, 3, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 2, 1, 2, 2)
			mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 3, LastLogIndex: tc.lastIndex, LastLogTerm: tc.lastTrm})
			got := tr.take()
			if len(got) != 1 || got[0].VoteGranted != tc.grant || got[0].Term != 3 {
				t.Fatalf("got %+v, want granted=%v", got, tc.grant)
			}
		})
	}
}

func TestStaleTermIsRejected(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 5, 1)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 4, LastLogIndex: 10, LastLogTerm: 4})
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 4, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 4, Type: EntryNormal}}})
	got := tr.take()
	if len(got) != 2 || got[0].VoteGranted || got[1].Success || got[0].Term != 5 || got[1].Term != 5 {
		t.Fatalf("got %+v", got)
	}
	if r.lastIndex != 1 || r.term != 5 {
		t.Fatalf("state changed: %+v", r.status())
	}
}

func TestHigherTermStepsDownLeader(t *testing.T) {
	r, tr, _, st := newTestRaft(t, 1, []uint64{1, 2, 3}, 0)
	for r.state != StateCandidate {
		r.tick()
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if r.state != StateLeader {
		t.Fatalf("not leader: %+v", r.status())
	}
	tr.take()
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 7})
	if r.state != StateFollower || r.term != 7 || st.hs != (HardState{Term: 7}) {
		t.Fatalf("status %+v hs %+v", r.status(), st.hs)
	}
}

// Figure 2, AppendEntries receiver steps 2–4.
func TestAppendEntriesConsistencyCheck(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 3, 1, 1, 2)

	// Missing prevLogIndex → reject with our last index as hint.
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevLogIndex: 5, PrevLogTerm: 3})
	got := tr.take()
	if len(got) != 1 || got[0].Success || got[0].LastLogIndex != 3 || got[0].PrevLogIndex != 5 {
		t.Fatalf("missing prev: %+v", got)
	}
	// Term mismatch at prevLogIndex → reject, log unchanged.
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevLogIndex: 3, PrevLogTerm: 3})
	if got := tr.take(); len(got) != 1 || got[0].Success {
		t.Fatalf("term mismatch: %+v", got)
	}
	if !equalU64(logTerms(t, r), []uint64{1, 1, 2}) {
		t.Fatalf("log changed: %v", logTerms(t, r))
	}
}

func TestAppendEntriesConflictTruncates(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 3, 1, 1, 2, 2)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 3, Type: EntryNormal}}})
	got := tr.take()
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 3 {
		t.Fatalf("got %+v", got)
	}
	if !equalU64(logTerms(t, r), []uint64{1, 1, 3}) {
		t.Fatalf("log %v, want [1 1 3]", logTerms(t, r))
	}
}

// A delayed AppendEntries whose entries we already have must not cut off
// later entries.
func TestStaleAppendDoesNotTruncate(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 1, 1, 1, 1, 1)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 1, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 1, Type: EntryNormal}}})
	got := tr.take()
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 2 {
		t.Fatalf("got %+v", got)
	}
	if r.lastIndex != 4 {
		t.Fatalf("log truncated to %d entries", r.lastIndex)
	}
}

// Figure 2, step 5: commitIndex = min(leaderCommit, index of last new entry).
func TestFollowerCommitLimitedByLastNewEntry(t *testing.T) {
	r, _, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 2, 1, 1, 1, 1)
	// The leader has committed 10 but this message only proves entries up to 2.
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 1, Type: EntryNormal}}, LeaderCommit: 10})
	if r.commitIndex != 2 || r.lastApplied != 2 {
		t.Fatalf("status %+v", r.status())
	}
	// A later, older message must not move commitIndex back.
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 1})
	if r.commitIndex != 2 {
		t.Fatalf("commit index went back to %d", r.commitIndex)
	}
}

// Figure 8 / §5.4.2: a leader must not commit an entry from an earlier term
// by counting replicas; only once an entry of its own term is on a majority.
func TestLeaderDoesNotCommitOldTermByCounting(t *testing.T) {
	peers := []uint64{1, 2, 3, 4, 5}
	r, tr, _, _ := newTestRaft(t, 1, peers, 3, 1, 2)
	for r.state != StateCandidate {
		r.tick()
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 4, VoteGranted: true})
	mustStep(t, r, Message{Type: MsgVoteResp, From: 3, To: 1, Term: 4, VoteGranted: true})
	if r.state != StateLeader || r.lastIndex != 3 {
		t.Fatalf("status %+v", r.status())
	}
	tr.take()

	// Index 2 (term 2) is now on 3 of 5 nodes, but it must not be committed.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Success: true, MatchIndex: 2})
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 4, Success: true, MatchIndex: 2})
	if r.commitIndex != 0 {
		t.Fatalf("committed %d from an older term by counting replicas", r.commitIndex)
	}
	// Once the term-4 no-op is on a majority, everything up to it commits.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Success: true, MatchIndex: 3})
	if r.commitIndex != 0 {
		t.Fatalf("committed with only 2 of 5 replicas")
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 4, Success: true, MatchIndex: 3})
	if r.commitIndex != 3 || r.lastApplied != 3 {
		t.Fatalf("status %+v", r.status())
	}
}

func TestLeaderBacksUpNextIndex(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 1, 1, 1, 1, 1, 1)
	for r.state != StateCandidate {
		r.tick()
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 2, VoteGranted: true})
	tr.take()
	if r.next[3] != 6 {
		t.Fatalf("next[3] = %d", r.next[3])
	}
	// Follower 3 only has 2 entries: skip straight to index 3.
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, PrevLogIndex: 5, LastLogIndex: 2})
	if r.next[3] != 3 {
		t.Fatalf("next[3] = %d, want 3", r.next[3])
	}
	got := tr.take()
	if len(got) != 1 || got[0].PrevLogIndex != 2 || len(got[0].Entries) != 4 {
		t.Fatalf("retry: %+v", got)
	}
	// A duplicate of the old rejection is ignored.
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, PrevLogIndex: 5, LastLogIndex: 2})
	if r.next[3] != 3 || len(tr.take()) != 0 {
		t.Fatalf("stale rejection changed state: next=%d", r.next[3])
	}
	// Follower 3 has a long conflicting log: decrement by one.
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, PrevLogIndex: 2, LastLogIndex: 9})
	if r.next[3] != 2 {
		t.Fatalf("next[3] = %d, want 2", r.next[3])
	}
}

func TestTwoLeadersInOneTermIsFatal(t *testing.T) {
	r, _, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 0)
	for r.state != StateCandidate {
		r.tick()
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	err := r.step(Message{Type: MsgApp, From: 3, To: 1, Term: 1})
	if !errors.Is(err, errSafety) {
		t.Fatalf("got %v, want errSafety", err)
	}
}
