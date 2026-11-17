package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSnapshotStore(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSnapshotStore(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if l, err := s.List(); err != nil || len(l) != 0 {
		t.Fatalf("empty list: %v %v", l, err)
	}
	for _, m := range []SnapshotMeta{{30, 2}, {10, 1}, {20, 2}} {
		if err := s.Save(m, []byte(fmt.Sprint("state@", m.Index))); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(dir, "00000000000000000040-00000000000000000003.snap.tmp"), []byte("torn"), 0o644)
	l, _ := s.List()
	if fmt.Sprint(l) != "[{10 1} {20 2} {30 2}]" {
		t.Fatalf("list: %v", l)
	}
	if d, err := s.Load(20); err != nil || string(d) != "state@20" {
		t.Fatalf("load: %q %v", d, err)
	}
	if err := s.DeleteBefore(30); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.List(); fmt.Sprint(l) != "[{30 2}]" {
		t.Fatalf("after delete: %v", l)
	}
	if _, err := s.Load(20); err == nil {
		t.Fatal("load of deleted snapshot succeeded")
	}

	// Flip a byte: the checksum catches it.
	p := filepath.Join(dir, "00000000000000000030-00000000000000000002.snap")
	b, _ := os.ReadFile(p)
	b[0] ^= 1
	os.WriteFile(p, b, 0o644)
	if _, err := s.Load(30); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("corrupt load: %v", err)
	}
}

func snapCfg(every, trailing int) func(*Config) {
	return func(c *Config) { c.SnapshotEntries, c.TrailingEntries = every, trailing }
}

func TestSnapshotRestoreOnRestart(t *testing.T) {
	c := newClusterCfg(t, 3, 21, snapCfg(10, 0))
	l := c.waitLeader()
	var last uint64
	for i := range 35 {
		last = c.propose(l, fmt.Sprintf("v%d", i))
	}
	c.waitApplied(last)
	for _, id := range c.ids {
		metas := c.snaps[id].metas
		if len(metas) != 1 || metas[0].Index < 30 {
			t.Fatalf("node %d snapshots %v; want only the latest", id, metas)
		}
		if c.logs[id].FirstIndex() != 1 {
			t.Fatalf("node %d log compacted without TrailingEntries", id)
		}
	}
	f := c.ids[0]
	if f == l {
		f = c.ids[1]
	}
	snapIdx := c.snaps[f].metas[0].Index
	c.crash(f)
	c.start(f)
	if r := c.rafts[f]; r.lastApplied != snapIdx || r.commitIndex != snapIdx || uint64(len(c.apps[f].entries)) != snapIdx {
		t.Fatalf("after restart: %+v, applier has %d entries; want both at %d", r.status(), len(c.apps[f].entries), snapIdx)
	}
	c.waitApplied(last)
}

// A follower that is behind the start of the leader's log gets the base
// snapshot, then the retained log after it (§7, design D5).
func TestLaggingFollowerInstallsSnapshot(t *testing.T) {
	c := newClusterCfg(t, 3, 22, snapCfg(10, 16))
	l := c.waitLeader()
	f := c.ids[0]
	if f == l {
		f = c.ids[1]
	}
	c.crash(f)
	var last uint64
	for i := range 200 {
		last = c.propose(l, fmt.Sprintf("v%d", i))
		if i%10 == 0 {
			c.step()
		}
	}
	c.waitApplied(last)
	if c.logs[l].FirstIndex() <= 1 {
		t.Fatalf("leader log not compacted: first=%d", c.logs[l].FirstIndex())
	}
	base, ok := c.rafts[l].baseSnapshot()
	if !ok {
		t.Fatal("leader has no base snapshot")
	}
	c.start(f)
	c.waitApplied(last)
	if got := c.logs[f].FirstIndex(); got > base.Index+1 {
		t.Fatalf("follower log starts at %d, after the leader's base snapshot %d", got, base.Index)
	}
	c.checkLogMatching()
}

// A crash between saving an installed snapshot and resetting the log leaves a
// log that does not continue the snapshot; restart must finish the install.
func TestInterruptedSnapshotInstall(t *testing.T) {
	for _, tc := range []struct {
		name     string
		logTerms []uint64
	}{
		{"conflicting entry at snapshot index", []uint64{1, 1, 2, 2, 2, 2, 2}},
		{"log shorter than snapshot", []uint64{1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &memLog{}
			for i, lt := range tc.logTerms {
				log.Append(encodeEntry(Entry{Index: uint64(i + 1), Term: lt, Type: EntryNormal}))
			}
			src := &recApplier{}
			for i := uint64(1); i <= 5; i++ {
				src.Apply(Entry{Index: i, Term: 3, Type: EntryNormal, Data: []byte{byte(i)}})
			}
			data, _ := src.Snapshot()
			snaps := &memSnaps{}
			snaps.Save(SnapshotMeta{Index: 5, Term: 3}, data)

			app := &recApplier{}
			r, err := newRaft(Config{ID: 1, Peers: []uint64{1, 2, 3}, Seed: 1},
				Storage{Log: log, Stable: &memStable{hs: HardState{Term: 3}}, Snapshots: snaps}, &sinkTransport{}, app)
			if err != nil {
				t.Fatal(err)
			}
			if r.lastIndex != 5 || r.lastTerm != 3 || r.lastApplied != 5 || log.FirstIndex() != 6 || len(app.entries) != 5 {
				t.Fatalf("status %+v first %d applied %d", r.status(), log.FirstIndex(), len(app.entries))
			}
		})
	}
}

func TestSnapshotWithMatchingEntryKeepsLog(t *testing.T) {
	r, tr, log, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 2, 1, 1, 2, 2, 2)
	mustStep(t, r, Message{Type: MsgSnap, From: 2, To: 1, Term: 2,
		Snapshot: &Snapshot{Meta: SnapshotMeta{Index: 3, Term: 2}, Data: nil}})
	got := tr.take()
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 3 {
		t.Fatalf("reply %+v", got)
	}
	if log.FirstIndex() != 1 || r.lastIndex != 5 || r.commitIndex != 3 || r.lastApplied != 3 {
		t.Fatalf("status %+v first %d", r.status(), log.FirstIndex())
	}
}

func TestStaleSnapshotIsAcknowledged(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 2, 1, 1, 2)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 3})
	tr.take()
	mustStep(t, r, Message{Type: MsgSnap, From: 2, To: 1, Term: 2,
		Snapshot: &Snapshot{Meta: SnapshotMeta{Index: 2, Term: 1}}})
	got := tr.take()
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 3 || r.lastIndex != 3 {
		t.Fatalf("reply %+v status %+v", got, r.status())
	}
}

// An AppendEntries whose prevLogIndex lies inside the follower's snapshot is
// answered with the follower's commit index.
func TestAppendIntoCompactedRegion(t *testing.T) {
	r, tr, log, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 2)
	mustStep(t, r, Message{Type: MsgSnap, From: 2, To: 1, Term: 2,
		Snapshot: &Snapshot{Meta: SnapshotMeta{Index: 20, Term: 2}}})
	tr.take()
	if log.FirstIndex() != 21 || r.commitIndex != 20 {
		t.Fatalf("after install: first %d %+v", log.FirstIndex(), r.status())
	}
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 10, PrevLogTerm: 1,
		Entries: []Entry{{Index: 11, Term: 1, Type: EntryNormal}}})
	got := tr.take()
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 20 || r.lastIndex != 20 {
		t.Fatalf("reply %+v status %+v", got, r.status())
	}
}

// §8: a read is served only after a majority acknowledged the leader after
// the request, and after the leader committed an entry of its own term.
func TestReadIndexNeedsMajorityAndCurrentTermCommit(t *testing.T) {
	r, tr, _, _ := newTestRaft(t, 1, []uint64{1, 2, 3}, 0)
	if _, err := r.requestRead(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower read: %v", err)
	}
	for r.state != StateCandidate {
		r.tick()
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	tr.take()

	id, err := r.requestRead()
	if err != nil {
		t.Fatal(err)
	}
	sent := tr.take()
	if len(sent) != 2 || sent[0].ReadID != id {
		t.Fatalf("read heartbeats %+v", sent)
	}
	// A reply to an older heartbeat does not count.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 0, ReadID: id - 1})
	if ready, _ := r.readyReads(); len(ready) != 0 {
		t.Fatalf("ready without a majority: %v", ready)
	}
	// Acknowledged by a majority, but the no-op (index 1) is not committed:
	// a rejection still proves leadership, but match stays 0.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, PrevLogIndex: 0, LastLogIndex: 0, ReadID: id})
	if ready, _ := r.readyReads(); len(ready) != 0 {
		t.Fatalf("ready before the current term committed: %v", ready)
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 1, Success: true, MatchIndex: 1, ReadID: id})
	ready, err := r.readyReads()
	if err != nil || len(ready) != 1 || ready[0] != id || r.commitIndex != 1 {
		t.Fatalf("ready %v err %v status %+v", ready, err, r.status())
	}
	// Losing leadership drops pending reads.
	r.requestRead()
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 5})
	if len(r.reads) != 0 {
		t.Fatal("reads survived step-down")
	}
}

func TestNodeReadIndex(t *testing.T) {
	c := newLiveCluster(t, 3)
	idx := c.propose("x")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range c.ids {
		got, err := c.nodes[id].node.ReadIndex(ctx)
		if errors.Is(err, ErrNotLeader) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if got < idx {
			t.Fatalf("read index %d below committed write %d", got, idx)
		}
		if n := len(c.nodes[id].app.data()); n < 1 {
			t.Fatalf("leader state machine does not reflect the read index")
		}
		return
	}
	t.Skip("no leader found (leadership moved)")
}

func TestNodeClusterCompactsAndRecovers(t *testing.T) {
	c := newLiveClusterCfg(t, 3, snapCfg(20, 30))
	var want []string
	for i := range 50 {
		d := fmt.Sprintf("a%d", i)
		c.propose(d)
		want = append(want, d)
	}
	c.stop(3)
	for i := range 150 {
		d := fmt.Sprintf("b%d", i)
		c.propose(d)
		want = append(want, d)
	}
	c.start(3)
	c.propose("c")
	want = append(want, "c")
	for _, id := range c.ids {
		got := c.waitApplied(id, len(want))
		if fmt.Sprint(dedupe(got)) != fmt.Sprint(want) {
			t.Fatalf("node %d applied %d values, want %d", id, len(dedupe(got)), len(want))
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	compacted := false
	for _, id := range c.ids {
		st, err := c.nodes[id].node.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		compacted = compacted || st.FirstIndex > 1
	}
	if !compacted {
		t.Fatal("no node compacted its log")
	}
}
