package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// --- in-memory stores -----------------------------------------------------------

// memLog is an in-memory LogStore. It survives a simulated crash because the
// cluster keeps it while the raft instance is thrown away. Like wal.Log it
// compacts in whole "segments" of memSegment entries, so FirstIndex can stay
// below the requested index.
type memLog struct {
	first   uint64 // 0 means 1
	entries [][]byte
}

const memSegment = 8

func (l *memLog) FirstIndex() uint64 {
	if l.first == 0 {
		return 1
	}
	return l.first
}

func (l *memLog) LastIndex() uint64 { return l.FirstIndex() + uint64(len(l.entries)) - 1 }

func (l *memLog) Append(entries ...[]byte) (uint64, error) {
	for _, e := range entries {
		l.entries = append(l.entries, slices.Clone(e))
	}
	return l.LastIndex(), nil
}

func (l *memLog) Read(i uint64) ([]byte, error) {
	if i < l.FirstIndex() || i > l.LastIndex() {
		return nil, fmt.Errorf("memLog: index %d out of range [%d, %d]", i, l.FirstIndex(), l.LastIndex())
	}
	return slices.Clone(l.entries[i-l.FirstIndex()]), nil
}

func (l *memLog) TruncateAfter(i uint64) error {
	if i+1 < l.FirstIndex() {
		return fmt.Errorf("memLog: truncate after %d below first %d", i, l.FirstIndex())
	}
	if i < l.LastIndex() {
		l.entries = l.entries[:i+1-l.FirstIndex()]
	}
	return nil
}

func (l *memLog) CompactBefore(i uint64) error {
	newFirst := (i-1)/memSegment*memSegment + 1
	if last := l.LastIndex(); newFirst > last && last >= l.FirstIndex() {
		newFirst = last // like wal, never drop the active segment entirely
	}
	if newFirst > l.FirstIndex() {
		l.entries = l.entries[newFirst-l.FirstIndex():]
		l.first = newFirst
	}
	return nil
}

func (l *memLog) Reset(next uint64) error {
	l.first, l.entries = next, nil
	return nil
}

type memStable struct{ hs HardState }

func (s *memStable) Load() (HardState, error) { return s.hs, nil }
func (s *memStable) Save(hs HardState) error  { s.hs = hs; return nil }

type memSnaps struct {
	metas []SnapshotMeta
	data  map[uint64][]byte
}

func (s *memSnaps) List() ([]SnapshotMeta, error) { return slices.Clone(s.metas), nil }

func (s *memSnaps) Load(i uint64) ([]byte, error) {
	d, ok := s.data[i]
	if !ok {
		return nil, fmt.Errorf("memSnaps: no snapshot %d", i)
	}
	return slices.Clone(d), nil
}

func (s *memSnaps) Save(m SnapshotMeta, data []byte) error {
	if s.data == nil {
		s.data = map[uint64][]byte{}
	}
	s.metas = slices.DeleteFunc(s.metas, func(x SnapshotMeta) bool { return x.Index == m.Index })
	s.metas = append(s.metas, m)
	slices.SortFunc(s.metas, func(a, b SnapshotMeta) int { return compareU64(a.Index, b.Index) })
	s.data[m.Index] = slices.Clone(data)
	return nil
}

func (s *memSnaps) DeleteBefore(i uint64) error {
	s.metas = slices.DeleteFunc(s.metas, func(x SnapshotMeta) bool {
		if x.Index < i {
			delete(s.data, x.Index)
			return true
		}
		return false
	})
	return nil
}

// recApplier records applied entries. Its "state" is the whole list of
// applied entries, so snapshots carry the full history and the state machine
// safety check keeps working across restores.
type recApplier struct{ entries []Entry }

func (a *recApplier) Apply(e Entry) error {
	if want := uint64(len(a.entries)) + 1; e.Index != want {
		return fmt.Errorf("applied index %d, want %d", e.Index, want)
	}
	e.Data = slices.Clone(e.Data)
	a.entries = append(a.entries, e)
	return nil
}

func (a *recApplier) Snapshot() ([]byte, error) {
	var b []byte
	for _, e := range a.entries {
		enc := encodeEntry(e)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(enc)))
		b = append(b, enc...)
	}
	return b, nil
}

func (a *recApplier) Restore(b []byte) error {
	a.entries = nil
	for len(b) > 0 {
		n := binary.LittleEndian.Uint32(b)
		e, err := decodeEntry(uint64(len(a.entries))+1, b[4:4+n])
		if err != nil {
			return err
		}
		e.Data = slices.Clone(e.Data)
		a.entries = append(a.entries, e)
		b = b[4+n:]
	}
	return nil
}

// sinkTransport collects sent messages (for single-node unit tests).
type sinkTransport struct{ msgs []Message }

func (t *sinkTransport) Send(m Message) { t.msgs = append(t.msgs, m) }

func (t *sinkTransport) take() []Message {
	m := t.msgs
	t.msgs = nil
	return m
}

// --- deterministic cluster ------------------------------------------------------

// testNet is a fake network. Messages sent in one round are delivered in the
// next one (or later, or never, depending on the fault settings).
type testNet struct {
	rng     *rand.Rand
	queue   []Message
	delayed []Message

	dropRate  float64 // probability a message is lost
	dupRate   float64 // probability a message is delivered twice
	delayRate float64 // probability a message is held back one more round
	reorder   bool    // shuffle the messages of each round

	// group[id] is the partition a node is in; nodes in different groups
	// cannot talk. Empty map means no partition.
	group map[uint64]int

	delivered map[MessageType]int // messages handed to a node, by type
}

func (n *testNet) Send(m Message) { n.queue = append(n.queue, m) }

func (n *testNet) connected(a, b uint64) bool {
	return len(n.group) == 0 || n.group[a] == n.group[b]
}

type cluster struct {
	t       testing.TB
	ids     []uint64
	cfg     Config
	net     *testNet
	rafts   map[uint64]*raft // nil while the node is down
	logs    map[uint64]*memLog
	stables map[uint64]*memStable
	snaps   map[uint64]*memSnaps
	apps    map[uint64]*recApplier
	joined  map[uint64]bool // started with Join
	round   int

	// For invariant checks.
	leaderOf  map[uint64]uint64 // term → leader
	committed []Entry           // longest sequence applied by any node
	// commitTerm[i] is an upper bound of the term in which committed[i] was
	// committed: the current term of the first node seen applying it. A
	// node only applies an entry after it hears of the commit from a leader
	// whose term is at least the commit term, so the bound is safe.
	commitTerm []uint64
}

func newCluster(t testing.TB, size int, seed uint64) *cluster {
	t.Helper()
	return newClusterCfg(t, size, seed, nil)
}

// newClusterCfg is newCluster with a hook to change the node config.
func newClusterCfg(t testing.TB, size int, seed uint64, tweak func(*Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		net:      &testNet{rng: rand.New(rand.NewPCG(seed, 0xc0ffee))},
		rafts:    map[uint64]*raft{},
		logs:     map[uint64]*memLog{},
		stables:  map[uint64]*memStable{},
		snaps:    map[uint64]*memSnaps{},
		apps:     map[uint64]*recApplier{},
		leaderOf: map[uint64]uint64{},
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	c.cfg = Config{Peers: c.ids, ElectionTicks: 10, HeartbeatTicks: 1, MaxEntriesPerMsg: 8, Seed: seed}
	if tweak != nil {
		tweak(&c.cfg)
	}
	for _, id := range c.ids {
		c.logs[id] = &memLog{}
		c.stables[id] = &memStable{}
		c.snaps[id] = &memSnaps{}
		c.start(id)
	}
	return c
}

// start boots node id from its persistent stores, with empty volatile state.
func (c *cluster) start(id uint64) {
	c.t.Helper()
	cfg := c.cfg
	cfg.ID = id
	cfg.Seed = c.cfg.Seed*1_000_003 + uint64(c.round) + 1 // non-zero, differs per restart
	cfg.Join = c.joined[id]
	c.apps[id] = &recApplier{}
	r, err := newRaft(cfg, c.storage(id), c.net, c.apps[id])
	if err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	c.rafts[id] = r
}

// addNode starts a new, empty node that joins the cluster (Config.Join).
func (c *cluster) addNode(id uint64) {
	c.t.Helper()
	if c.joined == nil {
		c.joined = map[uint64]bool{}
	}
	c.ids = append(c.ids, id)
	c.joined[id] = true
	c.logs[id], c.stables[id], c.snaps[id] = &memLog{}, &memStable{}, &memSnaps{}
	c.start(id)
}

func (c *cluster) storage(id uint64) Storage {
	return Storage{Log: c.logs[id], Stable: c.stables[id], Snapshots: c.snaps[id]}
}

// crash stops node id, losing all volatile state.
func (c *cluster) crash(id uint64) { c.rafts[id] = nil }

func (c *cluster) up(id uint64) bool { return c.rafts[id] != nil }

// partition splits the nodes into the given groups.
func (c *cluster) partition(groups ...[]uint64) {
	c.net.group = map[uint64]int{}
	for g, ids := range groups {
		for _, id := range ids {
			c.net.group[id] = g + 1
		}
	}
}

func (c *cluster) heal() { c.net.group = nil }

func (c *cluster) fatal(id uint64, err error) {
	c.t.Helper()
	c.t.Fatalf("round %d: node %d: %v", c.round, id, err)
}

// step runs one round: every live node ticks once, then the messages sent in
// the previous round are delivered.
func (c *cluster) step() {
	c.t.Helper()
	c.round++
	for _, id := range c.ids {
		if r := c.rafts[id]; r != nil {
			if err := r.tick(); err != nil {
				c.fatal(id, err)
			}
		}
	}
	n := c.net
	msgs := append(n.delayed, n.queue...)
	n.queue, n.delayed = nil, nil
	if n.reorder {
		n.rng.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	}
	for _, m := range msgs {
		if n.delayRate > 0 && n.rng.Float64() < n.delayRate {
			n.delayed = append(n.delayed, m)
			continue
		}
		copies := 1
		if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
			copies = 0
		} else if n.dupRate > 0 && n.rng.Float64() < n.dupRate {
			copies = 2
		}
		for range copies {
			r := c.rafts[m.To]
			if r == nil || !n.connected(m.From, m.To) {
				break
			}
			if n.delivered == nil {
				n.delivered = map[MessageType]int{}
			}
			n.delivered[m.Type]++
			if err := r.step(m); err != nil {
				c.fatal(m.To, err)
			}
		}
	}
	c.check()
}

func (c *cluster) run(rounds int) {
	c.t.Helper()
	for range rounds {
		c.step()
	}
}

// runUntil steps until cond holds, failing after max rounds.
func (c *cluster) runUntil(max int, what string, cond func() bool) {
	c.t.Helper()
	for range max {
		if cond() {
			return
		}
		c.step()
	}
	if !cond() {
		c.t.Fatalf("after %d rounds: %s did not happen\n%s", max, what, c.dump())
	}
}

// leader returns the live leader with the highest term, or 0.
func (c *cluster) leader() uint64 {
	var best, term uint64
	for _, id := range c.ids {
		if r := c.rafts[id]; r != nil && r.state == StateLeader && r.term >= term {
			best, term = id, r.term
		}
	}
	return best
}

// stableLeader returns a leader that every live member of its configuration
// in its partition agrees on.
func (c *cluster) stableLeader() uint64 {
	l := c.leader()
	if l == 0 {
		return 0
	}
	for _, id := range c.ids {
		r := c.rafts[id]
		if _, member := c.rafts[l].conf.Member(id); !member {
			continue
		}
		if r != nil && c.net.connected(id, l) && (r.leader != l || r.term != c.rafts[l].term) {
			return 0
		}
	}
	return l
}

func (c *cluster) waitLeader() uint64 {
	c.t.Helper()
	c.runUntil(500, "leader election", func() bool { return c.stableLeader() != 0 })
	return c.stableLeader()
}

// propose submits data on node id and returns the index, or 0 if id is not
// the leader.
func (c *cluster) propose(id uint64, data string) uint64 {
	c.t.Helper()
	r := c.rafts[id]
	if r == nil || r.state != StateLeader {
		return 0
	}
	idx, err := r.propose([][]byte{[]byte(data)})
	if err != nil {
		c.fatal(id, err)
	}
	return idx
}

// waitApplied runs until every live node has applied at least index; with
// a leader, every live member of its configuration (a removed node no longer
// gets entries).
func (c *cluster) waitApplied(index uint64) {
	c.t.Helper()
	c.runUntil(1000, fmt.Sprintf("apply of index %d on all live nodes", index), func() bool {
		l := c.leader()
		for _, id := range c.ids {
			if l != 0 {
				if _, member := c.rafts[l].conf.Member(id); !member {
					continue
				}
			}
			if r := c.rafts[id]; r != nil && r.lastApplied < index {
				return false
			}
		}
		return true
	})
}

func (c *cluster) dump() string {
	var b bytes.Buffer
	for _, id := range c.ids {
		r := c.rafts[id]
		if r == nil {
			fmt.Fprintf(&b, "  node %d: down\n", id)
			continue
		}
		fmt.Fprintf(&b, "  node %d: %+v\n", id, r.status())
	}
	return b.String()
}

// --- invariants (Figure 3) -----------------------------------------------------

func (c *cluster) check() {
	c.t.Helper()
	c.checkElectionSafety()
	c.checkStateMachineSafety()
	c.checkLeaderCompleteness()
	if c.round%25 == 0 {
		c.checkLogMatching()
	}
}

// Election Safety: at most one leader per term.
func (c *cluster) checkElectionSafety() {
	c.t.Helper()
	for _, id := range c.ids {
		r := c.rafts[id]
		if r == nil || r.state != StateLeader {
			continue
		}
		if prev, ok := c.leaderOf[r.term]; ok && prev != id {
			c.t.Fatalf("round %d: election safety: nodes %d and %d both leader in term %d", c.round, prev, id, r.term)
		}
		c.leaderOf[r.term] = id
	}
}

func sameEntry(a, b Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && bytes.Equal(a.Data, b.Data)
}

// State Machine Safety: no two nodes apply different entries at an index.
// Every applied sequence must be a prefix of the longest one seen so far.
func (c *cluster) checkStateMachineSafety() {
	c.t.Helper()
	for _, id := range c.ids {
		applied := c.apps[id].entries
		for i, e := range applied {
			if i < len(c.committed) {
				if !sameEntry(e, c.committed[i]) {
					c.t.Fatalf("round %d: state machine safety: node %d applied %+v at %d, others applied %+v",
						c.round, id, e, e.Index, c.committed[i])
				}
				continue
			}
			c.committed = append(c.committed, e)
			c.commitTerm = append(c.commitTerm, c.rafts[id].term)
		}
	}
}

// Leader Completeness: an entry committed in term T is in the log of every
// leader of a term >= T. (T is the term of the commit, not of the entry: a
// stale leader of term 26 need not hold a term-23 entry committed in term 27.)
func (c *cluster) checkLeaderCompleteness() {
	c.t.Helper()
	for _, id := range c.ids {
		r := c.rafts[id]
		if r == nil || r.state != StateLeader {
			continue
		}
		for i, e := range c.committed {
			if c.commitTerm[i] > r.term {
				continue
			}
			if e.Index > r.lastIndex {
				c.t.Fatalf("round %d: leader completeness: leader %d (term %d) lacks committed index %d", c.round, id, r.term, e.Index)
			}
			t, err := r.termAt(e.Index)
			if errors.Is(err, errCompacted) {
				continue // covered by a snapshot, which holds committed state
			}
			if err != nil {
				c.fatal(id, err)
			}
			if t != e.Term {
				c.t.Fatalf("round %d: leader completeness: leader %d (term %d) has term %d at committed index %d (want %d)",
					c.round, id, r.term, t, e.Index, e.Term)
			}
		}
	}
}

// Log Matching: if two logs have an entry with the same index and term, the
// logs are identical up to that index (checked over the part both still hold).
func (c *cluster) checkLogMatching() {
	c.t.Helper()
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			la, lb := c.logs[a], c.logs[b]
			lo := max(la.FirstIndex(), lb.FirstIndex())
			hi := min(la.LastIndex(), lb.LastIndex())
			// Find the highest index where the terms agree; everything before
			// it must be byte-identical.
			match := uint64(0)
			for k := hi; k >= lo && k > 0; k-- {
				ea, _ := la.Read(k)
				eb, _ := lb.Read(k)
				if bytes.Equal(ea[:8], eb[:8]) {
					match = k
					break
				}
			}
			for k := lo; match > 0 && k <= match; k++ {
				ea, _ := la.Read(k)
				eb, _ := lb.Read(k)
				if !bytes.Equal(ea, eb) {
					c.t.Fatalf("round %d: log matching: nodes %d and %d agree on term at index %d but differ at index %d",
						c.round, a, b, match, k)
				}
			}
		}
	}
}
