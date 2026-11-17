package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// StateType is the role of a node.
type StateType uint8

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return fmt.Sprintf("StateType(%d)", uint8(s))
	}
}

// Config configures a node. Times are counted in ticks; Node turns a tick
// into wall-clock time with TickInterval.
type Config struct {
	// ID of this node. Must be non-zero.
	ID uint64
	// Peers lists every member of the cluster, including ID (static
	// membership, design D8).
	Peers []uint64
	// ElectionTicks is the minimum election timeout. The actual timeout is
	// chosen at random from [ElectionTicks, 2*ElectionTicks). Default 10.
	ElectionTicks int
	// HeartbeatTicks is the interval between heartbeats from the leader.
	// Must be lower than ElectionTicks. Default 1.
	HeartbeatTicks int
	// TickInterval is the length of one tick (used by Node only).
	// Default 100ms.
	TickInterval time.Duration
	// MaxEntriesPerMsg limits the entries in one AppendEntries. Default 64.
	MaxEntriesPerMsg int
	// Seed seeds the election timeout randomness. 0 picks a random seed.
	Seed uint64
}

func (c *Config) setDefaults() error {
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 1
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = 64
	}
	if c.Seed == 0 {
		c.Seed = rand.Uint64()
	}
	if c.ID == 0 {
		return errors.New("raft: config: ID must be non-zero")
	}
	if c.HeartbeatTicks < 1 || c.ElectionTicks <= c.HeartbeatTicks {
		return errors.New("raft: config: need 1 <= HeartbeatTicks < ElectionTicks")
	}
	if c.TickInterval < 0 || c.MaxEntriesPerMsg < 1 {
		return errors.New("raft: config: invalid TickInterval or MaxEntriesPerMsg")
	}
	seen := map[uint64]bool{}
	for _, p := range c.Peers {
		if p == 0 || seen[p] {
			return fmt.Errorf("raft: config: invalid or duplicate peer %d", p)
		}
		seen[p] = true
	}
	if !seen[c.ID] {
		return fmt.Errorf("raft: config: Peers must contain ID %d", c.ID)
	}
	return nil
}

// Status is a snapshot of a node's Raft state.
type Status struct {
	ID          uint64
	Term        uint64
	State       StateType
	Leader      uint64 // 0 if unknown
	LastIndex   uint64
	CommitIndex uint64
	LastApplied uint64
}

// errSafety marks a broken Raft invariant. It means a bug or corrupted
// storage; the node must stop.
var errSafety = errors.New("raft: safety violation")

// raft is the protocol state machine of one node. It is not safe for
// concurrent use. Every error it returns is fatal for the node.
type raft struct {
	id     uint64
	peers  []uint64 // sorted, includes id
	cfg    Config
	log    LogStore
	stable StableStore
	tr     Transport
	app    Applier
	rng    *rand.Rand

	// Persistent state (Figure 2). The log itself lives in log.
	term     uint64
	votedFor uint64

	// Cached position of the last log entry.
	lastIndex uint64
	lastTerm  uint64

	// Volatile state on all servers.
	commitIndex uint64
	lastApplied uint64
	state       StateType
	leader      uint64

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	// Candidate state.
	votes map[uint64]bool

	// Leader state, reinitialized after election.
	next  map[uint64]uint64
	match map[uint64]uint64
}

// newRaft creates a follower from the persisted state in log and stable.
func newRaft(cfg Config, log LogStore, stable StableStore, tr Transport, app Applier) (*raft, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	hs, err := stable.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &raft{
		id:       cfg.ID,
		peers:    peers,
		cfg:      cfg,
		log:      log,
		stable:   stable,
		tr:       tr,
		app:      app,
		rng:      rand.New(rand.NewPCG(cfg.Seed, cfg.ID)),
		term:     hs.Term,
		votedFor: hs.VotedFor,
		state:    StateFollower,
		next:     map[uint64]uint64{},
		match:    map[uint64]uint64{},
	}
	r.lastIndex = log.LastIndex()
	if r.lastIndex > 0 {
		e, err := r.entryAt(r.lastIndex)
		if err != nil {
			return nil, err
		}
		r.lastTerm = e.Term
	}
	if r.lastTerm > r.term {
		return nil, fmt.Errorf("%w: last log term %d > current term %d", errSafety, r.lastTerm, r.term)
	}
	r.resetElectionTimer()
	return r, nil
}

func (r *raft) quorum() int { return len(r.peers)/2 + 1 }

func (r *raft) status() Status {
	return Status{
		ID:          r.id,
		Term:        r.term,
		State:       r.state,
		Leader:      r.leader,
		LastIndex:   r.lastIndex,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
	}
}

// --- log access -------------------------------------------------------------

func (r *raft) entryAt(i uint64) (Entry, error) {
	b, err := r.log.Read(i)
	if err != nil {
		return Entry{}, fmt.Errorf("raft: read entry %d: %w", i, err)
	}
	return decodeEntry(i, b)
}

func (r *raft) termAt(i uint64) (uint64, error) {
	switch {
	case i == 0:
		return 0, nil
	case i == r.lastIndex:
		return r.lastTerm, nil
	case i > r.lastIndex:
		return 0, fmt.Errorf("raft: term of index %d past last index %d", i, r.lastIndex)
	}
	e, err := r.entryAt(i)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (r *raft) appendLog(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	bufs := make([][]byte, len(entries))
	for i, e := range entries {
		if e.Index != r.lastIndex+1+uint64(i) {
			return fmt.Errorf("%w: append index %d after %d", errSafety, e.Index, r.lastIndex)
		}
		bufs[i] = encodeEntry(e)
	}
	last, err := r.log.Append(bufs...)
	if err != nil {
		return fmt.Errorf("raft: append: %w", err)
	}
	tail := entries[len(entries)-1]
	if last != tail.Index {
		return fmt.Errorf("%w: log store returned index %d, want %d", errSafety, last, tail.Index)
	}
	r.lastIndex, r.lastTerm = tail.Index, tail.Term
	return nil
}

// truncateAfter deletes every entry after index i. Committed entries must
// never be deleted (§5.4); doing so means a bug.
func (r *raft) truncateAfter(i uint64) error {
	if i < r.commitIndex {
		return fmt.Errorf("%w: truncate after %d below commit index %d", errSafety, i, r.commitIndex)
	}
	if err := r.log.TruncateAfter(i); err != nil {
		return fmt.Errorf("raft: truncate after %d: %w", i, err)
	}
	r.lastIndex = i
	t, err := r.termAt(i)
	if err != nil {
		return err
	}
	r.lastTerm = t
	return nil
}

func (r *raft) persist() error {
	if err := r.stable.Save(HardState{Term: r.term, VotedFor: r.votedFor}); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	return nil
}

// --- roles --------------------------------------------------------------------

func (r *raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.electionTimeout = r.cfg.ElectionTicks + r.rng.IntN(r.cfg.ElectionTicks)
}

// becomeFollower moves to follower state. A higher term is persisted (and the
// vote cleared) before anything else happens.
func (r *raft) becomeFollower(term, leader uint64) error {
	if term > r.term {
		r.term, r.votedFor = term, 0
		if err := r.persist(); err != nil {
			return err
		}
	}
	r.state = StateFollower
	r.leader = leader
	r.votes = nil
	r.resetElectionTimer()
	return nil
}

// campaign starts an election (§5.2).
func (r *raft) campaign() error {
	r.term++
	r.votedFor = r.id
	if err := r.persist(); err != nil {
		return err
	}
	r.state = StateCandidate
	r.leader = 0
	r.votes = map[uint64]bool{r.id: true}
	r.resetElectionTimer()
	if len(r.votes) >= r.quorum() {
		return r.becomeLeader()
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.send(Message{Type: MsgVote, To: p, LastLogIndex: r.lastIndex, LastLogTerm: r.lastTerm})
	}
	return nil
}

func (r *raft) becomeLeader() error {
	r.state = StateLeader
	r.leader = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	for _, p := range r.peers {
		r.next[p] = r.lastIndex + 1
		r.match[p] = 0
	}
	// A no-op entry of the new term lets the leader commit entries from
	// earlier terms (§5.4.2, §8).
	noop := Entry{Index: r.lastIndex + 1, Term: r.term, Type: EntryNoop}
	if err := r.appendLog([]Entry{noop}); err != nil {
		return err
	}
	if err := r.broadcastAppend(); err != nil {
		return err
	}
	return r.maybeCommit()
}

// --- inputs -------------------------------------------------------------------

// tick advances the logical clock by one tick.
func (r *raft) tick() error {
	if r.state == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.cfg.HeartbeatTicks {
			r.heartbeatElapsed = 0
			return r.broadcastAppend()
		}
		return nil
	}
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		return r.campaign()
	}
	return nil
}

// propose appends data as new entries if this node is the leader and returns
// the index of the first one.
func (r *raft) propose(data [][]byte) (uint64, error) {
	if r.state != StateLeader {
		return 0, &NotLeaderError{Leader: r.leader}
	}
	first := r.lastIndex + 1
	entries := make([]Entry, len(data))
	for i, d := range data {
		entries[i] = Entry{Index: first + uint64(i), Term: r.term, Type: EntryNormal, Data: d}
	}
	if err := r.appendLog(entries); err != nil {
		return 0, err
	}
	if err := r.broadcastAppend(); err != nil {
		return 0, err
	}
	return first, r.maybeCommit()
}

// step handles one incoming message.
func (r *raft) step(m Message) error {
	if m.To != r.id {
		return nil
	}
	switch {
	case m.Term > r.term:
		// Rule for all servers: discover a higher term → follower.
		var leader uint64
		if m.Type == MsgApp {
			leader = m.From
		}
		if err := r.becomeFollower(m.Term, leader); err != nil {
			return err
		}
	case m.Term < r.term:
		// Stale sender: reply so it learns the current term; drop stale replies.
		switch m.Type {
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgApp:
			r.send(Message{Type: MsgAppResp, To: m.From, PrevLogIndex: m.PrevLogIndex, LastLogIndex: r.lastIndex})
		}
		return nil
	}

	switch m.Type {
	case MsgVote:
		return r.handleVote(m)
	case MsgVoteResp:
		return r.handleVoteResp(m)
	case MsgApp:
		return r.handleAppend(m)
	case MsgAppResp:
		return r.handleAppendResp(m)
	default:
		return nil
	}
}

// isUpToDate implements the election restriction of §5.4.1.
func (r *raft) isUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > r.lastTerm || (lastTerm == r.lastTerm && lastIndex >= r.lastIndex)
}

func (r *raft) handleVote(m Message) error {
	if (r.votedFor == 0 || r.votedFor == m.From) && r.isUpToDate(m.LastLogIndex, m.LastLogTerm) {
		if r.votedFor != m.From {
			r.votedFor = m.From
			if err := r.persist(); err != nil {
				return err
			}
		}
		r.resetElectionTimer()
		r.send(Message{Type: MsgVoteResp, To: m.From, VoteGranted: true})
		return nil
	}
	r.send(Message{Type: MsgVoteResp, To: m.From})
	return nil
}

func (r *raft) handleVoteResp(m Message) error {
	if r.state != StateCandidate || !m.VoteGranted {
		return nil
	}
	r.votes[m.From] = true
	if len(r.votes) >= r.quorum() {
		return r.becomeLeader()
	}
	return nil
}

func (r *raft) handleAppend(m Message) error {
	if r.state == StateLeader {
		return fmt.Errorf("%w: two leaders in term %d (%d and %d)", errSafety, r.term, r.id, m.From)
	}
	if r.state == StateCandidate {
		if err := r.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}
	r.leader = m.From
	r.resetElectionTimer()

	reject := Message{Type: MsgAppResp, To: m.From, PrevLogIndex: m.PrevLogIndex, LastLogIndex: r.lastIndex}
	if m.PrevLogIndex > r.lastIndex {
		r.send(reject)
		return nil
	}
	t, err := r.termAt(m.PrevLogIndex)
	if err != nil {
		return err
	}
	if t != m.PrevLogTerm {
		r.send(reject)
		return nil
	}

	// Skip entries we already have; on the first conflict delete it and
	// everything after it, then append the rest (Figure 2, steps 3 and 4).
	for i, e := range m.Entries {
		if e.Index > r.lastIndex {
			if err := r.appendLog(m.Entries[i:]); err != nil {
				return err
			}
			break
		}
		t, err := r.termAt(e.Index)
		if err != nil {
			return err
		}
		if t != e.Term {
			if err := r.truncateAfter(e.Index - 1); err != nil {
				return err
			}
			if err := r.appendLog(m.Entries[i:]); err != nil {
				return err
			}
			break
		}
	}

	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if c := min(m.LeaderCommit, lastNew); c > r.commitIndex {
		r.commitIndex = c
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
	return r.applyCommitted()
}

func (r *raft) handleAppendResp(m Message) error {
	if r.state != StateLeader {
		return nil
	}
	from := m.From
	if m.Success {
		if m.MatchIndex > r.match[from] {
			r.match[from] = m.MatchIndex
		}
		if r.next[from] < r.match[from]+1 {
			r.next[from] = r.match[from] + 1
		}
		if err := r.maybeCommit(); err != nil {
			return err
		}
		if r.next[from] <= r.lastIndex {
			return r.sendAppend(from)
		}
		return nil
	}
	// Rejected. Ignore replies to requests we have moved past.
	if m.PrevLogIndex+1 != r.next[from] {
		return nil
	}
	// Decrement nextIndex and retry (§5.3). The follower's last index lets us
	// skip entries it cannot have; nextIndex never goes below match+1.
	next := min(m.PrevLogIndex, m.LastLogIndex+1)
	r.next[from] = max(next, r.match[from]+1, 1)
	return r.sendAppend(from)
}

// maybeCommit advances commitIndex to the highest index stored on a majority,
// but only if that entry is from the current term (§5.4.2).
func (r *raft) maybeCommit() error {
	matched := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		if p == r.id {
			matched = append(matched, r.lastIndex)
		} else {
			matched = append(matched, r.match[p])
		}
	}
	slices.Sort(matched)
	n := matched[len(matched)-r.quorum()]
	if n <= r.commitIndex {
		return nil
	}
	t, err := r.termAt(n)
	if err != nil {
		return err
	}
	if t != r.term {
		return nil
	}
	r.commitIndex = n
	return r.applyCommitted()
}

func (r *raft) applyCommitted() error {
	for r.lastApplied < r.commitIndex {
		e, err := r.entryAt(r.lastApplied + 1)
		if err != nil {
			return err
		}
		if err := r.app.Apply(e); err != nil {
			return fmt.Errorf("raft: apply entry %d: %w", e.Index, err)
		}
		r.lastApplied++
	}
	return nil
}

// --- outputs ------------------------------------------------------------------

func (r *raft) send(m Message) {
	m.From = r.id
	m.Term = r.term
	r.tr.Send(m)
}

func (r *raft) broadcastAppend() error {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		if err := r.sendAppend(p); err != nil {
			return err
		}
	}
	return nil
}

// sendAppend sends AppendEntries to a follower, starting at its nextIndex.
func (r *raft) sendAppend(to uint64) error {
	prev := r.next[to] - 1
	prevTerm, err := r.termAt(prev)
	if err != nil {
		return err
	}
	var entries []Entry
	for i := r.next[to]; i <= r.lastIndex && len(entries) < r.cfg.MaxEntriesPerMsg; i++ {
		e, err := r.entryAt(i)
		if err != nil {
			return err
		}
		entries = append(entries, e)
	}
	r.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	})
	return nil
}
