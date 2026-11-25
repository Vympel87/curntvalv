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
	// Peers lists the voters of the cluster, including ID, for a cluster
	// that has no stored configuration yet: the initial configuration.
	// Once a membership change is in the log or a snapshot, that one is
	// used (design D18).
	Peers []uint64
	// Addrs gives the address of each of Peers; it is stored with the first
	// membership change. Optional.
	Addrs map[uint64]string
	// Join starts a node that is not a member yet and waits to be added by
	// the leader (Peers is then ignored). It never campaigns until it is a
	// voter.
	Join bool
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
	// SnapshotEntries is the number of applied entries after which a new
	// snapshot is taken. Default 1024.
	SnapshotEntries int
	// TrailingEntries enables log compaction: the log is cut at the newest
	// snapshot that is at least this many entries behind the last applied
	// entry. 0 (the default) does not limit the cut point.
	TrailingEntries int
	// Retention, if set, enables log compaction and limits the cut point:
	// the log is cut at the newest snapshot at or below CompactLimit (design
	// D5). It is asked after every snapshot and every CompactTicks ticks.
	// With both TrailingEntries and Retention the lower limit wins; with
	// neither the log is never cut.
	Retention Retention
	// CompactTicks is how often (in ticks) Retention is asked again while no
	// snapshot is taken. Default 100.
	CompactTicks int
	// PreVote makes a node whose election timeout fires first ask the
	// others whether it could win (thesis §9.6) and start a real election,
	// with a higher term, only if a majority agrees. A node that was cut off
	// then rejoins without forcing a new election. A node agrees only if
	// the candidate's log is up to date and it has not heard from a leader
	// for ElectionTicks.
	PreVote bool
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
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = 1024
	}
	if c.CompactTicks == 0 {
		c.CompactTicks = 100
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
	if c.TickInterval < 0 || c.MaxEntriesPerMsg < 1 || c.SnapshotEntries < 1 || c.TrailingEntries < 0 || c.CompactTicks < 1 {
		return errors.New("raft: config: invalid TickInterval, MaxEntriesPerMsg, SnapshotEntries, TrailingEntries or CompactTicks")
	}
	seen := map[uint64]bool{}
	for _, p := range c.Peers {
		if p == 0 || seen[p] {
			return fmt.Errorf("raft: config: invalid or duplicate peer %d", p)
		}
		seen[p] = true
	}
	if !seen[c.ID] && !c.Join {
		return fmt.Errorf("raft: config: Peers must contain ID %d", c.ID)
	}
	return nil
}

// bootstrapConf is the configuration of a cluster without a stored one.
func (c *Config) bootstrapConf() Configuration {
	var conf Configuration
	if c.Join {
		return conf
	}
	for _, p := range c.Peers {
		conf.Members = append(conf.Members, Member{ID: p, Addr: c.Addrs[p], Voter: true})
	}
	slices.SortFunc(conf.Members, func(a, b Member) int { return compareU64(a.ID, b.ID) })
	return conf
}

// Retention decides how much of the log must be kept (design D5).
type Retention interface {
	// CompactLimit returns the highest log index that may be removed. It is
	// called from the node's Run goroutine and must not block.
	CompactLimit() uint64
}

// Storage groups the durable stores of a node.
type Storage struct {
	Log       LogStore
	Stable    StableStore
	Snapshots SnapshotStore
}

// Status is a snapshot of a node's Raft state.
type Status struct {
	ID            uint64
	Term          uint64
	State         StateType
	Leader        uint64 // 0 if unknown
	FirstIndex    uint64
	LastIndex     uint64
	CommitIndex   uint64
	LastApplied   uint64
	SnapshotIndex uint64 // latest snapshot, 0 if none
	Counters      Counters
	// Config is the configuration in effect on this node; ConfigIndex the
	// log index of its entry (0 if it comes from a snapshot or Peers).
	Config      Configuration
	ConfigIndex uint64
	// Match is, on the leader, the highest log index known to be stored on
	// each other member.
	Match map[uint64]uint64
}

// Counters count events on this node since Run started (for metrics).
type Counters struct {
	Elections          uint64 // elections this node started
	BecameLeader       uint64 // times this node won an election
	SnapshotsTaken     uint64
	SnapshotsInstalled uint64 // received from a leader, replacing the state
}

var (
	// errSafety marks a broken Raft invariant. It means a bug or corrupted
	// storage; the node must stop.
	errSafety = errors.New("raft: safety violation")
	// errCompacted means an entry's term is no longer known locally.
	errCompacted = errors.New("raft: entry compacted")
)

type readState struct {
	id        uint64
	acks      map[uint64]bool
	confirmed bool
}

// confEntry is a configuration entry in the log after the latest snapshot.
type confEntry struct {
	index uint64
	conf  Configuration
}

// raft is the protocol state machine of one node. It is not safe for
// concurrent use. Every error it returns is fatal for the node.
type raft struct {
	id    uint64
	cfg   Config
	log   LogStore
	store StableStore
	snaps SnapshotStore
	tr    Transport
	app   Applier
	rng   *rand.Rand

	// Persistent state (Figure 2). The log itself lives in log.
	term     uint64
	votedFor uint64

	// Membership (design D18). conf is the latest configuration in the log,
	// else the latest snapshot's (baseConf), else bootstrap; confIndex is
	// the index of its entry, 0 if it is not in the log.
	bootstrap Configuration
	baseConf  *Configuration
	confs     []confEntry
	conf      Configuration
	confIndex uint64

	// Cached position of the last log entry.
	lastIndex uint64
	lastTerm  uint64
	// Stored snapshots, ascending by index.
	snapshots []SnapshotMeta

	// Volatile state on all servers.
	commitIndex uint64
	lastApplied uint64
	// Ticks since compaction was last checked (with Retention).
	compactElapsed int
	counters       Counters
	state          StateType
	leader         uint64

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	// Candidate state.
	votes map[uint64]bool
	// Pre-candidate state (PreVote): granted pre-votes for term+1 while a
	// follower asks; nil otherwise.
	preVotes map[uint64]bool

	// Leader state, reinitialized after election.
	next  map[uint64]uint64
	match map[uint64]uint64
	// Pending read-only requests (§8).
	readSeq uint64
	reads   []*readState
}

// newRaft creates a follower from the persisted state: it restores the latest
// snapshot into app, repairs a log left behind by an interrupted snapshot
// install, and loads term and vote.
func newRaft(cfg Config, st Storage, tr Transport, app Applier) (*raft, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	hs, err := st.Stable.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}
	snaps, err := st.Snapshots.List()
	if err != nil {
		return nil, err
	}
	r := &raft{
		id:        cfg.ID,
		bootstrap: cfg.bootstrapConf(),
		cfg:       cfg,
		log:       st.Log,
		store:     st.Stable,
		snaps:     st.Snapshots,
		tr:        tr,
		app:       app,
		rng:       rand.New(rand.NewPCG(cfg.Seed, cfg.ID)),
		term:      hs.Term,
		votedFor:  hs.VotedFor,
		snapshots: snaps,
		state:     StateFollower,
		next:      map[uint64]uint64{},
		match:     map[uint64]uint64{},
	}
	if err := r.restore(); err != nil {
		return nil, err
	}
	if r.lastTerm > r.term {
		return nil, fmt.Errorf("%w: last log term %d > current term %d", errSafety, r.lastTerm, r.term)
	}
	r.resetElectionTimer()
	return r, nil
}

func (r *raft) restore() error {
	r.lastIndex = r.log.LastIndex()
	r.lastTerm = 0
	if len(r.snapshots) > 0 {
		latest := r.snapshots[len(r.snapshots)-1]
		first := r.log.FirstIndex()
		if latest.Index+1 < first {
			return fmt.Errorf("%w: log starts at %d, after latest snapshot %d", errSafety, first, latest.Index)
		}
		// The log must continue the snapshot. If it does not, a snapshot
		// install was interrupted between saving the snapshot and resetting
		// the log: finish it.
		consistent := r.lastIndex >= latest.Index
		if consistent && latest.Index >= first {
			e, err := r.entryAt(latest.Index)
			if err != nil {
				return err
			}
			consistent = e.Term == latest.Term
		}
		if !consistent {
			if err := r.log.Reset(latest.Index + 1); err != nil {
				return fmt.Errorf("raft: reset log after snapshot %d: %w", latest.Index, err)
			}
			r.lastIndex = latest.Index
		}
		data, err := r.snaps.Load(latest.Index)
		if err != nil {
			return err
		}
		conf, app, err := unwrapSnapshot(data)
		if err != nil {
			return fmt.Errorf("raft: snapshot %d: %w", latest.Index, err)
		}
		if err := r.app.Restore(app); err != nil {
			return fmt.Errorf("raft: restore snapshot %d: %w", latest.Index, err)
		}
		r.baseConf = conf
		r.commitIndex, r.lastApplied = latest.Index, latest.Index
	}
	if r.lastIndex > 0 {
		t, err := r.termAt(r.lastIndex)
		if err != nil {
			return fmt.Errorf("raft: term of last entry %d: %w", r.lastIndex, err)
		}
		r.lastTerm = t
	}
	// Configuration entries after the snapshot (at most about
	// SnapshotEntries of them to read).
	for i := max(r.commitIndex+1, r.log.FirstIndex()); i <= r.lastIndex; i++ {
		e, err := r.entryAt(i)
		if err != nil {
			return err
		}
		if err := r.recordConf(e); err != nil {
			return err
		}
	}
	r.updateConf()
	return nil
}

func (r *raft) quorum() int { return r.conf.quorum() }

// recordConf remembers a configuration entry that is now in the log.
func (r *raft) recordConf(e Entry) error {
	if e.Type != EntryConfig {
		return nil
	}
	c, err := decodeConfig(e.Data)
	if err != nil {
		return fmt.Errorf("raft: entry %d: %w", e.Index, err)
	}
	r.confs = append(r.confs, confEntry{index: e.Index, conf: c})
	return nil
}

// updateConf makes the latest configuration the current one and, on a
// leader, starts or stops replicating to the servers that came or went.
func (r *raft) updateConf() {
	switch n := len(r.confs); {
	case n > 0:
		r.conf, r.confIndex = r.confs[n-1].conf, r.confs[n-1].index
	case r.baseConf != nil:
		r.conf, r.confIndex = *r.baseConf, 0
	default:
		r.conf, r.confIndex = r.bootstrap, 0
	}
	if r.state != StateLeader {
		return
	}
	for _, m := range r.conf.Members {
		if _, ok := r.next[m.ID]; !ok {
			r.next[m.ID], r.match[m.ID] = r.lastIndex+1, 0
		}
	}
	for id := range r.next {
		if _, ok := r.conf.Member(id); !ok {
			delete(r.next, id)
			delete(r.match, id)
		}
	}
}

// confAt returns the configuration in effect at index i (at or after the
// latest snapshot), nil if it is the bootstrap one.
func (r *raft) confAt(i uint64) *Configuration {
	c := r.baseConf
	for _, ce := range r.confs {
		if ce.index <= i {
			conf := ce.conf
			c = &conf
		}
	}
	return c
}

func (r *raft) status() Status {
	s := Status{
		ID:          r.id,
		Term:        r.term,
		State:       r.state,
		Leader:      r.leader,
		FirstIndex:  r.log.FirstIndex(),
		LastIndex:   r.lastIndex,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		Counters:    r.counters,
		Config:      r.conf.clone(),
		ConfigIndex: r.confIndex,
	}
	if r.state == StateLeader {
		s.Match = make(map[uint64]uint64, len(r.match))
		for id, m := range r.match {
			s.Match[id] = m
		}
	}
	if n := len(r.snapshots); n > 0 {
		s.SnapshotIndex = r.snapshots[n-1].Index
	}
	return s
}

// --- log access -------------------------------------------------------------

func (r *raft) entryAt(i uint64) (Entry, error) {
	b, err := r.log.Read(i)
	if err != nil {
		return Entry{}, fmt.Errorf("raft: read entry %d: %w", i, err)
	}
	return decodeEntry(i, b)
}

// termAt returns the term of entry i. For an index before the start of the
// log the term is known only if a snapshot ends there; otherwise it returns
// errCompacted.
func (r *raft) termAt(i uint64) (uint64, error) {
	switch {
	case i == 0:
		return 0, nil
	case i == r.lastIndex && r.lastTerm != 0:
		return r.lastTerm, nil
	case i > r.lastIndex:
		return 0, fmt.Errorf("raft: term of index %d past last index %d", i, r.lastIndex)
	case i >= r.log.FirstIndex():
		e, err := r.entryAt(i)
		if err != nil {
			return 0, err
		}
		return e.Term, nil
	}
	for _, s := range r.snapshots {
		if s.Index == i {
			return s.Term, nil
		}
	}
	return 0, errCompacted
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
	changed := false
	for _, e := range entries {
		if e.Type == EntryConfig {
			if err := r.recordConf(e); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		r.updateConf()
	}
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
	r.lastIndex, r.lastTerm = i, 0
	t, err := r.termAt(i)
	if err != nil {
		return err
	}
	r.lastTerm = t
	// A configuration whose entry was removed no longer applies (§4.1).
	if n := len(r.confs); n > 0 && r.confs[n-1].index > i {
		r.confs = slices.DeleteFunc(r.confs, func(c confEntry) bool { return c.index > i })
		r.updateConf()
	}
	return nil
}

func (r *raft) persist() error {
	if err := r.store.Save(HardState{Term: r.term, VotedFor: r.votedFor}); err != nil {
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
	r.preVotes = nil
	r.reads = nil
	r.resetElectionTimer()
	return nil
}

// preCampaign asks for pre-votes for term+1 (thesis §9.6). The node stays a
// follower of its term and changes no persistent state.
func (r *raft) preCampaign() error {
	if r.state == StateCandidate {
		// The election failed. Asking again first keeps a candidate that
		// was cut off from raising its term over and over.
		if err := r.becomeFollower(r.term, 0); err != nil {
			return err
		}
	}
	r.leader = 0
	r.preVotes = map[uint64]bool{r.id: true}
	r.resetElectionTimer()
	if len(r.preVotes) >= r.quorum() {
		return r.campaign()
	}
	for _, p := range r.conf.Voters() {
		if p == r.id {
			continue
		}
		r.sendTerm(Message{Type: MsgPreVote, To: p, LastLogIndex: r.lastIndex, LastLogTerm: r.lastTerm}, r.term+1)
	}
	return nil
}

// campaign starts an election (§5.2).
func (r *raft) campaign() error {
	r.counters.Elections++
	r.term++
	r.votedFor = r.id
	if err := r.persist(); err != nil {
		return err
	}
	r.state = StateCandidate
	r.leader = 0
	r.reads = nil
	r.preVotes = nil
	r.votes = map[uint64]bool{r.id: true}
	r.resetElectionTimer()
	if len(r.votes) >= r.quorum() {
		return r.becomeLeader()
	}
	for _, p := range r.conf.Voters() {
		if p == r.id {
			continue
		}
		r.send(Message{Type: MsgVote, To: p, LastLogIndex: r.lastIndex, LastLogTerm: r.lastTerm})
	}
	return nil
}

func (r *raft) becomeLeader() error {
	r.counters.BecameLeader++
	r.state = StateLeader
	r.leader = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	clear(r.next)
	clear(r.match)
	for _, m := range r.conf.Members {
		r.next[m.ID] = r.lastIndex + 1
		r.match[m.ID] = 0
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
	if r.cfg.Retention != nil {
		r.compactElapsed++
		if r.compactElapsed >= r.cfg.CompactTicks {
			r.compactElapsed = 0
			if len(r.snapshots) > 0 {
				if err := r.compact(); err != nil {
					return err
				}
			}
		}
	}
	if r.state == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.cfg.HeartbeatTicks {
			r.heartbeatElapsed = 0
			return r.broadcastAppend()
		}
		return nil
	}
	r.electionElapsed++
	if !r.conf.IsVoter(r.id) {
		return nil // a learner, or not a member (yet or any more)
	}
	if r.electionElapsed >= r.electionTimeout {
		if r.cfg.PreVote {
			return r.preCampaign()
		}
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

// requestRead starts a read-only request (§8) and returns its id. The read
// becomes ready (see readyReads) once a majority has acknowledged a heartbeat
// sent after the request, proving this node was still the leader.
func (r *raft) requestRead() (uint64, error) {
	if r.state != StateLeader {
		return 0, &NotLeaderError{Leader: r.leader}
	}
	r.readSeq++
	rs := &readState{id: r.readSeq, acks: map[uint64]bool{}}
	if r.conf.IsVoter(r.id) {
		rs.acks[r.id] = true
	}
	rs.confirmed = len(rs.acks) >= r.quorum()
	r.reads = append(r.reads, rs)
	if !rs.confirmed {
		return rs.id, r.broadcastAppend()
	}
	return rs.id, nil
}

// readyReads removes and returns the ids of reads that may now be served at
// commitIndex. Besides the majority check, the leader must have committed an
// entry of its own term so that its commitIndex is up to date (§8).
func (r *raft) readyReads() ([]uint64, error) {
	if len(r.reads) == 0 || r.state != StateLeader {
		return nil, nil
	}
	t, err := r.termAt(r.commitIndex)
	if err != nil && !errors.Is(err, errCompacted) {
		return nil, err
	}
	if t != r.term {
		return nil, nil
	}
	var ready []uint64
	rest := r.reads[:0]
	for _, rs := range r.reads {
		if rs.confirmed {
			ready = append(ready, rs.id)
		} else {
			rest = append(rest, rs)
		}
	}
	r.reads = rest
	return ready, nil
}

// step handles one incoming message.
func (r *raft) step(m Message) error {
	if m.To != r.id {
		return nil
	}
	// Pre-votes carry the term of an election that has not started; they
	// must not move this node's term (thesis §9.6).
	switch m.Type {
	case MsgPreVote:
		r.handlePreVote(m)
		return nil
	case MsgPreVoteResp:
		return r.handlePreVoteResp(m)
	}
	// A server that hears from a current leader ignores vote requests for a
	// while (thesis §4.2.3), so a removed server, which no longer gets
	// heartbeats, cannot depose the leader with ever higher terms.
	if m.Type == MsgVote && m.Term > r.term && r.heardFromLeader() {
		return nil
	}
	switch {
	case m.Term > r.term:
		// Rule for all servers: discover a higher term → follower.
		var leader uint64
		if m.Type == MsgApp || m.Type == MsgSnap {
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
		case MsgApp, MsgSnap:
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
	case MsgSnap:
		return r.handleSnapshot(m)
	case MsgAppResp:
		return r.handleAppendResp(m)
	default:
		return nil
	}
}

// heardFromLeader reports whether this node is the leader or heard from one
// within the minimum election timeout.
func (r *raft) heardFromLeader() bool {
	return r.state == StateLeader || (r.leader != 0 && r.electionElapsed < r.cfg.ElectionTicks)
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

// handlePreVote grants a pre-vote if an election in m.Term could happen now:
// the term is ahead, the candidate's log is up to date, and this node has
// not heard from a leader for a minimum election timeout.
func (r *raft) handlePreVote(m Message) {
	if m.Term > r.term && !r.heardFromLeader() && r.isUpToDate(m.LastLogIndex, m.LastLogTerm) {
		r.sendTerm(Message{Type: MsgPreVoteResp, To: m.From, VoteGranted: true}, m.Term)
		return
	}
	r.send(Message{Type: MsgPreVoteResp, To: m.From})
}

func (r *raft) handlePreVoteResp(m Message) error {
	if !m.VoteGranted {
		if m.Term > r.term {
			return r.becomeFollower(m.Term, 0)
		}
		return nil
	}
	if r.preVotes == nil || r.state != StateFollower || m.Term != r.term+1 || !r.conf.IsVoter(m.From) {
		return nil // stale, or not a voter
	}
	r.preVotes[m.From] = true
	if len(r.preVotes) >= r.quorum() {
		return r.campaign()
	}
	return nil
}

func (r *raft) handleVoteResp(m Message) error {
	if r.state != StateCandidate || !m.VoteGranted || !r.conf.IsVoter(m.From) {
		return nil
	}
	r.votes[m.From] = true
	if len(r.votes) >= r.quorum() {
		return r.becomeLeader()
	}
	return nil
}

// acceptLeader is the common start of handling AppendEntries and
// InstallSnapshot from the leader of the current term.
func (r *raft) acceptLeader(m Message) error {
	if r.state == StateLeader {
		return fmt.Errorf("%w: two leaders in term %d (%d and %d)", errSafety, r.term, r.id, m.From)
	}
	if r.state == StateCandidate {
		if err := r.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}
	r.leader = m.From
	r.preVotes = nil
	r.resetElectionTimer()
	return nil
}

func (r *raft) handleAppend(m Message) error {
	if err := r.acceptLeader(m); err != nil {
		return err
	}
	reply := Message{Type: MsgAppResp, To: m.From, PrevLogIndex: m.PrevLogIndex, ReadID: m.ReadID}

	if m.PrevLogIndex > r.lastIndex {
		reply.LastLogIndex = r.lastIndex
		r.send(reply)
		return nil
	}
	t, err := r.termAt(m.PrevLogIndex)
	if errors.Is(err, errCompacted) {
		// prevLogIndex is inside our snapshot, so it is committed, and
		// committed entries are the same on every node: we match the leader
		// at least up to our commitIndex.
		reply.Success, reply.MatchIndex = true, r.commitIndex
		r.send(reply)
		return nil
	}
	if err != nil {
		return err
	}
	if t != m.PrevLogTerm {
		reply.LastLogIndex = r.lastIndex
		r.send(reply)
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
		if errors.Is(err, errCompacted) {
			continue // committed, so it matches
		}
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
	reply.Success, reply.MatchIndex = true, lastNew
	r.send(reply)
	return r.applyCommitted()
}

// handleSnapshot implements the InstallSnapshot receiver (Figure 13).
func (r *raft) handleSnapshot(m Message) error {
	if err := r.acceptLeader(m); err != nil {
		return err
	}
	s := m.Snapshot
	if s == nil {
		return nil
	}
	reply := Message{Type: MsgAppResp, To: m.From, Success: true, ReadID: m.ReadID}

	// Already have everything the snapshot covers.
	if s.Meta.Index <= r.commitIndex {
		reply.MatchIndex = r.commitIndex
		r.send(reply)
		return nil
	}
	// The log has the snapshot's last entry: keep the entries after it and
	// let the normal apply path catch up.
	if s.Meta.Index <= r.lastIndex {
		t, err := r.termAt(s.Meta.Index)
		if err != nil {
			return err
		}
		if t == s.Meta.Term {
			r.commitIndex = s.Meta.Index
			reply.MatchIndex = s.Meta.Index
			r.send(reply)
			return r.applyCommitted()
		}
	}
	// Otherwise discard the log and reset the state machine. The snapshot is
	// saved first; restore() finishes the job if we crash in between.
	if err := r.snaps.Save(s.Meta, s.Data); err != nil {
		return fmt.Errorf("raft: save snapshot %d: %w", s.Meta.Index, err)
	}
	r.snapshots = append(r.snapshots, s.Meta)
	r.counters.SnapshotsInstalled++
	if err := r.log.Reset(s.Meta.Index + 1); err != nil {
		return fmt.Errorf("raft: reset log to %d: %w", s.Meta.Index+1, err)
	}
	conf, app, err := unwrapSnapshot(s.Data)
	if err != nil {
		return fmt.Errorf("raft: snapshot %d from %d: %w", s.Meta.Index, m.From, err)
	}
	if err := r.app.Restore(app); err != nil {
		return fmt.Errorf("raft: restore snapshot %d: %w", s.Meta.Index, err)
	}
	r.baseConf, r.confs = conf, nil
	r.updateConf()
	r.lastIndex, r.lastTerm = s.Meta.Index, s.Meta.Term
	r.commitIndex, r.lastApplied = s.Meta.Index, s.Meta.Index
	if err := r.dropSnapshotsBefore(s.Meta.Index); err != nil {
		return err
	}
	reply.MatchIndex = s.Meta.Index
	r.send(reply)
	return nil
}

func (r *raft) handleAppendResp(m Message) error {
	if r.state != StateLeader {
		return nil
	}
	from := m.From
	if _, ok := r.conf.Member(from); !ok {
		return nil // removed meanwhile
	}
	// Any reply in our term from a voter acknowledges us as leader for read
	// requests up to m.ReadID.
	if m.ReadID > 0 && r.conf.IsVoter(from) {
		for _, rs := range r.reads {
			if rs.id <= m.ReadID && !rs.confirmed {
				rs.acks[from] = true
				rs.confirmed = len(rs.acks) >= r.quorum()
			}
		}
	}
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
		if r.state != StateLeader {
			return nil // stepped down after its removal
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
//
// Only voters count. A leader that removed itself is not one: it manages
// the cluster until the change is committed and then steps down (thesis
// §4.2.2).
func (r *raft) maybeCommit() error {
	voters := r.conf.Voters()
	if len(voters) == 0 {
		return nil
	}
	matched := make([]uint64, 0, len(voters))
	for _, p := range voters {
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
	if err := r.applyCommitted(); err != nil {
		return err
	}
	if !r.conf.IsVoter(r.id) && r.confIndex <= r.commitIndex {
		// Our removal is committed: tell the others, then step down.
		if err := r.broadcastAppend(); err != nil {
			return err
		}
		return r.becomeFollower(r.term, 0)
	}
	return nil
}

// proposeConf appends a configuration entry for cc and returns its index.
// The new configuration is in effect at once (§4.1).
func (r *raft) proposeConf(cc ConfChange) (uint64, error) {
	if r.state != StateLeader {
		return 0, &NotLeaderError{Leader: r.leader}
	}
	if r.confIndex > r.commitIndex {
		return 0, fmt.Errorf("%w: entry %d is not committed yet", ErrConfChangeInProgress, r.confIndex)
	}
	// A new leader must commit an entry of its term first, or an older
	// leader's uncommitted change could still win on top of this one.
	if t, err := r.termAt(r.commitIndex); err != nil && !errors.Is(err, errCompacted) {
		return 0, err
	} else if t != r.term {
		return 0, fmt.Errorf("%w: the leader has not committed an entry of term %d yet", ErrConfChangeInProgress, r.term)
	}
	next, err := r.conf.apply(cc)
	if err != nil {
		return 0, err
	}
	idx := r.lastIndex + 1
	if err := r.appendLog([]Entry{{Index: idx, Term: r.term, Type: EntryConfig, Data: encodeConfig(next)}}); err != nil {
		return 0, err
	}
	if err := r.broadcastAppend(); err != nil {
		return 0, err
	}
	return idx, r.maybeCommit()
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
	return r.maybeSnapshot()
}

// --- snapshots and compaction (§7) ---------------------------------------------

// maybeSnapshot takes a snapshot every SnapshotEntries applied entries and
// then compacts the log if TrailingEntries allows it.
func (r *raft) maybeSnapshot() error {
	var latest uint64
	if n := len(r.snapshots); n > 0 {
		latest = r.snapshots[n-1].Index
	}
	if r.lastApplied < latest+uint64(r.cfg.SnapshotEntries) {
		return nil
	}
	app, err := r.app.Snapshot()
	if err != nil {
		return fmt.Errorf("raft: take snapshot: %w", err)
	}
	t, err := r.termAt(r.lastApplied)
	if err != nil {
		return err
	}
	// The configuration at the snapshot travels with it; configuration
	// entries it covers are no longer needed in memory.
	conf := r.confAt(r.lastApplied)
	data := wrapSnapshot(conf, app)
	r.baseConf = conf
	r.confs = slices.DeleteFunc(r.confs, func(c confEntry) bool { return c.index <= r.lastApplied })
	meta := SnapshotMeta{Index: r.lastApplied, Term: t}
	if err := r.snaps.Save(meta, data); err != nil {
		return fmt.Errorf("raft: save snapshot %d: %w", meta.Index, err)
	}
	r.snapshots = append(r.snapshots, meta)
	r.counters.SnapshotsTaken++
	return r.compact()
}

// compact cuts the log at the newest snapshot allowed by TrailingEntries
// and Retention, then deletes snapshots that are no longer needed. Kept are:
// the latest (for restarts), the base, i.e. the oldest one covering the
// start of the log (for InstallSnapshot), and every snapshot newer than the
// current cut point (future cut points).
func (r *raft) compact() error {
	keep := r.snapshots[len(r.snapshots)-1].Index
	if limit, ok := r.compactLimit(); ok {
		var cut uint64
		for _, s := range r.snapshots {
			if s.Index <= limit {
				cut = s.Index
			}
		}
		if cut > 0 && cut+1 > r.log.FirstIndex() {
			if err := r.log.CompactBefore(cut + 1); err != nil {
				return fmt.Errorf("raft: compact log before %d: %w", cut+1, err)
			}
		}
		if cut > 0 {
			keep = cut
		} else {
			keep = r.snapshots[0].Index
		}
	}
	if base, ok := r.baseSnapshot(); ok {
		keep = min(keep, base.Index)
	}
	return r.dropSnapshotsBefore(keep)
}

// compactLimit returns the highest index the log may be cut at. ok is false
// when compaction is disabled.
func (r *raft) compactLimit() (limit uint64, ok bool) {
	tr, ret := uint64(r.cfg.TrailingEntries), r.cfg.Retention
	if tr == 0 && ret == nil {
		return 0, false
	}
	limit = r.lastApplied
	if tr > 0 {
		limit = r.lastApplied - min(tr, r.lastApplied)
	}
	if ret != nil {
		limit = min(limit, ret.CompactLimit())
	}
	return limit, true
}

// baseSnapshot returns the oldest snapshot from which the log continues
// without a gap. It is what a follower behind the start of the log needs, so
// that it ends up with the same retained history as the leader (design D5).
// ok is false if the log is not compacted (it starts at index 1).
func (r *raft) baseSnapshot() (SnapshotMeta, bool) {
	first := r.log.FirstIndex()
	if first <= 1 {
		return SnapshotMeta{}, false
	}
	for _, s := range r.snapshots {
		if s.Index+1 >= first {
			return s, true
		}
	}
	return SnapshotMeta{}, false
}

func (r *raft) dropSnapshotsBefore(index uint64) error {
	if len(r.snapshots) == 0 || r.snapshots[0].Index >= index {
		return nil
	}
	if err := r.snaps.DeleteBefore(index); err != nil {
		return fmt.Errorf("raft: delete snapshots before %d: %w", index, err)
	}
	r.snapshots = slices.DeleteFunc(r.snapshots, func(s SnapshotMeta) bool { return s.Index < index })
	return nil
}

// --- outputs ------------------------------------------------------------------

func (r *raft) send(m Message) { r.sendTerm(m, r.term) }

// sendTerm sends m with another term than the current one; only pre-votes
// do that.
func (r *raft) sendTerm(m Message, term uint64) {
	m.From = r.id
	m.Term = term
	r.tr.Send(m)
}

func (r *raft) broadcastAppend() error {
	for _, m := range r.conf.Members {
		if m.ID == r.id {
			continue
		}
		if err := r.sendAppend(m.ID); err != nil {
			return err
		}
	}
	return nil
}

// sendAppend sends AppendEntries to a follower, starting at its nextIndex,
// or the base snapshot if those entries are no longer in the log.
func (r *raft) sendAppend(to uint64) error {
	next := r.next[to]
	prev := next - 1
	prevTerm, err := r.termAt(prev)
	if errors.Is(err, errCompacted) || next < r.log.FirstIndex() {
		return r.sendSnapshot(to)
	}
	if err != nil {
		return err
	}
	var entries []Entry
	for i := next; i <= r.lastIndex && len(entries) < r.cfg.MaxEntriesPerMsg; i++ {
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
		ReadID:       r.readSeq,
	})
	return nil
}

func (r *raft) sendSnapshot(to uint64) error {
	base, ok := r.baseSnapshot()
	if !ok {
		return fmt.Errorf("%w: follower %d needs entry %d but no snapshot covers the log start %d",
			errSafety, to, r.next[to], r.log.FirstIndex())
	}
	data, err := r.snaps.Load(base.Index)
	if err != nil {
		return err
	}
	r.send(Message{Type: MsgSnap, To: to, Snapshot: &Snapshot{Meta: base, Data: data}, ReadID: r.readSeq})
	return nil
}
