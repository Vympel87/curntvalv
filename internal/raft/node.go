package raft

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	// ErrNotLeader is matched (with errors.Is) by *NotLeaderError.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrLeadershipLost means the node stopped being leader before a proposal
	// was committed. The entry may or may not be committed later.
	ErrLeadershipLost = errors.New("raft: leadership lost before commit")
	// ErrStopped is returned once the node's Run has returned.
	ErrStopped = errors.New("raft: node stopped")
	// ErrCompacted is returned by ReadCommitted for entries that log
	// compaction has removed.
	ErrCompacted = errors.New("raft: entries compacted")
)

// NotLeaderError is returned by Propose on a node that is not the leader.
type NotLeaderError struct {
	Leader uint64 // the leader this node knows of, 0 if none
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not the leader (leader is %d)", e.Leader)
}

// Is makes errors.Is(err, ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

type proposal struct {
	data   []byte
	term   uint64             // 0: any term
	result chan proposeResult // buffered, size 1
}

type proposeResult struct {
	index uint64
	err   error
}

// Node runs one Raft node. All Raft state is owned by the goroutine that
// calls Run; the other methods talk to it through channels and are safe for
// concurrent use.
type Node struct {
	cfg     Config
	storage Storage
	tr      Transport
	app     Applier

	recvc   chan Message
	propc   chan proposal
	readc   chan chan proposeResult
	statusc chan chan Status
	done    chan struct{}
	running atomic.Bool
	err     error // written before done is closed
}

// NewNode creates a node. It does no I/O; state is loaded when Run starts.
func NewNode(cfg Config, storage Storage, tr Transport, app Applier) (*Node, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	if storage.Log == nil || storage.Stable == nil || storage.Snapshots == nil {
		return nil, errors.New("raft: Storage needs Log, Stable and Snapshots")
	}
	return &Node{
		cfg:     cfg,
		storage: storage,
		tr:      tr,
		app:     app,
		recvc:   make(chan Message, 1024),
		propc:   make(chan proposal, 1024),
		readc:   make(chan chan proposeResult, 1024),
		statusc: make(chan chan Status),
		done:    make(chan struct{}),
	}, nil
}

// Run loads the persisted state and runs the node until ctx is cancelled
// (returns nil) or a fatal error happens (returns it). Run may be called
// only once.
func (n *Node) Run(ctx context.Context) error {
	if !n.running.CompareAndSwap(false, true) {
		return errors.New("raft: Run called twice")
	}
	w := &waitingApplier{inner: n.app, waiters: map[uint64]waiter{}}
	reads := map[uint64]chan proposeResult{}
	failReads := func(err error) {
		for id, c := range reads {
			c <- proposeResult{err: err}
			delete(reads, id)
		}
	}
	defer close(n.done)
	defer failReads(ErrStopped)
	defer w.failAll(ErrStopped)

	r, err := newRaft(n.cfg, n.storage, n.tr, w)
	if err != nil {
		n.err = err
		return err
	}
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = r.tick()
		case m := <-n.recvc:
			err = r.step(m)
		case p := <-n.propc:
			err = n.propose(r, w, p)
		case c := <-n.readc:
			var id uint64
			if id, err = r.requestRead(); err == nil {
				reads[id] = c
			} else if errors.Is(err, ErrNotLeader) {
				c <- proposeResult{err: err}
				err = nil
			}
		case c := <-n.statusc:
			c <- r.status()
		}
		if err != nil {
			n.err = err
			return err
		}
		if r.state != StateLeader {
			w.failAll(ErrLeadershipLost)
			failReads(ErrLeadershipLost)
			continue
		}
		w.failOtherTerms(r.term)
		ready, err := r.readyReads()
		if err != nil {
			n.err = err
			return err
		}
		for _, id := range ready {
			// On the leader lastApplied == commitIndex after every event, so
			// the state machine already reflects the read index.
			reads[id] <- proposeResult{index: r.commitIndex}
			delete(reads, id)
		}
	}
}

// propose appends p and any other queued proposals as one batch.
func (n *Node) propose(r *raft, w *waitingApplier, p proposal) error {
	batch := []proposal{p}
	for len(batch) < n.cfg.MaxEntriesPerMsg {
		select {
		case q := <-n.propc:
			batch = append(batch, q)
			continue
		default:
		}
		break
	}
	if r.state != StateLeader {
		for _, q := range batch {
			q.result <- proposeResult{err: &NotLeaderError{Leader: r.leader}}
		}
		return nil
	}
	// Register waiters first: a single-node cluster commits inside propose.
	data := make([][]byte, 0, len(batch))
	first := r.lastIndex + 1
	for _, q := range batch {
		if q.term != 0 && q.term != r.term {
			// Made for an earlier term: appending it now could leave a gap
			// after proposals of that term that were lost.
			q.result <- proposeResult{err: ErrLeadershipLost}
			continue
		}
		w.waiters[first+uint64(len(data))] = waiter{term: r.term, result: q.result}
		data = append(data, q.data)
	}
	if len(data) == 0 {
		return nil
	}
	_, err := r.propose(data)
	return err
}

// Propose replicates data through Raft and blocks until it is committed,
// then returns its log index. It returns a *NotLeaderError on a follower and
// ErrLeadershipLost if leadership changes first (the entry may still be
// committed later). The caller must not modify data afterwards.
func (n *Node) Propose(ctx context.Context, data []byte) (uint64, error) {
	p, err := n.ProposeAsync(ctx, 0, data)
	if err != nil {
		return 0, err
	}
	return p.Wait(ctx)
}

// ProposeAsync queues data for replication and returns without waiting for
// the commit. Proposals queued by one goroutine are appended to the log in
// the order of the calls, so several can be in flight at once (one append
// and one round trip for all of them) while their order is kept.
//
// With term != 0 the proposal is only appended while this node is the
// leader of that term, and fails with ErrLeadershipLost otherwise. Within
// one term a leader's entries are never lost from its own log, so if a
// proposal of a term is committed, all earlier proposals of that term
// (queued by the same goroutine) are committed too.
//
// It blocks only while the queue is full. The caller must not modify data
// afterwards.
func (n *Node) ProposeAsync(ctx context.Context, term uint64, data []byte) (*Proposal, error) {
	p := proposal{data: data, term: term, result: make(chan proposeResult, 1)}
	select {
	case n.propc <- p:
		return &Proposal{result: p.result, stopped: n.done}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// Proposal is a queued proposal (see ProposeAsync). Its methods must be
// called from one goroutine.
type Proposal struct {
	result  chan proposeResult
	stopped <-chan struct{}
	res     *proposeResult // once known
}

// Wait blocks until the proposal is committed and returns its log index, or
// returns the error that ended it: *NotLeaderError, ErrLeadershipLost (the
// entry may still be committed later), ErrStopped or ctx's error. After ctx
// ends, Wait may be called again.
func (p *Proposal) Wait(ctx context.Context) (uint64, error) {
	if p.res == nil {
		select {
		case res := <-p.result:
			p.res = &res
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-p.stopped:
			select {
			case res := <-p.result:
				p.res = &res
			default:
				p.res = &proposeResult{err: ErrStopped}
			}
		}
	}
	return p.res.index, p.res.err
}

// Done reports, without blocking, whether the outcome is known; Wait then
// returns it at once.
func (p *Proposal) Done() bool {
	if p.res != nil {
		return true
	}
	select {
	case res := <-p.result:
		p.res = &res
		return true
	case <-p.stopped:
		p.Wait(context.Background())
		return true
	default:
		return false
	}
}

// ReadIndex implements the read-only protocol of §8. It returns once this
// node has confirmed with a majority that it is still the leader, and the
// returned index is committed and already applied to the local state
// machine. Reading the state machine after ReadIndex returns is therefore
// linearizable. Fails with *NotLeaderError or ErrLeadershipLost.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	c := make(chan proposeResult, 1)
	select {
	case n.readc <- c:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.done:
		return 0, ErrStopped
	}
	select {
	case res := <-c:
		return res.index, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.done:
		select {
		case res := <-c:
			return res.index, res.err
		default:
			return 0, ErrStopped
		}
	}
}

// ReadCommitted returns up to max committed entries starting at index from
// (0 means the oldest retained entry). It reads the log store directly from
// the caller's goroutine, which is safe because committed entries are never
// changed, only removed by compaction (then it returns ErrCompacted). The
// LogStore must allow concurrent reads; *wal.Log does.
func (n *Node) ReadCommitted(ctx context.Context, from uint64, max int) ([]Entry, error) {
	st, err := n.Status(ctx)
	if err != nil {
		return nil, err
	}
	log := n.storage.Log
	if from == 0 {
		from = log.FirstIndex()
	}
	if from < log.FirstIndex() {
		return nil, ErrCompacted
	}
	if from > st.CommitIndex || max <= 0 {
		return nil, nil
	}
	last := min(st.CommitIndex, from+uint64(max)-1)
	entries := make([]Entry, 0, last-from+1)
	for i := from; i <= last; i++ {
		b, err := log.Read(i)
		if err != nil {
			if i < log.FirstIndex() {
				return nil, ErrCompacted
			}
			return nil, fmt.Errorf("raft: read entry %d: %w", i, err)
		}
		e, err := decodeEntry(i, b)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Step delivers a message received from another node. Transports call it.
func (n *Node) Step(ctx context.Context, m Message) error {
	select {
	case n.recvc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// Status returns the node's current Raft state.
func (n *Node) Status(ctx context.Context) (Status, error) {
	c := make(chan Status, 1)
	select {
	case n.statusc <- c:
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.done:
		return Status{}, ErrStopped
	}
	return <-c, nil
}

// Done is closed when Run has returned.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err returns the fatal error that stopped Run, if any. Valid after Done.
func (n *Node) Err() error {
	select {
	case <-n.done:
		return n.err
	default:
		return nil
	}
}

type waiter struct {
	term   uint64
	result chan proposeResult
}

// waitingApplier forwards entries to the real Applier and completes pending
// proposals. It is used only by the Run goroutine.
type waitingApplier struct {
	inner   Applier
	waiters map[uint64]waiter
}

func (w *waitingApplier) Apply(e Entry) error {
	if err := w.inner.Apply(e); err != nil {
		return err
	}
	if wt, ok := w.waiters[e.Index]; ok {
		delete(w.waiters, e.Index)
		if wt.term == e.Term {
			wt.result <- proposeResult{index: e.Index}
		} else {
			wt.result <- proposeResult{err: ErrLeadershipLost}
		}
	}
	return nil
}

func (w *waitingApplier) Snapshot() ([]byte, error) { return w.inner.Snapshot() }
func (w *waitingApplier) Restore(data []byte) error { return w.inner.Restore(data) }

func (w *waitingApplier) failAll(err error) {
	for i, wt := range w.waiters {
		wt.result <- proposeResult{err: err}
		delete(w.waiters, i)
	}
}

func (w *waitingApplier) failOtherTerms(term uint64) {
	for i, wt := range w.waiters {
		if wt.term != term {
			wt.result <- proposeResult{err: ErrLeadershipLost}
			delete(w.waiters, i)
		}
	}
}
