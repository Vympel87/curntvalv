// Package sink delivers the changes in the log to external systems (design
// D4). A Runner reads changes after the sink's stored offset, hands them to
// a Sink and then stores the new offset through Raft. Delivery is
// at-least-once: a crash between delivering and storing the offset makes
// the next leader deliver those changes again, so sinks use each change's
// ID to make redelivery harmless.
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Sink delivers changes to one external system. Deliver returns nil only
// when every change was delivered. After an error it is called again with
// the same changes, and after a leader change some changes can be
// delivered twice: a sink must tolerate both, using Change.ID.
type Sink interface {
	Deliver(ctx context.Context, changes []event.Change) error
}

// Node is what a runner needs from Raft. *raft.Node implements it.
type Node interface {
	Propose(ctx context.Context, data []byte) (uint64, error)
	ReadIndex(ctx context.Context) (uint64, error)
	Status(ctx context.Context) (raft.Status, error)
}

// Reader reads changes from the local log. *event.Reader implements it.
type Reader interface {
	Read(ctx context.Context, from uint64, max int, filter event.Filter) (event.Result, error)
}

// Offsets reads stored sink positions. *fsm.FSM implements it.
type Offsets interface {
	SinkPosition(name string) (fsm.SinkPosition, bool)
}

// Config configures one runner.
type Config struct {
	// Name identifies the sink; its offset is stored under it. Every node
	// must be started with the same sinks.
	Name string
	// Type is shown in status output ("webhook", "clickhouse", ...).
	Type string
	// Tables limits the changes delivered; empty means all tables.
	Tables event.Filter
	// StartLatest makes a new sink (one without a stored offset) skip the
	// history and start with the next change. By default it starts with
	// the oldest retained entry.
	StartLatest bool
	// MaxEntries is how many log entries one read covers (default 100).
	MaxEntries int
	// MaxBatch is the most changes passed to one Deliver call (default 500).
	MaxBatch int
	// RetryMin and RetryMax bound the backoff after a failed delivery
	// (defaults 100ms and 30s).
	RetryMin, RetryMax time.Duration
	// SkipCommitInterval is how often the offset is stored when entries are
	// read but none of their changes pass Tables (default 1s).
	SkipCommitInterval time.Duration
	Logger             *slog.Logger
}

// ErrCompacted means the sink's offset points before the oldest retained
// entry: changes it has not delivered were removed by log compaction.
var ErrCompacted = errors.New("sink: undelivered entries were removed by log compaction")

// State is what a runner is doing.
type State string

const (
	StateStandby  State = "standby"  // not the leader; another node delivers
	StateRunning  State = "running"  // delivering or waiting for changes
	StateRetrying State = "retrying" // the last delivery failed
	StateFailed   State = "failed"   // stopped until a reset, a leadership change or a restart
)

// Status is a runner's view of its sink. The stored offset is in the state
// machine, not here.
type Status struct {
	Name      string
	Type      string
	State     State
	Delivered uint64 // changes delivered by this node since it started
	Failures  uint64 // failed Deliver calls on this node since it started
	LastError string
}

// Runner delivers changes to one sink on whichever node is the leader.
type Runner struct {
	cfg     Config
	sink    Sink
	node    Node
	reader  Reader
	offsets Offsets
	log     *slog.Logger

	mu     sync.Mutex
	status Status
}

// NewRunner returns a runner. It does no I/O.
func NewRunner(cfg Config, s Sink, node Node, reader Reader, offsets Offsets) (*Runner, error) {
	if !fsm.ValidSinkName(cfg.Name) {
		return nil, fmt.Errorf("sink: invalid name %q (lower case letters, digits, _ and -)", cfg.Name)
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 100
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 500
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = 100 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.SkipCommitInterval <= 0 {
		cfg.SkipCommitInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Runner{
		cfg: cfg, sink: s, node: node, reader: reader, offsets: offsets,
		log:    cfg.Logger.With("component", "sink", "sink", cfg.Name),
		status: Status{Name: cfg.Name, Type: cfg.Type, State: StateStandby},
	}, nil
}

// Status returns the runner's current status.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *Runner) setState(s State, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.State = s
	if err != nil {
		r.status.LastError = err.Error()
	} else if s == StateRunning {
		r.status.LastError = ""
	}
}

func (r *Runner) addFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.Failures++
}

func (r *Runner) addDelivered(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.Delivered += uint64(n)
}

// Run delivers while this node is the leader, until ctx is cancelled. Errors
// are retried; only ErrCompacted stops the sink, until it is reset,
// leadership changes or the process restarts.
func (r *Runner) Run(ctx context.Context) error {
	const poll = 200 * time.Millisecond
	backoff := r.cfg.RetryMin
	var failedTerm, failedGen uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(poll):
		}
		st, err := r.node.Status(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, raft.ErrStopped) {
				return nil
			}
			continue
		}
		if st.State != raft.StateLeader {
			if r.Status().State != StateStandby {
				r.setState(StateStandby, nil)
			}
			continue
		}
		if st.Term == failedTerm && r.gen() == failedGen {
			continue
		}
		err = r.whileLeader(ctx, st.Term)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, errReset):
			backoff = r.cfg.RetryMin
			r.log.Info("sink reset; restarting from the new offset")
		case err == nil, errors.Is(err, errNotLeader):
			backoff = r.cfg.RetryMin
			r.setState(StateStandby, nil)
		case errors.Is(err, ErrCompacted):
			failedTerm, failedGen = st.Term, r.gen()
			r.setState(StateFailed, err)
			r.log.Error("sink stopped; it is further behind than the retained log", "err", err)
		default:
			r.setState(StateRetrying, err)
			r.log.Warn("sink interrupted, retrying", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, r.cfg.RetryMax)
		}
	}
}

var (
	errNotLeader = errors.New("sink: no longer the leader")
	errReset     = errors.New("sink: reset")
)

// gen returns the sink's stored generation.
func (r *Runner) gen() uint64 {
	pos, _ := r.offsets.SinkPosition(r.cfg.Name)
	return pos.Gen
}

// whileLeader runs one session and cancels it as soon as this node stops
// being the leader of term.
func (r *Runner) whileLeader(ctx context.Context, term uint64) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.session(sctx) }()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return errNotLeader
			}
			return err
		case <-tick.C:
			st, err := r.node.Status(ctx)
			if err != nil || st.State != raft.StateLeader || st.Term != term {
				cancel()
				<-done
				return errNotLeader
			}
		}
	}
}

// session delivers from the stored offset until an error happens or ctx is
// cancelled.
func (r *Runner) session(ctx context.Context) error {
	// Make sure the state machine holds every offset committed so far.
	idx, err := r.node.ReadIndex(ctx)
	if err != nil {
		return err
	}
	pos, ok := r.offsets.SinkPosition(r.cfg.Name)
	offset, gen := pos.Offset, pos.Gen
	if !ok && r.cfg.StartLatest {
		if err := r.commit(ctx, gen, idx); err != nil {
			return err
		}
		offset = idx
	}
	from := uint64(0) // oldest retained entry
	if offset > 0 {
		from = offset + 1
	}
	r.setState(StateRunning, nil)
	r.log.Info("sink started", "offset", offset, "generation", gen)

	var pending uint64 // offset to store for entries without matching changes
	lastCommit := time.Now()
	for {
		rctx, cancel := ctx, context.CancelFunc(func() {})
		if pending > 0 {
			rctx, cancel = context.WithDeadline(ctx, lastCommit.Add(r.cfg.SkipCommitInterval))
		}
		res, err := r.reader.Read(rctx, from, r.cfg.MaxEntries, r.cfg.Tables)
		cancel()
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case r.gen() != gen:
			// Reset: what was read belongs to the old position.
			return errReset
		case errors.Is(err, context.DeadlineExceeded):
			// Idle with skipped entries: store how far we got.
			if err := r.commit(ctx, gen, pending); err != nil {
				return err
			}
			pending, lastCommit = 0, time.Now()
			continue
		case errors.Is(err, raft.ErrCompacted):
			return fmt.Errorf("%w (reading from %d)", ErrCompacted, from)
		case err != nil:
			return err
		}
		from = res.Next
		if len(res.Changes) == 0 {
			if res.TxEntries > 0 {
				pending = res.Next - 1
			}
			if pending > 0 && time.Since(lastCommit) >= r.cfg.SkipCommitInterval {
				if err := r.commit(ctx, gen, pending); err != nil {
					return err
				}
				pending, lastCommit = 0, time.Now()
			}
			continue
		}
		if err := r.deliver(ctx, res.Changes); err != nil {
			return err
		}
		if err := r.commit(ctx, gen, res.Next-1); err != nil {
			return err
		}
		pending, lastCommit = 0, time.Now()
	}
}

// deliver hands changes to the sink in batches, retrying each batch with
// backoff until it succeeds or ctx is cancelled.
func (r *Runner) deliver(ctx context.Context, changes []event.Change) error {
	for len(changes) > 0 {
		batch := changes[:min(len(changes), r.cfg.MaxBatch)]
		backoff := r.cfg.RetryMin
		for {
			err := r.sink.Deliver(ctx, batch)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.setState(StateRetrying, err)
			r.addFailure()
			r.log.Warn("delivery failed, retrying", "err", err, "first", batch[0].ID(), "changes", len(batch), "retry_in", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, r.cfg.RetryMax)
		}
		r.setState(StateRunning, nil)
		r.addDelivered(len(batch))
		changes = changes[len(batch):]
	}
	return nil
}

// commit stores offset for generation gen through Raft.
func (r *Runner) commit(ctx context.Context, gen, offset uint64) error {
	data, err := fsm.EncodeSinkOffset(r.cfg.Name, gen, offset)
	if err != nil {
		return err
	}
	if _, err := r.node.Propose(ctx, data); err != nil {
		return fmt.Errorf("sink: store offset %d: %w", offset, err)
	}
	return nil
}
