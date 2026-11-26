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
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
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
	// Shape filters and projects the changes further (design D21); the
	// zero value changes nothing.
	Shape Shape
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
	// SkipPoison passes over a change the sink keeps refusing while it
	// accepts others (on_error=skip, design D28), and records it through
	// Raft. By default a failing change is retried until it succeeds.
	SkipPoison bool
	// MaxAttempts is how often a batch, and then a single change, is tried
	// before SkipPoison looks for a poison change (default 5).
	MaxAttempts int
	// Transactions delivers whole transactions (design D30): a Deliver call
	// never holds part of one, unless it has more than MaxTxChanges changes
	// for this sink, and each change carries TxChanges. With SkipPoison a
	// refused transaction is skipped whole.
	Transactions bool
	// MaxTxChanges bounds the changes of one transaction held in memory
	// (default 100000); a larger one is delivered in parts.
	MaxTxChanges int
	// SchemaChanges delivers a change with op schema before the first change
	// of a table under a definition new to the log (design D31). By default
	// they are left out.
	SchemaChanges bool
	// SkipCommitInterval is how often the offset is stored when entries are
	// read but none of their changes pass Tables and Shape (default 1s).
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
	Skipped   uint64 // changes passed over by this node since it started
	LastError string
}

// Runner delivers changes to one sink on whichever node is the leader.
type Runner struct {
	cfg     Config
	sink    Sink
	node    Node
	reader  Reader
	offsets Offsets
	shape   *shaper
	// canary is the last unit the sink accepted: delivering it again tells
	// whether the sink is up (design D28).
	canary unit
	log    *slog.Logger

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
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.MaxTxChanges <= 0 {
		cfg.MaxTxChanges = 100000
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
		cfg: cfg, sink: s, node: node, reader: reader, offsets: offsets, shape: newShaper(cfg.Shape),
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
	tx := event.NewGrouper(r.cfg.MaxTxChanges)
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
			offset, pending, lastCommit = pending, 0, time.Now()
			continue
		case errors.Is(err, raft.ErrCompacted):
			return fmt.Errorf("%w (reading from %d)", ErrCompacted, from)
		case err != nil:
			return err
		}
		from = res.Next
		changes := res.Changes
		if !r.cfg.SchemaChanges {
			changes = event.WithoutSchemaChanges(changes)
		}
		changes = r.shape.Apply(changes)
		units := singles(changes)
		safe := res.Next - 1
		if r.cfg.Transactions {
			units = nil
			for _, g := range tx.Add(changes, res.Txs) {
				units = append(units, g)
			}
			safe = tx.Safe(res.Next)
			if lsn := tx.Splitting(); lsn != 0 && len(units) > 0 && units[len(units)-1][0].CommitLSN == lsn {
				r.log.Warn("transaction too large to deliver whole; delivering it in parts",
					"lsn", event.FormatLSN(lsn), "max_tx", r.cfg.MaxTxChanges)
			}
		}
		if len(units) == 0 {
			if res.TxEntries > 0 && safe > offset {
				pending = safe
			}
			if pending > 0 && time.Since(lastCommit) >= r.cfg.SkipCommitInterval {
				if err := r.commit(ctx, gen, pending); err != nil {
					return err
				}
				offset, pending, lastCommit = pending, 0, time.Now()
			}
			continue
		}
		if err := r.deliver(ctx, units); err != nil {
			return err
		}
		pending = 0
		if safe > offset {
			if err := r.commit(ctx, gen, safe); err != nil {
				return err
			}
			offset, lastCommit = safe, time.Now()
		}
	}
}

// deliver hands units to the sink in batches of whole units, up to
// MaxBatch changes unless one unit alone is larger, retrying each batch
// with backoff until it succeeds or ctx is cancelled. With SkipPoison a
// batch that keeps failing is delivered one unit at a time, passing over
// poison units.
func (r *Runner) deliver(ctx context.Context, units []unit) error {
	for len(units) > 0 {
		n, size := 1, len(units[0])
		for n < len(units) && size+len(units[n]) <= r.cfg.MaxBatch {
			size += len(units[n])
			n++
		}
		batch := units[:n]
		changes := flatten(batch)
		backoff := r.cfg.RetryMin
		for attempt := 1; ; attempt++ {
			err := r.sink.Deliver(ctx, changes)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.setState(StateRetrying, err)
			r.addFailure()
			if r.cfg.SkipPoison && attempt >= r.cfg.MaxAttempts {
				if err := r.oneByOne(ctx, batch, units[n:]); err != nil {
					return err
				}
				break
			}
			r.log.Warn("delivery failed, retrying", "err", err, "first", changes[0].ID(), "changes", len(changes), "retry_in", backoff)
			if err := r.wait(ctx, backoff); err != nil {
				return err
			}
			backoff = min(2*backoff, r.cfg.RetryMax)
		}
		r.setState(StateRunning, nil)
		r.addDelivered(len(changes))
		r.canary = batch[n-1]
		units = units[n:]
	}
	return nil
}

// unit is what a runner delivers in one piece: one change, or with
// Transactions every change of one transaction the sink receives.
type unit []event.Change

// singles makes every change a unit.
func singles(changes []event.Change) []unit {
	out := make([]unit, len(changes))
	for i := range changes {
		out[i] = changes[i : i+1 : i+1]
	}
	return out
}

// flatten joins units into one slice of changes.
func flatten(units []unit) []event.Change {
	if len(units) == 1 {
		return units[0]
	}
	var out []event.Change
	for _, u := range units {
		out = append(out, u...)
	}
	return out
}

func (r *Runner) wait(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// oneByOne delivers batch unit by unit (design D28). A unit that fails
// MaxAttempts times is poison if the sink still accepts another unit, the
// canary or a later one: it is recorded and passed over. Otherwise the sink
// is taken to be down and the unit is retried.
func (r *Runner) oneByOne(ctx context.Context, batch, later []unit) error {
	for i := 0; i < len(batch); i++ {
		u := batch[i]
		backoff := r.cfg.RetryMin
		for {
			var err error
			for attempt := 0; attempt < r.cfg.MaxAttempts; attempt++ {
				if err = r.sink.Deliver(ctx, u); err == nil || ctx.Err() != nil {
					break
				}
				r.addFailure()
				if attempt+1 < r.cfg.MaxAttempts {
					if err := r.wait(ctx, r.cfg.RetryMin); err != nil {
						return err
					}
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				r.canary = u
				break
			}
			if r.sinkAccepts(ctx, append(batch[i+1:len(batch):len(batch)], later...)) {
				if err := r.recordSkip(ctx, u, err); err != nil {
					return err
				}
				break
			}
			r.setState(StateRetrying, err)
			r.log.Warn("delivery failed and the sink accepts nothing; retrying", "err", err, "change", u[0].ID(), "retry_in", backoff)
			if err := r.wait(ctx, backoff); err != nil {
				return err
			}
			backoff = min(2*backoff, r.cfg.RetryMax)
		}
	}
	return nil
}

// sinkAccepts reports whether the sink accepts some other unit: the canary,
// or else the first of later (delivering it early is harmless: it is
// delivered again in its turn).
func (r *Runner) sinkAccepts(ctx context.Context, later []unit) bool {
	probe := r.canary
	if probe == nil && len(later) > 0 {
		probe = later[0]
	}
	if probe == nil {
		return false
	}
	return r.sink.Deliver(ctx, probe) == nil
}

// recordSkip stores that u was passed over. A transaction is recorded by
// its first change.
func (r *Runner) recordSkip(ctx context.Context, u unit, cause error) error {
	c := u[0]
	msg := cause.Error()
	if len(u) > 1 {
		msg = fmt.Sprintf("transaction of %d changes: %s", len(u), msg)
	}
	if len(msg) > 500 {
		msg = msg[:500]
	}
	data, err := fsm.EncodeSinkSkip(&pb.SinkSkip{Sink: r.cfg.Name, Index: c.Index, Id: c.ID(), Error: msg, TimeUs: time.Now().UnixMicro()})
	if err != nil {
		return err
	}
	if _, err := r.node.Propose(ctx, data); err != nil {
		return fmt.Errorf("sink: record skipped change %s: %w", c.ID(), err)
	}
	r.mu.Lock()
	r.status.Skipped += uint64(len(u))
	r.mu.Unlock()
	r.log.Error("skipped: the sink refuses it but accepts others", "change", c.ID(), "changes", len(u), "index", c.Index, "err", cause)
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
