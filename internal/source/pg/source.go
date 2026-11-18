package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Node is what the source needs from Raft. *raft.Node implements it.
type Node interface {
	Propose(ctx context.Context, data []byte) (uint64, error)
	ReadIndex(ctx context.Context) (uint64, error)
	Status(ctx context.Context) (raft.Status, error)
}

// State reads the stored source position and schemas. *fsm.FSM implements it.
type State interface {
	Source() fsm.SourceState
	HasSchema(id uint64) bool
}

// Config configures the Postgres source.
type Config struct {
	// URL is a libpq connection string or URL. The source opens one normal
	// and one replication connection with it.
	URL         string
	Slot        string // logical replication slot, created if the cluster is new
	Publication string // must exist
	// StatusInterval is how often the position is reported to Postgres when
	// nothing else happens. Default 10s; keep it below wal_sender_timeout.
	StatusInterval time.Duration
	Limits         Limits
	Logger         *slog.Logger
}

var (
	// ErrConfig means Postgres is not set up for curntValv.
	ErrConfig = errors.New("pg: postgres is not configured for logical replication")
	// ErrSlotLost means the slot is gone although changes were already
	// stored: Postgres may have dropped WAL we never read (design D7).
	ErrSlotLost = errors.New("pg: replication slot is missing but changes were already stored; refusing to continue (changes may be lost)")
	// ErrSlotAhead means the slot was acknowledged past what the cluster
	// stored, so changes in between are lost (design D7).
	ErrSlotAhead = errors.New("pg: replication slot is ahead of the stored position; refusing to continue (changes may be lost)")
)

var (
	slotName = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	pubName  = regexp.MustCompile(`^[A-Za-z0-9_]{1,63}$`)
)

// Source runs the Postgres ingest on whichever node is the Raft leader.
type Source struct {
	cfg   Config
	node  Node
	state State
	log   *slog.Logger

	mu      sync.Mutex
	health  string // standby, running, retrying, failed
	lastErr string
	stats   Stats
}

// Stats count what this node proposed to the log since it started. After a
// leader change Postgres resends unacknowledged transactions; the state
// machine drops copies (design D3), but they are counted here again.
type Stats struct {
	Batches      uint64 // log entries proposed, including position-only ones
	Transactions uint64 // transactions with changes, counted at their last batch
	Events       uint64 // row changes
}

// Stats returns the counters.
func (s *Source) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Source) count(b *pb.TxBatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Batches++
	s.stats.Events += uint64(len(b.GetEvents()))
	if b.GetFinal() && (len(b.GetEvents()) > 0 || b.GetFirstSeq() > 0) {
		s.stats.Transactions++
	}
}

// Health reports what the source is doing on this node: "standby" (not the
// leader), "running", "retrying" or "failed", and the last error.
func (s *Source) Health() (state, lastErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health, s.lastErr
}

func (s *Source) setHealth(state string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health = state
	switch {
	case err != nil:
		s.lastErr = err.Error()
	case state == "running":
		s.lastErr = ""
	}
}

// New returns a source. It does no I/O.
func New(cfg Config, node Node, state State) (*Source, error) {
	if cfg.URL == "" {
		return nil, errors.New("pg: empty connection URL")
	}
	if !slotName.MatchString(cfg.Slot) {
		return nil, fmt.Errorf("pg: invalid slot name %q (lower case letters, digits, _)", cfg.Slot)
	}
	if !pubName.MatchString(cfg.Publication) {
		return nil, fmt.Errorf("pg: invalid publication name %q (letters, digits, _)", cfg.Publication)
	}
	if cfg.StatusInterval <= 0 {
		cfg.StatusInterval = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Source{cfg: cfg, node: node, state: state, log: cfg.Logger.With("component", "pg-source"), health: "standby"}, nil
}

// isFatal reports errors that retrying cannot fix: they need an operator.
func isFatal(err error) bool {
	return errors.Is(err, ErrConfig) || errors.Is(err, ErrSlotLost) || errors.Is(err, ErrSlotAhead) ||
		errors.Is(err, ErrPartialLost) || errors.Is(err, ErrProtocol)
}

// Run ingests while this node is the leader, until ctx is cancelled. When
// leadership moves away the session stops; transient errors are retried with
// backoff. A fatal error stops ingest on this node and is logged loudly
// until leadership changes or the process restarts.
func (s *Source) Run(ctx context.Context) error {
	const poll = 200 * time.Millisecond
	backoff := time.Second
	var failedTerm uint64 // term in which a fatal error happened
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(poll):
		}
		st, err := s.node.Status(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, raft.ErrStopped) {
				return nil
			}
			continue
		}
		if st.State != raft.StateLeader {
			if h, _ := s.Health(); h != "standby" {
				s.setHealth("standby", nil)
			}
			continue
		}
		if st.Term == failedTerm {
			continue
		}

		err = s.whileLeader(ctx, st.Term)
		switch {
		case ctx.Err() != nil:
			return nil
		case err == nil, errors.Is(err, errNotLeader):
			backoff = time.Second
			s.setHealth("standby", nil)
		case isFatal(err):
			failedTerm = st.Term
			s.setHealth("failed", err)
			s.log.Error("ingest stopped; fix the cause and restart the node or move leadership", "err", err)
		default:
			s.setHealth("retrying", err)
			s.log.Warn("ingest interrupted, retrying", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, 30*time.Second)
		}
	}
}

var errNotLeader = errors.New("pg: no longer the leader")

// whileLeader runs one session and cancels it as soon as this node stops
// being the leader of term.
func (s *Source) whileLeader(ctx context.Context, term uint64) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.session(sctx) }()
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
			st, err := s.node.Status(ctx)
			if err != nil || st.State != raft.StateLeader || st.Term != term {
				cancel()
				<-done
				return errNotLeader
			}
		}
	}
}

// session connects to Postgres and streams changes into the log until an
// error happens or ctx is cancelled.
func (s *Source) session(ctx context.Context) error {
	// Make sure the state machine reflects everything committed so far, and
	// that we are still the leader, before reading the source position.
	if _, err := s.node.ReadIndex(ctx); err != nil {
		return err
	}
	pos := s.state.Source()

	conn, err := pgx.Connect(ctx, s.cfg.URL)
	if err != nil {
		return fmt.Errorf("pg: connect: %w", err)
	}
	defer conn.Close(context.Background())
	create, err := s.prepare(ctx, conn, pos)
	if err != nil {
		return err
	}

	rcfg, err := pgconn.ParseConfig(s.cfg.URL)
	if err != nil {
		return fmt.Errorf("%w: bad connection URL: %v", ErrConfig, err)
	}
	rcfg.RuntimeParams["replication"] = "database"
	rc, err := pgconn.ConnectConfig(ctx, rcfg)
	if err != nil {
		return fmt.Errorf("pg: replication connect: %w", err)
	}
	defer rc.Close(context.Background())

	if create {
		res, err := pglogrepl.CreateReplicationSlot(ctx, rc, s.cfg.Slot, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
		if err != nil {
			return fmt.Errorf("pg: create slot %s: %w", s.cfg.Slot, err)
		}
		s.log.Info("created replication slot", "slot", s.cfg.Slot, "consistent_point", res.ConsistentPoint)
	}

	start := pglogrepl.LSN(pos.EndLSN)
	err = pglogrepl.StartReplication(ctx, rc, s.cfg.Slot, start, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", s.cfg.Publication)},
	})
	if err != nil {
		return fmt.Errorf("pg: start replication: %w", err)
	}
	s.setHealth("running", nil)
	s.log.Info("ingest started", "slot", s.cfg.Slot, "from", start,
		"stored_commit_lsn", pglogrepl.LSN(pos.CommitLSN), "partial_lsn", pglogrepl.LSN(pos.PartialLSN))
	return s.stream(ctx, rc, pos)
}

// prepare checks the Postgres setup, fences off a previous leader's
// connection and decides whether the slot must be created.
func (s *Source) prepare(ctx context.Context, conn *pgx.Conn, pos fsm.SourceState) (create bool, err error) {
	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return false, fmt.Errorf("pg: read wal_level: %w", err)
	}
	if walLevel != "logical" {
		return false, fmt.Errorf("%w: wal_level is %q, need logical", ErrConfig, walLevel)
	}
	var pubExists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", s.cfg.Publication).Scan(&pubExists); err != nil {
		return false, fmt.Errorf("pg: check publication: %w", err)
	}
	if !pubExists {
		return false, fmt.Errorf("%w: publication %q does not exist (CREATE PUBLICATION %s FOR TABLE ...)", ErrConfig, s.cfg.Publication, s.cfg.Publication)
	}

	fresh := pos.CommitLSN == 0 && pos.PartialLSN == 0
	for attempt := 0; ; attempt++ {
		var plugin string
		var activePID *int32
		var confirmed *string
		err := conn.QueryRow(ctx, `SELECT plugin, active_pid, confirmed_flush_lsn::text FROM pg_replication_slots
			WHERE slot_name = $1 AND database = current_database()`, s.cfg.Slot).Scan(&plugin, &activePID, &confirmed)
		if errors.Is(err, pgx.ErrNoRows) {
			if !fresh {
				return false, fmt.Errorf("%w: slot %q (stored position %s)", ErrSlotLost, s.cfg.Slot, pglogrepl.LSN(pos.EndLSN))
			}
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("pg: read slot: %w", err)
		}
		if plugin != "pgoutput" {
			return false, fmt.Errorf("%w: slot %q uses plugin %q, need pgoutput", ErrConfig, s.cfg.Slot, plugin)
		}
		if confirmed != nil && !fresh {
			lsn, err := pglogrepl.ParseLSN(*confirmed)
			if err != nil {
				return false, fmt.Errorf("pg: parse slot position %q: %w", *confirmed, err)
			}
			if uint64(lsn) > pos.EndLSN {
				return false, fmt.Errorf("%w: slot at %s, stored up to %s", ErrSlotAhead, lsn, pglogrepl.LSN(pos.EndLSN))
			}
		}
		if activePID == nil {
			return false, nil
		}
		// Fencing (design D3): the slot is still used, normally by the
		// previous leader. That node can no longer commit anything, but
		// Postgres allows only one reader per slot.
		if attempt == 0 {
			s.log.Info("terminating previous reader of the slot", "slot", s.cfg.Slot, "pid", *activePID)
		}
		if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend($1)", *activePID); err != nil {
			return false, fmt.Errorf("pg: terminate previous slot reader: %w", err)
		}
		if attempt >= 50 {
			return false, fmt.Errorf("pg: slot %q still in use by pid %d", s.cfg.Slot, *activePID)
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// stream reads the replication stream and stores batches until an error.
func (s *Source) stream(ctx context.Context, rc *pgconn.PgConn, pos fsm.SourceState) error {
	dec := newDecoder(s.state, s.cfg.Limits, pos)
	ack := pos.EndLSN
	sentAck := uint64(0)
	nextStatus := time.Now()
	sendStatus := func() error {
		err := pglogrepl.SendStandbyStatusUpdate(ctx, rc, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(ack),
			WALFlushPosition: pglogrepl.LSN(ack),
			WALApplyPosition: pglogrepl.LSN(ack),
			ClientTime:       time.Now(),
		})
		if err != nil {
			return fmt.Errorf("pg: send status: %w", err)
		}
		sentAck = ack
		nextStatus = time.Now().Add(s.cfg.StatusInterval)
		return nil
	}
	propose := func(tb *pb.TxBatch) error {
		data, err := fsm.EncodeTxBatch(tb)
		if err != nil {
			return err
		}
		if _, err := s.node.Propose(ctx, data); err != nil {
			return err
		}
		s.count(tb)
		return nil
	}

	for {
		if time.Now().After(nextStatus) || (ack > sentAck && time.Until(nextStatus) < s.cfg.StatusInterval-time.Second) {
			// Transactions without published changes still move the slot
			// forward, so Postgres can free WAL. Store the newest one first:
			// we only ever acknowledge what the cluster has stored.
			if e := dec.takeEmpty(); e != nil {
				if err := propose(e); err != nil {
					return err
				}
				ack = e.EndLsn
			}
			if err := sendStatus(); err != nil {
				return err
			}
		}

		rctx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := rc.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("pg: receive: %w", err)
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("pg: replication error: %s (%s)", msg.Message, msg.Code)
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("%w: keepalive: %v", ErrProtocol, err)
				}
				dec.idle(uint64(ka.ServerWALEnd))
				if ka.ReplyRequested {
					nextStatus = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("%w: xlog data: %v", ErrProtocol, err)
				}
				m, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("%w: %v", ErrProtocol, err)
				}
				batches, err := dec.handle(m)
				if err != nil {
					return err
				}
				for _, b := range batches {
					if err := propose(b); err != nil {
						return err
					}
					if b.Final {
						ack = b.EndLsn
					}
				}
			}
		}
	}
}
