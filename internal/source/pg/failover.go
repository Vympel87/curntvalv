package pg

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Postgres failover (design D7).

var (
	// ErrSourceChanged means Postgres is another cluster than the one the
	// stored position came from (another system identifier): a different
	// server, or one restored from a backup.
	ErrSourceChanged = errors.New("pg: postgres is not the cluster the stored position came from; refusing to continue")
	// ErrDiverged means the cluster stored changes that Postgres no longer
	// has, typically after a failover to a standby that had not received
	// them. Continuing would skip new transactions as already stored.
	ErrDiverged = errors.New("pg: the stored position is not in this postgres server's history; refusing to continue (changes may be lost)")
	// ErrNotFailed is returned by Reset when ingest has not stopped.
	ErrNotFailed = errors.New("pg: ingest has not stopped on this node; a source reset is only for a source that failed")

	errStandby = errors.New("pg: connected to a standby (in recovery); waiting for a primary")
)

// server is what a session learns about the Postgres server it reads from.
type server struct {
	systemID uint64
	timeline uint32
	walEnd   uint64 // flushed WAL when connecting
	version  int    // server_version_num
}

// identify asks the server who it is and refuses a standby.
func identify(ctx context.Context, conn *pgx.Conn, rc *pgconn.PgConn) (server, error) {
	var srv server
	res, err := pglogrepl.IdentifySystem(ctx, rc)
	if err != nil {
		return srv, fmt.Errorf("pg: identify system: %w", err)
	}
	if srv.systemID, err = strconv.ParseUint(res.SystemID, 10, 64); err != nil {
		return srv, fmt.Errorf("%w: system identifier %q", ErrProtocol, res.SystemID)
	}
	srv.timeline, srv.walEnd = uint32(res.Timeline), uint64(res.XLogPos)
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery(), current_setting('server_version_num')::int").
		Scan(&inRecovery, &srv.version); err != nil {
		return srv, fmt.Errorf("pg: read server state: %w", err)
	}
	if inRecovery {
		return srv, errStandby
	}
	return srv, nil
}

// storedEnd is the end of the WAL the stored position depends on: the end
// of the last stored transaction, or of the partly stored one's commit.
func storedEnd(pos fsm.SourceState) uint64 {
	if pos.PartialLSN != 0 {
		return max(pos.EndLSN, pos.PartialLSN+1)
	}
	return pos.EndLSN
}

// checkServer compares the stored position with the server it is about to
// be continued on. history returns the switch points of the server's
// timeline history: for each earlier timeline, the LSN where it ended.
func checkServer(ctx context.Context, pos fsm.SourceState, srv server, history func(context.Context) (map[uint32]uint64, error)) error {
	if pos.CommitLSN == 0 && pos.EndLSN == 0 && pos.PartialLSN == 0 {
		return nil // nothing stored yet
	}
	end := storedEnd(pos)
	at := pglogrepl.LSN(end)
	if pos.SystemID != 0 && pos.SystemID != srv.systemID {
		return fmt.Errorf("%w: system identifier %d, the stored position (%s) came from %d", ErrSourceChanged, srv.systemID, at, pos.SystemID)
	}
	if pos.SystemID != 0 && pos.Timeline != 0 {
		switch {
		case srv.timeline < pos.Timeline:
			return fmt.Errorf("%w: the server is on timeline %d, the stored position (%s) is on timeline %d; is this an old primary?",
				ErrDiverged, srv.timeline, at, pos.Timeline)
		case srv.timeline > pos.Timeline:
			h, err := history(ctx)
			if err != nil {
				return err
			}
			switchAt, ok := h[pos.Timeline]
			if !ok {
				return fmt.Errorf("%w: timeline %d of the stored position (%s) is not in the history of the server's timeline %d",
					ErrDiverged, pos.Timeline, at, srv.timeline)
			}
			if end > switchAt {
				return fmt.Errorf("%w: timeline %d ended at %s on this server, but the cluster stored up to %s; "+
					"changes in between were lost in the failover", ErrDiverged, pos.Timeline, pglogrepl.LSN(switchAt), at)
			}
		}
	}
	if srv.walEnd < end {
		return fmt.Errorf("%w: the server's WAL ends at %s, but the cluster stored up to %s", ErrDiverged, pglogrepl.LSN(srv.walEnd), at)
	}
	return nil
}

// timelineHistory reads the history file of a timeline over the
// replication connection.
func timelineHistory(rc *pgconn.PgConn, timeline uint32) func(context.Context) (map[uint32]uint64, error) {
	return func(ctx context.Context) (map[uint32]uint64, error) {
		res, err := pglogrepl.TimelineHistory(ctx, rc, int32(timeline))
		if err != nil {
			return nil, fmt.Errorf("pg: timeline history: %w", err)
		}
		return parseTimelineHistory(res.Content)
	}
}

// parseTimelineHistory parses a timeline history file: lines of
// "TIMELINE<tab>SWITCHPOINT<tab>REASON", meaning TIMELINE ended at
// SWITCHPOINT.
func parseTimelineHistory(b []byte) (map[uint32]uint64, error) {
	out := map[uint32]uint64{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			return nil, fmt.Errorf("%w: timeline history line %q", ErrProtocol, line)
		}
		tli, err := strconv.ParseUint(f[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: timeline history line %q", ErrProtocol, line)
		}
		lsn, err := pglogrepl.ParseLSN(f[1])
		if err != nil {
			return nil, fmt.Errorf("%w: timeline history line %q", ErrProtocol, line)
		}
		out[uint32(tli)] = uint64(lsn)
	}
	return out, sc.Err()
}

// failoverSlots reports whether the server can keep the slot on its
// standbys (Postgres 17 failover slots).
func (srv server) failoverSlots() bool { return srv.version >= 170000 }

// createSlot creates the replication slot, as a failover slot where the
// server supports it, and returns its consistent point.
func (s *Source) createSlot(ctx context.Context, rc *pgconn.PgConn, srv server) (uint64, error) {
	if !srv.failoverSlots() {
		res, err := pglogrepl.CreateReplicationSlot(ctx, rc, s.cfg.Slot, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
		if err != nil {
			return 0, fmt.Errorf("pg: create slot %s: %w", s.cfg.Slot, err)
		}
		lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
		if err != nil {
			return 0, fmt.Errorf("%w: consistent point %q", ErrProtocol, res.ConsistentPoint)
		}
		return uint64(lsn), nil
	}
	// The slot name is checked by New.
	res, err := rc.Exec(ctx, fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (SNAPSHOT 'nothing', FAILOVER true)", s.cfg.Slot)).ReadAll()
	if err != nil {
		return 0, fmt.Errorf("pg: create slot %s: %w", s.cfg.Slot, err)
	}
	if len(res) != 1 || len(res[0].Rows) != 1 || len(res[0].Rows[0]) < 2 {
		return 0, fmt.Errorf("%w: unexpected CREATE_REPLICATION_SLOT result", ErrProtocol)
	}
	lsn, err := pglogrepl.ParseLSN(string(res[0].Rows[0][1]))
	if err != nil {
		return 0, fmt.Errorf("%w: consistent point %q", ErrProtocol, res[0].Rows[0][1])
	}
	return uint64(lsn), nil
}

// enableFailover turns an existing slot into a failover slot, and says once
// per process what else a safe failover needs.
func (s *Source) enableFailover(ctx context.Context, conn *pgx.Conn, rc *pgconn.PgConn, srv server, failover bool) {
	if !srv.failoverSlots() {
		return
	}
	if !failover {
		if _, err := rc.Exec(ctx, fmt.Sprintf("ALTER_REPLICATION_SLOT %s (FAILOVER true)", s.cfg.Slot)).ReadAll(); err != nil {
			s.log.Warn("could not make the slot a failover slot; it will not survive a Postgres failover", "slot", s.cfg.Slot, "err", err)
			return
		}
		s.log.Info("slot is now a failover slot", "slot", s.cfg.Slot)
	}
	s.mu.Lock()
	hinted := s.hinted
	s.hinted = true
	s.mu.Unlock()
	if hinted {
		return
	}
	var standbys string
	if err := conn.QueryRow(ctx, "SELECT current_setting('synchronized_standby_slots')").Scan(&standbys); err == nil && standbys == "" {
		s.log.Info("synchronized_standby_slots is empty: after a Postgres failover ingest stops if this node read changes the standby " +
			"did not have; set it to the standbys' physical slots to make failovers safe (design D7)")
	}
}

// Reset moves the source to the current position of its replication slot
// on the server it connects to now, creating the slot if it is missing, and
// stores an OP_SOURCE_RESET event with the old and the new position. It is
// for a source stopped by a failover check (ErrSlotLost, ErrSlotAhead,
// ErrSourceChanged, ErrDiverged) and works only on the leader whose source
// failed in the current term. Changes between the two positions may be
// missing or repeated; the event tells sinks and subscribers so. Ingest
// starts again afterwards.
func (s *Source) Reset(ctx context.Context) (uint64, *pb.SourceReset, error) {
	st, err := s.node.Status(ctx)
	if err != nil {
		return 0, nil, err
	}
	if st.State != raft.StateLeader {
		return 0, nil, &raft.NotLeaderError{Leader: st.Leader}
	}
	s.mu.Lock()
	if s.health != "failed" || s.failedTerm != st.Term {
		s.mu.Unlock()
		return 0, nil, ErrNotFailed
	}
	reason := s.lastErr
	s.health = "resetting"
	s.mu.Unlock()

	idx, r, err := s.reset(ctx, st.Term, reason)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.health = "failed" // keep the error that stopped ingest
		return 0, nil, err
	}
	s.failedTerm, s.health, s.lastErr = 0, "standby", ""
	s.log.Warn("source reset: ingest continues from the slot's position", "previous", pglogrepl.LSN(r.GetPreviousLsn()),
		"lsn", pglogrepl.LSN(r.GetLsn()), "system_id", r.GetSystemId(), "timeline", r.GetTimeline(), "index", idx)
	return idx, r, nil
}

func (s *Source) reset(ctx context.Context, term uint64, reason string) (uint64, *pb.SourceReset, error) {
	if _, err := s.node.ReadIndex(ctx); err != nil {
		return 0, nil, err
	}
	pos := s.state.Source()
	conn, err := pgx.Connect(ctx, s.cfg.URL)
	if err != nil {
		return 0, nil, fmt.Errorf("pg: connect: %w", err)
	}
	defer conn.Close(context.Background())
	rc, err := s.replicationConn(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer rc.Close(context.Background())
	srv, err := identify(ctx, conn, rc)
	if err != nil {
		return 0, nil, err
	}
	if err := checkSetup(ctx, conn, s.cfg.Publication); err != nil {
		return 0, nil, err
	}
	var confirmed *string
	err = conn.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots
		WHERE slot_name = $1 AND database = current_database()`, s.cfg.Slot).Scan(&confirmed)
	var lsn uint64
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if lsn, err = s.createSlot(ctx, rc, srv); err != nil {
			return 0, nil, err
		}
		s.log.Info("created replication slot", "slot", s.cfg.Slot, "consistent_point", pglogrepl.LSN(lsn))
	case err != nil:
		return 0, nil, fmt.Errorf("pg: read slot: %w", err)
	case confirmed == nil:
		return 0, nil, fmt.Errorf("pg: slot %q has no confirmed position", s.cfg.Slot)
	default:
		l, err := pglogrepl.ParseLSN(*confirmed)
		if err != nil {
			return 0, nil, fmt.Errorf("pg: parse slot position %q: %w", *confirmed, err)
		}
		lsn = uint64(l)
	}
	r := &pb.SourceReset{
		PreviousLsn: storedEnd(pos), PreviousSystemId: pos.SystemID, PreviousTimeline: pos.Timeline,
		Lsn: lsn, SystemId: srv.systemID, Timeline: srv.timeline, Reason: reason,
	}
	tb := &pb.TxBatch{CommitLsn: max(lsn, 1) - 1, EndLsn: lsn, Final: true, CommitTimeUs: time.Now().UnixMicro(),
		SystemId: srv.systemID, Timeline: srv.timeline, Events: []*pb.Event{{Op: pb.Op_OP_SOURCE_RESET, SourceReset: r}}}
	data, err := fsm.EncodeTxBatch(tb)
	if err != nil {
		return 0, nil, err
	}
	p, err := s.node.ProposeAsync(ctx, term, data)
	if err != nil {
		return 0, nil, err
	}
	idx, err := p.Wait(ctx)
	if err != nil {
		return 0, nil, err
	}
	return idx, r, nil
}
