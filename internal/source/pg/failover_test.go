package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func TestParseTimelineHistory(t *testing.T) {
	h, err := parseTimelineHistory([]byte("1\t0/3000000\tno recovery target specified\n\n2\t0/5000A28\tbefore 2026-11-25\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 2 || h[1] != 0x3000000 || h[2] != 0x5000A28 {
		t.Fatalf("%v", h)
	}
	if _, err := parseTimelineHistory([]byte("x\t0/1\n")); !errors.Is(err, ErrProtocol) {
		t.Fatalf("bad line: %v", err)
	}
}

func TestCheckServer(t *testing.T) {
	history := func(h map[uint32]uint64) func(context.Context) (map[uint32]uint64, error) {
		return func(context.Context) (map[uint32]uint64, error) { return h, nil }
	}
	noHistory := history(nil)
	at := func(end uint64, sys uint64, tli uint32) fsm.SourceState {
		return fsm.SourceState{CommitLSN: end - 8, EndLSN: end, SystemID: sys, Timeline: tli}
	}
	srv := server{systemID: 42, timeline: 3, walEnd: 10_000}
	for _, tc := range []struct {
		name    string
		pos     fsm.SourceState
		srv     server
		history func(context.Context) (map[uint32]uint64, error)
		want    error
	}{
		{"nothing stored", fsm.SourceState{}, server{systemID: 1, timeline: 1}, noHistory, nil},
		{"same timeline", at(5000, 42, 3), srv, noHistory, nil},
		{"stored by v0.2", at(5000, 0, 0), srv, noHistory, nil},
		{"stored by v0.2, past the WAL", at(20_000, 0, 0), srv, noHistory, ErrDiverged},
		{"other cluster", at(5000, 7, 3), srv, noHistory, ErrSourceChanged},
		{"past the WAL", at(20_000, 42, 3), srv, noHistory, ErrDiverged},
		{"old primary", at(5000, 42, 4), srv, noHistory, ErrDiverged},
		{"failover, position before the switch", at(5000, 42, 2), srv, history(map[uint32]uint64{1: 1000, 2: 6000}), nil},
		{"failover, position at the switch", at(6000, 42, 2), srv, history(map[uint32]uint64{1: 1000, 2: 6000}), nil},
		{"failover, position after the switch", at(6008, 42, 2), srv, history(map[uint32]uint64{1: 1000, 2: 6000}), ErrDiverged},
		{"two failovers", at(900, 42, 1), srv, history(map[uint32]uint64{1: 1000, 2: 6000}), nil},
		{"two failovers, diverged on the first", at(1100, 42, 1), srv, history(map[uint32]uint64{1: 1000, 2: 6000}), ErrDiverged},
		{"not an ancestor", at(5000, 42, 2), srv, history(map[uint32]uint64{1: 1000}), ErrDiverged},
		{"partial transaction after the switch", fsm.SourceState{CommitLSN: 4000, EndLSN: 4008, PartialLSN: 6000, SystemID: 42, Timeline: 2},
			srv, history(map[uint32]uint64{2: 6000}), ErrDiverged},
		{"partial transaction before the switch", fsm.SourceState{CommitLSN: 4000, EndLSN: 4008, PartialLSN: 5000, SystemID: 42, Timeline: 2},
			srv, history(map[uint32]uint64{2: 6000}), nil},
	} {
		err := checkServer(context.Background(), tc.pos, tc.srv, tc.history)
		if (tc.want == nil) != (err == nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
			t.Errorf("%s: got %v, want %v", tc.name, err, tc.want)
		}
		if err != nil && !isFatal(err) {
			t.Errorf("%s: %v is not fatal", tc.name, err)
		}
	}
	if err := checkServer(context.Background(), at(5000, 42, 2), srv, func(context.Context) (map[uint32]uint64, error) {
		return nil, errors.New("connection reset")
	}); err == nil || isFatal(err) {
		t.Errorf("history error should be retried: %v", err)
	}
}

// systemID returns the test server's system identifier.
func (e *pgEnv) systemID() uint64 {
	e.t.Helper()
	var id int64
	if err := e.db.QueryRow(context.Background(), "SELECT system_identifier FROM pg_control_system()").Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return uint64(id)
}

func (e *pgEnv) walEnd() uint64 {
	e.t.Helper()
	var lsn string
	e.db.QueryRow(context.Background(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&lsn)
	var hi, lo uint64
	fmt.Sscanf(lsn, "%X/%X", &hi, &lo)
	return hi<<32 | lo
}

// store proposes a batch as if the source had read it.
func (n *testNode) store(tb *pb.TxBatch) {
	n.t.Helper()
	data, err := fsm.EncodeTxBatch(tb)
	if err != nil {
		n.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitFor(n.t, "leader", 5*time.Second, func() bool {
		st, _ := n.node.Status(ctx)
		return st.State == raft.StateLeader
	})
	if _, err := n.node.Propose(ctx, data); err != nil {
		n.t.Fatal(err)
	}
}

func TestIngestRecordsTheServer(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	runSource(t, e.config(Limits{}), n)
	e.waitSlot()
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", e.table))
	waitFor(t, "event", 10*time.Second, func() bool { return len(n.events()) == 1 })
	if got := n.fsm.Source(); got.SystemID != e.systemID() || got.Timeline == 0 {
		t.Fatalf("source %+v, system %d", got, e.systemID())
	}
	var version int
	e.db.QueryRow(context.Background(), "SELECT current_setting('server_version_num')::int").Scan(&version)
	if version >= 170000 {
		var failover bool
		e.db.QueryRow(context.Background(), "SELECT failover FROM pg_replication_slots WHERE slot_name = $1", e.slot).Scan(&failover)
		if !failover {
			t.Fatal("slot created without FAILOVER on Postgres 17")
		}
	}
}

func TestExistingSlotBecomesFailoverSlot(t *testing.T) {
	e := newPGEnv(t)
	var version int
	e.db.QueryRow(context.Background(), "SELECT current_setting('server_version_num')::int").Scan(&version)
	if version < 170000 {
		t.Skip("failover slots need Postgres 17")
	}
	e.exec("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", e.slot)
	n := startNode(t, t.TempDir())
	runSource(t, e.config(Limits{}), n)
	waitFor(t, "failover slot", 10*time.Second, func() bool {
		var failover bool
		e.db.QueryRow(context.Background(), "SELECT failover FROM pg_replication_slots WHERE slot_name = $1", e.slot).Scan(&failover)
		return failover
	})
}

func TestServerChecksAreFatal(t *testing.T) {
	e := newPGEnv(t)
	sys, end := e.systemID(), e.walEnd()
	var tli uint32
	e.db.QueryRow(context.Background(), "SELECT timeline_id FROM pg_control_checkpoint()").Scan(&tli)
	for _, tc := range []struct {
		name string
		tb   *pb.TxBatch
		want error
		msg  string
	}{
		{"another cluster", &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true, SystemId: sys + 1, Timeline: tli}, ErrSourceChanged, "system identifier"},
		{"a later timeline", &pb.TxBatch{CommitLsn: 100, EndLsn: 108, Final: true, SystemId: sys, Timeline: tli + 1}, ErrDiverged, "old primary"},
		{"past the WAL", &pb.TxBatch{CommitLsn: end + 1<<20, EndLsn: end + 1<<20 + 8, Final: true, SystemId: sys, Timeline: tli}, ErrDiverged, "WAL ends"},
	} {
		n := startNode(t, t.TempDir())
		n.store(tc.tb)
		err := sessionErr(t, e.config(Limits{}), n)
		if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("%s: got %v", tc.name, err)
		}
		n.stop()
	}
	// Nothing was created on the way: the checks come before the slot.
	var exists bool
	e.db.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", e.slot).Scan(&exists)
	if exists {
		t.Fatal("slot created despite a failed check")
	}
}

// After a lost slot stops ingest, a reset creates a new slot, stores the
// reset event, and ingest continues.
func TestResetAfterLostSlot(t *testing.T) {
	e := newPGEnv(t)
	n := startNode(t, t.TempDir())
	src, stop := runSourceGet(t, e.config(Limits{}), n)
	e.waitSlot()
	ctx := context.Background()
	if _, _, err := src.Reset(ctx); !errors.Is(err, ErrNotFailed) {
		t.Fatalf("reset of a running source: %v", err)
	}
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", e.table))
	waitFor(t, "event", 10*time.Second, func() bool { return len(n.events()) == 1 })
	stop()
	waitFor(t, "slot released", 10*time.Second, func() bool {
		_, err := e.db.Exec(ctx, "SELECT pg_drop_replication_slot($1)", e.slot)
		return err == nil
	})
	before := n.fsm.Source()
	src, _ = runSourceGet(t, e.config(Limits{}), n)
	waitFor(t, "failed source", 10*time.Second, func() bool { h, _ := src.Health(); return h == "failed" })
	if _, msg := src.Health(); !strings.Contains(msg, "slot") {
		t.Fatalf("health error %q", msg)
	}

	idx, r, err := src.Reset(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.GetPreviousLsn() != before.EndLSN || r.GetLsn() == 0 || r.GetSystemId() != e.systemID() || !strings.Contains(r.GetReason(), "slot") {
		t.Fatalf("reset %v", r)
	}
	if got := n.fsm.Source(); got.EndLSN != r.GetLsn() || got.SystemID != r.GetSystemId() {
		t.Fatalf("source after reset %+v", got)
	}
	evs := n.events()
	if len(evs) != 2 || evs[1].op != pb.Op_OP_SOURCE_RESET {
		t.Fatalf("events %+v", evs)
	}
	es, _ := n.node.ReadCommitted(ctx, idx, 1)
	if tb, _, _ := fsm.DecodeTxBatch(es[0].Data); fsm.SourceReset(tb) == nil {
		t.Fatalf("entry %d is not the reset", idx)
	}
	// Ingest starts again on its own and takes new changes.
	waitFor(t, "running source", 10*time.Second, func() bool { h, _ := src.Health(); return h == "running" })
	e.exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'b')", e.table))
	waitFor(t, "event after the reset", 10*time.Second, func() bool { return len(n.events()) == 3 })
	if evs := n.events(); evs[2].id != "2" {
		t.Fatalf("events %+v", evs)
	}
}
