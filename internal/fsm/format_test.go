package fsm

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/compat"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/wal"
)

// On-disk formats (design D34). testdata/v1 holds files written by
// curntValv 1.0: the data directory of a one-node cluster (WAL, Raft state,
// a snapshot with a membership configuration) and a state machine snapshot
// with every section. Every later 1.x release must read them and arrive at
// the state recorded in expect.txt. The files are written once, when
// missing (make baseline), and never again: a test that fails here means
// the new code cannot read what an older release left on disk.

const fixtureDir = "testdata/v1"

type nopTransport struct{}

func (nopTransport) Send(raft.Message) {}

// fixtureNode runs a one-node cluster on dir until the returned stop is
// called.
func fixtureNode(t *testing.T, dir string, snapshotEntries int) (*raft.Node, *FSM, func()) {
	t.Helper()
	log, err := wal.Open(filepath.Join(dir, "wal"), wal.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dir, "snapshots"), true)
	if err != nil {
		t.Fatal(err)
	}
	f := New()
	n, err := raft.NewNode(raft.Config{ID: 1, Peers: []uint64{1}, Addrs: map[uint64]string{1: "127.0.0.1:7001"},
		TickInterval: 10 * time.Millisecond, SnapshotEntries: snapshotEntries},
		raft.Storage{Log: log, Stable: raft.NewFileStableStore(filepath.Join(dir, "raft-state"), true), Snapshots: snaps},
		nopTransport{}, f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); n.Run(ctx) }()
	stop := func() { cancel(); <-done; log.Close() }
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := n.Status(ctx)
		if err == nil && st.State == raft.StateLeader {
			break
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return n, f, stop
}

func ordersSchema() *pb.Schema {
	s := &pb.Schema{RelationOid: 16384, Namespace: "public", Name: "orders", ReplicaIdentity: 'd', Columns: []*pb.Column{
		{Name: "id", TypeOid: 23, TypeModifier: -1, Key: true}, {Name: "note", TypeOid: 25, TypeModifier: -1}}}
	s.Id = SchemaID(s)
	return s
}

func row(id, note string) *pb.Row {
	return &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(id)}, {Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(note)}}}
}

// writeFixture builds the fixture: entries 1-12 with every kind of command
// and a membership change, a snapshot at 12, entries 13-16 after it.
func writeFixture(t *testing.T, dir string) {
	t.Helper()
	n, f, stop := fixtureNode(t, dir, 12)
	defer stop()
	ctx := context.Background()
	propose := func(data []byte, err error) uint64 {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		idx, err := n.Propose(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		return idx
	}
	sc := ordersSchema()
	propose(EncodePut("a", []byte("1")))
	propose(EncodePut("b", []byte("2")))
	propose(EncodeDelete("b"))
	propose(EncodeTxBatch(&pb.TxBatch{CommitLsn: 100, EndLsn: 101, CommitTimeUs: 1_700_000_000_000_000, Xid: 7, Final: true,
		SystemId: 7300000000000000001, Timeline: 1, Schemas: []*pb.Schema{sc}, Events: []*pb.Event{
			{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: row("1", "one")},
			{SchemaId: sc.Id, Op: pb.Op_OP_UPDATE, Before: row("1", "one"), After: row("1", "uno")}}}))
	propose(EncodeSinkOffset("hook", 0, 5))
	propose(EncodeSinkReset("hook", 2))
	propose(EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_CREATE, Name: "billing"}))
	propose(EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_COMMIT, Name: "billing", Generation: 0, Offset: 4}))
	bfID := propose(EncodeBackfill(&pb.BackfillCommand{Start: []string{"public.orders"}}))
	if _, err := n.ChangeMembership(ctx, raft.ConfChange{Type: raft.ConfAddLearner, ID: 2, Addr: "127.0.0.1:7002"}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ChangeMembership(ctx, raft.ConfChange{Type: raft.ConfRemove, ID: 2}); err != nil {
		t.Fatal(err)
	}
	// 12 entries so far: the snapshot is taken here. Then:
	propose(EncodeTxBatch(&pb.TxBatch{CommitLsn: 150, EndLsn: 151, CommitTimeUs: 1_700_000_001_000_000, Xid: 8, Final: true,
		SystemId: 7300000000000000001, Timeline: 1, Events: []*pb.Event{{SchemaId: sc.Id, Op: pb.Op_OP_BACKFILL, After: row("2", "two")}},
		Backfill: &pb.BackfillChunk{Table: "public.orders", Id: bfID, LastKey: [][]byte{[]byte("2")}}}))
	propose(EncodeTxBatch(&pb.TxBatch{CommitLsn: 200, CommitTimeUs: 1_700_000_002_000_000, Xid: 9,
		SystemId: 7300000000000000001, Timeline: 1, Events: []*pb.Event{
			{SchemaId: sc.Id, Op: pb.Op_OP_INSERT, After: row("3", "three")}, {SchemaId: sc.Id, Op: pb.Op_OP_DELETE, Before: row("2", "")}}}))
	propose(EncodeSinkSkip(&pb.SinkSkip{Sink: "hook", Index: 5, Id: "0/64:1", Error: "rejected", TimeUs: 1_700_000_003_000_000}))
	last := propose(EncodePut("c", []byte("3")))
	if last != 16 {
		t.Fatalf("last index %d, want 16", last)
	}
	for f.AppliedIndex() < last {
		time.Sleep(10 * time.Millisecond)
	}
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap[0] != 8 {
		t.Fatalf("snapshot version %d, want 8", snap[0])
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(dir), "fsm-snapshot-v8.bin"), snap, 0o644); err != nil {
		t.Fatal(err)
	}
	st, _ := n.Status(ctx)
	expect := append(describe(f), describeConfig(st.Config)...)
	if err := os.WriteFile(filepath.Join(filepath.Dir(dir), "expect.txt"), []byte(strings.Join(expect, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// describe lists the state of f, one fact per line. Lines may be added for
// new state, but the ones a fixture recorded must come out the same.
func describe(f *FSM) []string {
	var out []string
	for _, k := range f.Keys() {
		v, _ := f.Get(k)
		out = append(out, fmt.Sprintf("kv %s=%s", k, v))
	}
	src := f.Source()
	out = append(out, fmt.Sprintf("source commit=%d end=%d partial=%d next_seq=%d anomalies=%d system=%d timeline=%d",
		src.CommitLSN, src.EndLSN, src.PartialLSN, src.PartialNextSeq, src.Anomalies, src.SystemID, src.Timeline))
	sc := ordersSchema()
	if got, ok := f.Schema(sc.GetId()); ok {
		var cols []string
		for _, c := range got.GetColumns() {
			cols = append(cols, fmt.Sprintf("%s:%d:%d:%v", c.GetName(), c.GetTypeOid(), c.GetTypeModifier(), c.GetKey()))
		}
		out = append(out, fmt.Sprintf("schema %d %s.%s oid=%d identity=%c %s", got.GetId(), got.GetNamespace(), got.GetName(),
			got.GetRelationOid(), got.GetReplicaIdentity(), strings.Join(cols, ",")))
	}
	out = append(out, fmt.Sprintf("last_change %d", f.LastChange()))
	positions := f.SinkPositions()
	names := make([]string, 0, len(positions))
	for name := range positions {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		out = append(out, fmt.Sprintf("sink %s offset=%d gen=%d", name, positions[name].Offset, positions[name].Gen))
		sk := f.SinkSkips(name)
		for _, r := range sk.Recent {
			out = append(out, fmt.Sprintf("skip %s total=%d index=%d id=%s error=%q time=%d", name, sk.Total, r.GetIndex(), r.GetId(), r.GetError(), r.GetTimeUs()))
		}
	}
	for _, s := range f.Subscriptions() {
		out = append(out, fmt.Sprintf("subscription %s offset=%d gen=%d drop=%v created=%d", s.GetName(), s.GetOffset(), s.GetGeneration(), s.GetDrop(), s.GetCreatedIndex()))
	}
	for _, b := range f.Backfills() {
		var key []string
		for _, k := range b.GetLastKey() {
			key = append(key, string(k))
		}
		out = append(out, fmt.Sprintf("backfill %s id=%d state=%s rows=%d last_key=%s end=%d", b.GetTable(), b.GetId(), b.GetState(), b.GetRows(), strings.Join(key, ","), b.GetEndIndex()))
	}
	return out
}

func describeConfig(c raft.Configuration) []string {
	var out []string
	for _, m := range c.Members {
		out = append(out, fmt.Sprintf("member %d addr=%s voter=%v", m.ID, m.Addr, m.Voter))
	}
	return out
}

// checkExpect fails unless every recorded line is among got; the Raft
// configuration ("member" lines) only with config.
func checkExpect(t *testing.T, what string, got []string, config bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureDir, "expect.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if config || !strings.HasPrefix(l, "member ") {
			want = append(want, l)
		}
	}
	if gone, _ := compat.Diff(want, got); len(gone) > 0 {
		t.Errorf("%s: state differs from what 1.0 recorded (design D34):\n  missing %s\n  got %s",
			what, strings.Join(gone, "\n  missing "), strings.Join(got, "\n  got "))
	}
}

// copyDir copies the fixture, so that a node may write to it.
func copyDir(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return to
}

func TestFormatFixtures(t *testing.T) {
	node := filepath.Join(fixtureDir, "node")
	if _, err := os.Stat(node); os.IsNotExist(err) {
		if !compat.Updating() {
			t.Fatalf("%s is missing; write it once with `make baseline`", node)
		}
		writeFixture(t, node)
	}

	t.Run("restart", func(t *testing.T) {
		// A node restarts from the data directory: it restores the snapshot
		// (version 7, with a membership configuration) and applies the rest.
		n, f, stop := fixtureNode(t, copyDir(t, node), 1000)
		defer stop()
		for f.AppliedIndex() < 16 {
			time.Sleep(10 * time.Millisecond)
		}
		st, err := n.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		checkExpect(t, "restarted node", append(describe(f), describeConfig(st.Config)...), true)
	})

	t.Run("log", func(t *testing.T) {
		// Every entry in the WAL decodes and applies from an empty state.
		log, err := wal.Open(copyDir(t, filepath.Join(node, "wal")), wal.Options{NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		defer log.Close()
		f := New()
		var types []string
		for i := log.FirstIndex(); i <= log.LastIndex(); i++ {
			b, err := log.Read(i)
			if err != nil || len(b) < 9 {
				t.Fatalf("entry %d: %v", i, err)
			}
			// | term u64 LE | type u8 | data |
			e := raft.Entry{Index: i, Term: binary.LittleEndian.Uint64(b), Type: raft.EntryType(b[8]), Data: b[9:]}
			types = append(types, e.Type.String())
			if e.Type != raft.EntryNormal {
				continue
			}
			if err := f.Apply(e); err != nil {
				t.Fatalf("entry %d: %v", i, err)
			}
		}
		if !slices.Contains(types, "config") || !slices.Contains(types, "noop") {
			t.Fatalf("entry types %v", types)
		}
		checkExpect(t, "log replayed", describe(f), false)
	})

	t.Run("snapshot v8", func(t *testing.T) {
		b, err := os.ReadFile(filepath.Join(fixtureDir, "fsm-snapshot-v8.bin"))
		if err != nil {
			t.Fatal(err)
		}
		f := New()
		if err := f.Restore(b); err != nil {
			t.Fatal(err)
		}
		checkExpect(t, "snapshot restored", describe(f), false)
	})
}
