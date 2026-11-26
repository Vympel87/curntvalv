package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func numericMod(p, s int32) int32 { return p<<16 | s + 4 }

func TestMirrorTypesAndValues(t *testing.T) {
	for _, tc := range []struct {
		col  *pb.Column
		want string
	}{
		{&pb.Column{TypeOid: oidInt8}, "Int64"},
		{&pb.Column{TypeOid: oidNumeric, TypeModifier: numericMod(10, 2)}, "Decimal(10, 2)"},
		{&pb.Column{TypeOid: oidNumeric, TypeModifier: -1}, "String"},
		{&pb.Column{TypeOid: oidTimestampTZ}, "DateTime64(6, 'UTC')"},
		{&pb.Column{TypeOid: oidUUID}, "UUID"},
		{&pb.Column{TypeOid: oidJSONB}, "String"},
		{&pb.Column{TypeOid: 1009}, "String"}, // text[]
	} {
		if got := chType(tc.col); got != tc.want {
			t.Errorf("%v: %s, want %s", tc.col, got, tc.want)
		}
	}
	for _, tc := range []struct {
		oid  uint32
		mod  int32
		in   string
		want any
	}{
		{oidTimestampTZ, 0, "2026-11-25 10:11:12.345678+07", "2026-11-25 03:11:12.345678"},
		{oidTimestampTZ, 0, "2026-11-25 10:11:12+05:30", "2026-11-25 04:41:12.000000"},
		{oidTimestamp, 0, "2026-11-25 10:11:12.5", "2026-11-25 10:11:12.500000"},
		{oidTimestamp, 0, "infinity", nil},
		{oidDate, 0, "-infinity", nil},
		{oidFloat8, 0, "NaN", nil},
		{oidNumeric, numericMod(10, 2), "NaN", nil},
		{oidNumeric, -1, "NaN", "NaN"}, // String column: kept
		{oidBool, 0, "f", false},
	} {
		got, _ := mirrorValue(&pb.Column{TypeOid: tc.oid, TypeModifier: tc.mod}, text(tc.in))
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%d %q: %v, want %v", tc.oid, tc.in, got, tc.want)
		}
	}
	if version(event.Change{Index: 3, Seq: 5}) <= version(event.Change{Index: 2, Seq: 1 << 20}) {
		t.Fatal("a later entry must have a higher version")
	}
	for _, cfg := range []ClickHouseConfig{{URL: "http://x", Mirror: true, Table: "a b"}, {URL: "http://x", Mirror: true, Table: "x;{table}"}} {
		if _, err := NewClickHouse(cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

// mirrorSchema is a table with typed columns and a two-column key.
func mirrorSchema(extra ...*pb.Column) *pb.Schema {
	s := &pb.Schema{Namespace: "public", Name: "orders", Columns: append([]*pb.Column{
		{Name: "shop", TypeOid: 25, Key: true}, {Name: "id", TypeOid: oidInt4, Key: true},
		{Name: "amount", TypeOid: oidNumeric, TypeModifier: numericMod(10, 2)}, {Name: "paid", TypeOid: oidBool},
		{Name: "at", TypeOid: oidTimestampTZ}, {Name: "body", TypeOid: 25},
	}, extra...)}
	s.Id = fsm.SchemaID(s)
	return s
}

type mc struct {
	op            pb.Op
	after, before []string // "<null>", "<toast>" or text
}

func mirrorChanges(s *pb.Schema, index uint64, ops ...mc) []event.Change {
	row := func(vals []string) *pb.Row {
		if vals == nil {
			return nil
		}
		r := &pb.Row{}
		for _, v := range vals {
			switch v {
			case "<null>":
				r.Values = append(r.Values, &pb.Value{Kind: pb.ValueKind_VALUE_KIND_NULL})
			case "<toast>":
				r.Values = append(r.Values, &pb.Value{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED})
			default:
				r.Values = append(r.Values, text(v))
			}
		}
		return r
	}
	var out []event.Change
	for i, o := range ops {
		out = append(out, event.Change{Index: index, Seq: uint32(i), CommitLSN: index << 8, Schema: s,
			Event: &pb.Event{SchemaId: s.Id, Op: o.op, After: row(o.after), Before: row(o.before)}})
	}
	return out
}

func TestClickHouseMirrorReal(t *testing.T) {
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	if chURL == "" {
		t.Skip("CURNTVALV_TEST_CH_URL not set; run `make test-ch`")
	}
	prefix := fmt.Sprintf("m%d_", time.Now().UnixNano()%1_000_000_000)
	c, err := NewClickHouse(ClickHouseConfig{URL: chURL, Mirror: true, Table: prefix + "{table}"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	table := "`default`.`" + prefix + "orders`"
	t.Cleanup(func() { c.exec(context.Background(), "DROP TABLE IF EXISTS "+table, nil) })
	rows := func() string {
		out, err := c.Query(ctx, "SELECT shop, id, amount, paid, toString(at), body FROM "+table+" FINAL ORDER BY shop, id FORMAT TSV")
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	s := mirrorSchema()
	steps := [][]event.Change{
		mirrorChanges(s, 1,
			mc{op: pb.Op_OP_BACKFILL, after: []string{"a", "1", "10.50", "t", "2026-11-25 10:00:00+07", "big text"}},
			mc{op: pb.Op_OP_BACKFILL, after: []string{"a", "2", "<null>", "f", "<null>", "two"}},
			mc{op: pb.Op_OP_INSERT, after: []string{"b", "1", "3", "t", "infinity", "b1"}}),
		// The big column did not change: its value stays.
		mirrorChanges(s, 2, mc{op: pb.Op_OP_UPDATE, after: []string{"a", "1", "11", "t", "2026-11-25 10:00:00+07", "<toast>"}}),
		// Twice in one batch: the last one wins, the TOASTed value from the first.
		mirrorChanges(s, 3,
			mc{op: pb.Op_OP_UPDATE, after: []string{"a", "2", "1", "f", "<null>", "two v2"}},
			mc{op: pb.Op_OP_UPDATE, after: []string{"a", "2", "2", "t", "<null>", "<toast>"}}),
		// A key change and a delete.
		mirrorChanges(s, 4,
			mc{op: pb.Op_OP_UPDATE, before: []string{"b", "1", "<null>", "<null>", "<null>", "<null>"}, after: []string{"b", "9", "3", "t", "<null>", "moved"}},
			mc{op: pb.Op_OP_DELETE, before: []string{"a", "1", "<null>", "<null>", "<null>", "<null>"}}),
	}
	for i, st := range steps {
		if err := c.Deliver(ctx, st); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			out, _ := c.Query(ctx, "SELECT amount, body FROM "+table+" FINAL WHERE shop = 'a' AND id = 1 FORMAT TSV")
			if strings.TrimSpace(string(out)) != "11\tbig text" {
				t.Fatalf("unchanged TOAST value not kept: %q", out)
			}
		}
	}
	want := "a\t2\t2\ttrue\t\\N\ttwo v2\nb\t9\t3\ttrue\t\\N\tmoved"
	if got := rows(); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	// Everything redelivered (a leader change): the same rows.
	for _, st := range steps {
		if err := c.Deliver(ctx, st); err != nil {
			t.Fatal(err)
		}
	}
	if got := rows(); got != want {
		t.Fatalf("after redelivery\n%s", got)
	}
	// A column added in Postgres; then a truncate followed by a row.
	s2 := mirrorSchema(&pb.Column{Name: "note", TypeOid: 25})
	if err := c.Deliver(ctx, mirrorChanges(s2, 5,
		mc{op: pb.Op_OP_INSERT, after: []string{"c", "1", "1", "t", "<null>", "c1", "hello"}})); err != nil {
		t.Fatal(err)
	}
	out, _ := c.Query(ctx, "SELECT note FROM "+table+" FINAL WHERE shop = 'c' FORMAT TSV")
	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("new column: %q", out)
	}
	if err := c.Deliver(ctx, mirrorChanges(s2, 6,
		mc{op: pb.Op_OP_TRUNCATE}, mc{op: pb.Op_OP_INSERT, after: []string{"d", "1", "1", "t", "<null>", "d1", "<null>"}})); err != nil {
		t.Fatal(err)
	}
	if got := rows(); got != "d\t1\t1\ttrue\t\\N\td1" {
		t.Fatalf("after truncate\n%s", got)
	}
	// A one-column key, with an unchanged TOASTed value read back.
	one := &pb.Schema{Namespace: "public", Name: "docs", Columns: []*pb.Column{{Name: "id", TypeOid: oidInt8, Key: true}, {Name: "body", TypeOid: 25}}}
	one.Id = fsm.SchemaID(one)
	t.Cleanup(func() { c.exec(context.Background(), "DROP TABLE IF EXISTS `default`.`"+prefix+"docs`", nil) })
	for i, body := range []string{"long text", "<toast>"} {
		if err := c.Deliver(ctx, mirrorChanges(one, uint64(10+i), mc{op: pb.Op_OP_UPDATE, after: []string{"5", body}})); err != nil {
			t.Fatal(err)
		}
	}
	if out, _ := c.Query(ctx, "SELECT body FROM `default`.`"+prefix+"docs` FINAL FORMAT TSV"); strings.TrimSpace(string(out)) != "long text" {
		t.Fatalf("one-column key: %q", out)
	}
	// A table without a key cannot be mirrored.
	nokey := &pb.Schema{Namespace: "public", Name: "logs", Columns: []*pb.Column{{Name: "line", TypeOid: 25}}}
	nokey.Id = fsm.SchemaID(nokey)
	if err := c.Deliver(ctx, []event.Change{{Index: 7, Schema: nokey, Event: &pb.Event{SchemaId: nokey.Id, Op: pb.Op_OP_INSERT,
		After: &pb.Row{Values: []*pb.Value{text("x")}}}}}); err == nil || !strings.Contains(err.Error(), "no key columns") {
		t.Fatalf("no key: %v", err)
	}
}

// TestClickHouseMirrorRetypeReal: a column whose Postgres type changed is
// changed in the mirror, rows already there converted; a key column cannot
// change.
func TestClickHouseMirrorRetypeReal(t *testing.T) {
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	if chURL == "" {
		t.Skip("CURNTVALV_TEST_CH_URL not set; run `make test-ch`")
	}
	prefix := fmt.Sprintf("r%d_", time.Now().UnixNano()%1_000_000_000)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	table := "`default`.`" + prefix + "items`"
	schema := func(qty uint32, idType uint32) *pb.Schema {
		s := &pb.Schema{Namespace: "public", Name: "items", Columns: []*pb.Column{
			{Name: "id", TypeOid: idType, Key: true}, {Name: "qty", TypeOid: qty}}}
		s.Id = fsm.SchemaID(s)
		return s
	}
	newSink := func() *ClickHouse {
		c, err := NewClickHouse(ClickHouseConfig{URL: chURL, Mirror: true, Table: prefix + "{table}"})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := newSink()
	t.Cleanup(func() { c.exec(context.Background(), "DROP TABLE IF EXISTS "+table, nil) })
	if err := c.Deliver(ctx, mirrorChanges(schema(oidInt4, oidInt4), 1, mc{op: pb.Op_OP_INSERT, after: []string{"1", "7"}})); err != nil {
		t.Fatal(err)
	}
	// ALTER TABLE items ALTER qty TYPE bigint, then a value int4 cannot hold;
	// a new sink (another leader) finds the table as it is.
	for _, cs := range []*ClickHouse{c, newSink()} {
		if err := cs.Deliver(ctx, mirrorChanges(schema(oidInt8, oidInt4), 2, mc{op: pb.Op_OP_INSERT, after: []string{"2", "5000000000"}})); err != nil {
			t.Fatal(err)
		}
	}
	out, err := c.Query(ctx, "SELECT toTypeName(qty), groupArray(qty) FROM (SELECT qty FROM "+table+" FINAL ORDER BY id) GROUP BY 1 FORMAT TSV")
	if err != nil || strings.TrimSpace(string(out)) != "Nullable(Int64)\t[7,5000000000]" {
		t.Fatalf("after retype: %q %v", out, err)
	}
	// The key's type cannot change in place.
	err = newSink().Deliver(ctx, mirrorChanges(schema(oidInt8, oidInt8), 3, mc{op: pb.Op_OP_INSERT, after: []string{"3", "1"}}))
	if err == nil || !strings.Contains(err.Error(), "key column id") {
		t.Fatalf("key retype: %v", err)
	}
}
