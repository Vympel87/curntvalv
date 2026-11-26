package sink

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func TestExpr(t *testing.T) {
	sc := &pb.Schema{Columns: []*pb.Column{
		{Name: "id", TypeOid: oidInt4, Key: true}, {Name: "status", TypeOid: 25}, {Name: "amount", TypeOid: oidNumeric},
		{Name: "paid", TypeOid: oidBool}, {Name: "region", TypeOid: 25}, {Name: "big", TypeOid: 25}, {Name: "Odd Name", TypeOid: 25},
		{Name: "code", TypeOid: 25},
	}}
	row := &pb.Row{Values: []*pb.Value{text("42"), text("paid"), text("100.50"), text("t"), {Kind: pb.ValueKind_VALUE_KIND_NULL},
		{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED}, text("it's"), text("9")}}
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"status = paid", true},
		{"status = 'paid'", true},
		{"STATUS = paid", false}, // column names are case-sensitive
		{"status <> paid", false},
		{"amount >= 100", true},
		{"amount > 100.5", false},
		{"amount = 100.5", true}, // numerically
		{"amount < 1e3", true},
		{"id in (1, 42, 7)", true},
		{"id not in (1, 2)", true},
		{"status in ('open', 'shipped')", false},
		{"paid = true", true},
		{"paid = false", false},
		{"status like 'pa%'", true},
		{"status like 'p_id'", true},
		{"status not like '%x%'", true},
		{"status like 'PA%'", false},
		{"code < 10", false}, // text columns compare as bytes: "9" sorts after "10"
		{"id < 100", true},   // numeric columns as numbers
		{`"Odd Name" = 'it''s'`, true},
		// NULL is a value of its own.
		{"region is null", true},
		{"region is not null", false},
		{"region = eu", false},
		{"region != eu", true},
		{"region not in (eu)", true},
		{"region like '%'", false},
		// Unknown (an unchanged TOASTed value, a missing column): SQL logic.
		{"big = x", false},
		{"not big = x", false},
		{"big = x or status = paid", true},
		{"big = x and status = paid", false},
		{"not (big = x and status = open)", true}, // unknown and false is false
		{"nothere = 1", false},
		// Precedence: and binds tighter than or; not tighter than both.
		{"status = open and amount > 1 or paid = true", true},
		{"status = open and (amount > 1 or paid = true)", false},
		{"not status = open and paid = TRUE", true},
		{"(status = paid) AND NOT (region IS NOT NULL)", true},
	} {
		e, err := ParseExpr(tc.expr)
		if err != nil {
			t.Errorf("%q: %v", tc.expr, err)
			continue
		}
		if got := e.Eval(sc, row); got != tc.want {
			t.Errorf("%q = %v, want %v", tc.expr, got, tc.want)
		}
	}
	if e, _ := ParseExpr("status = paid"); e.Eval(sc, nil) || e.Eval(nil, row) {
		t.Error("a missing row or schema matched")
	}
	for _, bad := range []string{"", "status", "status =", "= x", "status = paid and", "(status = paid", "status = paid)",
		"status in ()", "status in (a b)", "status is", "status is not", "status not = x", "status ! x", "'unterminated",
		"status = and", "and = x", "status == paid extra"} {
		if _, err := ParseExpr(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestShapeMaskAndRename(t *testing.T) {
	in := changesOf(1, op{op: pb.Op_OP_UPDATE, id: "7", note: "secret", oldID: "6"})
	s := newShaper(Shape{Mask: []string{"note", "id", "amount"}, Rename: map[string]string{"note": "comment", "id": "order_id", "paid": "amount"},
		Drop: []string{"meta"}})
	out := s.Apply(in)
	b, _ := json.Marshal(out[0].JSON().After)
	// id is a key: renamed but not masked; paid is not renamed, because
	// the table has an amount column already.
	if string(b) != `{"amount":"***","comment":"***","order_id":"7","paid":"t"}` {
		t.Fatalf("%s", b)
	}
	if b, _ := json.Marshal(out[0].JSON().Before); string(b) != `{"amount":null,"comment":null,"order_id":"6","paid":null}` {
		t.Fatalf("before %s (NULL stays NULL)", b)
	}
	sc := out[0].Schema
	if !sc.GetColumns()[0].GetKey() || sc.GetColumns()[1].GetTypeOid() != oidText {
		t.Fatalf("schema %v", sc)
	}
	// Mirrors still work on the renamed key, and masked numbers stay text.
	if k, err := documentKey(sc, out[0].Event.GetAfter()); err != nil || k != "7" {
		t.Fatalf("key %q %v", k, err)
	}
	if d := document(sc, out[0].Event.GetAfter(), "7"); d["amount"] != "***" {
		t.Fatalf("document %v", d)
	}
	// Filters see the Postgres names and the original values.
	s = newShaper(Shape{Where: []*Expr{where(t, "note = secret")}, Rename: map[string]string{"note": "comment"}, Mask: []string{"note"}})
	if out := s.Apply(in); len(out) != 1 {
		t.Fatal("filter on a renamed, masked column did not see the original")
	}
	for _, bad := range []string{"a", "a:", ":b", "a:a", "a:b+a:c", "a:c+b:c"} {
		if _, err := ParseRename(bad); err == nil {
			t.Errorf("rename %q accepted", bad)
		}
	}
	if m, err := ParseRename("a:b+c:d"); err != nil || m["a"] != "b" || m["c"] != "d" {
		t.Fatalf("%v %v", m, err)
	}
}

func TestShapeHash(t *testing.T) {
	in := changesOf(1, op{op: pb.Op_OP_UPDATE, id: "7", note: "alice@example.com", oldID: "6"})
	key := []byte("0123456789abcdef")
	s := newShaper(Shape{Hash: []string{"note", "id"}, HashKey: key})
	out := s.Apply(in)
	after, before := out[0].JSON().After, out[0].JSON().Before
	// HMAC-SHA256("0123456789abcdef", "alice@example.com"), hex.
	m := hmac.New(sha256.New, key)
	m.Write([]byte("alice@example.com"))
	if after["note"] != hex.EncodeToString(m.Sum(nil)) || len(after["id"].(string)) != 64 || after["amount"] != "12.50" {
		t.Fatalf("after %v", after)
	}
	if before["note"] != nil || before["id"] == after["id"] {
		t.Fatalf("before %v: NULL stays, different keys stay different", before)
	}
	// Deterministic, and the key column stays the key.
	again := newShaper(Shape{Hash: []string{"note", "id"}, HashKey: key}).Apply(in)
	if again[0].JSON().After["id"] != after["id"] {
		t.Fatal("not deterministic")
	}
	if k, err := documentKey(out[0].Schema, out[0].Event.GetAfter()); err != nil || k != after["id"] {
		t.Fatalf("key %q %v", k, err)
	}
	if out[0].Schema.GetColumns()[0].GetTypeOid() != oidText || !out[0].Schema.GetColumns()[0].GetKey() {
		t.Fatalf("schema %v", out[0].Schema.GetColumns()[0])
	}
	// Another key, another hash.
	other := newShaper(Shape{Hash: []string{"note"}, HashKey: []byte("another key, 16+")}).Apply(in)
	if other[0].JSON().After["note"] == after["note"] {
		t.Fatal("the hash does not depend on the key")
	}
}
