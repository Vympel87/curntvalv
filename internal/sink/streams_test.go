package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// ordersSchema has a primary key and typed columns.
var ordersSchema = &pb.Schema{Id: 7, Namespace: "public", Name: "orders", Columns: []*pb.Column{
	{Name: "id", TypeOid: oidInt4, Key: true}, {Name: "note", TypeOid: 25}, {Name: "amount", TypeOid: oidNumeric},
	{Name: "paid", TypeOid: oidBool}, {Name: "meta", TypeOid: oidJSONB},
}}

func text(s string) *pb.Value { return &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(s)} }

func orderRow(id, note string) *pb.Row {
	return &pb.Row{Values: []*pb.Value{text(id), text(note), text("12.50"), text("t"), text(`{"a": [1]}`)}}
}

// changesOf builds changes in log order; each element is one op on one row.
type op struct {
	op       pb.Op
	id, note string
	oldID    string // update that moves the key
}

func changesOf(index uint64, ops ...op) []event.Change {
	var out []event.Change
	for i, o := range ops {
		ev := &pb.Event{SchemaId: 7, Op: o.op}
		switch o.op {
		case pb.Op_OP_INSERT, pb.Op_OP_UPDATE, pb.Op_OP_BACKFILL:
			ev.After = orderRow(o.id, o.note)
			if o.oldID != "" {
				ev.Before = &pb.Row{Values: []*pb.Value{text(o.oldID), {Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL},
					{Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}
			}
		case pb.Op_OP_DELETE:
			ev.Before = &pb.Row{Values: []*pb.Value{text(o.id), {Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL},
				{Kind: pb.ValueKind_VALUE_KIND_NULL}, {Kind: pb.ValueKind_VALUE_KIND_NULL}}}
		}
		out = append(out, event.Change{Index: index + uint64(i)/2, CommitLSN: 0x1000 + index*16 + uint64(i)/2, Seq: uint32(i % 2),
			CommitTimeUs: 1_700_000_000_000_000, Event: ev, Schema: ordersSchema})
	}
	return out
}

func TestDocumentKey(t *testing.T) {
	two := &pb.Schema{Columns: []*pb.Column{{Name: "a", Key: true}, {Name: "b", Key: true}, {Name: "c"}}}
	row := func(vals ...string) *pb.Row {
		r := &pb.Row{}
		for _, v := range vals {
			r.Values = append(r.Values, text(v))
		}
		return r
	}
	for _, tc := range []struct {
		row  *pb.Row
		want string
	}{
		{row("17", "x", "ignored"), "17-x"},
		{row("a b", "x", ""), "-MzphIGIxOng"},
		{row("1-2", "x", ""), "-MzoxLTIxOng"}, // a "-" would be ambiguous
		{row("", "x", ""), "-MDoxOng"},
	} {
		got, err := documentKey(two, tc.row)
		if err != nil || got != tc.want {
			t.Errorf("%v: %q %v, want %q", tc.row, got, err, tc.want)
		}
	}
	if _, err := documentKey(two, &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_NULL}, text("x")}}); err == nil {
		t.Error("NULL key accepted")
	}
	if _, err := documentKey(&pb.Schema{Name: "t", Columns: []*pb.Column{{Name: "a"}}}, row("1")); err == nil {
		t.Error("table without key accepted")
	}
	// Different keys never collide.
	seen := map[string]string{}
	for _, parts := range [][2]string{{"a", "b-c"}, {"a-b", "c"}, {"a", "bc"}, {"ab", "c"}, {"-", "x"}, {"a", "b"}} {
		k, _ := documentKey(two, row(parts[0], parts[1], ""))
		if prev, dup := seen[k]; dup {
			t.Errorf("%v and %s share key %q", parts, prev, k)
		}
		seen[k] = fmt.Sprint(parts)
	}
}

func TestDocumentValues(t *testing.T) {
	r := orderRow("5", "hi")
	r.Values = append(r.Values[:2], text("NaN"), text("f"), &pb.Value{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED})
	b, _ := json.Marshal(document(ordersSchema, r, "5"))
	if string(b) != `{"amount":"NaN","curntvalv_key":"5","id":5,"note":"hi","paid":false}` {
		t.Fatalf("%s", b)
	}
	b, _ = json.Marshal(document(ordersSchema, orderRow("6", "x"), "6"))
	if string(b) != `{"amount":12.50,"curntvalv_key":"6","id":6,"meta":{"a":[1]},"note":"x","paid":true}` {
		t.Fatalf("%s", b)
	}
}

func TestMeilisearchPlan(t *testing.T) {
	m, err := NewMeilisearch(MeilisearchConfig{URL: "http://:key@localhost:7700", Index: "cdc-{table}"})
	if err != nil {
		t.Fatal(err)
	}
	changes := changesOf(1,
		op{op: pb.Op_OP_BACKFILL, id: "1"}, op{op: pb.Op_OP_INSERT, id: "2"}, op{op: pb.Op_OP_UPDATE, id: "3", oldID: "2"},
		op{op: pb.Op_OP_DELETE, id: "1"}, op{op: pb.Op_OP_TRUNCATE}, op{op: pb.Op_OP_UPDATE, id: "4", oldID: "4"})
	changes = append(changes, event.Change{Index: 9, Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET, SourceReset: &pb.SourceReset{}}})
	ops, err := m.plan(changes)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range ops {
		var keys []string
		for _, d := range o.docs {
			keys = append(keys, d[keyField].(string))
		}
		got = append(got, fmt.Sprintf("%s %s %v%v", o.index, o.kind, keys, o.ids))
	}
	want := "[cdc-orders upsert [1 2][] cdc-orders delete [][2] cdc-orders upsert [3][] cdc-orders delete [][1] cdc-orders clear [][] cdc-orders upsert [4][]]"
	if fmt.Sprint(got) != want {
		t.Fatalf("plan\n got %v\nwant %s", got, want)
	}
	if m.key != "key" || m.endpoint != "http://localhost:7700" {
		t.Fatalf("%q %q", m.key, m.endpoint)
	}
	for _, bad := range []MeilisearchConfig{{URL: "ftp://x"}, {URL: "http://x", Index: "a b"}} {
		if _, err := NewMeilisearch(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

// A failed Meilisearch task is an error, so the batch is retried.
func TestMeilisearchFailedTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, `{"message":"no key","code":"missing_authorization_header"}`, http.StatusUnauthorized)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/tasks") {
			fmt.Fprint(w, `{"results":[{"uid":3,"status":"failed","type":"documentAdditionOrUpdate","indexUid":"public_orders",
				"error":{"message":"bad primary key","code":"invalid_document_id"}}]}`)
			return
		}
		fmt.Fprint(w, `{"taskUid":3}`)
	}))
	defer srv.Close()
	m, _ := NewMeilisearch(MeilisearchConfig{URL: strings.Replace(srv.URL, "http://", "http://:k@", 1)})
	err := m.Deliver(context.Background(), changesOf(1, op{op: pb.Op_OP_INSERT, id: "1"}))
	if err == nil || !strings.Contains(err.Error(), "bad primary key") {
		t.Fatalf("%v", err)
	}
	m, _ = NewMeilisearch(MeilisearchConfig{URL: srv.URL})
	if err := m.Deliver(context.Background(), changesOf(1, op{op: pb.Op_OP_INSERT, id: "1"})); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("without key: %v", err)
	}
}

func TestSubjectsAndStreams(t *testing.T) {
	n, err := NewNATS(NATSConfig{URL: "nats://tok@h", Subject: "cdc.db1"})
	if err != nil {
		t.Fatal(err)
	}
	if n.token != "tok" || n.addr != "h:4222" {
		t.Fatalf("%+v", n)
	}
	sc := &pb.Schema{Namespace: "my schema", Name: "orders.v2"}
	if got := n.subjectOf(event.Change{Schema: sc, Event: &pb.Event{Op: pb.Op_OP_INSERT}}); got != "cdc.db1.my_schema.orders_v2" {
		t.Fatalf("subject %q", got)
	}
	if got := n.subjectOf(event.Change{Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET}}); got != "cdc.db1.source_reset" {
		t.Fatalf("subject %q", got)
	}
	for _, bad := range []NATSConfig{{URL: "http://x"}, {URL: "nats://x", Subject: "a.>"}, {URL: "nats://x", Stream: "a.b"}} {
		if _, err := NewNATS(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
	r, err := NewRedis(RedisConfig{URL: "rediss://u:p@h/3", Stream: "cdc:{table}"})
	if err != nil {
		t.Fatal(err)
	}
	if r.db != 3 || r.user != "u" || r.password != "p" || r.tls == nil || r.addr != "h:6379" {
		t.Fatalf("%+v", r)
	}
	c := changesOf(4, op{op: pb.Op_OP_INSERT, id: "1"}, op{op: pb.Op_OP_INSERT, id: "2"})
	if r.streamKey(c[1]) != "cdc:public.orders" || entryID(c[1]) != "4-1" {
		t.Fatalf("%s %s", r.streamKey(c[1]), entryID(c[1]))
	}
	if r.streamKey(event.Change{Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET}}) != "cdc:_source_reset" {
		t.Fatal("reset stream")
	}
	if !idBefore("9-5", "10-0") || idBefore("10-1", "10-0") || idBefore("3-2", "3-2") {
		t.Fatal("idBefore")
	}
	for _, bad := range []RedisConfig{{URL: "redis://h/x"}, {URL: "http://h"}, {URL: "redis://h", MaxLen: -1}} {
		if _, err := NewRedis(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestReadResp(t *testing.T) {
	in := "+OK\r\n-ERR bad\r\n:42\r\n$5\r\nhe\r\nl\r\n$-1\r\n*2\r\n*1\r\n$3\r\n1-0\r\n:7\r\n*-1\r\n"
	r := bufio.NewReader(strings.NewReader(in))
	var got []string
	for range 7 {
		v, err := readResp(r)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%q/%d/%v/%d/%v", v.str, v.num, v.err, len(v.array), v.null))
	}
	want := []string{`"OK"/0/<nil>/0/false`, `""/0/ERR bad/0/false`, `""/42/<nil>/0/false`, `"he\r\nl"/0/<nil>/0/false`,
		`""/0/<nil>/0/true`, `""/0/<nil>/2/false`, `""/0/<nil>/0/true`}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	for _, bad := range []string{"?x\r\n", "$x\r\n", ":1\n", "$3\r\nab"} {
		if _, err := readResp(bufio.NewReader(strings.NewReader(bad))); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// --- against real servers (make test-sinks) ------------------------------------------

func envOrSkip(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skip(name + " not set; run `make test-sinks`")
	}
	return v
}

func uniqueName(t *testing.T) string {
	return fmt.Sprintf("t%d", time.Now().UnixNano()%1_000_000_000)
}

func TestRedisReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_REDIS_URL")
	stream := "curntvalv-test:" + uniqueName(t) + ":{table}"
	r, err := NewRedis(RedisConfig{URL: u, Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	batch := changesOf(10, op{op: pb.Op_OP_INSERT, id: "1"}, op{op: pb.Op_OP_INSERT, id: "2"}, op{op: pb.Op_OP_UPDATE, id: "1"},
		op{op: pb.Op_OP_DELETE, id: "2"})
	if err := r.Deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// Redelivered, also cut differently (a leader change): stored once.
	if err := r.Deliver(ctx, batch[1:]); err != nil {
		t.Fatal(err)
	}
	more := changesOf(12, op{op: pb.Op_OP_INSERT, id: "3"})
	if err := r.Deliver(ctx, append(batch[2:], more...)); err != nil {
		t.Fatal(err)
	}
	key := r.streamKey(batch[0])
	c, _ := r.connection(ctx)
	c.command("XRANGE", key, "-", "+")
	rep, err := c.roundTrip(1)
	if err != nil || rep[0].err != nil {
		t.Fatal(err, rep)
	}
	var ids []string
	for _, e := range rep[0].array {
		f := map[string]string{}
		for i := 0; i+1 < len(e.array[1].array); i += 2 {
			f[e.array[1].array[i].str] = e.array[1].array[i+1].str
		}
		var j event.JSON
		if err := json.Unmarshal([]byte(f["json"]), &j); err != nil || j.ID != f["id"] || f["table"] != "public.orders" {
			t.Fatalf("entry %v: %v", f, err)
		}
		ids = append(ids, e.array[0].str+"="+f["op"])
	}
	if fmt.Sprint(ids) != "[10-0=insert 10-1=insert 11-0=update 11-1=delete 12-0=insert]" {
		t.Fatalf("stream %v", ids)
	}
	c.command("DEL", key)
	c.command("XADD", key, "*", "x", "foreign") // another writer, far ahead
	if _, err := c.roundTrip(2); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(ctx, batch[:1]); err == nil || !strings.Contains(err.Error(), "did not write") {
		t.Fatalf("foreign writer: %v", err)
	}
	c.command("DEL", key)
	c.roundTrip(1)
}

func TestNATSReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_NATS_URL")
	name := uniqueName(t)
	n, err := NewNATS(NATSConfig{URL: u, Subject: "test." + name, Stream: name})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx := context.Background()
	batch := changesOf(10, op{op: pb.Op_OP_INSERT, id: "1"}, op{op: pb.Op_OP_UPDATE, id: "1"}, op{op: pb.Op_OP_DELETE, id: "1"})
	if err := n.Deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := n.Deliver(ctx, append(batch[1:], changesOf(20, op{op: pb.Op_OP_INSERT, id: "9"})...)); err != nil {
		t.Fatal(err)
	}
	// A new connection after a pause, and a reset.
	n.lastUsed = time.Now().Add(-time.Minute)
	if err := n.Deliver(ctx, []event.Change{{Index: 30, CommitLSN: 1, Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET, SourceReset: &pb.SourceReset{Lsn: 5}}}}); err != nil {
		t.Fatal(err)
	}
	info, err := n.conn.request("$JS.API.STREAM.INFO."+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		State struct {
			Messages int `json:"messages"`
		} `json:"state"`
		Config struct {
			Subjects []string `json:"subjects"`
		} `json:"config"`
	}
	json.Unmarshal(info.data, &st)
	if st.State.Messages != 5 || fmt.Sprint(st.Config.Subjects) != "[test."+name+".>]" {
		t.Fatalf("stream %s", info.data)
	}
	// A subject no stream takes.
	other, _ := NewNATS(NATSConfig{URL: u, Subject: "nostream." + name, Stream: name})
	other.ready = true
	defer other.Close()
	if err := other.Deliver(ctx, batch[:1]); err == nil || !strings.Contains(err.Error(), "no JetStream stream") {
		t.Fatalf("no stream: %v", err)
	}
	n.conn.request("$JS.API.STREAM.DELETE."+name, nil)
}

func TestMeilisearchReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_MEILI_URL")
	name := uniqueName(t)
	m, err := NewMeilisearch(MeilisearchConfig{URL: u, Index: name + "_{table}"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	steps := [][]event.Change{
		changesOf(1, op{op: pb.Op_OP_BACKFILL, id: "1", note: "a"}, op{op: pb.Op_OP_BACKFILL, id: "2", note: "b"}),
		changesOf(2, op{op: pb.Op_OP_INSERT, id: "3", note: "c"}, op{op: pb.Op_OP_UPDATE, id: "1", note: "a2"}),
		changesOf(3, op{op: pb.Op_OP_UPDATE, id: "20", note: "moved", oldID: "2"}, op{op: pb.Op_OP_DELETE, id: "3"}),
	}
	for _, s := range steps {
		if err := m.Deliver(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	// Redelivery changes nothing.
	if err := m.Deliver(ctx, steps[2]); err != nil {
		t.Fatal(err)
	}
	docs := func() string {
		var resp struct {
			Results []map[string]any `json:"results"`
		}
		if err := m.call(ctx, http.MethodGet, "/indexes/"+name+"_orders/documents?limit=100", nil, &resp); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, d := range resp.Results {
			out = append(out, fmt.Sprintf("%v:%v:%v:%v", d["id"], d["note"], d["amount"], d["paid"]))
		}
		sort.Strings(out)
		return fmt.Sprint(out)
	}
	if got := docs(); got != "[1:a2:12.5:true 20:moved:12.5:true]" {
		t.Fatalf("documents %s", got)
	}
	if err := m.Deliver(ctx, changesOf(4, op{op: pb.Op_OP_TRUNCATE}, op{op: pb.Op_OP_INSERT, id: "7", note: "after"})); err != nil {
		t.Fatal(err)
	}
	if got := docs(); got != "[7:after:12.5:true]" {
		t.Fatalf("after truncate %s", got)
	}
	// Truncating an index that does not exist yet is fine.
	m2, _ := NewMeilisearch(MeilisearchConfig{URL: u, Index: name + "_none"})
	if err := m2.Deliver(ctx, changesOf(5, op{op: pb.Op_OP_TRUNCATE})); err != nil {
		t.Fatal(err)
	}
	m.call(ctx, http.MethodDelete, "/indexes/"+name+"_orders", nil, nil)
}
