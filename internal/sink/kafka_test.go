package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func TestKafkaConfig(t *testing.T) {
	k, err := NewKafka(KafkaConfig{URLs: []string{"kafka+tls://u:p@b1", "kafka://b2:9093"}, SASL: "scram-sha-512", Topic: "cdc.{table}.v1"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(k.brokers) != "[b1:9092 b2:9093]" || k.tls == nil || k.mech.Name() != "SCRAM-SHA-512" || strings.Contains(k.shown, ":p@") {
		t.Fatalf("%+v %s", k.brokers, k.shown)
	}
	sc := &pb.Schema{Namespace: "my schema", Name: "orders"}
	if got := k.topicOf(event.Change{Schema: sc, Event: &pb.Event{Op: pb.Op_OP_INSERT}}); got != "cdc.my_schema.orders.v1" {
		t.Fatalf("topic %q", got)
	}
	if got := k.topicOf(event.Change{Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET}}); got != "cdc._source_reset.v1" {
		t.Fatalf("topic %q", got)
	}
	for _, bad := range []KafkaConfig{{}, {URLs: []string{"http://x"}}, {URLs: []string{"kafka://x"}, Topic: "a b"},
		{URLs: []string{"kafka://u:p@x"}, SASL: "gssapi"}, {URLs: []string{"kafka://x"}, SASL: "plain"}} {
		if _, err := NewKafka(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestKafkaRecordKey(t *testing.T) {
	c := changesOf(1, op{op: pb.Op_OP_INSERT, id: "5"}, op{op: pb.Op_OP_DELETE, id: "6"}, op{op: pb.Op_OP_TRUNCATE})
	for i, want := range []string{`{"id":"5"}`, `{"id":"6"}`, ""} {
		k, err := recordKey(c[i])
		if err != nil || string(k) != want {
			t.Errorf("%d: %s %v, want %s", i, k, err, want)
		}
	}
}

// consumeAll reads every record of the topics until none came for a while.
func consumeAll(t *testing.T, brokers []string, topics ...string) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topics...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	for idle := 0; idle < 3; {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fs := cl.PollFetches(ctx)
		cancel()
		n := 0
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r); n++ })
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestKafkaReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_KAFKA_URL")
	name := uniqueName(t)
	k, err := NewKafka(KafkaConfig{URLs: []string{u}, Topic: "test-" + name + ".{table}"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx := context.Background()
	var ops []op
	for i := range 30 {
		ops = append(ops, op{op: pb.Op_OP_INSERT, id: fmt.Sprint(i % 6), note: fmt.Sprint(i)})
	}
	batch := changesOf(10, ops...)
	if err := k.Deliver(ctx, batch[:20]); err != nil {
		t.Fatal(err)
	}
	if err := k.Deliver(ctx, batch[20:]); err != nil {
		t.Fatal(err)
	}
	if err := k.Deliver(ctx, []event.Change{{Index: 99, CommitLSN: 1, Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET, SourceReset: &pb.SourceReset{Lsn: 3}}}}); err != nil {
		t.Fatal(err)
	}
	topic := k.topicOf(batch[0])
	recs := consumeAll(t, k.brokers, topic, k.topicOf(event.Change{Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET}}))
	if len(recs) != 31 {
		t.Fatalf("%d records", len(recs))
	}
	// Per key, in the order produced (one partition per key).
	byKey := map[string][]string{}
	resets := 0
	for _, r := range recs {
		var j event.JSON
		if err := json.Unmarshal(r.Value, &j); err != nil || header(r, "curntvalv-id") != j.ID || header(r, "curntvalv-op") != j.Op {
			t.Fatalf("record %s %v: %v", r.Value, r.Headers, err)
		}
		if j.Op == "source_reset" {
			resets++
			continue
		}
		byKey[string(r.Key)] = append(byKey[string(r.Key)], j.After["note"].(string))
	}
	if resets != 1 || len(byKey) != 6 || fmt.Sprint(byKey[`{"id":"1"}`]) != "[1 7 13 19 25]" {
		t.Fatalf("resets %d, by key %v", resets, byKey)
	}
	// A broker that is not there: an error, not a hang.
	down, _ := NewKafka(KafkaConfig{URLs: []string{"kafka://127.0.0.1:1"}, Timeout: 2 * time.Second})
	defer down.Close()
	if err := down.Deliver(ctx, batch[:1]); err == nil {
		t.Fatal("delivered to nowhere")
	}
}

func TestKafkaTombstonesReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_KAFKA_URL")
	ctx := context.Background()
	batch := changesOf(10,
		op{op: pb.Op_OP_INSERT, id: "1", note: "a"},
		op{op: pb.Op_OP_UPDATE, id: "2", note: "moved", oldID: "1"}, // key 1 -> 2
		op{op: pb.Op_OP_UPDATE, id: "2", note: "again", oldID: "2"}, // same key (REPLICA IDENTITY FULL)
		op{op: pb.Op_OP_DELETE, id: "2"},
		op{op: pb.Op_OP_TRUNCATE})
	for _, on := range []bool{true, false} {
		name := uniqueName(t)
		k, err := NewKafka(KafkaConfig{URLs: []string{u}, Topic: "test-" + name + ".{table}", Tombstones: on})
		if err != nil {
			t.Fatal(err)
		}
		if err := k.Deliver(ctx, batch); err != nil {
			t.Fatal(err)
		}
		k.Close()
		var got []string
		for _, r := range consumeAll(t, k.brokers, k.topicOf(batch[0])) {
			v := "value"
			if r.Value == nil {
				v = "null"
			}
			got = append(got, fmt.Sprintf("%s %s %s", r.Key, header(r, "curntvalv-op"), v))
		}
		// One partition per key keeps its order; compare per key.
		perKey := map[string][]string{}
		for _, g := range got {
			key, rest, _ := strings.Cut(g, " ")
			perKey[key] = append(perKey[key], rest)
		}
		want := map[string]string{
			`{"id":"1"}`: "[insert value tombstone null]",
			`{"id":"2"}`: "[update value update value delete value tombstone null]",
			``:           "[truncate value]",
		}
		if !on {
			want[`{"id":"1"}`] = "[insert value]"
			want[`{"id":"2"}`] = "[update value update value delete value]"
		}
		for key, w := range want {
			if fmt.Sprint(perKey[key]) != w {
				t.Errorf("tombstones=%v, key %q: %v, want %s", on, key, perKey[key], w)
			}
		}
	}
}

func TestKafkaTransactionsReal(t *testing.T) {
	u := envOrSkip(t, "CURNTVALV_TEST_KAFKA_URL")
	ctx := context.Background()
	name := uniqueName(t)
	cfg := KafkaConfig{URLs: []string{u}, Topic: "test-" + name + ".{table}", TransactionalID: "curntvalv-test-" + name}
	k1, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer k1.Close()
	batch := changesOf(10, op{op: pb.Op_OP_INSERT, id: "1"}, op{op: pb.Op_OP_INSERT, id: "2"}, op{op: pb.Op_OP_INSERT, id: "3"})
	if err := k1.Deliver(ctx, batch[:2]); err != nil {
		t.Fatal(err)
	}
	// Another leader's producer with the same id fences the first one.
	k2, _ := NewKafka(cfg)
	defer k2.Close()
	if err := k2.Deliver(ctx, batch[2:]); err != nil {
		t.Fatal(err)
	}
	if err := k1.Deliver(ctx, batch[2:]); err == nil {
		t.Fatal("a fenced producer delivered")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(k1.brokers...), kgo.ConsumeTopics(k1.topicOf(batch[0])),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var got []string
	for idle := 0; idle < 3; {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		n := 0
		fs.EachRecord(func(r *kgo.Record) { got = append(got, header(r, "curntvalv-id")); n++ })
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}
	slices.Sort(got)
	want := []string{batch[0].ID(), batch[1].ID(), batch[2].ID()}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("committed records %v, want %v", got, want)
	}
}
