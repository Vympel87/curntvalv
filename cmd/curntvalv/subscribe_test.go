package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/server"
)

// fixedReader serves changes with index >= from, one entry per read, then
// waits.
type fixedReader struct {
	mu      sync.Mutex
	changes []event.Change
	froms   []uint64
}

func (r *fixedReader) Read(ctx context.Context, from uint64, _ int, filter event.Filter) (event.Result, error) {
	r.mu.Lock()
	r.froms = append(r.froms, from)
	for _, c := range r.changes {
		if c.Index >= from && filter.Match(c.Schema) {
			r.mu.Unlock()
			return event.Result{Changes: []event.Change{c}, Next: c.Index + 1, TxEntries: 1}, nil
		}
	}
	r.mu.Unlock()
	<-ctx.Done()
	return event.Result{}, ctx.Err()
}

func serveSubscribe(t *testing.T, r server.ChangeReader) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterSubscribeServiceServer(gs, server.NewSubscribe(r, nil, nil))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestSubscribeCommand(t *testing.T) {
	orders := &pb.Schema{Id: 1, Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}}}
	items := &pb.Schema{Id: 2, Namespace: "public", Name: "items", Columns: []*pb.Column{{Name: "id"}}}
	mk := func(index uint64, sc *pb.Schema, id string) event.Change {
		return event.Change{Index: index, CommitLSN: index << 4, Schema: sc, Event: &pb.Event{SchemaId: sc.Id, Op: pb.Op_OP_INSERT,
			After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(id)}}}}}
	}
	r := &fixedReader{changes: []event.Change{mk(3, orders, "a"), mk(4, items, "x"), mk(6, orders, "b"), mk(9, orders, "c")}}
	addr := serveSubscribe(t, r)

	// The first address is dead: the command moves on to the next one.
	dead := freePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	err := subscribeCmd(ctx, []string{"--addr", dead + "," + addr, "--table", "orders", "--from", "4", "--limit", "2"}, &out, &errOut)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var j event.JSON
		if err := json.Unmarshal([]byte(line), &j); err != nil {
			t.Fatalf("bad line %q: %v", line, err)
		}
		got = append(got, j.After["id"].(string)+"@"+j.Table)
	}
	if strings.Join(got, " ") != "b@public.orders c@public.orders" {
		t.Fatalf("got %v\nstderr: %s", got, errOut.String())
	}
	if !strings.Contains(errOut.String(), "retrying from index 4") {
		t.Fatalf("no retry message: %s", errOut.String())
	}
}

func TestListFlag(t *testing.T) {
	var l listFlag
	l.Set("a, b,")
	l.Set("c")
	if l.String() != "a,b,c" {
		t.Fatalf("%v", l)
	}
}

func TestParseSinkSpec(t *testing.T) {
	sp, err := parseSinkSpec("name=ch,type=clickhouse,url=http://u:p@h:8123,table=orders,table=public.items,start=latest,into=ev,policy=drop")
	if err != nil {
		t.Fatal(err)
	}
	if sp.name != "ch" || sp.typ != "clickhouse" || sp.url != "http://u:p@h:8123" || strings.Join(sp.tables, " ") != "orders public.items" ||
		!sp.startLatest || sp.chTable != "ev" || !sp.drop {
		t.Fatalf("%+v", sp)
	}
	if _, err := newSink(sp); err != nil {
		t.Fatal(err)
	}
	sh, err := parseSinkSpec("name=s,type=webhook,url=http://x,op=insert,op=update,where=status=paid,where=region!=eu,columns=id+note,drop=secret")
	if err != nil || len(sh.shape.Ops) != 2 || len(sh.shape.Where) != 2 || sh.shape.Where[1].String() != "region!=eu" ||
		fmt.Sprint(sh.shape.Columns) != "[id note]" || fmt.Sprint(sh.shape.Drop) != "[secret]" {
		t.Fatalf("shape: %+v %v", sh.shape, err)
	}
	if cfg := sinkConfig(sh); len(cfg.Shape.Ops) != 2 {
		t.Fatalf("config %+v", cfg.Shape)
	}
	for _, bad := range []string{"op=upsert", "where=status", "columns=a++b", "drop=", "columns=a,columns=b"} {
		if _, err := parseSinkSpec("name=s,type=webhook,url=http://x," + bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	ex, err := parseSinkSpec(`name=s,type=webhook,url=http://x,where=status in (paid, 'a,b') and amount >= 10,mask=email+phone,rename=note:comment,table=orders`)
	if err != nil || len(ex.shape.Where) != 1 || ex.shape.Where[0].String() != `status in (paid, 'a,b') and amount >= 10` ||
		fmt.Sprint(ex.shape.Mask) != "[email phone]" || ex.shape.Rename["note"] != "comment" || fmt.Sprint(ex.tables) != "[orders]" {
		t.Fatalf("expression spec: %+v %v", ex, err)
	}
	if got := splitSpec(`a=1,b=f(x, y),c='p,q',d="r,s",e=)`); fmt.Sprint(got) != `[a=1 b=f(x, y) c='p,q' d="r,s" e=)]` {
		t.Fatalf("split %q", got)
	}
	for _, bad := range []string{"where=amount >=", "rename=a", "mask=a++b", "where=(a = 1"} {
		if _, err := parseSinkSpec("name=s,type=webhook,url=http://x," + bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if ts, err := parseSinkSpec("name=bus,type=kafka,url=kafka://a:9092,tombstones=true"); err != nil || !ts.tombstones {
		t.Fatalf("tombstones: %+v %v", ts, err)
	}
	for _, bad := range []string{"name=bus,type=kafka,url=kafka://a,tombstones=maybe", "name=r,type=redis,url=redis://x,tombstones=true"} {
		if _, err := parseSinkSpec(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	mir, err := parseSinkSpec("name=olap,type=clickhouse,url=http://x,mode=mirror,into=pg_{table}")
	if err != nil || !mir.mirror || mir.chTable != "pg_{table}" {
		t.Fatalf("mirror: %+v %v", mir, err)
	}
	if _, err := newSink(mir); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"name=o,type=clickhouse,url=http://x,mode=copy", "name=o,type=webhook,url=http://x,mode=mirror"} {
		if _, err := parseSinkSpec(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if sp, _ := parseSinkSpec("name=o,type=clickhouse,url=http://x,into={table}"); sp.name != "" {
		if _, err := newSink(sp); err == nil {
			t.Error("a table template without mode=mirror accepted")
		}
	}
	kf, err := parseSinkSpec("name=bus,type=kafka,url=kafka://a:9092,url=kafka://b:9092,topic=cdc.{table},table=orders")
	if err != nil || len(kf.urls) != 2 || kf.topic != "cdc.{table}" {
		t.Fatalf("kafka: %+v %v", kf, err)
	}
	for _, spec := range []string{"name=q,type=nats,url=nats://x,subject=a.b,stream=s", "name=r,type=redis,url=redis://x,stream=k,maxlen=9",
		"name=m,type=meilisearch,url=http://x,index=i", "name=bus,type=kafka,url=kafka://a:9092,url=kafka://b:9092"} {
		sp, err := parseSinkSpec(spec)
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if _, err := newSink(sp); err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
	}
	for _, bad := range []string{
		"type=webhook,url=http://x",                     // no name
		"name=Hook,type=webhook,url=http://x",           // bad name
		"name=h,type=mqtt,url=http://x",                 // unknown type
		"name=h,type=webhook,url=http://x,url=http://y", // several urls: kafka only
		"name=h,type=redis,url=redis://x,topic=t",       // kafka key on redis
		"name=h,type=redis,url=redis://x,maxlen=-1",
		"name=h,type=webhook",                          // no url
		"name=h,type=webhook,url=http://x,into=t",      // clickhouse key on webhook
		"name=h,type=webhook,url=http://x,start=now",   // bad start
		"name=h,type=webhook,url=http://x,colour=blue", // unknown key
		"name=h,name=g,type=webhook,url=http://x",      // twice
		"name=h,type=webhook,url",                      // no value
		"name=h,type=webhook,url=http://x,policy=keep", // bad policy
	} {
		if _, err := parseSinkSpec(bad); !errors.Is(err, errUsage) {
			t.Errorf("%q: got %v", bad, err)
		}
	}
	base := []string{"--id", "1", "--peers", "1=a:1", "--data-dir", "/d"}
	cfg, err := parseServerFlags(append(base, "--sink", "name=a,type=webhook,url=http://x", "--sink", "name=b,type=webhook,url=http://y"), io.Discard, os.Getenv)
	if err != nil || len(cfg.sinks) != 2 || cfg.sinks[1].url != "http://y" || !cfg.raft.PreVote {
		t.Fatalf("%+v %v", cfg, err)
	}
	if cfg, _ := parseServerFlags(append(base, "--pre-vote=false"), io.Discard, os.Getenv); cfg.raft.PreVote {
		t.Fatal("--pre-vote=false ignored")
	}
	_, err = parseServerFlags(append(base, "--sink", "name=a,type=webhook,url=http://x", "--sink", "name=a,type=webhook,url=http://y"), io.Discard, os.Getenv)
	if !errors.Is(err, errUsage) {
		t.Fatalf("duplicate sink name: %v", err)
	}
}

func TestPrintPipeline(t *testing.T) {
	var out bytes.Buffer
	printPipeline(&out, &pb.StatusResponse{SourceState: "retrying", SourceError: "pg: connect: refused",
		Retention: &pb.RetentionStatus{MaxAgeSeconds: 7 * 86400, MaxBytes: 10 << 30, LogBytes: 3 << 19, CompactLimit: 4, BlockedBy: "hook"},
		Sinks:     []*pb.SinkStatus{{Name: "hook", Type: "webhook", Policy: "block", State: "running", Offset: 9, Lag: 2, Delivered: 30}}})
	want := "\nRETENTION max age 168h0m0s, max size 10.0 GiB; log 1.5 MiB; removable up to entry 4 (held back by sink hook)\n" +
		"SOURCE postgres: retrying (pg: connect: refused)\n" +
		"SINK  TYPE     POLICY  STATE    OFFSET  LAG  DELIVERED  ERROR\n" +
		"hook  webhook  block   running  9       2    30         -\n"
	if out.String() != want {
		t.Fatalf("got\n%q\nwant\n%q", out.String(), want)
	}
	out.Reset()
	printPipeline(&out, &pb.StatusResponse{})
	if out.Len() != 0 {
		t.Fatalf("printed %q for a node without pipeline", out.String())
	}
	printPipeline(&out, &pb.StatusResponse{Retention: &pb.RetentionStatus{LogBytes: 100}})
	if want := "\nRETENTION keep everything; log 100 B\n"; out.String() != want {
		t.Fatalf("got %q", out.String())
	}
}

func TestVersionFrom(t *testing.T) {
	info := func(v string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Main: debug.Module{Version: v}}, true }
	}
	none := func() (*debug.BuildInfo, bool) { return nil, false }
	for _, tc := range []struct {
		ldflags string
		info    func() (*debug.BuildInfo, bool)
		want    string
	}{
		{"0.1.0", info("v0.2.0"), "0.1.0"}, // a release build wins
		{devVersion, info("v0.2.0"), "0.2.0"},
		{devVersion, info("(devel)"), devVersion},
		{devVersion, none, devVersion},
	} {
		if got := versionFrom(tc.ldflags, tc.info); got != tc.want {
			t.Errorf("versionFrom(%q) = %q, want %q", tc.ldflags, got, tc.want)
		}
	}
}
