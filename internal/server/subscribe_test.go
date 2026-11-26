package server

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

var _ ChangeReader = (*event.Reader)(nil)

type readResult struct {
	res event.Result
	err error
}

// scriptedReader returns the queued results in order and records the
// arguments it was called with.
type scriptedReader struct {
	svc     *Subscribe // the service using this reader
	results chan readResult
	calls   chan readCall
}

type readCall struct {
	from   uint64
	filter event.Filter
}

func (r *scriptedReader) Read(ctx context.Context, from uint64, _ int, filter event.Filter) (event.Result, error) {
	r.calls <- readCall{from, filter}
	select {
	case rr := <-r.results:
		return rr.res, rr.err
	case <-ctx.Done():
		return event.Result{}, ctx.Err()
	}
}

func subscribeClient(t *testing.T) (pb.SubscribeServiceClient, *scriptedReader) {
	t.Helper()
	r := &scriptedReader{results: make(chan readResult, 10), calls: make(chan readCall, 100)}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	r.svc = NewSubscribe(r, nil, nil)
	pb.RegisterSubscribeServiceServer(gs, r.svc)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewSubscribeServiceClient(conn), r
}

var ordersSchema = &pb.Schema{Id: 7, Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}}}

func change(index, lsn uint64, seq uint32) event.Change {
	return event.Change{Index: index, CommitLSN: lsn, Seq: seq, Xid: 3, CommitTimeUs: 99, Schema: ordersSchema,
		Event: &pb.Event{SchemaId: 7, Op: pb.Op_OP_INSERT, After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte("1")}}}}}
}

func TestSubscribeStreamsChanges(t *testing.T) {
	c, r := subscribeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.Subscribe(ctx, &pb.SubscribeRequest{FromIndex: 5, Tables: []string{"orders"}})
	if err != nil {
		t.Fatal(err)
	}
	if call := <-r.calls; call.from != 5 || len(call.filter) != 1 || call.filter[0] != "orders" {
		t.Fatalf("first read %+v", call)
	}
	r.results <- readResult{res: event.Result{Changes: []event.Change{change(5, 0x100, 0), change(5, 0x100, 1)}, Next: 7, TxEntries: 2}}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Events) != 2 || resp.NextIndex != 7 || resp.Events[1].Id != "0/100:1" || resp.Events[0].Index != 5 {
		t.Fatalf("first response %+v", resp)
	}
	if sc := resp.Schemas[7]; sc.GetName() != "orders" || len(resp.Schemas) != 1 {
		t.Fatalf("schemas %v", resp.Schemas)
	}

	// The next read continues at next_index; the schema is not sent again.
	if call := <-r.calls; call.from != 7 {
		t.Fatalf("second read from %d", call.from)
	}
	r.results <- readResult{res: event.Result{Changes: []event.Change{change(8, 0x200, 0)}, Next: 9, TxEntries: 1}}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Events) != 1 || resp.NextIndex != 9 || len(resp.Schemas) != 0 {
		t.Fatalf("second response %+v", resp)
	}

	// A read with no matching changes is not sent right away...
	<-r.calls
	r.results <- readResult{res: event.Result{Next: 12, TxEntries: 3}}
	if call := <-r.calls; call.from != 12 {
		t.Fatalf("third read from %d", call.from)
	}
	// ...but next_index catches up with the next response.
	r.results <- readResult{res: event.Result{Changes: []event.Change{change(12, 0x300, 0)}, Next: 13}}
	if resp, err = stream.Recv(); err != nil || resp.NextIndex != 13 {
		t.Fatalf("after skipped entries: %+v %v", resp, err)
	}
	if st := r.svc.Stats(); st.Active != 1 || st.Sent != 4 {
		t.Fatalf("stats %+v", st)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for r.svc.Stats().Active != 0 {
		if time.Now().After(deadline) {
			t.Fatal("stream still counted after the client left")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSubscribeErrors(t *testing.T) {
	c, r := subscribeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _ := c.Subscribe(ctx, &pb.SubscribeRequest{FromIndex: 1})
	r.results <- readResult{err: raft.ErrCompacted}
	if _, err := stream.Recv(); status.Code(err) != codes.OutOfRange {
		t.Fatalf("compacted: %v", err)
	}
	stream, _ = c.Subscribe(ctx, &pb.SubscribeRequest{Tables: []string{""}})
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty table: %v", err)
	}
}

func TestSubscribeSplitsLargeReadsBetweenEntries(t *testing.T) {
	c, r := subscribeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _ := c.Subscribe(ctx, &pb.SubscribeRequest{})
	big := func(index uint64, seq uint32) event.Change {
		ch := change(index, index<<8, seq)
		ch.Event = &pb.Event{SchemaId: 7, After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: make([]byte, 400<<10)}}}}
		return ch
	}
	// Entries 1 (3 changes, 1.2 MiB), 2 and 3 (1 change each).
	changes := []event.Change{big(1, 0), big(1, 1), big(1, 2), big(2, 0), big(3, 0)}
	r.results <- readResult{res: event.Result{Changes: changes, Next: 4}}
	var got [][]uint64
	var next []uint64
	for total := 0; total < len(changes); {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		var idx []uint64
		for _, e := range resp.Events {
			idx = append(idx, e.Index)
		}
		got, next = append(got, idx), append(next, resp.NextIndex)
		total += len(resp.Events)
	}
	// Entry 1 crosses the limit and is sent whole; entries 2 and 3 together
	// stay under it and travel in the second response.
	if len(got) != 2 || len(got[0]) != 3 || next[0] != 2 || next[1] != 4 {
		t.Fatalf("responses %v next %v", got, next)
	}
}
