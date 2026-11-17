package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

const defaultAddrs = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"

// clientFlags are shared by the client commands.
type clientFlags struct {
	addrs   []string
	timeout time.Duration
}

func parseClientFlags(name string, args []string, stderr io.Writer) (clientFlags, []string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	def := os.Getenv("CURNTVALV_ADDR")
	if def == "" {
		def = defaultAddrs
	}
	addr := fs.String("addr", def, "node addresses, comma separated (env CURNTVALV_ADDR)")
	timeout := fs.Duration("timeout", 5*time.Second, "overall timeout")
	// Allow flags after positional arguments: "kv get KEY --addr x".
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return clientFlags{}, nil, err
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
	var addrs []string
	for _, a := range strings.Split(*addr, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return clientFlags{}, nil, usageErr("--addr is empty")
	}
	return clientFlags{addrs: addrs, timeout: *timeout}, pos, nil
}

func dial(addr string) (*grpc.ClientConn, pb.AdminServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewAdminServiceClient(conn), nil
}

func statusCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cf, pos, err := parseClientFlags("status", args, stderr)
	if err != nil {
		return err
	}
	if len(pos) > 0 {
		return usageErr("status takes no arguments")
	}
	ctx, cancel := context.WithTimeout(ctx, cf.timeout)
	defer cancel()

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ADDR\tID\tSTATE\tTERM\tLEADER\tFIRST\tLAST\tCOMMIT\tAPPLIED\tSNAPSHOT")
	reachable := 0
	for _, addr := range cf.addrs {
		st, err := nodeStatus(ctx, addr)
		if err != nil {
			fmt.Fprintf(w, "%s\t-\tunreachable\t-\t-\t-\t-\t-\t-\t-\n", addr)
			continue
		}
		reachable++
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", addr, st.GetId(), st.GetState(), st.GetTerm(),
			st.GetLeader(), st.GetFirstIndex(), st.GetLastIndex(), st.GetCommitIndex(), st.GetLastApplied(), st.GetSnapshotIndex())
	}
	w.Flush()
	if reachable == 0 {
		return errors.New("no node reachable")
	}
	return nil
}

func nodeStatus(ctx context.Context, addr string) (*pb.StatusResponse, error) {
	conn, client, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return client.Status(ctx, &pb.StatusRequest{})
}

func kvCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cf, pos, err := parseClientFlags("kv", args, stderr)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return usageErr("want: kv put KEY VALUE | kv get KEY | kv del KEY")
	}
	op, key := pos[0], pos[1]
	var call func(ctx context.Context, c pb.AdminServiceClient) error
	switch {
	case op == "put" && len(pos) == 3:
		call = func(ctx context.Context, c pb.AdminServiceClient) error {
			r, err := c.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(pos[2])})
			if err == nil {
				fmt.Fprintf(stdout, "OK (index %d)\n", r.GetIndex())
			}
			return err
		}
	case op == "get" && len(pos) == 2:
		call = func(ctx context.Context, c pb.AdminServiceClient) error {
			r, err := c.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				return err
			}
			if !r.GetFound() {
				return errNotFound
			}
			fmt.Fprintln(stdout, string(r.GetValue()))
			return nil
		}
	case op == "del" && len(pos) == 2:
		call = func(ctx context.Context, c pb.AdminServiceClient) error {
			r, err := c.Delete(ctx, &pb.DeleteRequest{Key: key})
			if err == nil {
				fmt.Fprintf(stdout, "OK (index %d)\n", r.GetIndex())
			}
			return err
		}
	default:
		return usageErr("want: kv put KEY VALUE | kv get KEY | kv del KEY")
	}
	ctx, cancel := context.WithTimeout(ctx, cf.timeout)
	defer cancel()
	return onLeader(ctx, cf.addrs, call)
}

var errNotFound = errors.New("key not found")

// onLeader runs call against the nodes in turn until one that is the leader
// answers. Followers answer FAILED_PRECONDITION and unreachable nodes
// UNAVAILABLE; while no leader is known it keeps retrying until ctx expires.
func onLeader(ctx context.Context, addrs []string, call func(context.Context, pb.AdminServiceClient) error) error {
	var last error
	for {
		for _, addr := range addrs {
			conn, client, err := dial(addr)
			if err != nil {
				last = err
				continue
			}
			actx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = call(actx, client)
			cancel()
			conn.Close()
			if errors.Is(err, errNotFound) {
				return err
			}
			switch status.Code(err) {
			case codes.OK:
				return err // nil, or errNotFound
			case codes.FailedPrecondition, codes.Unavailable, codes.DeadlineExceeded:
				last = fmt.Errorf("%s: %s", addr, status.Convert(err).Message())
				if ctx.Err() != nil {
					return last
				}
				continue
			default:
				return fmt.Errorf("%s: %s", addr, status.Convert(err).Message())
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("no leader answered: %w", last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func logCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("log", flag.ContinueOnError)
	fs.SetOutput(stderr)
	from := fs.Uint64("from", 0, "first log index (0 = oldest retained)")
	limit := fs.Int("limit", 0, "stop after this many events (0 = no limit)")
	follow := fs.Bool("follow", false, "keep waiting for new entries")
	def := os.Getenv("CURNTVALV_ADDR")
	if def == "" {
		def = defaultAddrs
	}
	addr := fs.String("addr", def, "node addresses, comma separated; the first reachable one is used")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return usageErr("log takes no arguments")
	}
	client, closeFn, err := firstReachable(ctx, strings.Split(*addr, ","))
	if err != nil {
		return err
	}
	defer closeFn()

	enc := json.NewEncoder(stdout)
	next, printed := *from, 0
	for {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.ReadLog(rctx, &pb.ReadLogRequest{FromIndex: next, Limit: 500})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.New(status.Convert(err).Message())
		}
		for _, e := range resp.GetEntries() {
			next = e.GetIndex() + 1
			lookup := func(id uint64) (*pb.Schema, bool) {
				sc, ok := resp.GetSchemas()[id]
				return sc, ok
			}
			for _, c := range event.Changes(e.GetIndex(), e.GetTxBatch(), lookup, nil) {
				if *limit > 0 && printed >= *limit {
					return nil
				}
				if err := enc.Encode(c.JSON()); err != nil {
					return err
				}
				printed++
			}
		}
		if len(resp.GetEntries()) > 0 {
			continue
		}
		if !*follow {
			return nil
		}
		if next == 0 {
			next = resp.GetFirstIndex()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func firstReachable(ctx context.Context, addrs []string) (pb.AdminServiceClient, func(), error) {
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, err := nodeStatus(ctx, a); err != nil {
			continue
		}
		conn, client, err := dial(a)
		if err != nil {
			continue
		}
		return client, func() { conn.Close() }, nil
	}
	return nil, nil, errors.New("no node reachable")
}

// listFlag is a flag that may be repeated and also takes comma-separated
// values.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			*l = append(*l, s)
		}
	}
	return nil
}

func subscribeCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	from := fs.Uint64("from", 0, "first log index (0 = oldest retained); use the last printed index + 1 to resume")
	limit := fs.Int("limit", 0, "stop after this many events (0 = run until interrupted)")
	var tables listFlag
	fs.Var(&tables, "table", "table to receive, as NAME or NAMESPACE.NAME; repeat or comma separate (default: all)")
	def := os.Getenv("CURNTVALV_ADDR")
	if def == "" {
		def = defaultAddrs
	}
	addr := fs.String("addr", def, "node addresses, comma separated; on failure the next one is used")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return usageErr("subscribe takes no arguments")
	}
	var addrs []string
	for _, a := range strings.Split(*addr, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return usageErr("--addr is empty")
	}

	enc := json.NewEncoder(stdout)
	next, printed := *from, 0
	for attempt := 0; ; attempt++ {
		a := addrs[attempt%len(addrs)]
		err := subscribeOnce(ctx, a, &pb.SubscribeRequest{FromIndex: next, Tables: tables}, func(resp *pb.SubscribeResponse, schemas map[uint64]*pb.Schema) error {
			for _, ce := range resp.GetEvents() {
				if *limit > 0 && printed >= *limit {
					return errLimit
				}
				c := event.Change{Index: ce.GetIndex(), CommitLSN: ce.GetCommitLsn(), Seq: ce.GetSeq(), Xid: ce.GetXid(),
					CommitTimeUs: ce.GetCommitTimeUs(), Event: ce.GetEvent(), Schema: schemas[ce.GetEvent().GetSchemaId()]}
				if err := enc.Encode(c.JSON()); err != nil {
					return err
				}
				printed++
			}
			next = resp.GetNextIndex()
			if *limit > 0 && printed >= *limit {
				return errLimit
			}
			return nil
		})
		switch code := status.Code(err); {
		case errors.Is(err, errLimit), ctx.Err() != nil:
			return nil
		case code == codes.OutOfRange, code == codes.InvalidArgument:
			return errors.New(status.Convert(err).Message())
		case err != nil && code == codes.Unknown:
			return err // e.g. writing stdout failed
		}
		fmt.Fprintf(stderr, "subscribe: %s: %s; retrying from index %d\n", a, status.Convert(err).Message(), next)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

var errLimit = errors.New("limit reached")

// subscribeOnce runs one Subscribe stream against addr and calls handle for
// every response, with all schemas received on the stream so far.
func subscribeOnce(ctx context.Context, addr string, req *pb.SubscribeRequest, handle func(*pb.SubscribeResponse, map[uint64]*pb.Schema) error) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := pb.NewSubscribeServiceClient(conn).Subscribe(ctx, req)
	if err != nil {
		return err
	}
	schemas := map[uint64]*pb.Schema{}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		for id, sc := range resp.GetSchemas() {
			schemas[id] = sc
		}
		if err := handle(resp, schemas); err != nil {
			return err
		}
	}
}
