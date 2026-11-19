package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vympel87/curntvalv/client"
	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/security"
)

const defaultAddrs = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"

// clientFlags are shared by the client commands.
type clientFlags struct {
	addrs   []string
	timeout time.Duration
	conn    connFlags
}

// connFlags are the TLS and token settings of a client command.
type connFlags struct {
	caFile, tokenFile *string
	tls               *tls.Config
	token             string
	dial              []grpc.DialOption
}

// addConnFlags adds --tls-ca and --token-file to fs; load reads the files
// after fs.Parse.
func addConnFlags(fs *flag.FlagSet) *connFlags {
	return &connFlags{
		caFile:    fs.String("tls-ca", os.Getenv("CURNTVALV_TLS_CA"), "connect with TLS, trusting this CA certificate (env CURNTVALV_TLS_CA)"),
		tokenFile: fs.String("token-file", "", "file with the cluster's token (or env CURNTVALV_TOKEN)"),
	}
}

func (c *connFlags) load() error {
	if *c.caFile != "" {
		cfg, err := security.ClientTLS(*c.caFile, "", "")
		if err != nil {
			return usageErr("--tls-ca: %v", err)
		}
		c.tls = cfg
	}
	c.token = os.Getenv("CURNTVALV_TOKEN")
	if *c.tokenFile != "" {
		tok, err := security.ReadToken(*c.tokenFile)
		if err != nil {
			return usageErr("--token-file: %v", err)
		}
		c.token = tok
	}
	c.dial = security.DialOptions(c.tls, c.token)
	return nil
}

// clientOptions are the client package options for c.
func (c *connFlags) clientOptions() []client.Option {
	var opts []client.Option
	if c.tls != nil {
		opts = append(opts, client.WithTLS(c.tls))
	}
	if c.token != "" {
		opts = append(opts, client.WithToken(c.token))
	}
	return opts
}

// parseClientFlags parses the shared client flags; extra adds a command's
// own flags.
func parseClientFlags(name string, args []string, stderr io.Writer, extra ...func(*flag.FlagSet)) (clientFlags, []string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	for _, f := range extra {
		f(fs)
	}
	def := os.Getenv("CURNTVALV_ADDR")
	if def == "" {
		def = defaultAddrs
	}
	addr := fs.String("addr", def, "node addresses, comma separated (env CURNTVALV_ADDR)")
	timeout := fs.Duration("timeout", 5*time.Second, "overall timeout")
	conn := addConnFlags(fs)
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
	if err := conn.load(); err != nil {
		return clientFlags{}, nil, err
	}
	return clientFlags{addrs: addrs, timeout: *timeout, conn: *conn}, pos, nil
}

func dial(opts []grpc.DialOption, addr string) (*grpc.ClientConn, pb.AdminServiceClient, error) {
	conn, err := grpc.NewClient(addr, opts...)
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
	var leader *pb.StatusResponse
	for _, addr := range cf.addrs {
		st, err := nodeStatus(ctx, cf.conn.dial, addr)
		if err != nil {
			fmt.Fprintf(w, "%s\t-\tunreachable\t-\t-\t-\t-\t-\t-\t-\n", addr)
			continue
		}
		reachable++
		if st.GetState() == "leader" {
			leader = st
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", addr, st.GetId(), st.GetState(), st.GetTerm(),
			st.GetLeader(), st.GetFirstIndex(), st.GetLastIndex(), st.GetCommitIndex(), st.GetLastApplied(), st.GetSnapshotIndex())
	}
	w.Flush()
	if reachable == 0 {
		return errors.New("no node reachable")
	}
	if leader != nil {
		printPipeline(stdout, leader)
	}
	return nil
}

// printPipeline prints the source and sinks as the leader (which runs them)
// sees them. It is separated from the node table by an empty line.
func printPipeline(out io.Writer, st *pb.StatusResponse) {
	if st.GetSourceState() == "" && len(st.GetSinks()) == 0 && st.GetRetention() == nil {
		return
	}
	fmt.Fprintln(out)
	if r := st.GetRetention(); r != nil {
		fmt.Fprintf(out, "RETENTION %s; log %s", retentionLimits(r), formatBytes(r.GetLogBytes()))
		switch {
		case r.GetBlockedBy() != "":
			fmt.Fprintf(out, "; removable up to entry %d (held back by sink %s)", r.GetCompactLimit(), r.GetBlockedBy())
		case r.GetCompactLimit() > 0:
			fmt.Fprintf(out, "; removable up to entry %d", r.GetCompactLimit())
		}
		fmt.Fprintln(out)
	}
	if st.GetSourceState() != "" {
		fmt.Fprintf(out, "SOURCE postgres: %s", st.GetSourceState())
		if st.GetSourceError() != "" {
			fmt.Fprintf(out, " (%s)", st.GetSourceError())
		}
		fmt.Fprintln(out)
	}
	if len(st.GetSinks()) == 0 {
		return
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SINK\tTYPE\tPOLICY\tSTATE\tOFFSET\tLAG\tDELIVERED\tERROR")
	for _, s := range st.GetSinks() {
		e := s.GetLastError()
		if e == "" {
			e = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\n", s.GetName(), s.GetType(), s.GetPolicy(), s.GetState(),
			s.GetOffset(), s.GetLag(), s.GetDelivered(), e)
	}
	w.Flush()
}

func retentionLimits(r *pb.RetentionStatus) string {
	var parts []string
	if a := r.GetMaxAgeSeconds(); a > 0 {
		parts = append(parts, "max age "+(time.Duration(a)*time.Second).String())
	}
	if b := r.GetMaxBytes(); b > 0 {
		parts = append(parts, "max size "+formatBytes(b))
	}
	if len(parts) == 0 {
		return "keep everything"
	}
	return strings.Join(parts, ", ")
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// sinkCmd runs "sink reset NAME --to earliest|latest|INDEX".
func sinkCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	const want = "want: sink reset NAME --to earliest|latest|INDEX"
	var to string
	cf, pos, err := parseClientFlags("sink", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&to, "to", "", "new position: earliest (replay everything retained), latest (skip to now) "+
			"or a log index (the last entry counted as delivered)")
	})
	if err != nil {
		return err
	}
	if len(pos) != 2 || pos[0] != "reset" {
		return usageErr(want)
	}
	req := &pb.ResetSinkRequest{Name: pos[1]}
	switch to {
	case "earliest":
		req.Position = pb.ResetPosition_RESET_POSITION_EARLIEST
	case "latest":
		req.Position = pb.ResetPosition_RESET_POSITION_LATEST
	default:
		idx, err := strconv.ParseUint(to, 10, 64)
		if err != nil {
			return usageErr(want)
		}
		req.Position, req.Index = pb.ResetPosition_RESET_POSITION_INDEX, idx
	}
	ctx, cancel := context.WithTimeout(ctx, cf.timeout)
	defer cancel()
	return onLeader(ctx, cf, func(ctx context.Context, c pb.AdminServiceClient) error {
		r, err := c.ResetSink(ctx, req)
		if err == nil {
			fmt.Fprintf(stdout, "sink %s reset to offset %d, generation %d (index %d)\n", req.Name, r.GetOffset(), r.GetGeneration(), r.GetIndex())
		}
		return err
	})
}

func nodeStatus(ctx context.Context, opts []grpc.DialOption, addr string) (*pb.StatusResponse, error) {
	conn, client, err := dial(opts, addr)
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
	return onLeader(ctx, cf, call)
}

var errNotFound = errors.New("key not found")

// onLeader runs call against the nodes in turn until one that is the leader
// answers. Followers answer FAILED_PRECONDITION and unreachable nodes
// UNAVAILABLE; while no leader is known it keeps retrying until ctx expires.
func onLeader(ctx context.Context, cf clientFlags, call func(context.Context, pb.AdminServiceClient) error) error {
	var last error
	for {
		for _, addr := range cf.addrs {
			conn, client, err := dial(cf.conn.dial, addr)
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
	cf := addConnFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return usageErr("log takes no arguments")
	}
	if err := cf.load(); err != nil {
		return err
	}
	client, closeFn, err := firstReachable(ctx, cf.dial, strings.Split(*addr, ","))
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

func firstReachable(ctx context.Context, opts []grpc.DialOption, addrs []string) (pb.AdminServiceClient, func(), error) {
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, err := nodeStatus(ctx, opts, a); err != nil {
			continue
		}
		conn, client, err := dial(opts, a)
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
	cf := addConnFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return usageErr("subscribe takes no arguments")
	}
	if err := cf.load(); err != nil {
		return err
	}
	c, err := client.New([]string{*addr}, cf.clientOptions()...)
	if err != nil {
		return usageErr("--addr is empty")
	}
	defer c.Close()

	enc := json.NewEncoder(stdout)
	printed := 0
	opts := client.Options{From: *from, Tables: tables, OnRetry: func(addr string, err error, next uint64) {
		fmt.Fprintf(stderr, "subscribe: %s: %s; retrying from index %d\n", addr, status.Convert(err).Message(), next)
	}}
	for ch, err := range c.Subscribe(ctx, opts) {
		if err != nil {
			return err
		}
		if err := enc.Encode(ch); err != nil {
			return err
		}
		if printed++; *limit > 0 && printed >= *limit {
			return nil
		}
	}
	return nil
}
