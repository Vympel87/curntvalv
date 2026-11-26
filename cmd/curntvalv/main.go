// Command curntvalv is the single binary for the curntValv server and CLI.
//
//	curntvalv dev --pg-url postgres://...   (one local node and its dashboard)
//	curntvalv server --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data-dir ./data/1
//	curntvalv server --config /etc/curntvalv/node1.conf
//	curntvalv status [--addr ...]
//	curntvalv kv put|get|del KEY [VALUE] [--addr ...]
//	curntvalv log [--from N] [--follow] [--addr ...]
//	curntvalv subscribe [--table T]... [--from N] [--addr ...]
//	curntvalv sink reset NAME --to earliest|latest|INDEX [--addr ...]
//	curntvalv source reset --to-slot [--addr ...]
//	curntvalv backfill start|cancel TABLE... [--addr ...]
//	curntvalv member add ID=HOST:PORT | remove ID | list [--addr ...]
//	curntvalv subscription list | reset NAME --to ... | delete NAME [--addr ...]
//	curntvalv version
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/retention"
	"github.com/Vympel87/curntvalv/internal/security"
	"github.com/Vympel87/curntvalv/internal/server"
	"github.com/Vympel87/curntvalv/internal/sink"
	pgsource "github.com/Vympel87/curntvalv/internal/source/pg"
	"github.com/Vympel87/curntvalv/internal/transport"
	"github.com/Vympel87/curntvalv/internal/wal"
	"github.com/Vympel87/curntvalv/internal/web"
)

// version is set by release builds (-ldflags "-X main.version=...").
var version = devVersion

const devVersion = "0.0.0-dev"

// appVersion is the release build's version, else the module version that
// `go install ...@v0.2.0` or `go tool` records, else devVersion.
func appVersion() string { return versionFrom(version, debug.ReadBuildInfo) }

func versionFrom(ldflags string, buildInfo func() (*debug.BuildInfo, bool)) string {
	if ldflags != devVersion {
		return ldflags
	}
	if bi, ok := buildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return strings.TrimPrefix(bi.Main.Version, "v")
	}
	return devVersion
}

const usage = `usage:
  curntvalv dev [--pg-url URL]                  one local node and its dashboard
  curntvalv server --id N --peers ID=HOST:PORT,... --data-dir DIR [flags]
  curntvalv server --config FILE [--check] [flags]
  curntvalv status [--addr HOST:PORT,...]
  curntvalv kv put KEY VALUE [--addr HOST:PORT,...]
  curntvalv kv get KEY [--addr HOST:PORT,...]
  curntvalv kv del KEY [--addr HOST:PORT,...]
  curntvalv log [--from N] [--follow] [--addr HOST:PORT,...]
  curntvalv subscribe [--table NAME]... [--from N | --name NAME] [--addr HOST:PORT,...]
  curntvalv sink reset NAME --to earliest|latest|INDEX [--addr HOST:PORT,...]
  curntvalv source reset --to-slot [--addr HOST:PORT,...]   after a failover check stopped ingest
  curntvalv backfill start TABLE... [--addr HOST:PORT,...]  copy existing rows into the log
  curntvalv backfill cancel TABLE... [--addr HOST:PORT,...]
  curntvalv member add ID=HOST:PORT [--addr HOST:PORT,...]  add a node started with --join
  curntvalv member remove ID [--addr HOST:PORT,...]
  curntvalv member list [--addr HOST:PORT,...]
  curntvalv subscription list [--addr HOST:PORT,...]  named subscriptions (subscribe --name)
  curntvalv subscription reset NAME --to earliest|latest|INDEX [--addr HOST:PORT,...]
  curntvalv subscription delete NAME [--addr HOST:PORT,...]
  curntvalv certs [--out DIR] NODE...           a CA and node certificates for TLS
  curntvalv version

Run "curntvalv COMMAND -h" for the flags of a command.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, "curntvalv", appVersion())
		return 0
	case "dev":
		err = devCmd(ctx, args[1:], stderr)
	case "server":
		err = serverCmd(ctx, args[1:], stderr)
	case "status":
		err = statusCmd(ctx, args[1:], stdout, stderr)
	case "kv":
		err = kvCmd(ctx, args[1:], stdout, stderr)
	case "log":
		err = logCmd(ctx, args[1:], stdout, stderr)
	case "subscribe":
		err = subscribeCmd(ctx, args[1:], stdout, stderr)
	case "sink":
		err = sinkCmd(ctx, args[1:], stdout, stderr)
	case "source":
		err = sourceCmd(ctx, args[1:], stdout, stderr)
	case "backfill":
		err = backfillCmd(ctx, args[1:], stdout, stderr)
	case "member":
		err = memberCmd(ctx, args[1:], stdout, stderr)
	case "subscription":
		err = subscriptionCmd(ctx, args[1:], stdout, stderr)
	case "certs":
		err = certsCmd(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, err)
		return 2
	default:
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
}

var errUsage = errors.New("usage error")

func usageErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, a...))
}

// parseAge parses a duration that may also use days ("7d", "1d12h").
func parseAge(s string) (time.Duration, error) {
	if s == "0" || s == "" {
		return 0, nil
	}
	var days time.Duration
	if before, after, ok := strings.Cut(s, "d"); ok {
		n, err := strconv.ParseUint(before, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("bad duration %q, want e.g. 7d, 36h or 90m", s)
		}
		days, s = time.Duration(n)*24*time.Hour, after
		if s == "" {
			return days, nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("bad duration %q, want e.g. 7d, 36h or 90m", s)
	}
	return days + d, nil
}

// parseBytes parses a size: a number of bytes with an optional unit KB, MB,
// GB or TB (powers of 1024).
func parseBytes(s string) (int64, error) {
	num := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30}, {"TB", 1 << 40}, {"B", 1}} {
		if strings.HasSuffix(num, u.suffix) {
			num, mult = strings.TrimSuffix(num, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil || n < 0 || n > (1<<62)/mult {
		return 0, fmt.Errorf("bad size %q, want e.g. 512MB or 10GB", s)
	}
	return n * mult, nil
}

// parsePeers parses "1=host:port,2=host:port".
func parsePeers(s string) (map[uint64]string, error) {
	peers := map[uint64]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if !ok || err != nil || id == 0 || addr == "" {
			return nil, usageErr("bad peer %q, want ID=HOST:PORT", part)
		}
		if _, dup := peers[id]; dup {
			return nil, usageErr("peer %d listed twice", id)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, usageErr("--peers is empty")
	}
	return peers, nil
}

type serverConfig struct {
	raft     raft.Config
	peers    map[uint64]string
	dataDir  string
	listen   string
	noSync   bool
	logLevel slog.Level
	pg       pgsource.Config // pg.URL empty: no Postgres source
	sinks    []sinkSpec
	// retention without Block; the block sinks come from sinks.
	retention    retention.Config
	segmentBytes int64
	httpListen   string // empty: no HTTP server
	// TLS files, all set or all empty (design D15), and the token.
	tlsCert, tlsKey, tlsCA string
	token                  string
	check                  bool     // --check: validate and exit
	warnings               []string // from reading the config file
}

// parseServerFlags parses the server's flags and, with --config or
// CURNTVALV_CONFIG, a config file for the flags left unset (design D16).
func parseServerFlags(args []string, stderr io.Writer, getenv func(string) string) (serverConfig, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configFile := fs.String("config", getenv("CURNTVALV_CONFIG"), "read unset flags from this file of KEY = VALUE lines, KEY being a flag name (env CURNTVALV_CONFIG);\nflags beat environment variables, which beat the file")
	check := fs.Bool("check", false, "check the flags and config file, load the TLS files, and exit")
	id := fs.Uint64("id", 0, "id of this node (must appear in --peers, unless --join)")
	peersFlag := fs.String("peers", "", "the initial cluster members as ID=HOST:PORT,... (including this node); once members were added or removed (curntvalv member), the stored membership is used")
	join := fs.Bool("join", false, "start a new node that waits to be added with `curntvalv member add ID=HOST:PORT`; --peers then lists the current members")
	dataDir := fs.String("data-dir", "", "directory for the log, Raft state and snapshots")
	listen := fs.String("listen", "", "address to listen on (default: this node's address in --peers)")
	tick := fs.Duration("tick", 100*time.Millisecond, "Raft tick length")
	election := fs.Int("election-ticks", 10, "minimum election timeout in ticks")
	heartbeat := fs.Int("heartbeat-ticks", 1, "heartbeat interval in ticks")
	snapEvery := fs.Int("snapshot-entries", 1024, "take a snapshot every N applied entries")
	preVote := fs.Bool("pre-vote", true, "ask the other nodes before starting an election, so a node that was cut off does not force one when it returns")
	retAge := fs.String("retention-age", "0", "remove log segments older than this, e.g. 7d or 36h (0 = no age limit)")
	retBytes := fs.String("retention-bytes", "0", "remove the oldest log segments while the log is larger than this, e.g. 10GB (0 = no size limit)")
	segBytes := fs.String("wal-segment-bytes", "64MB", "size at which a new log segment is started; retention removes whole segments")
	httpListen := fs.String("http-listen", "127.0.0.1:8080", "address for the dashboard, /metrics, /healthz and /readyz (empty = off); TLS and the token apply here too")
	noSync := fs.Bool("unsafe-no-fsync", false, "skip fsync (tests and demos only: a crash can lose committed data)")
	logLevel := fs.String("log-level", "info", "debug, info, warn or error")
	pgURL := fs.String("pg-url", getenv("CURNTVALV_PG_URL"), "Postgres connection URL to capture changes from (env CURNTVALV_PG_URL; empty = no source)")
	pgSlot := fs.String("pg-slot", "curntvalv", "logical replication slot (created on a new cluster)")
	pgPub := fs.String("pg-publication", "curntvalv", "Postgres publication to read (must exist)")
	backfillInitial := fs.Bool("backfill-initial", false, "when the cluster starts from nothing, also copy the existing rows of every published table with a primary key into the log")
	tlsCert := fs.String("tls-cert", "", "this node's TLS certificate (PEM); with --tls-key and --tls-ca enables TLS, mutual between nodes")
	tlsKey := fs.String("tls-key", "", "key of --tls-cert (PEM)")
	tlsCA := fs.String("tls-ca", "", "CA certificate that signed every node's certificate (PEM)")
	tokenFile := fs.String("auth-token-file", "", "file with a token that clients must send (or env CURNTVALV_AUTH_TOKEN); the same on every node")
	var sinkFlags repeatFlag
	fs.Var(&sinkFlags, "sink", "deliver changes to a sink, repeatable; give every node the same sinks:\n"+sinkUsage)
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if fs.NArg() > 0 {
		return serverConfig{}, usageErr("unexpected argument %q", fs.Arg(0))
	}
	var warnings []string
	if *configFile != "" {
		var err error
		if warnings, err = applyConfigFile(fs, *configFile, getenv); err != nil {
			return serverConfig{}, err
		}
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		return serverConfig{}, err
	}
	if _, ok := peers[*id]; !ok && !*join {
		return serverConfig{}, usageErr("--id %d is not in --peers (use --join to add a node to a running cluster)", *id)
	}
	if _, ok := peers[*id]; !ok && *listen == "" {
		return serverConfig{}, usageErr("--join for a node not in --peers needs --listen HOST:PORT")
	}
	if *id == 0 {
		return serverConfig{}, usageErr("--id must not be 0")
	}
	if *dataDir == "" {
		return serverConfig{}, usageErr("--data-dir is required")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return serverConfig{}, usageErr("bad --log-level %q", *logLevel)
	}
	ids := make([]uint64, 0, len(peers))
	for p := range peers {
		ids = append(ids, p)
	}
	slices.Sort(ids)
	cfg := serverConfig{
		raft: raft.Config{
			ID:              *id,
			Peers:           ids,
			Addrs:           peers,
			Join:            *join,
			ElectionTicks:   *election,
			HeartbeatTicks:  *heartbeat,
			TickInterval:    *tick,
			SnapshotEntries: *snapEvery,
			PreVote:         *preVote,
		},
		peers:      peers,
		dataDir:    *dataDir,
		listen:     *listen,
		noSync:     *noSync,
		logLevel:   level,
		pg:         pgsource.Config{URL: *pgURL, Slot: *pgSlot, Publication: *pgPub, BackfillInitial: *backfillInitial},
		httpListen: *httpListen,
		tlsCert:    *tlsCert,
		tlsKey:     *tlsKey,
		tlsCA:      *tlsCA,
		token:      getenv("CURNTVALV_AUTH_TOKEN"),
		check:      *check,
		warnings:   warnings,
	}
	if n := countSet(*tlsCert, *tlsKey, *tlsCA); n != 0 && n != 3 {
		return serverConfig{}, usageErr("--tls-cert, --tls-key and --tls-ca go together")
	}
	if *tokenFile != "" {
		if cfg.token, err = security.ReadToken(*tokenFile); err != nil {
			return serverConfig{}, usageErr("--auth-token-file: %v", err)
		}
	}
	if cfg.listen == "" {
		cfg.listen = peers[*id]
	}
	if cfg.retention.MaxAge, err = parseAge(*retAge); err != nil {
		return serverConfig{}, usageErr("--retention-age: %v", err)
	}
	if cfg.retention.MaxBytes, err = parseBytes(*retBytes); err != nil {
		return serverConfig{}, usageErr("--retention-bytes: %v", err)
	}
	if cfg.segmentBytes, err = parseBytes(*segBytes); err != nil || cfg.segmentBytes == 0 {
		return serverConfig{}, usageErr("--wal-segment-bytes: want a size above 0, e.g. 64MB")
	}
	names := map[string]bool{}
	for _, f := range sinkFlags {
		sp, err := parseSinkSpec(f)
		if err != nil {
			return serverConfig{}, err
		}
		if names[sp.name] {
			return serverConfig{}, usageErr("--sink: name %q used twice", sp.name)
		}
		if err := loadHashKey(&sp); err != nil {
			return serverConfig{}, err
		}
		names[sp.name] = true
		cfg.sinks = append(cfg.sinks, sp)
	}
	return cfg, nil
}

// loadTLS reads the node's TLS files; both configs are nil without TLS.
func loadTLS(cfg serverConfig) (srv, peer *tls.Config, err error) {
	if cfg.tlsCert == "" {
		return nil, nil, nil
	}
	if srv, err = security.ServerTLS(cfg.tlsCert, cfg.tlsKey, cfg.tlsCA); err != nil {
		return nil, nil, err
	}
	if peer, err = security.ClientTLS(cfg.tlsCA, cfg.tlsCert, cfg.tlsKey); err != nil {
		return nil, nil, err
	}
	return srv, peer, nil
}

func countSet(vals ...string) int {
	n := 0
	for _, v := range vals {
		if v != "" {
			n++
		}
	}
	return n
}

func serverCmd(ctx context.Context, args []string, stderr io.Writer) error {
	cfg, err := parseServerFlags(args, stderr, os.Getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.logLevel})).With("node", cfg.raft.ID)
	for _, w := range cfg.warnings {
		logger.Warn(w)
	}
	if cfg.check {
		if _, _, err := loadTLS(cfg); err != nil {
			return err
		}
		fmt.Fprintf(stderr, "configuration OK: node %d of %d, data in %s, %d sink(s)\n", cfg.raft.ID, len(cfg.peers), cfg.dataDir, len(cfg.sinks))
		return nil
	}
	return runServer(ctx, cfg, logger)
}

// runServer wires one node together and runs it until ctx is cancelled or a
// component fails.
func runServer(ctx context.Context, cfg serverConfig, logger *slog.Logger) error {
	log, err := wal.Open(filepath.Join(cfg.dataDir, "wal"), wal.Options{SegmentSize: cfg.segmentBytes, NoSync: cfg.noSync})
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer log.Close()
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(cfg.dataDir, "snapshots"), cfg.noSync)
	if err != nil {
		return err
	}
	storage := raft.Storage{
		Log:       log,
		Stable:    raft.NewFileStableStore(filepath.Join(cfg.dataDir, "raft-state"), cfg.noSync),
		Snapshots: snaps,
	}

	meta := fsm.New()
	retCfg := cfg.retention
	for _, sp := range cfg.sinks {
		if !sp.drop {
			retCfg.Block = append(retCfg.Block, sp.name)
		}
	}
	ret := retention.New(retCfg, log, meta)
	raftCfg := cfg.raft
	if retCfg.MaxAge > 0 || retCfg.MaxBytes > 0 {
		raftCfg.Retention = ret
	}
	// Credentials (design D15). Nodes present their certificate to each
	// other; the listener asks for one and Raft streams need it.
	srvTLS, peerTLS, err := loadTLS(cfg)
	if err != nil {
		return err
	}
	if srvTLS == nil && cfg.token != "" {
		logger.Warn("a token without TLS travels in plain text; use it on a trusted network only")
	}
	guard := security.Guard{Token: cfg.token, MutualTLS: srvTLS != nil}
	peerDial := security.DialOptions(peerTLS, cfg.token)

	tr := transport.New(cfg.raft.ID, cfg.peers, transport.Options{Logger: logger, DialOptions: peerDial})
	node, err := raft.NewNode(raftCfg, storage, tr, meta)
	if err != nil {
		return err
	}

	var src *pgsource.Source
	if cfg.pg.URL != "" {
		pgCfg := cfg.pg
		pgCfg.Logger = logger
		if src, err = pgsource.New(pgCfg, node, meta); err != nil {
			return err
		}
	}
	reader := event.NewReader(node, meta)
	var runners []*sink.Runner
	for _, sp := range cfg.sinks {
		s, err := newSink(sp)
		if err != nil {
			return fmt.Errorf("sink %s: %w", sp.name, err)
		}
		if c, ok := s.(io.Closer); ok {
			// After every runner stopped (deferred calls run after wg.Wait).
			defer c.Close()
		}
		rcfg := sinkConfig(sp)
		rcfg.Logger = logger
		r, err := sink.NewRunner(rcfg, s, node, reader, meta)
		if err != nil {
			return err
		}
		runners = append(runners, r)
	}

	gs := grpc.NewServer(append(guard.ServerOptions(srvTLS), grpc.MaxRecvMsgSize(transport.MaxMessageSize))...)
	raftSrv := transport.NewServer(cfg.raft.ID, cfg.peers, node)
	pb.RegisterRaftServiceServer(gs, raftSrv)
	health := nodeHealth{src: src, runners: runners, specs: cfg.sinks, ret: ret, retCfg: retCfg, log: log}
	var resetter server.Source // a nil *Source must stay a nil interface
	if src != nil {
		resetter = src
	}
	admin := server.NewAdmin(node, node, meta, health, resetter, node, cfg.peers)
	subscribe := server.NewSubscribe(reader, node, meta)
	pb.RegisterAdminServiceServer(gs, admin)
	pb.RegisterSubscribeServiceServer(gs, subscribe)

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.listen, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var hs *http.Server
	var hlis net.Listener
	if cfg.httpListen != "" {
		if hlis, err = net.Listen("tcp", cfg.httpListen); err != nil {
			lis.Close()
			return fmt.Errorf("listen on %s: %w", cfg.httpListen, err)
		}
		peers := web.NewGRPCPeers(peerDial...)
		defer peers.Close()
		addrs := make([]string, 0, len(cfg.raft.Peers))
		for _, id := range cfg.raft.Peers {
			addrs = append(addrs, cfg.peers[id])
		}
		ws := web.New(web.Config{Version: appVersion(), Peers: addrs, Self: cfg.peers[cfg.raft.ID], Logger: logger,
			Counters: func(ctx context.Context) web.Counters { return counters(ctx, node, src, subscribe) }},
			admin, peers, reader)
		// Requests end with the node, so open change feeds do not delay
		// the shutdown.
		hs = &http.Server{Handler: security.HTTP(cfg.token, ws.Handler(), "/healthz", "/readyz"), ReadHeaderTimeout: 10 * time.Second,
			BaseContext: func(net.Listener) context.Context { return ctx }}
	}

	var wg sync.WaitGroup
	errc := make(chan error, 5+len(runners))
	if hs != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if srvTLS != nil {
				// Browsers have no client certificate; do not ask for one.
				hs.TLSConfig = &tls.Config{Certificates: srvTLS.Certificates, MinVersion: tls.VersionTLS12}
				err = hs.ServeTLS(hlis, "", "")
			} else {
				err = hs.Serve(hlis)
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("http server: %w", err)
			}
		}()
	}
	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := gs.Serve(lis); err != nil {
			errc <- fmt.Errorf("grpc server: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := tr.Run(ctx); err != nil {
			errc <- fmt.Errorf("transport: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := node.Run(ctx); err != nil {
			errc <- fmt.Errorf("raft: %w", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncMembers(ctx, node, cfg.peers, logger, tr, raftSrv)
	}()
	if src != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.Run(ctx); err != nil {
				errc <- fmt.Errorf("postgres source: %w", err)
			}
		}()
	}
	for _, r := range runners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Run(ctx); err != nil {
				errc <- fmt.Errorf("sink %s: %w", r.Status().Name, err)
			}
		}()
	}
	sinkNames := make([]string, len(cfg.sinks))
	for i, sp := range cfg.sinks {
		sinkNames[i] = sp.name
	}
	logger.Info("curntvalv node started", "version", appVersion(), "listen", cfg.listen, "http", cfg.httpListen, "data_dir", cfg.dataDir,
		"peers", cfg.peers, "postgres_source", src != nil, "sinks", sinkNames,
		"retention_age", retCfg.MaxAge, "retention_bytes", retCfg.MaxBytes, "retention_block", retCfg.Block,
		"tls", srvTLS != nil, "auth_token", cfg.token != "")

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case runErr = <-errc:
		logger.Error("stopping after failure", "err", runErr)
	}
	cancel()
	if hs != nil {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		if hs.Shutdown(sctx) != nil {
			hs.Close()
		}
		scancel()
	}
	// Peer streams never end on their own, so give in-flight admin calls a
	// moment and then close everything.
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		gs.Stop()
	}
	wg.Wait()
	return runErr
}
