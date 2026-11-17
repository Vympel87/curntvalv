// Command curntvalv is the single binary for the curntValv server and CLI.
//
//	curntvalv server --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data-dir ./data/1
//	curntvalv status [--addr ...]
//	curntvalv kv put|get|del KEY [VALUE] [--addr ...]
//	curntvalv log [--from N] [--follow] [--addr ...]
//	curntvalv version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/server"
	pgsource "github.com/Vympel87/curntvalv/internal/source/pg"
	"github.com/Vympel87/curntvalv/internal/transport"
	"github.com/Vympel87/curntvalv/internal/wal"
)

var version = "0.0.0-dev"

const usage = `usage:
  curntvalv server --id N --peers ID=HOST:PORT,... --data-dir DIR [flags]
  curntvalv status [--addr HOST:PORT,...]
  curntvalv kv put KEY VALUE [--addr HOST:PORT,...]
  curntvalv kv get KEY [--addr HOST:PORT,...]
  curntvalv kv del KEY [--addr HOST:PORT,...]
  curntvalv log [--from N] [--follow] [--addr HOST:PORT,...]
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
		fmt.Fprintln(stdout, "curntvalv", version)
		return 0
	case "server":
		err = serverCmd(ctx, args[1:], stderr)
	case "status":
		err = statusCmd(ctx, args[1:], stdout, stderr)
	case "kv":
		err = kvCmd(ctx, args[1:], stdout, stderr)
	case "log":
		err = logCmd(ctx, args[1:], stdout, stderr)
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
}

func parseServerFlags(args []string, stderr io.Writer) (serverConfig, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	id := fs.Uint64("id", 0, "id of this node (must appear in --peers)")
	peersFlag := fs.String("peers", "", "all cluster members as ID=HOST:PORT,... (including this node)")
	dataDir := fs.String("data-dir", "", "directory for the log, Raft state and snapshots")
	listen := fs.String("listen", "", "address to listen on (default: this node's address in --peers)")
	tick := fs.Duration("tick", 100*time.Millisecond, "Raft tick length")
	election := fs.Int("election-ticks", 10, "minimum election timeout in ticks")
	heartbeat := fs.Int("heartbeat-ticks", 1, "heartbeat interval in ticks")
	snapEvery := fs.Int("snapshot-entries", 1024, "take a snapshot every N applied entries")
	trailing := fs.Int("trailing-entries", 0, "compact the log, keeping at least N entries behind the last snapshot (0 = keep everything)")
	noSync := fs.Bool("unsafe-no-fsync", false, "skip fsync (tests and demos only: a crash can lose committed data)")
	logLevel := fs.String("log-level", "info", "debug, info, warn or error")
	pgURL := fs.String("pg-url", os.Getenv("CURNTVALV_PG_URL"), "Postgres connection URL to capture changes from (env CURNTVALV_PG_URL; empty = no source)")
	pgSlot := fs.String("pg-slot", "curntvalv", "logical replication slot (created on a new cluster)")
	pgPub := fs.String("pg-publication", "curntvalv", "Postgres publication to read (must exist)")
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if fs.NArg() > 0 {
		return serverConfig{}, usageErr("unexpected argument %q", fs.Arg(0))
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		return serverConfig{}, err
	}
	if _, ok := peers[*id]; !ok {
		return serverConfig{}, usageErr("--id %d is not in --peers", *id)
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
			ElectionTicks:   *election,
			HeartbeatTicks:  *heartbeat,
			TickInterval:    *tick,
			SnapshotEntries: *snapEvery,
			TrailingEntries: *trailing,
		},
		peers:    peers,
		dataDir:  *dataDir,
		listen:   *listen,
		noSync:   *noSync,
		logLevel: level,
		pg:       pgsource.Config{URL: *pgURL, Slot: *pgSlot, Publication: *pgPub},
	}
	if cfg.listen == "" {
		cfg.listen = peers[*id]
	}
	return cfg, nil
}

func serverCmd(ctx context.Context, args []string, stderr io.Writer) error {
	cfg, err := parseServerFlags(args, stderr)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.logLevel})).With("node", cfg.raft.ID)
	return runServer(ctx, cfg, logger)
}

// runServer wires one node together and runs it until ctx is cancelled or a
// component fails.
func runServer(ctx context.Context, cfg serverConfig, logger *slog.Logger) error {
	log, err := wal.Open(filepath.Join(cfg.dataDir, "wal"), wal.Options{NoSync: cfg.noSync})
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
	tr := transport.New(cfg.raft.ID, cfg.peers, transport.Options{Logger: logger})
	node, err := raft.NewNode(cfg.raft, storage, tr, meta)
	if err != nil {
		return err
	}

	gs := grpc.NewServer(grpc.MaxRecvMsgSize(transport.MaxMessageSize))
	pb.RegisterRaftServiceServer(gs, transport.NewServer(cfg.raft.ID, cfg.peers, node))
	pb.RegisterAdminServiceServer(gs, server.NewAdmin(node, node, meta, cfg.peers))

	var src *pgsource.Source
	if cfg.pg.URL != "" {
		pgCfg := cfg.pg
		pgCfg.Logger = logger
		if src, err = pgsource.New(pgCfg, node, meta); err != nil {
			return err
		}
	}

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.listen, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, 4)
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
	if src != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.Run(ctx); err != nil {
				errc <- fmt.Errorf("postgres source: %w", err)
			}
		}()
	}
	logger.Info("curntvalv node started", "version", version, "listen", cfg.listen, "data_dir", cfg.dataDir,
		"peers", cfg.peers, "postgres_source", src != nil)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case runErr = <-errc:
		logger.Error("stopping after failure", "err", runErr)
	}
	cancel()
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
