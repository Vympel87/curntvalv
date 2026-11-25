package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	pgsource "github.com/Vympel87/curntvalv/internal/source/pg"
)

// devConfig is what `curntvalv dev` adds to a one-node server.
type devConfig struct {
	server serverConfig
	open   bool // open the dashboard in a browser
}

// parseDevFlags builds a one-node server configuration for local
// development.
func parseDevFlags(args []string, stderr io.Writer, getenv func(string) string) (devConfig, error) {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	fs.SetOutput(stderr)
	defURL := getenv("CURNTVALV_PG_URL")
	if defURL == "" {
		defURL = getenv("DATABASE_URL")
	}
	pgURL := fs.String("pg-url", defURL, "Postgres connection URL (env CURNTVALV_PG_URL or DATABASE_URL)")
	pgSlot := fs.String("pg-slot", "curntvalv", "logical replication slot (created on first start)")
	pgPub := fs.String("pg-publication", "curntvalv", "Postgres publication to read (must exist)")
	dataDir := fs.String("data-dir", ".curntvalv", "directory for the log")
	listen := fs.String("listen", "127.0.0.1:7001", "gRPC address for status, subscribe and the client package")
	httpListen := fs.String("http-listen", "127.0.0.1:8080", "address of the dashboard")
	noOpen := fs.Bool("no-open", false, "do not open the dashboard in a browser")
	logLevel := fs.String("log-level", "info", "debug, info, warn or error")
	var sinkFlags repeatFlag
	fs.Var(&sinkFlags, "sink", "deliver changes to a sink, repeatable:\n"+sinkUsage)
	if err := fs.Parse(args); err != nil {
		return devConfig{}, err
	}
	if fs.NArg() > 0 {
		return devConfig{}, usageErr("unexpected argument %q", fs.Arg(0))
	}
	if *pgURL == "" {
		return devConfig{}, usageErr("no Postgres URL: pass --pg-url or set DATABASE_URL")
	}
	if *httpListen == "" {
		return devConfig{}, usageErr("--http-listen is empty")
	}
	// The same checks and defaults as a server; one node needs no election
	// wait to speak of.
	sargs := []string{"--id", "1", "--peers", "1=" + *listen, "--data-dir", *dataDir, "--http-listen", *httpListen,
		"--pg-url", *pgURL, "--pg-slot", *pgSlot, "--pg-publication", *pgPub, "--log-level", *logLevel}
	for _, s := range sinkFlags {
		sargs = append(sargs, "--sink", s)
	}
	// dev takes no config file: CURNTVALV_CONFIG is meant for servers.
	noConfig := func(k string) string {
		if k == "CURNTVALV_CONFIG" {
			return ""
		}
		return getenv(k)
	}
	cfg, err := parseServerFlags(sargs, stderr, noConfig)
	if err != nil {
		return devConfig{}, err
	}
	return devConfig{server: cfg, open: !*noOpen}, nil
}

func devCmd(ctx context.Context, args []string, stderr io.Writer) error {
	dc, err := parseDevFlags(args, stderr, os.Getenv)
	if err != nil {
		return err
	}
	cfg := dc.server
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = pgsource.Check(cctx, cfg.pg.URL, cfg.pg.Publication)
	cancel()
	if err != nil {
		if errors.Is(err, pgsource.ErrConfig) {
			return fmt.Errorf("%w\n\nPostgres needs logical replication and a publication, e.g.:\n"+
				"  ALTER SYSTEM SET wal_level = logical;   -- then restart Postgres\n"+
				"  CREATE PUBLICATION %s FOR ALL TABLES;", err, cfg.pg.Publication)
		}
		return err
	}

	dashboard := "http://" + cfg.httpListen
	fmt.Fprintf(stderr, "curntvalv dev %s\n", appVersion())
	fmt.Fprintf(stderr, "  postgres   %s (publication %s, slot %s)\n", redactURL(cfg.pg.URL), cfg.pg.Publication, cfg.pg.Slot)
	fmt.Fprintf(stderr, "  dashboard  %s\n", dashboard)
	fmt.Fprintf(stderr, "  grpc       %s (curntvalv status, curntvalv subscribe, client.New)\n", cfg.listen)
	fmt.Fprintf(stderr, "  data       %s (add it to .gitignore)\n\n", cfg.dataDir)

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.logLevel}))
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	if dc.open {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if waitHealthy(ctx, dashboard+"/healthz") {
				if err := openBrowser(ctx, dashboard); err != nil {
					logger.Warn("could not open a browser; open the dashboard yourself", "url", dashboard, "err", err)
				}
			}
		}()
	}
	err = runServer(ctx, cfg, logger)
	stop()
	wg.Wait()
	return err
}

// waitHealthy polls u until it answers 200, for up to 30s.
func waitHealthy(ctx context.Context, u string) bool {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func openBrowser(ctx context.Context, u string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.CommandContext(ctx, "open", u)
	case "windows":
		cmd = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.CommandContext(ctx, "xdg-open", u)
	}
	return cmd.Run()
}

// redactURL hides the password of a URL; other connection strings are not
// printed.
func redactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" {
		return "(connection string)"
	}
	return u.Redacted()
}
