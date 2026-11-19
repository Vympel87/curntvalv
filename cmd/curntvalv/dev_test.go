package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/client"
)

func TestParseDevFlags(t *testing.T) {
	env := map[string]string{"DATABASE_URL": "postgres://app@db/app"}
	getenv := func(k string) string { return env[k] }
	dc, err := parseDevFlags([]string{"--no-open"}, io.Discard, getenv)
	if err != nil {
		t.Fatal(err)
	}
	c := dc.server
	if c.pg.URL != "postgres://app@db/app" || c.listen != "127.0.0.1:7001" || c.httpListen != "127.0.0.1:8080" ||
		c.dataDir != ".curntvalv" || len(c.raft.Peers) != 1 || c.raft.ID != 1 || dc.open {
		t.Fatalf("%+v", dc)
	}
	env["CURNTVALV_PG_URL"] = "postgres://cv@db/cv" // wins over DATABASE_URL
	if dc, _ := parseDevFlags(nil, io.Discard, getenv); dc.server.pg.URL != "postgres://cv@db/cv" || !dc.open {
		t.Fatalf("%+v", dc)
	}
	if _, err := parseDevFlags(nil, io.Discard, func(string) string { return "" }); !errors.Is(err, errUsage) {
		t.Fatalf("no URL: %v", err)
	}
	dc, err = parseDevFlags([]string{"--sink", "name=h,type=webhook,url=http://x"}, io.Discard, getenv)
	if err != nil || len(dc.server.sinks) != 1 {
		t.Fatalf("%+v %v", dc.server.sinks, err)
	}
}

func TestRedactURL(t *testing.T) {
	if got := redactURL("postgres://u:secret@h:5432/db"); strings.Contains(got, "secret") || !strings.Contains(got, "u:") {
		t.Fatal(got)
	}
	if got := redactURL("host=h password=secret"); strings.Contains(got, "secret") {
		t.Fatal(got)
	}
}

// `curntvalv dev` against a real Postgres: it captures changes, serves the
// dashboard and the subscribe API, and stops cleanly.
func TestDevCommand(t *testing.T) {
	f := newPGFixture(t)
	grpcAddr, httpAddr := freePort(t), freePort(t)
	args := append([]string{"--pg-url", f.url, "--data-dir", filepath.Join(t.TempDir(), ".curntvalv"),
		"--listen", grpcAddr, "--http-listen", httpAddr, "--no-open", "--log-level", "warn"}, f.args()...)

	ctx, cancel := context.WithCancel(context.Background())
	var stderr syncBuffer
	done := make(chan error, 1)
	go func() { done <- devCmd(ctx, args, &stderr) }()
	t.Cleanup(func() { // before the fixture drops the slot
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("dev: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("dev did not stop")
		}
	})

	waitE2E(t, "dashboard", 30*time.Second, func() bool {
		resp, err := http.Get("http://" + httpAddr + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	f.waitSlot()
	f.insert(1, 3, 3)

	c, err := client.New(grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	sctx, scancel := context.WithTimeout(ctx, 20*time.Second)
	defer scancel()
	var ids []string
	for ch, err := range c.Subscribe(sctx, client.Options{Tables: []string{f.table}}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, *ch.After["id"])
		if len(ids) == 3 {
			break
		}
	}
	if strings.Join(ids, ",") != "1,2,3" {
		t.Fatalf("got %v", ids)
	}

	resp, err := http.Get("http://" + httpAddr + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var view struct {
		Pipeline struct {
			SourceState string `json:"source_state"`
		} `json:"pipeline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil || view.Pipeline.SourceState != "running" {
		t.Fatalf("status %+v %v", view, err)
	}
	if out := stderr.String(); !strings.Contains(out, "dashboard  http://"+httpAddr) {
		t.Fatalf("banner:\n%s", out)
	}
}

func TestDevCommandExplainsSetup(t *testing.T) {
	f := newPGFixture(t)
	var stderr bytes.Buffer
	err := devCmd(context.Background(), []string{"--pg-url", f.url, "--pg-publication", "missing_pub", "--no-open",
		"--data-dir", t.TempDir()}, &stderr)
	if err == nil || !strings.Contains(err.Error(), "CREATE PUBLICATION missing_pub FOR ALL TABLES") {
		t.Fatalf("got %v", err)
	}
}
