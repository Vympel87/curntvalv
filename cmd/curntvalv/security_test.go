package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/security"
)

// A cluster with TLS and a token: nodes talk mutual TLS, clients need the
// CA and the token, leader failover still works, and the dashboard is
// served over HTTPS behind the token.
func TestEndToEndTLSAndToken(t *testing.T) {
	certs := t.TempDir()
	var out bytes.Buffer
	if err := certsCmd([]string{"--out", certs, "127.0.0.1,localhost"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "127.0.0.1.crt") {
		t.Fatalf("certs output: %s", out.String())
	}
	p := func(n string) string { return filepath.Join(certs, n) }
	tokenFile := p("token")
	if err := os.WriteFile(tokenFile, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newE2EWith(t, nil, "--tls-cert", p("127.0.0.1.crt"), "--tls-key", p("127.0.0.1.key"), "--tls-ca", p("ca.crt"),
		"--auth-token-file", tokenFile)
	c.cliEnv = []string{"CURNTVALV_TLS_CA=" + p("ca.crt"), "CURNTVALV_TOKEN=s3cret"}

	c.mustCLI("kv", "put", "a", "1")
	old := c.leader()
	c.kill(old)
	c.mustCLI("kv", "put", "b", "2")
	if got := strings.TrimSpace(c.mustCLI("kv", "get", "a")); got != "1" {
		t.Fatalf("get a = %q", got)
	}
	c.start(old)
	waitE2E(t, "restarted node back", 15*time.Second, func() bool {
		return strings.Count(c.mustCLI("status"), "unreachable") == 0
	})

	// Without the token, or without TLS, clients are refused.
	for name, env := range map[string][]string{
		"no token":  {"CURNTVALV_TLS_CA=" + p("ca.crt")},
		"no TLS":    {"CURNTVALV_TOKEN=s3cret"},
		"bad token": {"CURNTVALV_TLS_CA=" + p("ca.crt"), "CURNTVALV_TOKEN=guess"},
	} {
		cmd := exec.Command(c.bin, "kv", "get", "a", "--addr", strings.Join(c.addrs, ","), "--timeout", "2s")
		cmd.Env = append(os.Environ(), env...)
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Errorf("%s: kv get succeeded: %s", name, out)
		}
	}

	// HTTPS: health checks are open, the rest needs the token.
	pool, err := security.ClientTLS(p("ca.crt"), "", "")
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: pool}}
	get := func(path string, auth bool) (int, string) {
		req, _ := http.NewRequest("GET", "https://"+c.https[0]+path, nil)
		if auth {
			req.SetBasicAuth("me", "s3cret")
		}
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get("/readyz", false); code != 200 {
		t.Fatalf("readyz: %d", code)
	}
	if code, _ := get("/api/status", false); code != 401 {
		t.Fatalf("api/status without token: %d", code)
	}
	code, body := get("/api/status", true)
	var view struct {
		Nodes []struct {
			Addr  string `json:"addr"`
			State string `json:"state"`
			Error string `json:"error"`
		} `json:"nodes"`
	}
	if code != 200 || json.Unmarshal([]byte(body), &view) != nil {
		t.Fatalf("api/status: %d %s", code, body)
	}
	leaders := 0
	for _, n := range view.Nodes {
		if n.State == "leader" {
			leaders++
		}
		if n.Error != "" {
			t.Fatalf("node %s unreachable from the dashboard: %s", n.Addr, n.Error)
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders %d: %s", leaders, body)
	}
	// Plain HTTP on the HTTPS port fails.
	if resp, err := http.Get("http://" + c.https[0] + "/readyz"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Fatal("plain HTTP answered")
		}
	}
}
