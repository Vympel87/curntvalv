package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	entries, err := parseConfig(`
# a comment
id = 1
peers=1=a:1,2=b:2
pg-url = "postgres://u:p#w@db/app?x=1"   # quoted keeps the #
log-level = debug # unquoted ends at " #"
sink = name=a,type=webhook,url=http://x/#frag
	sink = name=b,type=webhook,url=http://y
quoted = "tab\there"
empty =
`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, fmt.Sprintf("%d:%s=%s", e.line, e.key, e.value))
	}
	want := []string{
		"3:id=1",
		"4:peers=1=a:1,2=b:2",
		"5:pg-url=postgres://u:p#w@db/app?x=1",
		"6:log-level=debug",
		"7:sink=name=a,type=webhook,url=http://x/#frag",
		"8:sink=name=b,type=webhook,url=http://y",
		"9:quoted=tab\there",
		"10:empty=",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
	for _, bad := range []string{
		"id 1",              // no =
		"= 1",               // no key
		"two words = 1",     // space in key
		`pg-url = "open`,    // unterminated
		`pg-url = "a" junk`, // text after quotes
		"[server]",          // sections are not a thing
	} {
		if _, err := parseConfig("# ok\n" + bad); err == nil || !strings.HasPrefix(err.Error(), "2: ") {
			t.Errorf("%q: got %v, want an error on line 2", bad, err)
		}
	}
}

// writeConfig writes a config file and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "node.conf")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

const baseConfig = `
id = 2
peers = 1=a:1,2=b:2,3=c:3
data-dir = /var/lib/curntvalv
retention-age = 7d
pg-url = postgres://file@db/app
sink = name=a,type=webhook,url=http://x
sink = name=b,type=webhook,url=http://y
`

func TestConfigFileFillsUnsetFlags(t *testing.T) {
	path := writeConfig(t, baseConfig)
	cfg, err := parseServerFlags([]string{"--config", path}, io.Discard, envFrom(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.raft.ID != 2 || len(cfg.peers) != 3 || cfg.listen != "b:2" || cfg.dataDir != "/var/lib/curntvalv" ||
		cfg.retention.MaxAge != 7*24*time.Hour || cfg.pg.URL != "postgres://file@db/app" || len(cfg.sinks) != 2 {
		t.Fatalf("%+v", cfg)
	}
	// The file through CURNTVALV_CONFIG; flags win over it.
	cfg, err = parseServerFlags([]string{"--data-dir", "/flag", "--sink", "name=c,type=webhook,url=http://z"}, io.Discard,
		envFrom(map[string]string{"CURNTVALV_CONFIG": path}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dataDir != "/flag" || cfg.raft.ID != 2 {
		t.Fatalf("flag did not win: %+v", cfg)
	}
	if len(cfg.sinks) != 1 || cfg.sinks[0].name != "c" {
		t.Fatalf("--sink should replace the file's sinks: %+v", cfg.sinks)
	}
}

func TestConfigFileEnvironmentBeatsFile(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	os.WriteFile(tokFile, []byte("from-file\n"), 0o600)
	path := writeConfig(t, baseConfig+"auth-token-file = "+tokFile+"\n")
	env := map[string]string{"CURNTVALV_PG_URL": "postgres://env@db/app", "CURNTVALV_AUTH_TOKEN": "from-env"}
	cfg, err := parseServerFlags([]string{"--config", path}, io.Discard, envFrom(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pg.URL != "postgres://env@db/app" || cfg.token != "from-env" {
		t.Fatalf("pg %q token %q", cfg.pg.URL, cfg.token)
	}
	// Without the environment the file applies.
	cfg, err = parseServerFlags([]string{"--config", path}, io.Discard, envFrom(nil))
	if err != nil || cfg.pg.URL != "postgres://file@db/app" || cfg.token != "from-file" {
		t.Fatalf("pg %q token %q err %v", cfg.pg.URL, cfg.token, err)
	}
	// And a flag beats the environment.
	cfg, err = parseServerFlags([]string{"--config", path, "--pg-url", "postgres://flag@db/app"}, io.Discard, envFrom(env))
	if err != nil || cfg.pg.URL != "postgres://flag@db/app" {
		t.Fatalf("pg %q err %v", cfg.pg.URL, err)
	}
}

func TestConfigFileErrors(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{baseConfig + "colour = blue\n", `:9: unknown key "colour"`},
		{baseConfig + "--id = 1\n", `:9: unknown key "--id"`},
		{baseConfig + "config = other.conf\n", `:9: unknown key "config"`},
		{baseConfig + "id = 3\n", ":9: id already set on line 2"},
		{baseConfig + "election-ticks = many\n", ":9: election-ticks:"},
		{baseConfig + "retention-bytes = lots\n", "--retention-bytes"},
		{baseConfig + "sink = name=a,type=webhook,url=http://z\n", `name "a" used twice`},
		{"id = 1\nbroken line\n", ":2: want KEY = VALUE"},
	} {
		path := writeConfig(t, tc.body)
		_, err := parseServerFlags([]string{"--config", path}, io.Discard, envFrom(nil))
		if !errors.Is(err, errUsage) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: got %v, want %q", tc.want, err, tc.want)
		}
	}
	_, err := parseServerFlags([]string{"--config", filepath.Join(t.TempDir(), "missing.conf")}, io.Discard, envFrom(nil))
	if !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "--config") {
		t.Errorf("missing file: %v", err)
	}
}

func TestConfigFileWarnsAboutReadablePassword(t *testing.T) {
	for _, tc := range []struct {
		url  string
		mode os.FileMode
		warn bool
	}{
		{"postgres://u:secret@db/app", 0o644, true},
		{"postgres://u@db/app?password=secret", 0o644, true},
		{"postgres://u:secret@db/app", 0o600, false},
		{"postgres://u@db/app", 0o644, false},
	} {
		path := writeConfig(t, strings.Replace(baseConfig, "postgres://file@db/app", tc.url, 1))
		if err := os.Chmod(path, tc.mode); err != nil {
			t.Fatal(err)
		}
		cfg, err := parseServerFlags([]string{"--config", path}, io.Discard, envFrom(nil))
		if err != nil || (len(cfg.warnings) == 1) != tc.warn {
			t.Errorf("%s %o: warnings %v err %v", tc.url, tc.mode, cfg.warnings, err)
		}
	}
}

func TestServerCheck(t *testing.T) {
	path := writeConfig(t, baseConfig)
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"server", "--config", path, "--check"}, io.Discard, &stderr); code != 0 ||
		!strings.Contains(stderr.String(), "configuration OK: node 2 of 3") {
		t.Fatalf("code %d: %s", code, stderr.String())
	}
	// --check loads the TLS files too.
	bad := writeConfig(t, baseConfig+"tls-cert = /nope.crt\ntls-key = /nope.key\ntls-ca = /nope-ca.crt\n")
	stderr.Reset()
	if code := run(context.Background(), []string{"server", "--config", bad, "--check"}, io.Discard, &stderr); code != 1 {
		t.Fatalf("code %d: %s", code, stderr.String())
	}
	stderr.Reset()
	if code := run(context.Background(), []string{"server", "--config", writeConfig(t, "id = x\n"), "--check"}, io.Discard, &stderr); code != 2 {
		t.Fatalf("code %d: %s", code, stderr.String())
	}
}

func TestDevIgnoresConfigEnv(t *testing.T) {
	path := writeConfig(t, "colour = blue\n")
	dc, err := parseDevFlags([]string{"--pg-url", "postgres://x/y"}, io.Discard,
		envFrom(map[string]string{"CURNTVALV_CONFIG": path}))
	if err != nil || dc.server.dataDir != ".curntvalv" {
		t.Fatalf("%+v %v", dc, err)
	}
}

// TestEndToEndServerFromConfigFile starts a node with nothing but --config
// and talks to it with the CLI.
func TestEndToEndServerFromConfigFile(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end test skipped in -short mode")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "curntvalv")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	addr, httpAddr := freePort(t), freePort(t)
	path := filepath.Join(dir, "node1.conf")
	os.WriteFile(path, []byte(fmt.Sprintf(`# one node
id = 1
peers = 1=%s
data-dir = %s
tick = 20ms
http-listen = %s
log-level = debug
`, addr, filepath.Join(dir, "data"), httpAddr)), 0o600)
	logs := &syncBuffer{}
	cmd := exec.Command(bin, "server", "--config", path)
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
		if t.Failed() {
			t.Logf("log:\n%s", logs)
		}
	})
	waitE2E(t, "leader", 15*time.Second, func() bool {
		out, _ := exec.Command(bin, "status", "--addr", addr).Output()
		return strings.Contains(string(out), "leader")
	})
	if out, err := exec.Command(bin, "kv", "put", "k", "v", "--addr", addr).CombinedOutput(); err != nil {
		t.Fatalf("kv put: %v %s", err, out)
	}
	if out, _ := exec.Command(bin, "kv", "get", "k", "--addr", addr).Output(); strings.TrimSpace(string(out)) != "v" {
		t.Fatalf("kv get: %q", out)
	}
}
