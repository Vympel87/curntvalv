package main

import (
	"bytes"
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Vympel87/curntvalv/internal/compat"
)

var (
	flagLine = regexp.MustCompile(`^  -(\S+)(?: (\S+))?$`)
	envName  = regexp.MustCompile(`\bCURNTVALV_[A-Z_]+\b`)
	usageCmd = regexp.MustCompile(`^  curntvalv ((?:[a-z]+ ?)+)`)
)

// TestCLICompat lists the commands, their flags (server flags are also the
// keys of a config file) and the environment variables they read against
// compat/cli.txt (design D34).
func TestCLICompat(t *testing.T) {
	var lines []string
	var top []string
	for _, l := range strings.Split(usage, "\n") {
		m := usageCmd.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		cmd := strings.TrimSpace(m[1])
		lines = append(lines, "command "+cmd)
		if first, _, _ := strings.Cut(cmd, " "); !slices.Contains(top, first) {
			top = append(top, first)
		}
	}
	for _, cmd := range top {
		if cmd == "version" {
			continue
		}
		var out bytes.Buffer
		run(context.Background(), []string{cmd, "-h"}, &out, &out)
		for _, l := range strings.Split(out.String(), "\n") {
			if m := flagLine.FindStringSubmatch(l); m != nil {
				lines = append(lines, strings.TrimSpace("flag "+cmd+" --"+m[1]+" "+m[2]))
			}
			for _, env := range envName.FindAllString(l, -1) {
				lines = append(lines, "env "+env)
			}
		}
	}
	if len(lines) < 50 {
		t.Fatalf("only %d lines", len(lines))
	}
	compat.Check(t, "cli.txt", lines, compat.Updating())
}

var sinkUsageKey = regexp.MustCompile(`(\w+)=([a-z0-9_-]+(?:\|[a-z0-9_-]+)+|[A-Z][A-Za-z+:/@._\[\]-]*|\S)`)

// TestSinkOptionsCompat lists the --sink keys, the fixed values some take,
// and which sink types take a key of their own, against compat/sink.txt.
func TestSinkOptionsCompat(t *testing.T) {
	var lines []string
	for _, m := range sinkUsageKey.FindAllStringSubmatch(sinkUsage, -1) {
		lines = append(lines, "key "+m[1])
		if strings.Contains(m[2], "|") {
			for _, v := range strings.Split(m[2], "|") {
				lines = append(lines, "value "+m[1]+"="+v)
			}
		}
	}
	for _, typ := range []string{"webhook", "clickhouse", "nats", "redis", "meilisearch", "kafka", "s3"} {
		lines = append(lines, "type "+typ)
		for _, k := range sinkKeys(typ) {
			lines = append(lines, "type "+typ+" key "+k)
		}
	}
	compat.Check(t, "sink.txt", lines, compat.Updating())
}
