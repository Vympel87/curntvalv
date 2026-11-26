package web

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Vympel87/curntvalv/internal/compat"
)

// TestMetricsCompat lists the metric families with their type and labels
// against compat/metrics.txt (design D34): dashboards and alerts use them.
func TestMetricsCompat(t *testing.T) {
	st := leaderStatus()
	st.Sinks[0].Skipped = 1
	out := string(writeMetrics(st, Counters{Elections: 1, SourceEvents: 1, SubscribeStreams: 1}, "x"))
	types := map[string]string{}
	labels := map[string][]string{}
	label := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="`)
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) == 4 && f[0] == "#" && f[1] == "TYPE":
			types[f[2]] = f[3]
		case len(f) >= 2 && f[0] != "#":
			name, rest, _ := strings.Cut(f[0], "{")
			for _, m := range label.FindAllStringSubmatch(rest, -1) {
				if !slices.Contains(labels[name], m[1]) {
					labels[name] = append(labels[name], m[1])
				}
			}
		}
	}
	var lines []string
	for name, typ := range types {
		l := labels[name]
		slices.Sort(l)
		lines = append(lines, strings.TrimSpace("metric "+name+" "+typ+" "+strings.Join(l, ",")))
	}
	if len(lines) < 20 {
		t.Fatalf("only %d metrics", len(lines))
	}
	compat.Check(t, "metrics.txt", lines, compat.Updating())
}
