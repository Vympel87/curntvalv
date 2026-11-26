package web

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Counters are this node's event counters that are not part of the admin
// status.
type Counters struct {
	Elections, BecameLeader                         uint64 // Raft
	SnapshotsTaken, SnapshotsInstalled              uint64
	SourceBatches, SourceTransactions, SourceEvents uint64 // Postgres source
	SubscribeStreams                                int64  // open subscribe streams
	SubscribeSent                                   uint64 // changes sent to subscribers
}

// metrics writes the Prometheus text exposition format (version 0.0.4).
// Families are written in the order they are first used; labels in the
// order given.
type metrics struct {
	b    bytes.Buffer
	seen map[string]bool
}

type label struct{ name, value string }

func (m *metrics) family(name, typ, help string) {
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if m.seen[name] {
		return
	}
	m.seen[name] = true
	fmt.Fprintf(&m.b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (m *metrics) sample(name string, v float64, labels ...label) {
	m.b.WriteString(name)
	if len(labels) > 0 {
		m.b.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				m.b.WriteByte(',')
			}
			m.b.WriteString(l.name)
			m.b.WriteString(`="`)
			m.b.WriteString(escapeLabel(l.value))
			m.b.WriteByte('"')
		}
		m.b.WriteByte('}')
	}
	m.b.WriteByte(' ')
	m.b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	m.b.WriteByte('\n')
}

func (m *metrics) gauge(name, help string, v float64, labels ...label) {
	m.family(name, "gauge", help)
	m.sample(name, v, labels...)
}

func (m *metrics) counter(name, help string, v float64, labels ...label) {
	m.family(name, "counter", help)
	m.sample(name, v, labels...)
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeLabel(s string) string { return labelEscaper.Replace(s) }

func boolf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

var (
	raftStates      = []string{"follower", "candidate", "leader"}
	componentStates = []string{"standby", "running", "retrying", "failed"}
	backfillStates  = []string{"pending", "running", "done", "cancelled", "failed"}
)

// writeMetrics renders all metrics of one node.
func writeMetrics(st *pb.StatusResponse, c Counters, version string) []byte {
	var m metrics
	node := strconv.FormatUint(st.GetId(), 10)
	m.gauge("curntvalv_info", "Build and node information.", 1, label{"version", version}, label{"node", node})

	m.gauge("curntvalv_raft_term", "Current Raft term.", float64(st.GetTerm()))
	for _, s := range raftStates {
		m.gauge("curntvalv_raft_state", "Raft role of this node (1 for the current one).", boolf(st.GetState() == s), label{"state", s})
	}
	m.gauge("curntvalv_raft_leader_known", "1 if this node knows a leader.", boolf(st.GetLeader() != 0))
	m.gauge("curntvalv_raft_first_index", "Oldest log index still stored on this node.", float64(st.GetFirstIndex()))
	m.gauge("curntvalv_raft_last_index", "Newest log index on this node.", float64(st.GetLastIndex()))
	m.gauge("curntvalv_raft_commit_index", "Highest log index known to be committed.", float64(st.GetCommitIndex()))
	m.gauge("curntvalv_raft_applied_index", "Highest log index applied to the state machine.", float64(st.GetLastApplied()))
	m.gauge("curntvalv_raft_snapshot_index", "Index of the latest snapshot.", float64(st.GetSnapshotIndex()))
	m.counter("curntvalv_raft_elections_total", "Elections started by this node.", float64(c.Elections))
	m.counter("curntvalv_raft_became_leader_total", "Elections won by this node.", float64(c.BecameLeader))
	m.counter("curntvalv_raft_snapshots_taken_total", "Snapshots taken by this node.", float64(c.SnapshotsTaken))
	m.counter("curntvalv_raft_snapshots_installed_total", "Snapshots received from a leader.", float64(c.SnapshotsInstalled))

	if r := st.GetRetention(); r != nil {
		m.gauge("curntvalv_log_bytes", "Size of the log on this node.", float64(r.GetLogBytes()))
		m.gauge("curntvalv_retention_max_age_seconds", "Configured retention age (0: none).", float64(r.GetMaxAgeSeconds()))
		m.gauge("curntvalv_retention_max_bytes", "Configured retention size (0: none).", float64(r.GetMaxBytes()))
		m.gauge("curntvalv_retention_compact_limit", "Highest log index that may be removed now.", float64(r.GetCompactLimit()))
		if r.GetBlockedBy() != "" {
			m.gauge("curntvalv_retention_blocked", "1 for the block sink holding the log back.", 1, label{"sink", r.GetBlockedBy()})
		}
	}

	if st.GetSourceState() != "" {
		for _, s := range componentStates {
			m.gauge("curntvalv_source_state", "State of the Postgres source on this node (1 for the current one).",
				boolf(st.GetSourceState() == s), label{"state", s})
		}
		m.counter("curntvalv_source_batches_total", "Log entries proposed by the source on this node.", float64(c.SourceBatches))
		m.counter("curntvalv_source_transactions_total", "Transactions with changes proposed on this node.", float64(c.SourceTransactions))
		m.counter("curntvalv_source_events_total", "Row changes proposed on this node.", float64(c.SourceEvents))
	}

	sinks := slices.Clone(st.GetSinks())
	slices.SortFunc(sinks, func(a, b *pb.SinkStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	for _, s := range sinks {
		l := []label{{"sink", s.GetName()}, {"type", s.GetType()}}
		m.gauge("curntvalv_sink_offset", "Last log index the sink has delivered (replicated).", float64(s.GetOffset()), l...)
		m.gauge("curntvalv_sink_lag_entries", "Log entries between the offset and the newest entry with changes.", float64(s.GetLag()), l...)
		m.gauge("curntvalv_sink_generation", "Number of resets of the sink (replicated).", float64(s.GetGeneration()), l...)
		m.counter("curntvalv_sink_delivered_total", "Changes delivered by this node.", float64(s.GetDelivered()), l...)
		m.counter("curntvalv_sink_failures_total", "Failed deliveries on this node.", float64(s.GetFailures()), l...)
		for _, state := range componentStates {
			m.gauge("curntvalv_sink_state", "State of the sink on this node (1 for the current one).",
				boolf(s.GetState() == state), append(l, label{"state", state})...)
		}
	}

	for _, bf := range backfills(st) {
		l := label{"table", bf.Table}
		m.gauge("curntvalv_backfill_rows", "Rows stored by the table's backfill (replicated).", float64(bf.Rows), l)
		for _, state := range backfillStates {
			m.gauge("curntvalv_backfill_state", "State of the table's backfill (1 for the current one).", boolf(bf.State == state), l, label{"state", state})
		}
	}

	for _, s := range st.GetSubscriptions() {
		l := label{"subscription", s.GetName()}
		m.gauge("curntvalv_subscription_offset", "Last log index the named subscription has processed (replicated).", float64(s.GetOffset()), l)
		m.gauge("curntvalv_subscription_lag_entries", "Log entries between a named subscription's offset and the newest entry with changes.", float64(s.GetLag()), l)
	}

	m.gauge("curntvalv_subscribe_streams", "Open subscribe streams on this node.", float64(c.SubscribeStreams))
	m.counter("curntvalv_subscribe_events_sent_total", "Changes sent to subscribers by this node.", float64(c.SubscribeSent))

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.gauge("go_goroutines", "Number of goroutines.", float64(runtime.NumGoroutine()))
	m.gauge("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", float64(ms.HeapAlloc))
	m.gauge("go_memstats_sys_bytes", "Bytes of memory obtained from the OS.", float64(ms.Sys))
	m.counter("go_gc_cycles_total", "Completed GC cycles.", float64(ms.NumGC))
	return m.b.Bytes()
}
