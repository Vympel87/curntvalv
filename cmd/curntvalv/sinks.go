package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/retention"
	"github.com/Vympel87/curntvalv/internal/server"
	"github.com/Vympel87/curntvalv/internal/sink"
	pgsource "github.com/Vympel87/curntvalv/internal/source/pg"
	"github.com/Vympel87/curntvalv/internal/wal"
	"github.com/Vympel87/curntvalv/internal/web"
)

// sinkSpec is one --sink flag:
//
//	name=NAME,type=webhook,url=URL[,table=T]...[,start=earliest|latest][,policy=block|drop]
//	name=NAME,type=clickhouse,url=URL[,database=DB][,into=TABLE][,table=T]...[,start=...][,policy=...]
//	name=NAME,type=nats,url=nats://HOST:PORT[,subject=PREFIX][,stream=STREAM]...
//	name=NAME,type=redis,url=redis://HOST:PORT[/DB][,stream=KEY][,maxlen=N]...
//	name=NAME,type=meilisearch,url=http://[:KEY@]HOST:PORT[,index=UID]...
//	name=NAME,type=kafka,url=kafka://HOST:PORT[,url=...][,topic=T][,sasl=M]...
type sinkSpec struct {
	name, typ, url    string
	urls              []string // every url (kafka takes several)
	tables            []string
	startLatest       bool
	drop              bool // policy=drop: retention does not wait for this sink
	database, chTable string
	mirror            bool   // clickhouse mode=mirror
	subject, stream   string // nats; stream also redis
	maxLen            int64  // redis
	index             string // meilisearch
	topic, sasl       string // kafka
	tombstones        bool   // kafka
	shape             sink.Shape
	skipPoison        bool   // on_error=skip
	transactions      bool   // deliver whole transactions (design D30)
	maxTx             int    // max_tx
	txID              string // kafka transactional_id
	hashKeyFile       string // read by loadHashKey
}

const sinkUsage = "name=NAME,type=webhook|clickhouse|nats|redis|meilisearch|kafka,url=URL[,table=T]...[,start=earliest|latest][,policy=block|drop]" +
	"[,op=OP]...[,where=EXPR]...[,columns=A+B][,drop=A+B][,mask=A+B][,hash=A+B,hash_key_file=PATH][,rename=OLD:NEW+...][,on_leave=skip|delete][,on_error=retry|skip][,transactions=true][,max_tx=N]" +
	"[,database=DB][,into=TABLE][,mode=changelog|mirror] (clickhouse) [,subject=PREFIX][,stream=NAME] (nats) [,stream=KEY][,maxlen=N] (redis) [,index=UID] (meilisearch)" +
	" [,url=...]...[,topic=T][,sasl=plain|scram-sha-256|scram-sha-512][,tombstones=true][,transactional_id=ID] (kafka)"

// sinkKeys lists the keys only some sink types take.
func sinkKeys(typ string) []string {
	switch typ {
	case "clickhouse":
		return []string{"database", "into", "mode"}
	case "nats":
		return []string{"subject", "stream"}
	case "redis":
		return []string{"stream", "maxlen"}
	case "meilisearch":
		return []string{"index"}
	case "kafka":
		return []string{"topic", "sasl", "tombstones", "transactional_id"}
	}
	return nil
}

func parseSinkSpec(s string) (sinkSpec, error) {
	var sp sinkSpec
	seen := map[string]bool{}
	for _, part := range splitSpec(s) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || v == "" {
			return sinkSpec{}, usageErr("--sink: bad part %q, want KEY=VALUE (%s)", part, sinkUsage)
		}
		if seen[k] && !slices.Contains([]string{"table", "url", "op", "where"}, k) {
			return sinkSpec{}, usageErr("--sink: %s given twice", k)
		}
		seen[k] = true
		switch k {
		case "name":
			sp.name = v
		case "type":
			sp.typ = v
		case "url":
			if sp.url == "" {
				sp.url = v
			}
			sp.urls = append(sp.urls, v)
		case "table":
			sp.tables = append(sp.tables, v)
		case "start":
			if v != "earliest" && v != "latest" {
				return sinkSpec{}, usageErr("--sink: start must be earliest or latest")
			}
			sp.startLatest = v == "latest"
		case "policy":
			if v != "block" && v != "drop" {
				return sinkSpec{}, usageErr("--sink: policy must be block or drop")
			}
			sp.drop = v == "drop"
		case "database":
			sp.database = v
		case "into":
			sp.chTable = v
		case "mode":
			if v != "changelog" && v != "mirror" {
				return sinkSpec{}, usageErr("--sink: mode must be changelog or mirror")
			}
			sp.mirror = v == "mirror"
		case "subject":
			sp.subject = v
		case "stream":
			sp.stream = v
		case "maxlen":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return sinkSpec{}, usageErr("--sink: maxlen must be a number of entries")
			}
			sp.maxLen = n
		case "index":
			sp.index = v
		case "op":
			op, err := sink.ParseOp(v)
			if err != nil {
				return sinkSpec{}, usageErr("--sink: %v", err)
			}
			sp.shape.Ops = append(sp.shape.Ops, op)
		case "where":
			e, err := sink.ParseWhere(v)
			if err != nil {
				return sinkSpec{}, usageErr("--sink: %v", err)
			}
			sp.shape.Where = append(sp.shape.Where, e)
		case "mask":
			names := strings.Split(v, "+")
			if slices.Contains(names, "") {
				return sinkSpec{}, usageErr("--sink: mask wants column names joined by +")
			}
			sp.shape.Mask = names
		case "hash":
			names := strings.Split(v, "+")
			if slices.Contains(names, "") {
				return sinkSpec{}, usageErr("--sink: hash wants column names joined by +")
			}
			sp.shape.Hash = names
		case "hash_key_file":
			sp.hashKeyFile = v
		case "on_error":
			if v != "retry" && v != "skip" {
				return sinkSpec{}, usageErr("--sink: on_error must be retry or skip")
			}
			sp.skipPoison = v == "skip"
		case "transactions":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return sinkSpec{}, usageErr("--sink: transactions must be true or false")
			}
			sp.transactions = b
		case "max_tx":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return sinkSpec{}, usageErr("--sink: max_tx must be a positive number of changes")
			}
			sp.maxTx = n
		case "transactional_id":
			sp.txID = v
		case "on_leave":
			if v != "skip" && v != "delete" {
				return sinkSpec{}, usageErr("--sink: on_leave must be skip or delete")
			}
			sp.shape.LeaveAsDelete = v == "delete"
		case "rename":
			m, err := sink.ParseRename(v)
			if err != nil {
				return sinkSpec{}, usageErr("--sink: %v", err)
			}
			sp.shape.Rename = m
		case "columns", "drop":
			names := strings.Split(v, "+")
			if slices.Contains(names, "") {
				return sinkSpec{}, usageErr("--sink: %s wants column names joined by +", k)
			}
			if k == "columns" {
				sp.shape.Columns = names
			} else {
				sp.shape.Drop = names
			}
		case "topic":
			sp.topic = v
		case "sasl":
			sp.sasl = v
		case "tombstones":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return sinkSpec{}, usageErr("--sink: tombstones must be true or false")
			}
			sp.tombstones = b
		default:
			return sinkSpec{}, usageErr("--sink: unknown key %q (%s)", k, sinkUsage)
		}
	}
	switch {
	case !fsm.ValidSinkName(sp.name):
		return sinkSpec{}, usageErr("--sink: name %q must be lower case letters, digits, _ and - (%s)", sp.name, sinkUsage)
	case !slices.Contains([]string{"webhook", "clickhouse", "nats", "redis", "meilisearch", "kafka"}, sp.typ):
		return sinkSpec{}, usageErr("--sink %s: type must be webhook, clickhouse, nats, redis, meilisearch or kafka", sp.name)
	case sp.url == "":
		return sinkSpec{}, usageErr("--sink %s: url is required", sp.name)
	case len(sp.urls) > 1 && sp.typ != "kafka":
		return sinkSpec{}, usageErr("--sink %s: url given twice (only kafka takes several brokers)", sp.name)
	}
	for _, k := range []string{"database", "into", "mode", "subject", "stream", "maxlen", "index", "topic", "sasl", "tombstones", "transactional_id"} {
		if seen[k] && !slices.Contains(sinkKeys(sp.typ), k) {
			return sinkSpec{}, usageErr("--sink %s: %s is not a %s option", sp.name, k, sp.typ)
		}
	}
	switch {
	case len(sp.shape.Hash) > 0 && sp.hashKeyFile == "":
		return sinkSpec{}, usageErr("--sink %s: hash needs hash_key_file=PATH (a secret of at least %d bytes, the same on every node)", sp.name, sink.MinHashKey)
	case len(sp.shape.Hash) == 0 && sp.hashKeyFile != "":
		return sinkSpec{}, usageErr("--sink %s: hash_key_file without hash", sp.name)
	}
	if (sp.maxTx > 0 || sp.txID != "") && !sp.transactions {
		return sinkSpec{}, usageErr("--sink %s: max_tx and transactional_id need transactions=true", sp.name)
	}
	if sp.transactions && sp.typ == "kafka" && sp.txID == "" {
		sp.txID = "curntvalv-" + sp.name
	}
	if sp.shape.LeaveAsDelete && len(sp.shape.Where) == 0 {
		return sinkSpec{}, usageErr("--sink %s: on_leave needs where", sp.name)
	}
	for _, c := range sp.shape.Hash {
		if slices.Contains(sp.shape.Mask, c) {
			return sinkSpec{}, usageErr("--sink %s: column %s is both masked and hashed", sp.name, c)
		}
	}
	return sp, nil
}

// loadHashKey reads the hash key of a sink spec (design D26).
func loadHashKey(sp *sinkSpec) error {
	if sp.hashKeyFile == "" {
		return nil
	}
	b, err := os.ReadFile(sp.hashKeyFile)
	if err != nil {
		return usageErr("--sink %s: hash_key_file: %v", sp.name, err)
	}
	key := bytes.TrimSpace(b)
	if len(key) < sink.MinHashKey {
		return usageErr("--sink %s: hash key in %s is %d bytes, want at least %d", sp.name, sp.hashKeyFile, len(key), sink.MinHashKey)
	}
	sp.shape.HashKey = key
	return nil
}

// splitSpec splits a --sink value at commas that are not inside
// parentheses or quotes, so a where expression may hold "in (a, b)".
func splitSpec(s string) []string {
	var parts []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')' && depth > 0:
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// newSink builds the sink a spec describes. It does no I/O.
func newSink(sp sinkSpec) (sink.Sink, error) {
	switch sp.typ {
	case "webhook":
		return sink.NewWebhook(sink.WebhookConfig{Name: sp.name, URL: sp.url})
	case "clickhouse":
		return sink.NewClickHouse(sink.ClickHouseConfig{URL: sp.url, Database: sp.database, Table: sp.chTable, Mirror: sp.mirror})
	case "nats":
		return sink.NewNATS(sink.NATSConfig{URL: sp.url, Subject: sp.subject, Stream: sp.stream})
	case "redis":
		return sink.NewRedis(sink.RedisConfig{URL: sp.url, Stream: sp.stream, MaxLen: sp.maxLen})
	case "meilisearch":
		return sink.NewMeilisearch(sink.MeilisearchConfig{URL: sp.url, Index: sp.index})
	case "kafka":
		return sink.NewKafka(sink.KafkaConfig{URLs: sp.urls, Topic: sp.topic, SASL: sp.sasl, Tombstones: sp.tombstones, TransactionalID: sp.txID})
	}
	return nil, fmt.Errorf("unknown sink type %q", sp.typ)
}

func sinkConfig(sp sinkSpec) sink.Config {
	return sink.Config{Name: sp.name, Type: sp.typ, Tables: event.Filter(sp.tables), StartLatest: sp.startLatest, Shape: sp.shape,
		SkipPoison: sp.skipPoison, Transactions: sp.transactions, MaxTxChanges: sp.maxTx}
}

// repeatFlag collects every value of a repeated flag as is.
type repeatFlag []string

func (r *repeatFlag) String() string { return strings.Join(*r, " ") }

func (r *repeatFlag) Set(v string) error {
	*r = append(*r, v)
	return nil
}

// nodeHealth implements server.Health over this node's source, sinks and
// log retention.
type nodeHealth struct {
	src     *pgsource.Source // nil without a Postgres source
	runners []*sink.Runner
	specs   []sinkSpec // same order as runners
	ret     *retention.Policy
	retCfg  retention.Config
	log     *wal.Log
}

func (h nodeHealth) Source() (string, string) {
	if h.src == nil {
		return "", ""
	}
	return h.src.Health()
}

func (h nodeHealth) Sinks() []*pb.SinkStatus {
	out := make([]*pb.SinkStatus, 0, len(h.runners))
	for _, r := range h.runners {
		st := r.Status()
		s := &pb.SinkStatus{Name: st.Name, Type: st.Type, State: string(st.State),
			Delivered: st.Delivered, Failures: st.Failures, LastError: st.LastError, Policy: "block", OnError: "retry"}
		if i := len(out); i < len(h.specs) && h.specs[i].drop {
			s.Policy = "drop"
		}
		if i := len(out); i < len(h.specs) && h.specs[i].skipPoison {
			s.OnError = "skip"
		}
		out = append(out, s)
	}
	return out
}

func (h nodeHealth) Retention() *pb.RetentionStatus {
	if h.ret == nil {
		return nil
	}
	st := h.ret.Status()
	var size int64
	for _, s := range h.log.Segments() {
		size += s.Bytes
	}
	return &pb.RetentionStatus{MaxAgeSeconds: int64(h.retCfg.MaxAge / time.Second), MaxBytes: h.retCfg.MaxBytes,
		LogBytes: size, CompactLimit: st.Limit, BlockedBy: st.BlockedBy}
}

// counters collects the event counters of this node for /metrics.
func counters(ctx context.Context, node *raft.Node, src *pgsource.Source, sub *server.Subscribe) web.Counters {
	var c web.Counters
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if st, err := node.Status(ctx); err == nil {
		rc := st.Counters
		c.Elections, c.BecameLeader, c.SnapshotsTaken, c.SnapshotsInstalled = rc.Elections, rc.BecameLeader, rc.SnapshotsTaken, rc.SnapshotsInstalled
	}
	if src != nil {
		ss := src.Stats()
		c.SourceBatches, c.SourceTransactions, c.SourceEvents = ss.Batches, ss.Transactions, ss.Events
	}
	ss := sub.Stats()
	c.SubscribeStreams, c.SubscribeSent = ss.Active, ss.Sent
	return c
}
