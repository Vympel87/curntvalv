package main

import (
	"fmt"
	"strings"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/sink"
	pgsource "github.com/Vympel87/curntvalv/internal/source/pg"
)

// sinkSpec is one --sink flag:
//
//	name=NAME,type=webhook,url=URL[,table=T]...[,start=earliest|latest]
//	name=NAME,type=clickhouse,url=URL[,database=DB][,into=TABLE][,table=T]...[,start=...]
type sinkSpec struct {
	name, typ, url    string
	tables            []string
	startLatest       bool
	database, chTable string
}

const sinkUsage = "name=NAME,type=webhook|clickhouse,url=URL[,table=T]...[,start=earliest|latest][,database=DB][,into=TABLE]"

func parseSinkSpec(s string) (sinkSpec, error) {
	var sp sinkSpec
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || v == "" {
			return sinkSpec{}, usageErr("--sink: bad part %q, want KEY=VALUE (%s)", part, sinkUsage)
		}
		if seen[k] && k != "table" {
			return sinkSpec{}, usageErr("--sink: %s given twice", k)
		}
		seen[k] = true
		switch k {
		case "name":
			sp.name = v
		case "type":
			sp.typ = v
		case "url":
			sp.url = v
		case "table":
			sp.tables = append(sp.tables, v)
		case "start":
			if v != "earliest" && v != "latest" {
				return sinkSpec{}, usageErr("--sink: start must be earliest or latest")
			}
			sp.startLatest = v == "latest"
		case "database":
			sp.database = v
		case "into":
			sp.chTable = v
		default:
			return sinkSpec{}, usageErr("--sink: unknown key %q (%s)", k, sinkUsage)
		}
	}
	switch {
	case !fsm.ValidSinkName(sp.name):
		return sinkSpec{}, usageErr("--sink: name %q must be lower case letters, digits, _ and - (%s)", sp.name, sinkUsage)
	case sp.typ != "webhook" && sp.typ != "clickhouse":
		return sinkSpec{}, usageErr("--sink %s: type must be webhook or clickhouse", sp.name)
	case sp.url == "":
		return sinkSpec{}, usageErr("--sink %s: url is required", sp.name)
	case sp.typ != "clickhouse" && (sp.database != "" || sp.chTable != ""):
		return sinkSpec{}, usageErr("--sink %s: database and into are for clickhouse sinks", sp.name)
	}
	return sp, nil
}

// newSink builds the sink a spec describes. It does no I/O.
func newSink(sp sinkSpec) (sink.Sink, error) {
	switch sp.typ {
	case "webhook":
		return sink.NewWebhook(sink.WebhookConfig{Name: sp.name, URL: sp.url})
	case "clickhouse":
		return sink.NewClickHouse(sink.ClickHouseConfig{URL: sp.url, Database: sp.database, Table: sp.chTable})
	}
	return nil, fmt.Errorf("unknown sink type %q", sp.typ)
}

func sinkConfig(sp sinkSpec) sink.Config {
	return sink.Config{Name: sp.name, Type: sp.typ, Tables: event.Filter(sp.tables), StartLatest: sp.startLatest}
}

// repeatFlag collects every value of a repeated flag as is.
type repeatFlag []string

func (r *repeatFlag) String() string { return strings.Join(*r, " ") }

func (r *repeatFlag) Set(v string) error {
	*r = append(*r, v)
	return nil
}

// nodeHealth implements server.Health over this node's source and sinks.
type nodeHealth struct {
	src     *pgsource.Source // nil without a Postgres source
	runners []*sink.Runner
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
		out = append(out, &pb.SinkStatus{Name: st.Name, Type: st.Type, State: string(st.State),
			Delivered: st.Delivered, LastError: st.LastError})
	}
	return out
}
