// Package event turns committed log entries into row changes for the
// consumers of the log: the subscribe API, the sinks and the CLI. It also
// defines the JSON form of a change that all of them share.
package event

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Change is one row change together with its position in the log and in
// the Postgres transaction.
type Change struct {
	// Index is the Raft index of the entry holding the change.
	Index        uint64
	CommitLSN    uint64
	Seq          uint32
	Xid          uint32
	CommitTimeUs int64
	Event        *pb.Event
	// Schema is the table definition, nil if the cluster does not know it
	// (which would be a bug).
	Schema *pb.Schema
}

// ID returns the idempotency key of the change (design D4): the commit LSN
// of its transaction and its position in it, e.g. "0/15185F8:3". It is the
// same however often the change is delivered.
func ID(commitLSN uint64, seq uint32) string {
	return fmt.Sprintf("%s:%d", FormatLSN(commitLSN), seq)
}

// FormatLSN formats an LSN the way Postgres does.
func FormatLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

// ID returns the idempotency key of c.
func (c Change) ID() string { return ID(c.CommitLSN, c.Seq) }

// IsSourceReset reports whether c marks a source reset rather than a row
// change (design D7). Such a change has no table and passes every filter.
func (c Change) IsSourceReset() bool { return c.Event.GetOp() == pb.Op_OP_SOURCE_RESET }

// Table returns "namespace.name", or "schema#ID" if the schema is unknown,
// or "" for a source reset.
func (c Change) Table() string {
	if c.IsSourceReset() {
		return ""
	}
	if c.Schema == nil {
		return fmt.Sprintf("schema#%d", c.Event.GetSchemaId())
	}
	return c.Schema.GetNamespace() + "." + c.Schema.GetName()
}

// Op returns the operation in lower case: insert, update, delete, truncate,
// or source_reset.
func (c Change) Op() string {
	return strings.ToLower(strings.TrimPrefix(c.Event.GetOp().String(), "OP_"))
}

// JSON is the JSON form of a change, shared by the CLI, the webhook sink and
// the ClickHouse sink. Values are Postgres text (binary ones base64), NULL is
// null, and unchanged TOAST columns are listed in unchanged instead.
type JSON struct {
	ID         string         `json:"id"`
	Index      uint64         `json:"index"`
	LSN        string         `json:"lsn"`
	Seq        uint32         `json:"seq"`
	Xid        uint32         `json:"xid"`
	CommitTime string         `json:"commit_time"`
	Table      string         `json:"table"`
	Op         string         `json:"op"`
	Before     map[string]any `json:"before,omitempty"`
	After      map[string]any `json:"after,omitempty"`
	Unchanged  []string       `json:"unchanged,omitempty"`
	// Reset is set for op source_reset: an operator moved the source
	// position, so changes between previous_lsn and lsn may be missing or
	// repeated.
	Reset *ResetJSON `json:"reset,omitempty"`
}

// ResetJSON is the JSON form of a source reset. System identifiers are
// strings: they do not fit a JavaScript number.
type ResetJSON struct {
	PreviousLSN      string `json:"previous_lsn"`
	PreviousSystemID uint64 `json:"previous_system_id,string"`
	PreviousTimeline uint32 `json:"previous_timeline"`
	LSN              string `json:"lsn"`
	SystemID         uint64 `json:"system_id,string"`
	Timeline         uint32 `json:"timeline"`
	Reason           string `json:"reason,omitempty"`
}

// CommitTime returns the commit time of the transaction.
func (c Change) CommitTime() time.Time { return time.UnixMicro(c.CommitTimeUs).UTC() }

// JSON returns the JSON form of c.
func (c Change) JSON() JSON {
	j := JSON{
		ID:         c.ID(),
		Index:      c.Index,
		LSN:        FormatLSN(c.CommitLSN),
		Seq:        c.Seq,
		Xid:        c.Xid,
		CommitTime: c.CommitTime().Format(time.RFC3339Nano),
		Table:      c.Table(),
		Op:         c.Op(),
	}
	if r := c.Event.GetSourceReset(); c.IsSourceReset() && r != nil {
		j.Reset = &ResetJSON{
			PreviousLSN: FormatLSN(r.GetPreviousLsn()), PreviousSystemID: r.GetPreviousSystemId(), PreviousTimeline: r.GetPreviousTimeline(),
			LSN: FormatLSN(r.GetLsn()), SystemID: r.GetSystemId(), Timeline: r.GetTimeline(), Reason: r.GetReason(),
		}
	}
	cols := c.Schema.GetColumns()
	row := func(r *pb.Row) map[string]any {
		if r == nil {
			return nil
		}
		m := map[string]any{}
		for i, v := range r.GetValues() {
			name := fmt.Sprintf("col%d", i)
			if i < len(cols) {
				name = cols[i].GetName()
			}
			switch v.GetKind() {
			case pb.ValueKind_VALUE_KIND_NULL:
				m[name] = nil
			case pb.ValueKind_VALUE_KIND_TEXT:
				m[name] = string(v.GetData())
			case pb.ValueKind_VALUE_KIND_BINARY:
				m[name] = base64.StdEncoding.EncodeToString(v.GetData())
			case pb.ValueKind_VALUE_KIND_UNCHANGED:
				if !slices.Contains(j.Unchanged, name) {
					j.Unchanged = append(j.Unchanged, name)
				}
			}
		}
		return m
	}
	j.Before, j.After = row(c.Event.GetBefore()), row(c.Event.GetAfter())
	return j
}

// SchemaLookup finds a stored schema by id. (*fsm.FSM).Schema is one.
type SchemaLookup func(id uint64) (*pb.Schema, bool)

// Filter selects tables. An entry "orders" matches a table of that name in
// any namespace, "public.orders" only that one. An empty filter matches
// everything.
type Filter []string

// Match reports whether a change to the table of sc passes the filter.
func (f Filter) Match(sc *pb.Schema) bool {
	if len(f) == 0 {
		return true
	}
	if sc == nil {
		return false
	}
	for _, t := range f {
		ns, name, qualified := strings.Cut(t, ".")
		if !qualified && t == sc.GetName() {
			return true
		}
		if qualified && ns == sc.GetNamespace() && name == sc.GetName() {
			return true
		}
	}
	return false
}

// Changes returns the changes of one transaction batch stored at index that
// pass filter; a source reset passes any filter. Schemas are taken from the
// batch itself or from lookup.
func Changes(index uint64, tb *pb.TxBatch, lookup SchemaLookup, filter Filter) []Change {
	var out []Change
	for i, ev := range tb.GetEvents() {
		var sc *pb.Schema
		if ev.GetOp() != pb.Op_OP_SOURCE_RESET {
			sc = schemaOf(ev.GetSchemaId(), tb, lookup)
			if !filter.Match(sc) {
				continue
			}
		}
		out = append(out, Change{
			Index:        index,
			CommitLSN:    tb.GetCommitLsn(),
			Seq:          tb.GetFirstSeq() + uint32(i),
			Xid:          tb.GetXid(),
			CommitTimeUs: tb.GetCommitTimeUs(),
			Event:        ev,
			Schema:       sc,
		})
	}
	return out
}

func schemaOf(id uint64, tb *pb.TxBatch, lookup SchemaLookup) *pb.Schema {
	for _, sc := range tb.GetSchemas() {
		if sc.GetId() == id {
			return sc
		}
	}
	if lookup != nil {
		if sc, ok := lookup(id); ok {
			return sc
		}
	}
	return nil
}
