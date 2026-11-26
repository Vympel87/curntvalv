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
	// TxChanges is set by a sink delivering whole transactions (design
	// D30): how many changes of this transaction the sink receives, all in
	// one delivery. 0 means unknown.
	TxChanges uint32
}

// ID returns the idempotency key of the change (design D4): the commit LSN
// of its transaction and its position in it, e.g. "0/15185F8:3". It is the
// same however often the change is delivered.
func ID(commitLSN uint64, seq uint32) string {
	return fmt.Sprintf("%s:%d", FormatLSN(commitLSN), seq)
}

// FormatLSN formats an LSN the way Postgres does.
func FormatLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

// ID returns the idempotency key of c. A schema change (design D31) has
// the position of the change it precedes and the suffix ":schema".
func (c Change) ID() string {
	if c.IsSchemaChange() {
		return ID(c.CommitLSN, c.Seq) + ":schema"
	}
	return ID(c.CommitLSN, c.Seq)
}

// IsSchemaChange reports whether c announces a new table definition rather
// than a row change (design D31). Its Schema is the new definition.
func (c Change) IsSchemaChange() bool { return c.Event.GetOp() == pb.Op_OP_SCHEMA }

// WithoutSchemaChanges returns changes without schema changes, reusing its
// array.
func WithoutSchemaChanges(changes []Change) []Change {
	out := changes[:0]
	for _, c := range changes {
		if !c.IsSchemaChange() {
			out = append(out, c)
		}
	}
	return out
}

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
// backfill, source_reset or schema.
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
	// TxChanges is how many changes of the transaction this sink receives,
	// when it delivers whole transactions (design D30).
	TxChanges uint32 `json:"tx_changes,omitempty"`
	// Columns is the new table definition of op schema (design D31).
	Columns []ColumnJSON `json:"columns,omitempty"`
	// Reset is set for op source_reset: an operator moved the source
	// position, so changes between previous_lsn and lsn may be missing or
	// repeated.
	Reset *ResetJSON `json:"reset,omitempty"`
}

// ColumnJSON is a column of a table definition: its name, Postgres type
// (the name for common types, else empty; type_oid always), type modifier
// (-1 without one, e.g. 24 for varchar(20)) and whether it is part of the
// replica identity key.
type ColumnJSON struct {
	Name         string `json:"name"`
	Type         string `json:"type,omitempty"`
	TypeOID      uint32 `json:"type_oid"`
	TypeModifier int32  `json:"type_modifier"`
	Key          bool   `json:"key,omitempty"`
}

// typeNames names common Postgres types by OID.
var typeNames = map[uint32]string{
	16: "boolean", 17: "bytea", 20: "bigint", 21: "smallint", 23: "integer", 25: "text", 114: "json",
	700: "real", 701: "double precision", 1042: "character", 1043: "character varying", 1082: "date",
	1083: "time without time zone", 1114: "timestamp without time zone", 1184: "timestamp with time zone",
	1186: "interval", 1266: "time with time zone", 1700: "numeric", 2950: "uuid", 3802: "jsonb",
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
		TxChanges:  c.TxChanges,
	}
	if r := c.Event.GetSourceReset(); c.IsSourceReset() && r != nil {
		j.Reset = &ResetJSON{
			PreviousLSN: FormatLSN(r.GetPreviousLsn()), PreviousSystemID: r.GetPreviousSystemId(), PreviousTimeline: r.GetPreviousTimeline(),
			LSN: FormatLSN(r.GetLsn()), SystemID: r.GetSystemId(), Timeline: r.GetTimeline(), Reason: r.GetReason(),
		}
	}
	cols := c.Schema.GetColumns()
	if c.IsSchemaChange() {
		j.Columns = make([]ColumnJSON, len(cols))
		for i, col := range cols {
			j.Columns[i] = ColumnJSON{Name: col.GetName(), Type: typeNames[col.GetTypeOid()], TypeOID: col.GetTypeOid(),
				TypeModifier: col.GetTypeModifier(), Key: col.GetKey()}
		}
	}
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
// batch itself or from lookup. Each table definition the batch carries
// (one new to the log) is announced by a schema change before the first
// change that uses it (design D31); readers that did not ask for them drop
// them with WithoutSchemaChanges.
func Changes(index uint64, tb *pb.TxBatch, lookup SchemaLookup, filter Filter) []Change {
	var out []Change
	var announced []uint64 // new schemas announced in this batch
	for i, ev := range tb.GetEvents() {
		var sc *pb.Schema
		if ev.GetOp() != pb.Op_OP_SOURCE_RESET {
			sc = schemaOf(ev.GetSchemaId(), tb, lookup)
			if !filter.Match(sc) {
				continue
			}
		}
		c := Change{
			Index:        index,
			CommitLSN:    tb.GetCommitLsn(),
			Seq:          tb.GetFirstSeq() + uint32(i),
			Xid:          tb.GetXid(),
			CommitTimeUs: tb.GetCommitTimeUs(),
			Event:        ev,
			Schema:       sc,
		}
		// A definition the batch carries is new to the log: announce it
		// before its first change (design D31).
		if id := ev.GetSchemaId(); sc != nil && !slices.Contains(announced, id) && newSchema(tb, id) {
			announced = append(announced, id)
			sch := c
			sch.Event = &pb.Event{SchemaId: id, Op: pb.Op_OP_SCHEMA}
			out = append(out, sch)
		}
		out = append(out, c)
	}
	return out
}

// newSchema reports whether the batch carries schema id, which it does only
// for definitions the cluster had not stored.
func newSchema(tb *pb.TxBatch, id uint64) bool {
	for _, sc := range tb.GetSchemas() {
		if sc.GetId() == id {
			return true
		}
	}
	return false
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
