package sink

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// ClickHouse mirror tables (design D24): with Mirror, each Postgres table
// gets a typed ClickHouse table holding its current rows,
//
//	<columns>, _version UInt64, _deleted UInt8
//	ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY (<key columns>)
//
// A change inserts the row with _version from its position in the log, so a
// redelivered change replaces itself; a delete inserts the key with
// _deleted = 1. Query with FINAL to see each row once and no deleted ones.

// More Postgres type OIDs mapped to ClickHouse types.
const (
	oidUUID        = 2950
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
)

// mirrorTable is a mirror table for one schema.
type mirrorTable struct {
	name string // `db`.`table`
	cols []*pb.Column
	keys []int // indexes of the key columns
}

// chType returns the ClickHouse type of a Postgres column.
func chType(c *pb.Column) string {
	switch c.GetTypeOid() {
	case oidBool:
		return "Bool"
	case oidInt2:
		return "Int16"
	case oidInt4:
		return "Int32"
	case oidInt8:
		return "Int64"
	case oidFloat4:
		return "Float32"
	case oidFloat8:
		return "Float64"
	case oidNumeric:
		// typmod = ((precision << 16) | scale) + 4; -1 without one, which
		// stays exact as String.
		if m := c.GetTypeModifier(); m >= 4 {
			p, s := (m-4)>>16, (m-4)&0xffff
			if p >= 1 && p <= 76 && s <= p {
				return fmt.Sprintf("Decimal(%d, %d)", p, s)
			}
		}
	case oidUUID:
		return "UUID"
	case oidDate:
		return "Date32"
	case oidTimestamp, oidTimestampTZ:
		return "DateTime64(6, 'UTC')"
	}
	return "String"
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func (c *ClickHouse) mirrorName(s *pb.Schema) string {
	r := strings.NewReplacer("{namespace}", s.GetNamespace(), "{table}", s.GetName())
	return quoteIdent(c.database) + "." + quoteIdent(r.Replace(c.template))
}

// mirrorFor creates or extends the mirror table of a schema, once per
// schema.
func (c *ClickHouse) mirrorFor(ctx context.Context, s *pb.Schema) (*mirrorTable, error) {
	c.mu.Lock()
	t, ok := c.mirrors[s.GetId()]
	c.mu.Unlock()
	if ok {
		return t, nil
	}
	t = &mirrorTable{name: c.mirrorName(s), cols: s.GetColumns()}
	var defs, keys, adds []string
	for i, col := range t.cols {
		typ := chType(col)
		if col.GetKey() {
			t.keys = append(t.keys, i)
			keys = append(keys, quoteIdent(col.GetName()))
		} else {
			typ = "Nullable(" + typ + ")"
			adds = append(adds, "ADD COLUMN IF NOT EXISTS "+quoteIdent(col.GetName())+" "+typ)
		}
		defs = append(defs, quoteIdent(col.GetName())+" "+typ)
	}
	if len(t.keys) == 0 {
		return nil, fmt.Errorf("clickhouse: table %s.%s has no key columns (no primary key or REPLICA IDENTITY NOTHING); it cannot be mirrored",
			s.GetNamespace(), s.GetName())
	}
	create := "CREATE TABLE IF NOT EXISTS " + t.name + " (" + strings.Join(defs, ", ") +
		", _version UInt64, _deleted UInt8) ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY (" + strings.Join(keys, ", ") + ")"
	if _, err := c.exec(ctx, create, nil); err != nil {
		return nil, err
	}
	// Columns added in Postgres since the table was created.
	if len(adds) > 0 {
		if _, err := c.exec(ctx, "ALTER TABLE "+t.name+" "+strings.Join(adds, ", "), nil); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	c.mirrors[s.GetId()] = t
	c.mu.Unlock()
	return t, nil
}

// mirrorRow is one row to insert; unchanged lists columns whose value
// Postgres did not send (unchanged TOAST).
type mirrorRow struct {
	key       string
	values    map[string]any
	unchanged map[string]bool
}

// version orders the changes of a row: the log index, then the position in
// the transaction.
func version(c event.Change) uint64 { return c.Index<<24 | uint64(c.Seq&0xffffff) }

// mirrorValue converts a Postgres text value for a column of chType(col).
// Values ClickHouse cannot hold (infinity, NaN, BC dates) become NULL.
func mirrorValue(col *pb.Column, v *pb.Value) (any, bool) {
	switch v.GetKind() {
	case pb.ValueKind_VALUE_KIND_NULL:
		return nil, true
	case pb.ValueKind_VALUE_KIND_UNCHANGED:
		return nil, false
	case pb.ValueKind_VALUE_KIND_BINARY:
		return base64.StdEncoding.EncodeToString(v.GetData()), true
	}
	text := string(v.GetData())
	switch col.GetTypeOid() {
	case oidBool:
		return text == "t", true
	case oidInt2, oidInt4, oidInt8:
		return json.Number(text), true
	case oidFloat4, oidFloat8:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, true
		}
		return json.Number(text), true
	case oidNumeric:
		if chType(col) != "String" && !numberText(text) {
			return nil, true // NaN, Infinity
		}
	case oidDate:
		if strings.Contains(text, "infinity") || strings.HasSuffix(text, " BC") {
			return nil, true
		}
	case oidTimestamp, oidTimestampTZ:
		t, ok := parseTimestamp(text)
		if !ok {
			return nil, true
		}
		return t.UTC().Format("2006-01-02 15:04:05.000000"), true
	}
	return text, true
}

// joinKey encodes key values for comparison, each length-prefixed.
func joinKey(parts [][]byte) string {
	var b []byte
	for _, p := range parts {
		b = strconv.AppendInt(b, int64(len(p)), 10)
		b = append(b, ':')
		b = append(b, p...)
	}
	return string(b)
}

func numberText(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil && s != "" && (s[0] == '-' || s[0] >= '0' && s[0] <= '9')
}

// parseTimestamp parses Postgres timestamp and timestamptz text (DateStyle
// ISO); without an offset the time is UTC.
func parseTimestamp(s string) (time.Time, bool) {
	if strings.Contains(s, "infinity") || strings.HasSuffix(s, " BC") {
		return time.Time{}, false
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999-07", "2006-01-02 15:04:05.999999-07:00", "2006-01-02 15:04:05.999999-07:00:00",
		"2006-01-02 15:04:05.999999"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// rowOf converts a row image; ok is false if a key column is not known.
func (t *mirrorTable) rowOf(r *pb.Row) (mirrorRow, error) {
	row := mirrorRow{values: map[string]any{}, unchanged: map[string]bool{}}
	var keyParts [][]byte
	for i, col := range t.cols {
		if i >= len(r.GetValues()) {
			break
		}
		v, known := mirrorValue(col, r.GetValues()[i])
		if !known {
			row.unchanged[col.GetName()] = true
			continue
		}
		row.values[col.GetName()] = v
	}
	for _, i := range t.keys {
		if i >= len(r.GetValues()) || r.GetValues()[i].GetKind() != pb.ValueKind_VALUE_KIND_TEXT {
			return mirrorRow{}, fmt.Errorf("key column %s is not known", t.cols[i].GetName())
		}
		keyParts = append(keyParts, r.GetValues()[i].GetData())
	}
	row.key = joinKey(keyParts)
	return row, nil
}

// keyOnly returns the row reduced to its key columns.
func (t *mirrorTable) keyOnly(row mirrorRow) mirrorRow {
	out := mirrorRow{key: row.key, values: map[string]any{}, unchanged: map[string]bool{}}
	for _, i := range t.keys {
		name := t.cols[i].GetName()
		out.values[name] = row.values[name]
	}
	return out
}

// pendingTable collects the rows for one mirror table; the last change of a
// key wins.
type pendingTable struct {
	t     *mirrorTable
	order []string
	rows  map[string]mirrorRow
}

func (p *pendingTable) put(r mirrorRow) {
	if prev, ok := p.rows[r.key]; ok {
		// Values Postgres did not send are the ones of the previous version.
		for name := range r.unchanged {
			if v, ok := prev.values[name]; ok && !prev.unchanged[name] {
				r.values[name] = v
				delete(r.unchanged, name)
			}
		}
	} else {
		p.order = append(p.order, r.key)
	}
	p.rows[r.key] = r
}

// deliverMirror applies the changes to the mirror tables, in order.
func (c *ClickHouse) deliverMirror(ctx context.Context, changes []event.Change) error {
	pending := map[string]*pendingTable{}
	var tables []string
	pendingFor := func(t *mirrorTable) *pendingTable {
		p, ok := pending[t.name]
		if !ok {
			p = &pendingTable{t: t, rows: map[string]mirrorRow{}}
			pending[t.name] = p
			tables = append(tables, t.name)
		}
		return p
	}
	for _, ch := range changes {
		if ch.IsSourceReset() {
			continue
		}
		if ch.Schema == nil {
			return fmt.Errorf("clickhouse: change %s: unknown schema %d", ch.ID(), ch.Event.GetSchemaId())
		}
		t, err := c.mirrorFor(ctx, ch.Schema)
		if err != nil {
			return err
		}
		ver := version(ch)
		switch ch.Event.GetOp() {
		case pb.Op_OP_INSERT, pb.Op_OP_UPDATE, pb.Op_OP_BACKFILL:
			row, err := t.rowOf(ch.Event.GetAfter())
			if err != nil {
				return fmt.Errorf("clickhouse: change %s: %w", ch.ID(), err)
			}
			if before := ch.Event.GetBefore(); before != nil {
				old, err := t.rowOf(before)
				if err != nil {
					return fmt.Errorf("clickhouse: change %s: old row: %w", ch.ID(), err)
				}
				if old.key != row.key {
					// The key changed: the old row goes away.
					gone := t.keyOnly(old)
					gone.values["_version"], gone.values["_deleted"] = ver, 1
					pendingFor(t).put(gone)
				} else {
					// REPLICA IDENTITY FULL sends the old values of unchanged
					// TOASTed columns.
					for name := range row.unchanged {
						if v, ok := old.values[name]; ok && !old.unchanged[name] {
							row.values[name] = v
							delete(row.unchanged, name)
						}
					}
				}
			}
			row.values["_version"], row.values["_deleted"] = ver, 0
			pendingFor(t).put(row)
		case pb.Op_OP_DELETE:
			old, err := t.rowOf(ch.Event.GetBefore())
			if err != nil {
				return fmt.Errorf("clickhouse: change %s: %w", ch.ID(), err)
			}
			gone := t.keyOnly(old)
			gone.values["_version"], gone.values["_deleted"] = ver, 1
			pendingFor(t).put(gone)
		case pb.Op_OP_TRUNCATE:
			if p, ok := pending[t.name]; ok {
				if err := c.flushMirror(ctx, p); err != nil {
					return err
				}
				delete(pending, t.name)
			}
			if _, err := c.exec(ctx, "TRUNCATE TABLE "+t.name, nil); err != nil {
				return err
			}
		}
	}
	for _, name := range tables {
		if p, ok := pending[name]; ok {
			if err := c.flushMirror(ctx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushMirror inserts the pending rows of one table, first taking the
// stored values of columns Postgres did not send.
func (c *ClickHouse) flushMirror(ctx context.Context, p *pendingTable) error {
	if err := c.fillUnchanged(ctx, p); err != nil {
		return err
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, k := range p.order {
		if err := enc.Encode(p.rows[k].values); err != nil {
			return fmt.Errorf("clickhouse: encode: %w", err)
		}
	}
	_, err := c.exec(ctx, "INSERT INTO "+p.t.name+" SETTINGS date_time_input_format = 'best_effort' FORMAT JSONEachRow", &body)
	return err
}

// fillUnchanged reads the current values of unchanged TOASTed columns
// from the mirror (rows it does not hold get NULL).
func (c *ClickHouse) fillUnchanged(ctx context.Context, p *pendingTable) error {
	need := map[string]bool{}
	var rows []string
	for _, k := range p.order {
		r := p.rows[k]
		if len(r.unchanged) == 0 {
			continue
		}
		rows = append(rows, k)
		for name := range r.unchanged {
			need[name] = true
		}
	}
	if len(rows) == 0 {
		return nil
	}
	var keyCols, sel, tuples []string
	for _, i := range p.t.keys {
		name := p.t.cols[i].GetName()
		keyCols = append(keyCols, "toString("+quoteIdent(name)+")")
	}
	for name := range need {
		sel = append(sel, quoteIdent(name))
	}
	for _, k := range rows {
		var lits []string
		for _, i := range p.t.keys {
			lits = append(lits, "'"+strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(fmt.Sprint(p.rows[k].values[p.t.cols[i].GetName()]))+"'")
		}
		tuples = append(tuples, "("+strings.Join(lits, ", ")+")")
	}
	// One key column compares as a scalar: ('7') is not a tuple.
	key := "tuple(" + strings.Join(keyCols, ", ") + ")"
	if len(keyCols) == 1 {
		key = keyCols[0]
		for i, tup := range tuples {
			tuples[i] = strings.TrimSuffix(strings.TrimPrefix(tup, "("), ")")
		}
	}
	q := "SELECT " + key + " AS _key, " + strings.Join(sel, ", ") + " FROM " + p.t.name +
		" FINAL WHERE " + key + " IN (" + strings.Join(tuples, ", ") + ") FORMAT JSONEachRow"
	out, err := c.exec(ctx, q, nil)
	if err != nil {
		return err
	}
	stored := map[string]map[string]any{}
	dec := json.NewDecoder(bytes.NewReader(out))
	dec.UseNumber()
	for dec.More() {
		var r map[string]any
		if err := dec.Decode(&r); err != nil {
			return fmt.Errorf("clickhouse: read stored values: %w", err)
		}
		parts, ok := r["_key"].([]any)
		if !ok {
			parts = []any{r["_key"]}
		}
		var key [][]byte
		for _, part := range parts {
			key = append(key, []byte(fmt.Sprint(part)))
		}
		stored[joinKey(key)] = r
	}
	for _, k := range rows {
		r := p.rows[k]
		for name := range r.unchanged {
			r.values[name] = stored[k][name] // nil if the mirror lacks the row
		}
		r.unchanged = nil
		p.rows[k] = r
	}
	return nil
}
