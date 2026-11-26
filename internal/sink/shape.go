package sink

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Shape filters and reshapes the changes of one sink before delivery
// (designs D21, D23). Changes it removes are skipped like those of other
// tables: the offset moves past them. Source resets always pass unchanged.
// Every option names columns by their Postgres names.
type Shape struct {
	// Ops keeps only these operations; empty keeps all.
	Ops []pb.Op
	// Where keeps a change only if every expression is true on its new row
	// (the old row for a delete). Truncates pass: they have no row.
	Where []*Expr
	// Columns keeps only these columns, Drop removes these. Key columns
	// are always kept: sinks need them to de-duplicate and mirror.
	Columns []string
	Drop    []string
	// Mask replaces the values of these columns with "***" (NULL stays
	// NULL). Key columns are not masked.
	Mask []string
	// Rename maps Postgres column names to the names the sink sees. It is
	// not applied to a table that already has a column of the new name.
	Rename map[string]string
	// Hash replaces the values of these columns, key columns too, with
	// the hex HMAC-SHA256 of their Postgres text under HashKey (design
	// D26): equal values stay equal, so they can still be joined, but
	// cannot be guessed without the key. NULL stays NULL.
	Hash    []string
	HashKey []byte
	// LeaveAsDelete delivers an update whose new row fails Where as a
	// delete, unless its old row is known to have failed too, and passes
	// a delete unless its old row is known to fail (design D27). Mirrors
	// then drop rows that leave the filter. Without it such updates are
	// skipped, and deletes need their old row to match.
	LeaveAsDelete bool
}

// MinHashKey is the shortest hash key accepted, in bytes.
const MinHashKey = 16

// maskValue replaces masked values.
const maskValue = "***"

// shaper applies a Shape, remembering reshaped schemas. Used by one runner
// goroutine only.
type shaper struct {
	Shape
	schemas map[uint64]*projection // by original schema id
}

func newShaper(s Shape) *shaper { return &shaper{Shape: s, schemas: map[uint64]*projection{}} }

// oldCondition is the first form of where (design D21): COLUMN=VALUE or
// COLUMN!=VALUE with any value.
var oldCondition = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(!?=)(.*)$`)

// ParseWhere compiles a where option: an expression (design D23), or a
// COLUMN=VALUE / COLUMN!=VALUE of the first form whose value is taken as it
// is (e.g. "name=John Smith", "note=").
func ParseWhere(s string) (*Expr, error) {
	e, err := ParseExpr(s)
	if err == nil {
		return e, nil
	}
	m := oldCondition.FindStringSubmatch(s)
	if m == nil {
		return nil, err
	}
	t := test{column: m[1], op: "=", values: []string{m[3]}}
	if m[2] == "!=" {
		t.op = "!="
	}
	return &Expr{src: s, root: t}, nil
}

// ParseOp parses an operation name as in the JSON form ("insert", ...).
func ParseOp(s string) (pb.Op, error) {
	op, ok := pb.Op_value["OP_"+strings.ToUpper(s)]
	if !ok || pb.Op(op) == pb.Op_OP_UNSPECIFIED || pb.Op(op) == pb.Op_OP_SOURCE_RESET {
		return 0, fmt.Errorf("sink: unknown op %q (insert, update, delete, truncate, backfill)", s)
	}
	return pb.Op(op), nil
}

// ParseRename parses "OLD:NEW" pairs joined by "+".
func ParseRename(s string) (map[string]string, error) {
	out := map[string]string{}
	seen := map[string]bool{}
	for _, pair := range strings.Split(s, "+") {
		from, to, ok := strings.Cut(pair, ":")
		if !ok || from == "" || to == "" || from == to {
			return nil, fmt.Errorf("sink: bad rename %q (want OLD:NEW, joined by +)", pair)
		}
		if _, dup := out[from]; dup || seen[to] {
			return nil, fmt.Errorf("sink: rename %q: a column renamed twice, or two to one name", pair)
		}
		out[from], seen[to] = to, true
	}
	return out, nil
}

// projection maps a schema to its reshaped form.
type projection struct {
	schema *pb.Schema
	keep   []int  // indexes of the kept columns; nil keeps everything as is
	mask   []bool // by kept position
	hash   []bool // by kept position
}

// Empty reports whether the shape changes nothing.
func (s Shape) Empty() bool {
	return len(s.Ops) == 0 && len(s.Where) == 0 && len(s.Columns) == 0 && len(s.Drop) == 0 &&
		len(s.Mask) == 0 && len(s.Rename) == 0 && len(s.Hash) == 0
}

// whereOf evaluates every Where expression on row, three-valued.
func (s *shaper) whereOf(sc *pb.Schema, row *pb.Row) tri {
	out := triTrue
	for _, e := range s.Where {
		switch e.eval(sc, row) {
		case triFalse:
			return triFalse
		case triUnknown:
			out = triUnknown
		}
	}
	return out
}

// Apply returns the changes that pass, reshaped. It does not modify its
// input.
func (s *shaper) Apply(changes []event.Change) []event.Change {
	if s.Empty() {
		return changes
	}
	out := make([]event.Change, 0, len(changes))
	for _, c := range changes {
		if c.IsSourceReset() {
			out = append(out, c)
			continue
		}
		if len(s.Ops) > 0 && !slices.Contains(s.Ops, c.Event.GetOp()) {
			continue
		}
		c, ok := s.filter(c)
		if !ok {
			continue
		}
		out = append(out, s.reshape(c))
	}
	return out
}

// filter applies Where; with LeaveAsDelete an update may come back as a
// delete.
func (s *shaper) filter(c event.Change) (event.Change, bool) {
	op := c.Event.GetOp()
	if len(s.Where) == 0 || op == pb.Op_OP_TRUNCATE {
		return c, true
	}
	if op == pb.Op_OP_DELETE {
		w := s.whereOf(c.Schema, oldRow(c.Schema, c.Event.GetBefore()))
		return c, w == triTrue || s.LeaveAsDelete && w == triUnknown
	}
	if s.whereOf(c.Schema, c.Event.GetAfter()) == triTrue {
		return c, true
	}
	if !s.LeaveAsDelete || op != pb.Op_OP_UPDATE {
		return c, false
	}
	// The new row fails. Unless the old one is known to have failed too,
	// the sink may hold the row: it goes.
	if s.whereOf(c.Schema, oldRow(c.Schema, c.Event.GetBefore())) == triFalse {
		return c, false
	}
	before := c.Event.GetBefore()
	if before == nil {
		before = keyOnly(c.Schema, c.Event.GetAfter())
	}
	c.Event = &pb.Event{SchemaId: c.Event.GetSchemaId(), Op: pb.Op_OP_DELETE, Before: before}
	return c, true
}

// identityFull is pgoutput's replica identity of REPLICA IDENTITY FULL.
const identityFull = 'f'

// oldRow returns an old row image for filtering: unless the table has
// REPLICA IDENTITY FULL, Postgres sends only the key columns and NULL for
// the others, which are unknown rather than NULL.
func oldRow(sc *pb.Schema, row *pb.Row) *pb.Row {
	if row == nil || sc.GetReplicaIdentity() == identityFull {
		return row
	}
	out := &pb.Row{Values: make([]*pb.Value, len(row.GetValues()))}
	for i, v := range row.GetValues() {
		if i < len(sc.GetColumns()) && sc.GetColumns()[i].GetKey() {
			out.Values[i] = v
		} else {
			out.Values[i] = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_UNCHANGED}
		}
	}
	return out
}

// keyOnly returns row with every non-key column NULL, as Postgres sends
// the old row of a delete with the default replica identity.
func keyOnly(sc *pb.Schema, row *pb.Row) *pb.Row {
	out := &pb.Row{Values: make([]*pb.Value, len(row.GetValues()))}
	for i, v := range row.GetValues() {
		if i < len(sc.GetColumns()) && sc.GetColumns()[i].GetKey() {
			out.Values[i] = v
		} else {
			out.Values[i] = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_NULL}
		}
	}
	return out
}

// reshape projects, masks and renames.
func (s *shaper) reshape(c event.Change) event.Change {
	if len(s.Columns) == 0 && len(s.Drop) == 0 && len(s.Mask) == 0 && len(s.Rename) == 0 && len(s.Hash) == 0 || c.Schema == nil {
		return c
	}
	p := s.projectionOf(c.Schema)
	if p.keep == nil {
		return c
	}
	ev := &pb.Event{SchemaId: p.schema.GetId(), Op: c.Event.GetOp(),
		Before: s.pick(c.Event.GetBefore(), p), After: s.pick(c.Event.GetAfter(), p)}
	c.Event, c.Schema = ev, p.schema
	return c
}

func (s *shaper) pick(r *pb.Row, p *projection) *pb.Row {
	if r == nil {
		return nil
	}
	out := &pb.Row{Values: make([]*pb.Value, 0, len(p.keep))}
	for j, i := range p.keep {
		if i >= len(r.GetValues()) {
			continue
		}
		v := r.GetValues()[i]
		known := v.GetKind() == pb.ValueKind_VALUE_KIND_TEXT || v.GetKind() == pb.ValueKind_VALUE_KIND_BINARY
		switch {
		case p.mask[j] && known:
			v = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(maskValue)}
		case p.hash[j] && known:
			m := hmac.New(sha256.New, s.HashKey)
			m.Write(v.GetData())
			v = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(hex.EncodeToString(m.Sum(nil)))}
		}
		out.Values = append(out.Values, v)
	}
	return out
}

// oidText is the type a masked column gets in the reshaped schema.
const oidText = 25

// projectionOf returns the reshaped schema, computed once per schema.
func (s *shaper) projectionOf(sc *pb.Schema) *projection {
	if p, ok := s.schemas[sc.GetId()]; ok {
		return p
	}
	names := map[string]bool{}
	for _, col := range sc.GetColumns() {
		names[col.GetName()] = true
	}
	var keep []int
	var mask, hash []bool
	var cols []*pb.Column
	changed := false
	for i, col := range sc.GetColumns() {
		name := col.GetName()
		if !col.GetKey() && (len(s.Columns) > 0 && !slices.Contains(s.Columns, name) || slices.Contains(s.Drop, name)) {
			changed = true
			continue
		}
		nc := &pb.Column{Name: name, TypeOid: col.GetTypeOid(), TypeModifier: col.GetTypeModifier(), Key: col.GetKey()}
		m := !col.GetKey() && slices.Contains(s.Mask, name)
		h := slices.Contains(s.Hash, name)
		if m || h {
			nc.TypeOid, nc.TypeModifier = oidText, -1
		}
		if to, ok := s.Rename[name]; ok && !names[to] {
			nc.Name = to
		}
		if m || h || nc.Name != name {
			changed = true
		}
		keep, mask, hash, cols = append(keep, i), append(mask, m), append(hash, h), append(cols, nc)
	}
	p := &projection{schema: sc}
	if changed {
		ps := &pb.Schema{RelationOid: sc.GetRelationOid(), Namespace: sc.GetNamespace(), Name: sc.GetName(),
			ReplicaIdentity: sc.GetReplicaIdentity(), Columns: cols}
		ps.Id = fsm.SchemaID(ps)
		p.schema, p.keep, p.mask, p.hash = ps, keep, mask, hash
	}
	s.schemas[sc.GetId()] = p
	return p
}
