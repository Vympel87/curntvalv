package sink

import (
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
}

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
}

// Empty reports whether the shape changes nothing.
func (s Shape) Empty() bool {
	return len(s.Ops) == 0 && len(s.Where) == 0 && len(s.Columns) == 0 && len(s.Drop) == 0 &&
		len(s.Mask) == 0 && len(s.Rename) == 0
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
		if !s.matches(c) {
			continue
		}
		out = append(out, s.reshape(c))
	}
	return out
}

func (s *shaper) matches(c event.Change) bool {
	if len(s.Where) == 0 || c.Event.GetOp() == pb.Op_OP_TRUNCATE {
		return true
	}
	row := c.Event.GetAfter()
	if c.Event.GetOp() == pb.Op_OP_DELETE {
		row = c.Event.GetBefore()
	}
	for _, e := range s.Where {
		if !e.Eval(c.Schema, row) {
			return false
		}
	}
	return true
}

// reshape projects, masks and renames.
func (s *shaper) reshape(c event.Change) event.Change {
	if len(s.Columns) == 0 && len(s.Drop) == 0 && len(s.Mask) == 0 && len(s.Rename) == 0 || c.Schema == nil {
		return c
	}
	p := s.projectionOf(c.Schema)
	if p.keep == nil {
		return c
	}
	ev := &pb.Event{SchemaId: p.schema.GetId(), Op: c.Event.GetOp(),
		Before: pick(c.Event.GetBefore(), p.keep, p.mask), After: pick(c.Event.GetAfter(), p.keep, p.mask)}
	c.Event, c.Schema = ev, p.schema
	return c
}

func pick(r *pb.Row, keep []int, mask []bool) *pb.Row {
	if r == nil {
		return nil
	}
	out := &pb.Row{Values: make([]*pb.Value, 0, len(keep))}
	for j, i := range keep {
		if i >= len(r.GetValues()) {
			continue
		}
		v := r.GetValues()[i]
		if mask[j] && (v.GetKind() == pb.ValueKind_VALUE_KIND_TEXT || v.GetKind() == pb.ValueKind_VALUE_KIND_BINARY) {
			v = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(maskValue)}
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
	var mask []bool
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
		if m {
			nc.TypeOid, nc.TypeModifier = oidText, -1
		}
		if to, ok := s.Rename[name]; ok && !names[to] {
			nc.Name = to
		}
		if m || nc.Name != name {
			changed = true
		}
		keep, mask, cols = append(keep, i), append(mask, m), append(cols, nc)
	}
	p := &projection{schema: sc}
	if changed {
		ps := &pb.Schema{RelationOid: sc.GetRelationOid(), Namespace: sc.GetNamespace(), Name: sc.GetName(),
			ReplicaIdentity: sc.GetReplicaIdentity(), Columns: cols}
		ps.Id = fsm.SchemaID(ps)
		p.schema, p.keep, p.mask = ps, keep, mask
	}
	s.schemas[sc.GetId()] = p
	return p
}
