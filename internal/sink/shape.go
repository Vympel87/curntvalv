package sink

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Shape filters and reshapes the changes of one sink before delivery
// (design D21). Changes it removes are skipped like those of other tables:
// the offset moves past them. Source resets always pass unchanged.
type Shape struct {
	// Ops keeps only these operations; empty keeps all.
	Ops []pb.Op
	// Where keeps a change only if every condition holds on its new row
	// (the old row for a delete). Truncates pass: they have no row.
	Where []Condition
	// Columns keeps only these columns, Drop removes these. Key columns
	// are always kept: sinks need them to de-duplicate and mirror.
	Columns []string
	Drop    []string
}

// shaper applies a Shape, remembering projected schemas. Used by one
// runner goroutine only.
type shaper struct {
	Shape
	schemas map[uint64]*projection // by original schema id
}

func newShaper(s Shape) *shaper { return &shaper{Shape: s, schemas: map[uint64]*projection{}} }

// Condition compares a column's Postgres text value with Value. A NULL
// equals nothing, so "!=" holds for it; an unchanged TOASTed value is
// unknown and fails both.
type Condition struct {
	Column string
	Value  string
	Not    bool // "!=" instead of "="
}

// ParseCondition parses "COLUMN=VALUE" or "COLUMN!=VALUE".
func ParseCondition(s string) (Condition, error) {
	i := strings.IndexByte(s, '=')
	c := Condition{Value: s[i+1:]}
	if i > 0 && s[i-1] == '!' {
		c.Column, c.Not = s[:i-1], true
	} else if i > 0 {
		c.Column = s[:i]
	}
	if i < 0 || c.Column == "" {
		return Condition{}, fmt.Errorf("sink: bad condition %q (want COLUMN=VALUE or COLUMN!=VALUE)", s)
	}
	return c, nil
}

// ParseOp parses an operation name as in the JSON form ("insert", ...).
func ParseOp(s string) (pb.Op, error) {
	op, ok := pb.Op_value["OP_"+strings.ToUpper(s)]
	if !ok || pb.Op(op) == pb.Op_OP_UNSPECIFIED || pb.Op(op) == pb.Op_OP_SOURCE_RESET {
		return 0, fmt.Errorf("sink: unknown op %q (insert, update, delete, truncate, backfill)", s)
	}
	return pb.Op(op), nil
}

// projection maps a schema to its projected form.
type projection struct {
	schema *pb.Schema
	keep   []int // indexes of the kept columns; nil keeps all
}

// Empty reports whether the shape changes nothing.
func (s Shape) Empty() bool {
	return len(s.Ops) == 0 && len(s.Where) == 0 && len(s.Columns) == 0 && len(s.Drop) == 0
}

// Apply returns the changes that pass, projected. It does not modify its
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
		out = append(out, s.project(c))
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
	if row == nil || c.Schema == nil {
		return false
	}
	for _, cond := range s.Where {
		i := slices.IndexFunc(c.Schema.GetColumns(), func(col *pb.Column) bool { return col.GetName() == cond.Column })
		if i < 0 || i >= len(row.GetValues()) {
			return false // the table has no such column
		}
		v := row.GetValues()[i]
		var eq bool
		switch v.GetKind() {
		case pb.ValueKind_VALUE_KIND_NULL:
			eq = false
		case pb.ValueKind_VALUE_KIND_TEXT:
			eq = bytes.Equal(v.GetData(), []byte(cond.Value))
		default:
			return false // unchanged TOAST or binary: unknown
		}
		if eq == cond.Not {
			return false
		}
	}
	return true
}

// project removes the columns the shape does not keep.
func (s *shaper) project(c event.Change) event.Change {
	if len(s.Columns) == 0 && len(s.Drop) == 0 || c.Schema == nil {
		return c
	}
	p := s.projectionOf(c.Schema)
	if p.keep == nil {
		return c
	}
	ev := &pb.Event{SchemaId: p.schema.GetId(), Op: c.Event.GetOp(), Before: pick(c.Event.GetBefore(), p.keep), After: pick(c.Event.GetAfter(), p.keep)}
	c.Event, c.Schema = ev, p.schema
	return c
}

func pick(r *pb.Row, keep []int) *pb.Row {
	if r == nil {
		return nil
	}
	out := &pb.Row{Values: make([]*pb.Value, 0, len(keep))}
	for _, i := range keep {
		if i < len(r.GetValues()) {
			out.Values = append(out.Values, r.GetValues()[i])
		}
	}
	return out
}

// projectionOf returns the projected schema, computed once per schema.
func (s *shaper) projectionOf(sc *pb.Schema) *projection {
	if p, ok := s.schemas[sc.GetId()]; ok {
		return p
	}
	p := &projection{schema: sc}
	var keep []int
	var cols []*pb.Column
	for i, col := range sc.GetColumns() {
		name := col.GetName()
		if col.GetKey() || (len(s.Columns) == 0 || slices.Contains(s.Columns, name)) && !slices.Contains(s.Drop, name) {
			keep = append(keep, i)
			cols = append(cols, col)
		}
	}
	if len(keep) < len(sc.GetColumns()) {
		ps := &pb.Schema{RelationOid: sc.GetRelationOid(), Namespace: sc.GetNamespace(), Name: sc.GetName(),
			ReplicaIdentity: sc.GetReplicaIdentity(), Columns: cols}
		ps.Id = fsm.SchemaID(ps)
		p.schema, p.keep = ps, keep
	}
	s.schemas[sc.GetId()] = p
	return p
}
