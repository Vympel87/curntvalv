package sink

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Row filter expressions (design D23):
//
//	expr    = or
//	or      = and { "or" and }
//	and     = not { "and" not }
//	not     = "not" not | "(" expr ")" | test
//	test    = column op value
//	        | column [ "not" ] "in" "(" value { "," value } ")"
//	        | column [ "not" ] "like" value
//	        | column "is" [ "not" ] "null"
//	op      = "=" | "!=" | "<>" | "<" | "<=" | ">" | ">="
//	value   = 'quoted' (” for ') | a bare word or number
//	column  = name | "quoted name"
//
// Keywords are case-insensitive. A test compares the column's Postgres text
// value; for numeric columns and number literals it compares numbers
// exactly, for boolean columns true/false match t/f. NULL is a value of its
// own: "=", "<", "in" and "like" are false for it, "!=", "not in" and "not
// like" true. A value that is not known (an unchanged TOASTed value, binary
// data, a column the table does not have) makes the test unknown, with
// SQL's three-valued and/or/not; an expression that is not true filters the
// row out.

// Expr is a compiled row filter expression.
type Expr struct {
	src  string
	root node
}

func (e *Expr) String() string { return e.src }

// tri is a three-valued truth value.
type tri int8

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

func triOf(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

type node interface {
	eval(s *pb.Schema, r *pb.Row) tri
}

type andNode struct{ a, b node }
type orNode struct{ a, b node }
type notNode struct{ a node }

func (n andNode) eval(s *pb.Schema, r *pb.Row) tri {
	a, b := n.a.eval(s, r), n.b.eval(s, r)
	switch {
	case a == triFalse || b == triFalse:
		return triFalse
	case a == triTrue && b == triTrue:
		return triTrue
	}
	return triUnknown
}

func (n orNode) eval(s *pb.Schema, r *pb.Row) tri {
	a, b := n.a.eval(s, r), n.b.eval(s, r)
	switch {
	case a == triTrue || b == triTrue:
		return triTrue
	case a == triFalse && b == triFalse:
		return triFalse
	}
	return triUnknown
}

func (n notNode) eval(s *pb.Schema, r *pb.Row) tri {
	switch n.a.eval(s, r) {
	case triTrue:
		return triFalse
	case triFalse:
		return triTrue
	}
	return triUnknown
}

// test is one comparison of a column.
type test struct {
	column string
	op     string // = != < <= > >= in like null
	neg    bool   // not in, not like, is not null
	values []string
	like   *regexp.Regexp
}

// Postgres type OIDs compared as numbers.
func numericType(oid uint32) bool {
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidFloat4, oidFloat8, oidNumeric:
		return true
	}
	return false
}

func (t test) eval(s *pb.Schema, r *pb.Row) tri {
	i := slices.IndexFunc(s.GetColumns(), func(c *pb.Column) bool { return c.GetName() == t.column })
	if i < 0 || i >= len(r.GetValues()) {
		return triUnknown
	}
	v := r.GetValues()[i]
	switch v.GetKind() {
	case pb.ValueKind_VALUE_KIND_NULL:
		if t.op == "null" {
			return triOf(!t.neg)
		}
		// NULL is a value of its own: it equals nothing.
		return triOf(t.op == "!=" || t.neg)
	case pb.ValueKind_VALUE_KIND_TEXT:
	default:
		return triUnknown
	}
	if t.op == "null" {
		return triOf(t.neg)
	}
	col := s.GetColumns()[i]
	text := string(v.GetData())
	switch t.op {
	case "in":
		in := slices.ContainsFunc(t.values, func(lit string) bool { return compare(col, text, lit) == 0 })
		return triOf(in != t.neg)
	case "like":
		return triOf(t.like.MatchString(text) != t.neg)
	}
	c := compare(col, text, t.values[0])
	switch t.op {
	case "=":
		return triOf(c == 0)
	case "!=":
		return triOf(c != 0)
	case "<":
		return triOf(c < 0)
	case "<=":
		return triOf(c <= 0)
	case ">":
		return triOf(c > 0)
	default: // ">="
		return triOf(c >= 0)
	}
}

// compare compares a column's text value with a literal: as numbers when
// the column is numeric and both parse, as t/f for booleans, else as bytes.
func compare(col *pb.Column, text, lit string) int {
	if numericType(col.GetTypeOid()) {
		a, okA := new(big.Rat).SetString(text)
		b, okB := new(big.Rat).SetString(lit)
		if okA && okB {
			return a.Cmp(b)
		}
	}
	if col.GetTypeOid() == oidBool {
		switch strings.ToLower(lit) {
		case "true":
			lit = "t"
		case "false":
			lit = "f"
		}
	}
	return strings.Compare(text, lit)
}

// ParseExpr compiles a row filter expression. "COLUMN=VALUE" and
// "COLUMN!=VALUE" of the first version (design D21) are expressions too.
func ParseExpr(src string) (*Expr, error) {
	p := &parser{src: src}
	if err := p.lex(); err != nil {
		return nil, fmt.Errorf("sink: where %q: %w", src, err)
	}
	if len(p.toks) == 0 {
		return nil, fmt.Errorf("sink: where: empty expression")
	}
	root, err := p.or()
	if err == nil && p.pos < len(p.toks) {
		err = fmt.Errorf("unexpected %q", p.toks[p.pos].text)
	}
	if err != nil {
		return nil, fmt.Errorf("sink: where %q: %w", src, err)
	}
	return &Expr{src: src, root: root}, nil
}

// Eval reports whether the row matches; a missing row does not.
func (e *Expr) Eval(s *pb.Schema, r *pb.Row) bool {
	return r != nil && s != nil && e.root.eval(s, r) == triTrue
}

type tokKind int

const (
	tokWord tokKind = iota // bare word or number, or keyword
	tokString
	tokIdent // "quoted name"
	tokOp
	tokPunct // ( ) ,
)

type token struct {
	kind tokKind
	text string
}

type parser struct {
	src  string
	toks []token
	pos  int
}

func (p *parser) lex() error {
	s := p.src
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == '(' || c == ')' || c == ',':
			p.toks = append(p.toks, token{tokPunct, string(c)})
			i++
		case c == '\'' || c == '"':
			var b strings.Builder
			j := i + 1
			for {
				if j >= len(s) {
					return fmt.Errorf("unterminated %c", c)
				}
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c { // doubled: the quote itself
						b.WriteByte(c)
						j += 2
						continue
					}
					break
				}
				b.WriteByte(s[j])
				j++
			}
			kind := tokString
			if c == '"' {
				kind = tokIdent
			}
			p.toks = append(p.toks, token{kind, b.String()})
			i = j + 1
		case strings.ContainsRune("=!<>", rune(c)):
			op := string(c)
			if i+1 < len(s) && (s[i+1] == '=' || c == '<' && s[i+1] == '>') {
				op = s[i : i+2]
			}
			i += len(op)
			switch op {
			case "!":
				return fmt.Errorf("lone '!' (use != or not)")
			case "<>":
				op = "!="
			case "==":
				op = "="
			}
			p.toks = append(p.toks, token{tokOp, op})
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t\n(),'\"=!<>", rune(s[j])) {
				j++
			}
			p.toks = append(p.toks, token{tokWord, s[i:j]})
			i = j
		}
	}
	return nil
}

func (p *parser) peek() (token, bool) {
	if p.pos < len(p.toks) {
		return p.toks[p.pos], true
	}
	return token{}, false
}

// keyword consumes the keyword kw if it comes next.
func (p *parser) keyword(kw string) bool {
	if t, ok := p.peek(); ok && t.kind == tokWord && strings.EqualFold(t.text, kw) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) punct(c string) bool {
	if t, ok := p.peek(); ok && t.kind == tokPunct && t.text == c {
		p.pos++
		return true
	}
	return false
}

func (p *parser) or() (node, error) {
	n, err := p.and()
	for err == nil && p.keyword("or") {
		var m node
		if m, err = p.and(); err == nil {
			n = orNode{n, m}
		}
	}
	return n, err
}

func (p *parser) and() (node, error) {
	n, err := p.not()
	for err == nil && p.keyword("and") {
		var m node
		if m, err = p.not(); err == nil {
			n = andNode{n, m}
		}
	}
	return n, err
}

func (p *parser) not() (node, error) {
	if p.keyword("not") {
		n, err := p.not()
		return notNode{n}, err
	}
	if p.punct("(") {
		n, err := p.or()
		if err == nil && !p.punct(")") {
			err = fmt.Errorf("missing )")
		}
		return n, err
	}
	return p.test()
}

func isKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "and", "or", "not", "in", "like", "is", "null":
		return true
	}
	return false
}

func (p *parser) column() (string, error) {
	t, ok := p.peek()
	switch {
	case !ok:
		return "", fmt.Errorf("missing column at the end")
	case t.kind == tokIdent:
	case t.kind == tokWord && !isKeyword(t.text):
	default:
		return "", fmt.Errorf("expected a column, got %q", t.text)
	}
	p.pos++
	return t.text, nil
}

func (p *parser) value() (string, error) {
	t, ok := p.peek()
	if !ok || (t.kind != tokString && t.kind != tokWord) || (t.kind == tokWord && isKeyword(t.text)) {
		if !ok {
			return "", fmt.Errorf("missing value at the end")
		}
		return "", fmt.Errorf("expected a value, got %q", t.text)
	}
	p.pos++
	return t.text, nil
}

func (p *parser) test() (node, error) {
	col, err := p.column()
	if err != nil {
		return nil, err
	}
	t := test{column: col}
	switch {
	case p.keyword("is"):
		t.op, t.neg = "null", p.keyword("not")
		if !p.keyword("null") {
			return nil, fmt.Errorf("expected null after is")
		}
		return t, nil
	case p.keyword("not"):
		t.neg = true
		if p.keyword("in") {
			return p.inList(t)
		}
		if p.keyword("like") {
			return p.likeTest(t)
		}
		return nil, fmt.Errorf("expected in or like after not")
	case p.keyword("in"):
		return p.inList(t)
	case p.keyword("like"):
		return p.likeTest(t)
	}
	op, ok := p.peek()
	if !ok || op.kind != tokOp {
		return nil, fmt.Errorf("expected an operator after %s", col)
	}
	p.pos++
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	t.op, t.values = op.text, []string{v}
	return t, nil
}

func (p *parser) inList(t test) (node, error) {
	t.op = "in"
	if !p.punct("(") {
		return nil, fmt.Errorf("expected ( after in")
	}
	for {
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		t.values = append(t.values, v)
		if p.punct(")") {
			return t, nil
		}
		if !p.punct(",") {
			return nil, fmt.Errorf("expected , or ) in the in list")
		}
	}
}

// likeTest compiles a LIKE pattern: % any run, _ one character.
func (p *parser) likeTest(t test) (node, error) {
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	var re strings.Builder
	re.WriteString("^(?s:")
	for _, r := range v {
		switch r {
		case '%':
			re.WriteString(".*")
		case '_':
			re.WriteString(".")
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString(")$")
	rx, err := regexp.Compile(re.String())
	if err != nil {
		return nil, fmt.Errorf("like %q: %w", v, err)
	}
	t.op, t.like = "like", rx
	return t, nil
}
