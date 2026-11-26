package client

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Vympel87/curntvalv/internal/compat"
)

// TestClientCompat lists the exported API of this package and the JSON form
// of Change against compat/client.txt (design D34): the client follows
// semantic versioning with the server.
func TestClientCompat(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	var lines []string
	expr := func(n ast.Node) string {
		var b bytes.Buffer
		printer.Fprint(&b, fset, n)
		return strings.Join(strings.Fields(b.String()), " ")
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				recv := ""
				if d.Recv != nil {
					recv = "(" + expr(d.Recv.List[0].Type) + ") "
				}
				lines = append(lines, "func "+recv+d.Name.Name+strings.TrimPrefix(expr(d.Type), "func"))
			case *ast.GenDecl:
				for _, sp := range d.Specs {
					switch sp := sp.(type) {
					case *ast.TypeSpec:
						if !sp.Name.IsExported() {
							continue
						}
						st, ok := sp.Type.(*ast.StructType)
						if !ok {
							lines = append(lines, "type "+sp.Name.Name+" "+expr(sp.Type))
							continue
						}
						lines = append(lines, "type "+sp.Name.Name+" struct")
						for _, fl := range st.Fields.List {
							for _, n := range fl.Names {
								if n.IsExported() {
									lines = append(lines, "field "+sp.Name.Name+"."+n.Name+" "+expr(fl.Type))
								}
							}
						}
					case *ast.ValueSpec:
						for i, n := range sp.Names {
							if !n.IsExported() {
								continue
							}
							line := d.Tok.String() + " " + n.Name
							if sp.Type != nil {
								line += " " + expr(sp.Type)
							}
							if d.Tok == token.CONST && i < len(sp.Values) {
								line += " = " + expr(sp.Values[i])
							}
							lines = append(lines, line)
						}
					}
				}
			}
		}
	}
	lines = append(lines, compat.JSONFields("json Change", Change{})...)
	compat.Check(t, "client.txt", lines, compat.Updating())
}
