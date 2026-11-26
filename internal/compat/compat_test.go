package compat

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiff(t *testing.T) {
	gone, added := Diff([]string{"a", "b", "c"}, []string{"c", "d", "a"})
	if fmt.Sprint(gone) != "[b]" || fmt.Sprint(added) != "[d]" {
		t.Fatalf("gone %v added %v", gone, added)
	}
}

type recorder struct {
	errs  []string
	fatal string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, a ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, a...))
}
func (r *recorder) Fatalf(format string, a ...any) { r.fatal = fmt.Sprintf(format, a...) }

func TestCheck(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "compat"), 0o755)
	os.WriteFile(filepath.Join(root, "compat", "README.md"), nil, 0o644)
	os.WriteFile(filepath.Join(root, "compat", "x.txt"), []byte("# comment\na\nb\n"), 0o644)
	pkg := filepath.Join(root, "internal", "p")
	os.MkdirAll(pkg, 0o755)
	t.Chdir(pkg)

	var r recorder
	Check(&r, "x.txt", []string{"a", "b"}, false)
	if len(r.errs) != 0 || r.fatal != "" {
		t.Fatalf("unchanged surface: %v %s", r.errs, r.fatal)
	}
	// A new line fails until recorded; recording keeps every old line.
	r = recorder{}
	Check(&r, "x.txt", []string{"a", "b", "c"}, false)
	if len(r.errs) != 1 || !strings.Contains(r.errs[0], "make baseline") {
		t.Fatalf("new line: %v", r.errs)
	}
	r = recorder{}
	Check(&r, "x.txt", []string{"a", "c"}, true)
	data, _ := os.ReadFile(filepath.Join(root, "compat", "x.txt"))
	if len(r.errs) != 1 || !strings.Contains(r.errs[0], "b") || !strings.HasSuffix(string(data), "a\nb\nc\n") {
		t.Fatalf("removed line while updating: %v\n%s", r.errs, data)
	}
}

func TestJSONFields(t *testing.T) {
	type inner struct {
		A string `json:"a"`
	}
	type outer struct {
		ID     string  `json:"id"`
		Rows   []inner `json:"rows,omitempty"`
		In     *inner  `json:"in,omitempty"`
		Big    uint64  `json:"big,string"`
		hidden int
		Skip   int `json:"-"`
	}
	got := strings.Join(JSONFields("x", outer{}), "\n")
	want := "x.id string\nx.rows []compat.inner omitempty\nx.rows[].a string\nx.in *compat.inner omitempty\nx.in.a string\nx.big uint64 string"
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}
