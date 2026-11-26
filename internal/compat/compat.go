// Package compat checks the public surface of curntValv against baselines
// kept in the repository's compat directory (design D34): the gRPC API, the
// JSON form of a change, the Go client, the command line, sink options and
// metrics. Within 1.x the surface may grow but nothing in it may go away or
// change meaning, so a test that lists the surface fails when a baseline
// line is missing. New lines must be recorded (make baseline), so the next
// release protects them too; recording never drops a line, so removing one
// takes an edit of the baseline that a reviewer sees.
package compat

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
)

// UpdateEnv set to 1 records new lines instead of failing on them.
const UpdateEnv = "CURNTVALV_UPDATE_BASELINE"

// TB is the part of testing.TB that Check uses.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// Dir finds the compat directory from a package directory: the first
// "compat" directory with a README.md above it.
func Dir() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		c := filepath.Join(dir, "compat")
		if _, err := os.Stat(filepath.Join(c, "README.md")); err == nil {
			return c, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("compat: no compat directory above the working directory")
		}
		dir = parent
	}
}

// Diff compares current with baseline: lines gone from current and lines
// new in it, each sorted.
func Diff(baseline, current []string) (gone, added []string) {
	for _, l := range baseline {
		if !slices.Contains(current, l) {
			gone = append(gone, l)
		}
	}
	for _, l := range current {
		if !slices.Contains(baseline, l) {
			added = append(added, l)
		}
	}
	slices.Sort(gone)
	slices.Sort(added)
	return slices.Compact(gone), slices.Compact(added)
}

// Check compares the lines of current with the baseline file name in the
// compat directory. Missing lines fail. New lines fail too unless
// update is set, which adds them to the file.
func Check(t TB, name string, current []string, update bool) {
	t.Helper()
	dir, err := Dir()
	if err != nil {
		t.Fatalf("%v", err)
	}
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%v", err)
	}
	var baseline []string
	for _, l := range strings.Split(string(data), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			baseline = append(baseline, l)
		}
	}
	gone, added := Diff(baseline, current)
	if len(gone) > 0 {
		t.Errorf("%s: %d line(s) of the public surface are gone or changed (design D34); within 1.x they must stay:\n  %s",
			name, len(gone), strings.Join(gone, "\n  "))
	}
	if len(added) == 0 {
		return
	}
	if !update {
		t.Errorf("%s: %d new line(s); record them with `make baseline` (they are then part of the promise):\n  %s",
			name, len(added), strings.Join(added, "\n  "))
		return
	}
	all := append(slices.Clone(baseline), added...)
	slices.Sort(all)
	const header = "# Public surface baseline (design D34). Lines may be added with `make baseline`;\n" +
		"# removing or changing one breaks the 1.x compatibility promise.\n"
	if err := os.WriteFile(path, []byte(header+strings.Join(slices.Compact(all), "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("%v", err)
	}
}

// Updating reports whether UpdateEnv asks to record new lines.
func Updating() bool { return os.Getenv(UpdateEnv) == "1" }

// JSONFields lists the JSON fields of the struct type of v, nested ones
// with their path ("reset.lsn", "columns[].name"), each with its Go type.
func JSONFields(prefix string, v any) []string {
	return jsonFields(prefix, reflect.TypeOf(v))
}

func jsonFields(prefix string, t reflect.Type) []string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		path := prefix + "." + name
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		line := path + " " + f.Type.String()
		if strings.Contains(opts, "omitempty") {
			line += " omitempty"
		}
		if strings.Contains(opts, "string") {
			line += " string"
		}
		out = append(out, line)
		switch {
		case ft.Kind() == reflect.Struct && ft.PkgPath() != "time":
			out = append(out, jsonFields(path, ft)...)
		case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Struct:
			out = append(out, jsonFields(path+"[]", ft.Elem())...)
		}
	}
	return out
}
