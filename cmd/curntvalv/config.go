package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config files (design D16). A file is lines of KEY = VALUE where KEY is a
// server flag name; it fills in the flags the command line leaves unset.

// configEntry is one KEY = VALUE line of a config file.
type configEntry struct {
	key, value string
	line       int
}

// repeatable reports whether a flag may appear more than once in a file.
func repeatable(key string) bool { return key == "sink" }

// envShadow returns the environment variable that overrides a flag when it
// comes from a file: flags beat the environment, which beats the file.
func envShadow(key string) string {
	switch key {
	case "pg-url":
		return "CURNTVALV_PG_URL"
	case "auth-token-file":
		return "CURNTVALV_AUTH_TOKEN"
	}
	return ""
}

// parseConfig reads KEY = VALUE lines; errors start with the line number. Blank lines and lines starting with #
// are skipped. A value may be double quoted (Go syntax, so \" and \\ work);
// an unquoted value ends at " #", which starts a comment.
func parseConfig(data string) ([]configEntry, error) {
	var out []configEntry
	sc := bufio.NewScanner(strings.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("%d: want KEY = VALUE", n)
		}
		val, err := configValue(strings.TrimSpace(val))
		if err != nil {
			return nil, fmt.Errorf("%d: %s: %v", n, key, err)
		}
		out = append(out, configEntry{key: key, value: val, line: n})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func configValue(v string) (string, error) {
	if strings.HasPrefix(v, `"`) {
		q, err := strconv.QuotedPrefix(v)
		if err != nil {
			return "", fmt.Errorf("unterminated or bad quoted value")
		}
		rest := strings.TrimSpace(v[len(q):])
		if rest != "" && !strings.HasPrefix(rest, "#") {
			return "", fmt.Errorf("text after the quoted value")
		}
		return strconv.Unquote(q)
	}
	for i := 1; i < len(v); i++ {
		if v[i] == '#' && (v[i-1] == ' ' || v[i-1] == '\t') {
			return strings.TrimSpace(v[:i]), nil
		}
	}
	return v, nil
}

// applyConfigFile sets the flags of fs that the command line left unset (and
// no environment variable overrides) from the file at path. It returns
// warnings to log once logging is set up.
func applyConfigFile(fs *flag.FlagSet, path string, getenv func(string) string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, usageErr("--config: %v", err)
	}
	entries, err := parseConfig(string(data))
	if err != nil {
		return nil, usageErr("%s:%v", path, err)
	}
	onCommandLine := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { onCommandLine[f.Name] = true })
	seen := map[string]int{}
	var warnings []string
	for _, e := range entries {
		if fs.Lookup(e.key) == nil || e.key == "config" || e.key == "check" {
			return nil, usageErr("%s:%d: unknown key %q (keys are server flag names without --)", path, e.line, e.key)
		}
		if first, dup := seen[e.key]; dup && !repeatable(e.key) {
			return nil, usageErr("%s:%d: %s already set on line %d", path, e.line, e.key, first)
		}
		if _, dup := seen[e.key]; !dup {
			seen[e.key] = e.line
		}
		if onCommandLine[e.key] {
			continue // a --sink on the command line replaces the file's sinks
		}
		if env := envShadow(e.key); env != "" && getenv(env) != "" {
			continue
		}
		if err := fs.Set(e.key, e.value); err != nil {
			return nil, usageErr("%s:%d: %s: %v", path, e.line, e.key, err)
		}
		if e.key == "pg-url" && hasPassword(e.value) && worldReadable(path) {
			warnings = append(warnings, path+" holds a Postgres password and other users can read it; chmod 600 it")
		}
	}
	return warnings, nil
}

func hasPassword(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	if _, ok := u.User.Password(); ok {
		return true
	}
	return u.Query().Get("password") != ""
}

func worldReadable(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().Perm()&0o004 != 0
}
