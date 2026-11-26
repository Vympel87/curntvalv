package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// chQuery runs a query against ClickHouse's HTTP interface.
func chQuery(t *testing.T, base, q string) string {
	t.Helper()
	resp, err := http.Post(base+"/?query="+url.QueryEscape(q), "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", q, b)
	}
	return strings.TrimSpace(string(b))
}

// TestEndToEndClickHouseMirror: a mirror table equals the Postgres table
// after a backfill, inserts, updates (one leaving a large TOASTed value
// unchanged), deletes and key changes, across a leader kill.
func TestEndToEndClickHouseMirror(t *testing.T) {
	chURL := os.Getenv("CURNTVALV_TEST_CH_URL")
	if chURL == "" {
		t.Skip("CURNTVALV_TEST_CH_URL not set; run `make test-ext`")
	}
	f := newPGFixture(t)
	f.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN amount numeric(10,2), ADD COLUMN at timestamptz, ADD COLUMN doc text", f.table))
	// Existing rows, one with a value large enough to be TOASTed.
	f.exec(fmt.Sprintf("INSERT INTO %s SELECT g, g * 1.5, '2026-11-25 10:00:00+07', 'small' FROM generate_series(1, 200) g", f.table))
	f.exec(fmt.Sprintf("UPDATE %s SET doc = (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300)) WHERE id = 7", f.table))
	u, _ := url.Parse(chURL)
	base := u.String()
	prefix := "e2e" + strings.TrimPrefix(f.table, "rows_") + "_"
	c := newE2EWith(t, f.env(), append(f.args(),
		"--sink", "name=mirror,type=clickhouse,url="+chURL+",mode=mirror,into="+prefix+"{table}")...)
	mirror := "default.`" + prefix + f.table + "`"
	t.Cleanup(func() { http.Post(base+"/?query="+url.QueryEscape("DROP TABLE IF EXISTS "+mirror), "text/plain", nil) })
	f.waitSlot()
	c.mustCLI("backfill", "start", f.table)

	f.insert(201, 300, 10)                                                    // amount and doc NULL
	f.exec(fmt.Sprintf("UPDATE %s SET amount = 99.99 WHERE id = 7", f.table)) // doc stays TOASTed
	c.kill(c.leader())
	f.exec(fmt.Sprintf("UPDATE %s SET doc = 'changed' WHERE id %% 10 = 3", f.table))
	f.exec(fmt.Sprintf("DELETE FROM %s WHERE id %% 9 = 0", f.table))
	f.exec(fmt.Sprintf("UPDATE %s SET id = id + 1000 WHERE id BETWEEN 50 AND 55", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (-1)", f.table))
	waitE2E(t, "backfill done", 60*time.Second, func() bool { s, _ := c.backfillStatus(f.table); return s == "done" })

	// The Postgres table, in the form ClickHouse prints it.
	var want []string
	rows, err := f.db.Query(t.Context(), fmt.Sprintf(`SELECT id, coalesce(amount::text, '\N'),
		coalesce(to_char(at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'), coalesce(md5(doc), '\N') FROM %s`, f.table))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var amount, at, doc string
		rows.Scan(&id, &amount, &at, &doc)
		want = append(want, fmt.Sprintf("%d\t%s\t%s\t%s", id, amount, at, doc))
	}
	rows.Close()
	sort.Strings(want)
	q := "SELECT id, ifNull(toDecimalString(amount, 2), '\\\\N'), ifNull(toString(at), '\\\\N'), ifNull(lower(hex(MD5(doc))), '\\\\N') FROM " +
		mirror + " FINAL FORMAT TSVRaw"
	var got []string
	deadline := time.Now().Add(90 * time.Second)
	for {
		if chQuery(t, base, "EXISTS TABLE "+mirror) == "1" {
			got = strings.Split(chQuery(t, base, q), "\n")
			sort.Strings(got)
			if fmt.Sprint(got) == fmt.Sprint(want) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("mirror differs from the table:\n%s", diffLines(want, got))
		}
		time.Sleep(500 * time.Millisecond)
	}
	if n := chQuery(t, base, "SELECT count() FROM "+mirror+" FINAL WHERE id = 7 AND amount = 99.99 AND length(doc) = 9600"); n != "1" {
		t.Fatalf("row 7 lost its TOASTed value: %s", n)
	}
}

// diffLines lists lines only in want (-) or only in got (+), at most 20.
func diffLines(want, got []string) string {
	in := func(l []string) map[string]bool {
		m := map[string]bool{}
		for _, s := range l {
			m[s] = true
		}
		return m
	}
	w, g := in(want), in(got)
	var out []string
	for _, s := range want {
		if !g[s] {
			out = append(out, "- "+s)
		}
	}
	for _, s := range got {
		if !w[s] {
			out = append(out, "+ "+s)
		}
	}
	if len(out) > 20 {
		out = append(out[:20], fmt.Sprintf("... %d more", len(out)-20))
	}
	return strings.Join(out, "\n")
}
