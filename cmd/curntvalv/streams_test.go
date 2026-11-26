package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// redisXLen returns the length of a Redis stream (a minimal RESP client).
func redisXLen(t *testing.T, rawURL, key string) int {
	t.Helper()
	u, _ := url.Parse(rawURL)
	c, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "*2\r\n$4\r\nXLEN\r\n$%d\r\n%s\r\n", len(key), key)
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, ":") {
		t.Fatalf("XLEN: %q %v", line, err)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	return n
}

// natsStreamMessages returns the number of messages in a JetStream stream
// (a minimal NATS client).
func natsStreamMessages(t *testing.T, rawURL, stream string) int {
	t.Helper()
	u, _ := url.Parse(rawURL)
	c, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(c)
	r.ReadString('\n') // INFO
	fmt.Fprintf(c, "CONNECT {\"verbose\":false}\r\nSUB _INBOX.e2e 1\r\nPUB $JS.API.STREAM.INFO.%s _INBOX.e2e 0\r\n\r\n", stream)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "PING") {
			fmt.Fprint(c, "PONG\r\n")
			continue
		}
		if !strings.HasPrefix(line, "MSG ") {
			continue
		}
		f := strings.Fields(line)
		n, _ := strconv.Atoi(f[len(f)-1])
		b := make([]byte, n+2)
		io.ReadFull(r, b)
		var info struct {
			State struct {
				Messages int `json:"messages"`
			} `json:"state"`
		}
		json.Unmarshal(b[:n], &info)
		return info.State.Messages
	}
}

// meiliDocs returns "id:note" of every document in an index.
func meiliDocs(t *testing.T, rawURL, index string) []string {
	t.Helper()
	u, _ := url.Parse(rawURL)
	key, _ := u.User.Password()
	u.User = nil
	req, _ := http.NewRequest(http.MethodGet, u.String()+"/indexes/"+index+"/documents?limit=10000", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Results []map[string]any `json:"results"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	var out []string
	for _, d := range body.Results {
		out = append(out, fmt.Sprintf("%v:%v", d["id"], d["note"]))
	}
	sort.Strings(out)
	return out
}

// TestEndToEndStreamSinks: inserts, updates, deletes and a backfill reach
// NATS JetStream, Redis streams and Meilisearch across a leader kill: each
// change once in the stream and the queue, and the search index equal to
// the table.
func TestEndToEndStreamSinks(t *testing.T) {
	natsURL, redisURL, meiliURL := os.Getenv("CURNTVALV_TEST_NATS_URL"), os.Getenv("CURNTVALV_TEST_REDIS_URL"), os.Getenv("CURNTVALV_TEST_MEILI_URL")
	if natsURL == "" || redisURL == "" || meiliURL == "" {
		t.Skip("CURNTVALV_TEST_NATS_URL, _REDIS_URL and _MEILI_URL not set; run `make test-ext`")
	}
	f := newPGFixture(t)
	f.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN note text", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'old' FROM generate_series(1, 300) g", f.table)) // before the cluster
	name := strings.ReplaceAll(f.table, "_", "")
	stream := "e2e-" + name
	c := newE2EWith(t, f.env(), append(f.args(),
		"--sink", "name=queue,type=nats,url="+natsURL+",subject=e2e."+name+",stream="+stream,
		"--sink", "name=log,type=redis,url="+redisURL+",stream="+stream+":{table}",
		"--sink", "name=search,type=meilisearch,url="+meiliURL+",index="+name+"_{table}")...)
	sub := c.subscribe("--table", f.table)
	f.waitSlot()
	c.mustCLI("backfill", "start", f.table)
	for i := 301; i <= 600; i += 10 {
		f.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'new' FROM generate_series(%d, %d) g", f.table, i, i+9))
		if i == 451 {
			c.kill(c.leader()) // redelivery to every sink
		}
	}
	f.exec(fmt.Sprintf("UPDATE %s SET note = 'upd' WHERE id %% 7 = 0", f.table))
	f.exec(fmt.Sprintf("DELETE FROM %s WHERE id %% 5 = 0", f.table))
	f.exec(fmt.Sprintf("UPDATE %s SET id = id + 1000 WHERE id BETWEEN 590 AND 594", f.table))
	f.exec(fmt.Sprintf("INSERT INTO %s VALUES (-1, 'end')", f.table))

	waitE2E(t, "subscriber has the end row", 90*time.Second, func() bool { return strings.Contains(sub.out.String(), `"id":"-1"`) })
	waitE2E(t, "backfill done", 60*time.Second, func() bool { s, _ := c.backfillStatus(f.table); return s == "done" })
	time.Sleep(500 * time.Millisecond)
	events := strings.Count(sub.out.String(), `"op":`)

	var want []string
	rows, err := f.db.Query(t.Context(), fmt.Sprintf("SELECT id, note FROM %s", f.table))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var note string
		rows.Scan(&id, &note)
		want = append(want, fmt.Sprintf("%d:%s", id, note))
	}
	rows.Close()
	sort.Strings(want)

	key := stream + ":public." + f.table
	waitE2E(t, "every sink caught up", 90*time.Second, func() bool {
		return redisXLen(t, redisURL, key) >= events && natsStreamMessages(t, natsURL, stream) >= events &&
			fmt.Sprint(meiliDocs(t, meiliURL, name+"_"+f.table)) == fmt.Sprint(want)
	})
	time.Sleep(time.Second) // a duplicate would show up now
	if n := redisXLen(t, redisURL, key); n != events {
		t.Errorf("redis stream has %d entries for %d changes", n, events)
	}
	if n := natsStreamMessages(t, natsURL, stream); n != events {
		t.Errorf("jetstream has %d messages for %d changes", n, events)
	}
	if got := meiliDocs(t, meiliURL, name+"_"+f.table); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("meilisearch has %d documents, the table %d rows", len(got), len(want))
	}
}
