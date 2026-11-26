package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestEndToEndArchive: an s3 sink with credentials from the environment
// archives every change (its contents are checked in internal/sink).
func TestEndToEndArchive(t *testing.T) {
	endpoint := os.Getenv("CURNTVALV_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CURNTVALV_TEST_S3_ENDPOINT not set; run `make test-ext`")
	}
	f := newPGFixture(t)
	env := append(f.env(), "AWS_ACCESS_KEY_ID=curntvalv", "AWS_SECRET_ACCESS_KEY=curntvalv-secret")
	c := newE2EWith(t, env, append(f.args(), "--sink",
		"name=archive,type=s3,url=s3://curntvalv-test/"+f.table+",endpoint="+endpoint+",linger=1s,max_rate=500")...)
	f.waitSlot()
	f.insert(1, 300, 50)
	var line string
	waitE2E(t, "archive caught up", 60*time.Second, func() bool {
		out, _ := c.cli("status")
		for _, l := range strings.Split(out, "\n") {
			if fl := strings.Fields(l); len(fl) > 6 && fl[0] == "archive" {
				line = l
				n, _ := strconv.Atoi(fl[6]) // delivered
				return fl[3] == "running" && fl[5] == "0" && n >= 300
			}
		}
		return false
	})
	if strings.Contains(line, "retrying") {
		t.Fatalf("archive: %s", line)
	}
}
