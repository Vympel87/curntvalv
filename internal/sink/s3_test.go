package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// TestSigV4Example checks the signer against the GET Object example of the
// AWS Signature Version 4 documentation ("Signature Calculations for the
// Authorization Header: Transferring Payload in a Single Chunk").
func TestSigV4Example(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	empty := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("X-Amz-Content-Sha256", empty)
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	got := sigV4(req, empty, "20130524T000000Z", "us-east-1",
		S3Credentials{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestS3Config(t *testing.T) {
	s, err := NewS3(S3Config{URL: "s3://ak:sk@archive/cdc/v1/", Endpoint: "http://minio:9000"})
	if err != nil {
		t.Fatal(err)
	}
	if s.bucket != "archive" || s.prefix != "cdc/v1" || s.region != "us-east-1" || strings.Contains(s.shown, "sk") {
		t.Fatalf("%+v", s)
	}
	if u := s.objectURL("a b/c.gz"); u.String() != "http://minio:9000/archive/a%20b/c.gz" {
		t.Fatalf("url %s", u)
	}
	aws, _ := NewS3(S3Config{URL: "s3://archive", Region: "eu-west-1", Creds: S3Credentials{AccessKey: "a", SecretKey: "b"}})
	if u := aws.objectURL("x/y"); u.String() != "https://archive.s3.eu-west-1.amazonaws.com/x/y" {
		t.Fatalf("aws url %s", u)
	}
	for _, bad := range []S3Config{{URL: "s3://bucket"}, {URL: "http://ak:sk@b"}, {URL: "s3://ak:sk@b", Endpoint: "minio:9000"}} {
		if _, err := NewS3(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
	// Objects per table and hour, named after their first and last change.
	cs := changesOf(123, op{op: pb.Op_OP_INSERT, id: "1"}, op{op: pb.Op_OP_INSERT, id: "2"})
	cs[1].CommitTimeUs = cs[0].CommitTimeUs + int64(time.Hour/time.Microsecond)
	cs = append(cs, event.Change{Index: 130, Seq: 0, Event: &pb.Event{Op: pb.Op_OP_SOURCE_RESET}, CommitTimeUs: cs[0].CommitTimeUs})
	var keys []string
	for _, g := range s.objects(cs) {
		keys = append(keys, g.key)
	}
	h0 := cs[0].CommitTime()
	h1 := cs[1].CommitTime()
	want := []string{
		fmt.Sprintf("cdc/v1/public.orders/dt=%s/hour=%02d/000000000123-0_000000000123-0.jsonl.gz", h0.Format("2006-01-02"), h0.Hour()),
		fmt.Sprintf("cdc/v1/public.orders/dt=%s/hour=%02d/000000000123-1_000000000123-1.jsonl.gz", h1.Format("2006-01-02"), h1.Hour()),
		fmt.Sprintf("cdc/v1/_source_reset/dt=%s/hour=%02d/000000000130-0_000000000130-0.jsonl.gz", h0.Format("2006-01-02"), h0.Hour()),
	}
	if strings.Join(keys, "\n") != strings.Join(want, "\n") {
		t.Fatalf("keys\n%s\nwant\n%s", strings.Join(keys, "\n"), strings.Join(want, "\n"))
	}
}

type listResult struct {
	Contents []struct {
		Key string
	}
}

func TestS3Real(t *testing.T) {
	endpoint := envOrSkip(t, "CURNTVALV_TEST_S3_ENDPOINT")
	ctx := context.Background()
	bucket := strings.ReplaceAll(uniqueName(t), "_", "-")
	s, err := NewS3(S3Config{URL: "s3://curntvalv:curntvalv-secret@" + bucket + "/cdc", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.do(ctx, http.MethodPut, "", nil, nil, ""); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	var ops []op
	for i := range 25 {
		ops = append(ops, op{op: pb.Op_OP_INSERT, id: fmt.Sprint(i), note: "n" + fmt.Sprint(i)})
	}
	batch := changesOf(10, ops...)
	for range 2 { // a redelivery cut the same way rewrites the same object
		if err := s.Deliver(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	out, err := s.do(ctx, http.MethodGet, "", url.Values{"list-type": {"2"}, "prefix": {"cdc/"}}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var list listResult
	first, last := batch[0], batch[len(batch)-1]
	name := fmt.Sprintf("%012d-%d_%012d-%d.jsonl.gz", first.Index, first.Seq, last.Index, last.Seq)
	if err := xml.Unmarshal(out, &list); err != nil || len(list.Contents) != 1 || !strings.HasSuffix(list.Contents[0].Key, name) {
		t.Fatalf("objects %+v %v\n%s", list, err, out)
	}
	obj, err := s.do(ctx, http.MethodGet, list.Contents[0].Key, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(obj))
	if err != nil {
		t.Fatal(err)
	}
	lines, _ := io.ReadAll(zr)
	var n int
	for _, line := range strings.Split(strings.TrimSpace(string(lines)), "\n") {
		var j event.JSON
		if err := json.Unmarshal([]byte(line), &j); err != nil || j.ID != batch[n].ID() || j.After["note"] != "n"+fmt.Sprint(n) {
			t.Fatalf("line %d: %s %v", n, line, err)
		}
		n++
	}
	if n != 25 {
		t.Fatalf("%d lines", n)
	}
	// Wrong credentials are refused, not ignored.
	bad, _ := NewS3(S3Config{URL: "s3://curntvalv:wrong@" + bucket, Endpoint: endpoint})
	if err := bad.Deliver(ctx, batch[:1]); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("wrong secret: %v", err)
	}
}
