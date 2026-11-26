package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// S3 archives changes in an S3-compatible object store (design D32): each
// delivery writes, per table and hour of commit time, one gzipped JSON Lines
// object of the shared JSON form,
//
//	PREFIX/NAMESPACE.TABLE/dt=2026-11-26/hour=14/000000000123-0_000000000140-3.jsonl.gz
//
// named after its first and last change (log index and position), so
// objects sort in log order. A redelivery cut the same way rewrites the
// same objects; after a leader change it may be cut differently and
// objects can overlap, so readers de-duplicate on "id". Requests are signed
// with AWS Signature Version 4.
type S3 struct {
	bucket, prefix string
	endpoint       *url.URL // path-style; nil for AWS's virtual-hosted style
	region         string
	creds          S3Credentials
	shown          string
	client         *http.Client
	now            func() time.Time
}

// S3Credentials are an access key, its secret and, for temporary
// credentials, a session token.
type S3Credentials struct {
	AccessKey, SecretKey, SessionToken string
}

// S3Config configures an S3 sink.
type S3Config struct {
	// URL is s3://[ACCESS_KEY:SECRET_KEY@]BUCKET[/PREFIX].
	URL string
	// Endpoint is the store's base URL for S3-compatible stores (MinIO,
	// SeaweedFS, R2, ...), addressed path-style. Empty means AWS:
	// https://BUCKET.s3.REGION.amazonaws.com.
	Endpoint string
	// Region signs the requests (default us-east-1).
	Region string
	// Creds are used when the URL has none.
	Creds   S3Credentials
	Timeout time.Duration
}

// NewS3 returns an S3 sink. It does no I/O.
func NewS3(cfg S3Config) (*S3, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return nil, errors.New("s3: bad url (want s3://[ACCESS_KEY:SECRET_KEY@]BUCKET[/PREFIX])")
	}
	s := &S3{bucket: u.Host, prefix: strings.Trim(u.Path, "/"), region: cfg.Region, creds: cfg.Creds, now: time.Now}
	if u.User != nil {
		s.creds.AccessKey = u.User.Username()
		s.creds.SecretKey, _ = u.User.Password()
		s.creds.SessionToken = ""
	}
	if s.creds.AccessKey == "" || s.creds.SecretKey == "" {
		return nil, errors.New("s3: no credentials (in the url, or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)")
	}
	if s.region == "" {
		s.region = "us-east-1"
	}
	if cfg.Endpoint != "" {
		e, err := url.Parse(cfg.Endpoint)
		if err != nil || (e.Scheme != "http" && e.Scheme != "https") || e.Host == "" {
			return nil, errors.New("s3: bad endpoint (want http(s)://HOST[:PORT])")
		}
		s.endpoint = e
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = time.Minute
	}
	s.client = &http.Client{Timeout: cfg.Timeout}
	s.shown = "s3://" + s.bucket + "/" + s.prefix
	return s, nil
}

// objectGroup is the changes of one object.
type objectGroup struct {
	key     string
	changes []event.Change
}

// objects groups changes by table and hour, in order of first appearance.
func (s *S3) objects(changes []event.Change) []objectGroup {
	var out []objectGroup
	where := map[string]int{}
	for _, c := range changes {
		table := c.Table()
		if c.IsSourceReset() {
			table = "_source_reset"
		}
		t := c.CommitTime()
		dir := fmt.Sprintf("%s/dt=%s/hour=%02d", table, t.Format("2006-01-02"), t.Hour())
		if s.prefix != "" {
			dir = s.prefix + "/" + dir
		}
		i, ok := where[dir]
		if !ok {
			i = len(out)
			where[dir] = i
			out = append(out, objectGroup{key: dir})
		}
		out[i].changes = append(out[i].changes, c)
	}
	for i := range out {
		cs := out[i].changes
		out[i].key += fmt.Sprintf("/%012d-%d_%012d-%d.jsonl.gz", cs[0].Index, cs[0].Seq, cs[len(cs)-1].Index, cs[len(cs)-1].Seq)
	}
	return out
}

// Deliver writes the objects of changes and returns once all are stored.
func (s *S3) Deliver(ctx context.Context, changes []event.Change) error {
	for _, g := range s.objects(changes) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		enc := json.NewEncoder(zw)
		for _, c := range g.changes {
			if err := enc.Encode(c.JSON()); err != nil {
				return fmt.Errorf("s3: encode: %w", err)
			}
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("s3: compress: %w", err)
		}
		if _, err := s.do(ctx, http.MethodPut, g.key, nil, buf.Bytes(), "application/gzip"); err != nil {
			return err
		}
	}
	return nil
}

// do sends one signed request for key in the bucket ("" for the bucket
// itself) and returns the response body.
func (s *S3) do(ctx context.Context, method, key string, query url.Values, body []byte, contentType string) ([]byte, error) {
	u := s.objectURL(key)
	u.RawQuery = canonicalQuery(query)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	s.sign(req, body, s.now())
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3: %s %s: %w", method, s.shown, redactErr(err))
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("s3: read response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		msg := bytes.TrimSpace(out)
		if len(msg) > 512 {
			msg = msg[:512]
		}
		return nil, fmt.Errorf("s3: %s %s/%s: %s: %s", method, s.shown, key, resp.Status, msg)
	}
	return out, nil
}

// objectURL addresses key path-style at the endpoint, or virtual-hosted at
// AWS.
func (s *S3) objectURL(key string) *url.URL {
	if s.endpoint != nil {
		u := *s.endpoint
		u.Path = strings.TrimSuffix(u.Path, "/") + "/" + s.bucket
		if key != "" {
			u.Path += "/" + key
		}
		u.RawPath = uriEncode(u.Path)
		return &u
	}
	u := &url.URL{Scheme: "https", Host: s.bucket + ".s3." + s.region + ".amazonaws.com", Path: "/" + key}
	u.RawPath = uriEncode(u.Path)
	return u
}

// sign adds AWS Signature Version 4 headers to req.
func (s *S3) sign(req *http.Request, body []byte, now time.Time) {
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	amzDate := now.UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	if s.creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.creds.SessionToken)
	}
	req.Header.Set("Authorization", sigV4(req, payload, amzDate, s.region, s.creds))
}

// sigV4 returns the Authorization header of req, signing the host and every
// x-amz-* header, and range.
func sigV4(req *http.Request, payload, amzDate, region string, creds S3Credentials) string {
	headers := map[string]string{"host": req.URL.Host}
	for k, v := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") || lk == "range" {
			headers[lk] = strings.TrimSpace(strings.Join(v, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	slices.Sort(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signed := strings.Join(names, ";")
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{req.Method, path, canonicalQuery(req.URL.Query()), canonHeaders.String(), signed, payload}, "\n")
	date := amzDate[:8]
	scope := date + "/" + region + "/s3/aws4_request"
	h := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	key := []byte("AWS4" + creds.SecretKey)
	for _, part := range []string{date, region, "s3", "aws4_request"} {
		key = hmacSHA256(key, part)
	}
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	return "AWS4-HMAC-SHA256 Credential=" + creds.AccessKey + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + sig
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// canonicalQuery encodes a query the way SigV4 wants it: sorted by key,
// every byte but the unreserved ones percent-encoded.
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var parts []string
	for _, k := range keys {
		vs := slices.Clone(q[k])
		slices.Sort(vs)
		for _, v := range vs {
			parts = append(parts, sigEscape(k, false)+"="+sigEscape(v, false))
		}
	}
	return strings.Join(parts, "&")
}

// uriEncode percent-encodes a path as SigV4 does, keeping "/".
func uriEncode(path string) string { return sigEscape(path, true) }

func sigEscape(s string, keepSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
