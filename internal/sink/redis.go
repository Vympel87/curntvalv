package sink

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// Redis appends changes to Redis streams with XADD (design D19), one entry
// per change with the fields id, index, table, op and json (the shared JSON
// form). The stream key is Stream with "{table}" replaced by the change's
// table; a source reset goes to "{table}" = "_source_reset".
//
// The entry ID is "<log index>-<seq>", not an automatic one: it grows with
// the log, and Redis refuses an ID that is not above the stream's last one.
// A redelivered change is therefore refused, and is taken as delivered once
// the entry is confirmed to be in the stream (or trimmed by MAXLEN). Every
// change is in its stream exactly once, as long as only this sink writes to
// it.
type Redis struct {
	addr     string // host:port
	shown    string // url without password
	tls      *tls.Config
	user     string
	password string
	db       int
	stream   string
	maxLen   int64
	timeout  time.Duration

	mu   sync.Mutex
	conn *respConn
}

// RedisConfig configures a Redis sink.
type RedisConfig struct {
	// URL is redis://[USER:PASSWORD@]HOST:PORT[/DB], or rediss:// for TLS.
	URL string
	// Stream is the stream key, may contain "{table}". Default "curntvalv".
	Stream string
	// MaxLen trims each stream to about this many entries (XADD MAXLEN ~).
	// 0 keeps everything.
	MaxLen  int64
	Timeout time.Duration
}

// NewRedis returns a Redis sink. It does no I/O.
func NewRedis(cfg RedisConfig) (*Redis, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
		return nil, errors.New("redis: bad url (want redis://[USER:PASSWORD@]HOST:PORT[/DB] or rediss://...)")
	}
	r := &Redis{addr: u.Host, shown: u.Redacted(), stream: cfg.Stream, maxLen: cfg.MaxLen, timeout: cfg.Timeout}
	if u.Port() == "" {
		r.addr = net.JoinHostPort(u.Hostname(), "6379")
	}
	if u.Scheme == "rediss" {
		r.tls = &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
	}
	if u.User != nil {
		r.user = u.User.Username()
		r.password, _ = u.User.Password()
	}
	if p := strings.Trim(u.Path, "/"); p != "" {
		if r.db, err = strconv.Atoi(p); err != nil || r.db < 0 {
			return nil, fmt.Errorf("redis: bad database %q in url", p)
		}
	}
	if r.stream == "" {
		r.stream = "curntvalv"
	}
	if r.maxLen < 0 {
		return nil, errors.New("redis: maxlen must not be negative")
	}
	if r.timeout <= 0 {
		r.timeout = 10 * time.Second
	}
	return r, nil
}

// streamKey returns the stream of a change.
func (r *Redis) streamKey(c event.Change) string {
	t := c.Table()
	if c.IsSourceReset() {
		t = "_source_reset"
	}
	return strings.ReplaceAll(r.stream, "{table}", t)
}

// entryID is the stream entry ID of a change.
func entryID(c event.Change) string { return fmt.Sprintf("%d-%d", c.Index, c.Seq) }

// isOldID reports Redis's answer to an XADD ID that is not above the
// stream's last entry.
func isOldID(err error) bool {
	var re redisError
	return errors.As(err, &re) && strings.Contains(string(re), "equal or smaller than the target stream top item")
}

// Deliver appends all changes in one pipelined round trip.
func (r *Redis) Deliver(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.deliver(ctx, changes)
	var re redisError
	if err != nil && !errors.As(err, &re) && r.conn != nil {
		// A broken connection: start over next time.
		r.conn.close()
		r.conn = nil
	}
	return err
}

func (r *Redis) deliver(ctx context.Context, changes []event.Change) error {
	c, err := r.connection(ctx)
	if err != nil {
		return err
	}
	c.deadline(ctx, r.timeout)
	for _, ch := range changes {
		b, err := json.Marshal(ch.JSON())
		if err != nil {
			return fmt.Errorf("redis: encode: %w", err)
		}
		args := []string{"XADD", r.streamKey(ch)}
		if r.maxLen > 0 {
			args = append(args, "MAXLEN", "~", strconv.FormatInt(r.maxLen, 10))
		}
		args = append(args, entryID(ch), "id", ch.ID(), "index", strconv.FormatUint(ch.Index, 10),
			"table", ch.Table(), "op", ch.Op(), "json", string(b))
		c.command(args...)
	}
	replies, err := c.roundTrip(len(changes))
	if err != nil {
		return err
	}
	// Changes refused as old were delivered before, unless the stream holds
	// entries of another writer beyond them.
	var old []event.Change
	for i, rep := range replies {
		switch {
		case rep.err == nil:
		case isOldID(rep.err):
			old = append(old, changes[i])
		default:
			return fmt.Errorf("redis: XADD to %s: %w", r.streamKey(changes[i]), rep.err)
		}
	}
	if len(old) == 0 {
		return nil
	}
	for _, ch := range old {
		c.command("XRANGE", r.streamKey(ch), entryID(ch), entryID(ch))
		c.command("XRANGE", r.streamKey(ch), "-", "+", "COUNT", "1")
	}
	replies, err = c.roundTrip(2 * len(old))
	if err != nil {
		return err
	}
	for i, ch := range old {
		found, first := replies[2*i], replies[2*i+1]
		if found.err != nil || first.err != nil {
			return fmt.Errorf("redis: XRANGE %s: %w", r.streamKey(ch), errors.Join(found.err, first.err))
		}
		if len(found.array) > 0 {
			continue // delivered before
		}
		// Missing: fine if MAXLEN trimmed it, i.e. it is older than the
		// stream's first entry; otherwise another writer is ahead of us.
		if r.maxLen == 0 || len(first.array) == 0 || !idBefore(entryID(ch), firstID(first)) {
			return fmt.Errorf("redis: stream %s has entries with IDs above %s that this sink did not write; "+
				"give the sink its own stream (stream=...)", r.streamKey(ch), entryID(ch))
		}
	}
	return nil
}

// firstID returns the ID of the entry in an XRANGE ... COUNT 1 answer.
func firstID(v respValue) string {
	if len(v.array) == 0 || len(v.array[0].array) == 0 {
		return ""
	}
	return v.array[0].array[0].str
}

// idBefore reports whether stream ID a is below b.
func idBefore(a, b string) bool {
	parse := func(s string) (uint64, uint64) {
		ms, seq, _ := strings.Cut(s, "-")
		x, _ := strconv.ParseUint(ms, 10, 64)
		y, _ := strconv.ParseUint(seq, 10, 64)
		return x, y
	}
	am, as := parse(a)
	bm, bs := parse(b)
	return am < bm || (am == bm && as < bs)
}

// connection returns the open connection or dials a new one and logs in.
func (r *Redis) connection(ctx context.Context) (*respConn, error) {
	if r.conn != nil {
		return r.conn, nil
	}
	d := net.Dialer{Timeout: r.timeout}
	nc, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, fmt.Errorf("redis: connect to %s: %w", r.shown, err)
	}
	if r.tls != nil {
		tc := tls.Client(nc, r.tls)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("redis: TLS with %s: %w", r.shown, err)
		}
		nc = tc
	}
	c := newRespConn(nc)
	c.deadline(ctx, r.timeout)
	n := 0
	if r.password != "" || r.user != "" {
		user := r.user
		if user == "" {
			user = "default"
		}
		c.command("AUTH", user, r.password)
		n++
	}
	if r.db != 0 {
		c.command("SELECT", strconv.Itoa(r.db))
		n++
	}
	c.command("PING")
	replies, err := c.roundTrip(n + 1)
	if err == nil {
		for _, rep := range replies {
			if rep.err != nil {
				err = rep.err
				break
			}
		}
	}
	if err != nil {
		c.close()
		return nil, fmt.Errorf("redis: log in to %s: %w", r.shown, err)
	}
	r.conn = c
	return c, nil
}

// Close closes the connection.
func (r *Redis) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		r.conn.close()
		r.conn = nil
	}
	return nil
}

// --- RESP (Redis serialization protocol, version 2) -----------------------------

// redisError is an error reply ("-ERR ...").
type redisError string

func (e redisError) Error() string { return string(e) }

// respValue is one reply: a simple or bulk string, an integer, an array, or
// an error reply (err). A nil bulk string or array has null set.
type respValue struct {
	str   string
	num   int64
	array []respValue
	null  bool
	err   error
}

type respConn struct {
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
}

func newRespConn(nc net.Conn) *respConn {
	return &respConn{nc: nc, r: bufio.NewReader(nc), w: bufio.NewWriter(nc)}
}

func (c *respConn) deadline(ctx context.Context, timeout time.Duration) {
	d := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(d) {
		d = dl
	}
	c.nc.SetDeadline(d)
}

func (c *respConn) close() { c.nc.Close() }

// command buffers one command.
func (c *respConn) command(args ...string) {
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
}

// roundTrip sends the buffered commands and reads n replies. An error
// reply is a reply, not an error; err means the connection is broken.
func (c *respConn) roundTrip(n int) ([]respValue, error) {
	if err := c.w.Flush(); err != nil {
		return nil, fmt.Errorf("redis: send: %w", err)
	}
	out := make([]respValue, n)
	for i := range out {
		v, err := readResp(c.r)
		if err != nil {
			return nil, fmt.Errorf("redis: read reply: %w", err)
		}
		out[i] = v
	}
	return out, nil
}

// readResp reads one RESP2 value.
func readResp(r *bufio.Reader) (respValue, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return respValue{}, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return respValue{}, fmt.Errorf("bad reply line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return respValue{str: body}, nil
	case '-':
		return respValue{err: redisError(body)}, nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return respValue{}, fmt.Errorf("bad integer %q", body)
		}
		return respValue{num: n}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > 512<<20 {
			return respValue{}, fmt.Errorf("bad bulk length %q", body)
		}
		if n == -1 {
			return respValue{null: true}, nil
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return respValue{}, err
		}
		return respValue{str: string(b[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > 1<<24 {
			return respValue{}, fmt.Errorf("bad array length %q", body)
		}
		if n == -1 {
			return respValue{null: true}, nil
		}
		v := respValue{array: make([]respValue, n)}
		for i := range v.array {
			if v.array[i], err = readResp(r); err != nil {
				return respValue{}, err
			}
		}
		return v, nil
	}
	return respValue{}, fmt.Errorf("unknown reply type %q", line[0])
}
