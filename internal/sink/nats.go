package sink

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
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

// NATS publishes changes to a NATS JetStream stream (design D19), one
// message per change: the shared JSON form, on the subject
// "<Subject>.<namespace>.<table>" ("<Subject>.source_reset" for a source
// reset), with the header Nats-Msg-Id set to the change's id. JetStream
// drops a message whose id it saw within the stream's duplicate window
// (2 minutes by default), so a change redelivered after a retry or a leader
// change is stored once. A change is delivered when JetStream acknowledged
// it. The stream is created if it does not exist, capturing "<Subject>.>".
//
// It speaks the NATS client protocol with the standard library.
type NATS struct {
	addr    string // host:port
	shown   string
	tls     bool // tls:// URL: always TLS
	user    string
	pass    string
	token   string
	subject string
	stream  string
	timeout time.Duration

	mu       sync.Mutex
	conn     *natsConn
	lastUsed time.Time
	ready    bool // stream exists
}

// NATSConfig configures a NATS sink.
type NATSConfig struct {
	// URL is nats://[USER:PASSWORD@|TOKEN@]HOST:PORT, or tls://... for TLS.
	URL string
	// Subject is the subject prefix. Default "curntvalv".
	Subject string
	// Stream is the JetStream stream, created if missing. Default
	// "curntvalv".
	Stream  string
	Timeout time.Duration
}

// natsName reports whether s is a valid stream name or subject token.
func natsName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r == '_' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

// NewNATS returns a NATS sink. It does no I/O.
func NewNATS(cfg NATSConfig) (*NATS, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "nats" && u.Scheme != "tls") || u.Host == "" {
		return nil, errors.New("nats: bad url (want nats://[USER:PASSWORD@|TOKEN@]HOST:PORT or tls://...)")
	}
	n := &NATS{addr: u.Host, shown: u.Redacted(), tls: u.Scheme == "tls", subject: cfg.Subject, stream: cfg.Stream, timeout: cfg.Timeout}
	if u.Port() == "" {
		n.addr = net.JoinHostPort(u.Hostname(), "4222")
	}
	if u.User != nil {
		if p, ok := u.User.Password(); ok {
			n.user, n.pass = u.User.Username(), p
		} else {
			n.token = u.User.Username()
		}
	}
	if n.subject == "" {
		n.subject = "curntvalv"
	}
	for _, tok := range strings.Split(n.subject, ".") {
		if !natsName(tok) {
			return nil, fmt.Errorf("nats: bad subject prefix %q (tokens of letters, digits, _ and -)", n.subject)
		}
	}
	if n.stream == "" {
		n.stream = "curntvalv"
	}
	if !natsName(n.stream) {
		return nil, fmt.Errorf("nats: bad stream name %q (letters, digits, _ and -)", n.stream)
	}
	if n.timeout <= 0 {
		n.timeout = 10 * time.Second
	}
	return n, nil
}

// subjectToken makes a namespace or table name a valid subject token.
func subjectToken(s string) string {
	if s == "" {
		return "_"
	}
	b := []byte(s)
	for i, c := range b {
		if !(c == '_' || c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			b[i] = '_'
		}
	}
	return string(b)
}

// subjectOf returns the subject of a change.
func (n *NATS) subjectOf(c event.Change) string {
	if c.IsSourceReset() {
		return n.subject + ".source_reset"
	}
	if c.Schema == nil {
		return n.subject + "._unknown." + subjectToken(c.Table())
	}
	return n.subject + "." + subjectToken(c.Schema.GetNamespace()) + "." + subjectToken(c.Schema.GetName())
}

// Deliver publishes all changes, pipelined, and waits for JetStream's
// acknowledgements.
func (n *NATS) Deliver(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	err := n.deliver(ctx, changes)
	if err != nil && n.conn != nil {
		n.conn.close()
		n.conn = nil
	}
	n.lastUsed = time.Now()
	return err
}

func (n *NATS) deliver(ctx context.Context, changes []event.Change) error {
	// The server pings idle clients and drops those that do not answer; we
	// only read while delivering, so start over after a pause.
	if n.conn != nil && time.Since(n.lastUsed) > 30*time.Second {
		n.conn.close()
		n.conn = nil
	}
	c, err := n.connection(ctx)
	if err != nil {
		return err
	}
	c.deadline(ctx, n.timeout)
	if !n.ready {
		if err := n.ensureStream(c); err != nil {
			return err
		}
		n.ready = true
	}
	replies := make(map[string]int, len(changes)) // reply subject → change
	for i, ch := range changes {
		b, err := json.Marshal(ch.JSON())
		if err != nil {
			return fmt.Errorf("nats: encode: %w", err)
		}
		reply := c.inbox(i)
		replies[reply] = i
		hdr := "NATS/1.0\r\nNats-Msg-Id: " + ch.ID() + "\r\n\r\n"
		if c.maxPayload > 0 && len(hdr)+len(b) > c.maxPayload {
			return fmt.Errorf("nats: change %s is %d bytes, more than the server's max_payload %d", ch.ID(), len(hdr)+len(b), c.maxPayload)
		}
		fmt.Fprintf(c.w, "HPUB %s %s %d %d\r\n%s%s\r\n", n.subjectOf(ch), reply, len(hdr), len(hdr)+len(b), hdr, b)
	}
	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("nats: send: %w", err)
	}
	for len(replies) > 0 {
		m, err := c.next()
		if err != nil {
			return err
		}
		i, ok := replies[m.subject]
		if !ok {
			continue // an answer to an earlier, failed attempt
		}
		delete(replies, m.subject)
		if err := pubAckError(m); err != nil {
			return fmt.Errorf("nats: publish %s to %s: %w", changes[i].ID(), n.subjectOf(changes[i]), err)
		}
	}
	return nil
}

// pubAckError checks a JetStream publish acknowledgement.
func pubAckError(m natsMsg) error {
	if m.status == "503" {
		return errors.New("no JetStream stream takes this subject (or JetStream is off)")
	}
	if m.status != "" {
		return fmt.Errorf("status %s %s", m.status, m.statusText)
	}
	var ack struct {
		Stream string `json:"stream"`
		Error  *struct {
			Code        int    `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(m.data, &ack); err != nil {
		return fmt.Errorf("bad acknowledgement %q", m.data)
	}
	if ack.Error != nil {
		return fmt.Errorf("JetStream error %d: %s", ack.Error.Code, ack.Error.Description)
	}
	if ack.Stream == "" {
		return fmt.Errorf("bad acknowledgement %q", m.data)
	}
	return nil
}

// ensureStream creates the stream if it does not exist.
func (n *NATS) ensureStream(c *natsConn) error {
	info, err := c.request("$JS.API.STREAM.INFO."+n.stream, nil)
	if err != nil {
		return err
	}
	if info.status == "503" {
		return fmt.Errorf("nats: JetStream is not enabled on %s (start nats-server with -js)", n.shown)
	}
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(info.data, &resp); err != nil {
		return fmt.Errorf("nats: stream info: bad answer %q", info.data)
	}
	if resp.Error == nil {
		return nil
	}
	if resp.Error.Code != 404 {
		return fmt.Errorf("nats: stream info %s: %s", n.stream, resp.Error.Description)
	}
	cfg, _ := json.Marshal(map[string]any{"name": n.stream, "subjects": []string{n.subject + ".>"}, "storage": "file"})
	created, err := c.request("$JS.API.STREAM.CREATE."+n.stream, cfg)
	if err != nil {
		return err
	}
	resp.Error = nil
	if err := json.Unmarshal(created.data, &resp); err != nil {
		return fmt.Errorf("nats: create stream: bad answer %q", created.data)
	}
	if resp.Error != nil {
		return fmt.Errorf("nats: create stream %s for %s.>: %s", n.stream, n.subject, resp.Error.Description)
	}
	return nil
}

// connection returns the open connection or connects and logs in.
func (n *NATS) connection(ctx context.Context) (*natsConn, error) {
	if n.conn != nil {
		return n.conn, nil
	}
	d := net.Dialer{Timeout: n.timeout}
	nc, err := d.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return nil, fmt.Errorf("nats: connect to %s: %w", n.shown, err)
	}
	c, err := natsHandshake(ctx, nc, n)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: %s: %w", n.shown, err)
	}
	n.conn = c
	return c, nil
}

// Close closes the connection.
func (n *NATS) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn != nil {
		n.conn.close()
		n.conn = nil
	}
	return nil
}

// --- the NATS client protocol ----------------------------------------------------

type natsConn struct {
	nc         net.Conn
	r          *bufio.Reader
	w          *bufio.Writer
	prefix     string // inbox prefix, subscribed as prefix+"*"
	maxPayload int
	timeout    time.Duration
	seq        int // for request inboxes
}

// natsMsg is a message received on our inbox. status is set for a header
// status line such as "NATS/1.0 503".
type natsMsg struct {
	subject    string
	status     string
	statusText string
	data       []byte
}

func natsHandshake(ctx context.Context, nc net.Conn, n *NATS) (*natsConn, error) {
	c := &natsConn{nc: nc, r: bufio.NewReader(nc), timeout: n.timeout}
	c.deadline(ctx, n.timeout)
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read INFO: %w", err)
	}
	if !strings.HasPrefix(line, "INFO ") {
		return nil, fmt.Errorf("expected INFO, got %q", strings.TrimSpace(line))
	}
	var info struct {
		TLSRequired bool `json:"tls_required"`
		Headers     bool `json:"headers"`
		MaxPayload  int  `json:"max_payload"`
	}
	if err := json.Unmarshal([]byte(line[5:]), &info); err != nil {
		return nil, fmt.Errorf("bad INFO: %w", err)
	}
	if !info.Headers {
		return nil, errors.New("the server does not support headers (needs nats-server 2.2 or later)")
	}
	c.maxPayload = info.MaxPayload
	if n.tls || info.TLSRequired {
		host, _, _ := net.SplitHostPort(n.addr)
		tc := tls.Client(nc, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err := tc.HandshakeContext(ctx); err != nil {
			return nil, fmt.Errorf("TLS: %w", err)
		}
		c.nc, c.r = tc, bufio.NewReader(tc)
	}
	c.w = bufio.NewWriter(c.nc)
	connect, _ := json.Marshal(map[string]any{
		"verbose": false, "pedantic": false, "tls_required": n.tls || info.TLSRequired, "name": "curntvalv",
		"lang": "go", "version": "curntvalv", "protocol": 1, "headers": true, "no_responders": true,
		"user": n.user, "pass": n.pass, "auth_token": n.token,
	})
	b := make([]byte, 8)
	rand.Read(b)
	c.prefix = "_INBOX.curntvalv." + hex.EncodeToString(b) + "."
	fmt.Fprintf(c.w, "CONNECT %s\r\nSUB %s* 1\r\nPING\r\n", connect, c.prefix)
	if err := c.w.Flush(); err != nil {
		return nil, fmt.Errorf("send CONNECT: %w", err)
	}
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("log in: %w", err)
		}
		switch line = strings.TrimSpace(line); {
		case line == "PONG":
			return c, nil
		case strings.HasPrefix(line, "-ERR"):
			return nil, fmt.Errorf("log in: %s", line)
		}
	}
}

func (c *natsConn) deadline(ctx context.Context, timeout time.Duration) {
	d := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(d) {
		d = dl
	}
	c.nc.SetDeadline(d)
}

func (c *natsConn) close() { c.nc.Close() }

func (c *natsConn) inbox(i int) string { return c.prefix + strconv.Itoa(i) }

// request publishes data to subject and waits for the answer.
func (c *natsConn) request(subject string, data []byte) (natsMsg, error) {
	c.seq++
	reply := c.prefix + "r" + strconv.Itoa(c.seq)
	fmt.Fprintf(c.w, "PUB %s %s %d\r\n%s\r\n", subject, reply, len(data), data)
	if err := c.w.Flush(); err != nil {
		return natsMsg{}, fmt.Errorf("nats: send: %w", err)
	}
	for {
		m, err := c.next()
		if err != nil {
			return natsMsg{}, err
		}
		if m.subject == reply {
			return m, nil
		}
	}
}

// next reads until a message arrives, answering pings on the way.
func (c *natsConn) next() (natsMsg, error) {
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return natsMsg{}, fmt.Errorf("nats: read: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		op, rest, _ := strings.Cut(line, " ")
		switch strings.ToUpper(op) {
		case "PING":
			fmt.Fprint(c.w, "PONG\r\n")
			if err := c.w.Flush(); err != nil {
				return natsMsg{}, fmt.Errorf("nats: send: %w", err)
			}
		case "PONG", "+OK", "INFO":
		case "-ERR":
			return natsMsg{}, fmt.Errorf("nats: server error: %s", rest)
		case "MSG", "HMSG":
			return c.readMsg(strings.ToUpper(op) == "HMSG", strings.Fields(rest))
		default:
			return natsMsg{}, fmt.Errorf("nats: unexpected %q", line)
		}
	}
}

// readMsg reads the body of MSG <subject> <sid> [reply] <size> or
// HMSG <subject> <sid> [reply] <header size> <total size>.
func (c *natsConn) readMsg(headers bool, f []string) (natsMsg, error) {
	want := 3
	if headers {
		want = 4
	}
	if len(f) != want && len(f) != want+1 {
		return natsMsg{}, fmt.Errorf("nats: bad message line %v", f)
	}
	m := natsMsg{subject: f[0]}
	total, err := strconv.Atoi(f[len(f)-1])
	if err != nil || total < 0 || total > 64<<20 {
		return natsMsg{}, fmt.Errorf("nats: bad message size %v", f)
	}
	hlen := 0
	if headers {
		if hlen, err = strconv.Atoi(f[len(f)-2]); err != nil || hlen < 0 || hlen > total {
			return natsMsg{}, fmt.Errorf("nats: bad header size %v", f)
		}
	}
	b := make([]byte, total+2)
	if _, err := io.ReadFull(c.r, b); err != nil {
		return natsMsg{}, fmt.Errorf("nats: read message: %w", err)
	}
	if hlen > 0 {
		first, _, _ := strings.Cut(string(b[:hlen]), "\r\n")
		if parts := strings.Fields(first); len(parts) >= 2 {
			m.status = parts[1]
			m.statusText = strings.Join(parts[2:], " ")
		}
	}
	m.data = b[hlen:total]
	return m, nil
}
