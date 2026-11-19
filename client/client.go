// Package client streams row changes from a curntValv cluster into a Go
// program.
//
//	c, err := client.New([]string{"node1:7000", "node2:7000", "node3:7000"})
//	if err != nil { ... }
//	defer c.Close()
//	for ch, err := range c.Subscribe(ctx, client.Options{Tables: []string{"orders"}}) {
//		if err != nil { ... } // the stream cannot continue
//		fmt.Println(ch.Op, ch.Table, ch.After)
//	}
//
// Any node can serve a subscription. When a node fails, Subscribe continues
// on the next one from where it stopped, so changes can be delivered again
// around a failure; use Change.ID to drop copies. The position is kept by
// the program: to resume later, subscribe From the Index of the last change
// it has fully processed (that entry's changes arrive again).
//
// A cluster that uses TLS or a token needs WithTLS and WithToken.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/security"
)

var (
	// ErrCompacted means the requested start was removed by retention.
	// Subscribe again From 0 (the oldest retained change) or later.
	ErrCompacted = errors.New("client: start index was removed by log retention")
	// ErrInvalid means the server rejected the request, e.g. an empty table
	// name.
	ErrInvalid = errors.New("client: invalid request")
	// ErrUnauthenticated means the cluster wants a token (WithToken) or
	// the token is wrong.
	ErrUnauthenticated = errors.New("client: missing or wrong token")
)

// Op is the kind of a row change.
type Op string

// Operations.
const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

// Change is one row change. Its JSON form is the one the CLI, the webhook
// sink and the dashboard use.
type Change struct {
	// ID identifies the change across redeliveries: "<commit LSN>:<seq>".
	ID string `json:"id"`
	// Index is the log index of the entry holding the change. One entry
	// can hold several changes.
	Index      uint64    `json:"index"`
	LSN        string    `json:"lsn"` // commit LSN of the transaction, as Postgres prints it
	Seq        uint32    `json:"seq"` // position of the change in its transaction
	Xid        uint32    `json:"xid"`
	CommitTime time.Time `json:"commit_time"`
	Table      string    `json:"table"` // "namespace.name"
	Op         Op        `json:"op"`
	// Before and After map column names to values in Postgres text format
	// (base64 for binary values); a nil value is NULL. Before is set for
	// updates and deletes as far as the table's replica identity allows,
	// After for inserts and updates.
	Before map[string]*string `json:"before,omitempty"`
	After  map[string]*string `json:"after,omitempty"`
	// Unchanged lists large (TOAST) columns an update did not touch; they
	// are missing from After.
	Unchanged []string `json:"unchanged,omitempty"`
}

// Options select what Subscribe sends.
type Options struct {
	// From is the first log index to send; 0 means the oldest retained
	// change.
	From uint64
	// Tables limits the stream to these tables, as "name" (any namespace)
	// or "namespace.name". Empty means all tables.
	Tables []string
	// OnRetry, if set, is called when a stream to a node failed and
	// Subscribe tries again, from index next.
	OnRetry func(addr string, err error, next uint64)
}

// Client connects to the nodes of one cluster. It is safe for concurrent
// use.
type Client struct {
	addrs []string
	tls   *tls.Config
	token string
}

// Option configures a Client.
type Option func(*Client)

// WithTLS connects with TLS. cfg.RootCAs should hold the cluster's CA
// (curntvalv server --tls-ca); nil RootCAs means the system roots.
func WithTLS(cfg *tls.Config) Option { return func(c *Client) { c.tls = cfg } }

// WithToken sends the cluster's token (curntvalv server --auth-token-file)
// with every call.
func WithToken(token string) Option { return func(c *Client) { c.token = token } }

// New returns a client for the nodes at addrs ("host:port", the nodes'
// --listen addresses; comma-separated entries are split). It does not
// connect yet.
func New(addrs []string, opts ...Option) (*Client, error) {
	c := &Client{}
	for _, a := range addrs {
		for _, s := range strings.Split(a, ",") {
			if s = strings.TrimSpace(s); s != "" {
				c.addrs = append(c.addrs, s)
			}
		}
	}
	if len(c.addrs) == 0 {
		return nil, errors.New("client: no node address")
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Close releases the client. Streams are closed when their loop ends.
func (c *Client) Close() error { return nil }

const (
	minBackoff = 500 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// Subscribe returns the stored changes from opts.From on, then new changes
// as they are committed. The sequence ends when ctx is done, when the loop
// stops, or after an error that retrying cannot fix (ErrCompacted,
// ErrInvalid), which is yielded last. Failing nodes are retried, one after
// the other, until ctx is done.
func (c *Client) Subscribe(ctx context.Context, opts Options) iter.Seq2[Change, error] {
	return func(yield func(Change, error) bool) {
		next := opts.From
		backoff := minBackoff
		for attempt := 0; ; attempt++ {
			addr := c.addrs[attempt%len(c.addrs)]
			received := false
			stop, err := c.stream(ctx, addr, &pb.SubscribeRequest{FromIndex: next, Tables: opts.Tables},
				func(resp *pb.SubscribeResponse, schemas map[uint64]*pb.Schema) bool {
					received = true
					for _, ce := range resp.GetEvents() {
						if !yield(convert(ce, schemas), nil) {
							return false
						}
					}
					next = resp.GetNextIndex()
					return true
				})
			if stop || ctx.Err() != nil {
				return
			}
			switch status.Code(err) {
			case codes.Unauthenticated:
				yield(Change{}, fmt.Errorf("%w: %s", ErrUnauthenticated, status.Convert(err).Message()))
				return
			case codes.OutOfRange:
				yield(Change{}, fmt.Errorf("%w: %s", ErrCompacted, status.Convert(err).Message()))
				return
			case codes.InvalidArgument:
				yield(Change{}, fmt.Errorf("%w: %s", ErrInvalid, status.Convert(err).Message()))
				return
			}
			if received {
				backoff = minBackoff
			}
			if opts.OnRetry != nil {
				opts.OnRetry(addr, err, next)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			// A full round over the nodes without progress backs off.
			if !received && attempt%len(c.addrs) == len(c.addrs)-1 {
				backoff = min(2*backoff, maxBackoff)
			}
		}
	}
}

// stream runs one Subscribe stream against addr and calls handle for every
// response, with all schemas received on the stream so far. stop is true
// when handle returned false.
func (c *Client) stream(ctx context.Context, addr string, req *pb.SubscribeRequest,
	handle func(*pb.SubscribeResponse, map[uint64]*pb.Schema) bool) (stop bool, err error) {
	conn, err := grpc.NewClient(addr, append(security.DialOptions(c.tls, c.token),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))...)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s, err := pb.NewSubscribeServiceClient(conn).Subscribe(ctx, req)
	if err != nil {
		return false, err
	}
	schemas := map[uint64]*pb.Schema{}
	for {
		resp, err := s.Recv()
		if err != nil {
			return false, err
		}
		for id, sc := range resp.GetSchemas() {
			schemas[id] = sc
		}
		if !handle(resp, schemas) {
			return true, nil
		}
	}
}

func convert(ce *pb.ChangeEvent, schemas map[uint64]*pb.Schema) Change {
	j := event.Change{Index: ce.GetIndex(), CommitLSN: ce.GetCommitLsn(), Seq: ce.GetSeq(), Xid: ce.GetXid(),
		CommitTimeUs: ce.GetCommitTimeUs(), Event: ce.GetEvent(), Schema: schemas[ce.GetEvent().GetSchemaId()]}.JSON()
	ch := Change{ID: j.ID, Index: j.Index, LSN: j.LSN, Seq: j.Seq, Xid: j.Xid,
		CommitTime: time.UnixMicro(ce.GetCommitTimeUs()).UTC(), Table: j.Table, Op: Op(j.Op),
		Before: row(j.Before), After: row(j.After), Unchanged: j.Unchanged}
	return ch
}

func row(m map[string]any) map[string]*string {
	if m == nil {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = &s
		} else {
			out[k] = nil
		}
	}
	return out
}
