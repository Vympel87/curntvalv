// Package transport carries Raft messages between nodes over gRPC.
//
// Each node keeps one outgoing stream per peer (RaftService.Stream), fed by a
// bounded queue. Send never blocks: when the queue is full or the peer is
// unreachable, messages are dropped, which Raft tolerates. Replies travel on
// the stream that the other node opens back to us.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// MaxMessageSize is the largest gRPC message a node sends or accepts.
const MaxMessageSize = 64 << 20

// Options tunes a Transport. The zero value is usable.
type Options struct {
	// QueueSize is the per-peer outgoing queue length. Default 1024.
	QueueSize int
	// MinBackoff and MaxBackoff bound the delay between reconnect attempts.
	// Defaults 50ms and 500ms. MaxBackoff should stay well below the Raft
	// election timeout: a restarted peer that does not hear from the leader
	// in time starts an election and briefly disrupts the cluster.
	MinBackoff, MaxBackoff time.Duration
	// Logger receives connection errors. Default: discard.
	Logger *slog.Logger
}

// Transport sends Raft messages to peers. It implements raft.Transport.
type Transport struct {
	self   uint64
	peers  map[uint64]string // id → address, without self
	opts   Options
	queues map[uint64]chan raft.Message

	dropped atomic.Uint64
	running atomic.Bool
}

// New returns a transport for node self. peers maps every node id (self may
// be included) to its gRPC address.
func New(self uint64, peers map[uint64]string, opts Options) *Transport {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1024
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = 50 * time.Millisecond
	}
	if opts.MaxBackoff < opts.MinBackoff {
		opts.MaxBackoff = max(500*time.Millisecond, opts.MinBackoff)
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	t := &Transport{self: self, peers: map[uint64]string{}, opts: opts, queues: map[uint64]chan raft.Message{}}
	for id, addr := range peers {
		if id == self {
			continue
		}
		t.peers[id] = addr
		t.queues[id] = make(chan raft.Message, opts.QueueSize)
	}
	return t
}

// Send queues m for its destination. It never blocks.
func (t *Transport) Send(m raft.Message) {
	q, ok := t.queues[m.To]
	if !ok {
		t.dropped.Add(1)
		return
	}
	select {
	case q <- m:
	default:
		t.dropped.Add(1)
	}
}

// Dropped returns how many messages were dropped so far.
func (t *Transport) Dropped() uint64 { return t.dropped.Load() }

// Run connects to every peer and delivers queued messages until ctx is
// cancelled. It returns after all its goroutines have stopped.
func (t *Transport) Run(ctx context.Context) error {
	if !t.running.CompareAndSwap(false, true) {
		return errors.New("transport: Run called twice")
	}
	var wg sync.WaitGroup
	for id, addr := range t.peers {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxMessageSize)),
			// gRPC's own reconnect backoff grows to 120s by default. A peer
			// that comes back must hear from the leader well within an
			// election timeout, or it starts elections and disrupts the
			// cluster, so cap it like our own backoff.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  t.opts.MinBackoff,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   t.opts.MaxBackoff,
				},
				MinConnectTimeout: max(t.opts.MaxBackoff, time.Second),
			}),
		)
		if err != nil {
			return fmt.Errorf("transport: client for node %d (%s): %w", id, addr, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			t.runPeer(ctx, id, pb.NewRaftServiceClient(conn))
		}()
	}
	wg.Wait()
	return nil
}

// runPeer keeps a stream to one peer open and drains its queue into it.
func (t *Transport) runPeer(ctx context.Context, id uint64, client pb.RaftServiceClient) {
	log := t.opts.Logger.With("peer", id)
	queue := t.queues[id]
	backoff := t.opts.MinBackoff
	for ctx.Err() == nil {
		err := t.stream(ctx, client, queue, &backoff)
		if ctx.Err() != nil {
			return
		}
		log.Debug("raft stream broken", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, t.opts.MaxBackoff)
	}
}

// stream sends queued messages on one stream until an error happens. The
// message being sent when the stream breaks is lost. A successful send resets
// the reconnect backoff.
func (t *Transport) stream(ctx context.Context, client pb.RaftServiceClient, queue <-chan raft.Message, backoff *time.Duration) error {
	s, err := client.Stream(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			s.CloseAndRecv()
			return ctx.Err()
		case m := <-queue:
			if err := s.Send(&pb.StreamRequest{Message: ToPB(m)}); err != nil {
				t.dropped.Add(1)
				if errors.Is(err, io.EOF) {
					_, err = s.CloseAndRecv() // the real error
				}
				return err
			}
			*backoff = t.opts.MinBackoff
		}
	}
}
