package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/security"
)

// commitEvery is how often a named subscription stores its position.
const commitEvery = time.Second

// position tracks a named subscription while Subscribe runs.
type position struct {
	opts       Options
	generation uint64
	committed  uint64 // stored in the cluster
	done       uint64 // processed: every change of entries up to here
	lastCommit time.Time
}

// from is where the stream starts: after the stored offset, or with the
// oldest retained entry.
func (p *position) from() uint64 {
	if p.committed == 0 {
		return 0
	}
	return p.committed + 1
}

// advance records that everything up to done is processed and stores it
// when due. restart means the subscription was reset and p now holds the
// new position; err that it cannot continue.
func (p *position) advance(ctx context.Context, c *Client, done uint64) (restart bool, err error) {
	p.done = max(p.done, done)
	if p.opts.ManualCommit || p.done <= p.committed || time.Since(p.lastCommit) < commitEvery {
		return false, nil
	}
	return p.commit(ctx, c)
}

func (p *position) commit(ctx context.Context, c *Client) (restart bool, err error) {
	p.lastCommit = time.Now()
	err = c.onLeader(ctx, func(ctx context.Context, s pb.SubscribeServiceClient) error {
		_, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: p.opts.Name, Generation: p.generation, Offset: p.done})
		return err
	})
	switch status.Code(err) {
	case codes.OK:
		p.committed = p.done
		return false, nil
	case codes.Aborted: // reset: take the new position
		fresh, err := c.join(ctx, p.opts)
		if err != nil {
			return false, err
		}
		*p = *fresh
		return true, nil
	case codes.NotFound:
		return false, fmt.Errorf("%w: %s", ErrSubscriptionDeleted, p.opts.Name)
	}
	if errors.Is(err, ErrUnauthenticated) {
		return false, err
	}
	return false, nil // no leader just now: try again later
}

// final stores the position when the loop ends.
func (p *position) final(c *Client) {
	if p.opts.ManualCommit || p.done <= p.committed {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.commit(ctx, c)
}

// join returns the stored position of opts.Name, creating the subscription
// if it does not exist.
func (c *Client) join(ctx context.Context, opts Options) (*position, error) {
	return c.joinReq(ctx, opts, false)
}

func (c *Client) joinReq(ctx context.Context, opts Options, mustExist bool) (*position, error) {
	req := &pb.JoinSubscriptionRequest{Name: opts.Name, Drop: opts.Drop, Start: pb.SubscriptionStart_SUBSCRIPTION_START_EARLIEST, MustExist: mustExist}
	if opts.StartLatest {
		req.Start = pb.SubscriptionStart_SUBSCRIPTION_START_LATEST
	}
	var resp *pb.JoinSubscriptionResponse
	err := c.onLeader(ctx, func(ctx context.Context, s pb.SubscribeServiceClient) error {
		var err error
		resp, err = s.JoinSubscription(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return nil, fmt.Errorf("%w: %s", ErrSubscriptionDeleted, opts.Name)
	}
	if err != nil {
		return nil, err
	}
	sub := resp.GetSubscription()
	return &position{opts: opts, generation: sub.GetGeneration(), committed: sub.GetOffset(), done: sub.GetOffset(), lastCommit: time.Now()}, nil
}

// Commit stores that the named subscription has processed every change up
// to and including log index index, for subscriptions with ManualCommit. A
// position is only ever moved forward. It fails with ErrSubscriptionDeleted
// for a subscription that does not exist.
func (c *Client) Commit(ctx context.Context, name string, index uint64) error {
	p, err := c.joinReq(ctx, Options{Name: name}, true)
	if err != nil {
		return err
	}
	if index <= p.committed {
		return nil
	}
	err = c.onLeader(ctx, func(ctx context.Context, s pb.SubscribeServiceClient) error {
		_, err := s.CommitSubscription(ctx, &pb.CommitSubscriptionRequest{Name: name, Generation: p.generation, Offset: index})
		return err
	})
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%w: %s", ErrSubscriptionDeleted, name)
	case codes.Aborted:
		return fmt.Errorf("client: subscription %s was reset meanwhile: %s", name, status.Convert(err).Message())
	}
	return err
}

// onLeader runs call against the nodes in turn until one, the leader,
// accepts it: not-leader and unavailable answers try the next node, and a
// full round without success waits before the next.
func (c *Client) onLeader(ctx context.Context, call func(context.Context, pb.SubscribeServiceClient) error) error {
	backoff := 100 * time.Millisecond
	var last error
	for {
		for _, addr := range c.addrs {
			conn, err := grpc.NewClient(addr, security.DialOptions(c.tls, c.token)...)
			if err != nil {
				last = err
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = call(cctx, pb.NewSubscribeServiceClient(conn))
			cancel()
			conn.Close()
			switch status.Code(err) {
			case codes.OK, codes.Aborted, codes.NotFound:
				return err
			case codes.Unauthenticated:
				return fmt.Errorf("%w: %s", ErrUnauthenticated, status.Convert(err).Message())
			case codes.InvalidArgument:
				return fmt.Errorf("%w: %s", ErrInvalid, status.Convert(err).Message())
			case codes.Unimplemented:
				return fmt.Errorf("%w: %s (the cluster is older than v0.4)", ErrInvalid, status.Convert(err).Message())
			}
			last = fmt.Errorf("%s: %w", addr, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("client: no leader answered: %w (last: %v)", ctx.Err(), last)
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 2*time.Second)
	}
}
