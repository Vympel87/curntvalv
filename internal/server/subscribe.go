package server

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// ChangeReader reads row changes from the local log. *event.Reader
// implements it.
type ChangeReader interface {
	Read(ctx context.Context, from uint64, max int, filter event.Filter) (event.Result, error)
}

// Subscriptions reads named subscriptions (design D22). *fsm.FSM
// implements it.
type Subscriptions interface {
	Subscription(name string) (*pb.Subscription, bool)
}

// Subscribe implements pb.SubscribeServiceServer.
type Subscribe struct {
	pb.UnimplementedSubscribeServiceServer
	reader ChangeReader
	node   RaftNode      // nil: no named subscriptions
	subs   Subscriptions // nil with node
	active atomic.Int64
	sent   atomic.Uint64
}

// SubscribeStats are the subscribe service's counters on this node.
type SubscribeStats struct {
	Active int64  // open streams
	Sent   uint64 // changes sent since start
}

// Stats returns the counters.
func (s *Subscribe) Stats() SubscribeStats {
	return SubscribeStats{Active: s.active.Load(), Sent: s.sent.Load()}
}

// NewSubscribe returns the subscribe service of one node. node and subs
// serve named subscriptions; both may be nil, which disables them.
func NewSubscribe(reader ChangeReader, node RaftNode, subs Subscriptions) *Subscribe {
	return &Subscribe{reader: reader, node: node, subs: subs}
}

// JoinSubscription returns a named subscription, creating it if needed.
func (s *Subscribe) JoinSubscription(ctx context.Context, req *pb.JoinSubscriptionRequest) (*pb.JoinSubscriptionResponse, error) {
	if s.node == nil {
		return nil, status.Error(codes.Unimplemented, "named subscriptions are not available on this node")
	}
	if !fsm.ValidSinkName(req.GetName()) {
		return nil, status.Errorf(codes.InvalidArgument, "bad subscription name %q (lower case letters, digits, _ and -)", req.GetName())
	}
	// A linearizable read, so an existing subscription is never created
	// again with other settings.
	idx, err := s.node.ReadIndex(ctx)
	if err != nil {
		return nil, subscriptionError(err)
	}
	if sub, ok := s.subs.Subscription(req.GetName()); ok {
		return &pb.JoinSubscriptionResponse{Subscription: sub}, nil
	}
	if req.GetMustExist() {
		return nil, status.Errorf(codes.NotFound, "no subscription %q", req.GetName())
	}
	cmd := &pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_CREATE, Name: req.GetName(), Drop: req.GetDrop()}
	if req.GetStart() == pb.SubscriptionStart_SUBSCRIPTION_START_LATEST {
		cmd.Offset = idx
	}
	data, err := fsm.EncodeSubscription(cmd)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	at, err := s.node.Propose(ctx, data)
	if err != nil {
		return nil, subscriptionError(err)
	}
	sub, ok := s.subs.Subscription(req.GetName())
	if !ok {
		return nil, status.Error(codes.Aborted, "subscription deleted while it was created")
	}
	return &pb.JoinSubscriptionResponse{Subscription: sub, Created: sub.GetCreatedIndex() == at}, nil
}

// CommitSubscription stores a subscriber's position.
func (s *Subscribe) CommitSubscription(ctx context.Context, req *pb.CommitSubscriptionRequest) (*pb.CommitSubscriptionResponse, error) {
	if s.node == nil {
		return nil, status.Error(codes.Unimplemented, "named subscriptions are not available on this node")
	}
	st, err := s.node.Status(ctx)
	if err != nil {
		return nil, subscriptionError(err)
	}
	if st.State != raft.StateLeader {
		return nil, subscriptionError(&raft.NotLeaderError{Leader: st.Leader})
	}
	sub, ok := s.subs.Subscription(req.GetName())
	switch {
	case !ok:
		return nil, status.Errorf(codes.NotFound, "no subscription %q (deleted?)", req.GetName())
	case sub.GetGeneration() != req.GetGeneration():
		return nil, status.Errorf(codes.Aborted, "subscription %q was reset (generation %d, not %d); join again", req.GetName(), sub.GetGeneration(), req.GetGeneration())
	case req.GetOffset() <= sub.GetOffset():
		return &pb.CommitSubscriptionResponse{}, nil
	}
	data, err := fsm.EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_COMMIT,
		Name: req.GetName(), Generation: req.GetGeneration(), Offset: req.GetOffset()})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := s.node.Propose(ctx, data); err != nil {
		return nil, subscriptionError(err)
	}
	return &pb.CommitSubscriptionResponse{}, nil
}

// subscriptionError maps Raft errors; a client tries the next node on
// FAILED_PRECONDITION and UNAVAILABLE.
func subscriptionError(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return status.Errorf(codes.FailedPrecondition, "not the leader; leader is node %d", nl.Leader)
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrStopped):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

const (
	// subscribeEntries is how many log entries one read covers.
	subscribeEntries = 256
	// subscribeMaxBytes bounds a response, well below gRPC's default 4 MiB
	// receive limit. A single larger change is still sent on its own.
	subscribeMaxBytes = 1 << 20
	// progressEvery is how often a response without events is sent to move
	// next_index past entries that matched nothing.
	progressEvery = time.Second
)

// Subscribe streams changes until the client goes away.
func (s *Subscribe) Subscribe(req *pb.SubscribeRequest, stream pb.SubscribeService_SubscribeServer) error {
	for _, t := range req.GetTables() {
		if t == "" {
			return status.Error(codes.InvalidArgument, "empty table name")
		}
	}
	s.active.Add(1)
	defer s.active.Add(-1)
	ctx := stream.Context()
	filter := event.Filter(req.GetTables())
	sentSchemas := map[uint64]bool{}
	from := req.GetFromIndex()
	lastSend := time.Now()
	for {
		res, err := s.reader.Read(ctx, from, subscribeEntries, filter)
		switch {
		case errors.Is(err, raft.ErrCompacted):
			return status.Errorf(codes.OutOfRange, "index %d was removed by log compaction; subscribe from 0 for the oldest retained entry", from)
		case ctx.Err() != nil:
			return status.FromContextError(ctx.Err()).Err()
		case errors.Is(err, raft.ErrStopped):
			return status.Error(codes.Unavailable, "node is shutting down")
		case err != nil:
			return status.Error(codes.Internal, err.Error())
		}
		from = res.Next
		if len(res.Changes) == 0 && time.Since(lastSend) < progressEvery {
			continue
		}
		if err := sendChanges(stream, res, sentSchemas); err != nil {
			return err
		}
		s.sent.Add(uint64(len(res.Changes)))
		lastSend = time.Now()
	}
}

// sendChanges sends the changes of one read, split between log entries into
// responses of about subscribeMaxBytes. Each response's next_index points
// just past the last entry it completes; the last one past the whole read.
func sendChanges(stream pb.SubscribeService_SubscribeServer, res event.Result, sentSchemas map[uint64]bool) error {
	resp := &pb.SubscribeResponse{Schemas: map[uint64]*pb.Schema{}}
	size := 0
	flush := func(next uint64) error {
		resp.NextIndex = next
		if err := stream.Send(resp); err != nil {
			return err
		}
		resp, size = &pb.SubscribeResponse{Schemas: map[uint64]*pb.Schema{}}, 0
		return nil
	}
	for i, c := range res.Changes {
		ce := &pb.ChangeEvent{
			Id:           c.ID(),
			Index:        c.Index,
			CommitLsn:    c.CommitLSN,
			Seq:          c.Seq,
			Xid:          c.Xid,
			CommitTimeUs: c.CommitTimeUs,
			Event:        c.Event,
		}
		n := proto.Size(ce)
		if id := c.Event.GetSchemaId(); c.Schema != nil && !sentSchemas[id] {
			sentSchemas[id] = true
			resp.Schemas[id] = c.Schema
			n += proto.Size(c.Schema)
		}
		resp.Events = append(resp.Events, ce)
		size += n
		// Split only between entries, so next_index never skips part of one.
		last := i == len(res.Changes)-1
		if !last && size >= subscribeMaxBytes && res.Changes[i+1].Index != c.Index {
			if err := flush(c.Index + 1); err != nil {
				return err
			}
		}
	}
	return flush(res.Next)
}
