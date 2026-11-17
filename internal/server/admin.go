// Package server implements curntValv's client-facing gRPC services:
// AdminService (cluster status and the metadata key-value store) and
// SubscribeService (streaming row changes).
package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// RaftNode is what the admin service needs from Raft. *raft.Node implements it.
type RaftNode interface {
	Propose(ctx context.Context, data []byte) (uint64, error)
	ReadIndex(ctx context.Context) (uint64, error)
	Status(ctx context.Context) (raft.Status, error)
}

// LogReader reads committed log entries. *raft.Node implements it.
type LogReader interface {
	ReadCommitted(ctx context.Context, from uint64, max int) ([]raft.Entry, error)
}

// Meta reads the replicated metadata. *fsm.FSM implements it.
type Meta interface {
	Get(key string) ([]byte, bool)
	Schema(id uint64) (*pb.Schema, bool)
	SinkOffset(name string) (uint64, bool)
}

// Health reports this node's pipeline components for Status.
type Health interface {
	// Source returns the Postgres source's state ("" if there is none) and
	// last error.
	Source() (state, lastErr string)
	// Sinks returns the configured sinks in a stable order. Offset and Lag
	// are filled in by the admin service.
	Sinks() []*pb.SinkStatus
}

// Admin implements pb.AdminServiceServer.
type Admin struct {
	pb.UnimplementedAdminServiceServer
	node   RaftNode
	log    LogReader
	meta   Meta
	health Health
	peers  map[uint64]string
}

// NewAdmin returns the admin service of one node. peers maps node ids to
// addresses and is used to tell clients where the leader is. health may be
// nil.
func NewAdmin(node RaftNode, log LogReader, meta Meta, health Health, peers map[uint64]string) *Admin {
	return &Admin{node: node, log: log, meta: meta, health: health, peers: peers}
}

const (
	defaultReadLogLimit = 100
	maxReadLogLimit     = 1000
)

// Status reports this node's Raft state.
func (a *Admin) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	st, err := a.node.Status(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	resp := &pb.StatusResponse{
		Id:            st.ID,
		Term:          st.Term,
		State:         st.State.String(),
		Leader:        st.Leader,
		LeaderAddr:    a.peers[st.Leader],
		FirstIndex:    st.FirstIndex,
		LastIndex:     st.LastIndex,
		CommitIndex:   st.CommitIndex,
		LastApplied:   st.LastApplied,
		SnapshotIndex: st.SnapshotIndex,
	}
	if a.health != nil {
		resp.SourceState, resp.SourceError = a.health.Source()
		resp.Sinks = a.health.Sinks()
		for _, s := range resp.Sinks {
			s.Offset, _ = a.meta.SinkOffset(s.Name)
			if st.LastApplied > s.Offset {
				s.Lag = st.LastApplied - s.Offset
			}
		}
	}
	return resp, nil
}

// Put stores a value and returns once it is committed.
func (a *Admin) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	data, err := fsm.EncodePut(req.GetKey(), req.GetValue())
	if err != nil {
		return nil, a.toStatus(err)
	}
	idx, err := a.node.Propose(ctx, data)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &pb.PutResponse{Index: idx}, nil
}

// Delete removes a key and returns once the removal is committed.
func (a *Admin) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	data, err := fsm.EncodeDelete(req.GetKey())
	if err != nil {
		return nil, a.toStatus(err)
	}
	idx, err := a.node.Propose(ctx, data)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &pb.DeleteResponse{Index: idx}, nil
}

// Get is a linearizable read: it confirms leadership with a majority first.
func (a *Admin) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	idx, err := a.node.ReadIndex(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	v, ok := a.meta.Get(req.GetKey())
	return &pb.GetResponse{Found: ok, Value: v, ReadIndex: idx}, nil
}

// ReadLog returns committed entries with their decoded transaction batches.
func (a *Admin) ReadLog(ctx context.Context, req *pb.ReadLogRequest) (*pb.ReadLogResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultReadLogLimit
	}
	limit = min(limit, maxReadLogLimit)
	st, err := a.node.Status(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	entries, err := a.log.ReadCommitted(ctx, req.GetFromIndex(), limit)
	if errors.Is(err, raft.ErrCompacted) {
		return nil, status.Errorf(codes.OutOfRange, "entries before %d were removed by log compaction", st.FirstIndex)
	}
	if err != nil {
		return nil, a.toStatus(err)
	}
	resp := &pb.ReadLogResponse{FirstIndex: st.FirstIndex, CommitIndex: st.CommitIndex, Schemas: map[uint64]*pb.Schema{}}
	for _, e := range entries {
		le := &pb.LogEntry{Index: e.Index, Term: e.Term}
		switch {
		case e.Type == raft.EntryNoop:
			le.Kind = pb.EntryKind_ENTRY_KIND_NOOP
		default:
			tb, ok, err := fsm.DecodeTxBatch(e.Data)
			if err != nil {
				return nil, a.toStatus(fmt.Errorf("entry %d: %w", e.Index, err))
			}
			if !ok {
				le.Kind = pb.EntryKind_ENTRY_KIND_META
				break
			}
			le.Kind, le.TxBatch = pb.EntryKind_ENTRY_KIND_TX_BATCH, tb
			for _, ev := range tb.GetEvents() {
				id := ev.GetSchemaId()
				if _, done := resp.Schemas[id]; done {
					continue
				}
				if sc, ok := a.meta.Schema(id); ok {
					resp.Schemas[id] = sc
				}
			}
		}
		resp.Entries = append(resp.Entries, le)
	}
	return resp, nil
}

// toStatus maps internal errors to gRPC status codes.
func (a *Admin) toStatus(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		msg := "not the leader; leader unknown"
		if nl.Leader != 0 {
			msg = fmt.Sprintf("not the leader; leader is node %d at %s", nl.Leader, a.peers[nl.Leader])
		}
		return status.Error(codes.FailedPrecondition, msg)
	case errors.Is(err, raft.ErrLeadershipLost):
		return status.Error(codes.Unavailable, "leadership changed before commit; the write may or may not have been applied")
	case errors.Is(err, raft.ErrStopped):
		return status.Error(codes.Unavailable, "node is shutting down")
	case errors.Is(err, fsm.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
