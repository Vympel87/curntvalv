// Package server implements curntValv's client-facing gRPC services. For now
// that is AdminService: cluster status and the metadata key-value store.
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

// KV reads the metadata store. *fsm.FSM implements it.
type KV interface {
	Get(key string) ([]byte, bool)
}

// Admin implements pb.AdminServiceServer.
type Admin struct {
	pb.UnimplementedAdminServiceServer
	node  RaftNode
	kv    KV
	peers map[uint64]string
}

// NewAdmin returns the admin service of one node. peers maps node ids to
// addresses and is used to tell clients where the leader is.
func NewAdmin(node RaftNode, kv KV, peers map[uint64]string) *Admin {
	return &Admin{node: node, kv: kv, peers: peers}
}

// Status reports this node's Raft state.
func (a *Admin) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	st, err := a.node.Status(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &pb.StatusResponse{
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
	}, nil
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
	v, ok := a.kv.Get(req.GetKey())
	return &pb.GetResponse{Found: ok, Value: v, ReadIndex: idx}, nil
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
