// Package server implements curntValv's client-facing gRPC services:
// AdminService (cluster status and the metadata key-value store) and
// SubscribeService (streaming row changes).
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	SinkPosition(name string) (fsm.SinkPosition, bool)
	SinkSkips(name string) fsm.SinkSkips
	LastChange() uint64
	Backfills() []*pb.Backfill
	Subscriptions() []*pb.Subscription
	Subscription(name string) (*pb.Subscription, bool)
}

// Health reports this node's pipeline components for Status.
type Health interface {
	// Source returns the Postgres source's state ("" if there is none) and
	// last error.
	Source() (state, lastErr string)
	// Sinks returns the configured sinks in a stable order. Offset, Lag and
	// Generation are filled in by the admin service.
	Sinks() []*pb.SinkStatus
	// Retention returns the log retention status, or nil.
	Retention() *pb.RetentionStatus
}

// SourceResetter moves the Postgres source to its slot's position after a
// failover check stopped it (design D7). *pg.Source implements it.
type SourceResetter interface {
	Reset(ctx context.Context) (index uint64, r *pb.SourceReset, err error)
}

// Backfiller starts and cancels table backfills (design D17). *pg.Source
// implements it.
type Backfiller interface {
	StartBackfill(ctx context.Context, tables []string) (index uint64, names []string, err error)
	CancelBackfill(ctx context.Context, tables []string) (index uint64, names []string, err error)
}

// Membership changes the cluster's members (design D18). *raft.Node
// implements it.
type Membership interface {
	AddMember(ctx context.Context, id uint64, addr string) (uint64, error)
	RemoveMember(ctx context.Context, id uint64) (uint64, error)
}

// Source is the Postgres source as the admin service uses it.
type Source interface {
	SourceResetter
	Backfiller
}

// Admin implements pb.AdminServiceServer.
type Admin struct {
	pb.UnimplementedAdminServiceServer
	node    RaftNode
	log     LogReader
	meta    Meta
	health  Health
	source  Source
	members Membership
	peers   map[uint64]string
}

// NewAdmin returns the admin service of one node. peers maps node ids to
// addresses (from --peers) and tells clients where the leader is when the
// cluster configuration does not. health and source may be nil (no Postgres
// source on this node), members too (no membership changes).
func NewAdmin(node RaftNode, log LogReader, meta Meta, health Health, source Source, members Membership, peers map[uint64]string) *Admin {
	return &Admin{node: node, log: log, meta: meta, health: health, source: source, members: members, peers: peers}
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
		FirstIndex:    st.FirstIndex,
		LastIndex:     st.LastIndex,
		CommitIndex:   st.CommitIndex,
		LastApplied:   st.LastApplied,
		SnapshotIndex: st.SnapshotIndex,
		ConfigIndex:   st.ConfigIndex,
	}
	for _, m := range st.Config.Members {
		pm := &pb.Member{Id: m.ID, Addr: m.Addr, Voter: m.Voter, MatchIndex: st.Match[m.ID]}
		if m.ID == st.ID && st.State == raft.StateLeader {
			pm.MatchIndex = st.LastIndex
		}
		resp.Members = append(resp.Members, pm)
	}
	resp.LeaderAddr = a.addr(st, st.Leader)
	last := a.meta.LastChange()
	for _, sub := range a.meta.Subscriptions() {
		ss := &pb.SubscriptionStatus{Name: sub.GetName(), Offset: sub.GetOffset(), Generation: sub.GetGeneration(), Policy: "block"}
		if sub.GetDrop() {
			ss.Policy = "drop"
		}
		if last > sub.GetOffset() {
			ss.Lag = last - sub.GetOffset()
		}
		resp.Subscriptions = append(resp.Subscriptions, ss)
	}
	if a.health != nil {
		resp.SourceState, resp.SourceError = a.health.Source()
		resp.Sinks = a.health.Sinks()
		resp.Retention = a.health.Retention()
		resp.Backfills = a.meta.Backfills()
		last := a.meta.LastChange()
		for _, s := range resp.Sinks {
			pos, _ := a.meta.SinkPosition(s.Name)
			s.Offset, s.Generation = pos.Offset, pos.Gen
			s.Skipped = a.meta.SinkSkips(s.Name).Total
			if last > s.Offset {
				s.Lag = last - s.Offset
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
		case e.Type == raft.EntryConfig:
			le.Kind = pb.EntryKind_ENTRY_KIND_CONFIG
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

// ResetSink moves a sink to another position and starts a new generation of
// its offsets. Only sinks configured on this node can be reset, so a typo
// does not create a stray offset.
func (a *Admin) ResetSink(ctx context.Context, req *pb.ResetSinkRequest) (*pb.ResetSinkResponse, error) {
	name := req.GetName()
	if !a.configured(name) {
		return nil, status.Errorf(codes.NotFound, "no sink %q is configured on this node", name)
	}
	var offset uint64
	switch req.GetPosition() {
	case pb.ResetPosition_RESET_POSITION_EARLIEST:
	case pb.ResetPosition_RESET_POSITION_LATEST, pb.ResetPosition_RESET_POSITION_INDEX:
		// A linearizable commit index; also fails early on a follower.
		commit, err := a.node.ReadIndex(ctx)
		if err != nil {
			return nil, a.toStatus(err)
		}
		offset = commit
		if req.GetPosition() == pb.ResetPosition_RESET_POSITION_INDEX {
			st, err := a.node.Status(ctx)
			if err != nil {
				return nil, a.toStatus(err)
			}
			offset = req.GetIndex()
			switch {
			case offset > commit:
				return nil, status.Errorf(codes.InvalidArgument, "index %d is past the commit index %d", offset, commit)
			case offset > 0 && offset+1 < st.FirstIndex:
				return nil, status.Errorf(codes.OutOfRange, "entries after %d were removed by log compaction; the oldest retained entry is %d",
					offset, st.FirstIndex)
			}
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "position must be earliest, latest or index")
	}
	data, err := fsm.EncodeSinkReset(name, offset)
	if err != nil {
		return nil, a.toStatus(err)
	}
	idx, err := a.node.Propose(ctx, data)
	if err != nil {
		return nil, a.toStatus(err)
	}
	pos, _ := a.meta.SinkPosition(name)
	return &pb.ResetSinkResponse{Index: idx, Offset: offset, Generation: pos.Gen}, nil
}

// ResetSource moves the source to its slot's position. Errors other than
// not being the leader are ABORTED with the reason, so clients do not retry
// a reset elsewhere.
func (a *Admin) ResetSource(ctx context.Context, _ *pb.ResetSourceRequest) (*pb.ResetSourceResponse, error) {
	if a.source == nil {
		return nil, status.Error(codes.Aborted, "no Postgres source is configured on this node")
	}
	idx, r, err := a.source.Reset(ctx)
	if err != nil {
		return nil, a.sourceError(err)
	}
	return &pb.ResetSourceResponse{Index: idx, SourceReset: r}, nil
}

// StartBackfill queues tables for a backfill. Like ResetSource, errors other
// than not being the leader are ABORTED.
func (a *Admin) StartBackfill(ctx context.Context, req *pb.StartBackfillRequest) (*pb.StartBackfillResponse, error) {
	if a.source == nil {
		return nil, status.Error(codes.Aborted, "no Postgres source is configured on this node")
	}
	idx, names, err := a.source.StartBackfill(ctx, req.GetTables())
	if err != nil {
		return nil, a.sourceError(err)
	}
	return &pb.StartBackfillResponse{Index: idx, Tables: names}, nil
}

// CancelBackfill stops pending backfills.
func (a *Admin) CancelBackfill(ctx context.Context, req *pb.CancelBackfillRequest) (*pb.CancelBackfillResponse, error) {
	if a.source == nil {
		return nil, status.Error(codes.Aborted, "no Postgres source is configured on this node")
	}
	idx, names, err := a.source.CancelBackfill(ctx, req.GetTables())
	if err != nil {
		return nil, a.sourceError(err)
	}
	return &pb.CancelBackfillResponse{Index: idx, Tables: names}, nil
}

// SinkSkips lists the changes a sink passed over.
func (a *Admin) SinkSkips(_ context.Context, req *pb.SinkSkipsRequest) (*pb.SinkSkipsResponse, error) {
	if !a.configured(req.GetName()) {
		return nil, status.Errorf(codes.NotFound, "no sink %q on this node", req.GetName())
	}
	sk := a.meta.SinkSkips(req.GetName())
	return &pb.SinkSkipsResponse{Total: sk.Total, Recent: sk.Recent}, nil
}

// ResetSubscription moves a named subscription and starts a new
// generation; running subscribers notice on their next commit.
func (a *Admin) ResetSubscription(ctx context.Context, req *pb.ResetSubscriptionRequest) (*pb.ResetSubscriptionResponse, error) {
	idx, err := a.node.ReadIndex(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	if _, ok := a.meta.Subscription(req.GetName()); !ok {
		return nil, status.Errorf(codes.NotFound, "no subscription %q", req.GetName())
	}
	var offset uint64
	switch req.GetPosition() {
	case pb.ResetPosition_RESET_POSITION_EARLIEST:
	case pb.ResetPosition_RESET_POSITION_LATEST:
		offset = idx
	case pb.ResetPosition_RESET_POSITION_INDEX:
		offset = req.GetIndex()
	default:
		return nil, status.Error(codes.InvalidArgument, "position must be earliest, latest or an index")
	}
	data, err := fsm.EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_RESET, Name: req.GetName(), Offset: offset})
	if err != nil {
		return nil, a.toStatus(err)
	}
	at, err := a.node.Propose(ctx, data)
	if err != nil {
		return nil, a.toStatus(err)
	}
	sub, _ := a.meta.Subscription(req.GetName())
	return &pb.ResetSubscriptionResponse{Index: at, Subscription: sub}, nil
}

// DeleteSubscription removes a named subscription.
func (a *Admin) DeleteSubscription(ctx context.Context, req *pb.DeleteSubscriptionRequest) (*pb.DeleteSubscriptionResponse, error) {
	if _, err := a.node.ReadIndex(ctx); err != nil {
		return nil, a.toStatus(err)
	}
	if _, ok := a.meta.Subscription(req.GetName()); !ok {
		return nil, status.Errorf(codes.NotFound, "no subscription %q", req.GetName())
	}
	data, err := fsm.EncodeSubscription(&pb.SubscriptionCommand{Action: pb.SubscriptionCommand_ACTION_DELETE, Name: req.GetName()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	at, err := a.node.Propose(ctx, data)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &pb.DeleteSubscriptionResponse{Index: at}, nil
}

// AddMember adds a server as a learner and makes it a voter once it has
// caught up (design D18).
func (a *Admin) AddMember(ctx context.Context, req *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	if a.members == nil {
		return nil, status.Error(codes.Unimplemented, "membership changes are not available on this node")
	}
	if req.GetId() == 0 || req.GetAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "a member needs an id and an address")
	}
	idx, err := a.members.AddMember(ctx, req.GetId(), req.GetAddr())
	if err != nil {
		return nil, a.memberError(err)
	}
	return &pb.AddMemberResponse{Index: idx}, nil
}

// RemoveMember removes a server.
func (a *Admin) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	if a.members == nil {
		return nil, status.Error(codes.Unimplemented, "membership changes are not available on this node")
	}
	idx, err := a.members.RemoveMember(ctx, req.GetId())
	if err != nil {
		return nil, a.memberError(err)
	}
	return &pb.RemoveMemberResponse{Index: idx}, nil
}

// memberError maps membership errors: an invalid change is
// INVALID_ARGUMENT with the reason (not retried), a change in progress
// UNAVAILABLE (retry).
func (a *Admin) memberError(err error) error {
	switch {
	case errors.Is(err, raft.ErrInvalidConfChange):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, raft.ErrConfChangeInProgress):
		return status.Error(codes.Unavailable, err.Error())
	}
	return a.toStatus(err)
}

// addr returns the address of node id: from the configuration in st, else
// from --peers.
func (a *Admin) addr(st raft.Status, id uint64) string {
	if m, ok := st.Config.Member(id); ok && m.Addr != "" {
		return m.Addr
	}
	return a.peers[id]
}

// sourceError maps an error of a source operation: Raft and context errors
// as usual (a client may retry on the leader), anything else ABORTED with
// the reason, so clients do not retry it elsewhere.
func (a *Admin) sourceError(err error) error {
	var nl *raft.NotLeaderError
	if errors.As(err, &nl) || errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrStopped) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return a.toStatus(err)
	}
	return status.Error(codes.Aborted, err.Error())
}

func (a *Admin) configured(name string) bool {
	if a.health == nil {
		return false
	}
	for _, s := range a.health.Sinks() {
		if s.GetName() == name {
			return true
		}
	}
	return false
}

// toStatus maps internal errors to gRPC status codes.
func (a *Admin) toStatus(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		msg := "not the leader; leader unknown"
		if nl.Leader != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			st, _ := a.node.Status(ctx)
			cancel()
			msg = fmt.Sprintf("not the leader; leader is node %d at %s", nl.Leader, a.addr(st, nl.Leader))
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
