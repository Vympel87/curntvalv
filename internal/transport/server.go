package transport

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Stepper delivers a received message to the local Raft node.
// *raft.Node implements it.
type Stepper interface {
	Step(ctx context.Context, m raft.Message) error
}

// Server receives Raft messages from peers. Register it on a grpc.Server with
// pb.RegisterRaftServiceServer.
type Server struct {
	pb.UnimplementedRaftServiceServer
	self  uint64
	peers map[uint64]bool
	node  Stepper
}

// NewServer returns the receiving side for node self. Messages from ids not
// in peers, or addressed to another node, are dropped.
func NewServer(self uint64, peers map[uint64]string, node Stepper) *Server {
	s := &Server{self: self, peers: map[uint64]bool{}, node: node}
	for id := range peers {
		if id != self {
			s.peers[id] = true
		}
	}
	return s
}

// Stream receives messages until the peer closes the stream.
func (s *Server) Stream(stream pb.RaftService_StreamServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pb.StreamResponse{})
		}
		if err != nil {
			return err
		}
		m, err := FromPB(req.GetMessage())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if m.To != s.self || !s.peers[m.From] {
			continue
		}
		if err := s.node.Step(ctx, m); err != nil {
			if errors.Is(err, raft.ErrStopped) {
				return status.Error(codes.Unavailable, "raft node stopped")
			}
			return err
		}
	}
}

// ToPB converts a Raft message to its wire form.
func ToPB(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type:         uint32(m.Type),
		From:         m.From,
		To:           m.To,
		Term:         m.Term,
		LastLogIndex: m.LastLogIndex,
		LastLogTerm:  m.LastLogTerm,
		PrevLogIndex: m.PrevLogIndex,
		PrevLogTerm:  m.PrevLogTerm,
		LeaderCommit: m.LeaderCommit,
		VoteGranted:  m.VoteGranted,
		Success:      m.Success,
		MatchIndex:   m.MatchIndex,
		ReadId:       m.ReadID,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &pb.Entry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if s := m.Snapshot; s != nil {
		out.Snapshot = &pb.Snapshot{Index: s.Meta.Index, Term: s.Meta.Term, Data: s.Data}
	}
	return out
}

// FromPB converts a wire message back, rejecting unknown message and entry
// types.
func FromPB(p *pb.RaftMessage) (raft.Message, error) {
	if p == nil {
		return raft.Message{}, errors.New("transport: empty message")
	}
	t := raft.MessageType(p.GetType())
	switch t {
	case raft.MsgVote, raft.MsgVoteResp, raft.MsgApp, raft.MsgAppResp, raft.MsgSnap:
	default:
		return raft.Message{}, fmt.Errorf("transport: unknown message type %d", p.GetType())
	}
	m := raft.Message{
		Type:         t,
		From:         p.GetFrom(),
		To:           p.GetTo(),
		Term:         p.GetTerm(),
		LastLogIndex: p.GetLastLogIndex(),
		LastLogTerm:  p.GetLastLogTerm(),
		PrevLogIndex: p.GetPrevLogIndex(),
		PrevLogTerm:  p.GetPrevLogTerm(),
		LeaderCommit: p.GetLeaderCommit(),
		VoteGranted:  p.GetVoteGranted(),
		Success:      p.GetSuccess(),
		MatchIndex:   p.GetMatchIndex(),
		ReadID:       p.GetReadId(),
	}
	if n := len(p.GetEntries()); n > 0 {
		m.Entries = make([]raft.Entry, n)
		for i, e := range p.GetEntries() {
			et := raft.EntryType(e.GetType())
			if et != raft.EntryNormal && et != raft.EntryNoop {
				return raft.Message{}, fmt.Errorf("transport: unknown entry type %d", e.GetType())
			}
			m.Entries[i] = raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Type: et, Data: e.GetData()}
		}
	}
	if s := p.GetSnapshot(); s != nil {
		m.Snapshot = &raft.Snapshot{Meta: raft.SnapshotMeta{Index: s.GetIndex(), Term: s.GetTerm()}, Data: s.GetData()}
	}
	return m, nil
}
