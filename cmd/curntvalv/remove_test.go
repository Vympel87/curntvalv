package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// slowRemover removes a member but answers too late the first time, like a
// leader whose commit is slow.
type slowRemover struct {
	pb.UnimplementedAdminServiceServer
	mu      sync.Mutex
	members map[uint64]bool
	calls   int
	delay   time.Duration
}

func (s *slowRemover) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	ok := s.members[req.GetId()]
	delete(s.members, req.GetId())
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "raft: invalid membership change: server %d is not a member", req.GetId())
	}
	if first {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &pb.RemoveMemberResponse{Index: 9}, nil
}

func serveAdmin(t *testing.T, srv pb.AdminServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterAdminServiceServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestRemoveMemberAfterTimedOutAttempt(t *testing.T) {
	srv := &slowRemover{members: map[uint64]bool{1: true, 2: true}, delay: time.Second}
	cf := clientFlags{addrs: []string{serveAdmin(t, srv)},
		conn: connFlags{dial: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	// The first attempt removes node 1 but times out; the retry is told it
	// is not a member, which after that means removed.
	if err := removeMember(ctx, cf, 200*time.Millisecond, 1, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "node 1 removed") || srv.calls != 2 {
		t.Fatalf("output %q after %d calls", out.String(), srv.calls)
	}
	// Without an uncertain attempt "not a member" is an error.
	if err := removeMember(ctx, cf, time.Second, 7, &out); err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("removing a non-member: %v", err)
	}
}
