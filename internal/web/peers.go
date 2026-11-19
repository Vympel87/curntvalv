package web

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// GRPCPeers asks other nodes for their status over the admin API, keeping
// one connection per node.
type GRPCPeers struct {
	opts  []grpc.DialOption
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCPeers returns a client. Connections are made on first use, with
// opts as credentials (plaintext if none).
func NewGRPCPeers(opts ...grpc.DialOption) *GRPCPeers {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GRPCPeers{opts: opts, conns: map[string]*grpc.ClientConn{}}
}

// NodeStatus returns the status of the node at addr.
func (p *GRPCPeers) NodeStatus(ctx context.Context, addr string) (*pb.StatusResponse, error) {
	p.mu.Lock()
	conn, ok := p.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, p.opts...)
		if err != nil {
			p.mu.Unlock()
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		p.conns[addr] = conn
	}
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return pb.NewAdminServiceClient(conn).Status(ctx, &pb.StatusRequest{})
}

// Close closes all connections.
func (p *GRPCPeers) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		c.Close()
		delete(p.conns, addr)
	}
	return nil
}
