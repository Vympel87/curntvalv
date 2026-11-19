// Package security holds TLS and token authentication for the gRPC and HTTP
// listeners of a node and for the clients that call them (design D15).
//
// TLS on the gRPC port is mutual between nodes: a node presents its
// certificate when it opens a Raft stream, and Raft streams are accepted only
// with a client certificate signed by the cluster CA. Applications and the
// CLI connect with plain TLS. A token, when configured, is required on the
// admin and subscribe APIs and on the HTTP API; without TLS it also guards
// Raft streams.
package security

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// raftService is the gRPC service that carries node-to-node traffic.
const raftService = "/curntvalv.v1.RaftService/"

// ReadToken reads a token from a file, trimming surrounding white space.
func ReadToken(file string) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("security: read token: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("security: token file %s is empty", file)
	}
	return tok, nil
}

func loadPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("security: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("security: no certificate in %s", caFile)
	}
	return pool, nil
}

// ServerTLS returns the TLS configuration of a node's listeners: its own
// certificate, and client certificates verified against the CA when one is
// presented (nodes present one, applications do not).
func ServerTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("security: load certificate: %w", err)
	}
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ClientTLS returns the TLS configuration of a client: servers are verified
// against caFile (the system roots if empty), and certFile/keyFile, if set,
// are presented as client certificate (node to node).
func ClientTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pool, err := loadPool(caFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("security: load certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// DialOptions returns the gRPC dial options for a client: TLS with tlsCfg
// (plaintext if nil) and the token on every call (none if empty).
func DialOptions(tlsCfg *tls.Config, token string) []grpc.DialOption {
	var opts []grpc.DialOption
	if tlsCfg != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenCreds(token)))
	}
	return opts
}

type tokenCreds string

func (t tokenCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

// RequireTransportSecurity is false: a token without TLS is allowed (and
// warned about by the node) for trusted networks.
func (tokenCreds) RequireTransportSecurity() bool { return false }

// Guard checks gRPC calls on a node.
type Guard struct {
	// Token is required on every call except Raft streams with a verified
	// client certificate. Empty: no token needed.
	Token string
	// MutualTLS means the listener verifies client certificates; Raft
	// streams then need one.
	MutualTLS bool
}

func (g Guard) check(ctx context.Context, method string) error {
	if strings.HasPrefix(method, raftService) {
		if g.MutualTLS {
			if verifiedPeer(ctx) {
				return nil
			}
			return status.Error(codes.Unauthenticated, "raft stream needs a client certificate signed by the cluster CA")
		}
	}
	if g.Token == "" {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if tok, ok := strings.CutPrefix(v, "Bearer "); ok && equal(tok, g.Token) {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "missing or wrong token")
}

func verifiedPeer(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	return ok && len(ti.State.VerifiedChains) > 0
}

func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// ServerOptions returns the gRPC server options for a node: TLS with tlsCfg
// (plaintext if nil) and the guard on every call.
func (g Guard) ServerOptions(tlsCfg *tls.Config) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if err := g.check(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if err := g.check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
			return h(srv, ss)
		}),
	}
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	return opts
}

// HTTP requires the token on every path except open ones, as HTTP Basic
// (any user name, the token as password: browsers ask for it, and their
// EventSource sends it) or as a Bearer token. Without a token it changes
// nothing.
func HTTP(token string, next http.Handler, open ...string) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range open {
			if r.URL.Path == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		if _, pass, ok := r.BasicAuth(); ok && equal(pass, token) {
			next.ServeHTTP(w, r)
			return
		}
		if tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && equal(tok, token) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="curntValv", charset="UTF-8"`)
		http.Error(w, "unauthorized: the token is the password (any user name)", http.StatusUnauthorized)
	})
}
