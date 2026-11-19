package security

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// pki writes a CA and certificates for a node and an outsider (signed by
// another CA) and returns the directory.
func pki(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := WriteFiles(dir, [][]string{{"127.0.0.1", "localhost"}}, time.Hour); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	if _, err := WriteFiles(other, [][]string{{"127.0.0.1"}}, time.Hour); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestWriteFiles(t *testing.T) {
	dir := t.TempDir()
	files, err := WriteFiles(dir, [][]string{{"node1", "10.0.0.1"}, {"node2"}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 6 {
		t.Fatalf("files %v", files)
	}
	for _, name := range []string{"ca.key", "node1.key", "node2.key"} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v %v", name, fi.Mode(), err)
		}
	}
	if _, err := ServerTLS(filepath.Join(dir, "node1.crt"), filepath.Join(dir, "node1.key"), filepath.Join(dir, "ca.crt")); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteFiles(dir, [][]string{{"node3"}}, time.Hour); err == nil {
		t.Fatal("overwrote an existing CA")
	}
}

type admin struct {
	pb.UnimplementedAdminServiceServer
}

func (admin) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{Id: 1}, nil
}

type raftSvc struct {
	pb.UnimplementedRaftServiceServer
}

func (raftSvc) Stream(s pb.RaftService_StreamServer) error {
	return s.SendAndClose(&pb.StreamResponse{})
}

func serveGRPC(t *testing.T, g Guard, tlsCfg *tls.Config) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(g.ServerOptions(tlsCfg)...)
	pb.RegisterAdminServiceServer(gs, admin{})
	pb.RegisterRaftServiceServer(gs, raftSvc{})
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

// calls returns the codes of an admin call and of a Raft stream.
func calls(t *testing.T, addr string, opts []grpc.DialOption) (adminCode, raftCode codes.Code) {
	t.Helper()
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = pb.NewAdminServiceClient(conn).Status(ctx, &pb.StatusRequest{})
	adminCode = status.Code(err)
	s, err := pb.NewRaftServiceClient(conn).Stream(ctx)
	if err == nil {
		_, err = s.CloseAndRecv()
	}
	return adminCode, status.Code(err)
}

func TestMutualTLSAndToken(t *testing.T) {
	dir := pki(t)
	p := func(n string) string { return filepath.Join(dir, n) }
	srv, err := ServerTLS(p("127.0.0.1.crt"), p("127.0.0.1.key"), p("ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	addr := serveGRPC(t, Guard{Token: "s3cret", MutualTLS: true}, srv)
	tlsFor := func(ca, cert, key string) *tls.Config {
		c, err := ClientTLS(ca, cert, key)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	node := tlsFor(p("ca.crt"), p("127.0.0.1.crt"), p("127.0.0.1.key"))
	app := tlsFor(p("ca.crt"), "", "")
	outsider := tlsFor(p("ca.crt"), p("other/127.0.0.1.crt"), p("other/127.0.0.1.key"))
	for _, tc := range []struct {
		name        string
		opts        []grpc.DialOption
		admin, raft codes.Code
	}{
		{"node with token", DialOptions(node, "s3cret"), codes.OK, codes.OK},
		{"node without token", DialOptions(node, ""), codes.Unauthenticated, codes.OK},
		{"app with token", DialOptions(app, "s3cret"), codes.OK, codes.Unauthenticated},
		{"app with wrong token", DialOptions(app, "guess"), codes.Unauthenticated, codes.Unauthenticated},
		{"app without token", DialOptions(app, ""), codes.Unauthenticated, codes.Unauthenticated},
		{"plaintext", DialOptions(nil, "s3cret"), codes.Unavailable, codes.Unavailable},
	} {
		a, r := calls(t, addr, tc.opts)
		if a != tc.admin || r != tc.raft {
			t.Errorf("%s: admin %v raft %v, want %v %v", tc.name, a, r, tc.admin, tc.raft)
		}
	}
	// A certificate from another CA fails the TLS handshake.
	if a, _ := calls(t, addr, DialOptions(outsider, "s3cret")); a != codes.Unavailable {
		t.Errorf("outsider: admin %v", a)
	}
	// The server is verified too: without the cluster CA the system roots
	// do not trust it.
	if a, _ := calls(t, addr, DialOptions(tlsFor("", "", ""), "s3cret")); a != codes.Unavailable {
		t.Errorf("unknown server CA: admin %v", a)
	}
}

func TestTokenWithoutTLSGuardsRaftToo(t *testing.T) {
	addr := serveGRPC(t, Guard{Token: "s3cret"}, nil)
	if a, r := calls(t, addr, DialOptions(nil, "s3cret")); a != codes.OK || r != codes.OK {
		t.Fatalf("with token: %v %v", a, r)
	}
	if a, r := calls(t, addr, DialOptions(nil, "")); a != codes.Unauthenticated || r != codes.Unauthenticated {
		t.Fatalf("without token: %v %v", a, r)
	}
	open := serveGRPC(t, Guard{}, nil)
	if a, r := calls(t, open, DialOptions(nil, "")); a != codes.OK || r != codes.OK {
		t.Fatalf("no guard: %v %v", a, r)
	}
}

func TestHTTP(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	h := HTTP("s3cret", ok, "/healthz")
	do := func(path string, set func(*http.Request)) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		if set != nil {
			set(r)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := do("/healthz", nil); w.Code != 200 {
		t.Fatalf("open path: %d", w.Code)
	}
	if w := do("/", nil); w.Code != 401 || w.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("no credentials: %d %v", w.Code, w.Header())
	}
	if w := do("/metrics", func(r *http.Request) { r.SetBasicAuth("anyone", "s3cret") }); w.Code != 200 {
		t.Fatalf("basic: %d", w.Code)
	}
	if w := do("/metrics", func(r *http.Request) { r.Header.Set("Authorization", "Bearer s3cret") }); w.Code != 200 {
		t.Fatalf("bearer: %d", w.Code)
	}
	if w := do("/metrics", func(r *http.Request) { r.SetBasicAuth("x", "guess") }); w.Code != 401 {
		t.Fatalf("wrong: %d", w.Code)
	}
	if HTTP("", ok) == nil {
		t.Fatal("nil handler")
	}
}

func TestReadToken(t *testing.T) {
	f := filepath.Join(t.TempDir(), "token")
	os.WriteFile(f, []byte("  abc\n"), 0o600)
	if tok, err := ReadToken(f); err != nil || tok != "abc" {
		t.Fatalf("%q %v", tok, err)
	}
	os.WriteFile(f, []byte("\n"), 0o600)
	if _, err := ReadToken(f); err == nil {
		t.Fatal("empty token accepted")
	}
}
