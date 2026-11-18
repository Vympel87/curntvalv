// Package web is the HTTP side of a node (design D13): health checks,
// Prometheus metrics, a JSON view of the cluster, a live change feed and the
// embedded dashboard. It only reads; changes go through the gRPC admin API.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Local is this node's admin status. *server.Admin implements it.
type Local interface {
	Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error)
}

// Peers fetches the status of other nodes. *GRPCPeers implements it.
type Peers interface {
	NodeStatus(ctx context.Context, addr string) (*pb.StatusResponse, error)
}

// ChangeReader reads row changes from the local log. *event.Reader
// implements it.
type ChangeReader interface {
	Read(ctx context.Context, from uint64, max int, filter event.Filter) (event.Result, error)
}

// Config configures a Server.
type Config struct {
	Version string
	// Peers lists every node's gRPC address in a stable order, this node
	// included; /api/status shows them in that order.
	Peers []string
	// Self is this node's address in Peers.
	Self string
	// Counters returns the event counters for /metrics.
	Counters func(ctx context.Context) Counters
	// MaxFeeds limits concurrent /api/events streams (default 16).
	MaxFeeds int
	Logger   *slog.Logger
}

// Server serves a node's HTTP endpoints.
type Server struct {
	cfg    Config
	local  Local
	peers  Peers
	reader ChangeReader
	log    *slog.Logger
	feeds  chan struct{} // semaphore for /api/events
	mux    *http.ServeMux
}

//go:embed static
var static embed.FS

// New returns a server. It does no I/O.
func New(cfg Config, local Local, peers Peers, reader ChangeReader) *Server {
	if cfg.MaxFeeds <= 0 {
		cfg.MaxFeeds = 16
	}
	if cfg.Counters == nil {
		cfg.Counters = func(context.Context) Counters { return Counters{} }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{cfg: cfg, local: local, peers: peers, reader: reader,
		log: cfg.Logger.With("component", "web"), feeds: make(chan struct{}, cfg.MaxFeeds), mux: http.NewServeMux()}
	files, _ := fs.Sub(static, "static") // the directory is embedded, so this cannot fail
	s.mux.Handle("GET /", http.FileServerFS(files))
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	s.mux.HandleFunc("GET /metrics", s.metrics)
	s.mux.HandleFunc("GET /api/status", s.apiStatus)
	s.mux.HandleFunc("GET /api/events", s.apiEvents)
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) localStatus(ctx context.Context) (*pb.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.local.Status(ctx, &pb.StatusRequest{})
}

// healthz: the node is running (its Raft loop answers).
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if _, err := s.localStatus(r.Context()); err != nil {
		http.Error(w, "raft not running: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok\n"))
}

// readyz: the node is running and knows a leader, so it is part of a
// working cluster.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	st, err := s.localStatus(r.Context())
	switch {
	case err != nil:
		http.Error(w, "raft not running: "+err.Error(), http.StatusServiceUnavailable)
	case st.GetLeader() == 0:
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
	default:
		w.Write([]byte("ok\n"))
	}
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	st, err := s.localStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write(writeMetrics(st, s.cfg.Counters(r.Context()), s.cfg.Version))
}

// clusterView is the JSON of /api/status.
type clusterView struct {
	Version string     `json:"version"`
	Self    string     `json:"self"`
	Nodes   []nodeView `json:"nodes"`
	// Pipeline is the leader's view of source, sinks and retention; null
	// when no node answers as leader.
	Pipeline *pipelineView `json:"pipeline"`
}

type nodeView struct {
	Addr     string `json:"addr"`
	Error    string `json:"error,omitempty"` // set when unreachable
	ID       uint64 `json:"id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	Leader   uint64 `json:"leader"`
	First    uint64 `json:"first_index"`
	Last     uint64 `json:"last_index"`
	Commit   uint64 `json:"commit_index"`
	Applied  uint64 `json:"applied_index"`
	Snapshot uint64 `json:"snapshot_index"`
}

type pipelineView struct {
	Node        uint64         `json:"node"`
	SourceState string         `json:"source_state"`
	SourceError string         `json:"source_error"`
	Sinks       []sinkView     `json:"sinks"`
	Retention   *retentionView `json:"retention"`
}

type sinkView struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Policy     string `json:"policy"`
	State      string `json:"state"`
	Offset     uint64 `json:"offset"`
	Lag        uint64 `json:"lag"`
	Delivered  uint64 `json:"delivered"`
	Failures   uint64 `json:"failures"`
	Generation uint64 `json:"generation"`
	LastError  string `json:"last_error"`
}

type retentionView struct {
	MaxAgeSeconds int64  `json:"max_age_seconds"`
	MaxBytes      int64  `json:"max_bytes"`
	LogBytes      int64  `json:"log_bytes"`
	CompactLimit  uint64 `json:"compact_limit"`
	BlockedBy     string `json:"blocked_by"`
}

func (s *Server) apiStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	view := clusterView{Version: s.cfg.Version, Self: s.cfg.Self, Nodes: make([]nodeView, len(s.cfg.Peers))}
	statuses := make([]*pb.StatusResponse, len(s.cfg.Peers))
	var wg sync.WaitGroup
	for i, addr := range s.cfg.Peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var st *pb.StatusResponse
			var err error
			if addr == s.cfg.Self {
				st, err = s.local.Status(ctx, &pb.StatusRequest{})
			} else {
				st, err = s.peers.NodeStatus(ctx, addr)
			}
			if err != nil {
				view.Nodes[i] = nodeView{Addr: addr, State: "unreachable", Error: err.Error()}
				return
			}
			statuses[i] = st
			view.Nodes[i] = nodeView{Addr: addr, ID: st.GetId(), State: st.GetState(), Term: st.GetTerm(), Leader: st.GetLeader(),
				First: st.GetFirstIndex(), Last: st.GetLastIndex(), Commit: st.GetCommitIndex(), Applied: st.GetLastApplied(),
				Snapshot: st.GetSnapshotIndex()}
		}()
	}
	wg.Wait()
	for _, st := range statuses {
		if st.GetState() == "leader" {
			view.Pipeline = pipeline(st)
			break
		}
	}
	writeJSON(w, view)
}

func pipeline(st *pb.StatusResponse) *pipelineView {
	p := &pipelineView{Node: st.GetId(), SourceState: st.GetSourceState(), SourceError: st.GetSourceError(), Sinks: []sinkView{}}
	for _, k := range st.GetSinks() {
		p.Sinks = append(p.Sinks, sinkView{Name: k.GetName(), Type: k.GetType(), Policy: k.GetPolicy(), State: k.GetState(),
			Offset: k.GetOffset(), Lag: k.GetLag(), Delivered: k.GetDelivered(), Failures: k.GetFailures(),
			Generation: k.GetGeneration(), LastError: k.GetLastError()})
	}
	if r := st.GetRetention(); r != nil {
		p.Retention = &retentionView{MaxAgeSeconds: r.GetMaxAgeSeconds(), MaxBytes: r.GetMaxBytes(), LogBytes: r.GetLogBytes(),
			CompactLimit: r.GetCompactLimit(), BlockedBy: r.GetBlockedBy()}
	}
	return p
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	// An error means the client went away; there is nobody left to tell.
	_ = json.NewEncoder(w).Encode(v)
}
