package sink

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// pickySink refuses batches holding a poison row, and everything while
// down.
type pickySink struct {
	mu     sync.Mutex
	poison map[string]bool
	down   bool
	rows   []string
}

func (s *pickySink) Deliver(_ context.Context, changes []event.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return errors.New("connection refused")
	}
	for _, c := range changes {
		if id := string(c.Event.GetAfter().GetValues()[0].GetData()); s.poison[id] {
			return errors.New("row " + id + " rejected: bad value")
		}
	}
	for _, c := range changes {
		s.rows = append(s.rows, string(c.Event.GetAfter().GetValues()[0].GetData()))
	}
	return nil
}

func (s *pickySink) set(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

// distinct returns the delivered rows without repeats, in first order.
func (s *pickySink) distinct() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.rows {
		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

func runPicky(t *testing.T, h *harness, s *pickySink, cfg Config) *Runner {
	t.Helper()
	cfg.Name, cfg.RetryMin, cfg.RetryMax = "test", time.Millisecond, 20*time.Millisecond
	r, err := NewRunner(cfg, s, h.node, event.NewReader(h.node, h.node.f), h.node.f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return r
}

func TestRunnerSkipsPoison(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1, 2, 3)
	h.node.ingest(t, 200, "orders", 4, 5)
	s := &pickySink{poison: map[string]bool{"3": true, "5": true}}
	r := runPicky(t, h, s, Config{SkipPoison: true, MaxAttempts: 2})
	waitFor(t, "rows delivered", func() bool { return len(s.distinct()) == 3 })
	waitFor(t, "offset at the end", func() bool { return h.offset("test") >= 2 })
	if got := s.distinct(); !slices.Equal(got, []string{"1", "2", "4"}) {
		t.Fatalf("delivered %v", got)
	}
	sk := h.node.f.SinkSkips("test")
	if sk.Total != 2 || sk.Recent[0].GetIndex() != 1 || sk.Recent[1].GetIndex() != 2 || sk.Recent[0].GetError() != "row 3 rejected: bad value" {
		t.Fatalf("skips %+v", sk)
	}
	if st := r.Status(); st.Skipped != 2 || st.State != StateRunning {
		t.Fatalf("status %+v", st)
	}
	// A poison change as the very last one is found with the canary.
	h.node.ingest(t, 300, "orders", 6, 7)
	s.mu.Lock()
	s.poison["7"] = true
	s.mu.Unlock()
	waitFor(t, "last change skipped", func() bool { return h.node.f.SinkSkips("test").Total == 3 })
}

// A sink that is down refuses everything: nothing is skipped, and
// delivery continues when it comes back.
func TestRunnerDoesNotSkipWhileDown(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1)
	s := &pickySink{poison: map[string]bool{}}
	runPicky(t, h, s, Config{SkipPoison: true, MaxAttempts: 2})
	waitFor(t, "row 1", func() bool { return len(s.distinct()) == 1 })
	s.set(true)
	h.node.ingest(t, 200, "orders", 2, 3)
	time.Sleep(300 * time.Millisecond)
	if sk := h.node.f.SinkSkips("test"); sk.Total != 0 {
		t.Fatalf("skipped while the sink was down: %+v", sk.Recent)
	}
	s.set(false)
	waitFor(t, "rows after the outage", func() bool { return slices.Equal(s.distinct(), []string{"1", "2", "3"}) })
}

// Without SkipPoison a poison change blocks the sink, as before.
func TestRunnerRetriesPoisonByDefault(t *testing.T) {
	h := newHarness(t)
	h.node.ingest(t, 100, "orders", 1, 2)
	s := &pickySink{poison: map[string]bool{"2": true}}
	r := runPicky(t, h, s, Config{MaxAttempts: 2})
	time.Sleep(300 * time.Millisecond)
	if len(s.distinct()) != 0 || h.node.f.SinkSkips("test").Total != 0 || r.Status().State != StateRetrying {
		t.Fatalf("delivered %v, status %+v", s.distinct(), r.Status())
	}
}
