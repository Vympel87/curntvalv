package sink

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// timedSink records when each Deliver call came and how many changes it had.
type timedSink struct {
	mu    sync.Mutex
	at    []time.Time
	sizes []int
}

func (s *timedSink) Deliver(_ context.Context, changes []event.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = append(s.at, time.Now())
	s.sizes = append(s.sizes, len(changes))
	return nil
}

func (s *timedSink) calls() ([]time.Time, []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.at...), append([]int(nil), s.sizes...)
}

func total(sizes []int) int {
	n := 0
	for _, s := range sizes {
		n += s
	}
	return n
}

func TestRunnerLingerCollects(t *testing.T) {
	h := newHarness(t)
	s := &callSink{}
	runSink(t, h, s, Config{Linger: 400 * time.Millisecond, MaxBatch: 100})
	start := time.Now()
	for i := range 3 {
		h.node.ingest(t, uint64(100*(i+1)), "orders", i+1)
		time.Sleep(50 * time.Millisecond)
	}
	waitCalls(t, s, "orders:1/0 orders:2/0 orders:3/0")
	if d := time.Since(start); d < 350*time.Millisecond {
		t.Fatalf("delivered after %v, before the linger ended", d)
	}
	// Nothing is stored past the changes while they wait.
	waitFor(t, "offset", func() bool { return h.offset("test") >= 3 })
}

func TestRunnerLingerFlushesFullBatch(t *testing.T) {
	h := newHarness(t)
	s := &callSink{}
	runSink(t, h, s, Config{Linger: time.Hour, MaxBatch: 2})
	h.node.ingest(t, 100, "orders", 1)
	time.Sleep(100 * time.Millisecond)
	if got := s.got(); len(got) != 0 {
		t.Fatalf("delivered before the batch was full: %q", got)
	}
	if got := h.offset("test"); got != 0 {
		t.Fatalf("offset %d stored for a change still waiting", got)
	}
	h.node.ingest(t, 200, "orders", 2, 3)
	// Full: delivered at once, in batches of at most 2.
	waitCalls(t, s, "orders:1/0 orders:2/0", "orders:3/0")
}

func TestRunnerMaxRate(t *testing.T) {
	h := newHarness(t)
	ids := make([]int, 60)
	for i := range ids {
		ids[i] = i
	}
	h.node.ingest(t, 100, "orders", ids...)
	s := &timedSink{}
	runSink(t, h, s, Config{MaxRate: 20})
	waitFor(t, "all changes", func() bool { _, sizes := s.calls(); return total(sizes) == 60 })
	at, sizes := s.calls()
	for _, n := range sizes {
		if n > 20 {
			t.Fatalf("a delivery of %d changes at 20/s: %v", n, sizes)
		}
	}
	// 60 changes at 20/s: the last batch starts about 2s after the first.
	if d := at[len(at)-1].Sub(at[0]); d < 1900*time.Millisecond {
		t.Fatalf("60 changes delivered within %v at 20/s (%v)", d, sizes)
	}
}
