package retention

import (
	"strings"
	"testing"
	"time"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
	"github.com/Vympel87/curntvalv/internal/wal"
)

var (
	_ raft.Retention = (*Policy)(nil)
	_ Segments       = (*wal.Log)(nil)
	_ Positions      = (*fsm.FSM)(nil)
)

type fakeSegs []wal.SegmentInfo

func (f fakeSegs) Segments() []wal.SegmentInfo { return f }

type fakePos map[string]uint64

func (f fakePos) SinkPosition(name string) (fsm.SinkPosition, bool) {
	off, ok := f[name]
	return fsm.SinkPosition{Offset: off}, ok
}

// Subscriptions returns the entries named "sub:NAME" (block) and
// "drop:NAME" (drop) as subscriptions.
func (f fakePos) Subscriptions() []*pb.Subscription {
	var out []*pb.Subscription
	for k, off := range f {
		if name, ok := strings.CutPrefix(k, "sub:"); ok {
			out = append(out, &pb.Subscription{Name: name, Offset: off})
		}
		if name, ok := strings.CutPrefix(k, "drop:"); ok {
			out = append(out, &pb.Subscription{Name: name, Offset: off, Drop: true})
		}
	}
	return out
}

var now = time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

// Four segments of 100 entries and 1000 bytes each, written 4, 3, 2 and 1
// days ago; the last one is active.
func segs() fakeSegs {
	var s fakeSegs
	for i := range 4 {
		s = append(s, wal.SegmentInfo{First: uint64(i*100 + 1), Last: uint64(i*100 + 100), Bytes: 1000,
			Written: now.Add(-time.Duration(4-i) * 24 * time.Hour)})
	}
	return s
}

func TestPolicy(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		name string
		cfg  Config
		segs fakeSegs
		pos  fakePos
		want Status
	}{
		{"keep everything", Config{}, segs(), nil, Status{}},
		{"age", Config{MaxAge: 60 * time.Hour}, segs(), nil, Status{Limit: 200, Wanted: 200}},
		{"age never removes the active segment", Config{MaxAge: time.Hour}, segs(), nil, Status{Limit: 300, Wanted: 300}},
		{"size", Config{MaxBytes: 2500}, segs(), nil, Status{Limit: 200, Wanted: 200}},
		{"size exactly at limit", Config{MaxBytes: 3000}, segs(), nil, Status{Limit: 100, Wanted: 100}},
		{"size never removes the active segment", Config{MaxBytes: 1}, segs(), nil, Status{Limit: 300, Wanted: 300}},
		{"age or size, the larger cut wins", Config{MaxAge: 3*day + time.Hour, MaxBytes: 2500}, segs(), nil, Status{Limit: 200, Wanted: 200}},
		{"block sink behind", Config{MaxAge: time.Hour, Block: []string{"ch", "hook"}}, segs(),
			fakePos{"ch": 250, "hook": 120}, Status{Limit: 120, Wanted: 300, BlockedBy: "hook"}},
		{"block sink ahead", Config{MaxAge: 60 * time.Hour, Block: []string{"ch"}}, segs(),
			fakePos{"ch": 350}, Status{Limit: 200, Wanted: 200}},
		{"block sink without a position keeps everything", Config{MaxAge: time.Hour, Block: []string{"new"}}, segs(),
			fakePos{}, Status{Limit: 0, Wanted: 300, BlockedBy: "new"}},
		{"drop sinks are not listed", Config{MaxAge: time.Hour}, segs(), fakePos{"ch": 5}, Status{Limit: 300, Wanted: 300}},
		{"block subscription behind", Config{MaxAge: time.Hour, Block: []string{"ch"}}, segs(),
			fakePos{"ch": 250, "sub:billing": 90, "drop:audit": 10}, Status{Limit: 90, Wanted: 300, BlockedBy: "subscription billing"}},
		{"new block subscription keeps everything", Config{MaxAge: time.Hour}, segs(),
			fakePos{"sub:new": 0}, Status{Limit: 0, Wanted: 300, BlockedBy: "subscription new"}},
		{"single segment", Config{MaxAge: time.Nanosecond, MaxBytes: 1}, segs()[:1], nil, Status{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Now = func() time.Time { return now }
			p := New(tc.cfg, tc.segs, tc.pos)
			if got := p.Status(); got != tc.want {
				t.Fatalf("status %+v, want %+v", got, tc.want)
			}
			if got := p.CompactLimit(); got != tc.want.Limit {
				t.Fatalf("limit %d", got)
			}
		})
	}
}
