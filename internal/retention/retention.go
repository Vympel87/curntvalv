// Package retention decides how much of the log a node keeps (design D5).
// Old segments become removable by age or by total size; sinks with the
// block policy keep everything they have not delivered yet. Raft asks for
// the limit and cuts the log at the newest snapshot below it.
package retention

import (
	"time"

	"github.com/Vympel87/curntvalv/internal/fsm"
	"github.com/Vympel87/curntvalv/internal/wal"
)

// Segments describes the log's segments, oldest first; the last one is the
// active segment. *wal.Log implements it.
type Segments interface {
	Segments() []wal.SegmentInfo
}

// Positions reads stored sink positions. *fsm.FSM implements it.
type Positions interface {
	SinkPosition(name string) (fsm.SinkPosition, bool)
}

// Config configures a Policy. The zero value keeps everything.
type Config struct {
	// MaxAge makes a segment removable once its last write is older than
	// this. 0 means no age limit.
	MaxAge time.Duration
	// MaxBytes makes the oldest segments removable while the log is larger
	// than this. 0 means no size limit.
	MaxBytes int64
	// Block lists the sinks with the block policy: the log is never cut
	// past what they have delivered. A listed sink without a stored
	// position keeps the whole log.
	Block []string
	// Now returns the current time (default time.Now).
	Now func() time.Time
}

// Policy implements raft.Retention.
type Policy struct {
	cfg  Config
	segs Segments
	pos  Positions
}

// New returns a policy. It does no I/O.
func New(cfg Config, segs Segments, pos Positions) *Policy {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Policy{cfg: cfg, segs: segs, pos: pos}
}

// Status explains the current limit.
type Status struct {
	// Limit is the highest log index that may be removed.
	Limit uint64
	// Wanted is the limit from age and size alone, before sinks.
	Wanted uint64
	// BlockedBy names the block sink that holds the limit below Wanted,
	// if any.
	BlockedBy string
}

// CompactLimit returns the highest log index that may be removed.
func (p *Policy) CompactLimit() uint64 { return p.Status().Limit }

// Status computes the limit and what holds it back.
func (p *Policy) Status() Status {
	segs := p.segs.Segments()
	var wanted uint64
	if len(segs) > 1 {
		old := segs[:len(segs)-1] // the active segment is never removed
		if p.cfg.MaxAge > 0 {
			cutoff := p.cfg.Now().Add(-p.cfg.MaxAge)
			for _, s := range old {
				if !s.Written.Before(cutoff) {
					break
				}
				wanted = max(wanted, s.Last)
			}
		}
		if p.cfg.MaxBytes > 0 {
			var total int64
			for _, s := range segs {
				total += s.Bytes
			}
			for _, s := range old {
				if total <= p.cfg.MaxBytes {
					break
				}
				wanted = max(wanted, s.Last)
				total -= s.Bytes
			}
		}
	}
	st := Status{Limit: wanted, Wanted: wanted}
	for _, name := range p.cfg.Block {
		pos, _ := p.pos.SinkPosition(name)
		if pos.Offset < st.Limit {
			st.Limit, st.BlockedBy = pos.Offset, name
		}
	}
	return st
}
