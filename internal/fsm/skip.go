package fsm

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Skipped changes of sinks with on_error=skip (design D28).

// MaxSkipsKept is how many skipped changes are kept per sink; older ones
// are only counted.
const MaxSkipsKept = 100

// SinkSkips is what a sink passed over.
type SinkSkips struct {
	Total  uint64
	Recent []*pb.SinkSkip // oldest first, at most MaxSkipsKept
}

// EncodeSinkSkip returns the log payload that records a skipped change.
func EncodeSinkSkip(s *pb.SinkSkip) ([]byte, error) {
	if !ValidSinkName(s.GetSink()) {
		return nil, fmt.Errorf("%w: bad sink name %q", ErrInvalid, s.GetSink())
	}
	body, err := marshalOpts.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode sink skip: %w", err)
	}
	b := []byte{byte(KindMeta), byte(opSinkSkip)}
	b = appendBytes(b, []byte(s.GetSink()))
	return append(b, body...), nil
}

func (f *FSM) applySinkSkip(name, body []byte) error {
	s := &pb.SinkSkip{}
	if err := proto.Unmarshal(body, s); err != nil || s.GetSink() != string(name) {
		return fmt.Errorf("%w: sink skip", ErrCorrupt)
	}
	cur := f.skips[string(name)]
	// The same change recorded twice (a retried proposal) counts once.
	if n := len(cur.Recent); n > 0 && cur.Recent[n-1].GetIndex() == s.GetIndex() && cur.Recent[n-1].GetId() == s.GetId() {
		return nil
	}
	cur.Total++
	cur.Recent = append(cur.Recent, s)
	if len(cur.Recent) > MaxSkipsKept {
		cur.Recent = slices.Delete(cur.Recent, 0, len(cur.Recent)-MaxSkipsKept)
	}
	f.skips[string(name)] = cur
	return nil
}

// SinkSkips returns what sink name passed over.
func (f *FSM) SinkSkips(name string) SinkSkips {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cur := f.skips[name]
	out := SinkSkips{Total: cur.Total, Recent: make([]*pb.SinkSkip, len(cur.Recent))}
	for i, s := range cur.Recent {
		out.Recent[i] = proto.Clone(s).(*pb.SinkSkip)
	}
	return out
}
