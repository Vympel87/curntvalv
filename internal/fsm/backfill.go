package fsm

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// backfillKey is the key of every backfill meta command; the body is a
// pb.BackfillCommand.
const backfillKey = "backfill"

// MaxBackfillTable is the longest allowed table name in a backfill command.
const MaxBackfillTable = 2*63 + 1

// EncodeBackfill returns the log payload of a backfill command (design D17).
func EncodeBackfill(cmd *pb.BackfillCommand) ([]byte, error) {
	for _, t := range slices.Concat(cmd.GetStart(), cmd.GetCancel()) {
		if !validTable(t) {
			return nil, fmt.Errorf("%w: bad table name %q (want namespace.name)", ErrInvalid, t)
		}
	}
	if t := cmd.GetFailTable(); t != "" && !validTable(t) {
		return nil, fmt.Errorf("%w: bad table name %q (want namespace.name)", ErrInvalid, t)
	}
	body, err := marshalOpts.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode backfill command: %w", err)
	}
	b := []byte{byte(KindMeta), byte(opBackfill)}
	b = appendBytes(b, []byte(backfillKey))
	return append(b, body...), nil
}

func validTable(t string) bool {
	ns, name, ok := strings.Cut(t, ".")
	return ok && ns != "" && name != "" && len(t) <= MaxBackfillTable
}

func (f *FSM) applyBackfillCommand(index uint64, body []byte) error {
	cmd := &pb.BackfillCommand{}
	if err := proto.Unmarshal(body, cmd); err != nil {
		return fmt.Errorf("%w: backfill command: %v", ErrCorrupt, err)
	}
	for _, t := range cmd.GetStart() {
		if bf, ok := f.backfills[t]; ok && bf.GetState() == pb.BackfillState_BACKFILL_STATE_PENDING {
			continue
		}
		f.backfills[t] = &pb.Backfill{Table: t, Id: index, State: pb.BackfillState_BACKFILL_STATE_PENDING}
	}
	for _, t := range cmd.GetCancel() {
		if bf, ok := f.backfills[t]; ok && bf.GetState() == pb.BackfillState_BACKFILL_STATE_PENDING {
			bf.State, bf.EndIndex = pb.BackfillState_BACKFILL_STATE_CANCELLED, index
		}
	}
	if t := cmd.GetFailTable(); t != "" {
		if bf, ok := f.backfills[t]; ok && bf.GetId() == cmd.GetFailId() && bf.GetState() == pb.BackfillState_BACKFILL_STATE_PENDING {
			bf.State, bf.EndIndex, bf.Error = pb.BackfillState_BACKFILL_STATE_FAILED, index, cmd.GetFailError()
		}
	}
	return nil
}

// applyBackfillChunk records the progress of a stored backfill chunk. It
// counts only if it continues the stored progress of the same pending
// backfill; otherwise (a cancelled or restarted backfill) its rows are in the
// log but the progress is not.
func (f *FSM) applyBackfillChunk(index uint64, ch *pb.BackfillChunk, rows int) {
	bf, ok := f.backfills[ch.GetTable()]
	if !ok || bf.GetId() != ch.GetId() || bf.GetState() != pb.BackfillState_BACKFILL_STATE_PENDING ||
		!slices.EqualFunc(bf.GetLastKey(), ch.GetAfterKey(), bytes.Equal) {
		return
	}
	if len(ch.GetLastKey()) > 0 {
		bf.LastKey = cloneKey(ch.GetLastKey())
	}
	bf.Rows += uint64(rows)
	if ch.GetDone() {
		bf.State, bf.EndIndex = pb.BackfillState_BACKFILL_STATE_DONE, index
	}
}

func cloneKey(k [][]byte) [][]byte {
	out := make([][]byte, len(k))
	for i, v := range k {
		out[i] = slices.Clone(v)
	}
	return out
}

// Backfills returns all backfills in the order they were started.
func (f *FSM) Backfills() []*pb.Backfill {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*pb.Backfill, 0, len(f.backfills))
	for _, bf := range f.backfills {
		out = append(out, proto.Clone(bf).(*pb.Backfill))
	}
	slices.SortFunc(out, func(a, b *pb.Backfill) int {
		return cmp.Or(cmp.Compare(a.GetId(), b.GetId()), strings.Compare(a.GetTable(), b.GetTable()))
	})
	return out
}
