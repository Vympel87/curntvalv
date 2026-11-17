package fsm

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// SourceState is the ingest position of the Postgres source (design D3).
// It changes only by applying TxBatch entries, so the events and the
// checkpoint are always consistent.
type SourceState struct {
	// CommitLSN is the commit LSN of the last transaction stored completely.
	CommitLSN uint64
	// EndLSN is the end LSN of that commit record; what may be acknowledged
	// to Postgres.
	EndLSN uint64
	// PartialLSN is non-zero while a large transaction is stored only in
	// part; PartialNextSeq is the sequence number of its next event.
	PartialLSN     uint64
	PartialNextSeq uint32
	// Anomalies counts batches that did not continue the stored position and
	// were therefore ignored. The source never produces them; a non-zero
	// value means a bug and is reported loudly.
	Anomalies uint64
}

var marshalOpts = proto.MarshalOptions{Deterministic: true}

// EncodeTxBatch returns the log payload for a transaction batch.
func EncodeTxBatch(tb *pb.TxBatch) ([]byte, error) {
	body, err := marshalOpts.Marshal(tb)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode tx batch: %w", err)
	}
	return append([]byte{byte(KindTxBatch)}, body...), nil
}

// DecodeTxBatch decodes a log payload made by EncodeTxBatch. ok is false if
// the payload is of another kind.
func DecodeTxBatch(b []byte) (tb *pb.TxBatch, ok bool, err error) {
	if len(b) == 0 || Kind(b[0]) != KindTxBatch {
		return nil, false, nil
	}
	tb = &pb.TxBatch{}
	if err := proto.Unmarshal(b[1:], tb); err != nil {
		return nil, true, fmt.Errorf("%w: tx batch: %v", ErrCorrupt, err)
	}
	return tb, true, nil
}

// SchemaID returns the content-derived id of a schema: the first 8 bytes of
// the SHA-256 of its deterministic encoding without the id field.
func SchemaID(s *pb.Schema) uint64 {
	c := proto.Clone(s).(*pb.Schema)
	c.Id = 0
	b, err := marshalOpts.Marshal(c)
	if err != nil {
		// Marshal of a valid message cannot fail.
		return 0
	}
	sum := sha256.Sum256(b)
	return max(binary.BigEndian.Uint64(sum[:8]), 1)
}

// applyTxBatch advances the source position. A batch must continue exactly
// where the stored position ends: the next part of the partial transaction,
// or the first part of a newer one. Anything else is counted as an anomaly
// and ignored. Ignoring (instead of failing) is deterministic on every node
// and avoids a poison entry that would stop the cluster forever.
func (f *FSM) applyTxBatch(index uint64, b []byte) error {
	tb := &pb.TxBatch{}
	if err := proto.Unmarshal(b, tb); err != nil {
		return fmt.Errorf("%w: tx batch: %v", ErrCorrupt, err)
	}
	s := &f.src
	var ok bool
	if s.PartialLSN != 0 {
		ok = tb.GetCommitLsn() == s.PartialLSN && tb.GetFirstSeq() == s.PartialNextSeq
	} else {
		ok = tb.GetCommitLsn() > s.CommitLSN && tb.GetFirstSeq() == 0
	}
	if !ok {
		s.Anomalies++
		return nil
	}
	if len(tb.GetEvents()) > 0 {
		f.lastChange = index
	}
	for _, sc := range tb.GetSchemas() {
		if _, known := f.schemas[sc.GetId()]; !known {
			f.schemas[sc.GetId()] = sc
		}
	}
	if tb.GetFinal() {
		s.CommitLSN, s.EndLSN = tb.GetCommitLsn(), tb.GetEndLsn()
		s.PartialLSN, s.PartialNextSeq = 0, 0
	} else {
		s.PartialLSN = tb.GetCommitLsn()
		s.PartialNextSeq = tb.GetFirstSeq() + uint32(len(tb.GetEvents()))
	}
	return nil
}

// Source returns the current ingest position.
func (f *FSM) Source() SourceState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.src
}

// Schema returns the stored schema with the given id.
func (f *FSM) Schema(id uint64) (*pb.Schema, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := f.schemas[id]
	return s, ok
}

// HasSchema reports whether a schema id is stored.
func (f *FSM) HasSchema(id uint64) bool {
	_, ok := f.Schema(id)
	return ok
}
