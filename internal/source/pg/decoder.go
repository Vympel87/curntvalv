// Package pg is the Postgres source: it reads committed transactions from a
// logical replication slot (pgoutput, protocol version 1) and turns them into
// TxBatch log entries (design D1, D3).
package pg

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"google.golang.org/protobuf/proto"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Registry tells which table schemas the cluster already stores.
// *fsm.FSM implements it.
type Registry interface {
	HasSchema(id uint64) bool
}

// Limits bound the size of one TxBatch. A transaction that exceeds them is
// split into several batches.
type Limits struct {
	MaxEvents int // default 1000
	MaxBytes  int // default 1 MiB (encoded events)
}

func (l Limits) withDefaults() Limits {
	if l.MaxEvents <= 0 {
		l.MaxEvents = 1000
	}
	if l.MaxBytes <= 0 {
		l.MaxBytes = 1 << 20
	}
	return l
}

var (
	// ErrProtocol means the replication stream did not look like pgoutput.
	ErrProtocol = errors.New("pg: unexpected replication message")
	// ErrPartialLost means a partly stored transaction was not sent again by
	// Postgres. Its remaining changes cannot be recovered.
	ErrPartialLost = errors.New("pg: partly stored transaction was not resent by Postgres")
)

// decoder turns pgoutput messages into TxBatches. It drops transactions that
// are already stored and continues a partly stored one where it stopped, so
// the log gets every event exactly once. Not safe for concurrent use.
type decoder struct {
	reg    Registry
	limits Limits

	relations map[uint32]*pb.Schema // latest definition per relation OID
	sent      map[uint64]bool       // schemas already put in a batch

	// Resume position, from the state machine.
	storedLSN      uint64 // drop transactions with commit LSN <= this
	partialLSN     uint64
	partialNextSeq uint32

	// Current transaction.
	inTx      bool
	skip      bool   // already stored
	skipUntil uint32 // events with a lower seq are already stored
	resuming  bool   // this is the partly stored transaction
	emitted   bool   // a batch of this transaction was returned
	seq       uint32 // seq of the next event
	batch     *pb.TxBatch
	size      int

	// lastEmpty is a position the source may move to without storing any
	// event: the most recent transaction without published changes, or the
	// server's WAL position reported while we were idle. The runner stores
	// it as an empty batch so the slot can advance and Postgres can free
	// WAL (since Postgres 15 empty transactions are not sent at all).
	lastEmpty *pb.TxBatch
}

func newDecoder(reg Registry, limits Limits, start fsm.SourceState) *decoder {
	return &decoder{
		reg:            reg,
		limits:         limits.withDefaults(),
		relations:      map[uint32]*pb.Schema{},
		sent:           map[uint64]bool{},
		storedLSN:      start.CommitLSN,
		partialLSN:     start.PartialLSN,
		partialNextSeq: start.PartialNextSeq,
	}
}

// handle processes one message and returns the batches that are ready, in
// order. A batch must be stored before the next one is.
func (d *decoder) handle(msg pglogrepl.Message) ([]*pb.TxBatch, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.relation(m)
		return nil, nil
	case *pglogrepl.BeginMessage:
		return nil, d.begin(m)
	case *pglogrepl.CommitMessage:
		return d.commit(m)
	case *pglogrepl.InsertMessage:
		return d.change(m.RelationID, pb.Op_OP_INSERT, nil, m.Tuple)
	case *pglogrepl.UpdateMessage:
		return d.change(m.RelationID, pb.Op_OP_UPDATE, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return d.change(m.RelationID, pb.Op_OP_DELETE, m.OldTuple, nil)
	case *pglogrepl.TruncateMessage:
		var out []*pb.TxBatch
		for _, rel := range m.RelationIDs {
			bs, err := d.change(rel, pb.Op_OP_TRUNCATE, nil, nil)
			if err != nil {
				return nil, err
			}
			out = append(out, bs...)
		}
		return out, nil
	default:
		// Type, Origin and logical decoding messages carry nothing we store.
		return nil, nil
	}
}

func (d *decoder) relation(m *pglogrepl.RelationMessage) {
	s := &pb.Schema{
		RelationOid:     m.RelationID,
		Namespace:       m.Namespace,
		Name:            m.RelationName,
		ReplicaIdentity: uint32(m.ReplicaIdentity),
	}
	for _, c := range m.Columns {
		s.Columns = append(s.Columns, &pb.Column{
			Name:         c.Name,
			TypeOid:      c.DataType,
			TypeModifier: c.TypeModifier,
			Key:          c.Flags&1 != 0,
		})
	}
	s.Id = fsm.SchemaID(s)
	d.relations[m.RelationID] = s
}

func (d *decoder) begin(m *pglogrepl.BeginMessage) error {
	if d.inTx {
		return fmt.Errorf("%w: BEGIN inside a transaction", ErrProtocol)
	}
	lsn := uint64(m.FinalLSN)
	d.inTx, d.emitted, d.seq, d.size = true, false, 0, 0
	d.skip = lsn <= d.storedLSN
	d.resuming, d.skipUntil = false, 0
	if d.partialLSN != 0 && !d.skip {
		switch {
		case lsn == d.partialLSN:
			d.resuming, d.skipUntil = true, d.partialNextSeq
		case lsn > d.partialLSN:
			return fmt.Errorf("%w: transaction at %s, got %s first", ErrPartialLost,
				pglogrepl.LSN(d.partialLSN), m.FinalLSN)
		default:
			d.skip = true // older than the partial one, so already stored
		}
	}
	d.batch = &pb.TxBatch{CommitLsn: lsn, CommitTimeUs: m.CommitTime.UnixMicro(), Xid: m.Xid}
	return nil
}

func (d *decoder) change(rel uint32, op pb.Op, before, after *pglogrepl.TupleData) ([]*pb.TxBatch, error) {
	if !d.inTx {
		return nil, fmt.Errorf("%w: change outside a transaction", ErrProtocol)
	}
	seq := d.seq
	d.seq++
	if d.skip || seq < d.skipUntil {
		return nil, nil
	}
	s, ok := d.relations[rel]
	if !ok {
		return nil, fmt.Errorf("%w: change for unknown relation %d", ErrProtocol, rel)
	}
	ev := &pb.Event{SchemaId: s.Id, Op: op, Before: row(before), After: row(after)}
	if len(d.batch.Events) == 0 {
		d.batch.FirstSeq = seq
	}
	if !d.sent[s.Id] && !d.reg.HasSchema(s.Id) {
		d.batch.Schemas = append(d.batch.Schemas, s)
		d.sent[s.Id] = true
	}
	d.batch.Events = append(d.batch.Events, ev)
	d.size += proto.Size(ev)
	if len(d.batch.Events) < d.limits.MaxEvents && d.size < d.limits.MaxBytes {
		return nil, nil
	}
	full := d.batch
	d.emitted = true
	d.batch = &pb.TxBatch{CommitLsn: full.CommitLsn, CommitTimeUs: full.CommitTimeUs, Xid: full.Xid}
	d.size = 0
	return []*pb.TxBatch{full}, nil
}

func (d *decoder) commit(m *pglogrepl.CommitMessage) ([]*pb.TxBatch, error) {
	if !d.inTx {
		return nil, fmt.Errorf("%w: COMMIT outside a transaction", ErrProtocol)
	}
	if uint64(m.CommitLSN) != d.batch.CommitLsn {
		return nil, fmt.Errorf("%w: COMMIT at %s for BEGIN at %s", ErrProtocol, m.CommitLSN, pglogrepl.LSN(d.batch.CommitLsn))
	}
	d.inTx = false
	b := d.batch
	d.batch = nil
	if d.skip {
		return nil, nil
	}
	b.EndLsn, b.Final = uint64(m.TransactionEndLSN), true
	if d.resuming {
		if d.seq < d.skipUntil {
			return nil, fmt.Errorf("%w: transaction at %s has %d changes, but %d are stored",
				ErrPartialLost, m.CommitLSN, d.seq, d.skipUntil)
		}
		d.partialLSN, d.partialNextSeq = 0, 0
	}
	d.storedLSN = b.CommitLsn
	if len(b.Events) == 0 {
		b.FirstSeq = d.seq
		if !d.emitted && !d.resuming {
			d.lastEmpty = b
			return nil, nil
		}
	}
	d.lastEmpty = nil
	return []*pb.TxBatch{b}, nil
}

// idle records the server's WAL position from a keepalive. Outside a
// transaction everything before it has been sent to us already, because
// pgoutput sends transactions in commit order before reporting a later
// position.
func (d *decoder) idle(walEnd uint64) {
	if d.inTx || d.partialLSN != 0 || walEnd <= d.storedLSN {
		return
	}
	if d.lastEmpty == nil || walEnd > d.lastEmpty.EndLsn {
		d.lastEmpty = &pb.TxBatch{CommitLsn: walEnd, EndLsn: walEnd, Final: true}
	}
}

// takeEmpty returns the newest position recorded by an empty transaction or
// idle, if nothing was returned since, and forgets it.
func (d *decoder) takeEmpty() *pb.TxBatch {
	b := d.lastEmpty
	d.lastEmpty = nil
	if b == nil || d.inTx || d.partialLSN != 0 || b.CommitLsn < d.storedLSN {
		return nil
	}
	d.storedLSN = b.CommitLsn
	return b
}

func row(t *pglogrepl.TupleData) *pb.Row {
	if t == nil {
		return nil
	}
	r := &pb.Row{Values: make([]*pb.Value, len(t.Columns))}
	for i, c := range t.Columns {
		v := &pb.Value{}
		switch c.DataType {
		case pglogrepl.TupleDataTypeNull:
			v.Kind = pb.ValueKind_VALUE_KIND_NULL
		case pglogrepl.TupleDataTypeToast:
			v.Kind = pb.ValueKind_VALUE_KIND_UNCHANGED
		case pglogrepl.TupleDataTypeBinary:
			v.Kind, v.Data = pb.ValueKind_VALUE_KIND_BINARY, append([]byte(nil), c.Data...)
		default:
			v.Kind, v.Data = pb.ValueKind_VALUE_KIND_TEXT, append([]byte(nil), c.Data...)
		}
		r.Values[i] = v
	}
	return r
}
