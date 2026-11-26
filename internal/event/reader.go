package event

import (
	"context"
	"fmt"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Log reads committed entries. *raft.Node implements it; it returns
// raft.ErrCompacted for entries removed by log compaction.
type Log interface {
	ReadCommitted(ctx context.Context, from uint64, max int) ([]raft.Entry, error)
}

// State is what the reader needs from the state machine. *fsm.FSM
// implements it.
type State interface {
	Applied() (uint64, <-chan struct{})
	Schema(id uint64) (*pb.Schema, bool)
}

// Reader reads row changes from the local log. Any node can use it: it
// returns only entries that this node has applied, so every schema they
// refer to is already known.
type Reader struct {
	log   Log
	state State
}

// NewReader returns a reader over a node's log and state machine.
func NewReader(log Log, state State) *Reader {
	return &Reader{log: log, state: state}
}

// Result is what one Read returns.
type Result struct {
	// Changes that passed the filter, in log order.
	Changes []Change
	// Next is the index to read from next time.
	Next uint64
	// TxEntries counts the transaction batches read, including those whose
	// changes were all filtered out.
	TxEntries int
	// Txs describes the transactions whose batches were read, in log order,
	// whether or not their changes passed the filter.
	Txs []Tx
}

// Tx describes the batches of one transaction that a read covered. The
// batches of a transaction are stored one after another (other entries may
// come between them, but no batch of another transaction).
type Tx struct {
	CommitLSN uint64
	// First is the index of its first batch in this read.
	First uint64
	// Begin and End report whether the read covered the transaction's first
	// and final batch. A transaction stored in parts by a source that then
	// lost its Postgres server can lack an end (design D7).
	Begin, End bool
}

// Read waits until at least one entry at index from or later is applied,
// then returns the changes of up to max entries. from 0 means the oldest
// retained entry. It fails with raft.ErrCompacted if from was removed by log
// compaction, and with ctx's error when ctx ends first.
func (r *Reader) Read(ctx context.Context, from uint64, max int, filter Filter) (Result, error) {
	if max <= 0 {
		max = 1
	}
	for {
		applied, changed := r.state.Applied()
		if applied > 0 && from <= applied {
			n := max
			if from > 0 {
				n = int(min(uint64(max), applied-from+1))
			}
			entries, err := r.log.ReadCommitted(ctx, from, n)
			if err != nil {
				return Result{}, err
			}
			res := Result{Next: from}
			for _, e := range entries {
				if e.Index > applied {
					break
				}
				res.Next = e.Index + 1
				if e.Type != raft.EntryNormal {
					continue
				}
				tb, ok, err := fsm.DecodeTxBatch(e.Data)
				if err != nil {
					return Result{}, fmt.Errorf("event: entry %d: %w", e.Index, err)
				}
				if !ok {
					continue
				}
				res.TxEntries++
				res.Txs = addTx(res.Txs, e.Index, tb)
				res.Changes = append(res.Changes, Changes(e.Index, tb, r.state.Schema, filter)...)
			}
			if res.Next > from {
				return res, nil
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
}

// addTx records a batch read at index.
func addTx(txs []Tx, index uint64, tb *pb.TxBatch) []Tx {
	if n := len(txs); n > 0 && txs[n-1].CommitLSN == tb.GetCommitLsn() && !txs[n-1].End {
		txs[n-1].End = tb.GetFinal()
		return txs
	}
	return append(txs, Tx{
		CommitLSN: tb.GetCommitLsn(),
		First:     index,
		// Only a transaction's first batch starts at its first change; a
		// batch resuming a partly stored one does not (design D7).
		Begin: tb.GetFirstSeq() == 0 && len(tb.GetEvents()) > 0,
		End:   tb.GetFinal(),
	})
}
