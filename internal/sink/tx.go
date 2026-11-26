package sink

import (
	"github.com/Vympel87/curntvalv/internal/event"
)

// txBuffer groups the changes a runner reads into whole transactions
// (design D30). The changes of a transaction whose final batch has not been
// read yet are held until it has. Used by one runner session only.
type txBuffer struct {
	max int // a transaction growing past this is delivered in parts

	held  []event.Change // changes of the open transaction
	open  uint64         // commit LSN of the open transaction, 0 if none
	from  uint64         // index of the open transaction's first batch
	begun bool           // its first batch was read in this session
	split bool           // it is delivered in parts
}

// unit is what a runner delivers in one piece: one change, or with
// Transactions every change of one transaction the sink receives.
type unit []event.Change

// singles makes every change a unit.
func singles(changes []event.Change) []unit {
	out := make([]unit, len(changes))
	for i := range changes {
		out[i] = changes[i : i+1 : i+1]
	}
	return out
}

// add takes the changes of one read, after Tables and Shape, and returns
// the transactions ready for delivery. A transaction gets TxChanges only if
// this session read it from its first to its final batch and did not split
// it; otherwise its changes are delivered with TxChanges 0.
func (b *txBuffer) add(changes []event.Change, txs []event.Tx) []unit {
	all := append(b.held, changes...)
	b.held = nil
	prev, prevFrom, prevBegun, prevSplit := b.open, b.from, b.begun, b.split
	find := func(lsn uint64) (event.Tx, bool) {
		for _, t := range txs {
			if t.CommitLSN == lsn {
				return t, true
			}
		}
		return event.Tx{}, false
	}
	if len(txs) > 0 {
		// A read without transaction batches leaves the open one open.
		b.open, b.from, b.begun, b.split = 0, 0, false, false
		if last := txs[len(txs)-1]; !last.End {
			b.open, b.from, b.begun = last.CommitLSN, last.First, last.Begin
			if last.CommitLSN == prev {
				// Still the one collected before.
				b.from, b.begun, b.split = prevFrom, prevBegun, prevSplit
			}
		}
	}
	var out []unit
	for len(all) > 0 {
		n := 1
		for n < len(all) && all[n].CommitLSN == all[0].CommitLSN {
			n++
		}
		g := unit(all[:n:n])
		all = all[n:]
		lsn := g[0].CommitLSN
		if len(all) == 0 && lsn == b.open && !b.split {
			if len(g) < b.max {
				b.held = g
				break
			}
			b.split = true
		}
		t, ok := find(lsn)
		begun := ok && t.Begin || lsn == prev && prevBegun
		ended := ok && t.End
		if begun && ended && !(lsn == prev && prevSplit) {
			for i := range g {
				g[i].TxChanges = uint32(len(g))
			}
		}
		out = append(out, g)
	}
	return out
}

// safe returns the highest offset that may be stored after a read up to
// next-1: never past the start of a transaction still being collected, so a
// restart reads it whole again.
func (b *txBuffer) safe(next uint64) uint64 {
	if b.open != 0 && !b.split {
		return min(next-1, b.from-1)
	}
	return next - 1
}
