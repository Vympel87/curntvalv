package event

// Grouper groups the changes of successive reads into whole transactions
// (design D30). The changes of a transaction whose final batch has not been
// read yet are held until it has. Used by one reader loop (a sink runner, a
// subscription stream) only.
type Grouper struct {
	max int // a transaction growing past this is passed on in parts

	held  []Change // changes of the open transaction
	open  uint64   // commit LSN of the open transaction, 0 if none
	from  uint64   // index of the open transaction's first batch
	begun bool     // its first batch was read in this session
	split bool     // it is delivered in parts
}

// NewGrouper returns a grouper that holds at most max changes of one
// transaction (100000 if max <= 0); a larger one is passed on in parts.
func NewGrouper(max int) *Grouper {
	if max <= 0 {
		max = 100000
	}
	return &Grouper{max: max}
}

// Add takes the changes of one read, after any filtering, and the read's
// Txs, and returns the transactions that are ready, in order. A transaction
// gets TxChanges only if this grouper saw it from its first to its final
// batch and did not split it; otherwise its changes come with TxChanges 0.
func (b *Grouper) Add(changes []Change, txs []Tx) [][]Change {
	all := append(b.held, changes...)
	b.held = nil
	prev, prevFrom, prevBegun, prevSplit := b.open, b.from, b.begun, b.split
	find := func(lsn uint64) (Tx, bool) {
		for _, t := range txs {
			if t.CommitLSN == lsn {
				return t, true
			}
		}
		return Tx{}, false
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
	var out [][]Change
	for len(all) > 0 {
		n := 1
		for n < len(all) && all[n].CommitLSN == all[0].CommitLSN {
			n++
		}
		g := all[:n:n]
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

// Splitting returns the commit LSN of the transaction being passed on in
// parts, 0 if none.
func (b *Grouper) Splitting() uint64 {
	if b.split {
		return b.open
	}
	return 0
}

// Safe returns the highest log index a reader may record as done after a
// read up to next-1: never past the start of a transaction still being
// collected, so a restart reads it whole again.
func (b *Grouper) Safe(next uint64) uint64 {
	if b.open != 0 && !b.split {
		return min(next-1, b.from-1)
	}
	return next - 1
}
