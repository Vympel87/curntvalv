package fsm

import (
	"testing"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// BenchmarkApplyTxBatch applies batches of 1000 row changes, the size the
// source cuts large transactions into.
func BenchmarkApplyTxBatch(b *testing.B) {
	f := New()
	var datas [][]byte
	for i := range 64 {
		tb := &pb.TxBatch{CommitLsn: uint64(100 + i), EndLsn: uint64(101 + i), Final: true, Events: events(1000)}
		data, err := EncodeTxBatch(tb)
		if err != nil {
			b.Fatal(err)
		}
		datas = append(datas, data)
	}
	b.ResetTimer()
	for i := range b.N {
		f2 := f
		if i%64 == 0 {
			f2 = New()
			f = f2
		}
		if err := f2.Apply(raft.Entry{Index: uint64(i + 1), Term: 1, Type: raft.EntryNormal, Data: datas[i%64]}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "changes/s")
}
