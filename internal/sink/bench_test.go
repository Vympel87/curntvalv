package sink

import (
	"fmt"
	"testing"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// BenchmarkShapeApply filters and reshapes 500 changes (one delivery) with
// a where expression, a dropped and a masked column.
func BenchmarkShapeApply(b *testing.B) {
	var ops []op
	for i := range 500 {
		ops = append(ops, op{op: pb.Op_OP_INSERT, id: fmt.Sprint(i), note: fmt.Sprintf("note %d", i%10)})
	}
	changes := changesOf(10, ops...)
	where, err := ParseWhere("note in ('note 1', 'note 2', 'note 3') or id >= 400")
	if err != nil {
		b.Fatal(err)
	}
	s := newShaper(Shape{Where: []*Expr{where}, Mask: []string{"note"}})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		s.Apply(changes)
	}
	b.ReportMetric(float64(b.N*len(changes))/b.Elapsed().Seconds(), "changes/s")
}
