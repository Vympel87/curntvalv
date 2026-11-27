package event

import (
	"encoding/json"
	"testing"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// BenchmarkChangeJSON encodes a change of a five-column row the way the
// webhook, Kafka, NATS, Redis and S3 sinks do.
func BenchmarkChangeJSON(b *testing.B) {
	sc := &pb.Schema{Id: 1, Namespace: "public", Name: "orders", Columns: []*pb.Column{
		{Name: "id", Key: true}, {Name: "customer"}, {Name: "status"}, {Name: "amount"}, {Name: "created_at"}}}
	c := Change{Index: 100, CommitLSN: 0x16B3748, Seq: 3, Xid: 900, CommitTimeUs: 1_700_000_000_000_000, Schema: sc,
		Event: &pb.Event{SchemaId: 1, Op: pb.Op_OP_UPDATE, After: &pb.Row{Values: []*pb.Value{
			text("42"), text("customer 7"), text("paid"), text("129.90"), text("2026-11-27 10:00:00+00")}}}}
	b.ReportAllocs()
	for range b.N {
		if _, err := json.Marshal(c.JSON()); err != nil {
			b.Fatal(err)
		}
	}
}
