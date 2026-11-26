package event

import (
	"testing"

	"github.com/Vympel87/curntvalv/internal/compat"
)

// TestJSONCompat checks the JSON form of a change against compat/json.txt
// (design D34): what webhooks, Kafka, NATS, Redis, S3 and the CLI emit.
func TestJSONCompat(t *testing.T) {
	compat.Check(t, "json.txt", compat.JSONFields("change", JSON{}), compat.Updating())
}
