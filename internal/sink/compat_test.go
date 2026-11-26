package sink

import (
	"testing"

	"github.com/Vympel87/curntvalv/internal/compat"
)

// TestWebhookCompat checks the webhook request body against
// compat/webhook.txt (design D34).
func TestWebhookCompat(t *testing.T) {
	compat.Check(t, "webhook.txt", compat.JSONFields("webhook", webhookBody{}), compat.Updating())
}
