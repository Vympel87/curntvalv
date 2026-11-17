package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

func testChanges() []event.Change {
	sc := &pb.Schema{Namespace: "public", Name: "orders", Columns: []*pb.Column{{Name: "id"}}}
	var out []event.Change
	for i, id := range []string{"1", "2"} {
		out = append(out, event.Change{Index: 5, CommitLSN: 0x10, Seq: uint32(i), Schema: sc, Event: &pb.Event{Op: pb.Op_OP_INSERT,
			After: &pb.Row{Values: []*pb.Value{{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: []byte(id)}}}}})
	}
	return out
}

func TestWebhookPostsJSON(t *testing.T) {
	var got webhookBody
	var header http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &got); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	w, err := NewWebhook(WebhookConfig{Name: "hook", URL: srv.URL + "/events"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Deliver(context.Background(), testChanges()); err != nil {
		t.Fatal(err)
	}
	if got.Sink != "hook" || len(got.Events) != 2 || got.Events[1].ID != "0/10:1" || got.Events[0].After["id"] != "1" {
		t.Fatalf("body %+v", got)
	}
	if header.Get("Idempotency-Key") != "0/10:0..0/10:1" || header.Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", header)
	}
}

func TestWebhookErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	u := strings.Replace(srv.URL, "http://", "http://user:secret@", 1)
	w, _ := NewWebhook(WebhookConfig{Name: "hook", URL: u})
	err := w.Deliver(context.Background(), testChanges())
	if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "nope") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("got %v", err)
	}
	srv.Close()
	err = w.Deliver(context.Background(), testChanges())
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("closed server: %v", err)
	}
	for _, bad := range []string{"", "ftp://x", "http://", "://x"} {
		if _, err := NewWebhook(WebhookConfig{URL: bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if err := w.Deliver(context.Background(), nil); err != nil {
		t.Fatalf("empty delivery: %v", err)
	}
}
