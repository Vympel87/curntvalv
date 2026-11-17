package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// Webhook POSTs changes as JSON to a URL:
//
//	{"sink": "NAME", "events": [<event.JSON>, ...]}
//
// Any 2xx answer means delivered; anything else is retried. The
// Idempotency-Key header names the first and last change of the request
// ("FIRST_ID..LAST_ID") and stays the same when a failed request is retried.
// After a leader change the same changes can arrive again in differently
// cut requests, so receivers should de-duplicate on each event's "id".
type Webhook struct {
	name   string
	url    string
	shown  string // url without password, for error messages
	client *http.Client
}

// WebhookConfig configures a webhook sink.
type WebhookConfig struct {
	Name    string // sink name, sent in the body
	URL     string // http or https
	Timeout time.Duration
}

// NewWebhook returns a webhook sink. It does no I/O.
func NewWebhook(cfg WebhookConfig) (*Webhook, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("webhook: bad url (want http://HOST/... or https://HOST/...)")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &Webhook{name: cfg.Name, url: cfg.URL, shown: u.Redacted(), client: &http.Client{Timeout: cfg.Timeout}}, nil
}

type webhookBody struct {
	Sink   string       `json:"sink"`
	Events []event.JSON `json:"events"`
}

// Deliver sends one request with all changes.
func (w *Webhook) Deliver(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	body := webhookBody{Sink: w.name, Events: make([]event.JSON, len(changes))}
	for i, c := range changes {
		body.Events[i] = c.JSON()
	}
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("webhook: encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "curntvalv")
	req.Header.Set("Idempotency-Key", changes[0].ID()+".."+changes[len(changes)-1].ID())
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: post to %s failed: %w", w.shown, redactErr(err))
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("webhook: %s answered %s: %s", w.shown, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

// redactErr drops the request URL (which may hold a password) from an
// http.Client error.
func redactErr(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
