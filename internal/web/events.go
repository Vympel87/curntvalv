package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// feedPing is how often an idle /api/events stream sends a comment, so
// proxies and browsers keep it open.
const feedPing = 15 * time.Second

// apiEvents streams row changes as server-sent events:
//
//	GET /api/events[?table=T]...[&from=INDEX]
//
// Each change is one event with the log index as its id and the shared JSON
// form of a change as data. Without from (or Last-Event-ID, sent by a
// reconnecting browser) the feed starts with the next change. A reconnect
// resends the whole entry it stopped in, so clients de-duplicate on the
// change's "id" field, as every consumer does (design D4).
func (s *Server) apiEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	select {
	case s.feeds <- struct{}{}:
		defer func() { <-s.feeds }()
	default:
		http.Error(w, "too many open feeds", http.StatusServiceUnavailable)
		return
	}
	var filter event.Filter
	for _, t := range r.URL.Query()["table"] {
		if t == "" {
			http.Error(w, "empty table name", http.StatusBadRequest)
			return
		}
		filter = append(filter, t)
	}
	from, err := s.feedStart(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if from == 0 {
		st, err := s.localStatus(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		from = st.GetLastApplied() + 1
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": curntvalv change feed\n\n")
	flusher.Flush()
	for {
		rctx, cancel := context.WithTimeout(ctx, feedPing)
		res, err := s.reader.Read(rctx, from, 256, filter)
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case errors.Is(err, context.DeadlineExceeded):
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
			continue
		case errors.Is(err, raft.ErrCompacted):
			from = 0 // fell behind retention: continue with the oldest entry
			continue
		case err != nil:
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			flusher.Flush()
			return
		}
		from = res.Next
		for _, c := range res.Changes {
			data, err := json.Marshal(c.JSON())
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", c.Index, data); err != nil {
				return
			}
		}
		if len(res.Changes) > 0 {
			flusher.Flush()
		}
	}
}

// feedStart returns the index to start at: from, else Last-Event-ID (the
// entry the browser saw last, resent whole), else 0 (the next change).
func (s *Server) feedStart(r *http.Request) (uint64, error) {
	v := r.URL.Query().Get("from")
	if v == "" {
		v = r.Header.Get("Last-Event-ID")
	}
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("bad start index %q", v)
	}
	return n, nil
}
