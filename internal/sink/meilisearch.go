package sink

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Meilisearch mirrors tables into Meilisearch indexes (design D19): one
// document per row, under the primary key field "curntvalv_key" made from
// the row's replica identity key. Inserts, updates and backfilled rows are
// written with "add or update" (an update without an unchanged TOASTed
// column keeps the stored value), deletes remove the document, a truncate
// removes all documents of the index, and a change of the key removes the
// old document. Source resets are not mirrored.
//
// Values are typed by column: integers, floating point and numeric columns
// become numbers, booleans booleans, json and jsonb JSON; everything else
// stays Postgres text. Meilisearch applies the tasks of an index in order;
// the sink waits until they succeeded, so a failure is retried and the
// mirror stays in step with the log. Writing a document again is harmless.
type Meilisearch struct {
	endpoint string // without user info
	shown    string
	key      string
	index    string // template with {namespace} and {table}
	client   *http.Client
	timeout  time.Duration
}

// MeilisearchConfig configures a Meilisearch sink.
type MeilisearchConfig struct {
	// URL is http[s]://[:API_KEY@]HOST:PORT; the password part is the API
	// key.
	URL string
	// Index is the index uid; "{namespace}" and "{table}" are replaced.
	// Default "{namespace}_{table}".
	Index   string
	Timeout time.Duration
}

// keyField is the documents' primary key field.
const keyField = "curntvalv_key"

var indexTemplate = regexp.MustCompile(`^([A-Za-z0-9_-]|\{namespace\}|\{table\})+$`)

// NewMeilisearch returns a Meilisearch sink. It does no I/O.
func NewMeilisearch(cfg MeilisearchConfig) (*Meilisearch, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("meilisearch: bad url (want http[s]://[:API_KEY@]HOST:PORT)")
	}
	m := &Meilisearch{shown: u.Redacted(), index: cfg.Index, timeout: cfg.Timeout}
	if u.User != nil {
		if p, ok := u.User.Password(); ok {
			m.key = p
		} else {
			m.key = u.User.Username()
		}
		u.User = nil
	}
	m.endpoint = strings.TrimSuffix(u.String(), "/")
	if m.index == "" {
		m.index = "{namespace}_{table}"
	}
	if !indexTemplate.MatchString(m.index) {
		return nil, fmt.Errorf("meilisearch: bad index %q (letters, digits, _, -, {namespace} and {table})", m.index)
	}
	if m.timeout <= 0 {
		m.timeout = 30 * time.Second
	}
	m.client = &http.Client{Timeout: m.timeout}
	return m, nil
}

// indexName returns the index of a table.
func (m *Meilisearch) indexName(s *pb.Schema) string {
	r := strings.NewReplacer("{namespace}", indexToken(s.GetNamespace()), "{table}", indexToken(s.GetName()))
	return r.Replace(m.index)
}

func indexToken(s string) string {
	b := []byte(s)
	for i, c := range b {
		if !(c == '_' || c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			b[i] = '_'
		}
	}
	return string(b)
}

// documentKey returns the document id of a row: the key column values
// joined by "-" when each is a non-empty run of letters, digits and _, or
// else "-" followed by their base64url encoding. The two forms cannot
// collide (only the second starts with "-").
func documentKey(s *pb.Schema, row *pb.Row) (string, error) {
	var parts [][]byte
	for i, c := range s.GetColumns() {
		if !c.GetKey() {
			continue
		}
		if i >= len(row.GetValues()) {
			return "", fmt.Errorf("row has no column %s", c.GetName())
		}
		v := row.GetValues()[i]
		if v.GetKind() != pb.ValueKind_VALUE_KIND_TEXT && v.GetKind() != pb.ValueKind_VALUE_KIND_BINARY {
			return "", fmt.Errorf("key column %s is %s", c.GetName(), strings.ToLower(strings.TrimPrefix(v.GetKind().String(), "VALUE_KIND_")))
		}
		parts = append(parts, v.GetData())
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("table %s.%s has no key columns (no primary key or REPLICA IDENTITY NOTHING)", s.GetNamespace(), s.GetName())
	}
	plain := true
	for _, p := range parts {
		if len(p) == 0 || !rawKeyPart(p) {
			plain = false
		}
	}
	if plain {
		joined := bytes.Join(parts, []byte("-"))
		if len(joined) <= 500 {
			return string(joined), nil
		}
	}
	var enc []byte
	for _, p := range parts {
		enc = append(enc, strconv.Itoa(len(p))...)
		enc = append(enc, ':')
		enc = append(enc, p...)
	}
	k := "-" + base64.RawURLEncoding.EncodeToString(enc)
	if len(k) > 511 {
		return "", errors.New("key too long for a Meilisearch document id (511 bytes)")
	}
	return k, nil
}

func rawKeyPart(p []byte) bool {
	for _, c := range p {
		if !(c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

// Postgres type OIDs that get a JSON type other than string.
const (
	oidBool    = 16
	oidInt8    = 20
	oidInt2    = 21
	oidInt4    = 23
	oidJSON    = 114
	oidFloat4  = 700
	oidFloat8  = 701
	oidNumeric = 1700
	oidJSONB   = 3802
)

// document returns the row as a document. Unchanged TOASTed columns are left
// out, so "add or update" keeps them.
func document(s *pb.Schema, row *pb.Row, key string) map[string]any {
	doc := map[string]any{keyField: key}
	for i, c := range s.GetColumns() {
		if i >= len(row.GetValues()) {
			break
		}
		v := row.GetValues()[i]
		switch v.GetKind() {
		case pb.ValueKind_VALUE_KIND_NULL:
			doc[c.GetName()] = nil
		case pb.ValueKind_VALUE_KIND_UNCHANGED:
		case pb.ValueKind_VALUE_KIND_BINARY:
			doc[c.GetName()] = base64.StdEncoding.EncodeToString(v.GetData())
		default:
			doc[c.GetName()] = typed(c.GetTypeOid(), v.GetData())
		}
	}
	return doc
}

func typed(oid uint32, text []byte) any {
	switch oid {
	case oidBool:
		return string(text) == "t"
	case oidInt2, oidInt4, oidInt8, oidFloat4, oidFloat8, oidNumeric:
		// NaN and Infinity are not JSON numbers.
		if len(text) > 0 && (text[0] == '-' || text[0] >= '0' && text[0] <= '9') && json.Valid(text) {
			return json.Number(text)
		}
	case oidJSON, oidJSONB:
		if json.Valid(text) {
			return json.RawMessage(text)
		}
	}
	return string(text)
}

// meiliOp is one request: documents to add or update, ids to delete, or
// all documents of the index to delete.
type meiliOp struct {
	index  string
	kind   string // "upsert", "delete", "clear"
	docs   []map[string]any
	ids    []string
	change string // last change in it, for errors
}

// plan turns changes into requests, in order, merging consecutive ones of
// the same kind on the same index.
func (m *Meilisearch) plan(changes []event.Change) ([]meiliOp, error) {
	var ops []meiliOp
	add := func(index, kind, change string, doc map[string]any, id string) {
		if n := len(ops); n > 0 && ops[n-1].index == index && ops[n-1].kind == kind && kind != "clear" {
			last := &ops[n-1]
			if doc != nil {
				last.docs = append(last.docs, doc)
			}
			if id != "" {
				last.ids = append(last.ids, id)
			}
			last.change = change
			return
		}
		op := meiliOp{index: index, kind: kind, change: change}
		if doc != nil {
			op.docs = []map[string]any{doc}
		}
		if id != "" {
			op.ids = []string{id}
		}
		ops = append(ops, op)
	}
	for _, c := range changes {
		if c.IsSourceReset() {
			continue
		}
		s := c.Schema
		if s == nil {
			return nil, fmt.Errorf("meilisearch: change %s: unknown schema %d", c.ID(), c.Event.GetSchemaId())
		}
		index := m.indexName(s)
		switch c.Event.GetOp() {
		case pb.Op_OP_INSERT, pb.Op_OP_UPDATE, pb.Op_OP_BACKFILL:
			key, err := documentKey(s, c.Event.GetAfter())
			if err != nil {
				return nil, fmt.Errorf("meilisearch: change %s: %w", c.ID(), err)
			}
			if before := c.Event.GetBefore(); before != nil {
				// With REPLICA IDENTITY FULL every column is in the key.
				old, err := documentKey(s, before)
				if err != nil {
					return nil, fmt.Errorf("meilisearch: change %s: old key: %w", c.ID(), err)
				}
				if old != key {
					add(index, "delete", c.ID(), nil, old)
				}
			}
			add(index, "upsert", c.ID(), document(s, c.Event.GetAfter(), key), "")
		case pb.Op_OP_DELETE:
			key, err := documentKey(s, c.Event.GetBefore())
			if err != nil {
				return nil, fmt.Errorf("meilisearch: change %s: %w", c.ID(), err)
			}
			add(index, "delete", c.ID(), nil, key)
		case pb.Op_OP_TRUNCATE:
			add(index, "clear", c.ID(), nil, "")
		}
	}
	return ops, nil
}

// Deliver sends the requests and waits until Meilisearch processed them.
func (m *Meilisearch) Deliver(ctx context.Context, changes []event.Change) error {
	ops, err := m.plan(changes)
	if err != nil || len(ops) == 0 {
		return err
	}
	tasks := make([]int64, 0, len(ops))
	for _, op := range ops {
		var method, path string
		var body any
		idx := url.PathEscape(op.index)
		switch op.kind {
		case "upsert":
			method, path, body = http.MethodPut, "/indexes/"+idx+"/documents?primaryKey="+keyField, op.docs
		case "delete":
			method, path, body = http.MethodPost, "/indexes/"+idx+"/documents/delete-batch", op.ids
		case "clear":
			method, path = http.MethodDelete, "/indexes/"+idx+"/documents"
		}
		var task struct {
			TaskUID *int64 `json:"taskUid"`
		}
		if err := m.call(ctx, method, path, body, &task); err != nil {
			if op.kind == "clear" && isNotFound(err) {
				continue // nothing to clear
			}
			return fmt.Errorf("meilisearch: %s on %s (up to change %s): %w", op.kind, op.index, op.change, err)
		}
		if task.TaskUID == nil {
			return fmt.Errorf("meilisearch: %s on %s: no task in the answer", op.kind, op.index)
		}
		tasks = append(tasks, *task.TaskUID)
	}
	return m.wait(ctx, tasks)
}

// wait polls the tasks until all succeeded; a failed one is an error.
func (m *Meilisearch) wait(ctx context.Context, tasks []int64) error {
	if len(tasks) == 0 {
		return nil
	}
	uids := make([]string, len(tasks))
	for i, t := range tasks {
		uids[i] = strconv.FormatInt(t, 10)
	}
	path := "/tasks?limit=" + strconv.Itoa(len(tasks)) + "&uids=" + strings.Join(uids, ",")
	deadline := time.Now().Add(m.timeout)
	pause := 20 * time.Millisecond
	for {
		var resp struct {
			Results []struct {
				UID    int64  `json:"uid"`
				Status string `json:"status"`
				Type   string `json:"type"`
				Index  string `json:"indexUid"`
				Error  *struct {
					Message string `json:"message"`
					Code    string `json:"code"`
				} `json:"error"`
			} `json:"results"`
		}
		if err := m.call(ctx, http.MethodGet, path, nil, &resp); err != nil {
			return fmt.Errorf("meilisearch: task status: %w", err)
		}
		done := 0
		for _, t := range resp.Results {
			switch {
			case t.Status == "succeeded":
				done++
			case t.Status == "failed" && t.Type == "documentDeletion" && t.Error != nil && t.Error.Code == "index_not_found":
				done++ // nothing to delete
			case t.Status == "failed" || t.Status == "canceled":
				msg := t.Status
				if t.Error != nil {
					msg = t.Error.Message + " (" + t.Error.Code + ")"
				}
				return fmt.Errorf("meilisearch: task %d (%s on %s) %s", t.UID, t.Type, t.Index, msg)
			}
		}
		if done == len(tasks) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("meilisearch: %d of %d tasks not processed after %s", len(tasks)-done, len(tasks), m.timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
		pause = min(2*pause, time.Second)
	}
}

// meiliError is an error answer of the Meilisearch API.
type meiliError struct {
	status  int
	code    string
	message string
}

func (e *meiliError) Error() string {
	return fmt.Sprintf("%d %s: %s", e.status, e.code, e.message)
}

func isNotFound(err error) bool {
	var me *meiliError
	return errors.As(err, &me) && me.status == http.StatusNotFound
}

// call sends one request with a JSON body and decodes the JSON answer.
func (m *Meilisearch) call(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.endpoint+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "curntvalv")
	if m.key != "" {
		req.Header.Set("Authorization", "Bearer "+m.key)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("request to %s failed: %w", m.shown, redactErr(err))
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read answer: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		e := &meiliError{status: resp.StatusCode}
		var ans struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		}
		if json.Unmarshal(b, &ans) == nil {
			e.code, e.message = ans.Code, ans.Message
		} else {
			e.message = string(bytes.TrimSpace(b))
		}
		return e
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			return fmt.Errorf("bad answer %q: %w", b, err)
		}
	}
	return nil
}
