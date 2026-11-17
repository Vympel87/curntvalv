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
	"regexp"
	"sync"
	"time"

	"github.com/Vympel87/curntvalv/internal/event"
)

// ClickHouse writes changes to one changelog table through ClickHouse's
// HTTP interface (design D12). The table is created if it does not exist:
//
//	id String, log_index UInt64, lsn UInt64, seq UInt32, xid UInt32,
//	commit_time DateTime64(6, 'UTC'), source_table LowCardinality(String),
//	op LowCardinality(String), before String, after String,
//	unchanged Array(String)
//	ENGINE = ReplacingMergeTree ORDER BY (source_table, lsn, seq)
//
// before and after hold the row as a JSON object (empty when absent). A
// redelivered change has the same sorting key, so ReplacingMergeTree drops
// the copy when it merges; query with FINAL to never see one.
type ClickHouse struct {
	endpoint string // without user info
	shown    string // for error messages
	user     string
	password string
	table    string // quoted `db`.`table`
	client   *http.Client

	mu    sync.Mutex
	ready bool // table exists
}

// ClickHouseConfig configures a ClickHouse sink.
type ClickHouseConfig struct {
	// URL of the HTTP interface, e.g. http://user:password@host:8123.
	URL      string
	Database string // default "default"
	Table    string // default "curntvalv_events"
	Timeout  time.Duration
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// NewClickHouse returns a ClickHouse sink. It does no I/O.
func NewClickHouse(cfg ClickHouseConfig) (*ClickHouse, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("clickhouse: bad url (want http://[USER:PASSWORD@]HOST:PORT)")
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if cfg.Table == "" {
		cfg.Table = "curntvalv_events"
	}
	if !identifier.MatchString(cfg.Database) || !identifier.MatchString(cfg.Table) {
		return nil, fmt.Errorf("clickhouse: bad database or table name %q.%q", cfg.Database, cfg.Table)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	c := &ClickHouse{
		shown:  u.Redacted(),
		table:  fmt.Sprintf("`%s`.`%s`", cfg.Database, cfg.Table),
		client: &http.Client{Timeout: cfg.Timeout},
	}
	if u.User != nil {
		c.user = u.User.Username()
		c.password, _ = u.User.Password()
		u.User = nil
	}
	c.endpoint = u.String()
	return c, nil
}

// clickHouseRow is one row of the changelog table in JSONEachRow format.
type clickHouseRow struct {
	ID          string   `json:"id"`
	LogIndex    uint64   `json:"log_index"`
	LSN         uint64   `json:"lsn"`
	Seq         uint32   `json:"seq"`
	Xid         uint32   `json:"xid"`
	CommitTime  string   `json:"commit_time"`
	SourceTable string   `json:"source_table"`
	Op          string   `json:"op"`
	Before      string   `json:"before"`
	After       string   `json:"after"`
	Unchanged   []string `json:"unchanged"`
}

func (c *ClickHouse) createTable() string {
	return "CREATE TABLE IF NOT EXISTS " + c.table + ` (
  id String,
  log_index UInt64,
  lsn UInt64,
  seq UInt32,
  xid UInt32,
  commit_time DateTime64(6, 'UTC'),
  source_table LowCardinality(String),
  op LowCardinality(String),
  before String,
  after String,
  unchanged Array(String)
) ENGINE = ReplacingMergeTree
ORDER BY (source_table, lsn, seq)`
}

// Deliver inserts all changes in one INSERT, creating the table first if
// needed.
func (c *ClickHouse) Deliver(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	if err := c.ensureTable(ctx); err != nil {
		return err
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, ch := range changes {
		j := ch.JSON()
		row := clickHouseRow{
			ID: j.ID, LogIndex: ch.Index, LSN: ch.CommitLSN, Seq: ch.Seq, Xid: ch.Xid,
			CommitTime:  ch.CommitTime().Format("2006-01-02 15:04:05.000000"),
			SourceTable: j.Table, Op: j.Op, Unchanged: j.Unchanged,
		}
		if row.Unchanged == nil {
			row.Unchanged = []string{}
		}
		var err error
		if row.Before, err = jsonText(j.Before); err != nil {
			return err
		}
		if row.After, err = jsonText(j.After); err != nil {
			return err
		}
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("clickhouse: encode: %w", err)
		}
	}
	_, err := c.exec(ctx, "INSERT INTO "+c.table+" FORMAT JSONEachRow", &body)
	return err
}

func jsonText(m map[string]any) (string, error) {
	if m == nil {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("clickhouse: encode row: %w", err)
	}
	return string(b), nil
}

func (c *ClickHouse) ensureTable(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready {
		return nil
	}
	if _, err := c.exec(ctx, c.createTable(), nil); err != nil {
		return err
	}
	c.ready = true
	return nil
}

// exec runs query with an optional data body and returns the response.
func (c *ClickHouse) exec(ctx context.Context, query string, data io.Reader) ([]byte, error) {
	if data == nil {
		data = http.NoBody
	}
	u := c.endpoint + "/?" + url.Values{"query": {query}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, data)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	if c.user != "" {
		req.Header.Set("X-ClickHouse-User", c.user)
		req.Header.Set("X-ClickHouse-Key", c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: request to %s failed: %w", c.shown, redactErr(err))
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg := bytes.TrimSpace(out)
		if len(msg) > 512 {
			msg = msg[:512]
		}
		return nil, fmt.Errorf("clickhouse: %s: %s", resp.Status, msg)
	}
	return out, nil
}
