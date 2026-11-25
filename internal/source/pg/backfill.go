package pg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	"github.com/Vympel87/curntvalv/internal/fsm"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

// Backfill (design D17): rows that existed before the slot are read from the
// tables in primary key order, one chunk at a time, between watermarks
// (window.go), and stored in the log as OP_BACKFILL events.

var (
	// ErrBackfillTable means a table cannot be backfilled: it does not
	// exist, is not a plain table, has no primary key or is not in the
	// publication; or Postgres is older than 14.
	ErrBackfillTable = errors.New("pg: cannot backfill")
	// ErrNoBackfill means a cancel named a table without a pending backfill.
	ErrNoBackfill = errors.New("pg: no pending backfill")
)

// minBackfillVersion is the first Postgres version whose pgoutput sends
// logical decoding messages.
const minBackfillVersion = 140000

// tableInfo is what a backfill needs to know about a table.
type tableInfo struct {
	oid       uint32
	ns, name  string
	keyCols   []string
	schema    *pb.Schema // as pgoutput's Relation message describes it
	rowFilter string     // the publication's row filter, if any
}

func (t tableInfo) qualified() string { return t.ns + "." + t.name }

// loadTable reads a table's definition. name is either what Postgres
// resolves (resolve) or the stored "namespace.name".
func loadTable(ctx context.Context, conn *pgx.Conn, publication, name string, resolve bool) (tableInfo, error) {
	var t tableInfo
	var kind, ident string
	q := `SELECT c.oid, n.nspname, c.relname, c.relkind::text, c.relreplident::text
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace `
	if resolve {
		q += `WHERE c.oid = to_regclass($1)`
	} else {
		q += `WHERE n.nspname || '.' || c.relname = $1`
	}
	err := conn.QueryRow(ctx, q, name).Scan(&t.oid, &t.ns, &t.name, &kind, &ident)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, fmt.Errorf("%w %s: no such table", ErrBackfillTable, name)
	}
	if err != nil {
		return t, fmt.Errorf("pg: look up table %s: %w", name, err)
	}
	if kind != "r" {
		return t, fmt.Errorf("%w %s: not a plain table (partitioned tables are not supported yet)", ErrBackfillTable, t.qualified())
	}
	rows, err := conn.Query(ctx, `SELECT a.attname FROM pg_index i
		CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid = $1 AND i.indisprimary ORDER BY k.ord`, t.oid)
	if err != nil {
		return t, fmt.Errorf("pg: read primary key of %s: %w", t.qualified(), err)
	}
	if t.keyCols, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return t, fmt.Errorf("pg: read primary key of %s: %w", t.qualified(), err)
	}
	if len(t.keyCols) == 0 {
		return t, fmt.Errorf("%w %s: no primary key", ErrBackfillTable, t.qualified())
	}
	// to_jsonb: rowfilter and attnames exist only since Postgres 15.
	var attnames []string
	err = conn.QueryRow(ctx, `SELECT coalesce(to_jsonb(p) ->> 'rowfilter', ''),
		ARRAY(SELECT jsonb_array_elements_text(coalesce(to_jsonb(p) -> 'attnames', '[]')))
		FROM pg_publication_tables p WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
		publication, t.ns, t.name).Scan(&t.rowFilter, &attnames)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, fmt.Errorf("%w %s: not in publication %s", ErrBackfillTable, t.qualified(), publication)
	}
	if err != nil {
		return t, fmt.Errorf("pg: read publication of %s: %w", t.qualified(), err)
	}
	// The columns as pgoutput sends them: no dropped or generated ones,
	// only those in the publication's column list, flagged when part of the
	// replica identity (all of them with REPLICA IDENTITY FULL).
	rows, err = conn.Query(ctx, `SELECT a.attname, a.atttypid, a.atttypmod,
		CASE c.relreplident WHEN 'f' THEN true WHEN 'n' THEN false
		ELSE a.attnum IN (SELECT unnest(i.indkey::int2[]) FROM pg_index i WHERE i.indrelid = c.oid
			AND CASE c.relreplident WHEN 'd' THEN i.indisprimary ELSE i.indisreplident END) END
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
		ORDER BY a.attnum`, t.oid)
	if err != nil {
		return t, fmt.Errorf("pg: read columns of %s: %w", t.qualified(), err)
	}
	t.schema = &pb.Schema{RelationOid: t.oid, Namespace: t.ns, Name: t.name, ReplicaIdentity: uint32(ident[0])}
	var col pb.Column
	_, err = pgx.ForEachRow(rows, []any{&col.Name, &col.TypeOid, &col.TypeModifier, &col.Key}, func() error {
		if len(attnames) == 0 || slices.Contains(attnames, col.Name) {
			t.schema.Columns = append(t.schema.Columns, &pb.Column{Name: col.Name, TypeOid: col.TypeOid, TypeModifier: col.TypeModifier, Key: col.Key})
		}
		return nil
	})
	if err != nil {
		return t, fmt.Errorf("pg: read columns of %s: %w", t.qualified(), err)
	}
	t.schema.Id = fsm.SchemaID(t.schema)
	return t, nil
}

// readChunk reads the rows after afterKey in primary key order, at most
// limits.MaxEvents rows and about limits.MaxBytes, as Postgres text: the
// form pgoutput sends. done means there are no more rows.
func readChunk(ctx context.Context, conn *pgx.Conn, t tableInfo, afterKey [][]byte, limits Limits) (rows []*pb.Row, keys []string, lastKey [][]byte, done bool, err error) {
	cols := make([]string, 0, len(t.schema.GetColumns())+len(t.keyCols))
	for _, c := range t.schema.GetColumns() {
		cols = append(cols, pgx.Identifier{c.GetName()}.Sanitize())
	}
	keyCols := make([]string, len(t.keyCols))
	for i, k := range t.keyCols {
		keyCols[i] = pgx.Identifier{k}.Sanitize()
	}
	var conds []string
	args := []any{pgx.QueryExecModeSimpleProtocol} // results in text format
	if len(afterKey) > 0 {
		ph := make([]string, len(afterKey))
		for i, v := range afterKey {
			ph[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, string(v))
		}
		conds = append(conds, fmt.Sprintf("(%s) > (%s)", strings.Join(keyCols, ", "), strings.Join(ph, ", ")))
	}
	if t.rowFilter != "" {
		conds = append(conds, "("+t.rowFilter+")")
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}
	sql := fmt.Sprintf("SELECT %s FROM ONLY %s%s ORDER BY %s LIMIT %d",
		strings.Join(append(cols, keyCols...), ", "), pgx.Identifier{t.ns, t.name}.Sanitize(), where,
		strings.Join(keyCols, ", "), limits.MaxEvents)
	res, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("pg: read %s: %w", t.qualified(), err)
	}
	defer res.Close()
	n, size, full := len(t.schema.GetColumns()), 0, false
	for res.Next() {
		raw := res.RawValues()
		if len(raw) != n+len(keyCols) {
			return nil, nil, nil, false, fmt.Errorf("pg: read %s: %d columns, want %d", t.qualified(), len(raw), n+len(keyCols))
		}
		r := &pb.Row{Values: make([]*pb.Value, n)}
		for i, v := range raw[:n] {
			if v == nil {
				r.Values[i] = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_NULL}
			} else {
				r.Values[i] = &pb.Value{Kind: pb.ValueKind_VALUE_KIND_TEXT, Data: slices.Clone(v)}
			}
		}
		lastKey = make([][]byte, len(keyCols))
		for i, v := range raw[n:] {
			lastKey[i] = slices.Clone(v)
		}
		rows, keys = append(rows, r), append(keys, encodeKey(lastKey))
		size += proto.Size(r)
		if size >= limits.MaxBytes {
			full = true
			break
		}
	}
	res.Close()
	if err := res.Err(); err != nil {
		return nil, nil, nil, false, fmt.Errorf("pg: read %s: %w", t.qualified(), err)
	}
	return rows, keys, lastKey, !full && len(rows) < limits.MaxEvents, nil
}

func emitWatermark(ctx context.Context, conn *pgx.Conn, content string) error {
	if _, err := conn.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, $2)", watermarkPrefix, content); err != nil {
		return fmt.Errorf("pg: write backfill watermark: %w", err)
	}
	return nil
}

func newNonce() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// nextBackfill returns the oldest pending backfill, or nil.
func nextBackfill(bfs []*pb.Backfill) *pb.Backfill {
	for _, bf := range bfs {
		if bf.GetState() == pb.BackfillState_BACKFILL_STATE_PENDING {
			return bf
		}
	}
	return nil
}

func findBackfill(bfs []*pb.Backfill, table string) *pb.Backfill {
	for _, bf := range bfs {
		if bf.GetTable() == table {
			return bf
		}
	}
	return nil
}

// permanent reports Postgres errors that reading the table again will not
// fix: data exceptions, missing objects or privileges, unsupported features.
func permanent(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	class := pe.Code[:2]
	return class == "22" || class == "42" || class == "0A"
}

// runBackfills runs pending backfills while the stream session lasts. It has
// its own connection and reconnects after errors, so a backfill problem
// never stops ingest.
func (s *Source) runBackfills(ctx context.Context, w *window, term uint64) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := s.backfillConn(ctx, w, term)
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("backfill interrupted, retrying", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

func (s *Source) backfillConn(ctx context.Context, w *window, term uint64) error {
	conn, err := pgx.Connect(ctx, s.cfg.URL)
	if err != nil {
		return fmt.Errorf("pg: connect: %w", err)
	}
	defer conn.Close(context.Background())
	limits := s.cfg.Limits.withDefaults()
	retries := 0
	for {
		bf := nextBackfill(s.state.Backfills())
		if bf == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		t, err := loadTable(ctx, conn, s.cfg.Publication, bf.GetTable(), false)
		if err == nil {
			var why string
			why, err = s.backfillChunk(ctx, conn, w, t, bf, limits)
			if err == nil && why != "" {
				// Normal under heavy writes to the chunk's keys; read it again.
				retries++
				s.log.Debug("backfill chunk given up, reading it again", "table", bf.GetTable(), "reason", why)
				if retries > 3 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(min(time.Duration(retries)*100*time.Millisecond, 5*time.Second)):
					}
				}
				continue
			}
		}
		if errors.Is(err, ErrBackfillTable) || permanent(err) {
			s.log.Error("backfill failed", "table", bf.GetTable(), "err", err)
			if err := s.propose(ctx, term, &pb.BackfillCommand{FailTable: bf.GetTable(), FailId: bf.GetId(), FailError: err.Error()}); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		retries = 0
		s.waitProgress(ctx, bf)
	}
}

// backfillChunk reads one chunk between watermarks and hands it to the
// stream. It returns why the chunk was given up, or "" when its rows were
// put into a batch.
func (s *Source) backfillChunk(ctx context.Context, conn *pgx.Conn, w *window, t tableInfo, bf *pb.Backfill, limits Limits) (string, error) {
	c := &chunk{nonce: newNonce(), table: bf.GetTable(), id: bf.GetId(), oid: t.oid, keyCols: t.keyCols, afterKey: bf.GetLastKey()}
	w.begin(c)
	defer w.abandon(c)
	if err := emitWatermark(ctx, conn, "low "+c.nonce); err != nil {
		return "", err
	}
	rows, keys, lastKey, done, err := readChunk(ctx, conn, t, c.afterKey, limits)
	if err != nil {
		return "", err
	}
	w.fill(c, t.schema, rows, keys, lastKey, done)
	if err := emitWatermark(ctx, conn, "high "+c.nonce); err != nil {
		return "", err
	}
	// The stream reaches the high watermark sooner or later: it is in the
	// WAL. Until then (the stream may be behind) there is nothing to do.
	select {
	case why := <-c.outcome:
		return why, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// waitProgress waits until the stored state of bf changed: the chunk just
// handed to the stream was stored. A chunk the state machine did not take
// is read again after a while.
func (s *Source) waitProgress(ctx context.Context, bf *pb.Backfill) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cur := findBackfill(s.state.Backfills(), bf.GetTable()); cur == nil || !proto.Equal(cur, bf) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// propose stores a backfill command in term and waits for its commit.
func (s *Source) propose(ctx context.Context, term uint64, cmd *pb.BackfillCommand) error {
	_, err := s.proposeIndex(ctx, term, cmd)
	return err
}

func (s *Source) proposeIndex(ctx context.Context, term uint64, cmd *pb.BackfillCommand) (uint64, error) {
	data, err := fsm.EncodeBackfill(cmd)
	if err != nil {
		return 0, err
	}
	p, err := s.node.ProposeAsync(ctx, term, data)
	if err != nil {
		return 0, err
	}
	return p.Wait(ctx)
}

// leaderTerm returns the current term if this node is the leader.
func (s *Source) leaderTerm(ctx context.Context) (uint64, error) {
	st, err := s.node.Status(ctx)
	if err != nil {
		return 0, err
	}
	if st.State != raft.StateLeader {
		return 0, &raft.NotLeaderError{Leader: st.Leader}
	}
	return st.Term, nil
}

// StartBackfill queues tables for a backfill (design D17) and returns the
// index of the command and the tables as "namespace.name". Every table must
// exist, be a plain table with a primary key and be in the publication.
// Leader only.
func (s *Source) StartBackfill(ctx context.Context, tables []string) (uint64, []string, error) {
	if len(tables) == 0 {
		return 0, nil, fmt.Errorf("%w: no tables given", ErrBackfillTable)
	}
	term, err := s.leaderTerm(ctx)
	if err != nil {
		return 0, nil, err
	}
	conn, err := pgx.Connect(ctx, s.cfg.URL)
	if err != nil {
		return 0, nil, fmt.Errorf("pg: connect: %w", err)
	}
	defer conn.Close(context.Background())
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return 0, nil, fmt.Errorf("pg: read server version: %w", err)
	}
	if version < minBackfillVersion {
		return 0, nil, fmt.Errorf("%w: needs Postgres 14 or later (server_version_num %d)", ErrBackfillTable, version)
	}
	var names []string
	for _, name := range tables {
		t, err := loadTable(ctx, conn, s.cfg.Publication, name, true)
		if err != nil {
			return 0, nil, err
		}
		if !slices.Contains(names, t.qualified()) {
			names = append(names, t.qualified())
		}
	}
	idx, err := s.proposeIndex(ctx, term, &pb.BackfillCommand{Start: names})
	if err != nil {
		return 0, nil, err
	}
	s.log.Info("backfill started", "tables", names, "index", idx)
	return idx, names, nil
}

// CancelBackfill stops pending backfills. A table may be given as
// "namespace.name", or by name alone if only one pending backfill has it.
// Leader only.
func (s *Source) CancelBackfill(ctx context.Context, tables []string) (uint64, []string, error) {
	term, err := s.leaderTerm(ctx)
	if err != nil {
		return 0, nil, err
	}
	if _, err := s.node.ReadIndex(ctx); err != nil {
		return 0, nil, err
	}
	var pending []string
	for _, bf := range s.state.Backfills() {
		if bf.GetState() == pb.BackfillState_BACKFILL_STATE_PENDING {
			pending = append(pending, bf.GetTable())
		}
	}
	var names []string
	for _, name := range tables {
		var match []string
		for _, p := range pending {
			if p == name || strings.HasSuffix(p, "."+name) {
				match = append(match, p)
			}
		}
		switch {
		case slices.Contains(match, name):
			match = []string{name}
		case len(match) == 0:
			return 0, nil, fmt.Errorf("%w of %s", ErrNoBackfill, name)
		case len(match) > 1:
			return 0, nil, fmt.Errorf("%w: %s is ambiguous (%s)", ErrNoBackfill, name, strings.Join(match, ", "))
		}
		names = append(names, match[0])
	}
	if len(names) == 0 {
		return 0, nil, fmt.Errorf("%w: no tables given", ErrNoBackfill)
	}
	idx, err := s.proposeIndex(ctx, term, &pb.BackfillCommand{Cancel: names})
	if err != nil {
		return 0, nil, err
	}
	s.log.Info("backfill cancelled", "tables", names, "index", idx)
	return idx, names, nil
}

// queueInitial starts a backfill of every published table with a primary
// key when the cluster has stored nothing yet (Config.BackfillInitial).
func (s *Source) queueInitial(ctx context.Context, conn *pgx.Conn, pos fsm.SourceState, srv server, term uint64) error {
	if !s.cfg.BackfillInitial || pos.CommitLSN != 0 || pos.PartialLSN != 0 || len(s.state.Backfills()) > 0 {
		return nil
	}
	if srv.version < minBackfillVersion {
		s.log.Warn("no initial backfill: it needs Postgres 14 or later", "server_version_num", srv.version)
		return nil
	}
	rows, err := conn.Query(ctx, `SELECT schemaname || '.' || tablename FROM pg_publication_tables
		WHERE pubname = $1 ORDER BY schemaname, tablename`, s.cfg.Publication)
	if err != nil {
		return fmt.Errorf("pg: list published tables: %w", err)
	}
	published, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("pg: list published tables: %w", err)
	}
	var names []string
	for _, name := range published {
		t, err := loadTable(ctx, conn, s.cfg.Publication, name, false)
		if errors.Is(err, ErrBackfillTable) {
			s.log.Warn("initial backfill skips a table", "err", err)
			continue
		}
		if err != nil {
			return err
		}
		names = append(names, t.qualified())
	}
	if len(names) == 0 {
		return nil
	}
	idx, err := s.proposeIndex(ctx, term, &pb.BackfillCommand{Start: names})
	if err != nil {
		return err
	}
	s.log.Info("initial backfill queued", "tables", names, "index", idx)
	return nil
}
