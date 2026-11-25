package pg

import (
	"encoding/binary"
	"strings"
	"sync"

	"github.com/jackc/pglogrepl"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Backfill watermarks (design D17). A chunk of a table is read between two
// transactional logical decoding messages, a low and a high watermark, that
// the replication stream delivers in commit order with everything else.
// Rows of the chunk whose key changed between the watermarks are dropped:
// the stream carries a newer version. The rest are unchanged from the read
// until the high watermark, so they are stored at its position.

// watermarkPrefix is the prefix of curntValv's logical decoding messages.
const watermarkPrefix = "curntvalv.backfill"

// maxWindowKeys bounds the keys remembered while a chunk's window is open;
// a busier window is given up and the chunk read again.
const maxWindowKeys = 100_000

// chunk is one backfill chunk on its way from the table into the log.
type chunk struct {
	nonce    string
	table    string
	id       uint64
	oid      uint32   // relation OID in the stream
	keyCols  []string // primary key columns
	afterKey [][]byte

	// Set by the reader before it writes the high watermark.
	ready   bool
	schema  *pb.Schema
	rows    []*pb.Row
	keys    []string // encoded primary key of each row
	lastKey [][]byte
	done    bool

	// Set by the stream.
	open    bool
	changed map[string]bool
	invalid string // why the chunk cannot be used, if it cannot

	// outcome receives "" when the chunk's rows were put into a batch,
	// or why it was given up.
	outcome chan string
}

// window is the chunk currently between its watermarks, shared by the
// backfill reader and the decoder.
type window struct {
	mu  sync.Mutex
	cur *chunk
}

// begin makes c the current chunk; a previous one is given up.
func (w *window) begin(c *chunk) {
	c.changed, c.outcome = map[string]bool{}, make(chan string, 1)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur != nil {
		w.cur.outcome <- "replaced"
	}
	w.cur = c
}

// fill hands the rows read to the stream.
func (w *window) fill(c *chunk, schema *pb.Schema, rows []*pb.Row, keys []string, lastKey [][]byte, done bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c.schema, c.rows, c.keys, c.lastKey, c.done, c.ready = schema, rows, keys, lastKey, done, true
}

// abandon gives c up if it is still current.
func (w *window) abandon(c *chunk) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == c {
		w.cur = nil
	}
}

// watermark handles one of our messages. At the high watermark it returns
// the chunk to store, or nil.
func (w *window) watermark(content string) *chunk {
	kind, nonce, _ := strings.Cut(content, " ")
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.cur
	if c == nil || c.nonce != nonce {
		return nil // an earlier session's, or another cluster's
	}
	switch kind {
	case "low":
		c.open = true
		return nil
	case "high":
		w.cur = nil
		switch {
		case !c.open:
			c.outcome <- "high watermark before the low one"
		case c.invalid != "":
			c.outcome <- c.invalid
		case !c.ready:
			c.outcome <- "rows not read before the high watermark"
		default:
			return c
		}
	}
	return nil
}

// observe records a change seen in the stream while a window is open.
func (w *window) observe(rel uint32, s *pb.Schema, op pb.Op, before, after *pglogrepl.TupleData) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.cur
	if c == nil || !c.open || c.invalid != "" || rel != c.oid {
		return
	}
	if op == pb.Op_OP_TRUNCATE {
		c.invalid = "table truncated"
		return
	}
	for _, t := range []*pglogrepl.TupleData{before, after} {
		if t == nil {
			continue
		}
		k, ok := tupleKey(s, c.keyCols, t)
		if !ok {
			c.invalid = "change without its primary key"
			return
		}
		c.changed[k] = true
	}
	if op == pb.Op_OP_DELETE && before == nil {
		c.invalid = "delete without its primary key"
	}
	if len(c.changed) > maxWindowKeys {
		c.invalid = "too many changes while the chunk was read"
	}
}

// tupleKey returns the encoded primary key of a tuple from the stream.
func tupleKey(s *pb.Schema, keyCols []string, t *pglogrepl.TupleData) (string, bool) {
	if s == nil {
		return "", false
	}
	vals := make([][]byte, 0, len(keyCols))
	for _, name := range keyCols {
		i := columnIndex(s, name)
		if i < 0 || i >= len(t.Columns) || t.Columns[i].DataType != pglogrepl.TupleDataTypeText {
			return "", false
		}
		vals = append(vals, t.Columns[i].Data)
	}
	return encodeKey(vals), true
}

func columnIndex(s *pb.Schema, name string) int {
	for i, c := range s.GetColumns() {
		if c.GetName() == name {
			return i
		}
	}
	return -1
}

// encodeKey encodes key values for comparison: each one length-prefixed.
func encodeKey(vals [][]byte) string {
	var b []byte
	for _, v := range vals {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return string(b)
}

// events returns the chunk's rows that did not change in the window.
func (c *chunk) events() []*pb.Event {
	var out []*pb.Event
	for i, r := range c.rows {
		if !c.changed[c.keys[i]] {
			out = append(out, &pb.Event{SchemaId: c.schema.GetId(), Op: pb.Op_OP_BACKFILL, After: r})
		}
	}
	return out
}
