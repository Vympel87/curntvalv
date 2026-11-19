# curntValv — Design Document

Status: draft v0.1 · Working name: **curntValv**

## 1. What curntValv is

curntValv is a Postgres change data capture (CDC) server with a built-in, replicated
event log. It runs as a small cluster (usually 3 nodes) of one Go binary.

One sentence: **Kafka-grade replay and durability for Postgres changes, without
running Kafka.**

curntValv does three jobs in one process:

1. **Capture** — reads row changes from Postgres logical replication.
2. **Store** — keeps those changes in an ordered log, replicated with Raft.
3. **Deliver** — pushes changes to sinks (ClickHouse, webhook, ...) and lets
   applications subscribe directly.

### Goals

- No extra infrastructure: no Kafka, no ZooKeeper, no JVM, no external store.
- Survive the loss of one node (in a 3-node cluster) with no lost change.
- Replay: a new sink can start from the oldest retained change, without
  re-reading tables from Postgres.
- A slow or broken sink must never make the Postgres replication slot grow.
- Correctness is proven by automated fault tests, not by claims.

### Non-goals

- Not a general-purpose message broker. Only curntValv itself writes to the log.
- Postgres is the only source (for now).
- No stream processing (joins, windows). Only simple filter/transform later.
- Not built for huge scale. Target: one Postgres, thousands of changes/second.

## 2. Architecture

```
                    Postgres (wal_level = logical)
                         │  replication slot, pgoutput
                         ▼
┌──────────────────────── curntValv node (leader) ────────────────────────┐
│                                                                    │
│  Source ──propose──► Raft ──replicate──► followers (2 other nodes) │
│  (leader only)        │                                            │
│                       ▼ committed entries                          │
│                 ┌───────────┐      ┌──────────────────────────┐    │
│                 │ Log (WAL) │      │ State machine (metadata) │    │
│                 │ = events  │      │ source LSN, sink offsets,│    │
│                 └─────┬─────┘      │ pipeline config, schemas │    │
│                       │            └──────────────────────────┘    │
│          ┌────────────┼─────────────┐                              │
│          ▼            ▼             ▼                              │
│     Sink runner   Sink runner   Subscribe API (gRPC)               │
│     ClickHouse    Webhook       any node can serve reads           │
│                                                                    │
│  HTTP: dashboard (embedded), /metrics, /healthz · CLI: `curntvalv ...`  │
└────────────────────────────────────────────────────────────────────┘
```

Every node runs the same binary and holds the same log. Only the **leader**
talks to Postgres and runs sinks. Followers replicate, serve subscribe reads,
and are ready to take over.

### Components

| Package (planned)      | Job |
|------------------------|-----|
| `internal/wal`         | Segmented append-only log on disk (CRC, crash repair, truncate, compact) |
| `internal/raft`        | Raft: election, log replication, snapshots. Written from scratch |
| `internal/fsm`         | State machine: metadata KV applied from committed entries |
| `internal/source/pg`   | Postgres logical replication reader (pgoutput) |
| `internal/event`       | Turns committed entries into row changes; shared JSON form |
| `internal/sink`        | Sink runner (offsets, retries) + ClickHouse and webhook sinks |
| `internal/retention`   | How much log to keep: age, size and block-sink limits (D5) |
| `internal/transport`   | Raft messages between nodes over gRPC (D10) |
| `internal/pb`          | Generated protobuf/gRPC code (from `proto/`) |
| `internal/server`      | Client-facing gRPC (admin, subscribe) |
| `internal/web`         | HTTP: health, Prometheus metrics, cluster view, change feed, dashboard (D13) |
| `client`               | Public Go package: subscribe to changes with failover (the only public API) |
| `cmd/curntvalv`             | Single binary: `curntvalv server`, `curntvalv status`, `curntvalv subscribe`, ... |

## 3. Key design decisions

### D1. The Raft log *is* the event log

We do not keep a second copy of events. Each committed Raft entry is one of:

- `TxBatch` — the row changes of one Postgres transaction (large transactions
  are split into several entries, the last one is marked `final`)
- `Meta` — a metadata command (sink offset update, config change, ...)
- `Noop` — written by a new leader at the start of its term

A consumer **offset is a Raft index**. Offsets can have gaps (because of
`Meta`/`Noop` entries); consumers must only assume they grow.

Why: one write path, one fsync, one thing to reason about. This is the same
idea as Kafka KRaft and Redpanda. Cost: log retention and Raft compaction are
now the same problem (see D5).

### D2. Raft is written from scratch

Libraries exist (`etcd-io/raft`, `hashicorp/raft`). We write our own because
(a) understanding consensus deeply is a goal of this project and (b) D1 needs
control over how the log is stored and truncated. The public test suite of the
project is the safety net. We follow the Raft paper closely and do not invent
protocol changes.

Raft core talks to the world through three small interfaces — `LogStore`,
`StableStore` (term + vote), `Transport` — so tests can run a whole cluster
in memory with a fake network.

### D3. Ingest is effectively-once (source → log)

- Each `TxBatch` carries the transaction's commit LSN.
- The state machine stores `source_lsn` = commit LSN of the last applied
  transaction. Because it is updated by applying the same entry, the event and
  the checkpoint are atomic.
- The leader acknowledges an LSN to Postgres **only after** the transaction is
  committed in Raft (stored on a majority).
- After a leader change, the new leader starts replication from `source_lsn`
  and drops any transaction with commit LSN ≤ `source_lsn`.

Result: a transaction is never lost (Postgres keeps it until a majority has
it) and never stored twice (LSN de-duplication). For transactions split into
several entries the state machine also records how far a partly stored
transaction got, so even its events are stored exactly once (D11).

Fencing: Postgres allows one active connection per slot. A new leader
terminates the old connection before it starts. The old leader cannot commit
anything anyway — its Raft term is stale.

### D4. Delivery is at-least-once, with idempotency keys (log → sink)

- Each sink has an offset stored in the state machine (through Raft).
- A sink runner reads entries after its offset, delivers them, then advances
  the offset. If it crashes between the two steps, some events are delivered
  again.
- Every event has a stable key: `(commit LSN, position in transaction)`.
  Sinks use it to make redelivery harmless (ClickHouse changelog table as a
  `ReplacingMergeTree` keyed by it, the webhook's event `id`; see D12).

We say "no lost events, no visible duplicates for idempotent sinks". We do
not claim exactly-once in general.

### D5. Retention and compaction

- Snapshots contain **only metadata** (the state machine), never events.
- Retention is by age and/or size, per node: `--retention-age 7d`,
  `--retention-bytes 10GB`. The default keeps everything. The log is stored
  in segments (`--wal-segment-bytes`, default 64MB) and only whole segments
  are removed, never the active one. A segment is removable when its last
  write is older than the age limit, or while the log is larger than the
  size limit (oldest first). Age is measured from the local write time, so a
  node keeps *at least* the retention window.
- The log may be cut only at a snapshot index at or below that limit (Raft
  §7: compaction is a local decision of each server; the protocol does not
  change). A node that is far behind (or new) installs the leader's base
  snapshot and then replays the retained log — it ends up with the same
  history as everyone else.
- If a sink is further behind than retention, policy is per sink
  (`policy=` in `--sink`): `block` (default) keeps the log — the cut point
  never passes the sink's stored offset, and a block sink without an offset
  keeps the whole log — or `drop` cuts anyway; the sink then stops with
  state `failed` until it is reset (D12).
- Implementation: `internal/retention` computes the limit from the segment
  list of `wal` and the sink offsets in the state machine; Raft asks for it
  (`Config.Retention`) after every snapshot and every 100 ticks, so an idle
  cluster is cleaned up too. Snapshots newer than the cut point are kept as
  future cut points: one small metadata file per `SnapshotEntries` entries
  inside the retention window.
- Subscribe clients keep their own position and never hold the log back.

### D6. Slow sinks never hold back Postgres

The Postgres slot is acknowledged from Raft commit (D3), not from sink
progress. Sink lag is absorbed by the curntValv log. Only a cluster without a
majority stops acknowledging.

### D7. Postgres failover (later milestone)

After reconnecting to a new primary curntValv compares its `source_lsn` with the
slot:

- slot is at or behind `source_lsn` → safe, duplicates are dropped by D3;
- slot is missing or ahead of `source_lsn` → changes were lost on the Postgres
  side. curntValv stops the pipeline and reports it loudly; it never continues
  silently.

Recommended setup: Postgres 17+ failover slots, or Patroni permanent slots.

### D8. Static membership first

v0.1 clusters are defined in the config file (fixed list of nodes). Adding and
removing nodes at runtime (Raft single-server membership change) comes later.

### D9. Shape of the Raft implementation

- **Two layers.** `raft` (unexported) is the protocol as a synchronous state
  machine: `tick()`, `step(msg)`, `propose(data)` are plain calls with no
  goroutines and no clock. `Node` runs it in one goroutine (`Run(ctx)`) that
  owns all Raft state and is fed by channels (messages, proposals, ticks,
  status requests). No mutex guards protocol state.
- **Logical time.** Timeouts are counted in ticks (`ElectionTicks`,
  randomized in `[E, 2E)`, and `HeartbeatTicks`); `Node` turns ticks into
  wall time with `TickInterval`. Randomness comes from `Config.Seed`.
  Together this makes fault tests deterministic and reproducible by seed.
- **RPCs are one-way messages.** A `Message` is a request or a reply; the
  `Transport` may lose, delay, duplicate or reorder any of them. `Send` must
  not block.
- **Interfaces** (defined in `raft`): `LogStore` (Append, Read, FirstIndex,
  LastIndex, TruncateAfter, CompactBefore, Reset — seven methods, above the
  usual 1–3, so that `*wal.Log` fits without an adapter), `StableStore`
  (Load/Save of term + vote), `SnapshotStore` (List, Load, Save,
  DeleteBefore), `Transport` (Send), `Applier` (Apply, Snapshot, Restore).
  The file stores write to a temp file, fsync and rename it.
- **Entry encoding.** `| term u64 LE | type u8 | data |`; the index is the
  position in the `LogStore`. Raft only knows `Normal` and `Noop` entries;
  `TxBatch` vs `Meta` (D1) is decided by the layer above, inside `data`.
- **Propose blocks until commit** and returns the index. It fails with
  `NotLeaderError` (carries the known leader) or `ErrLeadershipLost` (the
  entry may still commit later; D3's LSN de-duplication covers the retry).
  `ProposeAsync(term, data)` queues a proposal and returns a handle to wait
  on; proposals from one caller are appended in call order, and queued
  proposals are appended together (one fsync, one round of
  AppendEntries). With a term set, a proposal is appended only while the
  node is leader of that term, so one caller's proposals of a term form a
  gap-free run in the log. This changes only how clients queue entries,
  not the protocol.
- **Persistence order.** Term/vote are saved and entries appended (fsync)
  before any message that depends on them is sent.
- **Restart.** `commitIndex` and `lastApplied` are volatile (Figure 2). A
  restarted node restores its latest snapshot and applies the entries after
  it. If the log does not continue that snapshot (a crash between saving an
  installed snapshot and resetting the log), the log is reset on start.
- **Snapshots (§7).** A snapshot is taken every `SnapshotEntries` applied
  entries. The log is cut at the newest snapshot allowed by
  `TrailingEntries` (at least that many entries behind; used by tests)
  and by `Retention` (the policies of D5); with neither the whole log is
  kept. InstallSnapshot is one message (the
  snapshot is metadata only, D5). The leader always sends its **base
  snapshot** — the oldest one the log continues from — not the latest, so a
  new or lagging node receives the same retained history as everyone else.
  Because `wal` compacts whole segments, the base can be a little after the
  first retained entry; such a node's history then starts a few entries
  later than the leader's.
- **AppendEntries into a compacted region.** If `prevLogIndex` is covered by
  the follower's snapshot (its term is no longer known), the follower
  replies success with its `commitIndex`: everything up to there is
  committed and therefore identical on every node.
- **Linearizable reads (§8).** `Node.ReadIndex` returns once the leader has
  committed an entry of its own term and a majority answered a heartbeat
  sent after the request (the request id travels as `ReadID` and is echoed
  in replies). The state machine is then read locally.
- **Rejection hint.** A rejected AppendEntries reply carries the follower's
  last index; the leader sets `nextIndex = min(nextIndex-1, hint+1)`. This is
  still "decrement and retry" (§5.3), it only skips indexes the follower
  cannot have. No other optimizations: no CheckQuorum, no pipelining of
  AppendEntries, no leader leases.
- **PreVote** (Ongaro's thesis §9.6, on by default, `--pre-vote=false`
  turns it off). When its election timeout fires, a node (also a candidate
  whose election failed, which first steps back to follower of its term)
  sends `MsgPreVote` with the term it would campaign in and its last log
  entry, without changing its own term or vote. A node grants it if that
  term is ahead of its own, the log is up to date (§5.4.1), and it has not
  heard from a leader for `ElectionTicks`; granting changes nothing on the
  granting node. Only with a majority of grants does the node start a real
  election. A refusal carrying a higher term makes the asker a follower of
  that term. So a node that was cut off keeps its term and rejoins without
  deposing a working leader. Pre-votes travel as two more message types
  over the same transport. A v0.1 node closes the stream when it gets one
  (the sender reconnects); during a rolling upgrade from v0.1, elections
  still succeed through the old nodes, which campaign directly, or turn
  PreVote off until every node is upgraded. From v0.2 on the transport
  drops message types it does not know and keeps the stream.
- **Broken invariants are fatal.** Two leaders in one term, truncating below
  `commitIndex`, or a log term above the current term stop the node with an
  error (`errSafety`) instead of continuing.

### D10. Node-to-node transport and APIs

- **gRPC + protobuf.** Definitions live in `proto/curntvalv/v1`; generated
  code in `internal/pb` is committed, so building needs no code generator.
  `make proto` regenerates it with buf v1.75.0, protoc-gen-go v1.36.12 and
  protoc-gen-go-grpc v1.6.2. Runtime dependencies: `google.golang.org/grpc`
  v1.80.0 (the newest release that still supports Go 1.24) and
  `google.golang.org/protobuf`.
- **RaftService.Stream** is a client stream of one-way Raft messages. Every
  node opens one stream to each peer; replies come back on the peer's own
  stream. `Transport.Send` puts a message in a bounded per-peer queue and
  never blocks; when the queue is full or the peer is down the message is
  dropped (Raft retries). Broken streams reconnect with exponential backoff
  capped at 500ms by default; gRPC's own connection backoff (up to 120s by
  default) is capped the same way. The cap must stay well below the election
  timeout: a node that comes back and does not hear from the leader in time
  starts an election. Without PreVote (D9) that still happens sometimes after
  a restart and costs one re-election (about a second without a leader).
- **The receiver drops** messages that are not addressed to it or come from
  an id outside the configured peers.
- **AdminService** (status, put/get/delete on the metadata KV) is served on
  the same port. Writes and linearizable reads only work on the leader; other
  nodes answer `FAILED_PRECONDITION` naming the leader, and the CLI tries
  the next address.
- **SubscribeService** streams row changes to applications from any node
  (D12).
- **No TLS or authentication yet.** Clusters must run on a trusted network
  until a later milestone adds TLS.
- Maximum gRPC message size is 64 MiB (revisit with large transactions, M4).

### D11. Postgres ingest

- **Where it runs.** Only on the Raft leader. A supervisor polls the node's
  role; it starts a session when the node becomes leader and cancels it as
  soon as the node is no longer leader of that term.
- **Session start.** `ReadIndex` first (the state machine then reflects
  everything committed and we are still leader), then read the stored
  position. Check `wal_level = logical` and that the publication exists. If
  the slot is held by another connection, terminate it (fencing, D3) and
  wait until it is free.
- **Slot rules (D7).** The slot is created only when the cluster has stored
  nothing yet. A missing slot after that, or a slot whose confirmed position
  is past the stored `end_lsn`, stops ingest with an error and is not
  retried in the same term: changes may have been lost and an operator must
  decide.
- **Streaming.** pgoutput protocol version 1: Postgres sends a transaction
  only after it committed, and BEGIN already carries the commit LSN, so
  batches can be stored while the transaction is still arriving. A batch is
  closed at 1000 events or 1 MiB. Batches are proposed in order without
  waiting for each commit (`ProposeAsync` with the session's term), with
  at most 128 entries or 8 MiB in flight. Any failed proposal ends the
  session, and a session waits for the outcome of all its proposals before
  it ends, so the next session in the same term reads a position that
  includes them.
- **Exactly-once storage.** The state machine holds the last fully stored
  transaction (`commit_lsn`, `end_lsn`) and, while a large transaction is
  stored only in part, its LSN and next event sequence number. A new leader
  restarts replication at `end_lsn`, drops transactions at or below
  `commit_lsn`, and skips the events of the partial transaction that are
  already stored. A batch that does not continue the stored position exactly
  is ignored on every node and counted as an anomaly (never seen in tests);
  failing instead would make a poison entry that stops the cluster forever.
- **Acknowledgement.** Postgres is told `flush = end_lsn` of the last stored
  transaction, never more: the last final batch in the committed prefix of
  the proposals in flight. Since Postgres 15, transactions without published
  changes are not sent at all, so when idle the source also stores the WAL
  position from Postgres keepalives (as an empty batch, at most once per
  status interval, default 10s) and then acknowledges it. Without this, a
  database whose published tables are quiet would keep WAL forever (D6).
- **Errors.** Connection and Raft errors retry with backoff (1s to 30s).
- **Testing.** Tests that need Postgres read `CURNTVALV_TEST_PG_URL` and are
  skipped without it; `make test-pg` runs them against a throwaway Postgres
  17 in Docker, and CI runs a Postgres service. The end-to-end checker writes
  3000 numbered rows while killing the leader three times and verifies every
  row is stored exactly once, in commit order.
- **Throughput.** Proposals in flight share appends, fsyncs and
  AppendEntries rounds. Before pipelining, ingest managed about 25
  transactions a second on a laptop (macOS full fsync); in the chaos test
  an unpaced writer on one connection commits about 700 small transactions
  a second (20 rows each on average) and ingest keeps up, apart from the
  backlog built up during faults. Delivery to a webhook (about 750 entries
  a second there) is now the slower part.

### D12. Delivery: sinks and subscribe

- **Offsets.** A sink's offset is the last log index it has delivered,
  stored in the state machine by a `Meta` command. Applying an offset lower
  than the stored one changes nothing, so a late command from an old leader
  cannot move a sink backwards. Snapshots (format v3) include the offsets
  and the index of the last entry with row changes.
- **Where sinks run.** Like the source (D11): on the leader only, with the
  same leader-gated supervisor. A session starts with `ReadIndex`, reads the
  stored offset and delivers from the next index. Every node must be started
  with the same `--sink` flags; offsets are kept by sink name.
- **Delivery loop.** Read up to 100 committed entries after the offset, pass
  their changes to the sink in batches of at most 500, then store the
  offset of the last entry read. A failed batch is retried with backoff
  (100ms to 30s) until it succeeds or leadership moves; the source and
  Postgres are not affected (D6). A crash or leader change between delivery
  and storing the offset delivers those changes again (D4).
- **Skipped entries.** With a table filter, entries whose changes all fail
  the filter are not delivered, but the offset is still moved past them, at
  most once a second. Entries that are not transaction batches (no-ops,
  offsets of other sinks) never cause an offset update by themselves, so
  offset commands do not feed each other.
- **New sinks** start with the oldest retained entry, or with `start=latest`
  at the current commit index. A sink whose undelivered entries were removed
  by log compaction (only possible with `policy=drop`, D5) stops with an
  error (state `failed`) until it is reset, leadership changes or the node
  restarts.
- **Reset and replay.** `curntvalv sink reset NAME --to
  earliest|latest|INDEX` (`AdminService.ResetSink`, leader only, configured
  sinks only) stores a new offset through Raft — also a lower one — and
  starts a new *generation* of the sink's offsets. Offset commands carry the
  generation they were made for; one for another generation is ignored, so
  a runner that has not yet seen the reset cannot move the offset forward
  again. A runner checks the generation after every read and starts a new
  session when it changed. `earliest` is stored as offset 0: "from the
  oldest retained entry". Snapshot format v4 adds the generation; offset
  commands and snapshots from M5 (generation 0) are still read.
- **Webhook.** One POST per batch: `{"sink": NAME, "events": [...]}` with
  the shared JSON form of a change. 2xx means delivered. The
  `Idempotency-Key` header ("FIRST_ID..LAST_ID") is stable across retries of
  the same request, but a new leader may cut batches differently, so
  receivers de-duplicate on each event's `id` (`"<commit LSN>:<seq>"`).
- **ClickHouse.** Through the HTTP interface with the standard library, so
  no new dependency. One changelog table (default `curntvalv_events`),
  created if missing: event id, log index, LSN, seq, xid, commit time,
  source table, op, and `before`/`after` as JSON text. Engine
  `ReplacingMergeTree ORDER BY (source_table, lsn, seq)`: a redelivered
  change has the same key and disappears on merge; `FINAL` hides it before
  that. Mirror tables (one per Postgres table, upserted by primary key, with
  typed columns) are planned after v0.1.
- **Subscribe API.** `SubscribeService.Subscribe(from_index, tables)` is a
  server stream that any node can serve. It reads only entries the node has
  applied (so every schema is known), sends each schema once per stream,
  splits responses at about 1 MiB between log entries, and waits for new
  entries through a notification from the state machine instead of
  polling. Offsets are managed by the client: `next_index` in each response
  is where to resume. Server-side named consumers may follow later.
  The public Go package `client` wraps it: `Subscribe` returns an
  iterator of changes, moves to the next node when a stream fails and
  continues from the last `next_index`, and ends with `ErrCompacted` or
  `ErrInvalid` when retrying cannot help. Its `Change` marshals to the
  shared JSON form. It is the only package outside `internal/`, so it is
  the one API kept stable. `curntvalv subscribe` uses it and prints changes
  as JSON lines.
- **Status.** `curntvalv status` shows the source state and, per sink, its
  state on the leader, stored offset, lag (log entries between the offset and
  the newest entry with row changes, so 0 when nothing is left to deliver),
  changes delivered and last error.
- **Testing.** Runner tests use an in-memory node and cover retries, leader
  changes, filters, batching and compaction. The end-to-end checker also
  runs a webhook receiver that fails one request in five, a subscriber
  process and (with `CURNTVALV_TEST_CH_URL`, `make test-ch`) a ClickHouse
  sink; after de-duplication by event id each must hold every row exactly
  once.

### D13. HTTP: health, metrics, dashboard, change feed

- **One read-only HTTP server per node** (`internal/web`), on
  `--http-listen` (default `127.0.0.1:8080`, empty turns it off). It has no
  authentication, so the default binds to localhost only; changes go
  through the gRPC admin API, never through HTTP.
- **Health.** `/healthz` answers 200 while the node's Raft loop answers;
  `/readyz` also needs a known leader, so a node cut off from the majority
  is not ready.
- **Metrics.** `/metrics` in the Prometheus text format (0.0.4), written
  with the standard library (no client library dependency). Values are
  read at scrape time from the local admin status plus counters kept by
  Raft (elections, snapshots), the source (entries, transactions, changes)
  and the sink runners (failures). Per-sink values come from the node that
  runs the sinks, i.e. the leader; scrape every node. Names start with
  `curntvalv_`; counters end in `_total`.
- **Cluster view.** `/api/status` asks every node for its admin status in
  parallel (2s timeout; an unreachable node is shown as such) and returns
  JSON: the nodes in `--peers` order and the leader's view of source, sinks
  and retention. Any node can serve it.
- **Change feed.** `/api/events[?table=T][&from=INDEX]` streams row changes
  of applied entries as server-sent events: the log index is the event id,
  the data is the shared JSON form (D12). Without `from` it starts with the
  next change; a reconnecting browser sends `Last-Event-ID`, which resends
  that whole entry, so clients de-duplicate on the change's `id`. A feed
  that falls behind retention continues with the oldest entry. At most 16
  feeds per node; idle feeds get a comment every 15s.
- **Dashboard.** Static HTML, CSS and plain JavaScript embedded in the
  binary (no build step, no CDN). It polls `/api/status` every 2s and
  follows the change feed.
- **Shutdown.** Requests use the node's context, so open feeds end when the
  node stops.

### D14. Local development: `curntvalv dev`

- **One node, same code.** `dev` builds an ordinary server configuration
  (node 1 of a one-node cluster) and runs it; there is no separate code
  path. Defaults are for a laptop: data in `./.curntvalv`, gRPC on
  `127.0.0.1:7001` (the CLI's first default address), dashboard on
  `127.0.0.1:8080`, Postgres from `--pg-url`, `CURNTVALV_PG_URL` or
  `DATABASE_URL`.
- **Fail early with the fix.** Before starting, `dev` checks `wal_level`
  and the publication (`pgsource.Check`, the same check a session runs)
  and prints the SQL to fix them, instead of retrying in the background
  like a server does.
- **Browser.** Once `/healthz` answers, the dashboard is opened with the
  platform's opener (`open`, `xdg-open`, `rundll32`); `--no-open` skips it.
- **Install.** `go install …/cmd/curntvalv@VERSION` or `go get -tool` (Go
  1.24+); without release `-ldflags` the version comes from the build info.

## 4. Data model

```
Event {
  key        (commit_lsn, seq)       // idempotency key
  table      "public.orders"
  op         insert | update | delete | truncate
  before     row image (only with REPLICA IDENTITY FULL, else key columns)
  after      row image
  commit_ts  timestamp from Postgres
  schema_id  → schema registry in the state machine
}
```

Schemas (column names and types, from pgoutput `Relation` messages) are stored
once in the state machine and referenced by id. The id is derived from the
schema's content (first 8 bytes of a SHA-256), so every node computes the same
id and a changed table gets a new one. Entries are protobuf (`TxBatch` in
`proto/curntvalv/v1/event.proto`); JSON is produced at the edges (webhook,
CLI, dashboard).

Column values are kept exactly as pgoutput sends them: Postgres text format,
NULL, or "unchanged TOAST" (a large value an UPDATE did not touch; Postgres
does not send it). Converting text to typed values is the sinks' job.

## 5. Failure scenarios the tests must cover

| Scenario | Expected |
|---|---|
| Leader killed while ingesting | New leader within seconds; no lost / duplicated transaction in the log |
| Follower down for a while, comes back | Catches up from log, or snapshot + log |
| Network partition, leader in minority | Minority stops acknowledging Postgres; majority elects a leader; old leader's uncommitted entries are discarded |
| Crash during disk write (torn record) | Node restarts, repairs the tail, rejoins |
| Sink down for hours | Postgres slot does not grow; sink resumes from its offset |
| New sink added | Receives history from the oldest retained offset |
| Postgres primary failover | Continue if safe, stop loudly if not (D7) |

End-to-end checker: write N numbered rows while injecting faults, then verify
that the sink contains exactly rows 1..N.

The checker runs in two forms (`cmd/curntvalv`, with Postgres):

- `TestEndToEndPostgresNoLossNoDuplicates` (part of `make test-pg`): leader
  kills during a fixed workload; the log and every consumer hold each row
  exactly once.
- `TestChaos` (`make chaos`, `CHAOS_TIME`, default 2m): random faults from a
  logged seed (`CHAOS_SEED` repeats a run) — SIGKILL of the leader or any
  node, SIGSTOP of the leader for up to 3s (a stand-in for a partition;
  real partitions are covered by the in-process Raft tests), a restart of
  all nodes, `pg_terminate_backend` on the walsender, and webhook outages —
  with small retention, so logs are cut and nodes install snapshots
  meanwhile. Every consumer must hold exactly rows 1..N after
  de-duplication by event id, and every node the same log.
- `TestEndToEndRetentionAndReplay` (part of `make test-pg`): size retention,
  a block sink holding the cut back, a drop sink that falls behind and is
  recovered with a reset, a sink added by a rolling restart, and a replay.

## 6. Open questions

- Subscribe API: client-managed offsets are done (D12). Server-side named
  consumers (offset stored in curntValv, like a sink) may follow.
- Initial table snapshot (backfill of rows that existed before the slot):
  needed for real use, planned after v0.1.
- Entry encoding: the Raft entry header is hand-written binary (D9),
  node-to-node messages are protobuf (D10). The encoding of `TxBatch`
  payloads is decided in M4.
- In-memory offset index in `wal` costs 8 bytes per entry; switch to a sparse
  index if logs get very large.
- Delivery throughput: sinks read 100 entries at a time and small
  transactions are one entry each. Larger reads or merging small
  transactions into one entry may follow if needed.
