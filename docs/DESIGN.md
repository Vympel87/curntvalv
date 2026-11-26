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

### D7. Postgres failover

curntValv continues after a Postgres failover when that is safe and stops
loudly when it is not; it never continues silently. The danger is not only
a lost slot: after a failover to a standby that lagged, the new primary
writes new transactions at LSNs the cluster has already stored from the old
primary, and LSN de-duplication (D3) would drop them as "already stored".

- **Identity.** Every `TxBatch` carries the system identifier and timeline
  of the server it was read from (`IDENTIFY_SYSTEM`); the state machine keeps
  them with the position (snapshot version 5). Batches of v0.2 carry none;
  the first batch that does sets them. A batch of another system identifier
  is an anomaly; only a reset may switch.
- **Checks at every session start**, before the slot is touched (so a slot
  synced to a promoted standby is not trusted blindly):
  - the server is in recovery (a standby, e.g. while a virtual IP or DNS
    still moves): retried, not fatal;
  - another system identifier: `ErrSourceChanged`;
  - an older timeline than the stored one (an old primary came back):
    `ErrDiverged`;
  - a newer timeline: the server's `TIMELINE_HISTORY` says where the stored
    timeline ended; a stored position past that switch point is
    `ErrDiverged`, with both LSNs in the message;
  - a stored position past the server's flushed WAL: `ErrDiverged`;
  - then, as before, the slot: missing (`ErrSlotLost`) or ahead of the
    stored position (`ErrSlotAhead`); at or behind is safe, D3 drops what is
    sent again.
  The "stored position" is the end LSN, which includes idle positions from
  keepalives: if the old primary reported WAL the standby never got, the
  new primary may commit transactions there, so that is a divergence too.
- **Failover slots.** On Postgres 17 the slot is created with
  `FAILOVER true`, and an existing slot is altered to it, so standbys with
  `sync_replication_slots` keep a copy. The primary should list the
  standbys' physical slots in `synchronized_standby_slots`: then it does not
  send logical changes before the standbys have them, and a failover never
  diverges. Without it (or with asynchronous Patroni) a failover can diverge
  and stop ingest; the node logs this once. Postgres 14–16 work as before.
- **Recovery: `curntvalv source reset --to-slot`.** The fatal errors above
  need an operator. A reset, on the leader whose source failed in this term,
  connects to Postgres, takes the slot's confirmed position (creating the
  slot if it is missing) and stores a `TxBatch` with one `OP_SOURCE_RESET`
  event carrying the old and new position, system identifier and timeline,
  and the error. The state machine replaces the position with it (commit
  LSN lsn-1, end LSN lsn, as for any position-only batch). The event goes
  through the log like a change: sinks and subscribers receive it whatever
  their table filter, so consumers learn that changes between the two
  positions may be missing (slot ahead, lost slot) or repeated (slot behind,
  replayed from the shared part of the history). Ingest then starts again
  on its own.
- **Connection.** For a virtual IP or DNS name nothing is needed; with
  several hosts use libpq's `target_session_attrs=primary`.
- **Testing.** `make test-failover` creates a Postgres 17 primary and a
  streaming standby with slot sync for each scenario and moves a TCP proxy
  (the virtual IP) after `pg_promote()`: with `synchronized_standby_slots`
  ingest continues and stores every row once; without it, after the
  standby was cut off, ingest stops with `ErrDiverged` instead of skipping
  the new primary's rows, and a reset continues; with it and the standby
  cut off, the primary holds the changes back and the failover is safe.
  Disabling the checks makes the second scenario fail.
- **Upgrading.** v0.2 nodes cannot read snapshot version 5 or later and do
  not know the reset event; upgrade all nodes before a reset.

### D8. Static membership first

v0.1 and v0.2 clusters are defined by `--peers` (a fixed list of nodes).
Since v0.3 nodes can be added and removed at runtime (D18); `--peers` is
then only the initial configuration.

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
- **Membership** changes one server at a time (D18): the voters of the
  current configuration replace the fixed peer list in elections, commits
  and reads.
- **Broken invariants are fatal.** Two leaders in one term, truncating below
  `commitIndex`, or a log term above the current term stop the node with an
  error (`errSafety`) instead of continuing.

### D10. Node-to-node transport and APIs

- **gRPC + protobuf.** Definitions live in `proto/curntvalv/v1`; generated
  code in `internal/pb` is committed, so building needs no code generator.
  `make proto` regenerates it with buf v1.75.0, protoc-gen-go v1.36.12 and
  protoc-gen-go-grpc v1.6.2. Runtime dependencies: `google.golang.org/grpc`
  v1.80.0 (the newest release that still supports Go 1.24) and
  `google.golang.org/protobuf`. The Kafka sink adds franz-go (D20).
- **RaftService.Stream** is a client stream of one-way Raft messages. Every
  node opens one stream to each peer; replies come back on the peer's own
  stream. `Transport.Send` puts a message in a bounded per-peer queue and
  never blocks; when the queue is full or the peer is down the message is
  dropped (Raft retries). Broken streams reconnect with exponential backoff
  capped at 500ms by default; gRPC's own connection backoff (up to 120s by
  default) is capped the same way. The cap must stay well below the election
  timeout: a node that comes back and does not hear from the leader in time
  would start an election; PreVote (D9) keeps that from deposing the leader.
- **The receiver drops** messages that are not addressed to it or come from
  an id outside the configured peers.
- **AdminService** (status, put/get/delete on the metadata KV) is served on
  the same port. Writes and linearizable reads only work on the leader; other
  nodes answer `FAILED_PRECONDITION` naming the leader, and the CLI tries
  the next address.
- **SubscribeService** streams row changes to applications from any node
  (D12).
- **TLS and tokens** are optional (D15); without them a cluster must run on
  a trusted network.
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
- **Streaming.** pgoutput protocol version 1, and since Postgres 14 with
  `messages 'true'` for the backfill watermarks (D17); other logical
  decoding messages are ignored. Postgres sends a transaction
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
  Such a batch at WAL position P has commit LSN P-1 and end LSN P: a
  transaction still open at the keepalive may write its commit record
  exactly at P, Postgres sends it after a restart from P (it skips only
  commits before the start), and de-duplication must not take it as stored.
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
  that. Mirror tables (one typed table per Postgres table with its current
  rows) are `mode=mirror` (D24).
- **Subscribe API.** `SubscribeService.Subscribe(from_index, tables)` is a
  server stream that any node can serve. It reads only entries the node has
  applied (so every schema is known), sends each schema once per stream,
  splits responses at about 1 MiB between log entries, and waits for new
  entries through a notification from the state machine instead of
  polling. Offsets are managed by the client: `next_index` in each response
  is where to resume, or the cluster keeps it for a named subscription
  (D22).
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
  `--http-listen` (default `127.0.0.1:8080`, empty turns it off). Without a
  token (D15) it is open, so the default binds to localhost only; changes go
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

### D15. Security: TLS and tokens

- **Off by default**, so `dev` and the compose demo stay simple. Both are
  standard library plus grpc's `credentials` package; no new dependency
  (`internal/security`).
- **TLS** (`--tls-cert`, `--tls-key`, `--tls-ca`, all or none) covers the
  gRPC port and the HTTP port. On gRPC it is mutual between nodes: the
  listener asks for a client certificate and verifies it against the CA if
  one is presented (`VerifyClientCertIfGiven`), and Raft streams are
  accepted only with a verified one. Applications and the CLI connect
  without a certificate, so one port serves both. Any certificate the CA
  signed may join Raft traffic; peer ids are not bound to certificates.
  Clients verify the server's name against the certificate's SANs, so a
  node's certificate must list the names and IPs others use for it.
  Certificates are read at start; rotating them needs a restart.
- **Token** (`--auth-token-file` or `CURNTVALV_AUTH_TOKEN`, the same on
  every node; not a flag value, which `ps` would show). gRPC calls carry
  `authorization: Bearer TOKEN`; admin and subscribe calls need it. Raft
  streams need it too unless they come with a verified certificate, so a
  token without TLS still guards every port (in plain text: the node warns).
  HTTP accepts Basic (any user, the token as password: browsers ask for it
  and their EventSource sends it) or Bearer; `/healthz` and `/readyz` stay
  open for load balancers. Tokens are compared in constant time.
- **Clients.** The CLI takes `--tls-ca` / `CURNTVALV_TLS_CA` and
  `--token-file` / `CURNTVALV_TOKEN`; the Go client `client.WithTLS` and
  `client.WithToken`.
- **`curntvalv certs NODE...`** writes a CA and ECDSA P-256 node
  certificates (server and client use) for trying TLS; production setups
  should use their own CA.
- **Testing.** `internal/security` tests every combination (node and
  application certificates, wrong CA, missing or wrong token, plain text);
  an end-to-end test runs a 3-node cluster with TLS and a token through a
  leader kill and checks the CLI and HTTPS.

### D16. Configuration file

- **Format: `KEY = VALUE` lines, KEY a server flag name.** Read with the
  standard library: no YAML or TOML dependency, and nothing new to learn,
  since every key is documented by `curntvalv server -h`. `#` starts a
  comment line; an unquoted value ends at ` #`; a value may be double
  quoted in Go syntax. `sink` may repeat; any other key twice, an unknown
  key (`--id`, `colour`), or a bad value is an error naming the file and
  line. No sections, includes or variable expansion.
- **`--config FILE` or `CURNTVALV_CONFIG`.** The file only fills in flags
  the command line left unset, so precedence is flag > environment > file >
  default. The environment variables that set flags (`CURNTVALV_PG_URL`,
  `CURNTVALV_AUTH_TOKEN` in place of `auth-token-file`) override the file.
  A `--sink` on the command line replaces the file's sinks rather than
  adding to them, so one flag never merges with a hidden list.
- **All validation stays in one place.** File values go through
  `flag.FlagSet.Set` and then the same checks as flags; there is no second
  config struct to keep in sync.
- **Secrets.** The token stays in its own file. A Postgres URL with a
  password in a file others can read starts the node with a warning.
- **`--check`** parses everything, loads the TLS files and exits: 0 if the
  node would start, 2 on a usage error, 1 if a file cannot be loaded.
- **Not done.** `curntvalv dev` ignores `CURNTVALV_CONFIG`; there is no
  reload on SIGHUP (a restart applies changes, as with certificates).

### D17. Backfill

Rows that existed before the slot never pass through logical replication.
A backfill reads them from the table into the log, next to the stream.

- **Watermarks, not an exported snapshot.** Reading every table with the
  snapshot exported at slot creation would only work once, at the very
  first start, would hold one transaction open for the whole copy, and
  could not be resumed. Instead (the DBLog approach) the leader reads a
  table in primary key order, one chunk at a time, between two
  transactional logical decoding messages (`pg_logical_emit_message`,
  prefix `curntvalv.backfill`, sent by pgoutput with `messages 'true'`
  since Postgres 14):
  1. write the low watermark `low NONCE` and commit;
  2. `SELECT` the next rows after the stored key (`ORDER BY` the primary
     key, `LIMIT` 1000, stopping at 1 MiB), with the publication's row
     filter and column list, in text format;
  3. write the high watermark `high NONCE`.
  The stream sees the watermarks in commit order with everything else.
  Between them it remembers the primary keys of the table's changes; at the
  high watermark it drops chunk rows with those keys and stores the rest as
  `OP_BACKFILL` events of the watermark's transaction.
- **Why that is correct.** A chunk row whose key did not change between
  the watermarks had the same value at the `SELECT` (which ran between
  them) and at the high watermark, so storing it at the high watermark puts
  it in the right place in the log: every earlier change of the row is
  before it, every later one after it. A row that did change is carried by
  the stream, with its newest version, before the high watermark. A change
  whose key cannot be known (a delete without the old key, an unchanged
  TOASTed key, a TRUNCATE) gives the chunk up; it is read again with new
  watermarks. A replayed consumer ends with exactly the table: the
  end-to-end test checks this while three writers update, delete, insert
  and move rows just ahead of the backfill.
- **Exactly once, through the existing machinery.** The chunk is one final
  `TxBatch` at the high watermark's commit LSN, so the de-duplication of D3
  applies as to any transaction. The batch carries the progress
  (`BackfillChunk`: table, backfill id, the key it started after, the last
  key read, done), which the state machine applies only if it continues the
  stored progress of the same pending backfill. A chunk is at most 1000 rows
  and about 1 MiB, so it is never split. A new leader does not know the
  previous leader's nonces: their watermarks are empty transactions, and the
  chunk is read again from the stored key. Rows inserted ahead of the stored
  key while the backfill runs are read too, so they arrive as an insert and
  as a backfill row.
- **State.** The state machine keeps one `Backfill` per table: id (index of
  the command that started it), state (pending, done, cancelled, failed),
  last key, rows, end index, error. A metadata command (`BackfillCommand`)
  starts, cancels or fails backfills; starting a table with a pending
  backfill keeps it, starting a finished one begins again. Snapshot format
  v6 adds the backfills; v5 snapshots are still read.
- **Running.** The oldest pending backfill runs on the leader beside the
  stream session, with its own Postgres connection; its errors are retried
  and never stop ingest. A table that cannot be read any more (dropped, no
  primary key, a permission or data error) is marked failed through Raft.
- **Requirements.** Postgres 14 or later, a plain table (partitioned tables
  are not supported yet) with a primary key, in the publication. The rows
  use the schema pgoutput would send, built from the catalog (same columns,
  types and replica identity flags), so a backfilled row and the same row
  from the stream have the same schema id and the same text values.
- **Interface.** `curntvalv backfill start TABLE...` and `curntvalv
  backfill cancel TABLE...` (`AdminService.StartBackfill` and
  `CancelBackfill`, leader only); `start` checks every table first. With
  `--backfill-initial`, a cluster that has stored nothing yet queues every
  published table with a primary key before it starts streaming. Progress
  is in `curntvalv status`, the dashboard and `/metrics`
  (`curntvalv_backfill_rows`, `curntvalv_backfill_state`). Consumers see
  `"op":"backfill"` (`client.OpBackfill`) with the row in `after`, and treat
  it as an upsert.
- **Testing.** Unit tests for the window logic and the state machine; with
  Postgres, chunking, resuming after a restart, cancel and validation,
  `--backfill-initial`, equality with stream rows for many types, and the
  concurrent-writers replay check, which fails when the window logic is
  disabled. End to end, the leader is killed halfway through a backfill of
  40,000 rows: every row is backfilled exactly once.

### D18. Cluster membership changes

- **One server at a time** (Ongaro's thesis §4), not joint consensus
  (paper §6). Any two majorities of configurations that differ by one
  server overlap, so no extra phase is needed; the thesis recommends it as
  the simpler mechanism.
- **Configuration in the log.** An `EntryConfig` entry holds the members
  (id, address, voter or learner). A node uses the latest configuration
  in its log at once, committed or not (§4.1); an entry removed by a
  conflicting leader takes its configuration with it. Snapshots carry the
  configuration at their index: raft prepends it to the state machine's
  data behind a magic prefix. Snapshots taken before the first change have
  none and stay as they were, so older nodes can still install them; a
  node without a stored configuration uses `--peers`. Only voters
  campaign, vote counts and commit majorities count only voters, and the
  leader replicates to every member.
- **Safety rules.** A leader starts a change only when the previous one
  is committed and it has committed an entry of its own term (the fix to
  the single-server algorithm Ongaro published in 2015: without it, a new
  leader's change and an old leader's uncommitted one could form disjoint
  majorities).
- **Learners first** (§4.2.1). `curntvalv member add` adds the server as a
  learner: it receives the log (and the base snapshot) but does not vote
  or count for a majority, so a cluster never waits for a server that is
  still copying. The leader promotes it once it is no more than one
  AppendEntries (`MaxEntriesPerMsg`) behind; the command returns then.
  Calling it again only finishes what is missing.
- **Removing the leader** (§4.2.2). It keeps leading, without counting
  itself, until the configuration without it is committed, tells the
  others, and steps down; it never campaigns again.
- **Disruptive servers** (§4.2.3). A server that heard from a leader
  within the minimum election timeout ignores RequestVote, so a removed
  server that no longer gets heartbeats cannot raise the term. With
  PreVote (D9) such a server does not even start elections that could
  win.
- **Operations.** A new node starts with `--join` (empty data directory,
  `--peers` listing the current members, `--listen` its own address) and
  waits without campaigning; `curntvalv member add ID=HOST:PORT` adds it,
  `curntvalv member remove ID` removes one, `curntvalv member list` and
  `status` show the configuration with each member's match index.
  Addresses travel in the configuration: every node updates its Raft
  transport and the senders it accepts from the configuration it holds
  (checked every 200ms). `--peers` and `--join` may stay in a node's flags
  after the change; a stored configuration always wins.
- **Testing.** In the deterministic harness: adding a learner that does
  not count for a majority, promotion, removing a follower that keeps
  running without PreVote, the leader removing itself, one change at a
  time, rollback of an overwritten configuration, restarts from the log
  and from snapshots, and random changes under loss, reordering,
  partitions and crashes with the invariants of Figure 3 checked every
  round. Disabling the vote rule, counting learners, or the rollback each
  makes a test fail. End to end: a node joins, the leader is killed, a
  node is removed and the leader removes itself while Postgres changes
  flow in; every row arrives.

### D19. NATS, Redis and Meilisearch sinks

- **No new dependencies.** NATS's client protocol, Redis's RESP and
  Meilisearch's REST API are small enough for the standard library, like
  the ClickHouse sink; each sink keeps one connection and opens a new one
  after any error. Kafka's protocol is not; its sink uses a library (D20).
- **NATS JetStream.** One message per change, subject
  `<prefix>.<namespace>.<table>` (names reduced to subject-safe tokens),
  header `Nats-Msg-Id` = the change id, published with a reply subject and
  counted as delivered only on JetStream's acknowledgement. Publishes of a
  batch are pipelined on one connection, so they keep their order. The
  duplicate window (2 minutes by default) covers the redelivery after a
  leader change. The stream is created on first use if missing. The server
  pings idle clients; the sink only reads while delivering, so it reconnects
  after 30 seconds without use.
- **Redis Streams.** The entry ID is `<log index>-<seq>`, which grows with
  the log, instead of `*`. Redis refuses an ID that is not above the
  stream's last entry, so a redelivered change is refused: the sink then
  checks that the entry is there (or older than the first entry, when
  MAXLEN trimmed it) and otherwise fails, because another writer is ahead.
  Result: each change exactly once per stream. A sink reset that replays
  into the same stream writes nothing new.
- **Meilisearch.** A mirror, not a changelog: documents keyed by the
  replica identity key (`curntvalv_key`: the key values joined by `-` when
  they are plain, else `-` and their base64url encoding, so the forms
  cannot collide); "add or update" keeps unchanged TOASTed columns; a key
  change deletes the old document; truncate deletes all documents. Columns
  are typed from their type OID. Meilisearch works asynchronously, so the
  sink waits until the batch's tasks succeeded (a delete on a missing index
  counts); tasks of an index run in order, so retries converge.
- **Testing.** Protocol parsing, key encoding and request planning in unit
  tests; against real NATS, Redis and Meilisearch with `make test-sinks`
  (also in CI); end to end with a leader kill, which redelivers: Redis and
  JetStream hold exactly one entry per change, and Meilisearch equals the
  table. With automatic Redis IDs the end-to-end test finds the duplicates.

### D20. Kafka sink

- **franz-go** (`github.com/twmb/franz-go`, pure Go, no cgo), the only
  dependency added for a sink: Kafka's protocol (metadata, partition
  leaders, batches, idempotent producer, SASL) is too large to write by
  hand. v1.21.7, the newest release that supports Go 1.25 (v1.22 needs Go
  1.26). Only `pkg/kgo` and `pkg/sasl` are used; it brings `kmsg`,
  `klauspost/compress` and `pierrec/lz4`.
- **Records.** One per change: the value is the shared JSON form, the key
  the replica identity key as a JSON object (`{"id":"5"}`), so the changes
  of a row land in one partition in order; truncates and resets have no
  key. Headers `curntvalv-id` and `curntvalv-op`. Topic
  `curntvalv.{table}` by default (`{table}` = "namespace.name", characters
  Kafka does not allow become `_`; resets go to `_source_reset`). Topics
  are created by the brokers if they allow it; create them yourself to
  choose partitions and replication.
- **Delivery.** `acks=all` and the idempotent producer, so the client's
  own retries never duplicate or reorder a record; a batch counts as
  delivered when every record is acknowledged. After an error the producer
  is replaced. A new curntValv leader has a new producer id, so the
  redelivery after a leader change can repeat records: at least once, and
  consumers de-duplicate on `curntvalv-id`. Transactions would fence an old
  leader's producer but not make the hand-over exactly once, because the
  sink offset lives in Raft, not in Kafka.
- **Tombstones** for log-compacted topics are opt-in (D25).
- **Testing.** Configuration, topics and keys in unit tests; against a
  real Kafka 3.9 (KRaft) with `make test-sinks`: order per key, headers, a
  reset, an unreachable broker. End to end with a leader kill: every change
  of the log is in the topic.

### D21. Filters and projections per sink

- **Where.** In the sink runner, after the log is read and the table filter
  applied, before delivery, so every sink type has them and the log keeps
  everything (a sink reset or a new sink can choose differently). Changes
  a sink's shape removes are skipped like other tables' changes: the
  offset moves past them, at most once a second while nothing passes.
- **Filters.** `op=` (repeatable) keeps those operations. `where=COL=VALUE`
  and `where=COL!=VALUE` (repeatable, all must hold) compare the column's
  Postgres text value on the new row, or the old row for a delete; NULL
  equals nothing, an unchanged TOASTed value is unknown and fails, a table
  without the column fails, truncates pass (they have no row). An update
  that moves a row out of the filter is not delivered, so a mirror keeps
  the row as last seen: filtering on the old row too would need REPLICA
  IDENTITY FULL, and the simple rule is predictable.
- **Projection.** `columns=A+B` keeps only these columns, `drop=A+B`
  removes them, in the row images and in the schema handed to the sink (a
  new content-derived schema id). Key columns are always kept, because
  sinks de-duplicate and mirror by them.
- **Source resets always pass**: they tell a consumer that changes may be
  missing, which a filter must not hide.
- **Expressions, renames and masks** followed in D23.

### D22. Named subscriptions

- **A position kept in the cluster** for a subscriber, under a name, like a
  sink offset (D12): `Subscription{offset, generation, drop}` in the state
  machine, changed by a metadata command (create, commit, reset, delete).
  Commits only move forward within a generation; a reset sets any offset
  and starts a new generation. Snapshot format v7 adds them.
- **RPCs.** `JoinSubscription` returns the stored position and creates the
  subscription if it is new (after a ReadIndex, so a second join never
  creates it again with other settings; `must_exist` refuses to create).
  `CommitSubscription` stores a position: ABORTED after a reset, NOT_FOUND
  after a delete. Both are leader only; the stream itself is still the
  ordinary Subscribe from `offset + 1`, served by any node.
  `AdminService.ResetSubscription` and `DeleteSubscription` manage them;
  `Status` lists them with their lag.
- **At least once.** The client counts a log entry as processed when the
  loop asks for a change of a later entry (or the stream moved past it),
  and stores that position about once a second and when the loop ends.
  After a crash the entry in progress is delivered again; `Change.ID`
  identifies the copies. `ManualCommit` and `Client.Commit` let the
  program decide instead. A reset is noticed on the next commit: the
  client joins again and continues from the new position. One consumer per
  name: two would each read everything and store the larger position.
- **Created on first use.** A new name creates the subscription, at the
  oldest retained entry or (StartLatest) at the current commit index.
- **Retention.** With policy block (the default) the log is not cut past
  the offset, as for block sinks (D5); `status` names the subscription
  holding it. Unused subscriptions must be deleted. With drop, a
  subscriber left behind gets ErrCompacted.
- **CLI.** `curntvalv subscribe --name NAME [--start latest] [--policy
  drop]`; `curntvalv subscription list|reset NAME --to ...|delete NAME`.
  Metrics `curntvalv_subscription_offset` and `_lag_entries`.
- **Testing.** State machine and retention unit tests; RPCs against an
  in-memory node; the client against servers where only one is the
  leader, with a reset and a delete while it runs; end to end a
  subscription continues exactly after the stored position across a
  leader kill, is replayed after a reset, and is deleted.

### D23. Filter expressions, renames and masks

- **`where` takes an expression**: comparisons `= != <> < <= > >=`,
  `[not] in (...)`, `[not] like` (`%`, `_`), `is [not] null`, combined with
  `and`, `or`, `not` and parentheses; values quoted `'...'` (`''` for a
  quote) or bare words and numbers; column names bare or `"quoted"`. A
  hand-written recursive-descent parser in `internal/sink`, no library.
  Repeated `where=` options must all hold.
- **Types.** Values are Postgres text; numeric columns compared with a
  number literal compare exactly as numbers (`big.Rat`), boolean columns
  take `true`/`false`, everything else compares as bytes.
- **NULL and unknown.** NULL is a value of its own, as in D21: `=`, `<`,
  `in`, `like` are false for it and `!=`, `not in`, `not like` true, so the
  first form keeps its meaning. A value that is not known (an unchanged
  TOASTed value, binary data, a column the table lacks) makes its test
  unknown, combined with SQL's three-valued logic; a row passes only if the
  whole expression is true.
- **Compatibility.** A `where` that is not an expression but has the first
  form `COLUMN=VALUE` / `COLUMN!=VALUE` keeps its literal value
  (`name=John Smith`, `note=`). `--sink` values are split at commas outside
  parentheses and quotes, so an `in` list fits in a flag.
- **`mask=A+B`** replaces values with `***` (NULL stays NULL) and gives the
  column type text in the schema the sink sees; key columns are never
  masked, because sinks de-duplicate and mirror by them.
- **`rename=OLD:NEW+...`** renames columns in what the sink receives, key
  columns too (they stay keys). A rename to a name the table already has
  is not applied for that table.
- **Order.** Filters, then projection, masks and renames; every option
  names columns by their Postgres names.

### D24. ClickHouse mirror tables

- **`mode=mirror`** on a ClickHouse sink keeps one typed table per Postgres
  table (`into=` is a name template, default `{namespace}_{table}`)
  holding its current rows, next to the changelog mode of D12.
- **Engine.** `ReplacingMergeTree(_version, _deleted) ORDER BY (key
  columns)`: each change inserts the whole row, `_version` from its log
  position (`index << 24 | seq`), so a redelivered change replaces itself
  and a later change wins; a delete inserts the key with `_deleted = 1`.
  `SELECT ... FINAL` shows each row once, without deleted ones (checked on
  ClickHouse 25.8). A key change deletes the old key; a truncate runs
  `TRUNCATE TABLE` in its place in the batch. Changes of one key in a batch
  are merged into its last version before inserting.
- **Types.** bool → Bool, int2/4/8 → Int16/32/64, float4/8 → Float32/64,
  numeric(p,s) → Decimal(p,s) (numeric without a precision → String, to
  stay exact), uuid → UUID, date → Date32, timestamp and timestamptz →
  DateTime64(6, 'UTC'); everything else → String in Postgres text form. Key
  columns are not nullable, the others are. Values ClickHouse cannot hold
  (infinity, NaN, BC dates) become NULL.
- **Unchanged TOAST.** An update that leaves a large value untouched does
  not carry it. The sink takes it from the previous version of the row in
  the same batch, from the old row with REPLICA IDENTITY FULL, or reads it
  back from the mirror (`FINAL`, matching the key's text form); a row the
  mirror does not have gets NULL.
- **Schema changes.** A column added in Postgres is added with `ADD COLUMN
  IF NOT EXISTS`; dropped or retyped columns stay as they are. Tables
  without a key cannot be mirrored (an error for the sink). Projections,
  masks and renames (D21, D23) apply first: the mirror has the columns the
  sink sees.
- **Testing.** Type mapping and value conversion in unit tests; against a
  real ClickHouse: backfill, a TOASTed value kept, merged changes in one
  batch, a key change, a delete, redelivery of everything, a new column, a
  truncate, a one-column and a two-column key. End to end with a leader
  kill: the mirror equals the Postgres table. A one-column key bug (ClickHouse
  does not take `('7')` as a tuple) was found only end to end.

### D25. Kafka tombstones

- **`tombstones=true`** on a Kafka sink follows each delete record with a
  tombstone (the row's key, no value), and precedes an update that changes
  the key with a tombstone for the old key, so a log-compacted topic drops
  deleted and moved rows. Without it records are as in D20.
- **Opt-in**, because the Kafka sink of v0.4.0 had none, and consumers that
  do not expect records without a value would break.
- A tombstone carries the change's `curntvalv-id` and `curntvalv-op:
  tombstone`; it goes to the key's partition, so it follows the key's other
  records. An update with REPLICA IDENTITY FULL whose key stays the same
  gets none. Truncates and tables without a key get none (there is no key).
- **Testing.** Against a real Kafka: the records of each key in order, with
  and without the option; end to end in the stream sinks test.

### D26. Hashed columns

- **`hash=A+B`** replaces values with the hex HMAC-SHA256 of their
  Postgres text, under a secret read from `hash_key_file=PATH` at start (at
  least 16 bytes, surrounding white space removed; the same file on every
  node, like the sink flags). Equal values stay equal, so a sink can still
  join or count by them, but they cannot be guessed without the key, which
  a plain hash of low-entropy data (e-mail addresses, phone numbers) would
  allow.
- **Key columns may be hashed**, unlike masks: the hash is deterministic
  and collisions are negligible, so sinks still de-duplicate and mirror by
  the hashed key (pseudonymous ids). The column becomes text in the schema
  the sink sees.
- NULL stays NULL; an unchanged TOASTed value stays unchanged (it is not
  known), and a ClickHouse mirror fills it from the hashed value it holds.
  A column cannot be both masked and hashed. Filters see the original
  values. Changing the key changes every hash: a mirror then holds rows
  under both.

### D27. Rows leaving a filter

- **`on_leave=delete`** (default `skip`) keeps a filtered mirror right. An
  update whose new row fails `where` is delivered as a delete, unless its
  old row is known to have failed too (then the sink never had the row);
  a delete passes unless its old row is known to fail. The delete carries
  the old row, or the key taken from the new row. Deleting a row a mirror
  does not hold is harmless; keeping one it should not is not, so an
  unknown old row counts as a match.
- **The old row is mostly unknown.** With the default replica identity
  Postgres sends only the key columns of the old row and NULL for the
  others; these NULLs are not values, so the filter sees them as unknown
  (also without `on_leave`). Before this, a delete was judged on them:
  `where=status!=paid` passed every delete and `where=status=paid` none.
  Now a delete whose filter needs a non-key column is skipped unless
  `on_leave=delete` (or REPLICA IDENTITY FULL tells).
- `op=` judges the change as it arrives: an update made a delete passes
  `op=update`.
- **Testing.** Every combination of old row known in, known out and
  unknown, for updates and deletes, with and without the option; end to
  end a ClickHouse mirror with `where=status = paid` equals `SELECT ...
  WHERE status = 'paid'` after rows leave, enter, are deleted and change
  key, and does not without the option.

### D28. Poison changes

- **The problem.** A change a sink can never accept (a document Meilisearch
  rejects, a value that does not fit a ClickHouse column, a message above
  NATS's max_payload) is retried forever, and every later change waits
  behind it.
- **`on_error=skip`** (default `retry`, the behaviour of D12). After a batch
  failed `MaxAttempts` times (5) the runner delivers it one change at a
  time. A change that still fails is passed over only if the sink accepts
  another one: first the canary, the last change it accepted (delivering
  it again is harmless, delivery is at least once), else the next change.
  If nothing is accepted the sink is taken to be down and the change is
  retried with backoff: an outage never skips anything.
- **Recorded through Raft** (`SinkSkip`: sink, log index, change id, error,
  time), the same on every node: a total per sink and the last 100. The
  change itself stays in the log, so `curntvalv subscribe --from INDEX`
  shows it. `curntvalv sink skipped NAME` (`AdminService.SinkSkips`) lists
  them; `status` has a SKIPPED column, `/metrics`
  `curntvalv_sink_skipped_total`.
- **Snapshots.** Skips need snapshot format v8, which v0.6 nodes cannot
  read; a snapshot is v8 only while a sink has skipped something (D29).
- **Testing.** Runner tests with a sink that refuses some rows (in the
  middle and as the very last change), one that is down (nothing skipped,
  delivery resumes) and the default (a poison change blocks); end to end a
  webhook refusing one row with a strict and a lenient sink.

### D29. Rolling upgrades

- **Snapshots in the oldest format that holds their state.** A node of the
  new release may send its snapshot to a node of the old one during a
  rolling upgrade. The state machine therefore writes the lowest version
  that represents the state: v8 only with recorded skips (D28), v7
  otherwise; Raft adds its configuration envelope only after a membership
  change (D18). New state needs a new section and the old version as long
  as the section is empty.
- **New log entries are another matter**: an old node stops on a command it
  does not know (deliberately, rather than diverge), so features that
  write new commands are used only once every node runs the new release;
  CHANGELOG says which ones.
- **`make test-upgrade`** builds the previous release from its tag
  (`UPGRADE_FROM`, default v0.7.0) and runs TestRollingUpgrade: a cluster
  of the old release with Postgres and a webhook sink is upgraded one node
  at a time while rows are inserted; one old node is kept down until the
  log is cut past it and must install a snapshot from a new leader. Every
  row must arrive and every node apply the same log. With snapshots always
  in the newest format the test fails. CI runs it as its own job.

### D30. Whole transactions

- **The problem.** A Postgres transaction is stored in several batches
  when it is large (1000 changes or about 1 MB each, D3), and a runner cuts
  what it reads into deliveries of up to 500 changes. A receiver applying
  changes to its own database sees half of a transaction and has no way
  to know where one ends.
- **`transactions=true`** on a sink. The log reader reports, for every
  transaction batch it read, the commit LSN and whether the read covered
  the transaction's first batch (first sequence 0) and its final batch
  (`Final`). The runner holds the changes of a transaction whose final
  batch it has not read and delivers transactions only whole: several
  small ones may share a delivery (up to 500 changes), a large one goes
  alone, however large. Each change carries `tx_changes`: how many changes
  of its transaction this sink receives, after `table`, `op`, `where`; all
  of them are in the same delivery.
- **Offsets.** While a transaction is held the stored offset stays before
  its first batch, so a restart or a new leader reads it whole again.
- **Limits.** A transaction growing past `max_tx` changes for the sink
  (default 100000) is delivered in parts, as without the option, with a
  warning. So are the parts of a transaction whose start or end this
  runner did not read: one stored in part before a Postgres failover (D7)
  and never finished, or the rest of one already split. Their changes have
  no `tx_changes`: receivers must treat a change without it as on its own.
- **Atomic sinks.** A webhook receives one request per delivery, so it can
  apply a transaction in one database transaction. Kafka uses a
  transactional producer (`transactional_id`, default `curntvalv-NAME`):
  each delivery is one Kafka transaction, consumers with
  `isolation.level=read_committed` see all of it or nothing, and a new
  leader's producer fences the old one and aborts its open transaction.
  The id must be unique per producer, so two curntValv clusters writing to
  one Kafka need different ones. The other sinks get the grouping and
  `tx_changes` but write as before (NATS, Redis, Meilisearch and
  ClickHouse have no transaction over several writes that fits).
- **`on_error=skip`** (D28) then passes over whole transactions: the unit
  delivered one at a time is a transaction, and a refused one is recorded
  once, by its first change.
- **Compatibility.** Nothing new is stored: the option is a runner setting,
  so mixed-version clusters are fine. Without it nothing changes.
- **Testing.** Runner tests with transactions cut across reads and
  delivery batches, filtered tables, an open transaction across a restart
  (the offset must not pass it), one past `max_tx`, one never finished and
  one resumed, and a poison transaction; Kafka against a broker (commit,
  fencing, read_committed); end to end a 2500-row Postgres transaction
  reaching a webhook in one request.

## 4. Data model

```
Event {
  key        (commit_lsn, seq)       // idempotency key
  table      "public.orders"
  op         insert | update | delete | truncate | source_reset (D7)
             | backfill (D17)
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
| Leader killed during a backfill | New leader continues from the stored key; every row backfilled once (D17) |
| Membership changes under faults | One configuration at a time; Figure 3 invariants hold; no lost change (D18) |

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

- Entry encoding: the Raft entry header is hand-written binary (D9),
  node-to-node messages are protobuf (D10). The encoding of `TxBatch`
  payloads is decided in M4.
- In-memory offset index in `wal` costs 8 bytes per entry; switch to a sparse
  index if logs get very large.
- Delivery throughput: sinks read 100 entries at a time and small
  transactions are one entry each. Larger reads or merging small
  transactions into one entry may follow if needed.
