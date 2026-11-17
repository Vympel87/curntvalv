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
| `internal/sink/...`    | Sink interface + ClickHouse, webhook, stdout |
| `internal/server`      | gRPC (node-to-node, subscribe, admin) + HTTP (dashboard, metrics) |
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
it) and never stored twice (LSN de-duplication).

Fencing: Postgres allows one active connection per slot. A new leader
terminates the old connection before it starts. The old leader cannot commit
anything anyway — its Raft term is stale.

### D4. Delivery is at-least-once, with idempotency keys (log → sink)

- Each sink has an offset stored in the state machine (through Raft).
- A sink runner reads entries after its offset, delivers them, then advances
  the offset. If it crashes between the two steps, some events are delivered
  again.
- Every event has a stable key: `(commit LSN, position in transaction)`.
  Sinks use it to make redelivery harmless (ClickHouse `ReplacingMergeTree`
  versioned by LSN, upsert by primary key, webhook `Idempotency-Key` header).

We say "no lost events, no visible duplicates for idempotent sinks". We do
not claim exactly-once in general.

### D5. Retention and compaction

- Snapshots contain **only metadata** (the state machine), never events.
- Retention is by age and/or size (`retention: 7d`).
- The log may be cut only up to the index of a snapshot that is older than the
  retention window. So a node that is far behind (or new) installs that
  snapshot and then replays the retained log — it ends up with the same
  history as everyone else.
- If a sink is further behind than retention, policy is per sink:
  `block` (keep the log, default) or `drop` (cut anyway, mark sink as broken).

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
- **Interfaces** (defined in `raft`): `LogStore` (Append, Read, LastIndex,
  TruncateAfter — four methods so `*wal.Log` fits without an adapter),
  `StableStore` (Load/Save of term + vote), `Transport` (Send), `Applier`
  (Apply). `FileStableStore` writes term + vote + CRC to a temp file, fsyncs
  and renames it.
- **Entry encoding.** `| term u64 LE | type u8 | data |`; the index is the
  position in the `LogStore`. Raft only knows `Normal` and `Noop` entries;
  `TxBatch` vs `Meta` (D1) is decided by the layer above, inside `data`.
- **Propose blocks until commit** and returns the index. It fails with
  `NotLeaderError` (carries the known leader) or `ErrLeadershipLost` (the
  entry may still commit later; D3's LSN de-duplication covers the retry).
- **Persistence order.** Term/vote are saved and entries appended (fsync)
  before any message that depends on them is sent.
- **Restart.** `commitIndex` and `lastApplied` are volatile (Figure 2), so a
  restarted node applies its log again from index 1. Snapshots (M3) will
  bound this.
- **Rejection hint.** A rejected AppendEntries reply carries the follower's
  last index; the leader sets `nextIndex = min(nextIndex-1, hint+1)`. This is
  still "decrement and retry" (§5.3), it only skips indexes the follower
  cannot have. No other optimizations: no PreVote, no CheckQuorum, no
  pipelining, no read index. Without PreVote a node rejoining after a
  partition bumps the term and forces a new election; that is expected.
- **Broken invariants are fatal.** Two leaders in one term, truncating below
  `commitIndex`, or a log term above the current term stop the node with an
  error (`errSafety`) instead of continuing.

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
once in the state machine and referenced by id. Entries use a compact binary
encoding; JSON is produced at the edges (webhook, CLI, dashboard).

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

## 6. Open questions

- Subscribe API: server-side named consumers (offset stored in curntValv) vs.
  client-managed offsets. Leaning: support both, start with client-managed.
- Initial table snapshot (backfill of rows that existed before the slot):
  needed for real use, planned after v0.1.
- Entry encoding: protobuf vs. hand-written binary. (The Raft entry header
  is hand-written, D9; the encoding of `data` and of node-to-node messages is
  decided with the gRPC transport in M3.)
- In-memory offset index in `wal` costs 8 bytes per entry; switch to a sparse
  index if logs get very large.
