# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may
break compatibility.

## [0.9.0]

### Added

- Schema changes (design D31): with `schema_changes=true` (webhook, Kafka,
  NATS sinks), `curntvalv subscribe --schema-changes` or
  `Options.SchemaChanges` in the Go client, a change with op `schema` and
  the table's new `columns` comes before the first change of a table under
  a definition new to the log (after `ALTER TABLE`). Derived from the log,
  nothing new is stored. `op=schema` selects them.
- ClickHouse mirror tables follow column type changes with
  `ALTER TABLE ... MODIFY COLUMN`; a changed key column stops the sink
  with an error instead of mirroring wrong values.

### Upgrading from 0.8

- Nothing new is stored, so nodes can be upgraded one at a time. A node
  before 0.9 does not start with `schema_changes` in a `--sink`: add it
  once every node runs 0.9. A subscriber asking an older node for schema
  changes gets none.

## [0.8.0]

### Added

- Whole transactions (design D30): `transactions=true` on a sink delivers
  Postgres transactions whole, never split across deliveries, each change
  with `tx_changes`. A webhook gets a transaction in one request; Kafka
  writes each delivery as one Kafka transaction (`transactional_id`,
  default `curntvalv-NAME`), for `read_committed` consumers. With
  `on_error=skip` a refused transaction is skipped whole. `max_tx` bounds
  the changes held in memory.
- Whole transactions for subscribers: `SubscribeRequest.transactions` and
  `ChangeEvent.tx_changes`; in the Go client `Options.Transactions`,
  `Change.TxChanges` and `Client.Transactions`, which yields one
  transaction at a time; `curntvalv subscribe --transactions`.

### Changed

- The JSON form of a change has `tx_changes` when the sink delivers whole
  transactions.
- `make sinks-up` starts Kafka with an internal listener for the broker
  itself, which Kafka transactions need; remove an older container with
  `make sinks-down`.
- `make test-upgrade` upgrades from v0.7.0.

### Fixed

- `curntvalv member remove` reported "not a member" when its first attempt
  removed the server but answered after the per-attempt timeout; it now
  reports the removal.

### Upgrading from 0.7

- Nothing new is stored, so nodes can be upgraded one at a time. A node
  before 0.8 ignores a subscriber's request for transactions: its changes
  then come without `tx_changes` (one at a time from
  `Client.Transactions`).

## [0.7.0]

### Added

- Poison changes (design D28): `on_error=skip` passes over a change a sink
  keeps refusing while it accepts others, instead of retrying it forever;
  an outage never skips anything. Skipped changes are recorded through
  Raft: `curntvalv sink skipped NAME`, a SKIPPED column in `status`,
  `curntvalv_sink_skipped_total`.
- `make test-upgrade` and a CI job: a rolling upgrade from the previous
  release with Postgres changes flowing, including an old node that
  installs a snapshot from a new leader (design D29).

### Changed

- `curntvalv status` shows a SKIPPED column for sinks.

### Upgrading from 0.6

- Snapshots stay at version 7 until a sink skips a change; use
  `on_error=skip` only once every node runs 0.7 (a 0.6 node stops on a
  skip record).

## [0.6.0]

### Added

- Hashed columns (design D26): `hash=A+B` with `hash_key_file=PATH`
  replaces values with their HMAC-SHA256 under a secret key, so they can
  still be joined but not guessed; key columns may be hashed too.
- `on_leave=delete` (design D27): with `where`, an update that moves a row
  out of the filter is delivered as a delete, so filtered mirrors drop it;
  deletes whose old row is not known pass.

### Changed

- The non-key columns of an old row sent with the default replica
  identity are unknown to `where`, not NULL. A delete is therefore skipped
  by a filter on such a column (before, `!=` passed every delete and `=`
  none); `on_leave=delete` delivers it.

### Upgrading from 0.5

- No change to snapshots or the log; 0.5 and 0.6 nodes can run together.
  Use `hash` and `on_leave` once all nodes run 0.6 (every node needs the
  same `--sink` flags), and give every node the same `hash_key_file`.
- A sink with a `where` filter on non-key columns no longer receives
  deletes of tables without REPLICA IDENTITY FULL, unless `on_leave=delete`
  (see Changed); add it to mirror sinks.

## [0.5.0]

### Added

- Filter expressions for sinks (design D23): `where=` takes comparisons
  (`= != < <= > >=`, `in`, `like`, `is null`) combined with `and`, `or`,
  `not` and parentheses; numeric columns compare as numbers. The first
  `COLUMN=VALUE` form keeps working.
- `mask=` replaces column values with `***`, `rename=` renames columns in
  what a sink receives.
- ClickHouse mirror tables (design D24): `mode=mirror` keeps one typed
  table per Postgres table with its current rows
  (`ReplacingMergeTree(_version, _deleted)` by primary key), handling
  deletes, key changes, truncates, unchanged TOASTed values and new
  columns.
- Kafka tombstones (design D25): `tombstones=true` follows each delete with
  a tombstone and sends one for the old key of a key change, for
  log-compacted topics. Off by default.

### Upgrading from 0.4

- No change to snapshots or the log; 0.4 and 0.5 nodes can run together.
  Every node needs the same `--sink` flags, so use the new sink options
  (`where` expressions, `mask`, `rename`, `mode=mirror`, `tombstones`)
  once all nodes run 0.5.

## [0.4.0]

### Added

- Sinks for NATS JetStream (`type=nats`: a message per change with
  `Nats-Msg-Id`, the stream is created if missing), Redis Streams
  (`type=redis`: `XADD` with entry IDs from the log, so each change is in the
  stream once) and Meilisearch (`type=meilisearch`: a search index mirroring
  each table). No new dependencies (design D19). `make test-sinks` runs
  their tests against real servers in Docker; CI runs them too.
- Kafka sink (`type=kafka`, design D20): a record per change keyed by the
  row's key, idempotent producer with `acks=all`, SASL PLAIN/SCRAM and TLS,
  several seed brokers. Adds the dependency franz-go v1.21.7.
- Filters and projections per sink (design D21): `op=` and
  `where=COL=VALUE` / `COL!=VALUE` keep only some changes, `columns=` and
  `drop=` remove columns (key columns stay). Skipped changes still move
  the sink's offset.
- Named subscriptions (design D22): `client.Options{Name: ...}` and
  `curntvalv subscribe --name` keep the position in the cluster and
  continue there; auto-commit or `Client.Commit`; created on first use,
  hold back retention unless `Drop`; `curntvalv subscription
  list|reset|delete`, shown in `status` and `/metrics`.

### Upgrading from 0.3

- Snapshots are now version 7, which v0.3 nodes cannot read: upgrade all
  nodes close together, and before the first named subscription (a v0.3
  node stops on a subscription command).
- Sink options are new; existing `--sink` flags keep working unchanged.

## [0.3.0]

### Added

- `curntvalv server --config FILE` (or `CURNTVALV_CONFIG`): read unset
  flags from a file of `KEY = VALUE` lines, KEY being a flag name. Flags
  beat environment variables, which beat the file.
- `curntvalv server --check`: validate the flags, the config file and the
  TLS files, then exit.
- Postgres failover (design D7): the cluster records the Postgres system
  identifier and timeline with its position, and every session checks them
  against the server, including its timeline history. A standby is waited
  for; another cluster, an old primary, or a failover to a standby that
  lacks stored changes stops ingest (`ErrSourceChanged`, `ErrDiverged`)
  instead of skipping the new primary's transactions as already stored.
- On Postgres 17 the slot is a failover slot (new slots, and existing ones
  are converted); the node hints at `synchronized_standby_slots`.
- `curntvalv source reset --to-slot` (admin RPC `ResetSource`): after a
  failover check stopped ingest, continue from the slot's position. It
  stores an `OP_SOURCE_RESET` event that sinks and subscribers receive
  whatever their table filter (`"op":"source_reset"`, `client.OpSourceReset`).
- `make test-failover`: real Postgres 17 failovers (primary, standby,
  promotion) in three scenarios; also a CI job.
- Backfill (design D17): `curntvalv backfill start TABLE...` copies the
  existing rows of tables into the log, in primary key order and in chunks
  between watermarks, while ingest continues; `curntvalv backfill cancel`;
  `--backfill-initial` backfills every published table when a new cluster
  starts. Rows arrive as `"op":"backfill"` (`client.OpBackfill`). Progress
  in `curntvalv status`, the dashboard and `/metrics`. Needs Postgres 14.
- Cluster membership changes (design D18): `curntvalv member add
  ID=HOST:PORT` (a node started with `--join`; learner first, voter once
  caught up), `curntvalv member remove ID`, `curntvalv member list`. One
  server at a time (Raft thesis §4), with the 2015 fix; the configuration
  lives in the log and in snapshots. Status and the dashboard show the
  members.

### Fixed

- The idle-position data loss fixed in 0.2.1 (a transaction committing
  exactly at the stored idle position was skipped).
- `--http-listen` help no longer says the HTTP port has no authentication.

### Upgrading from 0.2

- Snapshots are now version 6, which v0.2 nodes cannot read: upgrade all
  nodes close together, and before any `source reset` or `backfill start`
  (a v0.2 node cannot apply those entries), and before the first
  `member add` or `member remove` (a v0.2 node stops on a configuration
  entry).

## [0.2.1]

### Fixed

- A transaction could be lost when it stayed open across an idle period
  (no other WAL written for one status interval, 10s by default) and its
  commit record then landed exactly at the stored idle position: the
  source took it as already stored. Position-only batches now store
  commit LSN P-1 for WAL position P. Also affects 0.1.0; upgrading needs
  no other change.

## [0.2.0]

### Added

- Public Go package `client`: `client.New(addrs, opts...)` and
  `Subscribe` as an iterator of changes, with failover between nodes,
  `WithTLS` and `WithToken`. `curntvalv subscribe` uses it.
- `curntvalv dev`: one local node with the dashboard opened in a browser;
  checks the Postgres setup first and prints the SQL to fix it.
- TLS (mutual between nodes) and a token for the gRPC and HTTP APIs
  (`--tls-cert`, `--tls-key`, `--tls-ca`, `--auth-token-file`), CLI
  `--tls-ca` and `--token-file`, and `curntvalv certs` to create a CA and
  node certificates.
- Raft PreVote (thesis §9.6), on by default (`--pre-vote=false` turns it
  off): a node that was cut off no longer forces an election when it
  returns.
- `curntvalv version` reports the module version when installed with
  `go install` or run with `go tool`.

### Changed

- Node-to-node transport drops message types it does not know instead of
  closing the stream, so later versions can add messages.

### Upgrading from 0.1

- PreVote adds two Raft message types that 0.1 nodes reject. For a rolling
  upgrade start the new nodes with `--pre-vote=false`, and turn it on once
  every node runs 0.2 (elections keep working either way).

## [0.1.0]

First release. Early development: not ready for production.

### Added

- `curntvalv server`: one binary per node, a static cluster of 3 or 5 nodes
  replicated with Raft (written from scratch: election, replication,
  snapshots, log compaction, linearizable reads) over gRPC.
- Segmented on-disk log with checksums and crash repair.
- Postgres source (logical replication, pgoutput): runs on the leader,
  stores every transaction exactly once in the log across leader changes,
  fences the previous reader of the slot, and acknowledges Postgres only
  what is stored. Proposals are pipelined (hundreds of transactions a
  second on a laptop).
- Sinks with replicated offsets and retries: ClickHouse (changelog table,
  `ReplacingMergeTree`) and webhook. Delivery is at-least-once with a stable
  event `id`.
- Sink policies `block` and `drop`, and `curntvalv sink reset NAME --to
  earliest|latest|INDEX` for replay.
- Retention by age (`--retention-age`) and size (`--retention-bytes`),
  never past what a `block` sink has delivered.
- Subscribe API (gRPC server stream from any node) and `curntvalv
  subscribe`.
- CLI: `status`, `log`, `subscribe`, `sink reset`, `kv`.
- HTTP on `--http-listen` (default `127.0.0.1:8080`): embedded dashboard,
  Prometheus `/metrics`, `/healthz`, `/readyz`, `/api/status` (whole
  cluster) and `/api/events` (live change feed as server-sent events).
- Docker image `ghcr.io/vympel87/curntvalv` (linux/amd64, linux/arm64),
  binaries for Linux and macOS, docker compose demo.
- Tests: in-process Raft fault tests, end-to-end tests against Postgres and
  ClickHouse with leader kills, and a chaos checker (`make chaos`).

### Known limits

- No TLS or authentication, on gRPC or HTTP: trusted networks only.
- No Postgres failover handling yet (design D7), no initial table
  snapshot, static membership.

[0.7.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.7.0
[0.6.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.6.0
[0.5.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.5.0
[0.4.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.4.0
[0.3.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.3.0
[0.2.1]: https://github.com/Vympel87/curntvalv/releases/tag/v0.2.1
[0.2.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.2.0
[0.1.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.1.0
