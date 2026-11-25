# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may
break compatibility.

## [Unreleased]

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

[0.2.1]: https://github.com/Vympel87/curntvalv/releases/tag/v0.2.1
[0.2.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.2.0
[0.1.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.1.0
