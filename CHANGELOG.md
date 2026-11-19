# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may
break compatibility.

## [Unreleased]

### Added

- Public Go package `client`: `Subscribe` as an iterator of changes, with
  failover between nodes. `curntvalv subscribe` uses it.
- `curntvalv dev`: one local node with the dashboard opened in a browser;
  checks the Postgres setup first and prints the SQL to fix it.
- Raft PreVote (thesis §9.6), on by default (`--pre-vote=false` turns it
  off): a node that was cut off no longer forces an election when it
  returns.
- `curntvalv version` reports the module version when installed with
  `go install` or run with `go tool`.

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

[0.1.0]: https://github.com/Vympel87/curntvalv/releases/tag/v0.1.0
