# Roadmap

Each milestone ends with something that can be run and shown.

Done: M0–M7 (v0.1.0), v0.2.0 to v0.10.0, v1.0.0.

| # | Milestone | Done when |
|---|---|---|
| M0 | Project skeleton + design docs | Repo builds, CI runs tests |
| M1 | `wal`: segmented log storage | Unit tests pass incl. crash repair, truncate, compact |
| M2 | `raft` core: election, replication, persistence | In-memory 3/5-node cluster passes tests with dropped messages, partitions, restarts |
| M3 | Snapshots, compaction, gRPC transport, `curntvalv server` | Real 3-node cluster on localhost / docker compose; metadata KV works; kill a node and it recovers |
| M4 | Postgres source | Changes from Postgres appear in the log; kill the leader → no lost or duplicated transaction |
| M5 | Sinks + subscribe API | ClickHouse + webhook sinks with offsets; `curntvalv subscribe --table orders` |
| M6 | Retention, replay, chaos end-to-end test | New sink replays history; numbered-rows checker passes under fault injection |
| M7 | Dashboard, metrics, release | Embedded dashboard, Prometheus metrics, Docker image, demo GIF, **v0.1.0** |

## v0.2 (done)

- Public Go package `client` (subscribe with failover, TLS, token)
- `curntvalv dev` for local development; install with `go install` or
  `go get -tool`
- Raft PreVote
- TLS (mutual between nodes), token authentication, `curntvalv certs`

## v0.3 (done)

- Config file: `curntvalv server --config FILE`, `--check` (design D16)
- Postgres failover: identity and timeline checks, failover slots,
  `curntvalv source reset` (design D7)
- Backfill of existing rows: `curntvalv backfill`, `--backfill-initial`
  (design D17)
- Dynamic cluster membership: `curntvalv member add|remove|list`,
  `--join` (design D18)

## v0.4 (done)

- Sinks: NATS JetStream, Redis Streams, Meilisearch (design D19), Kafka
  (design D20)
- Filters and column projection per sink (design D21)
- Named subscriptions: positions kept in the cluster (design D22)

## v0.5 (done)

- Filter expressions, renames and masks per sink (design D23)
- ClickHouse mirror tables (design D24)
- Kafka tombstones (design D25)

## v0.6 (done)

- Hashed columns with a secret key (design D26)
- Updates leaving a filter delivered as deletes (design D27)

## v0.7 (done)

- Poison changes: `on_error=skip` (design D28)
- Rolling upgrade test from the previous release (design D29)

## v0.8 (done)

- Whole transactions: `transactions=true`, Kafka transactions (design D30)
- Transactions in `curntvalv subscribe` and the Go client (`tx_changes`,
  `Client.Transactions`)

## v0.9: schema changes (done)

- A `schema` event when a table's definition changes, from the Relation
  messages pgoutput already sends (design D31)
- ClickHouse mirror tables follow type changes (`MODIFY COLUMN`); added
  columns were followed since v0.5

## v0.10: archive and pacing (done)

- An S3-compatible archive sink: JSON Lines, gzip, one object per table and
  time window, standard library only (SigV4 by hand)
- A rate limit per sink (`max_rate`), so a backfill does not flood a
  search index or a webhook; `batch` and `linger` (designs D32, D33)

## v1.0: stable (done)

- Compatibility promise (design D34): enforced by baselines in `compat/`
  and data fixtures written by 1.0 (done)
- Upgrade tests from every supported 0.x release: rolling from 0.4 to
  0.10, stopped from 0.2.1 and 0.3 (done)
- Operations guide (sizing, backups, recovery, monitoring), benchmarks
  (docs/OPERATIONS.md, docs/BENCHMARKS.md, `make bench`) (done)
- Longer chaos runs in CI: nightly, 30 minutes (done)
- README: drop the early-development warning; a new demo GIF recorded from
  a script in the repo (`docs/demo.tape`), so it can be made again (done)

## After 1.0

- More sources than Postgres. Every database captures changes its own
  way, so each is a source of its own next to `internal/source/pg`, with
  positions, failover and backfill worked out for it: MySQL and MariaDB
  (binlog, GTIDs) first, then SQL Server (CDC tables) and others as asked.
  The log, sinks and subscribers stay as they are; LSN-based fields of the
  JSON form get a source-neutral counterpart.
