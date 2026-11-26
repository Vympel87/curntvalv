# Roadmap

Each milestone ends with something that can be run and shown.

Done: M0–M7 (v0.1.0), v0.2.0, v0.3.0, v0.4.0, v0.5.0, v0.6.0, v0.7.0.

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

## v0.10: archive and pacing

- An S3-compatible archive sink: JSON Lines, gzip, one object per table and
  time window, standard library only (SigV4 by hand)
- A rate limit per sink (`max_rate`), so a backfill does not flood a
  search index or a webhook

## v1.0: stable

- Compatibility promise: log entries, snapshots, the JSON form and the gRPC
  API change only compatibly within 1.x
- Rolling upgrade test from every 0.x release that is still supported
- Operations guide (sizing, backups, recovery, monitoring), benchmarks
- Longer chaos runs in CI
