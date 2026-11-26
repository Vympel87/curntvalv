# Roadmap

Each milestone ends with something that can be run and shown.

Done: M0–M7 (v0.1.0), v0.2.0, v0.3.0, v0.4.0.

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

## v0.5 (in progress)

- Filter expressions, renames and masks per sink (design D23)
- ClickHouse mirror tables (design D24)

## Next

- Kafka tombstones for deletes and key changes (design D20)
- Hashing of column values with a secret key; deliver an update that
  leaves a `where` filter as a delete (needs REPLICA IDENTITY FULL)
