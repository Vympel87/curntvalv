# Roadmap

Each milestone ends with something that can be run and shown.

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

## After v0.1

- Postgres failover handling (design D7)
- Initial table snapshot / backfill
- Dynamic cluster membership
- More sinks: Meilisearch, Redis, Kafka, NATS
- Filters and simple transforms per sink
