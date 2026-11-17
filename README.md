# curntValv

**Postgres CDC with a built-in, replicated event log.**
Kafka-grade replay, single-binary simplicity.

> ⚠️ Early development. Nothing here is ready for production. See the
> [roadmap](docs/ROADMAP.md) for what exists today.

curntValv reads changes from Postgres (logical replication), stores them in an
ordered log replicated across a small cluster with Raft, and delivers them to
sinks or directly to your application.

```
Postgres ──► curntValv cluster (3 nodes, one binary) ──► ClickHouse
                    │                           ├──► Webhook
                    │                           └──► your app (subscribe)
                    └── replicated, replayable event log
```

## Why

To move changes out of Postgres you usually pick one of two paths:

- **Debezium + Kafka** — replay, retention, many consumers. Also Kafka, Kafka
  Connect and a JVM to operate.
- **A lightweight CDC tool** — easy to run, but often only a pipe. When you
  need replay or a durable buffer, you end up adding a broker anyway.

curntValv aims for the first set of features with the operating cost of the second.

## What it will do (v0.1 target)

- **Replay** — add a new sink and it receives history from the oldest retained
  change. No re-reading of tables.
- **Survive node loss** — kill the leader; another node takes over in seconds.
  No lost change, no duplicate stored.
- **Protect Postgres** — a slow or broken sink never makes the replication
  slot grow; the curntValv log absorbs the lag.
- **Subscribe directly** — applications can stream changes over gRPC without
  another broker.

## What it is not

- Not a general-purpose message broker. Use NATS or Kafka for that
  (they can be curntValv sinks).
- Postgres is the only source.

## Status

| Part | State |
|---|---|
| Design | [docs/DESIGN.md](docs/DESIGN.md) |
| Log storage (`internal/wal`) | ✅ implemented, tested |
| Raft: election, replication, snapshots, compaction, linearizable reads | ✅ implemented, fault-tested |
| gRPC transport, `curntvalv server`, metadata KV | ✅ implemented |
| Postgres source | ⏳ next |
| Sinks, subscribe, dashboard | planned |

## Try it: a 3-node cluster

With Docker:

```bash
docker compose up -d --build
make build
bin/curntvalv status                 # one leader, two followers
bin/curntvalv kv put hello world
docker compose kill node1            # kill a node (try the leader)
bin/curntvalv kv get hello           # still works with 2 of 3 nodes
docker compose start node1           # it recovers from its volume
docker compose down -v               # remove the cluster and its data
```

Without Docker, start three processes:

```bash
make build
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
bin/curntvalv server --id 1 --peers $P --data-dir data/1 &
bin/curntvalv server --id 2 --peers $P --data-dir data/2 &
bin/curntvalv server --id 3 --peers $P --data-dir data/3 &
bin/curntvalv status
```

The key-value store holds cluster metadata; it is how M3 shows that the
replicated state machine works. It is not meant for application data. There
is no TLS yet, so run it on a trusted network only.

## Development

```bash
make test    # go vet + go test -race
make build   # builds ./bin/curntvalv
make proto   # regenerates internal/pb (needs buf, see docs/DESIGN.md D10)
```

Requires Go 1.24+.

## License

MIT
