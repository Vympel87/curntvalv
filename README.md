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
| Postgres source (exactly-once into the log, tested with leader kills) | ✅ implemented |
| Sinks (ClickHouse, webhook) with stored offsets, subscribe API | ✅ implemented |
| Retention, replay policies, chaos test | ⏳ next |
| Dashboard, metrics | planned |

## Try it: a 3-node cluster with Postgres and ClickHouse

With Docker (starts Postgres with a demo `orders` table, ClickHouse, and
three nodes that deliver to it):

```bash
docker compose up -d --build
make build
bin/curntvalv status                 # one leader, two followers, source and sink
docker compose exec postgres psql -U postgres \
  -c "INSERT INTO orders (item, qty) VALUES ('apple', 3)"
bin/curntvalv subscribe --table orders   # the change as JSON; keeps streaming (Ctrl-C)
docker compose exec clickhouse clickhouse-client --password curntvalv \
  -q "SELECT op, after FROM curntvalv_events FINAL"
docker compose kill node1            # kill a node (try the leader)
docker compose exec postgres psql -U postgres \
  -c "UPDATE orders SET qty = 5 WHERE item = 'apple'"
bin/curntvalv subscribe --table orders   # still captured and delivered with 2 of 3 nodes
docker compose start node1           # it recovers from its volume
docker compose down -v               # remove everything, data included
```

`bin/curntvalv log` prints the raw log; `subscribe` filters by table and
resumes with `--from` (the last printed `index` + 1).

Against your own Postgres (14 or newer):

```sql
-- postgresql.conf: wal_level = logical (needs a restart)
CREATE PUBLICATION curntvalv FOR TABLE orders, customers;
```

```bash
export CURNTVALV_PG_URL='postgres://user:pass@host:5432/db'
bin/curntvalv server --id 1 --peers $P --data-dir data/1   # on each node
```

The leader creates the replication slot `curntvalv` on first start. The
user needs the `REPLICATION` attribute (or superuser) and must be able to
terminate the slot's previous reader.

## Sinks

Add `--sink` (repeatable) to every node, with the same values everywhere:

```bash
--sink 'name=ch,type=clickhouse,url=http://user:pass@clickhouse:8123'
--sink 'name=hook,type=webhook,url=https://example.com/cdc,table=orders'
```

| Key | Meaning |
|---|---|
| `name` | Required. Lower case letters, digits, `_`, `-`; the offset is stored under it |
| `type` | `clickhouse` or `webhook` |
| `url` | ClickHouse HTTP interface, or the webhook URL |
| `table` | Only these tables (`orders` or `public.orders`); repeat for more. Default: all |
| `start` | `earliest` (default: the whole retained history) or `latest`, for a new sink |
| `database`, `into` | ClickHouse database (default `default`) and table (default `curntvalv_events`) |

Delivery is at-least-once: after a leader change some changes can arrive
again. Every change has a stable `id` (`"<commit LSN>:<seq>"`). ClickHouse
gets a `ReplacingMergeTree` changelog table that drops copies on merge (use
`FINAL` in queries); webhook receivers should de-duplicate on `id`. The
webhook body is `{"sink": "hook", "events": [...]}`; any 2xx is success,
anything else is retried with backoff. `curntvalv status` shows each sink's
offset, lag and last error.

Without Docker, start three processes:

```bash
make build
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
bin/curntvalv server --id 1 --peers $P --data-dir data/1 &
bin/curntvalv server --id 2 --peers $P --data-dir data/2 &
bin/curntvalv server --id 3 --peers $P --data-dir data/3 &
bin/curntvalv status
bin/curntvalv kv put hello world     # metadata KV, works without Postgres
```

The key-value store holds cluster metadata; it is how M3 shows that the
replicated state machine works. It is not meant for application data. There
is no TLS yet, so run it on a trusted network only.

## Development

```bash
make test    # go vet + go test -race
make build   # builds ./bin/curntvalv
make test-pg # the same, plus the Postgres tests (starts Postgres in Docker)
make test-ch # the same, plus the ClickHouse tests (starts ClickHouse in Docker)
make test-ext # with both
make proto   # regenerates internal/pb (needs buf, see docs/DESIGN.md D10)
```

Requires Go 1.25+.

## License

MIT
