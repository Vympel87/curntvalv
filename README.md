# curntValv

**Postgres CDC with a built-in, replicated event log.**
Kafka-grade replay, single-binary simplicity.

> ⚠️ Early development. Nothing here is ready for production. See the
> [roadmap](docs/ROADMAP.md) for what exists today.

curntValv reads changes from Postgres (logical replication), stores them in an
ordered log replicated across a small cluster with Raft, and delivers them to
sinks or directly to your application.

![Demo: a 3-node cluster keeps capturing changes after its leader is killed](docs/demo.gif)

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

## What it does

- **Replay** — add a new sink and it receives history from the oldest retained
  change. No re-reading of tables.
- **Survive node loss** — kill the leader; another node takes over in seconds.
  No lost change, no duplicate stored.
- **Protect Postgres** — a slow or broken sink never makes the replication
  slot grow; the curntValv log absorbs the lag.
- **Subscribe directly** — applications can stream changes over gRPC without
  another broker.
- **Backfill** — copy the rows a table already has into the log, while
  changes keep flowing, without locking or a long transaction.

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
| Retention, sink policies, replay (`sink reset`), chaos test | ✅ implemented |
| Dashboard, Prometheus metrics, health checks, live change feed | ✅ implemented |
| Postgres failover checks, `source reset`, config file | ✅ implemented (v0.3) |
| Backfill of existing rows | ✅ implemented (v0.3) |
| Adding and removing nodes at runtime | ✅ implemented (v0.3) |

## Install

Each [release](https://github.com/Vympel87/curntvalv/releases) has binaries
for Linux and macOS (amd64, arm64) with a `SHA256SUMS` file, and a
multi-arch image:

```bash
docker pull ghcr.io/vympel87/curntvalv:latest
```

Or with Go 1.25+:

```bash
go install github.com/Vympel87/curntvalv/cmd/curntvalv@latest
```

## Local development: `curntvalv dev`

One node on your machine, reading your development database, with the
dashboard opened in the browser:

```bash
curntvalv dev --pg-url postgres://user:pass@localhost:5432/app   # or set DATABASE_URL
```

It keeps its log in `./.curntvalv/` (add it to `.gitignore`), serves gRPC
on `127.0.0.1:7001` (so `curntvalv status`, `curntvalv subscribe` and the
Go client work without flags) and the dashboard on `127.0.0.1:8080`.
Postgres needs `wal_level = logical` and a publication; `dev` checks both
first and prints the SQL if something is missing. `--no-open` skips the
browser; `--sink` works as for `server`.

In a Go project you can pin it as a tool instead of installing it:

```bash
go get -tool github.com/Vympel87/curntvalv/cmd/curntvalv
go tool curntvalv dev
```

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

While it runs, the dashboard of each node is at http://127.0.0.1:8081,
:8082 and :8083 (see [Dashboard and metrics](#dashboard-and-metrics)).

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

Without Docker, start three processes:

```bash
make build
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
bin/curntvalv server --id 1 --peers $P --data-dir data/1 --http-listen 127.0.0.1:8081 &
bin/curntvalv server --id 2 --peers $P --data-dir data/2 --http-listen 127.0.0.1:8082 &
bin/curntvalv server --id 3 --peers $P --data-dir data/3 --http-listen 127.0.0.1:8083 &
bin/curntvalv status
bin/curntvalv kv put hello world     # metadata KV, works without Postgres
```

The key-value store holds cluster metadata; it is how M3 shows that the
replicated state machine works. It is not meant for application data.
Without TLS and a token (see [Security](#security)) run it on a trusted
network only.

## Configuration

Every server flag can also come from a file of `KEY = VALUE` lines, the key
being the flag name without `--`:

```ini
# /etc/curntvalv/node1.conf
id        = 1
peers     = 1=node1:7001,2=node2:7001,3=node3:7001
data-dir  = /var/lib/curntvalv
pg-url    = "postgres://cdc@db:5432/app"
retention-age = 7d
sink = name=ch,type=clickhouse,url=http://clickhouse:8123,table=orders
sink = name=hook,type=webhook,url=https://example.com/hook   # sink may repeat
```

```bash
curntvalv server --config /etc/curntvalv/node1.conf --check   # validate, then exit
curntvalv server --config /etc/curntvalv/node1.conf           # or CURNTVALV_CONFIG=...
```

Flags beat environment variables, which beat the file; a `--sink` flag
replaces the file's sinks. Unknown or repeated keys are errors with the
line number. Keep passwords out of a file others can read (the node warns),
and the token in `auth-token-file`.

## Use it from Go

```bash
go get github.com/Vympel87/curntvalv/client
```

```go
c, err := client.New([]string{"node1:7000", "node2:7000", "node3:7000"})
if err != nil {
	log.Fatal(err)
}
defer c.Close()
for ch, err := range c.Subscribe(ctx, client.Options{Tables: []string{"orders"}}) {
	if err != nil {
		log.Fatal(err) // client.ErrCompacted: start again From 0
	}
	fmt.Println(ch.Op, ch.Table, ch.After) // after: map of column -> *string (nil = NULL)
}
```

`Subscribe` moves to another node when one fails, so changes can arrive
again around a failure: de-duplicate on `ch.ID`. To resume after a restart,
subscribe with `From` set to the `Index` of the last change you fully
processed, or let the cluster keep the position with a named
subscription:

```go
for ch, err := range c.Subscribe(ctx, client.Options{Name: "billing", Tables: []string{"orders"}}) {
	// the position is stored about once a second and when the loop ends;
	// the next run continues there (the change in progress may come again)
}
```

```bash
curntvalv subscribe --name billing --table orders   # continues where the last run stopped
curntvalv subscription list                          # offset, lag, policy
curntvalv subscription reset billing --to earliest   # replay
curntvalv subscription delete billing
```

A named subscription holds back log retention until it has read the
changes (`--policy drop` / `Options.Drop` does not); delete the ones you no
longer use. Use one consumer per name.

## Sinks

Add `--sink` (repeatable) to every node, with the same values everywhere:

```bash
--sink 'name=ch,type=clickhouse,url=http://user:pass@clickhouse:8123'
--sink 'name=hook,type=webhook,url=https://example.com/cdc,table=orders'
--sink 'name=queue,type=nats,url=nats://nats:4222'                 # JetStream
--sink 'name=log,type=redis,url=redis://redis:6379,stream=cdc:{table}'
--sink 'name=search,type=meilisearch,url=http://:MASTER_KEY@meili:7700'
--sink 'name=bus,type=kafka,url=kafka://k1:9092,url=kafka://k2:9092'
```

| Key | Meaning |
|---|---|
| `name` | Required. Lower case letters, digits, `_`, `-`; the offset is stored under it |
| `type` | `clickhouse`, `webhook`, `nats`, `redis`, `meilisearch` or `kafka` |
| `url` | ClickHouse HTTP interface, webhook URL, `nats://[USER:PASS@\|TOKEN@]HOST:PORT` (`tls://` for TLS), `redis://[USER:PASS@]HOST:PORT[/DB]` (`rediss://`), `http[s]://[:API_KEY@]HOST:PORT` for Meilisearch, or `kafka://[USER:PASS@]HOST:PORT` (`kafka+tls://`; repeat `url` for more seed brokers) |
| `table` | Only these tables (`orders` or `public.orders`); repeat for more. Default: all |
| `start` | `earliest` (default: the whole retained history) or `latest`, for a new sink |
| `policy` | `block` (default): the log keeps everything the sink has not delivered. `drop`: retention cuts anyway; a sink left behind stops (`failed`) until it is reset |
| `database`, `into` | ClickHouse database (default `default`) and table (default `curntvalv_events`) |
| `subject`, `stream` | NATS subject prefix (default `curntvalv`) and JetStream stream (default `curntvalv`, created if missing, capturing `<subject>.>`) |
| `stream`, `maxlen` | Redis stream key (default `curntvalv`; `{table}` gives one stream per table) and approximate trim length (default: keep all) |
| `index` | Meilisearch index uid (default `{namespace}_{table}`) |
| `op` | Only these operations, repeatable: `insert`, `update`, `delete`, `truncate`, `backfill` |
| `where` | Only rows where `COLUMN=VALUE` or `COLUMN!=VALUE` (Postgres text form; the new row, the old one for deletes); repeat for AND |
| `columns`, `drop` | Keep only these columns, or remove these (`columns=id+note`, `drop=password+ssn`); key columns always stay |
| `topic`, `sasl` | Kafka topic (default `curntvalv.{table}`) and SASL mechanism for the URL's credentials: `plain` (default), `scram-sha-256`, `scram-sha-512` |

For example, paid orders only, without personal data, to a search index:

```bash
--sink 'name=search,type=meilisearch,url=http://:KEY@meili:7700,table=orders,where=status=paid,drop=email+phone'
```

An update that moves a row out of a `where` filter is not delivered, so a
mirror keeps the last matching version. Source resets pass every filter.

Delivery is at-least-once: after a leader change some changes can arrive
again. Every change has a stable `id` (`"<commit LSN>:<seq>"`). ClickHouse
gets a `ReplacingMergeTree` changelog table that drops copies on merge (use
`FINAL` in queries); webhook receivers should de-duplicate on `id`. The
webhook body is `{"sink": "hook", "events": [...]}`; any 2xx is success,
anything else is retried with backoff. `curntvalv status` shows each sink's
offset, lag and last error.

The other sinks:

- **NATS JetStream**: one message per change on
  `<subject>.<schema>.<table>` (`<subject>.source_reset` for a reset), the
  change as JSON, with `Nats-Msg-Id` = its `id`. JetStream drops a copy
  within its duplicate window (2 minutes by default), so redeliveries after
  a leader change are stored once.
- **Redis Streams**: `XADD` with the fields `id`, `index`, `table`, `op` and
  `json`. The entry ID is `<log index>-<seq>`, so a redelivered change is
  refused by Redis and each change is in the stream exactly once. Give the
  sink a stream of its own.
- **Meilisearch**: a search mirror of each table, one document per row with
  the key field `curntvalv_key`; inserts, updates and backfilled rows are
  upserts, deletes and truncates remove documents. Numbers, booleans and
  JSON keep their type. The sink waits until Meilisearch has applied each
  batch.
- **Kafka**: one record per change, the JSON as value, the row's key
  (`{"id":"5"}`) as record key so a row's changes stay in order, headers
  `curntvalv-id` and `curntvalv-op`. Idempotent producer with `acks=all`;
  after a leader change some records can repeat, so de-duplicate on
  `curntvalv-id`. No tombstones for deletes yet.

## Retention and replay

By default the log is kept forever. Limit it per node:

```bash
--retention-age 7d          # remove log segments older than this
--retention-bytes 10GB      # remove the oldest segments while the log is larger
--wal-segment-bytes 64MB    # retention removes whole segments (default 64MB)
```

The log is never cut past what a `block` sink has delivered; `curntvalv
status` shows the retention line and which sink, if any, holds it back.
Subscribe clients keep their own position and do not hold the log: one that
falls behind the oldest entry gets `OUT_OF_RANGE` and starts again from 0.

Replay a sink, or recover one that a `drop` policy left behind:

```bash
curntvalv sink reset hook --to earliest   # everything still retained
curntvalv sink reset hook --to latest     # skip to now
curntvalv sink reset hook --to 1200       # continue after log index 1200
```

A sink added later starts with the oldest retained entry (or with
`start=latest`).

## Backfill

Logical replication only carries changes made after the slot existed. To
copy the rows a table already has (Postgres 14 or later, a table with a
primary key, in the publication):

```bash
curntvalv backfill start orders sales.customers
curntvalv status                  # BACKFILL  STATE  ROWS  LAST KEY ...
curntvalv backfill cancel orders  # stop; start again later from the beginning
```

or start the server with `--backfill-initial` to backfill every published
table once, when a new cluster creates its slot.

Rows arrive at every sink and subscriber as `"op":"backfill"` with the row
in `after` (`client.OpBackfill` in Go); treat them as upserts. They are read
in primary key order, 1000 at a time, interleaved with the live stream:
there is no lock and no long transaction, a row changed meanwhile arrives
in its newer version, and the order in the log stays correct for every row
(design D17). A backfill survives leader changes and continues from the
last key stored; each row is stored once. Rows inserted ahead of the
backfill while it runs can arrive both as an insert and as a backfill row.

## Adding and removing nodes

Start the new node with an empty data directory and `--join`, giving the
current members in `--peers` and its own address in `--listen`, then add
it from anywhere:

```bash
bin/curntvalv server --id 4 --join --listen 10.0.0.4:7001 \
  --peers 1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 --data-dir data/4
bin/curntvalv member add 4=10.0.0.4:7001   # learner, copies the log, then votes
bin/curntvalv member list
bin/curntvalv member remove 2              # then stop node 2 and delete its data
```

A new node does not count for a majority until it has caught up, so adding
one never stalls the cluster. Removing the leader makes it step down. The
membership is stored in the log, so `--peers` matters only for a cluster's
first start (design D18). Change one node at a time; keep an odd number of
voters.

## Postgres failover

curntValv follows a Postgres failover when it can prove nothing was lost,
and otherwise stops ingest and says why (design D7). On Postgres 17:

```ini
# primary
synchronized_standby_slots = 'standby1'   # the standbys' physical slots
# standbys
sync_replication_slots = on
hot_standby_feedback = on
primary_slot_name = 'standby1'            # and dbname=... in primary_conninfo
```

curntValv creates its slot as a failover slot (and converts an existing
one), so the standby keeps a copy and the primary never sends a change the
standby does not have. Point `--pg-url` at a virtual IP or DNS name that
follows the primary, or list the hosts with `target_session_attrs=primary`.
While it reaches a standby, ingest waits.

Without `synchronized_standby_slots` (or with asynchronous Patroni) the
new primary may lack changes curntValv already stored. Ingest then stops
with an error such as

```
SOURCE postgres: failed (pg: the stored position is not in this postgres server's history;
  refusing to continue (changes may be lost): timeline 1 ended at 0/5000A28 on this server,
  but the cluster stored up to 0/5003F10; changes in between were lost in the failover)
```

The same happens when the slot is missing or ahead of what was stored, or
when `--pg-url` reaches another Postgres cluster. To accept the gap and go
on from the slot (created if missing):

```bash
curntvalv source reset --to-slot
```

Sinks and subscribers then receive one event with `"op":"source_reset"`
and a `reset` object (old and new LSN, system id, timeline, reason), even
with a table filter: changes between the two positions may be missing or
repeated. In Go it is `client.OpSourceReset`.

## Dashboard and metrics

Every node serves HTTP on `--http-listen` (default `127.0.0.1:8080`; an
empty value turns it off). It is open unless a token is set (see
[Security](#security)).

| Path | |
|---|---|
| `/` | Dashboard: nodes, source, retention, sinks and a live change feed |
| `/healthz` | 200 while the node runs |
| `/readyz` | 200 when the node also knows a leader |
| `/metrics` | Prometheus metrics (`curntvalv_*`, `go_*`) |
| `/api/status` | The whole cluster as JSON, from any node |
| `/api/events` | Changes as server-sent events; `?table=orders`, `&from=INDEX` |

Scrape every node; sinks and the source report from the leader:

```yaml
scrape_configs:
  - job_name: curntvalv
    static_configs:
      - targets: ["node1:8080", "node2:8080", "node3:8080"]
```

Useful series: `curntvalv_sink_lag_entries`, `curntvalv_sink_state`,
`curntvalv_raft_leader_known`, `curntvalv_log_bytes`,
`curntvalv_raft_became_leader_total`, `curntvalv_backfill_rows`.

```bash
curl -N 'http://127.0.0.1:8081/api/events?table=orders'   # follow changes
```

## Security

TLS and a token are off by default. To try them:

```bash
curntvalv certs --out certs node1,10.0.0.1 node2,10.0.0.2 node3,10.0.0.3
openssl rand -hex 32 > token          # the same file on every node
curntvalv server ... --tls-cert certs/node1.crt --tls-key certs/node1.key \
  --tls-ca certs/ca.crt --auth-token-file token
export CURNTVALV_TLS_CA=certs/ca.crt CURNTVALV_TOKEN=$(cat token)
curntvalv status
```

- Nodes talk mutual TLS to each other; clients only need `ca.crt`. Each
  node's certificate must name the host names and IPs others use for it.
- The token protects the admin and subscribe APIs, the dashboard and
  `/metrics` (browser: any user name, the token as password; Prometheus:
  `authorization` with a bearer token). `/healthz` and `/readyz` stay open.
- Go: `client.New(addrs, client.WithTLS(cfg), client.WithToken(tok))`.
- `curntvalv certs` is for getting started; use your own CA in production.

## Development

```bash
make test    # go vet + go test -race
make build   # builds ./bin/curntvalv
make test-pg # the same, plus the Postgres tests (starts Postgres in Docker)
make test-ch # the same, plus the ClickHouse tests (starts ClickHouse in Docker)
make test-sinks # the same, plus NATS, Redis, Meilisearch and Kafka (in Docker)
make test-ext # with all of them
make chaos   # random faults against a real cluster and Postgres (CHAOS_TIME=2m, CHAOS_SEED=N)
make proto   # regenerates internal/pb (needs buf, see docs/DESIGN.md D10)
```

Requires Go 1.25+.

## License

MIT
