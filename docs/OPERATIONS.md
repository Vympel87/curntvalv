# Operating curntValv

This guide is for people running curntValv in production: how to lay out
a cluster, size it, set up Postgres, watch it, change it, and recover it.
The README shows the features; [DESIGN.md](DESIGN.md) explains why things
work the way they do; [BENCHMARKS.md](BENCHMARKS.md) has measured numbers.

## Layout

- **Three nodes**, each in its own failure domain (host, rack or zone). A
  cluster keeps working while a majority is up: 3 nodes survive the loss
  of 1, 5 nodes the loss of 2. More than 5 only adds replication work.
- **One cluster per Postgres database** (one publication and one slot).
  The leader reads from Postgres and runs every sink; followers keep a
  copy of the log, serve subscribers and take over within a few seconds.
- **Ports per node**: `--listen` (gRPC: Raft between nodes, the CLI and
  subscribers; default `:7001` in the examples) and `--http-listen`
  (dashboard, `/metrics`, `/healthz`, `/readyz`; default `127.0.0.1:8080`,
  so set it to a reachable address for monitoring).
- **Every node gets the same `--sink` options and Postgres URL.** Only the
  leader uses them, but any node can become the leader.
- **Use TLS and a token** on any network you do not fully trust
  (`curntvalv certs`, `--tls-*`, `--auth-token-file`; README, Security).

A systemd unit per node:

```ini
# /etc/systemd/system/curntvalv.service
[Unit]
Description=curntValv node
After=network-online.target
Wants=network-online.target

[Service]
User=curntvalv
ExecStartPre=/usr/local/bin/curntvalv server --config /etc/curntvalv/node.conf --check
ExecStart=/usr/local/bin/curntvalv server --config /etc/curntvalv/node.conf
Environment=CURNTVALV_PG_URL=postgres://cdc:secret@db.internal:5432/app
Restart=always
RestartSec=2
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

`SIGTERM` stops a node cleanly. When it was the leader, the others elect a
new one within a few seconds; nothing committed is lost.

## Sizing

- **Disk.** Each node keeps the whole log until retention removes it:
  plan `--retention-bytes` (or the volume of `--retention-age`) plus one
  segment (`--wal-segment-bytes`, 64 MB) and room for snapshots, which are
  small (metadata only, not rows). BENCHMARKS.md has the log bytes per
  row for a typical table; measure yours with `curntvalv_log_bytes`. Use local SSDs: every committed entry is
  fsynced on a majority of nodes, so fsync latency bounds how many
  separate transactions per second the cluster can store (large
  transactions are batched and are not affected).
- **Memory.** The state machine holds metadata (schemas, positions,
  subscriptions), not rows; a node needs little beyond the Go runtime,
  plus what sinks buffer: up to `batch` changes per sink (500 by default,
  10000 for `s3`), and with `transactions=true` up to `max_tx` changes of
  one transaction (100000).
- **CPU.** The leader decodes, replicates and delivers; followers mostly
  write to disk. Watch the leader's CPU under your peak load.
- **Network.** The leader sends every entry to each follower: plan twice
  the ingest volume out of the leader for 3 nodes, four times for 5.

## Postgres

Postgres 14 or later.

```ini
# postgresql.conf
wal_level = logical            # needs a restart
max_replication_slots = 10     # one for curntValv, plus your others
max_wal_senders = 10
max_slot_wal_keep_size = 50GB  # see below
```

```sql
CREATE ROLE cdc WITH LOGIN REPLICATION PASSWORD '...';
GRANT SELECT ON orders, customers TO cdc;     -- for backfill
CREATE PUBLICATION curntvalv FOR TABLE orders, customers;
-- Old values of every column in updates and deletes, for where filters on
-- deletes, on_leave=delete and Kafka key changes:
ALTER TABLE orders REPLICA IDENTITY FULL;
```

The user must be able to terminate the slot's previous reader (it is
itself, after a leader change): the `REPLICATION` attribute is enough for
its own connections.

**The slot holds WAL while curntValv does not read it.** If the cluster is
down or stuck (for example ingest stopped after a failover check, waiting
for `curntvalv source reset`), Postgres keeps WAL from the slot's position
on and the disk fills. `max_slot_wal_keep_size` caps it: past the cap
Postgres invalidates the slot, curntValv stops with "slot lost", and
changes in between are gone (recover as in "Losing the slot" below).
Alert on slot lag (Monitoring) well before the cap.

For Postgres failover follow the README section "Postgres failover"
(`synchronized_standby_slots`, failover slots, Postgres 17).

## Monitoring

Scrape `/metrics` on every node. Sinks and the source report on the
leader, so aggregate across nodes. Suggested Prometheus rules:

```yaml
groups:
  - name: curntvalv
    rules:
      - alert: CurntvalvNoLeader
        expr: max(curntvalv_raft_leader_known) == 0
        for: 1m
      - alert: CurntvalvNodeDown
        expr: up{job="curntvalv"} == 0
        for: 2m
      - alert: CurntvalvLeaderFlapping
        expr: sum(increase(curntvalv_raft_became_leader_total[15m])) > 3
      - alert: CurntvalvSourceNotRunning
        expr: max(curntvalv_source_state{state=~"retrying|failed"}) == 1
        for: 5m
      - alert: CurntvalvSinkNotRunning
        expr: max by (sink) (curntvalv_sink_state{state=~"retrying|failed"}) == 1
        for: 5m
      - alert: CurntvalvSinkLag
        expr: max by (sink) (curntvalv_sink_lag_entries) > 10000
        for: 10m
      - alert: CurntvalvSinkSkipped
        expr: sum by (sink) (increase(curntvalv_sink_skipped_total[1h])) > 0
      - alert: CurntvalvRetentionBlocked
        expr: max by (sink) (curntvalv_retention_blocked) == 1
        for: 30m
      - alert: CurntvalvLogNearLimit
        expr: curntvalv_log_bytes > 0.9 * curntvalv_retention_max_bytes and curntvalv_retention_max_bytes > 0
      - alert: CurntvalvBackfillFailed
        expr: max by (table) (curntvalv_backfill_state{state="failed"}) == 1
```

Watch the slot in Postgres too (with postgres_exporter or a query):

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots WHERE slot_name = 'curntvalv';
```

`curntvalv status` shows the same from the command line: nodes, source,
retention, sinks (state, lag, last error) and backfills.

## Routine changes

- **Upgrade** one node at a time, followers first: stop it, replace the
  binary, start it, wait until `curntvalv status` shows it with the same
  applied index as the others, go on. Read "Upgrading from" in
  CHANGELOG.md first: some new options may be used only once every node
  runs the new release. Which releases upgrades are tested from is in
  DESIGN.md, D34.
- **Replace a node** (a failed disk, a new host): `curntvalv member remove
  ID`, then start the new node with an empty data directory and `--join`
  and `curntvalv member add ID=HOST:PORT` (README, "Adding and removing
  nodes"). One change at a time.
- **Add or change a sink**: the same `--sink` on every node, then restart
  the nodes one at a time (followers first). A new sink starts with the
  oldest retained entry, or `start=latest`. Replay one with `curntvalv
  sink reset NAME --to earliest|latest|INDEX`.
- **Copy existing rows** into the log: `curntvalv backfill start TABLE`;
  it runs in chunks next to the stream and survives leader changes.
- **Rotate the token or certificates**: give every node the new files
  and restart them one at a time; with a new CA, give the nodes a bundle
  of the old and new CA first.

## Failures and recovery

What a node loses and how it comes back:

| Failure | Effect | What to do |
|---|---|---|
| One node down (of 3) | Nothing: the others elect a leader in a few seconds if it led | Bring it back; it catches up from the log or a snapshot |
| A node's disk lost | As above | Replace the node (member remove, then add a new one) |
| A majority down | Nothing is committed: ingest and sinks wait, Postgres keeps WAL in the slot | Bring nodes back; no data is lost while a majority's disks survive |
| A majority's disks lost | The log on those nodes is gone | See "Starting over" |
| Postgres failover | Followed if safe, else ingest stops with a reason | README, "Postgres failover"; `curntvalv source reset --to-slot` accepts a gap |
| A sink target down | Its runner retries with backoff; the log keeps the changes (policy block) | Fix the target; delivery resumes where it stopped |
| A change a sink always refuses | The sink retries it forever (on_error=retry) | Fix the data or target, or use `on_error=skip`; `curntvalv sink skipped NAME` |
| Sink behind retention (policy drop) | The sink stops (`failed`) | `curntvalv sink reset NAME --to earliest` (changes in between are lost to it) |

**Backups.** The log is replicated, so a cluster does not need backups
to survive losing nodes; Postgres stays the source of truth. A copy of a
*stopped* node's data directory is a consistent backup of that node and
can be restored in its place. Never start two nodes from copies of the
same data directory.

**Starting over** after losing a majority's disks: stop every node,
start a new cluster with empty data directories and the same publication
and slot. Ingest continues from the slot's position; changes the old
cluster had stored but its sinks had not delivered yet are gone, since
Postgres no longer has them. Rebuild targets that hold state (mirrors,
search indexes) with `curntvalv backfill start` (with `sink reset` if
needed), and tell consumers of event streams that a gap is possible.

**Losing the slot** (invalidated by `max_slot_wal_keep_size`, dropped by
someone): ingest stops with "slot lost". `curntvalv source reset
--to-slot` creates a new slot and goes on from now; sinks and subscribers
get a `source_reset` event. Backfill the tables whose targets must be
complete.

## Troubleshooting

| `curntvalv status` shows | Meaning | Action |
|---|---|---|
| no leader | No majority of nodes reach each other | Check that the nodes run and `--peers` / `--listen` addresses are reachable both ways |
| source `retrying` | Postgres unreachable, or `--pg-url` reaches a standby | Check the URL and Postgres; ingest resumes by itself |
| source `failed` | A failover or slot check refused to continue (the error says why) | Decide, then `curntvalv source reset --to-slot` |
| sink `retrying` with an error | The target refuses or is down | Fix it; for one bad change consider `on_error=skip` |
| sink `failed`: behind compaction | A `drop` sink fell behind retention | `curntvalv sink reset NAME --to earliest` |
| retention blocked by a sink | A `block` sink holds the log | Fix the sink, or reset it, or make it `policy=drop` |
| backfill `failed` | The table could not be read (the error says why) | Fix (permissions, a primary key), then `backfill start` again |

Logs are structured (`--log-level debug` for more); every line names the
node and the component.
