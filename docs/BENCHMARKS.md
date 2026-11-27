# Benchmarks

`make bench` runs both parts below: Go benchmarks of the hot paths, then
`TestBenchmark`, a 3-node cluster built from this tree with fsync on
(default tick and log level), reading from Postgres and delivering to a
local webhook. Run it on your hardware before sizing; `BENCH_ROWS`,
`BENCH_SINGLE` and `BENCH_OUT=FILE` change the run.

## Results

Two runs on one laptop (Apple M-series, darwin/arm64, 12 CPUs, Go 1.25,
SSD), 2026-11-27, before 1.0. Postgres 17 ran in Docker (a Linux VM),
the three nodes and the webhook on the host, all sharing the same disk
and CPUs. The range is the two runs.

| Workload | Measure | Result |
|---|---|---|
| 200000 rows in transactions of 1000 rows | Postgres commits | 146000–260000 rows/s |
| | ingest into the replicated log | 116000–159000 rows/s |
| | delivered to the webhook | 72000–99000 rows/s |
| | log bytes per row (59 bytes in Postgres) | 94 |
| 5000 one-row transactions from 8 connections | Postgres commits | 1100–1960 tx/s |
| | delivered to the webhook | 410–670 tx/s |
| One-row transactions at a steady 20/s | commit to webhook, p50 / p95 / p99 | 8–9 ms / 10–11 ms / 19 ms |

Go benchmarks (same machine, `go test -bench`):

| Benchmark | Result |
|---|---|
| WAL append, 1 entry of 512 bytes, fsync | 3.9 ms per append |
| WAL append, 64 entries, fsync | 3.9 ms per append (8 MB/s) |
| WAL append, 64 entries, no fsync | 26–55 µs per append |
| State machine: apply a batch of 1000 changes | 45 µs (about 22 million changes/s) |
| JSON form of a five-column change | 1.4 µs, 39 allocations |
| Shape (where, mask) over 500 changes | 0.2 ms (about 2.5 million changes/s) |

## Reading them

- **Large transactions are cheap.** The source cuts a transaction into
  batches of up to 1000 changes and the leader pipelines them, so one
  fsync covers many rows: ingest runs at the speed of decoding and
  replication, and delivery at the speed of the sink.
- **One-row transactions are bounded by fsync.** Each is its own log
  entry, stored on a majority with fsync before Postgres is told it is
  safe. On macOS Go's fsync is `F_FULLFSYNC`, about 4 ms here; on Linux
  with an NVMe drive it is usually well under a millisecond, so expect a
  multiple of these figures there. `--unsafe-no-fsync` removes the bound,
  and with it the guarantee (a crash can lose committed changes).
- **Latency** is about a Raft round trip, an fsync, and the sink's
  request. The leader delivers a change as soon as it is committed.
- **The log is larger than the table**: every change carries its
  position, its transaction and the row in Postgres text. Plan disk with
  the measured bytes per row of your tables (`curntvalv_log_bytes`).
- **Sinks set the pace of delivery.** The webhook here answers at once;
  a real target is slower, and `batch`, `linger` and `max_rate` tune it.

These are not a comparison with other tools, and one laptop running
every component is not a production setup: treat them as a lower bound
for throughput and an upper bound for latency on dedicated machines.
