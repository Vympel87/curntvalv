# Public surface baselines

Each file lists one part of curntValv's public surface, one item per line
(design D34). Tests compare the code with these lines:

- a line that is missing from the code fails: within 1.x nothing listed
  here may be removed or change meaning;
- a line new in the code fails until it is recorded with `make baseline`,
  which only ever adds lines.

| File | What |
|---|---|
| `api.txt` | gRPC services, messages, fields and enums (`proto/curntvalv/v1`) |
| `json.txt` | The JSON form of a change (webhook, Kafka, NATS, Redis, S3, CLI) |
| `client.txt` | The exported API of the Go client (`github.com/Vympel87/curntvalv/client`) |
| `cli.txt` | Commands and their flags (server flags are also config file keys) |
| `sink.txt` | `--sink` options |
| `metrics.txt` | Prometheus metric names and types |
