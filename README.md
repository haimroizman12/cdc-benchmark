# CDC Benchmark — Debezium vs Airbyte (Postgres → MSSQL)

Local, reproducible benchmark measuring replication latency of two CDC tools moving
changes from Postgres (source) to SQL Server (target). One containerized Python
harness generates a tunable change stream into Postgres, times every row against a
single host clock (no cross-DB skew), and renders a comparison table.

## Quick start

```bash
cp .env.example .env
make up                 # postgres + mssql
make selftest           # sanity check: harness writes straight to MSSQL (~0 latency)
```

## Run each tool

Debezium (streams the WAL — sub-second):

```bash
make debezium-up
make debezium-bench RATE=50 DURATION=20 MIX=100/0/0
make debezium-down
```

Airbyte (batch syncs; installs a local kind cluster via abctl — heavy, minutes):

```bash
make airbyte-up
make airbyte-bench RATE=20 DURATION=120 MIX=100/0/0
make airbyte-down
```

Then render the table from whatever runs are in `results/`:

```bash
make report
```

## Tunable load

All bench targets accept the same knobs (surfaced as make vars):

| var | meaning | default |
|---|---|---|
| `RATE` | target changes/sec | 100 |
| `DURATION` | seconds of load | 60 |
| `MIX` | INSERT/UPDATE/DELETE %, must sum to 100 | 70/20/10 |
| `SEED_ROWS` | rows to preload before timing | 0 |

Example soak: `make debezium-bench RATE=200 DURATION=3600 MIX=70/20/10`.

## What the numbers mean

- **p50/p95/p99/max latency (ms)** — time from a change committing in Postgres to its
  arrival in MSSQL, measured on one host clock (no cross-DB clock skew). p95/p99/max
  are the worst-case lag.
- **throughput (rows/s)** — rows landing in MSSQL over the *load window only* (the tail
  drain is excluded from the denominator so tools run for different durations are
  compared fairly). Compare against `RATE` to see whether the tool keeps up.
- **completeness (%)** — did every generated INSERT arrive? (Latency is measured on
  inserted rows keyed by a monotonic `seq`.)

## Honest framing

Debezium **streams** the Postgres WAL, so it is structurally sub-second. Airbyte
**batches** on a sync schedule (and uses Debezium internally for capture), so it is
structurally higher-latency. This benchmark compares **latency vs. ease-of-setup**,
not streaming-vs-streaming. A multi-second Airbyte number is expected and correct — it
is the cost of the batch model, not a defect.

## Caveats (this environment)

- **Host Postgres port is 5442** (`.env`) to avoid a clash with other local Postgres
  containers; the CDC pipeline talks to `postgres:5432` over the docker network.
- **Airbyte's ingress defaults to host port 8010** (`AIRBYTE_PORT`) because 8000 was
  taken locally. Override with `make airbyte-up AIRBYTE_PORT=8000`.
- **On an arm64 Mac, MSSQL 2022 runs under amd64 emulation**, which adds a fixed
  latency overhead. The comparison stays fair — both tools write to the *same* MSSQL —
  but absolute numbers run higher than on native amd64. The selftest quantifies this
  floor (writing straight to MSSQL still shows tens of ms, not single digits).
- The official `abctl` installer (`connect.airbyte.com`) has returned Cloudflare 526s;
  `make airbyte-install` falls back to the pinned GitHub release automatically.

## Commands

`make up | down | selftest | debezium-up/-bench/-down | airbyte-up/-bench/-down | report | clean`
