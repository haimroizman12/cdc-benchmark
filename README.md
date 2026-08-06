# CDC Benchmark — Debezium vs Airbyte (Postgres → MSSQL)

Local, reproducible benchmark measuring replication latency of two CDC tools moving
changes from Postgres (source) to SQL Server (target). One containerized Python
harness generates a tunable change stream into Postgres, times every row against a
single host clock (no cross-DB skew), and renders a comparison table.

For a step-by-step tour of the code (a change in Postgres → the WAL → the CDC tool →
SQL Server → the latency calculation), see [`docs/WALKTHROUGH.md`](docs/WALKTHROUGH.md).
For the edge/stress runs (large tables, high rate, batched-commit saturation) and the
cross-regime findings, see [`docs/EXTREME-TESTS.md`](docs/EXTREME-TESTS.md).

## Quick start

```bash
cp .env.example .env
make up                 # postgres + mssql
make selftest           # sanity check: harness writes straight to MSSQL (measurement floor)
make demo               # run BOTH tools (one at a time) + print the comparison table
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
| `RATE` | target changes/sec (per-row commits cap the real rate at a few hundred/s; the run's `generated` reports what was actually achieved) | 100 |
| `DURATION` | seconds of load | 60 |
| `MIX` | INSERT/UPDATE/DELETE %, must sum to 100 | 70/20/10 |
| `SEED_ROWS` | baseline rows bulk-loaded into the source **and fully replicated to the target before timing starts**, so the timed load runs against a large *existing* table; latency/completeness are measured on the incremental load only | 0 |
| `GRACE` | extra seconds after load to let the tail arrive (give batch/Airbyte much more) | 30 |

Example soak: `make debezium-bench RATE=200 DURATION=3600 MIX=70/20/10`.
Example large-table edge test: `make debezium-bench RATE=1000 DURATION=120 SEED_ROWS=100000`.

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

## Measured results

Both tools replicating `INSERT`s Postgres→MSSQL on the same rig (arm64 Mac, MSSQL
under amd64 emulation), produced by a single `make demo` run — **both delivered 100%
of rows**:

| Metric | debezium | airbyte |
|---|---|---|
| p50 latency (ms) | 557 | 15,765 |
| p95 latency (ms) | 1,785 | 292,461 |
| p99 latency (ms) | 2,590 | 294,846 |
| max latency (ms) | 2,785 | 295,461 |
| throughput (rows/s) | 44.8 | 19.3 |
| completeness (%) | 100 | 100 |

Run parameters: Debezium `RATE=50 DURATION=20`, Airbyte `RATE=20 DURATION=60
GRACE=300`, both `MIX=100/0/0`. (Separately, `make selftest` — the harness writing
straight to MSSQL — is the **measurement floor**: p50 ~28 ms, isolating the emulation +
poll overhead that sits under *both* tools.)

**Read the two tools' distributions differently — that difference IS the result:**

- **Debezium streams**, so its distribution is *tight*: p50 0.56 s and **every** row
  landed within ~2.8 s. Freshness is uniform.
- **Airbyte batches**, so its distribution is *bimodal*: ~76% of rows arrived within a
  minute (p50 ~16 s), but **~24% waited 2–5 minutes** — they missed a sync window and
  sat until a later batch swept them (there is a clean gap at 60–120 s: rows either make
  an early sync or wait for a late one). That is why p50 is ~16 s but p95 is ~290 s.

The headline is the latency axis: Debezium's typical latency is **sub-second**;
Airbyte's is **seconds at best and minutes at the tail**. Even Airbyte's *best case*
(p50) is ~28× Debezium; its tail is 100–500×. That is the batch-vs-stream model, not a
defect in either tool.

**On completeness vs. drain window (a finding in itself):** Airbyte's completeness is
sensitive to how long you let it drain. At `GRACE=180` an earlier run captured only
~75% within the window and showed a *smaller* tail; at `GRACE=300` it reaches 100% but
the recovered tail rows carry multi-minute latencies. A streaming tool has no such
knob — Debezium is 100% with a tight tail regardless. The Airbyte throughput number
(19.3 rows/s) reflects its lower offered rate here, not a keep-up failure.

## Recommendation

**For change-data replication where freshness matters, use Debezium.** It delivered
every row within ~3 seconds (100% under 2.8 s), versus Airbyte's ~16 s typical and a
2–5 minute tail, and it keeps up with a continuous stream rather than moving data in
periodic batches with a completeness-vs-drain-window trade-off.

Weigh it against setup cost:

- **Debezium** — you run and operate Kafka + Kafka Connect + two connectors (a
  Postgres source and a JDBC sink), and connector config lives in JSON you version
  yourself. More moving parts to stand up, but once `make debezium-up` is green it
  streams continuously and needed **zero** connector-config iteration here.
- **Airbyte** — one `abctl local install` gives you a UI, a connector catalog, and
  API-driven config, which is genuinely easier to *reason about*. But getting a
  Postgres-CDC→MSSQL sync to actually run took real fighting (see Caveats): a heavy
  kind-cluster install, a networking gap between the cluster and the databases, a
  certified MSSQL destination that **crashes introspecting an existing table** and so
  must own its own schema, and a batch model that is structurally 1–2 orders of
  magnitude slower.

**Pick Debezium when latency is the requirement** (near-real-time dashboards, event
propagation, cache invalidation). **Consider Airbyte when cadence is measured in
minutes/hours and you value the managed catalog + UI** over freshness — e.g. nightly
warehouse loads — and after validating that your specific destination connector is
healthy for your table types.

## Caveats (this environment)

- **Host Postgres port is 5442** (`.env`) to avoid a clash with other local Postgres
  containers; the CDC pipeline talks to `postgres:5432` over the docker network.
- **Airbyte's ingress defaults to host port 8010** (`AIRBYTE_PORT`) because 8000 was
  taken locally. Override with `make airbyte-up AIRBYTE_PORT=8000`.
- **On an arm64 Mac, MSSQL 2022 runs under amd64 emulation**, which adds a fixed
  latency overhead. The comparison stays fair — both tools write to the *same* MSSQL —
  but absolute numbers run higher than on native amd64. The selftest quantifies this
  floor (writing straight to MSSQL still shows tens of ms, not single digits).

### Airbyte integration frictions (part of the ease-of-use verdict)

All are handled automatically by the `airbyte-*` make targets; documented here because
they *are* the setup-cost half of the comparison:

- **abctl installer CDN** (`connect.airbyte.com`) has returned Cloudflare 526s;
  `make airbyte-install` falls back to the pinned GitHub release automatically.
- **Ingress port** defaults to 8010 (`AIRBYTE_PORT`) because 8000 was taken locally.
- **Cluster networking** — Airbyte's connector pods run inside abctl's kind Kubernetes
  cluster, a *separate* docker network from the DB rig, so the compose service names
  (`postgres`/`mssql`) don't resolve there (symptom: JDBC `08001`). `airbyte-up`
  connects the DB containers to the kind network and passes their kind-network IPs.
- **`destination-mssql` 2.2.20 cannot own an existing table** — it crashes
  introspecting columns whose types aren't in its `MssqlType` enum (`FLOAT`,
  `NVARCHAR`: "No enum constant …"). The connector must **create** its target table,
  so `airbyte-up` drops `dbo.source_events` first and lets Airbyte own it. For this
  reason `written_at` is a `BIGINT` (epoch micros), not `DOUBLE PRECISION`.
- **Two tools need different target-table shapes** (harness-created for Debezium,
  Airbyte-created for Airbyte), so `make up` alone does not restore a clean rig between
  tools — always switch via the tool's own `-up` target (as `make demo` does).
- **CDC `initial_waiting_seconds` must be ≥ 120** or the source watchdog times out.
- **Job status `incomplete` is transient** — Airbyte auto-retries a failed attempt and
  can still reach `succeeded`, so the sync driver treats only
  `succeeded`/`failed`/`cancelled` as terminal.

## Commands

`make up | down | selftest | demo | debezium-up/-bench/-down | airbyte-up/-bench/-down | report | clean`
