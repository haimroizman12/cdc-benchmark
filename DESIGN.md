# CDC Benchmark — Debezium vs Airbyte (Postgres → MSSQL) — Design

**Date:** 2026-08-05
**Status:** Design approved, pending written-spec review
**Goal:** A self-contained, `make`-driven local project that benchmarks two CDC (change-data-capture)
tools — **Debezium** and **Airbyte** — replicating **Postgres (source) → MSSQL (target)**, measures
replication latency under a configurable soak load, and produces a side-by-side summary table plus a
bottom-line recommendation.

---

## 1. Requirements (confirmed with the requester)

- **DB pair:** Postgres (source) → MSSQL (target). Fixed.
- **Schema/data:** synthetic table — no production schema. "Just something we can soak test."
- **Load:** a **CLI-configurable** load generator (flags to raise/lower throughput, volume, op mix)
  so multiple scenarios can be run. Sensible tunable defaults.
- **Metrics:** end-to-end write→arrival latency, reported richly — **p50/p95/p99/max**, throughput,
  lag-over-time, completeness. No single hard threshold; all metrics inform the decision.
- **Both tools required**, run **sequentially** (not simultaneously) against the same DB rig.
- **Reproducible via `make`** on any machine with Docker (the requester will run it himself).
- **Deliverables:** the runnable repo + README + a summary table + a written recommendation.
- **Repo:** new standalone repository (`~/Projects/cdc-benchmark`).

## 2. Known realism note (must be stated in the README + report)

Debezium is a **streaming** CDC tool (reads Postgres's WAL continuously → sub-second latency).
Airbyte is a **batch/scheduled** integration platform (syncs on a trigger, not a live stream) and
**uses Debezium internally** for Postgres CDC capture. So the comparison is effectively
"raw streaming Debezium vs. batch Airbyte." Debezium will almost certainly win raw latency; the
real decision is **latency vs. ease-of-setup**. The benchmark measures each tool *the way it
actually works* and reports the trade-off honestly — it does not pretend Airbyte is a streaming
tool.

---

## 3. Architecture — the rig

```
[loadgen.py] → Postgres(source) → [ Debezium stack  OR  Airbyte ] → MSSQL(target) → [measure.py]
```

- **Postgres** (Docker) — source, `wal_level=logical` enabled (Debezium requires it).
- **MSSQL** (Docker) — target.
- **The tool under test** — Debezium's stack *or* Airbyte, **never both at once** (they would
  contend for the same source). Each has its own `make` up/down targets.
- **Everything in Docker Compose** — `make up` works identically on any machine; no local installs
  beyond Docker.
- **Load generator + latency measurer** — Python, run on the host (so both timestamps share one
  clock; see §5).

### The two pipelines

- **Debezium:** `Postgres → Debezium PG source connector → Kafka → Debezium JDBC sink connector →
  MSSQL`. Runs as Kafka + Kafka Connect (with both connectors) in Docker. `make debezium-up` loads
  the connectors via Kafka Connect's REST API — scripted, no manual UI steps.
- **Airbyte:** `Postgres (CDC source) → Airbyte → MSSQL (destination)`. Runs as the Airbyte platform
  in Docker. `make airbyte-up` installs it, then configures source + destination + connection **via
  Airbyte's API** (scripted, reproducible), with the tightest sync schedule it supports.

Both write into the **same target table shape** in MSSQL, driven by the **same load** into Postgres.

---

## 4. Load generator (`loadgen.py`)

A Python CLI that writes a continuous stream of changes into the Postgres source table.

```
python loadgen.py --rate 200 --duration 3600 --seed-rows 100000 --mix 70/20/10
```

- `--rate` — changes per second (stress level).
- `--duration` — soak length in seconds.
- `--seed-rows` — initial bulk load before the soak begins.
- `--mix I/U/D` — insert/update/delete percentages (must sum to 100).

**Source table (synthetic):** a primary key `id`, a monotonic unique **`seq`** (BIGINT), a
client-stamped **`written_at`** (see §5), and a few payload columns (text/number) so rows are
non-trivial. Updates/deletes target existing rows at random.

Defaults are chosen so a bare `make ...-bench` runs a meaningful soak; every knob is overridable
via `make` variables (`RATE=`, `DURATION=`, `MIX=`, `SEED_ROWS=`).

---

## 5. Latency measurement (`measure.py`) — the method

Each inserted row carries a unique **`seq`** and a **`written_at`** timestamp **stamped by the
Python client (host wall clock), not by the database.** `measure.py` polls MSSQL rapidly (~50 ms)
and, the instant a row's `seq` appears, records **`observed_at`** — again from the **same host
clock**.

```
latency = observed_at − written_at
```

**Why client-stamped:** Postgres and MSSQL are separate containers with separate clocks; comparing
a Postgres timestamp to an MSSQL timestamp would be corrupted by clock skew. Taking *both*
timestamps from the single host clock removes skew entirely. The only error is the ~50 ms poll
granularity, which is documented.

**Measured stream vs. churn:** latency is measured on the **arrival of inserted `seq` rows** (clean,
unambiguous). The `--mix` update/delete share runs concurrently as additional churn — it stresses
the tool and proves it replicates all op types — and completeness is verified by row/seq
reconciliation. Headline latency = insert-arrival latency.

**Computed per run:** latency **p50 / p95 / p99 / max**, **throughput** (rows/sec landing in MSSQL
vs. the generated rate — does it keep up?), **lag-over-time** (is delay flat or rising across the
soak?), **completeness** (did every generated `seq` arrive?).

**Outputs:** every individual sample `(seq, written_at, observed_at, latency)` is appended to a raw
data file (CSV/JSONL) during the run; at the end the computed summary metrics are written to a
small per-run **JSON**. The raw file is the evidence; the JSON feeds the report. The exact same
measurement runs for both tools.

---

## 6. `make` command surface

```
make up              # start Postgres + MSSQL (shared rig)
make down            # stop the shared rig

make debezium-up     # Kafka + Kafka Connect + load connectors
make debezium-bench  # full soak through Debezium (RATE=/DURATION=/MIX=/SEED_ROWS=)
make debezium-down

make airbyte-up      # install Airbyte + configure source/dest/connection via API
make airbyte-bench   # full soak through Airbyte
make airbyte-down

make report          # render the comparison table from the per-run JSONs
make demo            # short end-to-end pass of BOTH tools with small defaults
make clean           # wipe containers, volumes, and results
```

`make demo` exists so the requester can see the whole thing work in minutes without reading docs;
the full soak is the longer, tunable run.

---

## 7. Deliverables

1. **Runnable repo** — `git clone` + `make up` on any Docker machine.
2. **README** — quick start, what each command does, the honest Debezium-vs-Airbyte framing (§2),
   and how to interpret the metrics.
3. **Summary table** — both tools side by side (p50/p95/p99/max latency, throughput, lag-over-time,
   setup effort), generated by `make report`.
4. **Recommendation** — a short written bottom-line: which tool, why, and for which need — grounded
   in the measured numbers and the setup-effort observation.

---

## 8. Out of scope (v1)

- Any DB pair other than Postgres → MSSQL.
- Production-grade tuning of either tool (we use reasonable defaults; we're comparing, not
  optimizing).
- Schema evolution / DDL replication, multi-table joins, or transformation logic.
- A UI — the deliverable is CLI/`make` + a generated table.

---

## 9. Risks & mitigations

- **Airbyte MSSQL destination connector maturity / minimum sync frequency** — verify early; if the
  destination or sub-minute scheduling is unworkable, report it as a *finding* (it's part of the
  ease-of-use verdict) rather than a blocker. Flag immediately if a connector is outright broken.
- **Debezium JDBC sink → MSSQL** needs the correct JDBC driver + connector; standard path, scripted.
- **Timeline:** Debezium pipeline + the full harness first (guaranteed runnable), Airbyte second.
  A runnable repo (both tools, measured) is the tomorrow-morning target.
