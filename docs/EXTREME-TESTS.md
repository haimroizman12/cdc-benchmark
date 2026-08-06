# Extreme / Edge Tests — Debezium vs Airbyte

Beyond the light-load `make demo`, we pushed the benchmark to the edge along three
dimensions to see where each tool bends: **a large pre-existing table**, **a high
sustained change rate**, and **high throughput past the load generator's own commit
ceiling**. This documents the runs, the exact parameters, and the findings.

## Method & rig (applies to every run below)

- **Rig:** Postgres 16 (source) → SQL Server 2022 (target), one at a time, on an
  arm64 Mac. **SQL Server runs under x86 emulation**, which is the shared write
  bottleneck — fair to both tools (same target), but absolute numbers run higher than
  on native hardware.
- **What's measured:** latency = target-arrival − source-commit, both on one host clock
  (no cross-DB skew); measured on the *incremental inserts* keyed by a monotonic `seq`.
- **Delivery semantics differ and matter:** the Debezium JDBC sink is configured
  `insert.mode: upsert` (exactly-once-effective, **zero duplicates**); Airbyte's
  destination does bulk **append + at-least-once** (fast, but produces **duplicate
  rows**). This is not semantics-matched — see Caveats.
- **`GRACE` (drain) differs by tool on purpose:** it's only the post-load wait for the
  tail to arrive (excluded from throughput math), sized so *each* tool fully drains to
  ~100% completeness. Batch/Airbyte needs much more.

Knobs (make vars): `RATE` (target ops/s — actual shown as `generated`), `DURATION`
(seconds of load), `MIX` (I/U/D %), `SEED_ROWS` (pre-replicated baseline), `GRACE`
(drain seconds), `BATCH` (ops per commit).

---

## Test (a) — does a large pre-existing table slow replication?

**Setup (Debezium):** seed a baseline into Postgres, wait until it is fully replicated
to SQL Server, *then* time a heavy incremental load against that large table. Only the
incremental inserts are measured. Two runs, identical load, the **only** variable is
the baseline size.

| Parameter | Seed = 100,000 | Seed = 0 (control) |
|---|---|---|
| RATE / DURATION / MIX | 1000 / 120s / 70-20-10 | 1000 / 120s / 70-20-10 |
| actual insert rate | 399/s | 401/s |
| p50 latency | 33,869 ms | 34,548 ms |
| p95 latency | 62,176 ms | 62,016 ms |
| max latency | 64,417 ms | 64,163 ms |
| throughput | 383.8 rows/s | 387.9 rows/s |
| completeness | 96.2% | 96.7% |

**Finding: a 100k-row baseline had no measurable effect.** Every metric matches the
control within noise. Debezium streams the WAL sequentially and upserts by primary key
into a B-tree index, whose cost grows only logarithmically with table size — so table
size isn't the binding constraint. (What *did* drive the ~60× jump from the light
demo's 0.55s p50 to ~34s here was the higher rate saturating the sink — see the edge
test.) To see index depth matter you'd need millions of rows; the harness supports it
(`SEED_ROWS=1000000`).

---

## Edge test — same big-seed + high rate, Debezium vs Airbyte

Identical load on both tools, drained to ~100% so the latency distributions are
complete and comparable.

| Input | Debezium | Airbyte |
|---|---|---|
| SEED_ROWS | 100,000 | 100,000 |
| RATE (target) | 1000 | 1000 |
| actual insert rate | ~407/s (~580 ops/s) | ~409/s |
| DURATION | 120 s | 120 s |
| MIX | 70/20/10 | 70/20/10 |
| GRACE | 300 s | 600 s |

| Result | Debezium | Airbyte |
|---|---|---|
| p50 latency | 34,266 ms | **16,540 ms** |
| p95 latency | 60,121 ms | **29,448 ms** |
| p99 latency | 62,367 ms | **31,075 ms** |
| max latency | 62,615 ms | **31,928 ms** |
| throughput | 407 rows/s | 412 rows/s |
| unique completeness | **100.0%** | 100% |
| **duplicate rows** | **0** | **14,188** |

**Finding: the light-load result flips.** In the demo (RATE=50) Debezium crushed Airbyte
(0.55s vs 16s). At a rate that saturates the target, **Airbyte's latency is ~2× lower
across the whole distribution** — because its bulk-append writes clear the backlog
faster than Debezium's row-by-row upsert. **But** that speed came with **14,188
duplicate rows** (append/at-least-once); Debezium stayed exactly-once. Speed vs
correctness.

---

## Test (b) — batched commits: break the source ceiling, find saturation

The load generator commits one row at a time, which capped the *offered* rate at
~400/s (so the edge test measured a source-limited load, not the tools' ceiling).
`BATCH` commits N ops per transaction, removing the per-commit fsync. Both tools, same
load; the only new lever is `BATCH=200`, which pushed the offered rate to **~2,270
ops/s (~4× the edge test).**

| Input | Debezium (b) | Airbyte (b) |
|---|---|---|
| BATCH | 200 | 200 |
| RATE (target) | 5000 | 5000 |
| actual insert rate | **~1,587/s (~2,270 ops/s)** | ~1,511/s (~2,160 ops/s) |
| DURATION | 60 s | 60 s |
| MIX / SEED_ROWS | 70/20/10 / 0 | 70/20/10 / 0 |
| GRACE | 300 s | 600 s |

| Result | Debezium (b) | Airbyte (b) |
|---|---|---|
| p50 latency | 144,354 ms (2.4 min) | **18,436 ms (18 s)** |
| p95 latency | **273,042 ms (4.6 min)** | 128,957 ms (2.1 min) |
| p99 latency | **283,946 ms** | 385,996 ms (6.4 min) |
| max latency | **287,261 ms (4.8 min)** | 386,618 ms (6.4 min) |
| throughput | 1,587 rows/s | 1,589 rows/s |
| unique completeness | **100%** (95,216/95,224) | **100%** (90,640 distinct) |
| **duplicate rows** | **0** | **25,676 (~28%)** |

**Finding: both absorbed ~1,600 rows/s and delivered 100% of unique rows** given enough
drain — the emulated SQL Server's write ceiling (~1,600/s) is roughly the same for both.
The difference is *how they schedule it, and at what correctness cost:*

- **Debezium = uniform, bounded backlog.** Steady upsert stream → latencies cluster
  (p50 144s → max 287s, a tight ~2× spread), **no outliers, zero duplicates.** Slow
  under saturation but predictable and correct.
- **Airbyte = low median, ugly tail, heavy duplication.** Bulk-append syncs clear big
  chunks fast, so the *typical* row lands in ~18s (8× better median), **but** rows that
  miss sync windows wait up to **6.4 min** (p99/max *worse* than Debezium), and
  append + at-least-once produced **25,676 duplicate rows (~28%)** — double the edge
  test's rate.

---

## Cross-regime summary

The tools trade places depending on load and on which metric you weight:

| Load regime (offered) | Median latency winner | Worst-case winner | Correctness |
|---|---|---|---|
| Light — demo (RATE 50 / 20)* | **Debezium** 0.56s vs 16s | **Debezium** | **Debezium** (0 dupes) |
| Edge — ~580 ops/s | **Airbyte** 16.5s vs 34s | **Airbyte** 32s vs 63s | **Debezium** (0 vs 14k dupes) |
| Saturated — ~2,270 ops/s (b) | **Airbyte** 18s vs 144s | **Debezium** 287s vs 386s | **Debezium** (0 vs 26k dupes) |

\*The light row is the `make demo` run and used different per-tool rates (Debezium 50,
Airbyte 20), so it is indicative, not a controlled same-rate comparison. Edge and (b)
are same-rate on both tools.

### Bottom line
- **Freshness at low/moderate load, and correctness always → Debezium.** Sub-second
  when not saturated, exactly-once, uniform bounded latency; its only failure mode
  under overload is latency growing (roughly linearly), never data loss or duplicates.
- **Best *typical* latency under heavy saturation, if you can tolerate duplicates →
  Airbyte.** Bulk-append keeps the median low even when overloaded, but expect a heavy
  tail (minutes) and large-scale duplication that downstream consumers must dedupe.

## Caveats
1. **Not semantics-matched.** Debezium-upsert (correct) vs Airbyte-append (duplicates)
   isn't strictly apples-to-apples — Airbyte is partly faster because it does less work
   per row and tolerates duplicates. A matched test would be Debezium sink in `insert`
   mode, or Airbyte in dedup mode (slower).
2. **Emulation.** SQL Server under x86 emulation is the shared ~1,600/s ceiling; native
   hardware would shift the absolute numbers and possibly the crossover point.
3. **Operational: start heavy repeated runs from a clean slate.** Re-registering the
   Debezium connector (`make debezium-up`) after prior runs left high-`seq` rows in the
   source causes an **initial snapshot of that stale data**, which pollutes the target
   and corrupts the measurement (seen once: a run reported 0.06% completeness). Between
   heavy runs, wipe volumes: `docker compose --env-file .env -f docker/docker-compose.db.yml down -v`
   (or `make clean`, which also clears `results/`).

## Reproduce
```bash
# (a) large table, no effect:
make debezium-up
make debezium-bench RATE=1000 DURATION=120 MIX=70/20/10 SEED_ROWS=100000 GRACE=300

# edge, same load both tools (run one, down, then the other):
make debezium-bench RATE=1000 DURATION=120 MIX=70/20/10 SEED_ROWS=100000 GRACE=300
make airbyte-bench  RATE=1000 DURATION=120 MIX=70/20/10 SEED_ROWS=100000 GRACE=600

# (b) batched saturation:
make debezium-bench RATE=5000 DURATION=60 MIX=70/20/10 BATCH=200 GRACE=300
make airbyte-bench  RATE=5000 DURATION=60 MIX=70/20/10 BATCH=200 GRACE=600
```
