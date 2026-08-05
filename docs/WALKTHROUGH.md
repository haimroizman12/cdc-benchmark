# Code Walkthrough — end-to-end flow

This traces a single change from the moment it is generated in Postgres to the moment
its latency lands in the results table, pointing at the exact file and line for each
step.

There are **two layers**, and keeping them separate is the key to reading the code:

- **Layer A — the harness (`bench/`, our Python):** the *stopwatch*. It creates the
  changes and times their arrival. Identical for both tools.
- **Layer B — the CDC tool (`connectors/*.json` for Debezium, `airbyte/configure.py`
  for Airbyte):** the *thing under test* — the actual "Postgres → log → SQL Server"
  pipeline. We only *configure* it; the tool does the moving.

---

## Step 1 — A change is made in Postgres  *(our code)*

- The generator loop is `LoadGen.run_for()` → `LoadGen._one()` in `bench/loadgen.py:38`
  and `:24`. Each tick rolls a 1–100 dice against the mix: insert (`:26`), update
  (`:32`), or delete (`:35`).
- The actual SQL is in `bench/run.py:13` `_pg_writers()` — `ins`/`upd`/`dele` run
  `INSERT/UPDATE/DELETE ... source_events` and `commit()` (`run.py:16–29`).
- **The critical timing line:** on every insert, `loadgen.py:28–29` stamps
  `now = time.time()` and stores it in `self.written_at[seq]` *before* writing. That
  in-memory dict `{seq → host-epoch-seconds}` is the "T0" for every row. The same value
  is also written into the row's `written_at` column (`run.py:19`), but the measurement
  uses the **in-memory dict**, not the DB column — so the clock is purely the host's.
- Rate control: `loadgen.py:40,46` sleeps `1/RATE` between operations.

## Step 2 — Postgres writes the change to its log  *(Postgres itself, enabled by our config)*

This is the "log file" — the **Postgres WAL (write-ahead log)**. We don't write it; we
*enable logical decoding* of it:

- `docker/docker-compose.db.yml:4` starts Postgres with `wal_level=logical` (plus
  replication slots/senders).
- `sql/postgres_init.sql` creates `source_events` and sets `REPLICA IDENTITY FULL` so
  updates/deletes carry full before-images into the WAL.

## Step 3 — The CDC tool reads the log and writes to SQL Server  *(the tool, configured by us)*

**Debezium path:**

1. **Read the WAL:** the Postgres source connector `connectors/pg-source.json` —
   `plugin.name: pgoutput`, `table.include.list: public.source_events`,
   `topic.prefix: cdc`. It tails the WAL and emits one message per change.
2. **Transport:** those messages land on a Kafka topic `cdc.public.source_events`
   (Kafka runs from `docker/docker-compose.debezium.yml`; the Connect image with both
   connectors + the MSSQL driver is built by `docker/connect.Dockerfile`).
3. **Write to SQL Server:** the JDBC sink connector `connectors/mssql-sink.json`
   consumes that topic and upserts into `dbo.source_events` (`insert.mode: upsert`,
   `primary.key.fields: id`, `delete.enabled: true`).
4. Both connectors are registered by `make debezium-up` (the `curl -X POST
   .../connectors` lines in the `Makefile`).

**Airbyte path** (same job, different mechanism — batch):

- `airbyte/configure.py` builds it over Airbyte's public API: `setup()` creates a
  Postgres **source** (CDC via `pgoutput`, using publication `airbyte_pub` + slot
  `airbyte_slot`), a SQL Server **destination**, and a **connection** between them.
- `sync()` triggers one sync job and waits for it to finish. `make airbyte-bench` runs
  these syncs back-to-back for the whole window, because Airbyte only moves data when a
  sync runs.
- Key difference for reading the numbers: Debezium is a continuous stream; Airbyte
  moves data in discrete batches, which is exactly why its latency is higher and
  lumpier (bimodal — see the README's results section).

## Step 4 — The harness detects arrival and computes the time  *(our code)*

- A background poller thread (`run.py:106–112`) calls `Measurer.poll_once()` every
  50 ms (`measure.py:18`).
- `poll_once` asks SQL Server "any new seq since the last one I saw?" via `_mssql_fetch`
  (`run.py:55–62`, the `SELECT seq FROM dbo.source_events WHERE seq > last_seq`).
- **The latency calculation** is `measure.py:20–23`: the moment a new `seq` appears, it
  records `observed = time.time()`, looks up that seq's send time
  `w = written_at[seq]`, and stores `latency_ms = (observed − w) × 1000`. Both
  timestamps are the host clock → no cross-database skew.
- When load generation finishes, `run.py:121–123` stops the poller and then **drains**
  (`measure.drain`, `measure.py:26`) for `GRACE` more seconds to catch the tail that is
  still in flight.

## Step 5 — Results: percentiles, completeness, files  *(our code)*

- `run.py:126` calls `metrics.summarize()` (`bench/metrics.py:37`):
  - percentiles via `percentile()` (`metrics.py:24`, nearest-rank, explicit
    round-half-up at `:32`),
  - `throughput = arrived / load_duration` — note `load_duration` (`run.py:118`)
    deliberately **excludes** the grace window so tools run for different durations
    compare fairly,
  - `completeness = arrived / generated` where `generated = gen.seq` (number of inserts)
    and `arrived = len(samples)`.
- `run.py:133–140` writes two files per run into `results/`: `<tool>-<ts>.json` (the
  summary) and `<tool>-<ts>.raw.csv` (every `seq,latency_ms` sample, for deeper
  analysis).
- `make report` loads the latest JSON per tool and renders the comparison table via
  `report.render_table()` in `bench/report.py`.

---

## One honest detail worth knowing

Latency and completeness are measured on **inserts only** — each insert gets a unique
`seq` the harness can follow end-to-end. Updates and deletes still exercise the pipeline
(they are real WAL changes the tool must replicate) and count toward load, but they
modify/remove existing rows rather than creating new `seq` values, so they are not
individually timed. That is why the demo uses `MIX=100/0/0` for the headline numbers —
every generated row is measured.

## Diagram

```
                          Layer A: harness (bench/)                Layer B: CDC tool
                          ─────────────────────────                ─────────────────
 loadgen._one()  ──INSERT──►  source_events (Postgres)
   stamps written_at[seq]          │
                                   │  wal_level=logical
                                   ▼
                              Postgres WAL  ──read──►  Debezium PG source  /  Airbyte source
                                                              │                     │
                                                          Kafka topic          Airbyte sync
                                                              │                     │
                                                        JDBC sink  /  destination-mssql
                                                              │
                                                              ▼
 measure.poll_once()  ◄──SELECT seq──  dbo.source_events (SQL Server)
   latency = now − written_at[seq]
                                   │
                                   ▼
 metrics.summarize()  ──►  results/<tool>-<ts>.json + .raw.csv  ──►  report.render_table()
```
