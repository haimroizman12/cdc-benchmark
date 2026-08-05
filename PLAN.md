# CDC Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A self-contained, `make`-driven local project that benchmarks Debezium vs Airbyte replicating Postgres→MSSQL, measures end-to-end latency under a configurable soak load, and emits a comparison table + recommendation.

**Architecture:** Docker Compose brings up Postgres (source, `wal_level=logical`) + MSSQL (target). A containerized Python harness (`run.py`) generates a tunable change stream into Postgres and polls MSSQL, timing every change with a single host clock (no cross-DB skew). Each CDC tool sits between the DBs one at a time; the same harness measures both. `make report` renders the results.

**Tech Stack:** Docker Compose; Postgres 16; MSSQL 2022; Debezium (Kafka + Kafka Connect + PG source + JDBC sink connectors); Airbyte (via `abctl`); Python 3.12 (`psycopg2-binary`, `pymssql`) in a `bench` container.

## Global Constraints

- **DB pair fixed:** Postgres (source) → MSSQL (target).
- **Tools never run simultaneously** — separate `make` up/down targets; shared DB rig.
- **Reproducible via `make` on any Docker host** — no host installs beyond Docker (harness runs in a container; `abctl` is auto-installed by a make target).
- **Latency timestamps are stamped by the harness (one host clock), never by a database** — avoids cross-DB clock skew.
- **Metrics reported:** p50/p95/p99/max latency, throughput (rows/s), completeness (%), lag-over-time.
- **Load generator is CLI-configurable:** `--rate`, `--duration`, `--seed-rows`, `--mix I/U/D` (sum to 100), surfaced as `make` vars `RATE= DURATION= SEED_ROWS= MIX=`.
- **Synthetic schema** — single source table; no production schema.
- **Honest framing (README + report):** Debezium streams (WAL); Airbyte batches (and wraps Debezium internally) — the comparison is latency-vs-ease-of-setup, not streaming-vs-streaming.
- Python: standard library + the two DB drivers only. Format with `black` defaults if available; not required to pass.

**File structure:**
```
cdc-benchmark/
├── DESIGN.md                     # the approved spec
├── PLAN.md                       # this file
├── README.md                     # quickstart + framing (Task 6)
├── Makefile                      # all commands
├── .env.example                  # DB creds / ports
├── docker/
│   ├── docker-compose.db.yml     # postgres + mssql (Task 2)
│   ├── docker-compose.debezium.yml  # kafka + connect (Task 4)
│   ├── bench.Dockerfile          # python harness image (Task 3)
│   └── connect.Dockerfile        # debezium connect + jdbc sink + mssql driver (Task 4)
├── sql/
│   ├── postgres_init.sql         # source table + wal setup (Task 2)
│   └── mssql_init.sql            # target db + table (Task 2)
├── connectors/
│   ├── pg-source.json            # debezium PG source connector config (Task 4)
│   └── mssql-sink.json           # debezium JDBC sink connector config (Task 4)
├── airbyte/
│   └── configure.py              # source/dest/connection via Airbyte API (Task 5)
├── bench/
│   ├── __init__.py
│   ├── metrics.py                # pure: mix parse, percentiles, summary (Task 1)
│   ├── report.py                 # pure: render markdown table (Task 1/6)
│   ├── db.py                     # pg + mssql connection helpers (Task 3)
│   ├── loadgen.py                # change generator (Task 3)
│   ├── measure.py                # poller (Task 3)
│   └── run.py                    # orchestrator + argparse (Task 3)
├── tests/
│   ├── test_metrics.py           # Task 1
│   └── test_report.py            # Task 1
├── results/                      # per-run JSON + raw CSV (gitignored)
└── requirements.txt
```

---

### Task 1: Pure measurement logic — mix parsing, percentiles, summary, report table

**Why first:** the math the whole benchmark rests on, fully unit-testable with no DB or Docker. Safe, fast, TDD.

**Files:**
- Create: `bench/__init__.py` (empty), `bench/metrics.py`, `bench/report.py`, `requirements.txt`, `tests/test_metrics.py`, `tests/test_report.py`

**Interfaces:**
- Produces:
  - `metrics.parse_mix(mix: str) -> tuple[int,int,int]`
  - `metrics.percentile(values: list[float], pct: float) -> float` (nearest-rank)
  - `metrics.summarize(latencies_ms: list[float], generated: int, arrived: int, duration_s: float) -> dict` — keys: `count, p50_ms, p95_ms, p99_ms, max_ms, throughput_rows_per_s, generated, arrived, completeness_pct`
  - `report.render_table(runs: dict[str, dict]) -> str`

- [ ] **Step 1: requirements.txt**
```
psycopg2-binary==2.9.9
pymssql==2.3.1
pytest==8.2.0
```

- [ ] **Step 2: Write failing tests** — `tests/test_metrics.py`
```python
import pytest
from bench import metrics

def test_parse_mix_ok():
    assert metrics.parse_mix("70/20/10") == (70, 20, 10)

def test_parse_mix_must_sum_100():
    with pytest.raises(ValueError):
        metrics.parse_mix("70/20/5")

def test_parse_mix_bad_shape():
    with pytest.raises(ValueError):
        metrics.parse_mix("70/30")

def test_percentile_nearest_rank():
    vals = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    assert metrics.percentile(vals, 50) == 60   # nearest-rank of 10 items
    assert metrics.percentile(vals, 100) == 100
    assert metrics.percentile(vals, 0) == 10

def test_summarize():
    s = metrics.summarize([10.0, 20.0, 30.0], generated=4, arrived=3, duration_s=3.0)
    assert s["count"] == 3
    assert s["max_ms"] == 30.0
    assert s["arrived"] == 3
    assert s["generated"] == 4
    assert s["completeness_pct"] == 75.0
    assert s["throughput_rows_per_s"] == 1.0
```

- [ ] **Step 3: Run — expect FAIL** (`ModuleNotFoundError: bench`)
```bash
cd ~/Projects/cdc-benchmark && python -m pytest tests/test_metrics.py -q
```

- [ ] **Step 4: Implement** — `bench/__init__.py` (empty) and `bench/metrics.py`
```python
from __future__ import annotations


def parse_mix(mix: str) -> tuple[int, int, int]:
    parts = mix.split("/")
    if len(parts) != 3:
        raise ValueError(f"mix must be I/U/D, got {mix!r}")
    i, u, d = (int(p) for p in parts)
    if min(i, u, d) < 0:
        raise ValueError("mix parts must be non-negative")
    if i + u + d != 100:
        raise ValueError(f"mix must sum to 100, got {i + u + d}")
    return i, u, d


def percentile(values: list[float], pct: float) -> float:
    if not values:
        raise ValueError("no values")
    s = sorted(values)
    k = int(round(pct / 100 * (len(s) - 1)))
    k = max(0, min(len(s) - 1, k))
    return s[k]


def summarize(
    latencies_ms: list[float], generated: int, arrived: int, duration_s: float
) -> dict:
    lat = latencies_ms or [0.0]
    return {
        "count": len(latencies_ms),
        "p50_ms": round(percentile(lat, 50), 2),
        "p95_ms": round(percentile(lat, 95), 2),
        "p99_ms": round(percentile(lat, 99), 2),
        "max_ms": round(max(lat), 2),
        "throughput_rows_per_s": round(arrived / duration_s, 2) if duration_s else 0.0,
        "generated": generated,
        "arrived": arrived,
        "completeness_pct": round(100 * arrived / generated, 2) if generated else 0.0,
    }
```

- [ ] **Step 5: Run — expect PASS**
```bash
python -m pytest tests/test_metrics.py -q
```

- [ ] **Step 6: Write failing test** — `tests/test_report.py`
```python
from bench import report

def test_render_table_two_tools():
    runs = {
        "debezium": {"p50_ms": 120, "p95_ms": 300, "p99_ms": 500, "max_ms": 800,
                     "throughput_rows_per_s": 200, "completeness_pct": 100.0},
        "airbyte": {"p50_ms": 45000, "p95_ms": 70000, "p99_ms": 88000, "max_ms": 90000,
                    "throughput_rows_per_s": 200, "completeness_pct": 100.0},
    }
    out = report.render_table(runs)
    assert "| Metric | debezium | airbyte |" in out
    assert "p50 latency (ms)" in out
    assert "120" in out and "45000" in out
```

- [ ] **Step 7: Run — expect FAIL**, then implement `bench/report.py`
```python
from __future__ import annotations

_ROWS = [
    ("p50 latency (ms)", "p50_ms"),
    ("p95 latency (ms)", "p95_ms"),
    ("p99 latency (ms)", "p99_ms"),
    ("max latency (ms)", "max_ms"),
    ("throughput (rows/s)", "throughput_rows_per_s"),
    ("completeness (%)", "completeness_pct"),
]


def render_table(runs: dict[str, dict]) -> str:
    tools = list(runs.keys())
    lines = [
        "| Metric | " + " | ".join(tools) + " |",
        "|" + "---|" * (len(tools) + 1),
    ]
    for label, key in _ROWS:
        cells = " | ".join(str(runs[t].get(key, "—")) for t in tools)
        lines.append(f"| {label} | {cells} |")
    return "\n".join(lines)
```

- [ ] **Step 8: Run all tests — expect PASS**
```bash
python -m pytest -q
```

- [ ] **Step 9: Commit**
```bash
git add bench/ tests/ requirements.txt && git commit -m "feat: pure measurement logic (mix, percentiles, summary, report table)"
```

---

### Task 2: Docker DB rig — Postgres + MSSQL + schemas + `make up`

**Files:**
- Create: `.env.example`, `docker/docker-compose.db.yml`, `sql/postgres_init.sql`, `sql/mssql_init.sql`, `Makefile` (initial), remove `main.py`

**Interfaces:**
- Produces: a running Postgres (`localhost:5432`) with a `source` table + logical replication, and MSSQL (`localhost:1433`) with a `target` table. Both reachable with creds from `.env`.

- [ ] **Step 1: `.env.example`** (copied to `.env` by make)
```
POSTGRES_USER=cdc
POSTGRES_PASSWORD=cdc_pw
POSTGRES_DB=source_db
POSTGRES_PORT=5432
MSSQL_SA_PASSWORD=Cdc_Str0ng_Pw!
MSSQL_PORT=1433
MSSQL_DB=target_db
```

- [ ] **Step 2: `sql/postgres_init.sql`** — source table + full replica identity (so updates/deletes carry before-images)
```sql
CREATE TABLE IF NOT EXISTS source_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seq         BIGINT NOT NULL UNIQUE,
    written_at  DOUBLE PRECISION NOT NULL,   -- host epoch seconds, set by the harness
    payload     TEXT NOT NULL
);
ALTER TABLE source_events REPLICA IDENTITY FULL;
```

- [ ] **Step 3: `sql/mssql_init.sql`** — target db + table (mirrors source columns the sink writes)
```sql
IF DB_ID('target_db') IS NULL EXEC('CREATE DATABASE target_db');
GO
USE target_db;
GO
IF OBJECT_ID('dbo.source_events','U') IS NULL
CREATE TABLE dbo.source_events (
    id          BIGINT NOT NULL PRIMARY KEY,
    seq         BIGINT NOT NULL,
    written_at  FLOAT  NOT NULL,
    payload     NVARCHAR(MAX) NULL
);
GO
CREATE INDEX ix_source_events_seq ON dbo.source_events(seq);
GO
```

- [ ] **Step 4: `docker/docker-compose.db.yml`**
```yaml
services:
  postgres:
    image: postgres:16
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"]
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports: ["${POSTGRES_PORT}:5432"]
    volumes:
      - ../sql/postgres_init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 3s
      timeout: 3s
      retries: 20

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: ${MSSQL_SA_PASSWORD}
    ports: ["${MSSQL_PORT}:1433"]
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"$$MSSQL_SA_PASSWORD\" -C -Q 'SELECT 1' || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 30

networks:
  default:
    name: cdc-bench
```

- [ ] **Step 5: `Makefile`** (initial targets; note `.env` bootstrap + mssql schema applied after healthy)
```makefile
COMPOSE_DB = docker compose --env-file .env -f docker/docker-compose.db.yml
SQLCMD = /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$$MSSQL_SA_PASSWORD" -C

.PHONY: env up down mssql-schema clean
env:
	@test -f .env || cp .env.example .env

up: env
	$(COMPOSE_DB) up -d
	@echo "waiting for databases to be healthy..."
	@until [ "$$($(COMPOSE_DB) ps mssql --format '{{.Health}}')" = "healthy" ]; do sleep 2; done
	@$(MAKE) mssql-schema

mssql-schema:
	$(COMPOSE_DB) exec -T mssql bash -lc '$(SQLCMD) -Q "$$(cat /dev/stdin)"' < sql/mssql_init.sql

down:
	$(COMPOSE_DB) down

clean:
	$(COMPOSE_DB) down -v
	rm -rf results/*.json results/*.csv
```

- [ ] **Step 6: Remove the PyCharm stub**
```bash
git rm -q main.py
```

- [ ] **Step 7: Verify the rig** (this is the task's test)
```bash
make up
# Expect: both containers up; the wait loop exits; mssql-schema runs without error.
docker compose --env-file .env -f docker/docker-compose.db.yml exec -T postgres \
  psql -U cdc -d source_db -c "\d source_events"    # shows the table
docker compose --env-file .env -f docker/docker-compose.db.yml exec -T mssql \
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -Q "USE target_db; SELECT COUNT(*) FROM dbo.source_events;"   # returns 0
```
Expected: Postgres shows `source_events` with a `REPLICA IDENTITY FULL`; MSSQL returns `0`. If the `mssql-tools18` path differs for the image tag, adjust `SQLCMD` to the path that exists (`ls /opt/` inside the container).

- [ ] **Step 8: Commit**
```bash
git add -A && git commit -m "feat: dockerized postgres+mssql rig with schemas and make up/down"
```

---

### Task 3: Python harness — loadgen + measure + orchestrator (`run.py`), containerized, with a self-test

**Why a self-test:** before any CDC tool exists, prove the harness mechanically works by pointing the writer directly at the MSSQL target (`--tool selftest`) — arrivals should show ~0 latency. This de-risks Tasks 4/5.

**Files:**
- Create: `docker/bench.Dockerfile`, `bench/db.py`, `bench/loadgen.py`, `bench/measure.py`, `bench/run.py`; modify `Makefile`

**Interfaces:**
- Consumes: `metrics.parse_mix`, `metrics.summarize`, `report` (Task 1).
- Produces: `run.py` CLI writing `results/<tool>-<ts>.json` (a `summarize()` dict + a `tool` key) and `results/<tool>-<ts>.raw.csv`. `db.pg_connect()`, `db.mssql_connect()`.

- [ ] **Step 1: `docker/bench.Dockerfile`** (freetds for pymssql)
```dockerfile
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev freetds-bin gcc && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bench/ ./bench/
ENTRYPOINT ["python", "-m", "bench.run"]
```

- [ ] **Step 2: `bench/db.py`**
```python
from __future__ import annotations
import os
import psycopg2
import pymssql


def pg_connect():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def mssql_connect():
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "mssql"),
        port=int(os.environ.get("MSSQL_PORT_INTERNAL", "1433")),
        user="sa",
        password=os.environ["MSSQL_SA_PASSWORD"],
        database=os.environ.get("MSSQL_DB", "target_db"),
    )
```

- [ ] **Step 3: `bench/loadgen.py`** — writes the change stream; each insert carries `seq` + host `written_at`
```python
from __future__ import annotations
import random
import time
from bench import metrics


class LoadGen:
    """Generates INSERT/UPDATE/DELETE against source_events at a target rate.

    Composes with run.py: caller supplies a write-target (Postgres for real runs,
    MSSQL directly for selftest). Records written_at (host epoch) per inserted seq.
    """

    def __init__(self, write_insert, write_update, write_delete, mix: str, rate: int):
        self.write_insert = write_insert
        self.write_update = write_update
        self.write_delete = write_delete
        self.i, self.u, self.d = metrics.parse_mix(mix)
        self.rate = rate
        self.seq = 0
        self.live_ids: list[int] = []
        self.written_at: dict[int, float] = {}

    def _one(self) -> None:
        roll = random.randint(1, 100)
        if roll <= self.i or not self.live_ids:
            self.seq += 1
            now = time.time()
            self.written_at[self.seq] = now
            self.write_insert(self.seq, now, f"payload-{self.seq}")
            self.live_ids.append(self.seq)
        elif roll <= self.i + self.u:
            self.write_update(random.choice(self.live_ids))
        else:
            victim = self.live_ids.pop(random.randrange(len(self.live_ids)))
            self.write_delete(victim)

    def run_for(self, duration_s: float) -> int:
        end = time.time() + duration_s
        interval = 1.0 / self.rate if self.rate else 0
        n = 0
        while time.time() < end:
            self._one()
            n += 1
            if interval:
                time.sleep(interval)
        return n
```

- [ ] **Step 4: `bench/measure.py`** — polls the target for new `seq`, records arrival latency
```python
from __future__ import annotations
import time


class Measurer:
    """Polls the MSSQL target for newly-arrived seq values and records latency.

    latency_ms = (observed_at - written_at) * 1000, both from the host clock.
    """

    def __init__(self, fetch_new_seqs, written_at: dict[int, float], poll_s: float = 0.05):
        self.fetch_new_seqs = fetch_new_seqs   # (since_seq) -> list[int] sorted asc
        self.written_at = written_at
        self.poll_s = poll_s
        self.samples: list[tuple[int, float]] = []  # (seq, latency_ms)
        self.last_seq = 0

    def poll_once(self) -> None:
        for seq in self.fetch_new_seqs(self.last_seq):
            observed = time.time()
            w = self.written_at.get(seq)
            if w is not None:
                self.samples.append((seq, (observed - w) * 1000.0))
            self.last_seq = max(self.last_seq, seq)

    def drain(self, deadline: float) -> None:
        while time.time() < deadline:
            self.poll_once()
            time.sleep(self.poll_s)
```

- [ ] **Step 5: `bench/run.py`** — orchestrator (argparse, threads, writes results)
```python
from __future__ import annotations
import argparse
import csv
import json
import os
import pathlib
import threading
import time

from bench import db, loadgen, measure, metrics


def _pg_writers(conn):
    cur = conn.cursor()

    def ins(seq, written_at, payload):
        cur.execute(
            "INSERT INTO source_events (seq, written_at, payload) VALUES (%s,%s,%s)",
            (seq, written_at, payload),
        )
        conn.commit()

    def upd(seq):
        cur.execute("UPDATE source_events SET payload = payload || '+' WHERE seq = %s", (seq,))
        conn.commit()

    def dele(seq):
        cur.execute("DELETE FROM source_events WHERE seq = %s", (seq,))
        conn.commit()

    return ins, upd, dele


def _mssql_writers(conn):
    cur = conn.cursor()

    def ins(seq, written_at, payload):
        cur.execute(
            "INSERT INTO dbo.source_events (id, seq, written_at, payload) VALUES (%s,%s,%s,%s)",
            (seq, seq, written_at, payload),
        )
        conn.commit()

    def upd(seq):
        cur.execute("UPDATE dbo.source_events SET payload = payload + '+' WHERE seq = %s", (seq,))
        conn.commit()

    def dele(seq):
        cur.execute("DELETE FROM dbo.source_events WHERE seq = %s", (seq,))
        conn.commit()

    return ins, upd, dele


def _mssql_fetch(conn):
    cur = conn.cursor()

    def fetch(since_seq):
        cur.execute("SELECT seq FROM dbo.source_events WHERE seq > %s ORDER BY seq", (since_seq,))
        return [r[0] for r in cur.fetchall()]

    return fetch


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tool", required=True)  # debezium | airbyte | selftest
    ap.add_argument("--rate", type=int, default=100)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--seed-rows", type=int, default=0)
    ap.add_argument("--mix", default="70/20/10")
    ap.add_argument("--grace", type=int, default=30)  # extra seconds to let the tail arrive
    args = ap.parse_args()

    target = db.mssql_connect()
    fetch = _mssql_fetch(target)

    if args.tool == "selftest":
        writers = _mssql_writers(db.mssql_connect())
    else:
        writers = _pg_writers(db.pg_connect())

    gen = loadgen.LoadGen(*writers, mix=args.mix, rate=args.rate)
    mea = measure.Measurer(fetch, gen.written_at)

    stop = threading.Event()

    def poll_loop():
        while not stop.is_set():
            mea.poll_once()
            time.sleep(mea.poll_s)

    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    start = time.time()
    generated = gen.run_for(args.duration)
    mea.drain(time.time() + args.grace)   # catch the tail
    stop.set()
    duration = time.time() - start

    lat = [ms for _, ms in mea.samples]
    summary = metrics.summarize(lat, generated=gen.seq, arrived=len(mea.samples), duration_s=duration)
    summary["tool"] = args.tool
    summary["rate"] = args.rate
    summary["mix"] = args.mix

    ts = time.strftime("%Y%m%d-%H%M%S")
    outdir = pathlib.Path("results")
    outdir.mkdir(exist_ok=True)
    (outdir / f"{args.tool}-{ts}.json").write_text(json.dumps(summary, indent=2))
    with (outdir / f"{args.tool}-{ts}.raw.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "latency_ms"])
        w.writerows(mea.samples)

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 6: Add the bench container + `selftest`/`bench` targets to `Makefile`**
```makefile
COMPOSE_BENCH = docker compose --env-file .env -f docker/docker-compose.db.yml -f docker/bench.compose.yml
RATE ?= 100
DURATION ?= 60
MIX ?= 70/20/10
SEED_ROWS ?= 0

.PHONY: bench-build selftest
bench-build:
	docker build -f docker/bench.Dockerfile -t cdc-bench .

selftest: bench-build
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool selftest --rate $(RATE) --duration 10 --mix 100/0/0
```
Verification note: `selftest` writes directly to MSSQL, so latency is ~0–5 ms and `completeness_pct` is 100.

- [ ] **Step 7: Run the self-test** (task test)
```bash
make up && make selftest
# Expect JSON with completeness_pct 100.0 and small p50/max (single-digit to low-tens ms).
ls results/   # a selftest-*.json and selftest-*.raw.csv exist
```

- [ ] **Step 8: Commit**
```bash
git add -A && git commit -m "feat: containerized python harness (loadgen+measure+run) with selftest"
```

---

### Task 4: Debezium pipeline (Postgres → Kafka → JDBC sink → MSSQL)

**Files:**
- Create: `docker/connect.Dockerfile`, `docker/docker-compose.debezium.yml`, `connectors/pg-source.json`, `connectors/mssql-sink.json`; modify `Makefile`

**Interfaces:**
- Consumes: the DB rig (Task 2), the harness (Task 3).
- Produces: `make debezium-up/-down/-bench`; rows written to Postgres `source_events` appear in MSSQL `dbo.source_events` within sub-second.

- [ ] **Step 1: `docker/connect.Dockerfile`** — Debezium Connect + JDBC sink + MSSQL driver
```dockerfile
FROM quay.io/debezium/connect:2.7
# Debezium JDBC sink connector + Microsoft JDBC driver into the plugin path.
ENV KAFKA_CONNECT_PLUGINS_DIR=/kafka/connect
USER root
RUN mkdir -p /kafka/connect/debezium-jdbc && \
    curl -fSL -o /tmp/jdbc.tar.gz \
      https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/2.7.0.Final/debezium-connector-jdbc-2.7.0.Final-plugin.tar.gz && \
    tar -xzf /tmp/jdbc.tar.gz -C /kafka/connect/ && \
    curl -fSL -o /kafka/connect/debezium-connector-jdbc/mssql-jdbc.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar
USER 1001
```
Verify the two URLs resolve (HTTP 200) before building; if a version 404s, bump to the nearest published version on Maven Central and note it.

- [ ] **Step 2: `docker/docker-compose.debezium.yml`** (Kafka in KRaft mode + Connect)
```yaml
services:
  kafka:
    image: quay.io/debezium/kafka:2.7
    environment:
      CLUSTER_ID: "cdc-bench-cluster-0001"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
    ports: ["9092:9092"]

  connect:
    build:
      context: ..
      dockerfile: docker/connect.Dockerfile
    depends_on: [kafka]
    environment:
      BOOTSTRAP_SERVERS: "kafka:9092"
      GROUP_ID: "cdc-connect"
      CONFIG_STORAGE_TOPIC: "connect_configs"
      OFFSET_STORAGE_TOPIC: "connect_offsets"
      STATUS_STORAGE_TOPIC: "connect_statuses"
    ports: ["8083:8083"]

networks:
  default:
    name: cdc-bench
```

- [ ] **Step 3: `connectors/pg-source.json`**
```json
{
  "name": "pg-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "cdc",
    "database.password": "cdc_pw",
    "database.dbname": "source_db",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput",
    "table.include.list": "public.source_events",
    "tombstones.on.delete": "false"
  }
}
```

- [ ] **Step 4: `connectors/mssql-sink.json`** (Debezium JDBC sink, upsert by `id`)
```json
{
  "name": "mssql-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "topics": "cdc.public.source_events",
    "connection.url": "jdbc:sqlserver://mssql:1433;databaseName=target_db;encrypt=false",
    "connection.username": "sa",
    "connection.password": "Cdc_Str0ng_Pw!",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "id",
    "delete.enabled": "true",
    "schema.evolution": "none",
    "table.name.format": "dbo.source_events"
  }
}
```
Note: creds are duplicated from `.env` here because Kafka Connect reads the raw JSON; if `.env` values change, update these two files (call this out in the README).

- [ ] **Step 5: `Makefile` Debezium targets**
```makefile
COMPOSE_DBZ = docker compose --env-file .env -f docker/docker-compose.db.yml -f docker/docker-compose.debezium.yml

.PHONY: debezium-up debezium-down debezium-bench
debezium-up: up
	$(COMPOSE_DBZ) up -d --build
	@echo "waiting for Kafka Connect REST..."
	@until curl -sf localhost:8083/ >/dev/null; do sleep 2; done
	curl -sf -X POST -H "Content-Type: application/json" --data @connectors/pg-source.json localhost:8083/connectors
	curl -sf -X POST -H "Content-Type: application/json" --data @connectors/mssql-sink.json localhost:8083/connectors
	@echo "connectors registered."

debezium-down:
	-curl -sf -X DELETE localhost:8083/connectors/mssql-sink
	-curl -sf -X DELETE localhost:8083/connectors/pg-source
	$(COMPOSE_DBZ) down

debezium-bench: bench-build
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool debezium --rate $(RATE) --duration $(DURATION) --mix $(MIX) --seed-rows $(SEED_ROWS)
```

- [ ] **Step 6: Verify replication end-to-end** (task test)
```bash
make debezium-up
# Confirm both connectors RUNNING:
curl -s localhost:8083/connectors/pg-source/status | grep -o '"state":"RUNNING"'
curl -s localhost:8083/connectors/mssql-sink/status | grep -o '"state":"RUNNING"'
make debezium-bench RATE=50 DURATION=20 MIX=100/0/0
# Expect: results JSON with completeness_pct ~100 and p50 sub-second (tens–low-hundreds ms).
```
If the sink stays FAILED, `curl -s localhost:8083/connectors/mssql-sink/status` shows the trace — most likely a driver/URL/creds mismatch; fix in `mssql-sink.json`. (This is the fiddly step — expect one or two iterations.)

- [ ] **Step 7: Commit**
```bash
git add -A && git commit -m "feat: debezium pipeline (pg source -> kafka -> jdbc sink -> mssql)"
```

---

### Task 5: Airbyte pipeline (Postgres CDC → Airbyte → MSSQL)

**Files:**
- Create: `airbyte/configure.py`; modify `Makefile`

**Interfaces:**
- Consumes: the DB rig (Task 2), the harness (Task 3).
- Produces: `make airbyte-up/-down/-bench`; rows replicate PG→MSSQL on Airbyte's sync cadence.

- [ ] **Step 1: `airbyte/configure.py`** — create source + destination + connection via the Airbyte API and expose a trigger-sync call
```python
"""Configure Airbyte for Postgres(CDC) -> MSSQL via its API.

Usage:
    python airbyte/configure.py setup     # create source, destination, connection
    python airbyte/configure.py sync      # trigger one sync and wait for completion
Reads AIRBYTE_URL (default http://localhost:8000/api/v1) and DB creds from env.
Prints/stores the connection id in airbyte/.connection_id.
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.request

BASE = os.environ.get("AIRBYTE_URL", "http://localhost:8000/api/v1")


def _post(path, body):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def setup():
    ws = _post("/workspaces/list", {})["workspaces"][0]["workspaceId"]
    src = _post("/sources/create", {
        "workspaceId": ws, "name": "pg-cdc",
        "sourceDefinitionId": PG_SOURCE_DEF_ID,
        "connectionConfiguration": {
            "host": "postgres", "port": 5432, "database": os.environ["POSTGRES_DB"],
            "username": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"],
            "replication_method": {"method": "CDC", "plugin": "pgoutput"},
            "ssl_mode": {"mode": "disable"},
        }})["sourceId"]
    dst = _post("/destinations/create", {
        "workspaceId": ws, "name": "mssql",
        "destinationDefinitionId": MSSQL_DEST_DEF_ID,
        "connectionConfiguration": {
            "host": "mssql", "port": 1433, "database": os.environ.get("MSSQL_DB", "target_db"),
            "username": "sa", "password": os.environ["MSSQL_SA_PASSWORD"], "schema": "dbo",
            "ssl_method": {"ssl_method": "unencrypted"},
        }})["destinationId"]
    conn = _post("/connections/create", {
        "sourceId": src, "destinationId": dst, "status": "active",
        "scheduleType": "manual", "namespaceDefinition": "destination",
    })["connectionId"]
    open("airbyte/.connection_id", "w").write(conn)
    print("connection:", conn)


def sync():
    conn = open("airbyte/.connection_id").read().strip()
    job = _post("/connections/sync", {"connectionId": conn})["job"]["id"]
    while True:
        st = _post("/jobs/get", {"id": job})["job"]["status"]
        if st in ("succeeded", "failed", "cancelled"):
            print("sync", st)
            return 0 if st == "succeeded" else 1
        time.sleep(2)


if __name__ == "__main__":
    {"setup": setup, "sync": sync}[sys.argv[1]]()
```
Note: `PG_SOURCE_DEF_ID` / `MSSQL_DEST_DEF_ID` are Airbyte connector-definition UUIDs; Step 3 fills them by querying `/source_definitions/list` and `/destination_definitions/list` on the running instance (they're stable per Airbyte version). Replace the two module-level constants with the discovered values, or look them up in `setup()`.

- [ ] **Step 2: `Makefile` Airbyte targets** (install via abctl; run the harness with a sync-trigger loop)
```makefile
.PHONY: airbyte-install airbyte-up airbyte-down airbyte-bench
airbyte-install:
	@command -v abctl >/dev/null || curl -LsfS https://connect.airbyte.com/v1/install | bash

airbyte-up: up airbyte-install
	abctl local install
	@echo "airbyte up at http://localhost:8000"
	python airbyte/configure.py setup

airbyte-down:
	abctl local uninstall

# Runs the harness AND drives back-to-back syncs for the duration (Airbyte is batch,
# so its best case is continuous syncs). SYNC loop runs in the background.
airbyte-bench: bench-build
	( end=$$(( $$(date +%s) + $(DURATION) )); while [ $$(date +%s) -lt $$end ]; do python airbyte/configure.py sync; done ) &
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool airbyte --rate $(RATE) --duration $(DURATION) --mix $(MIX) --seed-rows $(SEED_ROWS)
```

- [ ] **Step 3: Verify** (task test — expect batch latency, seconds+, not sub-second)
```bash
make airbyte-up          # heavy: pulls images, installs kind cluster (minutes)
# discover the two def IDs, patch airbyte/configure.py constants, re-run setup if needed
make airbyte-bench RATE=20 DURATION=120 MIX=100/0/0
# Expect: completeness ~100; p50 in the seconds+ range (batch), confirming the streaming-vs-batch gap.
```
If the MSSQL **destination** connector is unavailable/broken in the installed Airbyte version, STOP and report it as a finding (it's part of the ease-of-use verdict) — do not spend more than ~45 min fighting it; note the exact error in the README and results.

- [ ] **Step 4: Commit**
```bash
git add -A && git commit -m "feat: airbyte pipeline (pg cdc -> airbyte -> mssql) via abctl + api config"
```

---

### Task 6: Report, recommendation, README

**Files:**
- Create: `README.md`; modify `Makefile` (add `report`, `demo`)

**Interfaces:**
- Consumes: per-run JSONs in `results/`, `report.render_table` (Task 1).

- [ ] **Step 1: `make report` + `make demo` targets**
```makefile
.PHONY: report demo
report: bench-build
	@docker run --rm -v $$PWD/results:/app/results -v $$PWD/bench:/app/bench \
	  --entrypoint python cdc-bench -c "import json,glob,os; \
from bench.report import render_table; \
latest={}; \
[latest.__setitem__(json.load(open(f)).get('tool','?'), json.load(open(f))) for f in sorted(glob.glob('results/*.json'))]; \
print(render_table(latest))"

demo:
	$(MAKE) debezium-up
	$(MAKE) debezium-bench RATE=50 DURATION=20 MIX=100/0/0
	$(MAKE) debezium-down
	$(MAKE) airbyte-up
	$(MAKE) airbyte-bench RATE=20 DURATION=60 MIX=100/0/0
	$(MAKE) report
```
(The `report` one-liner loads every results JSON keyed by tool, latest wins, and prints the table.)

- [ ] **Step 2: `README.md`** — quickstart + framing + interpretation
```markdown
# CDC Benchmark — Debezium vs Airbyte (Postgres → MSSQL)

Local, reproducible benchmark measuring replication latency of two CDC tools.

## Quick start
```bash
cp .env.example .env
make up                 # postgres + mssql
make demo               # short run of BOTH tools + prints the comparison table
```

## Full soak (tunable)
```bash
make debezium-up
make debezium-bench RATE=200 DURATION=3600 MIX=70/20/10
make debezium-down
make airbyte-up
make airbyte-bench RATE=200 DURATION=3600 MIX=70/20/10
make report
```

## What the numbers mean
- **p50/p95/p99/max latency** — time from a change in Postgres to its arrival in MSSQL (host-clock, no cross-DB skew). p95/p99/max = worst-case lag.
- **throughput** — rows/sec landing in MSSQL vs. the generated rate (does it keep up?).
- **completeness** — did every generated row arrive?

## Honest framing
Debezium **streams** the Postgres WAL (sub-second). Airbyte **batches** on a sync schedule and uses Debezium internally for capture, so it is structurally higher-latency. This benchmark compares **latency vs. ease-of-setup**, not streaming-vs-streaming.

## Commands
`make up | down | selftest | debezium-up/-bench/-down | airbyte-up/-bench/-down | report | demo | clean`
```

- [ ] **Step 3: Run the full demo and capture real numbers**
```bash
make clean && make up && make demo
# Expect a printed table with real Debezium (sub-second) vs Airbyte (seconds+) numbers.
```

- [ ] **Step 4: Write the recommendation** — append a `## Recommendation` section to `README.md` filled from the demo's real numbers: which tool, the measured latency gap, the setup-effort observation (Debezium = Kafka+2 connectors; Airbyte = heavier install but API-configurable), and the "which to pick for which need" call.

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "feat: report table, demo target, README with framing + recommendation"
```

---

## Self-Review (plan author)

- **Spec coverage:** §1 requirements → all tasks; §3 rig → Task 2; §4 loadgen → Task 3; §5 measurement → Tasks 1+3; §6 make surface → Tasks 2–6; §7 deliverables → Task 6; §2 framing → README (Task 6). Debezium §3 → Task 4; Airbyte §3 → Task 5. All covered.
- **Placeholder scan:** the two Airbyte connector-definition UUIDs and the exact connector download versions are the only externally-sourced values; each has an explicit discover/verify step, not a "TODO." No vague error-handling placeholders.
- **Type consistency:** `parse_mix`/`summarize`/`render_table` signatures match across Tasks 1, 3, 6; `written_at` dict flows loadgen→measure; results JSON schema (`summarize()` dict + `tool`) is what `report` reads. Consistent.
- **Known risk (documented in-task):** Task 4 sink config and Task 5 Airbyte install/connector are the fragile spots — each has a "if it fails, here's where to look / when to stop and report" instruction.
