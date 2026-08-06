from __future__ import annotations
import argparse
import csv
import json
import os
import pathlib
import threading
import time

from bench import db, loadgen, measure, metrics


def _batched_writers(conn, written_at, batch, ins_sql, upd_sql, del_sql, ins_row):
    """Buffer ops and commit once per `batch` (batch=1 == commit per op). written_at
    for each inserted seq is stamped at COMMIT time — a row is only visible to CDC
    after its transaction commits, so commit-time is the honest T0 for latency."""
    cur = conn.cursor()
    pending: list[int] = []   # inserted seqs awaiting a commit stamp
    n = [0]                   # ops accumulated in the current transaction

    def _flush(force: bool = False) -> None:
        if n[0] >= batch or (force and n[0] > 0):
            # Stamp written_at just BEFORE commit so a row's timestamp is always set
            # before it can become visible to a reader (avoids a race where a poller on
            # the same DB — selftest — reads a committed row before it's stamped and
            # drops it). Commit takes ~ms, so this is commit-time within noise.
            t = time.time()
            for s in pending:
                written_at[s] = t
            conn.commit()
            pending.clear()
            n[0] = 0

    def ins(seq, payload):
        cur.execute(ins_sql, ins_row(seq, payload))
        pending.append(seq)
        n[0] += 1
        _flush()

    def upd(seq):
        cur.execute(upd_sql, (seq,))
        n[0] += 1
        _flush()

    def dele(seq):
        cur.execute(del_sql, (seq,))
        n[0] += 1
        _flush()

    def flush():
        _flush(force=True)

    return ins, upd, dele, flush


def _pg_writers(conn, written_at, batch):
    return _batched_writers(
        conn, written_at, batch,
        "INSERT INTO source_events (seq, written_at, payload) VALUES (%s,%s,%s)",
        "UPDATE source_events SET payload = payload || '+' WHERE seq = %s",
        "DELETE FROM source_events WHERE seq = %s",
        lambda seq, payload: (seq, int(time.time() * 1_000_000), payload),
    )


def _mssql_writers(conn, written_at, batch):
    return _batched_writers(
        conn, written_at, batch,
        "INSERT INTO dbo.source_events (id, seq, written_at, payload) VALUES (%s,%s,%s,%s)",
        "UPDATE dbo.source_events SET payload = payload + '+' WHERE seq = %s",
        "DELETE FROM dbo.source_events WHERE seq = %s",
        lambda seq, payload: (seq, seq, int(time.time() * 1_000_000), payload),
    )


def _mssql_fetch(conn):
    cur = conn.cursor()

    def fetch(since_seq):
        cur.execute("SELECT seq FROM dbo.source_events WHERE seq > %s ORDER BY seq", (since_seq,))
        return [r[0] for r in cur.fetchall()]

    return fetch


def _reset_target(conn) -> None:
    """Clear the MSSQL target so seq numbering starts clean and stale rows can't
    poison the poller's last_seq watermark."""
    cur = conn.cursor()
    cur.execute("DELETE FROM dbo.source_events")
    conn.commit()


def _reset_source(conn) -> None:
    """Clear the Postgres source so the harness's seq counter starts from 1."""
    cur = conn.cursor()
    cur.execute("TRUNCATE source_events RESTART IDENTITY")
    conn.commit()


def _seed_source_pg(conn, n: int) -> None:
    """Bulk-load n baseline rows into the Postgres source (seq 1..n) via COPY, so the
    timed load runs against a large PRE-EXISTING table. These rows are not measured."""
    import io

    now_us = int(time.time() * 1_000_000)
    buf = io.StringIO()
    for s in range(1, n + 1):
        buf.write(f"{s}\t{now_us}\tseed-{s}\n")
    buf.seek(0)
    cur = conn.cursor()
    cur.copy_from(buf, "source_events", columns=("seq", "written_at", "payload"))
    conn.commit()


def _seed_target_mssql(conn, n: int) -> None:
    """Bulk-load n baseline rows straight into the MSSQL target (selftest only)."""
    now_us = int(time.time() * 1_000_000)
    cur = conn.cursor()
    rows = [(s, s, now_us, f"seed-{s}") for s in range(1, n + 1)]
    cur.executemany(
        "INSERT INTO dbo.source_events (id, seq, written_at, payload) VALUES (%s,%s,%s,%s)",
        rows,
    )
    conn.commit()


def _target_count(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dbo.source_events")
    return cur.fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tool", required=True)  # debezium | airbyte | selftest
    ap.add_argument("--rate", type=int, default=100)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--seed-rows", type=int, default=0)
    ap.add_argument("--mix", default="70/20/10")
    ap.add_argument("--grace", type=int, default=30)  # extra seconds to let the tail arrive
    ap.add_argument("--batch", type=int, default=1)  # ops per commit (break the per-row-commit ceiling)
    args = ap.parse_args()

    target = db.mssql_connect()
    _reset_target(target)
    fetch = _mssql_fetch(target)
    written_at: dict[int, float] = {}  # seq -> commit-time epoch seconds; owned by the writers

    if args.tool == "selftest":
        writers = _mssql_writers(db.mssql_connect(), written_at, args.batch)
        if args.seed_rows:
            print(f"seeding {args.seed_rows} baseline rows straight into MSSQL...")
            _seed_target_mssql(target, args.seed_rows)
    else:
        source = db.pg_connect()
        _reset_source(source)
        if args.seed_rows:
            print(f"seeding {args.seed_rows} baseline rows into the Postgres source...")
            _seed_source_pg(source, args.seed_rows)
        writers = _pg_writers(source, written_at, args.batch)

    gen = loadgen.LoadGen(*writers, mix=args.mix, rate=args.rate)
    mea = measure.Measurer(fetch, written_at)
    if args.seed_rows:
        # Continue seq numbering above the baseline; let updates/deletes hit the big
        # existing set; make the poller ignore the baseline (it isn't timed).
        gen.seq = args.seed_rows
        gen.live_ids = list(range(1, args.seed_rows + 1))
        mea.last_seq = args.seed_rows

    # For real CDC tools, wait until the baseline has fully landed in MSSQL before
    # timing the incremental load — so we measure "changes against a large EXISTING
    # table", not changes stuck behind the baseline's replication backlog.
    if args.seed_rows and args.tool != "selftest":
        print(f"waiting for the {args.seed_rows}-row baseline to replicate to MSSQL...")
        deadline = time.time() + 900
        while time.time() < deadline:
            c = _target_count(target)
            if c >= args.seed_rows:
                print(f"  baseline replicated ({c} rows present in target).")
                break
            time.sleep(3)
        else:
            print(f"  WARNING: baseline not fully replicated (target has {_target_count(target)}).")

    stop = threading.Event()

    def poll_loop():
        while not stop.is_set():
            mea.poll_once()
            time.sleep(mea.poll_s)

    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    start = time.time()
    generated = gen.run_for(args.duration)
    # Throughput is rows/s over the LOAD window only — the grace drain must not be in
    # the denominator, or tools run with different --grace/--duration get unequal
    # penalties and the number stops meaning "does it keep up with the offered rate".
    load_duration = time.time() - start
    # Stop the background poller and join before draining: the fetch connection is a
    # single pymssql connection and must never be touched by two threads at once.
    stop.set()
    t.join()
    mea.drain(time.time() + args.grace)   # catch the tail on the main thread

    lat = [ms for _, ms in mea.samples]
    # generated counts only the TIMED inserts (seq above the baseline), matching what
    # the poller measures — so completeness is timed-arrived / timed-generated.
    summary = metrics.summarize(
        lat, generated=gen.seq - args.seed_rows, arrived=len(mea.samples), duration_s=load_duration
    )
    summary["tool"] = args.tool
    summary["rate"] = args.rate
    summary["mix"] = args.mix
    summary["seed_rows"] = args.seed_rows
    summary["batch"] = args.batch

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
