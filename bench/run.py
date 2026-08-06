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
            (seq, int(written_at * 1_000_000), payload),  # store epoch micros (BIGINT column)
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
            (seq, seq, int(written_at * 1_000_000), payload),  # epoch micros (BIGINT column)
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
    args = ap.parse_args()

    target = db.mssql_connect()
    _reset_target(target)
    fetch = _mssql_fetch(target)

    if args.tool == "selftest":
        writers = _mssql_writers(db.mssql_connect())
        if args.seed_rows:
            print(f"seeding {args.seed_rows} baseline rows straight into MSSQL...")
            _seed_target_mssql(target, args.seed_rows)
    else:
        source = db.pg_connect()
        _reset_source(source)
        if args.seed_rows:
            print(f"seeding {args.seed_rows} baseline rows into the Postgres source...")
            _seed_source_pg(source, args.seed_rows)
        writers = _pg_writers(source)

    gen = loadgen.LoadGen(*writers, mix=args.mix, rate=args.rate)
    mea = measure.Measurer(fetch, gen.written_at)
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
