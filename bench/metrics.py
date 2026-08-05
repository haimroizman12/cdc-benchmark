"""Pure measurement math for the CDC benchmark — no DB, no I/O.

Composes with run.py (feeds it latency samples) and report.py (consumes summaries).
"""

from __future__ import annotations

import math


def parse_mix(mix: str) -> tuple[int, int, int]:
    """Parse an insert/update/delete percentage string like "70/20/10"."""
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
    """Nearest-rank percentile of `values` (0-100)."""
    if not values:
        raise ValueError("no values")
    s = sorted(values)
    # Round-half-up explicitly: Python's built-in round() is banker's rounding
    # (round(4.5) == 4), which makes percentiles depend on parity — surprising
    # for a latency report. math.floor(x + 0.5) is deterministic round-half-up.
    k = math.floor(pct / 100 * (len(s) - 1) + 0.5)
    k = max(0, min(len(s) - 1, k))
    return s[k]


def summarize(
    latencies_ms: list[float], generated: int, arrived: int, duration_s: float
) -> dict:
    """Roll a run's latency samples + counts into the report summary dict."""
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
