"""Render the tool-vs-tool comparison table from run summaries. Pure, no I/O.

Consumes the summary dicts produced by metrics.summarize() (plus a `tool` key);
produces a Markdown table. Composes with run.py's JSON output and the
`make report` target.
"""

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
    """`runs` maps tool name -> its summary dict. Returns a Markdown table."""
    tools = list(runs.keys())
    lines = [
        "| Metric | " + " | ".join(tools) + " |",
        "|" + "---|" * (len(tools) + 1),
    ]
    for label, key in _ROWS:
        cells = " | ".join(str(runs[t].get(key, "—")) for t in tools)
        lines.append(f"| {label} | {cells} |")
    return "\n".join(lines)
