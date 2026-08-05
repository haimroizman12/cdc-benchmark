from bench import report


def test_render_table_two_tools():
    runs = {
        "debezium": {
            "p50_ms": 120, "p95_ms": 300, "p99_ms": 500, "max_ms": 800,
            "throughput_rows_per_s": 200, "completeness_pct": 100.0,
        },
        "airbyte": {
            "p50_ms": 45000, "p95_ms": 70000, "p99_ms": 88000, "max_ms": 90000,
            "throughput_rows_per_s": 200, "completeness_pct": 100.0,
        },
    }
    out = report.render_table(runs)
    assert "| Metric | debezium | airbyte |" in out
    assert "p50 latency (ms)" in out
    assert "120" in out and "45000" in out


def test_render_table_missing_key_shows_dash():
    out = report.render_table({"debezium": {"p50_ms": 120}})
    assert "—" in out  # missing metrics render as an em dash
