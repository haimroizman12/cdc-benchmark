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


def test_parse_mix_negative():
    with pytest.raises(ValueError):
        metrics.parse_mix("120/-10/-10")


def test_percentile_nearest_rank():
    vals = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    assert metrics.percentile(vals, 50) == 60  # nearest-rank of 10 items
    assert metrics.percentile(vals, 100) == 100
    assert metrics.percentile(vals, 0) == 10


def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        metrics.percentile([], 50)


def test_summarize():
    s = metrics.summarize([10.0, 20.0, 30.0], generated=4, arrived=3, duration_s=3.0)
    assert s["count"] == 3
    assert s["max_ms"] == 30.0
    assert s["arrived"] == 3
    assert s["generated"] == 4
    assert s["completeness_pct"] == 75.0
    assert s["throughput_rows_per_s"] == 1.0
