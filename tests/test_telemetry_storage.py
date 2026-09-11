"""Capacity metrics expose percentiles without high-cardinality labels."""

import pytest

from trpc_service.telemetry import MetricsRegistry
from trpc_service.telemetry import REQUIRED_METRICS


def test_metrics_report_percentiles_and_reject_high_cardinality_labels() -> None:
    metrics = MetricsRegistry()
    for value in (1, 2, 3, 100):
        metrics.observe("worker.run_latency_ms", value, tenant="tenant_acme")
    metrics.increment("gateway.callback_total", tenant="tenant_acme")
    report = metrics.snapshot()
    latency = report["worker.run_latency_ms{tenant=tenant_acme}"]
    assert latency["count"] == 4
    assert latency["p50"] == 2
    assert latency["p99"] == 100
    with pytest.raises(ValueError, match="high-cardinality"):
        metrics.increment("worker.active_runs", session_id="ses-1")


def test_required_capacity_metric_catalog_covers_all_components() -> None:
    prefixes = {name.split(".", 1)[0] for name in REQUIRED_METRICS}
    assert {"gateway", "queue", "worker", "session", "memory", "redis", "sql", "vector",
            "object", "model", "tool", "outbox", "migration"} <= prefixes


def test_long_running_metrics_retain_bounded_samples_and_series():
    metrics = MetricsRegistry(max_samples=8, max_series=2)
    for index in range(1000):
        metrics.observe("latency", index)
        metrics.increment("calls")
    snapshot = metrics.snapshot()
    assert snapshot["latency"]["count"] == 1000
    assert snapshot["latency"]["retained_samples"] == 8
    assert snapshot["calls"]["sum"] == 1000
    assert snapshot["calls"]["retained_samples"] == 1
    with pytest.raises(ValueError, match="series limit"):
        metrics.observe("third", 1)
