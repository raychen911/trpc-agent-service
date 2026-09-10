from __future__ import annotations

from uuid import uuid4

import pytest

from trpc_service.audit.models import PreAuthScope, TenantScope
from trpc_service.metrics.contracts import MetricsUnavailable
from trpc_service.storage.contracts import AccessDenied, InvalidRequest
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder


def test_metrics_are_scoped_offline_and_failure_is_operational_only() -> None:
    metrics = InMemoryMetricsRecorder()
    alpha, beta = TenantScope(tenant_id="tenant-alpha"), TenantScope(tenant_id="tenant-beta")
    trace = uuid4()
    metrics.record(alpha, trace_id=trace, stage="request", outcome="success", duration_ms=2)
    metrics.record(alpha, trace_id=trace, stage="agent", outcome="success", duration_ms=3)
    assert metrics.snapshot(alpha).request_count == 1
    assert metrics.snapshot(alpha).agent_latency_ms == 3
    assert metrics.snapshot(beta).request_count == 0
    assert metrics.snapshot(PreAuthScope()).model_metric_status == "not_applicable"
    metrics.record(alpha, trace_id=trace, stage="request", outcome="error", duration_ms=1)
    metrics.record(alpha, trace_id=trace, stage="agent", outcome="error", duration_ms=1)
    assert metrics.snapshot(alpha).error_count == 1
    with pytest.raises(AccessDenied):
        metrics.snapshot(object())
    with pytest.raises(InvalidRequest):
        metrics.record(alpha, trace_id=trace, stage="request", outcome="success", duration_ms=1, values={"raw_text": 1})
    metrics.fail_record = True
    with pytest.raises(MetricsUnavailable):
        metrics.record(alpha, trace_id=trace, stage="request", outcome="error", duration_ms=1)
    assert metrics.operational_events == [{"event": "metrics_incomplete", "trace_id": str(trace)}]
