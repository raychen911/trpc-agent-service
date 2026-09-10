from uuid import uuid4

from trpc_service.audit.models import TenantScope
from trpc_service.metrics.shared import SharedMetricsRecorder


def test_shared_metrics_use_anonymous_tenant_and_bounded_dimensions() -> None:
    recorder = SharedMetricsRecorder(node_id="node-a")
    recorder.observe_lease(TenantScope(tenant_id="tenant-alpha"), backend="redis",
                           session_id="sess_" + "a" * 64, outcome="stale_write", wait_ms=12)
    event = recorder.events[0]
    assert event["node_id"] == "node-a" and event["backend"] == "redis"
    assert event["tenant"] != "tenant-alpha" and event["session"] != "sess_" + "a" * 64
    assert set(event) == {"node_id", "tenant", "backend", "session", "outcome", "wait_ms"}


def test_shared_trace_metrics_carry_trace_roles_and_generation_without_raw_tenant() -> None:
    recorder = SharedMetricsRecorder(node_id="node-b")
    recorder.observe_trace(
        TenantScope(tenant_id="tenant-alpha"), backend="redis", outcome="takeover",
        first_trace=str(uuid4()), owner_trace=str(uuid4()),
        execution_trace=str(uuid4()), generation=2,
    )
    event = recorder.events[0]
    assert event["node_id"] == "node-b" and event["backend"] == "redis"
    assert event["tenant"] != "tenant-alpha" and event["generation"] == 2
    assert set(event) == {
        "node_id", "tenant", "backend", "outcome", "first_trace",
        "owner_trace", "execution_trace", "generation",
    }
