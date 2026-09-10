from __future__ import annotations

from trpc_service.audit.models import TenantScope
from trpc_service.channels.contracts import Channel
from trpc_service.metrics.shared import SharedMetricsRecorder


def test_channel_metrics_use_only_bounded_low_cardinality_dimensions() -> None:
    metrics = SharedMetricsRecorder(node_id="adapter-a")
    metrics.observe_channel(
        TenantScope(tenant_id="tenant-alpha"),
        channel=Channel.FEISHU,
        stage="delivery",
        outcome="unknown",
        duration_ms=12.5,
        attempt_no=2,
        generation=4,
    )
    event = metrics.events[-1]
    assert event == {
        "node_id": "adapter-a",
        "tenant": event["tenant"],
        "channel": "feishu",
        "stage": "delivery",
        "outcome": "unknown",
        "duration_ms": 12.5,
        "attempt_no": 2,
        "generation": 4,
    }
    assert event["tenant"].startswith("sha256:")
    assert not {"user_id", "message_id", "session_id", "identity_digest"} & event.keys()
