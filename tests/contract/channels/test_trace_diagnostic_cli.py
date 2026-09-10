from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from trpc_service._cli import build_trace_diagnostic_summary
from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.channels.contracts import Channel


def test_trace_diagnostic_summary_only_contains_pseudonymous_fields() -> None:
    trace_id = UUID(int=75)
    record = AuditRecord(
        audit_id=UUID(int=76),
        trace_id=trace_id,
        first_claim_trace_id=trace_id,
        execution_trace_id=trace_id,
        tenant_id="tenant-alpha",
        channel=Channel.FEISHU,
        binding_id_digest="sha256:" + "a" * 64,
        user_id="sha256:" + "b" * 64,
        session_id="sess_" + "c" * 64,
        decision=AuditDecision.DELIVERED,
        latency_ms=1,
        cost=Decimal("0"),
        external_message_digest="sha256:" + "d" * 64,
        adapter_node_id="adapter-a",
        adapter_generation=2,
        delivery_id=UUID(int=77),
        delivery_attempt_no=1,
        delivery_status="delivered",
        created_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
    )

    summary = build_trace_diagnostic_summary(
        TenantScope(tenant_id="tenant-alpha"), trace_id, [record]
    )
    serialized = json.dumps(summary, sort_keys=True)

    assert summary["tenant_digest"].startswith("sha256:")
    assert summary["trace_id"] == str(trace_id)
    assert summary["record_count"] == 1
    assert summary["records"][0]["decision"] == "delivered"
    assert "tenant-alpha" not in serialized
    assert "user_id" not in serialized
    assert "external_message_digest" not in serialized
