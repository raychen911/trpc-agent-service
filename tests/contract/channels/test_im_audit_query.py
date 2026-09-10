from __future__ import annotations

from uuid import UUID

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.audit.models import AuditDecision, TenantScope
from trpc_service.channels.contracts import Channel


@pytest.mark.asyncio
async def test_tenant_trace_query_contains_adapter_binding_execution_and_delivery() -> None:
    harness = await TwoNodeIMHarness.create(Channel.FEISHU)
    event = harness.event(
        message_id="audit-query-message",
        conversation_id="audit-query-chat",
        sender_id="audit-query-user",
        text="Remember validation token AUDIT.",
    )
    try:
        outcome = await harness.adapters[0].handle_provider_event(event)
        trace_id = UUID(outcome.trace_id)
        records = await harness.platform.audit.list_by_trace(
            TenantScope(tenant_id="tenant-alpha"), trace_id
        )
        assert {record.decision for record in records} >= {
            AuditDecision.RECEIVED,
            AuditDecision.EXECUTION_STARTED,
            AuditDecision.DELIVERED,
        }
        assert all(record.binding_id_digest.startswith("sha256:") for record in records)
        assert await harness.platform.audit.list_by_trace(
            TenantScope(tenant_id="tenant-beta"), trace_id
        ) == []
        serialized = "".join(record.model_dump_json() for record in records)
        assert "audit-query-message" not in serialized
        assert "audit-query-user" not in serialized
    finally:
        await harness.close()
