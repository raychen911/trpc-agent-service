from __future__ import annotations

from uuid import UUID

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.audit.models import AuditDecision, TenantScope
from trpc_service.channels.contracts import Channel
from trpc_service.channels.delivery import InMemoryDeliveryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [Channel.FEISHU, Channel.WECOM])
async def test_im_trace_links_adapter_gateway_execution_and_delivery(channel) -> None:
    harness = await TwoNodeIMHarness.create(channel)
    event = harness.event(
        message_id=f"{channel.value}-trace-message",
        conversation_id=f"{channel.value}-trace-chat",
        sender_id=f"{channel.value}-trace-user",
        text="Remember validation token TRACE.",
    )
    tenant_id = "tenant-alpha" if channel == Channel.FEISHU else "tenant-beta"
    try:
        outcome = await harness.adapters[0].handle_provider_event(event)
        trace_id = UUID(outcome.trace_id)
        records = await harness.platform.audit.list_by_trace(
            TenantScope(tenant_id=tenant_id), trace_id
        )
        decisions = {record.decision for record in records}
        assert {
            AuditDecision.RECEIVED,
            AuditDecision.AUTHORIZED,
            AuditDecision.EXECUTION_STARTED,
            AuditDecision.SUCCEEDED,
            AuditDecision.DELIVERED,
        } <= decisions
        assert all(record.channel == channel for record in records)
        assert any(
            record.adapter_node_id and record.adapter_generation == 1
            for record in records
        )
        final = next(record for record in records if record.decision == AuditDecision.SUCCEEDED)
        delivery = next(record for record in records if record.decision == AuditDecision.DELIVERED)
        assert final.first_claim_trace_id == trace_id
        assert final.execution_trace_id == trace_id
        assert delivery.execution_trace_id == trace_id
        assert delivery.delivery_id is not None
        assert delivery.delivery_status == "delivered"
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_late_ack_after_fence_loss_is_diagnostic_and_never_resent() -> None:
    validations = iter((True, True, False))
    repository = InMemoryDeliveryRepository(
        fence_validator=lambda _fence: next(validations, False)
    )
    harness = await TwoNodeIMHarness.create(
        Channel.FEISHU, delivery_repository=repository
    )
    event = harness.event(
        message_id="late-ack-message",
        conversation_id="late-ack-chat",
        sender_id="late-ack-user",
        text="Remember validation token LATEACK.",
    )
    try:
        outcome = await harness.adapters[0].handle_provider_event(event)
        records = await harness.platform.audit.list_by_trace(
            TenantScope(tenant_id="tenant-alpha"), UUID(outcome.trace_id)
        )

        assert outcome.safe_code == "reply_stale_fence"
        assert AuditDecision.STALE_ADAPTER_REJECTED in {
            record.decision for record in records
        }
        assert len(harness.providers[0].sent) == 1
        assert harness.agent_calls == 1
    finally:
        await harness.close()
