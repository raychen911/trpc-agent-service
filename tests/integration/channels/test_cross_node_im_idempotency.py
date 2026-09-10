from __future__ import annotations

import asyncio

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.channels.contracts import Channel
from trpc_service.storage.models import IdempotencyKey


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [Channel.FEISHU, Channel.WECOM])
async def test_provider_message_replayed_ten_times_across_two_nodes_executes_agent_once(
    channel: Channel,
) -> None:
    harness = await TwoNodeIMHarness.create(channel)
    event = harness.event(
        message_id=f"{channel.value}-replayed-message",
        conversation_id=f"{channel.value}-conversation",
        sender_id=f"{channel.value}-sender",
        text="Remember validation token ALPHA.",
    )
    try:
        results = await asyncio.gather(
            *[
                harness.adapters[index % 2].handle_provider_event(event)
                for index in range(10)
            ]
        )
        assert harness.agent_calls == 1
        assert sum(len(provider.sent) for provider in harness.providers) == 1
        assert sum(result.safe_code == "reply_delivered" for result in results) == 1
        assert all(
            result.safe_code in {
                "reply_delivered",
                "reply_not_deliverable",
                "duplicate_suppressed",
            }
            for result in results
        )

        binding_id = (
            "binding-feishu-alpha"
            if channel == Channel.FEISHU
            else "binding-wecom-beta"
        )
        tenant_id = "tenant-alpha" if channel == Channel.FEISHU else "tenant-beta"
        record = await harness.platform.idempotency.get(
            IdempotencyKey(
                tenant_id=tenant_id,
                channel=channel,
                binding_id=binding_id,
                external_message_id=f"{channel.value}-replayed-message",
            )
        )
        assert record.first_claim_trace_id is not None
        # The active owner is deliberately cleared at terminal state, while
        # first-claim and durable execution traces remain queryable.
        assert record.owner_trace_id is None
        assert record.execution_trace_id == record.first_claim_trace_id
        assert record.result is not None
        assert record.result.original_trace_id == record.execution_trace_id
    finally:
        await harness.close()
