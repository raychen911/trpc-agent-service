from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

import pytest

from trpc_service.audit.models import TenantScope
from trpc_service.channels.contracts import Channel, DeliveryAction, VerifiedBindingScope
from trpc_service.channels.delivery import InMemoryDeliveryRepository
from trpc_service.channels.identity import ProviderReplyContext
from trpc_service.storage.contracts import ConditionalWriteFailed
from trpc_service.storage.models import (
    AdapterFence,
    DeliveryOutcome,
    DeliveryStatus,
    ExecutionResult,
    ExecutionStatus,
)


NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def _values():
    scope = TenantScope(tenant_id="tenant-alpha")
    binding = VerifiedBindingScope._issue(
        binding_id="binding-alpha", channel=Channel.FEISHU
    )
    result = ExecutionResult(
        status=ExecutionStatus.SUCCEEDED,
        response_text="reply",
        original_trace_id=UUID(int=1),
        platform_session_id="sess_" + "a" * 64,
        started_at=NOW,
        finished_at=NOW,
        agent_event_count=1,
        final_response_count=1,
        delivery_action=DeliveryAction.DELIVER,
    )
    context = ProviderReplyContext(
        channel=Channel.FEISHU,
        conversation_type="direct",
        reply_target_id="chat",
        provider_message_id="message",
    )
    fence = AdapterFence(
        identity_digest="a" * 64,
        node_id="node-a",
        generation=1,
        owner_token="owner-" + "token-123456",
        expires_at=NOW,
    )
    return scope, binding, result, context, fence


@pytest.mark.asyncio
async def test_create_or_get_is_idempotent_and_concurrent_begin_has_one_winner() -> None:
    repository = InMemoryDeliveryRepository(now=lambda: NOW)
    values = _values()
    first, second = await asyncio.gather(
        repository.create_or_get(*values),
        repository.create_or_get(*values),
    )
    assert first.delivery_id == second.delivery_id

    async def begin():
        try:
            return await repository.begin_attempt(
                values[0], first.delivery_id, DeliveryStatus.PENDING, values[4], UUID(int=2)
            )
        except ConditionalWriteFailed:
            return None

    attempts = await asyncio.gather(begin(), begin())
    assert sum(item is not None for item in attempts) == 1
    attempt = next(item for item in attempts if item is not None)
    terminal = await repository.finish_attempt(
        values[0], attempt.attempt_id, DeliveryOutcome.UNKNOWN,
        "delivery_outcome_unknown", None, values[4]
    )
    assert terminal.status == DeliveryStatus.DELIVERY_UNKNOWN
    assert await repository.list_due(values[0], NOW, 10) == []
    with pytest.raises(ConditionalWriteFailed):
        await repository.begin_attempt(
            values[0], terminal.delivery_id, terminal.status, values[4], UUID(int=3)
        )
