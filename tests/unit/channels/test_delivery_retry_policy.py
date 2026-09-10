from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest

from tests.support_channels import FakeProviderClient, VirtualClock
from trpc_service.audit.models import TenantScope
from trpc_service.channels.base import (
    ProviderOutcomeUnknown,
    ProviderPermanentError,
    ProviderTransientError,
)
from trpc_service.channels.contracts import (
    Channel,
    DeliveryAction,
    OutboundReply,
    ReplyStatus,
    VerifiedBindingScope,
)
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.identity import ProviderReplyContext, RuntimeBotIdentity
from trpc_service.storage.models import AdapterFence


NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def _input(clock: VirtualClock, outcomes):
    provider = FakeProviderClient(
        RuntimeBotIdentity(
            channel=Channel.FEISHU,
            sender_type="bot",
            sender_id="bot",
            channel_identity_digest="a" * 64,
            authenticated_at=NOW,
        ),
        send_results=list(outcomes),
    )
    service = DeliveryService(
        InMemoryDeliveryRepository(now=clock.now),
        now=clock.now,
        sleep=clock.sleep,
    )
    return provider, service, dict(
        reply=OutboundReply(
            status=ReplyStatus.SUCCEEDED,
            trace_id=UUID(int=1),
            tenant_id="tenant-alpha",
            platform_session_id="sess_" + "a" * 64,
            external_message_id="message-1",
            text="reply",
            delivery_action=DeliveryAction.DELIVER,
        ),
        tenant_scope=TenantScope(tenant_id="tenant-alpha"),
        binding_scope=VerifiedBindingScope._issue(
            binding_id="binding-alpha", channel=Channel.FEISHU
        ),
        reply_context=ProviderReplyContext(
            channel=Channel.FEISHU,
            conversation_type="direct",
            reply_target_id="chat",
            provider_message_id="message-1",
        ),
        provider=provider,
        adapter_fence=AdapterFence(
            identity_digest="a" * 64,
            node_id="adapter-a",
            generation=1,
            owner_token="owner-" + "token-123456",
            expires_at=NOW,
        ),
    )


@pytest.mark.asyncio
async def test_transient_retries_at_one_two_four_seconds_and_stops_at_four_attempts() -> None:
    clock = VirtualClock()
    provider, service, kwargs = _input(
        clock, [ProviderTransientError()] * 4
    )
    result = await service.deliver_reply(**kwargs)
    assert result.status == "delivery_failed"
    assert result.attempt_no == 4
    assert clock.sleeps == [1, 2, 4]
    assert len(provider.sent) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ProviderPermanentError(), "delivery_failed"),
        (ProviderOutcomeUnknown(), "delivery_unknown"),
        (RuntimeError("vendor data must not leak"), "delivery_unknown"),
    ],
)
async def test_permanent_and_unknown_failures_are_never_retried(error, status) -> None:
    clock = VirtualClock()
    provider, service, kwargs = _input(clock, [error])
    result = await service.deliver_reply(**kwargs)
    assert result.status == status
    assert result.attempt_no == 1
    assert clock.sleeps == []
    assert len(provider.sent) == 1
    assert "vendor data" not in repr(result)
