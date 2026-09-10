from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest

from tests.support_channels import FakeProviderClient
from trpc_service.audit.models import TenantScope
from trpc_service.channels.contracts import Channel, DeliveryAction, OutboundReply, ReplyStatus, VerifiedBindingScope
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.identity import ProviderReplyContext, RuntimeBotIdentity
from trpc_service.storage.models import AdapterFence


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_success_is_sent_once_and_duplicate_reply_is_suppressed() -> None:
    provider = FakeProviderClient(RuntimeBotIdentity(channel=Channel.FEISHU, sender_type="bot", sender_id="bot", channel_identity_digest="a" * 64, authenticated_at=NOW))
    service = DeliveryService(InMemoryDeliveryRepository(now=lambda: NOW), now=lambda: NOW)
    scope = VerifiedBindingScope._issue(binding_id="binding-feishu", channel=Channel.FEISHU)
    tenant_scope = TenantScope(tenant_id="tenant-alpha")
    context = ProviderReplyContext(channel=Channel.FEISHU, conversation_type="direct", reply_target_id="original-chat", provider_message_id="original-message")
    fence = AdapterFence(identity_digest="a" * 64, node_id="adapter-node", generation=1, owner_token="owner-" + "token-123456", expires_at=NOW)
    success = OutboundReply(status=ReplyStatus.SUCCEEDED, trace_id=UUID(int=26), tenant_id="tenant-alpha", platform_session_id="sess_" + "a" * 64, external_message_id="original-message", text="reply once", delivery_action=DeliveryAction.DELIVER)
    duplicate = OutboundReply(status=ReplyStatus.DUPLICATE, trace_id=UUID(int=27), original_trace_id=UUID(int=26), tenant_id="tenant-alpha", platform_session_id="sess_" + "a" * 64, external_message_id="original-message", text="reply once", delivery_action=DeliveryAction.SUPPRESS)
    first = await service.deliver_reply(reply=success, tenant_scope=tenant_scope, binding_scope=scope, reply_context=context, provider=provider, adapter_fence=fence)
    second = await service.deliver_reply(reply=duplicate, tenant_scope=tenant_scope, binding_scope=scope, reply_context=context, provider=provider, adapter_fence=fence)
    assert first.status == "delivered"
    assert second.status == "suppressed"
    assert len(provider.sent) == 1
