from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import FakeProviderClient, dual_im_settings, feishu_text_event
from trpc_service.channels.contracts import Channel
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.storage.contracts import ConfigurationUnavailable, SecretBytes
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import NodeIdentity
from trpc_service.tenant.models import ResourceStatus


NOW = datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc)


class NeverGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def handle_verified_message(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("rejected binding must not reach Agent")


class NeverDelivery:
    async def deliver_reply(self, **kwargs):
        raise AssertionError("rejected binding must not send")


async def _attempt(repository, identity: ChannelIdentity) -> tuple[str, int]:
    gateway = NeverGateway()
    provider = FakeProviderClient(RuntimeBotIdentity(channel=Channel.FEISHU, sender_type="bot", sender_id="runtime-bot", channel_identity_digest=identity.identity_digest, authenticated_at=NOW))
    adapter = FeishuChannelAdapter(
        provider=provider,
        credential_secret=SecretBytes(b"credential-placeholder"),
        message_service=ChannelMessageService(repository, gateway, NeverDelivery(), now=lambda: NOW),
        now=lambda: NOW,
    )
    await adapter.start(identity, NodeIdentity(node_id="adapter-node"))
    result = await adapter.handle_provider_event(feishu_text_event())
    return result.safe_code, gateway.calls


@pytest.mark.parametrize(
    ("binding_status", "tenant_status", "agent_status"),
    [
        (ResourceStatus.DISABLED, ResourceStatus.ACTIVE, ResourceStatus.ACTIVE),
        (ResourceStatus.ACTIVE, ResourceStatus.DISABLED, ResourceStatus.ACTIVE),
        (ResourceStatus.ACTIVE, ResourceStatus.ACTIVE, ResourceStatus.DISABLED),
    ],
)
@pytest.mark.asyncio
async def test_disabled_binding_tenant_or_agent_fails_closed(binding_status, tenant_status, agent_status) -> None:
    settings, identities = dual_im_settings(
        feishu_binding_status=binding_status,
        feishu_tenant_status=tenant_status,
        feishu_agent_status=agent_status,
    )
    assert await _attempt(InMemoryPlatformAdapters(settings), identities[Channel.FEISHU]) == ("binding_rejected", 0)


@pytest.mark.asyncio
async def test_unknown_identity_and_unavailable_configuration_fail_closed() -> None:
    settings, _ = dual_im_settings()
    unknown = ChannelIdentity(channel=Channel.FEISHU, provider_tenant_key="unknown-tenant", provider_app_or_bot_id="unknown-bot")
    assert await _attempt(InMemoryPlatformAdapters(settings), unknown) == ("binding_rejected", 0)

    class Unavailable:
        async def resolve_by_channel_identity(self, *args, **kwargs):
            raise ConfigurationUnavailable()

    assert await _attempt(Unavailable(), unknown) == ("binding_rejected", 0)
