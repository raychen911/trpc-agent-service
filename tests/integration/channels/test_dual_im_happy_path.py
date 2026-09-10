from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import FakeProviderClient, feishu_text_event, wecom_text_event
from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.config.settings import PlatformSettings
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.storage.contracts import ResolvedChannelBinding, SecretBytes
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.tenant.models import AgentApplication, ChannelBinding, ResourceStatus, Tenant
from trpc_service.worker.service import AgentExecutor


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


def _settings(identities: dict[Channel, ChannelIdentity]) -> PlatformSettings:
    tenant = Tenant(tenant_id="tenant-alpha", display_name="Alpha", status=ResourceStatus.ACTIVE, created_at=NOW, config_version=1)
    agent = AgentApplication(tenant_id="tenant-alpha", agent_id="agent-alpha", agent_name="Alpha Agent", status=ResourceStatus.ACTIVE, model_profile="deterministic-offline", instruction="deterministic", config_version=1)
    bindings = tuple(ChannelBinding(binding_id=f"binding-{channel.value}", tenant_id="tenant-alpha", agent_id="agent-alpha", channel=channel, status=ResourceStatus.ACTIVE, secret_ref=f"{channel.value.upper()}_SECRET", signature_version="v1", provider_tenant_key=identity.provider_tenant_key, provider_app_or_bot_id=identity.provider_app_or_bot_id, channel_identity_digest=identity.identity_digest, created_at=NOW) for channel, identity in identities.items())
    return PlatformSettings(tenants=(tenant,), agents=(agent,), bindings=bindings)


class Resolver:
    def __init__(self, adapters, identities):
        self.adapters, self.identities = adapters, identities

    async def resolve_by_channel_identity(self, identity, *, external_user_id, trace_id):
        assert identity == self.identities[identity.channel]
        scope = VerifiedBindingScope._issue(binding_id=f"binding-{identity.channel.value}", channel=identity.channel)
        context = await self.adapters.resolve_active_context(scope, external_user_id=external_user_id, trace_id=trace_id)
        return ResolvedChannelBinding(scope=scope, context=context, secret_ref=f"{identity.channel.value.upper()}_SECRET", config_version=1)


@pytest.mark.asyncio
async def test_both_im_adapters_reach_existing_gateway_worker_and_official_runner() -> None:
    identities = {channel: ChannelIdentity(channel=channel, provider_tenant_key=f"tenant-{channel.value}", provider_app_or_bot_id=f"bot-{channel.value}") for channel in (Channel.FEISHU, Channel.WECOM)}
    platform = InMemoryPlatformAdapters(_settings(identities))
    worker = AgentExecutor(SessionBackendFactory())
    gateway = GatewayService(platform, InMemoryMetricsRecorder(), worker, now=lambda: NOW)
    service = ChannelMessageService(Resolver(platform, identities), gateway, DeliveryService(InMemoryDeliveryRepository(now=lambda: NOW), now=lambda: NOW))
    adapters, providers = [], []
    try:
        for channel, adapter_type, event_factory in ((Channel.FEISHU, FeishuChannelAdapter, feishu_text_event), (Channel.WECOM, WeComChannelAdapter, wecom_text_event)):
            identity = identities[channel]
            provider = FakeProviderClient(RuntimeBotIdentity(channel=channel, sender_type="bot", sender_id=f"runtime-{channel.value}", channel_identity_digest=identity.identity_digest, authenticated_at=NOW))
            adapter = adapter_type(provider=provider, credential_secret=SecretBytes(b"credential-placeholder"), message_service=service, now=lambda: NOW)
            await adapter.start(identity, NodeIdentity(node_id=f"node-{channel.value}"))
            event = event_factory(text="记住验证码 ALPHA") if channel == Channel.FEISHU else event_factory(body={**event_factory()["body"], "text": {"content": "记住验证码 BETA"}})
            result = await adapter.handle_provider_event(event)
            assert result.safe_code == "reply_delivered"
            assert provider.sent and provider.sent[0][0].reply_target_id.endswith("chat-001")
            adapters.append(adapter)
            providers.append(provider)
        assert worker.call_count == 2
        assert all(provider.sent[0][1].strip() for provider in providers)
    finally:
        for adapter in adapters:
            await adapter.stop("test_complete")
        await worker.close()
