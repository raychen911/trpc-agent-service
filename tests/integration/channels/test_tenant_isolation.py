from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import FakeProviderClient, dual_im_settings, feishu_text_event, wecom_text_event
from trpc_service.audit.models import AuditDecision, TenantScope
from trpc_service.channels.contracts import Channel
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.worker.service import AgentExecutor


NOW = datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_forged_tenant_is_ignored_and_equal_external_ids_do_not_collide_across_channels() -> None:
    settings, identities = dual_im_settings()
    platform = InMemoryPlatformAdapters(settings)
    worker = AgentExecutor(SessionBackendFactory())
    gateway = GatewayService(platform, InMemoryMetricsRecorder(), worker, now=lambda: NOW)
    service = ChannelMessageService(platform, gateway, DeliveryService(InMemoryDeliveryRepository(now=lambda: NOW), now=lambda: NOW), preauth_audit=platform.audit, now=lambda: NOW)
    adapters = []
    try:
        for channel, adapter_type, event in (
            (
                Channel.FEISHU,
                FeishuChannelAdapter,
                feishu_text_event(
                    message_id="same-message", chat_id="same-chat", tenant_id="tenant-beta",
                    sender={"sender_type": "user", "sender_id": {"open_id": "same-user"}},
                ),
            ),
            (
                Channel.WECOM,
                WeComChannelAdapter,
                wecom_text_event(
                    tenant_id="tenant-alpha",
                    body={
                        **wecom_text_event()["body"],
                        "msgid": "same-message", "chatid": "same-chat",
                        "from": {"userid": "same-user", "type": "user"},
                    },
                ),
            ),
        ):
            identity = identities[channel]
            provider = FakeProviderClient(RuntimeBotIdentity(channel=channel, sender_type="bot", sender_id=identity.provider_app_or_bot_id, channel_identity_digest=identity.identity_digest, authenticated_at=NOW))
            adapter = adapter_type(provider=provider, credential_secret=SecretBytes(b"credential-placeholder"), message_service=service, now=lambda: NOW)
            await adapter.start(identity, NodeIdentity(node_id=f"node-{channel.value}"))
            result = await adapter.handle_provider_event(event)
            assert result.safe_code == "reply_delivered"
            adapters.append(adapter)

        assert worker.call_count == 2
        alpha = await platform.audit.list_by_tenant(TenantScope(tenant_id="tenant-alpha"))
        beta = await platform.audit.list_by_tenant(TenantScope(tenant_id="tenant-beta"))
        alpha_sessions = {record.session_id for record in alpha if record.decision == AuditDecision.SUCCEEDED}
        beta_sessions = {record.session_id for record in beta if record.decision == AuditDecision.SUCCEEDED}
        assert alpha_sessions and beta_sessions and alpha_sessions.isdisjoint(beta_sessions)
        assert all(record.tenant_id == "tenant-alpha" for record in alpha)
        assert all(record.tenant_id == "tenant-beta" for record in beta)
        assert len(platform.idempotency._records) == 2
    finally:
        for adapter in adapters:
            await adapter.stop("test_complete")
        await worker.close()
