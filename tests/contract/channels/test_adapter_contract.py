from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import FakeProviderClient, feishu_text_event, wecom_text_event
from trpc_service.channels.base import AdapterReadiness
from trpc_service.channels.contracts import Channel, DeliveryAction, OutboundReply, ReplyStatus, VerifiedBindingScope
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.storage.contracts import ResolvedChannelBinding, SecretBytes
from trpc_service.storage.models import NodeIdentity
from trpc_service import _cli
from trpc_service.tenant.models import VerifiedTenantContext


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


class Resolver:
    async def resolve_by_channel_identity(self, identity, *, external_user_id, trace_id):
        scope = VerifiedBindingScope._issue(binding_id=f"binding-{identity.channel.value}", channel=identity.channel)
        return ResolvedChannelBinding(
            scope=scope,
            context=VerifiedTenantContext(
                tenant_id="tenant-alpha",
                agent_id="agent-alpha",
                agent_name="Alpha Agent",
                binding_id=scope.binding_id,
                channel=identity.channel,
                external_user_id=external_user_id,
                trace_id=trace_id,
                config_version=1,
            ),
            secret_ref="CHANNEL_SECRET",
            config_version=1,
        )


class Gateway:
    def __init__(self) -> None:
        self.messages = []

    async def handle_verified_message(self, scope, message):
        self.messages.append((scope, message))
        return OutboundReply(status=ReplyStatus.SUCCEEDED, trace_id=message.trace_id, tenant_id="tenant-alpha", platform_session_id="sess_" + "a" * 64, external_message_id=message.external_message_id, text="agent reply", delivery_action=DeliveryAction.DELIVER)


class Delivery:
    def __init__(self) -> None:
        self.calls = []

    async def deliver_reply(self, **kwargs):
        self.calls.append(kwargs)
        return "delivered"


@pytest.mark.parametrize(
    ("channel", "adapter_type", "event_factory", "sender_id"),
    [
        (Channel.FEISHU, FeishuChannelAdapter, feishu_text_event, "feishu-user-001"),
        (Channel.WECOM, WeComChannelAdapter, wecom_text_event, "wecom-user-001"),
    ],
)
@pytest.mark.asyncio
async def test_dual_adapters_share_lifecycle_receive_and_send_contract(channel, adapter_type, event_factory, sender_id) -> None:
    identity = ChannelIdentity(channel=channel, provider_tenant_key=f"tenant-{channel.value}", provider_app_or_bot_id=f"bot-{channel.value}")
    provider = FakeProviderClient(RuntimeBotIdentity(channel=channel, sender_type="bot", sender_id=f"runtime-bot-{channel.value}", channel_identity_digest=identity.identity_digest, authenticated_at=NOW))
    gateway = Gateway()
    delivery = Delivery()
    adapter = adapter_type(provider=provider, credential_secret=SecretBytes(b"credential-placeholder"), message_service=ChannelMessageService(Resolver(), gateway, delivery), now=lambda: NOW)
    assert adapter.readiness() == AdapterReadiness.NOT_READY
    assert await adapter.start(identity, NodeIdentity(node_id="adapter-node")) == AdapterReadiness.READY
    await provider.emit(event_factory())
    assert len(gateway.messages) == 1
    assert gateway.messages[0][1].external_user_id == sender_id
    assert len(delivery.calls) == 1
    assert provider.sent == []
    await adapter.stop("test_complete")
    assert adapter.readiness() == AdapterReadiness.NOT_READY
    assert provider.close_count == 1


def test_channel_serve_cli_dispatches_only_safe_channel_and_node(monkeypatch) -> None:
    captured = {}

    async def fake_run(channel, node, environ):
        captured.update(channel=channel, node=node, environ=environ)
        return 0

    monkeypatch.setattr("trpc_service.channels.runtime.run_channel_process", fake_run)
    assert _cli.channel_serve_main(["--channel", "feishu", "--node-id", "adapter-a"]) == 0
    assert captured["channel"] == Channel.FEISHU
    assert captured["node"] == NodeIdentity(node_id="adapter-a")
