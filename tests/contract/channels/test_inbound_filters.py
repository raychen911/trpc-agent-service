from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import FakeProviderClient, feishu_text_event, wecom_text_event
from trpc_service.channels.base import AdapterEventDisposition
from trpc_service.channels.contracts import Channel
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.models import NodeIdentity


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


class NeverResolver:
    async def resolve_by_channel_identity(self, *args, **kwargs):
        raise AssertionError("filtered input must not resolve a binding")


class NeverGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def handle_verified_message(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("filtered input must not call the Agent path")


class NeverDelivery:
    async def deliver_reply(self, **kwargs):
        raise AssertionError("filtered input must not send")


def _cases(channel: Channel, bot_id: str):
    if channel == Channel.FEISHU:
        return [
            feishu_text_event(sender={"sender_type": "bot", "sender_id": {"open_id": bot_id}}),
            feishu_text_event(sender={"sender_type": "unknown", "sender_id": {}}),
            feishu_text_event(chat_type="group", mentions=[]),
            feishu_text_event(chat_type="group", text="@_user_1", mentions=[{"key": "@_user_1", "id": {"open_id": bot_id}}]),
            feishu_text_event(message_type="image", text="ignored"),
        ]
    original = wecom_text_event()["body"]
    return [
        wecom_text_event(body={**original, "from": {"userid": bot_id, "type": "bot"}}),
        wecom_text_event(body={**original, "from": {"type": "unknown"}}),
        wecom_text_event(body={**original, "chattype": "group", "mentions": []}),
        wecom_text_event(body={**original, "chattype": "group", "text": {"content": "@robot"}, "mentions": [{"key": "@robot", "userid": bot_id}]}),
        wecom_text_event(body={**original, "msgtype": "image"}),
    ]


@pytest.mark.parametrize(("channel", "adapter_type", "bot_id"), [(Channel.FEISHU, FeishuChannelAdapter, "bot-feishu"), (Channel.WECOM, WeComChannelAdapter, "bot-wecom")])
@pytest.mark.asyncio
async def test_non_actionable_inbound_events_never_call_agent(channel, adapter_type, bot_id) -> None:
    identity = ChannelIdentity(channel=channel, provider_tenant_key="provider-tenant", provider_app_or_bot_id=bot_id)
    provider = FakeProviderClient(RuntimeBotIdentity(channel=channel, sender_type="bot", sender_id=bot_id, channel_identity_digest=identity.identity_digest, authenticated_at=NOW))
    gateway = NeverGateway()
    adapter = adapter_type(provider=provider, credential_secret=SecretBytes(b"credential-placeholder"), message_service=ChannelMessageService(NeverResolver(), gateway, NeverDelivery()), now=lambda: NOW)
    await adapter.start(identity, NodeIdentity(node_id="adapter-node"))
    results = [await adapter.handle_provider_event(event) for event in _cases(channel, bot_id)]
    assert gateway.calls == 0
    assert all(result.disposition in {AdapterEventDisposition.IGNORED, AdapterEventDisposition.REJECTED} for result in results)
    assert {result.safe_code for result in results} == {"self_message", "sender_identity_unverified", "group_bot_not_mentioned", "empty_text", "unsupported_event"}
