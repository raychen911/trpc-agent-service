from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from trpc_service.channels.contracts import Channel, ConversationType
from trpc_service.channels.feishu import FeishuProviderClient, parse_feishu_event
from trpc_service.channels.identity import ChannelIdentity, ProviderReplyContext, RuntimeBotIdentity
from trpc_service.storage.contracts import SecretBytes


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


def _identity() -> ChannelIdentity:
    return ChannelIdentity(channel=Channel.FEISHU, provider_tenant_key="tenant-key-test", provider_app_or_bot_id="app-id-test")


def _bot(identity: ChannelIdentity) -> RuntimeBotIdentity:
    return RuntimeBotIdentity(channel=Channel.FEISHU, sender_type="bot", sender_id="bot-open-id", channel_identity_digest=identity.identity_digest, authenticated_at=NOW)


def test_feishu_event_maps_provider_ids_sender_structured_mention_and_text() -> None:
    identity = _identity()
    parsed = parse_feishu_event(
        {
            "message_id": "om-feishu-001", "chat_id": "oc-feishu-001", "chat_type": "group",
            "sender": {"sender_type": "user", "sender_id": {"open_id": "ou-user-001"}},
            "message_type": "text", "text": "  @_user_1   你好，机器人  ",
            "mentions": [{"key": "@_user_1", "id": {"open_id": "bot-open-id"}}],
        },
        channel_identity=identity, runtime_bot_identity=_bot(identity),
        received_at=NOW, trace_id=UUID(int=21),
    )
    assert parsed.external_message_id == "om-feishu-001"
    assert parsed.external_conversation_id == "oc-feishu-001"
    assert parsed.conversation_type == ConversationType.GROUP
    assert parsed.sender is not None and parsed.sender.sender_id == "ou-user-001"
    assert parsed.bot_mentioned is True
    assert parsed.text == "你好，机器人"
    assert parsed.reply_context.reply_target_id == "oc-feishu-001"


@pytest.mark.asyncio
async def test_feishu_send_maps_unified_text_to_original_chat_without_sdk_leakage() -> None:
    class RawChannel:
        def __init__(self) -> None:
            self.sent: list[tuple[str, str]] = []

        async def send(self, target: str, message: str) -> object:
            self.sent.append((target, message))
            return SimpleNamespace(message_id="om-sent-001")

    identity = _identity()
    raw = RawChannel()
    provider = FeishuProviderClient(identity, SecretBytes(b"app-id-placeholder"), sdk_channel=raw)
    context = ProviderReplyContext(channel=Channel.FEISHU, conversation_type=ConversationType.DIRECT, reply_target_id="oc-original-chat", provider_message_id="om-original-message")
    ack = await provider.send_text(context, "统一回复")
    assert raw.sent == [("oc-original-chat", "统一回复")]
    assert ack.acknowledged is True
    assert ack.provider_request_digest is not None


@pytest.mark.asyncio
async def test_feishu_connect_uses_one_background_ready_entrypoint() -> None:
    class RawChannel:
        def __init__(self) -> None:
            self.handlers: dict[str, object] = {}
            self.background_calls = 0
            self.ready_calls = 0

        def on(self, name: str, handler: object) -> None:
            self.handlers[name] = handler

        async def start_background(self) -> None:
            self.background_calls += 1

        async def connect_until_ready(self) -> None:
            self.ready_calls += 1

    async def on_event(_event: object) -> None:
        return None

    async def on_disconnect() -> None:
        return None

    async def on_error(_error: object) -> None:
        return None

    raw = RawChannel()
    provider = FeishuProviderClient(
        _identity(), SecretBytes(b"app-id-placeholder"), sdk_channel=raw
    )
    await provider.connect(on_event, on_disconnect, on_error)

    assert raw.ready_calls == 1
    assert raw.background_calls == 0
    assert "disconnected" not in raw.handlers


@pytest.mark.asyncio
async def test_feishu_connect_supervises_callback_tasks_until_completion() -> None:
    class RawChannel:
        def __init__(self) -> None:
            self.handlers: dict[str, object] = {}

        def on(self, name: str, handler: object) -> None:
            self.handlers[name] = handler

        async def connect_until_ready(self) -> None:
            return None

    started = asyncio.Event()
    release = asyncio.Event()

    async def on_event(_event: object) -> None:
        started.set()
        await release.wait()

    async def on_disconnect() -> None:
        return None

    async def on_error(_error: object) -> None:
        return None

    raw = RawChannel()
    provider = FeishuProviderClient(
        _identity(), SecretBytes(b"app-id-placeholder"), sdk_channel=raw
    )
    await provider.connect(on_event, on_disconnect, on_error)

    raw.handlers["message"](object())
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(provider._callback_tasks) == 1

    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not provider._callback_tasks


@pytest.mark.asyncio
async def test_feishu_sdk_uses_warning_level_to_avoid_connection_url_output() -> None:
    class RawChannel:
        def on(self, _name: str, _handler: object) -> None:
            return None

        async def connect_until_ready(self) -> None:
            return None

    captured: dict[str, object] = {}

    def channel_factory(**kwargs):
        captured.update(kwargs)
        return RawChannel()

    provider = FeishuProviderClient(
        _identity(),
        SecretBytes(b"app-id-placeholder"),
        channel_factory=channel_factory,
    )

    await provider.authenticate(SecretBytes(b"credential-placeholder"))

    assert provider._channel is not None
    assert captured["log_level"].name == "WARNING"


def test_feishu_sdk_140_inbound_message_shape_maps_text_and_bot_mention() -> None:
    from lark_channel.channel.types import (
        Conversation,
        Identity,
        InboundMessage,
        TextContent,
    )

    identity = _identity()
    event = InboundMessage(
        id="om-sdk-140",
        create_time=1,
        conversation=Conversation(chat_id="oc-sdk-140", chat_type="group"),
        sender=Identity(
            open_id="ou-sdk-user",
            sender_type="user",
            is_bot=False,
        ),
        content=TextContent(text="@机器人 你好"),
        content_text="@机器人 你好",
        safe_content_text="你好",
        body_text="你好",
        raw_content_type="text",
        mentioned_bot=True,
    )

    parsed = parse_feishu_event(
        event,
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=22),
    )

    assert parsed.message_type == "text"
    assert parsed.bot_mentioned is True
    assert parsed.text == "你好"


def test_feishu_event_strips_missing_mention_list_display_name_prefix() -> None:
    identity = _identity()
    parsed = parse_feishu_event(
        {
            "message_id": "om-feishu-prefix-001",
            "chat_id": "oc-feishu-prefix-001",
            "chat_type": "group",
            "sender": {
                "sender_type": "user",
                "sender_id": {"open_id": "ou-user-prefix-001"},
            },
            "message_type": "text",
            "mentioned_bot": True,
            "mentions": [],
            "safe_content_text": "@trpc-agent 测试机器人 Recall the validation token.",
        },
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=23),
    )

    assert parsed.bot_mentioned is True
    assert parsed.text == "Recall the validation token."
