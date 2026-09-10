from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID
import asyncio

import pytest

from trpc_service.channels.contracts import Channel, ConversationType
from trpc_service.channels.identity import ChannelIdentity, ProviderReplyContext, RuntimeBotIdentity
from trpc_service.channels.wecom import WeComProviderClient, parse_wecom_event
from trpc_service.storage.contracts import SecretBytes


NOW = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


def _identity() -> ChannelIdentity:
    return ChannelIdentity(channel=Channel.WECOM, provider_tenant_key="corp-id-test", provider_app_or_bot_id="bot-id-test")


def _bot(identity: ChannelIdentity) -> RuntimeBotIdentity:
    return RuntimeBotIdentity(channel=Channel.WECOM, sender_type="bot", sender_id="bot-id-test", channel_identity_digest=identity.identity_digest, authenticated_at=NOW)


def test_wecom_event_uses_msgid_for_business_identity_and_keeps_req_id_protocol_only() -> None:
    identity = _identity()
    parsed = parse_wecom_event(
        {
            "headers": {"req_id": "protocol-req-001"},
            "body": {
                "msgid": "business-msg-001", "chatid": "group-chat-001", "chattype": "group",
                "from": {"userid": "wecom-user-001", "type": "user"}, "msgtype": "text",
                "text": {"content": "  @robot   你好，企微  "},
                "mentions": [{"key": "@robot", "userid": "bot-id-test"}],
            },
        },
        channel_identity=identity, runtime_bot_identity=_bot(identity),
        received_at=NOW, trace_id=UUID(int=22),
    )
    assert parsed.external_message_id == "business-msg-001"
    assert parsed.external_message_id != "protocol-req-001"
    assert parsed.external_conversation_id == "group-chat-001"
    assert parsed.sender is not None and parsed.sender.sender_id == "wecom-user-001"
    assert parsed.bot_mentioned is True
    assert parsed.text == "你好，企微"
    assert parsed.reply_context.protocol_request_id == "protocol-req-001"


def test_wecom_sdk_single_chat_shape_uses_from_as_sender_and_reply_target() -> None:
    identity = _identity()
    parsed = parse_wecom_event(
        {
            "headers": {"req_id": "protocol-sdk-single-001"},
            "body": {
                "msgid": "sdk-single-msg-001",
                "chattype": "single",
                "from": "wecom-user-single-001",
                "msgtype": "text",
                "text": {"content": "Remember validation token WECOM1."},
            },
        },
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=23),
    )

    assert parsed.external_message_id == "sdk-single-msg-001"
    assert parsed.external_conversation_id == "wecom-user-single-001"
    assert parsed.sender is not None
    assert parsed.sender.sender_id == "wecom-user-single-001"
    assert parsed.sender.sender_type == "user"
    assert parsed.text == "Remember validation token WECOM1."
    assert parsed.reply_context.reply_target_id == "wecom-user-single-001"


def test_wecom_sdk_text_callback_without_msgtype_maps_as_text() -> None:
    identity = _identity()
    parsed = parse_wecom_event(
        {
            "headers": {"req_id": "protocol-sdk-text-001"},
            "body": {
                "msgid": "sdk-text-msg-001",
                "chattype": "single",
                "from": "wecom-user-text-001",
                "text": {"content": "Remember validation token WECOM2."},
            },
        },
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=24),
    )

    assert parsed.message_type == "text"
    assert parsed.text == "Remember validation token WECOM2."


def test_wecom_real_single_callback_uses_from_userid_when_chatid_is_omitted() -> None:
    identity = _identity()
    parsed = parse_wecom_event(
        {
            "headers": {"req_id": "aibot_msg_callback_req-001"},
            "body": {
                "msgid": "fe0545d4d26d207b9e6b7bac14bf5f77",
                "aibotid": "bot-runtime-id",
                "chattype": "single",
                "from": {"userid": "LuWenJie"},
                "msgtype": "text",
                "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response",
                "text": {"content": "Remember validation token WECOM3."},
            },
        },
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=25),
    )

    assert parsed.external_conversation_id == "LuWenJie"
    assert parsed.sender is not None and parsed.sender.sender_id == "LuWenJie"
    assert parsed.text == "Remember validation token WECOM3."


def test_wecom_real_group_callback_detects_and_strips_display_name_mention() -> None:
    identity = _identity()
    parsed = parse_wecom_event(
        {
            "headers": {"req_id": "aibot_msg_callback_req-group-001"},
            "body": {
                "msgid": "real-group-msg-001",
                "chatid": "real-group-chat-001",
                "chattype": "group",
                "from": {"userid": "LuWenJie"},
                "msgtype": "text",
                "text": {"content": "@tRPC-Agent 测试机器人  Remember validation token WECOMGROUP."},
            },
        },
        channel_identity=identity,
        runtime_bot_identity=_bot(identity),
        received_at=NOW,
        trace_id=UUID(int=26),
    )

    assert parsed.bot_mentioned is True
    assert parsed.text == "Remember validation token WECOMGROUP."


@pytest.mark.asyncio
async def test_wecom_send_uses_protocol_reply_context_and_original_conversation() -> None:
    class RawClient:
        def __init__(self) -> None:
            self.replies: list[tuple[dict[str, object], dict[str, object]]] = []

        async def reply(self, frame: dict[str, object], body: dict[str, object]) -> object:
            self.replies.append((frame, body))
            return {"errcode": 0, "request_id": "provider-ack-001"}

    identity = _identity()
    raw = RawClient()
    provider = WeComProviderClient(identity, SecretBytes(b"bot-id-placeholder"), sdk_client=raw)
    context = ProviderReplyContext(channel=Channel.WECOM, conversation_type=ConversationType.GROUP, reply_target_id="group-chat-001", protocol_request_id="protocol-req-001", provider_message_id="business-msg-001")
    ack = await provider.send_text(context, "统一回复")
    frame, body = raw.replies[0]
    assert frame == {"headers": {"req_id": "protocol-req-001"}, "body": {"chatid": "group-chat-001", "msgid": "business-msg-001"}}
    assert body == {"msgtype": "markdown", "markdown": {"content": "统一回复"}}
    assert ack.acknowledged is True
    assert ack.provider_request_digest is not None


@pytest.mark.asyncio
async def test_wecom_connect_supervises_callback_tasks_until_completion() -> None:
    class RawClient:
        def __init__(self) -> None:
            self.handlers: dict[str, object] = {}

        def on(self, name: str, handler: object) -> None:
            self.handlers[name] = handler

        async def connect(self) -> None:
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

    raw = RawClient()
    provider = WeComProviderClient(
        _identity(), SecretBytes(b"bot-id-placeholder"), sdk_client=raw
    )
    await provider.connect(on_event, on_disconnect, on_error)

    raw.handlers["message"]({"body": {"msgid": "callback-task-001"}})
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(provider._callback_tasks) == 1

    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not provider._callback_tasks
