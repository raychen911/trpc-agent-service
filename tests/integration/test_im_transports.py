"""Public protocol clients with synthetic HTTP/WebSocket responses, never real IM accounts."""

import json
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import Bot
from telegram.request import BaseRequest
from wecom_aibot_sdk.types.api import WsFrame
from wecom_aibot_sdk import WSClient
from websockets.asyncio.server import serve

from trpc_service.channels.base import CallbackVerificationError
from trpc_service.channels.events import DeliveryResult, split_text
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom import WecomAdapter, QuietProtocolLogger
from trpc_service.tenant import ChannelBindingConfig


def telegram_binding(tenant):
    return ChannelBindingConfig(**{
        **tenant.channel_bindings[0].model_dump(), "channel": "telegram",
        "external_account_id": "123456"
    })


def update(event_id=50, **message):
    return {
        "update_id": event_id,
        "message": {
            "message_id": 7,
            "date": 1700000000,
            "chat": {
                "id": 42,
                "type": "private"
            },
            "from": {
                "id": 42,
                "is_bot": False,
                "first_name": "Test"
            },
            "text": "hello",
            **message
        }
    }


class ProtocolRequest(BaseRequest):
    """Exercise PTB serialization and its real API error decoder."""

    def __init__(self, response=None):
        self.response = response
        self.sent = []

    @property
    def read_timeout(self):
        return 10

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        operation = url.rsplit("/", 1)[-1]
        self.sent.append((operation, request_data.parameters))
        if operation == "getMe":
            result = {"ok": True, "result": {"id": 123456, "is_bot": True, "first_name": "Test"}}
            return 200, json.dumps(result).encode()
        if isinstance(self.response, BaseException):
            raise self.response
        if self.response:
            return self.response[0], json.dumps(self.response[1]).encode()
        return 200, json.dumps({"ok": True, "result": update()["message"]}).encode()


@pytest.mark.asyncio
async def test_telegram_public_client_preserves_topic_and_receipt():
    request = ProtocolRequest()
    async with Bot("123456:synthetic-test-token", request=request) as bot:
        result = await TelegramAdapter(bot).send_text({"chat_id": "-1004", "thread_id": 33}, "reply")
    assert result.outcome == "accepted" and result.external_message_id == "7"
    assert request.sent[-1] == ("sendMessage", {"chat_id": "-1004", "text": "reply", "message_thread_id": 33})


@pytest.mark.asyncio
@pytest.mark.parametrize("response,outcome,error,retry", [
    ((429, {
        "ok": False,
        "error_code": 429,
        "description": "Too Many Requests",
        "parameters": {
            "retry_after": 5
        }
    }), "rejected", "rate_limited", True),
    ((403, {
        "ok": False,
        "error_code": 403,
        "description": "private user data"
    }), "rejected", "permission_denied", False),
    (TimeoutError("secret in transport exception"), "unknown", "transport_outcome_unknown", False),
])
async def test_telegram_error_mapping_never_exposes_raw_error(response, outcome, error, retry):
    request = ProtocolRequest(response)
    async with Bot("123456:synthetic-test-token", request=request) as bot:
        result = await TelegramAdapter(bot).send_text({"chat_id": "42"}, "reply")
    assert (result.outcome, result.error_type, result.retryable) == (outcome, error, retry)
    assert "private user" not in result.model_dump_json() and "secret" not in result.model_dump_json()


def test_telegram_normalization_ignores_edits_anonymous_and_media(tenant):
    adapter = TelegramAdapter(Bot("123456:synthetic-test-token"))
    binding = telegram_binding(tenant)
    event = adapter.normalize(update(chat={"id": -1004, "type": "supergroup"}, message_thread_id=33), binding)
    assert event.message.conversation_type.value == "thread" and event.message.thread_id == "33"
    assert event.event_id == "50" and event.reply_context["message_id"] == 7
    for raw in ({
            "update_id": 51,
            "edited_message": update()["message"]
    }, update(sender_chat={
            "id": -1,
            "type": "group"
    }), update(text=None)):
        assert adapter.normalize(raw, binding).kind == "ignored"
    with pytest.raises(CallbackVerificationError):
        adapter.verify_webhook("wrong", "expected")
    adapter.verify_webhook("expected", "expected")


def test_telegram_callback_is_control_not_agent_text(tenant):
    raw = {
        "update_id": 100,
        "callback_query": {
            "id": "query",
            "from": update()["message"]["from"],
            "chat_instance": "instance",
            "data": "action-reference",
            "message": update(chat={"id": -1004, "type": "supergroup"}, message_thread_id=33)["message"]
        }
    }
    event = TelegramAdapter(Bot("123456:synthetic-test-token")).normalize(raw, telegram_binding(tenant))
    assert event.kind == "action" and event.message is None and event.external_actor_id == "42"
    assert event.reply_context["thread_id"] == 33
    assert event.external_chat_id == "-1004"


@pytest.mark.asyncio
async def test_wecom_ack_finish_and_account_authentication(tenant):
    binding = tenant.channel_bindings[0]
    client = SimpleNamespace(is_authenticated=True,
                             reply_stream=AsyncMock(return_value=WsFrame(headers={"req_id": "ack-1"}, errcode=0)),
                             send_message=AsyncMock(return_value=WsFrame(headers={"req_id": "ack-2"}, errcode=0)),
                             download_file=AsyncMock(return_value=(b"synthetic-file", "note.txt")))
    adapter = WecomAdapter(client)
    frame = WsFrame(headers={"req_id": "req-1"},
                    cmd="aibot_msg_callback",
                    body={
                        "aibotid": binding.external_account_id,
                        "msgid": "msg-1",
                        "from": {
                            "userid": "alice"
                        },
                        "chattype": "single",
                        "msgtype": "text",
                        "text": {
                            "content": "hello"
                        }
                    })
    event = adapter.normalize(frame, binding)
    result = await adapter.send_text(event.reply_context, "reply", stream_id="stream-1")
    client.reply_stream.assert_awaited_once_with({"req_id": "req-1"}, "stream-1", "reply", finish=True)
    assert result.receipt_kind == "protocol_ack" and result.external_message_id is None
    continuation = await adapter.send_text(event.reply_context, "part two", stream_id="stream-1", part_no=1)
    client.send_message.assert_awaited_once_with("alice", {
        "msgtype": "markdown", "markdown": {"content": "part two"}})
    assert continuation.provider_request_id == "ack-2"
    media_frame = WsFrame(headers={"req_id": "req-2"}, cmd="aibot_msg_callback", body={
        **frame.body, "msgid": "msg-2", "msgtype": "file",
        "file": {"url": "https://media.example.test/file", "aeskey": "synthetic-key", "name": "note.txt"}})
    media = adapter.normalize(media_frame, binding)
    assert media.pending_media[0].kind == "file"
    assert await adapter.download_media(media.pending_media[0], "msg-2") == (
        b"synthetic-file", "note.txt", "text/plain")
    client.reply_stream.return_value = WsFrame(headers={"req_id": "rate"}, errcode=45009)
    limited = await adapter.send_text(event.reply_context, "reply", stream_id="stream-2")
    assert limited.retryable and limited.error_type == "rate_limited"
    client.reply_stream.side_effect = TimeoutError("auth secret must not escape")
    assert (await adapter.send_text(event.reply_context, "reply", stream_id="stream-1")).outcome == "unknown"
    frame.body["aibotid"] = "another-account"
    with pytest.raises(CallbackVerificationError):
        adapter.normalize(frame, binding)


def test_unicode_chunks_and_invalid_delivery_state():
    text = "中文😀" * 3000
    chunks = split_text(text, 4096)
    assert "".join(chunks) == text and max(map(len, chunks)) <= 4096
    chunks = split_text(text, 20480, unit="utf8_bytes")
    assert "".join(chunks) == text and all(len(chunk.encode()) <= 20480 for chunk in chunks)
    with pytest.raises(ValueError):
        DeliveryResult(outcome="unknown", retryable=True)
    with pytest.raises(ValueError):
        DeliveryResult(outcome="accepted")


@pytest.mark.asyncio
async def test_wecom_public_client_authenticates_and_finishes_real_websocket_frame(tenant, caplog):
    """A local protocol server validates public SDK behavior without Tencent credentials."""
    binding = tenant.channel_bindings[0]
    received, ready = [], asyncio.Event()
    sent = []

    async def protocol(socket):
        auth = json.loads(await socket.recv())
        assert auth["cmd"] == "aibot_subscribe"
        assert auth["body"] == {"bot_id": binding.external_account_id, "secret": "synthetic-secret"}
        await socket.send(json.dumps({"headers": auth["headers"], "errcode": 0}))
        await socket.send(
            json.dumps({
                "headers": {
                    "req_id": "request-1"
                },
                "cmd": "aibot_msg_callback",
                "body": {
                    "aibotid": binding.external_account_id,
                    "msgid": "message-1",
                    "from": {
                        "userid": "alice"
                    },
                    "chattype": "single",
                    "msgtype": "text",
                    "text": {
                        "content": "synthetic-private-message"
                    }
                }
            }))
        reply = json.loads(await socket.recv())
        sent.append(reply)
        await socket.send(json.dumps({"headers": reply["headers"], "errcode": 0}))
        await socket.wait_closed()

    async def message(frame):
        received.append(frame)
        ready.set()

    async with serve(protocol, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = WSClient({
            "bot_id": binding.external_account_id,
            "secret": "synthetic-secret",
            "ws_url": f"ws://127.0.0.1:{port}",
            "logger": QuietProtocolLogger(),
            "max_reconnect_attempts": 1
        })
        adapter = WecomAdapter(client)
        client.on("message", message)
        try:
            await asyncio.wait_for(client.connect_async(), 5)
            await asyncio.wait_for(ready.wait(), 5)
            event = adapter.normalize(received[0], binding)
            result = await asyncio.wait_for(adapter.send_text(event.reply_context, "reply", stream_id="stream-1"), 5)
            assert result.outcome == "accepted" and result.receipt_kind == "protocol_ack"
            assert sent[0]["body"]["stream"] == {"id": "stream-1", "content": "reply", "finish": True}
        finally:
            await adapter.close()
        assert not client.is_connected and not client.is_authenticated
    assert "synthetic-secret" not in caplog.text
    assert "synthetic-private-message" not in caplog.text
