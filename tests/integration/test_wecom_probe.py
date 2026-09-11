"""The first-use probe only replies once to an exact private-chat challenge."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from websockets.asyncio.server import serve
from wecom_aibot_sdk import WSClient
from wecom_aibot_sdk.types.api import WsFrame

from trpc_service.channels.wecom import WecomAdapter, QuietProtocolLogger
from trpc_service.channels.wecom_probe import probe, REPLY
from trpc_service.channels.events import DeliveryResult


def frame(text, *, chat="single", bot="synthetic-bot", event="message-1"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": event
        },
        "body": {
            "aibotid": bot,
            "msgid": event,
            "from": {
                "userid": "synthetic-user"
            },
            "chattype": chat,
            "chatid": "synthetic-chat",
            "msgtype": "text",
            "text": {
                "content": text
            }
        }
    }


@pytest.mark.asyncio
async def test_public_websocket_probe_ignores_other_messages_and_duplicate_challenge(caplog):
    replies, output = [], []

    async def protocol(socket):
        auth = json.loads(await socket.recv())
        await socket.send(json.dumps({"headers": auth["headers"], "errcode": 0}))
        for value in (frame("private-unrelated"), frame("test-challenge", chat="group"),
                      frame("test-challenge", bot="other-bot"), frame("test-challenge"), frame("test-challenge")):
            await socket.send(json.dumps(value))
        reply = json.loads(await socket.recv())
        replies.append(reply)
        await socket.send(json.dumps({"headers": reply["headers"], "errcode": 0}))
        await socket.wait_closed()

    async with serve(protocol, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = WSClient({
            "bot_id": "synthetic-bot",
            "secret": "synthetic-secret",
            "ws_url": f"ws://127.0.0.1:{port}",
            "logger": QuietProtocolLogger(),
            "max_reconnect_attempts": 1,
            "reconnect_interval": 60000
        })
        result = await probe(WecomAdapter(client),
                             "synthetic-bot",
                             timeout=5,
                             challenge="test-challenge",
                             emit=output.append)
        assert result["status"] == "protocol_ack" and not client.is_connected
    assert len(replies) == 1 and replies[0]["body"]["stream"]["content"] == REPLY
    assert replies[0]["body"]["stream"]["finish"] is True
    assert "private-unrelated" not in " ".join(output) + caplog.text
    assert "synthetic-secret" not in " ".join(output) + caplog.text


@pytest.mark.asyncio
async def test_unknown_reply_is_not_retried():
    handlers = {}
    client = SimpleNamespace(on=lambda name, handler: handlers.update({name: handler}), is_authenticated=True)

    async def connect():
        await handlers["authenticated"](None)
        await handlers["message"](WsFrame(**frame("test-challenge")))

    client.connect_async = connect
    adapter = WecomAdapter(client)
    adapter.close = AsyncMock()
    adapter.send_text = AsyncMock(
        return_value=DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown"))
    result = await probe(adapter, "synthetic-bot", timeout=5, challenge="test-challenge", emit=lambda _: None)
    assert result["status"] == "reply_unknown" and adapter.send_text.await_count == 1
    adapter.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_auth_failure_is_bounded_and_sanitized():
    handlers, output = {}, []
    client = SimpleNamespace(on=lambda name, handler: handlers.update({name: handler}), is_authenticated=False)

    async def connect():
        await handlers["error"](RuntimeError("synthetic-secret-from-protocol"))

    client.connect_async = connect
    adapter = WecomAdapter(client)
    adapter.close = AsyncMock()
    adapter.send_text = AsyncMock()
    result = await asyncio.wait_for(probe(adapter, "synthetic-bot", timeout=5, emit=output.append), 2)
    assert result["reply_outcome"] == "not_sent"
    assert "synthetic-secret" not in " ".join(output)
    adapter.send_text.assert_not_awaited()
    adapter.close.assert_awaited_once()
