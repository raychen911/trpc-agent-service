# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the supported enterprise IM send paths."""

from __future__ import annotations

import json

import httpx

from trpc_service.channels import CHAT_PRIVATE
from trpc_service.channels import InboundMessage
from trpc_service.channels import OutboundMessage
from trpc_service.channels import DingTalkAdapter
from trpc_service.channels import FeishuAdapter
from trpc_service.channels import WecomAdapter
from trpc_service.channels import WechatCustomerServiceAdapter


# ---------------------------------------------------------------- wecom send
async def test_wecom_send_message():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((str(request.url), json.loads(request.content.decode())))
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok", "msgid": "123"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t", encoding_aes_key="k", access_token="acc", agent_id="100", http_client=client)

    result = await adapter.send_message(OutboundMessage(chat_id="u1", text="hello"))
    assert result.ok is True
    assert result.message_id == "123"
    assert "/cgi-bin/message/send" in calls[0][0]
    assert calls[0][1]["touser"] == "u1"
    assert calls[0][1]["text"]["content"] == "hello"
    assert calls[0][1]["agentid"] == 100
    await client.aclose()


async def test_wecom_send_message_without_access_token():
    adapter = WecomAdapter(token="t", encoding_aes_key="k")
    result = await adapter.send_message(OutboundMessage(chat_id="u1", text="hi"))
    assert result.ok is False
    assert "access_token" in result.error


async def test_wecom_fetches_and_caches_access_token():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "/cgi-bin/gettoken" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "errcode": 0,
                    "access_token": "generated",
                    "expires_in": 7200
                },
            )
        return httpx.Response(200, json={"errcode": 0, "msgid": "token-msg"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(
        token="t",
        encoding_aes_key="k",
        corp_id="corp",
        corp_secret="secret",
        agent_id="100",
        http_client=client,
    )

    first = await adapter.send_message(OutboundMessage(chat_id="u1", text="one"))
    second = await adapter.send_message(OutboundMessage(chat_id="u1", text="two"))

    assert first.ok is second.ok is True
    assert sum("/cgi-bin/gettoken" in url for url in calls) == 1
    assert sum("access_token=generated" in url for url in calls) == 2
    await client.aclose()


async def test_wecom_reply_text_splits_long_message():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok", "msgid": "1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t", encoding_aes_key="k", access_token="acc", agent_id="100", http_client=client)

    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m1",
                             text="")
    long_text = "你" * 1000  # 3000 UTF-8 bytes > 2048 byte limit
    result = await adapter.reply_text(inbound, long_text)
    assert result.ok is True
    assert len(calls) >= 2
    await client.aclose()


async def test_wecom_post_error():

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errcode": 40001, "errmsg": "invalid credential"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t", encoding_aes_key="k", access_token="acc", agent_id="100", http_client=client)

    result = await adapter.send_message(OutboundMessage(chat_id="u1", text="hi"))
    assert result.ok is False
    assert "errcode" in result.error
    await client.aclose()


async def test_wecom_send_stream_native_sequence():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((str(request.url), json.loads(request.content.decode())))
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok", "msgid": "9"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t",
                           encoding_aes_key="k",
                           access_token="acc",
                           agent_id="100",
                           http_client=client,
                           stream_edit_interval=0)

    async def gen():
        yield "hello"
        yield " world"

    result = await adapter.send_stream("u1", gen())
    assert result.ok is True
    # Sequence: open (send) → append (update) → finish (update, finish=True).
    assert len(calls) == 3
    assert "message/send" in calls[0][0]
    assert "message/update" in calls[1][0]
    assert "message/update" in calls[2][0]
    assert calls[0][1]["msgtype"] == "stream"
    assert calls[0][1]["stream"]["finish"] is False
    assert calls[2][1]["stream"]["finish"] is True
    assert calls[2][1]["stream"]["content"] == "hello world"
    await client.aclose()


async def test_wecom_send_stream_fallback_on_open_failure():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        calls.append(body)
        if body.get("msgtype") == "stream":
            return httpx.Response(200, json={"errcode": 45009, "errmsg": "stream not allowed"})
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok", "msgid": "9"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t",
                           encoding_aes_key="k",
                           access_token="acc",
                           agent_id="100",
                           http_client=client,
                           stream_edit_interval=0)

    async def gen():
        yield "hello"
        yield " world"

    await adapter.send_stream("u1", gen())
    # Fallback: both chunks delivered as plain text messages.
    text_calls = [c for c in calls if c.get("msgtype") == "text"]
    assert len(text_calls) == 2
    await client.aclose()


# ------------------------------------------------------ JSON adapter send paths


async def test_cn_adapters_render_platform_specific_payloads_with_sdk_hook():
    calls = []

    async def sender(body):
        calls.append(body)
        from trpc_service.channels import SendResult
        return SendResult(ok=True, message_id="reply-1")

    adapters = [
        WechatCustomerServiceAdapter(open_kfid="wk-1", send_hook=sender),
        DingTalkAdapter(robot_code="robot-1", send_hook=sender),
        FeishuAdapter(app_id="app-1", send_hook=sender),
    ]
    for adapter in adapters:
        result = await adapter.send_message(OutboundMessage(chat_id="c1", text="hello"))
        assert result.ok is True
    assert calls[0]["open_kfid"] == "wk-1"
    assert calls[1]["robotCode"] == "robot-1"
    assert json.loads(calls[2]["content"])["text"] == "hello"


async def test_json_adapter_webhook_transport_split_stream_and_error():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content.decode()))
        if len(calls) == 3:
            return httpx.Response(200, json={"errcode": 40001})
        return httpx.Response(200, json={"errcode": 0, "msgid": str(len(calls))})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = DingTalkAdapter(robot_code="robot",
                              webhook_url="https://example.test/send",
                              http_client=client,
                              message_limit_chars=2)
    inbound = InboundMessage(channel="dingtalk", chat_id="c", sender_id="u", message_id="m")
    assert (await adapter.reply_text(inbound, "abcd")).ok is True

    async def stream():
        yield "x"
        yield "y"

    assert (await adapter.send_stream("c", stream())).ok is False
    without_transport = FeishuAdapter(app_id="app")
    assert (await without_transport.send_message(OutboundMessage(chat_id="c", text="x"))).ok is False
    await client.aclose()


async def test_json_adapter_retries_transient_server_error():
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"error": "busy"})
        return httpx.Response(200, json={"errcode": 0, "msgid": "retry-ok"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = DingTalkAdapter(
        robot_code="robot",
        webhook_url="https://example.test/send",
        http_client=client,
        max_retries=1,
        retry_backoff=0,
    )
    result = await adapter.send_message(OutboundMessage(chat_id="c", text="hello"))

    assert result.ok is True
    assert result.message_id == "retry-ok"
    assert calls == 2
    await client.aclose()


async def test_wecom_retries_transient_server_error():
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(500, json={"errmsg": "busy"})
        return httpx.Response(200, json={"errcode": 0, "msgid": "retry-ok"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(
        token="t",
        encoding_aes_key="k",
        access_token="acc",
        http_client=client,
        max_retries=1,
        retry_backoff=0,
    )
    result = await adapter.send_message(OutboundMessage(chat_id="u1", text="hello"))

    assert result.ok is True
    assert calls == 2
    await client.aclose()
