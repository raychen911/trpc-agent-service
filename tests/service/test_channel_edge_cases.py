# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Attachment, transport-failure and streaming edge cases for IM adapters."""

from __future__ import annotations

import base64
import os

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels import (
    CHAT_GROUP,
    DingTalkAdapter,
    FeishuAdapter,
    InboundMessage,
    OutboundMessage,
    SendResult,
    WecomAdapter,
    WechatCustomerServiceAdapter,
)


async def test_json_adapter_transport_failure_lazy_client_and_reply_error(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    lazy = DingTalkAdapter(webhook_url="https://example.invalid/")
    assert lazy._client() is lazy._client()
    await lazy._http_client.aclose()

    async def fail(_request):
        raise httpx.ConnectError("offline")

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    adapter = DingTalkAdapter(webhook_url="https://example.test", http_client=client, message_limit_chars=2)
    result = await adapter.send_message(OutboundMessage(chat_id="c", text="x"))
    assert result.ok is False and "offline" in result.error
    inbound = InboundMessage(channel="dingtalk", chat_id="c", sender_id="u", message_id="m")
    assert (await adapter.reply_text(inbound, "abcd")).ok is False
    await client.aclose()


async def test_supported_json_adapters_parse_attachments_and_invalid_signatures():
    kf = await WechatCustomerServiceAdapter(token="secret").parse_message(
        {"message": {
            "msgid": "1",
            "origin": "u",
            "msgtype": "image",
            "image": {
                "media_id": "img"
            }
        }})
    assert kf.images == ["img"]
    ding = await DingTalkAdapter(secret="secret").parse_message({
        "msgId": "2",
        "senderId": "u",
        "conversationType": "group",
        "conversationId": "g",
        "images": ["img"],
        "files": ["file"],
    })
    assert ding.chat_type == CHAT_GROUP and ding.files == ["file"]
    feishu = await FeishuAdapter(verification_token="v", encrypt_key="k").parse_message({
        "header": {
            "event_id": "3"
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "u"
                }
            },
            "message": {
                "chat_id": "g",
                "chat_type": "group",
                "message_type": "file",
                "content": '{"file_key":"file"}',
            }
        },
    })
    assert feishu.files == ["file"]
    assert await FeishuAdapter(verification_token="v",
                               encrypt_key="k").verify_signature({"token": "bad"}, {"x-lark-signature": "bad"},
                                                                 {}) is False


def _aes_key() -> tuple[str, bytes]:
    raw = os.urandom(32)
    return base64.b64encode(raw).decode()[:-1], raw


def _encrypt(inner: str, receive_id: str, key: bytes) -> str:
    payload = inner.encode()
    framed = os.urandom(16) + len(payload).to_bytes(4, "big") + payload + receive_id.encode()
    padder = padding.PKCS7(128).padder()
    framed = padder.update(framed) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(cipher.update(framed) + cipher.finalize()).decode()


async def test_wecom_payload_validation_image_parse_and_lazy_client(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    adapter = WecomAdapter(token="t", encoding_aes_key="x", api_base="https://example.invalid/")
    assert adapter._client() is adapter._client()
    await adapter._http_client.aclose()
    with pytest.raises(ValueError, match="missing"):
        adapter._extract_encrypt(b"<xml />")
    with pytest.raises(ValueError, match="XML"):
        adapter._extract_encrypt({})
    assert await adapter.verify_signature("<bad", {}, {}) is False
    assert await adapter.verify_signature("<bad", {}, {"msg_signature": "x", "timestamp": "1", "nonce": "2"}) is False

    aes, raw = _aes_key()
    inner = ("<xml><FromUserName>u</FromUserName><MsgType>image</MsgType>"
             "<PicUrl>https://image</PicUrl><CreateTime>123</CreateTime></xml>")
    encrypted = _encrypt(inner, "corp", raw)
    inbound = await WecomAdapter(token="t",
                                 encoding_aes_key=aes).parse_message(f"<xml><Encrypt>{encrypted}</Encrypt></xml>")
    assert inbound.images == ["https://image"]
    assert inbound.message_id == "123"


async def test_wecom_transport_failure_optional_agent_and_stream_failures():
    bodies = []

    async def handler(request):
        bodies.append(request)
        if len(bodies) == 1:
            return httpx.Response(200, json={"errcode": 0})
        raise httpx.ConnectError("offline")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WecomAdapter(token="t", encoding_aes_key="k", access_token="a", http_client=client, message_limit_bytes=2)
    assert (await adapter.send_message(OutboundMessage(chat_id="u", text="ok"))).ok is True
    assert b'"agentid"' not in bodies[0].content
    assert (await adapter.send_message(OutboundMessage(chat_id="u", text="fail"))).ok is False
    inbound = InboundMessage(channel="wecom", chat_id="u", sender_id="u", message_id="m")
    assert (await adapter.reply_text(inbound, "abcd")).ok is False
    await client.aclose()


class PlannedWecom(WecomAdapter):

    def __init__(self, stream_results, send_results=()):
        super().__init__(token="t", encoding_aes_key="k", access_token="a", stream_edit_interval=0)
        self.stream_results = iter(stream_results)
        self.send_results = iter(send_results)

    async def _send_stream_msg(self, action, chat_id, stream_id, content, finish):
        return next(self.stream_results)

    async def send_message(self, outbound):
        return next(self.send_results)


async def test_wecom_stream_update_and_fallback_send_failures():

    async def chunks(*values):
        for value in values:
            yield value

    update_fails = PlannedWecom([SendResult(ok=True), SendResult(ok=False, error="update"), SendResult(ok=True)])
    assert (await update_fails.send_stream("u", chunks("", "a", "b"))).error == "update"

    fallback_fails = PlannedWecom(
        [SendResult(ok=False, error="open")],
        [SendResult(ok=False, error="plain1"),
         SendResult(ok=False, error="plain2")],
    )
    assert (await fallback_fails.send_stream("u", chunks("a", "b"))).error == "plain2"

    no_token = WecomAdapter(token="t", encoding_aes_key="k")
    assert (await no_token._send_stream_msg("send", "u", "s", "x", False)).ok is False
    assert "agentid" not in no_token._stream_body("u", "s", "x", False)
