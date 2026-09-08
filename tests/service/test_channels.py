# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the four supported Chinese enterprise IM adapters."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os

from trpc_service.channels import CHAT_GROUP
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.channels import DingTalkAdapter
from trpc_service.channels import FeishuAdapter
from trpc_service.channels import WecomAdapter
from trpc_service.channels import WechatCustomerServiceAdapter
from trpc_service.channels import InboundMessage
from trpc_service.channels import OutboundMessage
from trpc_service.channels import generate_session_id
from trpc_service.channels import split_text
from trpc_service.channels import split_text_bytes
from trpc_service.channels._crypto import decrypt_message
from trpc_service.channels._crypto import parse_decrypted
from trpc_service.channels._crypto import verify_hmac_signature
from trpc_service.channels._crypto import verify_sha1_signature
from trpc_service.channels._webhook_json import JsonWebhookAdapter
from trpc_service.channels._webhook_json import json_body
from trpc_service.channels._webhook_json import raw_bytes
from trpc_service.channels._webhook_json import verify_hmac_hex

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers import modes

# ---------------------------------------------------------------- session id


def test_generate_session_id_private_vs_group():
    private = generate_session_id("t1", "wecom", CHAT_PRIVATE, "user1", "group1")
    group = generate_session_id("t1", "wecom", CHAT_GROUP, "user1", "group1")
    assert private != group
    assert len(private) == 64  # sha256 hex digest


def test_generate_session_id_cross_tenant_isolation():
    a = generate_session_id("tenant_a", "wecom", CHAT_PRIVATE, "u1")
    b = generate_session_id("tenant_b", "wecom", CHAT_PRIVATE, "u1")
    assert a != b


def test_generate_session_id_is_stable():
    assert generate_session_id("t1", "wecom", CHAT_PRIVATE,
                               "u1") == generate_session_id("t1", "wecom", CHAT_PRIVATE, "u1")


def test_generate_session_id_encodes_boundaries_and_chat_type_without_collisions():
    assert generate_session_id("tenant", "a:b", CHAT_PRIVATE,
                               "c") != generate_session_id("tenant", "a", CHAT_PRIVATE, "b:c")
    assert generate_session_id("tenant", "wecom", CHAT_PRIVATE, "same",
                               "same") != generate_session_id("tenant", "wecom", CHAT_GROUP, "sender", "same")


# ----------------------------------------------------------------- splitting


def test_split_text():
    assert split_text("abcdef", 3) == ["abc", "def"]
    assert split_text("", 3) == []


def test_split_text_bytes_does_not_break_utf8():
    # '你' is 3 bytes in UTF-8; a 4-byte limit must keep it intact.
    chunks = split_text_bytes("你好", 4)
    assert chunks == ["你", "好"]
    for chunk in chunks:
        assert len(chunk.encode("utf-8")) <= 4
    assert split_text_bytes("", 4) == []


# ---------------------------------------------------------------- wecom crypto


def _make_aes_key() -> tuple[str, bytes]:
    key_bytes = os.urandom(32)
    encoding_aes_key = base64.b64encode(key_bytes).decode()[:-1]  # 43 chars
    return encoding_aes_key, key_bytes


def _wecom_encrypt(plaintext: str, receive_id: str, key: bytes) -> str:
    msg = plaintext.encode("utf-8")
    full = os.urandom(16) + len(msg).to_bytes(4, "big") + msg + receive_id.encode("utf-8")
    padder = padding.PKCS7(128).padder()
    padded = padder.update(full) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
    encryptor = cipher.encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()


def _wecom_xml(encrypt: str) -> str:
    return f"<xml><ToUserName><![CDATA[corp]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>"


def _hmac_sig(key: bytes, timestamp: str, nonce: str, encrypt: str) -> str:
    msg = f"{timestamp}\n{nonce}\n{encrypt}\n".encode("utf-8")
    return base64.b64encode(hmac.new(key, msg, hashlib.sha256).digest()).decode()


def _sha1_sig(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    return hashlib.sha1("".join(sorted([token, timestamp, nonce, encrypt])).encode("utf-8")).hexdigest()


def test_wecom_decrypt_roundtrip():
    aes_key, key = _make_aes_key()
    encrypt = _wecom_encrypt("<xml><Content><![CDATA[hello]]></Content></xml>", "corp123", key)
    plain = decrypt_message(encrypt, aes_key)
    message, receive_id = parse_decrypted(plain)
    assert "hello" in message
    assert receive_id == "corp123"


def test_wecom_signature_verification():
    aes_key, key = _make_aes_key()
    encrypt = _wecom_encrypt("hello", "corp123", key)
    token = "mytoken"
    ts, nonce = "1600000000", "abc123"

    assert verify_hmac_signature(aes_key, ts, nonce, encrypt, _hmac_sig(key, ts, nonce, encrypt))
    assert not verify_hmac_signature(aes_key, ts, nonce, encrypt, "badsig")

    assert verify_sha1_signature(token, ts, nonce, encrypt, _sha1_sig(token, ts, nonce, encrypt))
    assert not verify_sha1_signature(token, ts, nonce, encrypt, "badsig")


async def test_wecom_adapter_verify_and_parse():
    aes_key, key = _make_aes_key()
    token = "mytoken"
    inner = ("<xml><ToUserName><![CDATA[corp123]]></ToUserName>"
             "<FromUserName><![CDATA[zhangsan]]></FromUserName>"
             "<MsgType><![CDATA[text]]></MsgType>"
             "<Content><![CDATA[查订单]]></Content>"
             "<MsgId>123456</MsgId><AgentID>1</AgentID></xml>")
    encrypt = _wecom_encrypt(inner, "corp123", key)
    payload = _wecom_xml(encrypt)
    ts, nonce = "1600000000", "abc123"

    adapter = WecomAdapter(token=token, encoding_aes_key=aes_key, corp_id="corp123")
    query = {"msg_signature": _hmac_sig(key, ts, nonce, encrypt), "timestamp": ts, "nonce": nonce}
    assert await adapter.verify_signature(payload, {}, query) is True

    inbound = await adapter.parse_message(payload)
    assert inbound.channel == "wecom"
    assert inbound.sender_id == "zhangsan"
    assert inbound.chat_id == "zhangsan"
    assert inbound.chat_type == CHAT_PRIVATE
    assert inbound.message_id == "123456"
    assert inbound.text == "查订单"


async def test_wecom_group_message():
    aes_key, key = _make_aes_key()
    inner = ("<xml><FromUserName><![CDATA[zhangsan]]></FromUserName>"
             "<ChatId><![CDATA[group99]]></ChatId>"
             "<MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hi]]></Content>"
             "<MsgId>777</MsgId></xml>")
    encrypt = _wecom_encrypt(inner, "corp123", key)
    inbound = await WecomAdapter(token="t", encoding_aes_key=aes_key).parse_message(_wecom_xml(encrypt))
    assert inbound.chat_type == CHAT_GROUP
    assert inbound.chat_id == "group99"
    assert inbound.sender_id == "zhangsan"


async def test_wecom_bad_signature_rejected():
    aes_key, key = _make_aes_key()
    encrypt = _wecom_encrypt("x", "corp123", key)
    adapter = WecomAdapter(token="t", encoding_aes_key=aes_key)
    query = {"msg_signature": "bad", "timestamp": "1", "nonce": "2"}
    assert await adapter.verify_signature(_wecom_xml(encrypt), {}, query) is False


async def test_channel_adapters_close_only_their_owned_http_clients(monkeypatch):
    monkeypatch.delenv("ALL_PROXY", raising=False)
    monkeypatch.delenv("all_proxy", raising=False)
    dingtalk = DingTalkAdapter(webhook_url="https://example.invalid/hook")
    owned_client = dingtalk._client()
    await dingtalk.close()
    assert owned_client.is_closed is True

    import httpx

    external_client = httpx.AsyncClient()
    wecom = WecomAdapter(token="t", encoding_aes_key="a", http_client=external_client)
    await wecom.close()
    assert external_client.is_closed is False
    await external_client.aclose()


# ------------------------------------------------------- WeChat KF / DT / FS


async def test_wechat_kf_parse_message_and_signature():
    payload = {
        "event_id": "evt-1",
        "message": {
            "msgid": "kf-1",
            "origin": "external-user",
            "open_kfid": "wk-1",
            "msgtype": "text",
            "text": {
                "content": "查询售后"
            },
        },
    }
    raw = __import__("json").dumps(payload, ensure_ascii=False, separators=(",", ":"))
    signature = hmac.new(b"secret", raw.encode(), hashlib.sha256).hexdigest()
    adapter = WechatCustomerServiceAdapter(corp_id="corp", open_kfid="wk-1", token="secret")
    assert await adapter.verify_signature(raw, {"x-wechat-kf-signature": signature}, {}) is True
    inbound = await adapter.parse_message(raw)
    assert (inbound.channel, inbound.sender_id, inbound.text) == ("wechat_kf", "external-user", "查询售后")


async def test_dingtalk_parse_group_message_and_signature():
    payload = {
        "msgId": "dt-1",
        "senderStaffId": "staff-1",
        "conversationType": "2",
        "conversationId": "group-1",
        "text": {
            "content": " 查订单 "
        },
    }
    raw = __import__("json").dumps(payload, ensure_ascii=False, separators=(",", ":"))
    signature = hmac.new(b"secret", raw.encode(), hashlib.sha256).hexdigest()
    adapter = DingTalkAdapter(client_id="app", robot_code="robot", secret="secret")
    assert await adapter.verify_signature(raw, {"x-dingtalk-signature": signature}, {}) is True
    inbound = await adapter.parse_message(payload)
    assert inbound.channel == "dingtalk"
    assert inbound.chat_type == CHAT_GROUP
    assert inbound.chat_id == "group-1"
    assert inbound.text == "查订单"


async def test_feishu_parse_private_message_and_verify_token():
    payload = {
        "token": "verify",
        "header": {
            "event_id": "evt-1",
            "tenant_key": "tenant-key"
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "ou-1"
                }
            },
            "message": {
                "message_id": "fs-1",
                "chat_id": "oc-1",
                "chat_type": "p2p",
                "message_type": "text",
                "content": '{"text":"你好"}',
            },
        },
    }
    adapter = FeishuAdapter(app_id="app", verification_token="verify", encrypt_key="encrypt")
    assert await adapter.verify_signature(payload, {}, {}) is True
    inbound = await adapter.parse_message(payload)
    assert inbound.channel == "feishu"
    assert inbound.chat_type == CHAT_PRIVATE
    assert inbound.sender_id == "ou-1"
    assert inbound.text == "你好"


async def test_wecom_plain_fixture_is_only_a_parser_fixture():
    inbound = await WecomAdapter(token="", encoding_aes_key="").parse_message({
        "FromUserName": "u1",
        "ChatId": "g1",
        "MsgType": "text",
        "Content": "本地验证",
        "MsgId": "m1",
    })
    assert inbound.channel == "wecom"
    assert inbound.chat_type == CHAT_GROUP
    assert inbound.metadata["local_fixture"] is True


def test_json_webhook_helpers_cover_bytes_bytearray_and_base_renderer():
    assert json_body(b'{"x":1}') == {"x": 1}
    assert raw_bytes(b"x") == b"x"
    assert raw_bytes(bytearray(b"y")) == b"y"
    assert verify_hmac_hex("", {}, "") is False

    class ConcreteJsonAdapter(JsonWebhookAdapter):

        async def verify_signature(self, payload, headers, query):
            return True

        async def parse_message(self, payload):
            return InboundMessage(channel="test", chat_id="c", sender_id="u", message_id="m")

    adapter = ConcreteJsonAdapter()
    body = adapter.render_outbound(OutboundMessage(chat_id="c", text="hello"))
    assert body == {"chat_id": "c", "msgtype": "text", "text": "hello"}


async def test_feishu_non_json_content_falls_back_to_plain_text():
    inbound = await FeishuAdapter().parse_message({
        "event": {
            "sender": {
                "sender_id": {
                    "user_id": "u"
                }
            },
            "message": {
                "message_id": "m",
                "chat_type": "p2p",
                "content": "plain text",
            }
        },
    })
    assert inbound.text == "plain text"
