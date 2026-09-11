"""Enterprise WeChat callback cryptography and decoding contracts."""

from __future__ import annotations

import base64
import hashlib
import os

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.webhook.wecom import (
    WebhookVerificationError,
    decode_wecom_message,
    decrypt_wecom_payload,
    verify_wecom_signature,
)

_TOKEN = "callback-token"
_RECEIVE_ID = "corp-demo"
_AES_KEY = base64.b64encode(bytes(range(32))).decode().rstrip("=")


def _encrypt(plaintext: bytes) -> str:
    key = base64.b64decode(_AES_KEY + "=")
    raw = os.urandom(16) + len(plaintext).to_bytes(4, "big") + plaintext + _RECEIVE_ID.encode()
    padding = 32 - len(raw) % 32
    raw += bytes([padding]) * padding
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(raw) + encryptor.finalize()).decode()


def _signature(timestamp: str, nonce: str, encrypted: str) -> str:
    return hashlib.sha1("".join(sorted((_TOKEN, timestamp, nonce, encrypted))).encode()).hexdigest()


def test_verifies_signature_and_decrypts_wecom_url_challenge():
    encrypted = _encrypt(b"challenge-text")
    timestamp, nonce = "1700000000", "nonce-1"

    verify_wecom_signature(
        msg_signature=_signature(timestamp, nonce, encrypted),
        timestamp=timestamp,
        nonce=nonce,
        encrypted=encrypted,
        token=_TOKEN,
    )

    assert decrypt_wecom_payload(encrypted, aes_key=_AES_KEY, receive_id=_RECEIVE_ID) == b"challenge-text"


def test_tampered_signature_or_receive_id_fails_without_leaking_payload():
    encrypted = _encrypt(b"SENTINEL_CALLBACK_BODY")
    with pytest.raises(WebhookVerificationError) as signature_error:
        verify_wecom_signature(msg_signature="bad",
                               timestamp="1700000000",
                               nonce="nonce",
                               encrypted=encrypted,
                               token=_TOKEN)
    with pytest.raises(WebhookVerificationError) as receive_id_error:
        decrypt_wecom_payload(encrypted, aes_key=_AES_KEY, receive_id="other-corp")

    assert "SENTINEL_CALLBACK_BODY" not in str(signature_error.value)
    assert "SENTINEL_CALLBACK_BODY" not in str(receive_id_error.value)


def test_decodes_wecom_group_text_xml_after_decryption():
    xml = (b"<xml><ToUserName><![CDATA[corp-demo]]></ToUserName><FromUserName><![CDATA[user-1]]>"
           b"</FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType>"
           b"<Content><![CDATA[ hello ]]></Content><MsgId>msg-1</MsgId><ChatId><![CDATA[chat-1]]>"
           b"</ChatId></xml>")

    message = decode_wecom_message(xml, external_account_id=_RECEIVE_ID)

    assert message.channel == "wecom"
    assert message.conversation_kind == "group"
    assert message.external_conversation_id == "chat-1"
    assert message.text == "hello"
