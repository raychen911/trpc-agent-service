"""HTTP contracts for the Enterprise WeChat callback route."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import uuid

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from fastapi import FastAPI

from trpc_service.channels.binding import ChannelBinding
from trpc_service.config.secret_resolver import EnvSecretResolver
from trpc_service.gateway.webhook_routes import register_wecom_webhook_routes

_TOKEN = "callback-token"
_ACCOUNT = "corp-demo"
_AES_KEY = base64.b64encode(bytes(range(32))).decode().rstrip("=")


def _encrypt(plaintext: bytes) -> str:
    key = base64.b64decode(_AES_KEY + "=")
    raw = os.urandom(16) + len(plaintext).to_bytes(4, "big") + plaintext + _ACCOUNT.encode()
    padding = 32 - len(raw) % 32
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(raw + bytes([padding]) * padding) + encryptor.finalize()).decode()


def _binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=uuid.uuid4(),
        tenant_id="tenant_a",
        app_id="app_demo",
        channel="wecom",
        external_account_id=_ACCOUNT,
        secret_ref="env:TRPC_WECOM_BOT",
        enabled=True,
        version=1,
        webhook_token_ref="env:TRPC_WECOM_WEBHOOK_TOKEN",
        webhook_aes_key_ref="env:TRPC_WECOM_WEBHOOK_AES_KEY",
    )


class _Bindings:

    async def resolve_enabled(self, channel: str, account: str):
        return _binding() if (channel, account) == ("wecom", _ACCOUNT) else None


class _Ingress:

    async def chat(self, _message):
        raise AssertionError("GET verification must not enter ingress")


class _RecordingIngress:

    def __init__(self) -> None:
        self.messages = []

    async def chat(self, message):
        self.messages.append(message)


def test_get_wecom_callback_verifies_binding_and_returns_plain_challenge():
    encrypted = _encrypt(b"challenge-text")
    timestamp, nonce = "1700000000", "nonce-1"
    signature = hashlib.sha1("".join(sorted((_TOKEN, timestamp, nonce, encrypted))).encode()).hexdigest()
    app = FastAPI()
    register_wecom_webhook_routes(
        app,
        _Bindings(),
        _Ingress(),
        EnvSecretResolver({
            "TRPC_WECOM_WEBHOOK_TOKEN": _TOKEN,
            "TRPC_WECOM_WEBHOOK_AES_KEY": _AES_KEY
        }),
    )

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            return await client.get(
                f"/webhooks/wecom/{_ACCOUNT}",
                params={
                    "msg_signature": signature,
                    "timestamp": timestamp,
                    "nonce": nonce,
                    "echostr": encrypted
                },
            )

    response = asyncio.run(run())
    assert response.status_code == 200
    assert response.text == "challenge-text"


def test_post_wecom_callback_binds_decrypted_text_and_uses_ingress():
    plaintext = (b"<xml><FromUserName>user-1</FromUserName><CreateTime>1700000000</CreateTime>"
                 b"<MsgType>text</MsgType><Content>hello</Content><MsgId>msg-1</MsgId></xml>")
    encrypted = _encrypt(plaintext)
    timestamp, nonce = "1700000000", "nonce-1"
    signature = hashlib.sha1("".join(sorted((_TOKEN, timestamp, nonce, encrypted))).encode()).hexdigest()
    ingress = _RecordingIngress()
    app = FastAPI()
    register_wecom_webhook_routes(
        app,
        _Bindings(),
        ingress,
        EnvSecretResolver({
            "TRPC_WECOM_WEBHOOK_TOKEN": _TOKEN,
            "TRPC_WECOM_WEBHOOK_AES_KEY": _AES_KEY
        }),
    )

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            return await client.post(
                f"/webhooks/wecom/{_ACCOUNT}",
                params={
                    "msg_signature": signature,
                    "timestamp": timestamp,
                    "nonce": nonce
                },
                content=f"<xml><Encrypt><![CDATA[{encrypted}]]></Encrypt></xml>",
            )

    response = asyncio.run(run())
    assert response.status_code == 200
    assert response.text == "success"
    assert len(ingress.messages) == 1
    assert ingress.messages[0].tenant_id == "tenant_a"
    assert ingress.messages[0].external_message_id == "msg-1"


def test_post_with_valid_signature_but_invalid_decrypted_xml_returns_422():
    encrypted = _encrypt(b"<xml><MsgType>text</MsgType>")
    timestamp, nonce = "1700000000", "nonce-1"
    signature = hashlib.sha1("".join(sorted((_TOKEN, timestamp, nonce, encrypted))).encode()).hexdigest()
    app = FastAPI()
    register_wecom_webhook_routes(
        app,
        _Bindings(),
        _RecordingIngress(),
        EnvSecretResolver({
            "TRPC_WECOM_WEBHOOK_TOKEN": _TOKEN,
            "TRPC_WECOM_WEBHOOK_AES_KEY": _AES_KEY
        }),
    )

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            return await client.post(
                f"/webhooks/wecom/{_ACCOUNT}",
                params={
                    "msg_signature": signature,
                    "timestamp": timestamp,
                    "nonce": nonce
                },
                content=f"<xml><Encrypt><![CDATA[{encrypted}]]></Encrypt></xml>",
            )

    response = asyncio.run(run())
    assert response.status_code == 422
