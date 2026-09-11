"""Local end-to-end contract: WeCom callback → binding → ingress → Worker."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import uuid

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.binding import ChannelBinding
from trpc_service.gateway.app import create_gateway_app

from tests.test_gateway_app import FakeWorkerClient
from tests.tenant_helpers import FakeTenantConfigRepository, make_default_test_configs

_ACCOUNT = "corp-demo"
_TOKEN = "webhook-token"
_AES_KEY = base64.b64encode(bytes(range(32))).decode().rstrip("=")


class _Bindings:

    def __init__(self, binding: ChannelBinding) -> None:
        self.binding = binding

    async def resolve_enabled(self, channel: str, account: str):
        return self.binding if (channel, account) == ("wecom", self.binding.external_account_id) else None


def _encrypt(plaintext: bytes) -> str:
    key = base64.b64decode(_AES_KEY + "=")
    raw = os.urandom(16) + len(plaintext).to_bytes(4, "big") + plaintext + _ACCOUNT.encode()
    pad = 32 - len(raw) % 32
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(raw + bytes([pad]) * pad) + encryptor.finalize()).decode()


def test_wecom_webhook_reaches_existing_tenant_ingress_worker_path():
    binding = ChannelBinding(
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
    worker = FakeWorkerClient()
    app = create_gateway_app(
        worker_client=worker,
        tenant_repository=FakeTenantConfigRepository(make_default_test_configs()),
        channel_binding_repository=_Bindings(binding),
        environ={
            "TRPC_WECOM_WEBHOOK_TOKEN": _TOKEN,
            "TRPC_WECOM_WEBHOOK_AES_KEY": _AES_KEY
        },
    )
    encrypted = _encrypt(b"<xml><FromUserName>user-1</FromUserName><CreateTime>1700000000</CreateTime>"
                         b"<MsgType>text</MsgType><Content>hello</Content><MsgId>msg-1</MsgId></xml>")
    timestamp, nonce = "1700000000", "nonce-1"
    signature = hashlib.sha1("".join(sorted((_TOKEN, timestamp, nonce, encrypted))).encode()).hexdigest()

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
    assert len(worker.chat_tasks) == 1
    assert worker.chat_tasks[0].tenant_id == "tenant_a"
    assert worker.chat_tasks[0].channel == "wecom"
    assert worker.chat_tasks[0].message_id == "msg-1"
