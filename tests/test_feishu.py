from __future__ import annotations

import base64
import hashlib
import json
import os
import time
import unittest
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi.testclient import TestClient

from trpc_service.agent import AgentService
from trpc_service.channels import ChannelDispatcher, FeishuAdapter, make_session_id
from trpc_service.channels.feishu import HTTPResponse
from trpc_service.config import ServiceSettings
from trpc_service.tenant import AgentApp, ChannelBinding, TenantConfig, TenantRegistry
from trpc_service.web.app import ServiceRuntime, create_app

APP_ID = "cli_test"
VERIFY_TOKEN = "verification-token"
APP_SECRET = "app-secret"
ENCRYPT_KEY = "encrypt-key"


class FakeHTTPClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []

    async def post(self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse:
        self.calls.append((url, dict(headers), dict(payload)))
        if url.endswith("/open-apis/auth/v3/tenant_access_token/internal"):
            return _response({"code": 0, "tenant_access_token": "tenant-token", "expire": 7200})
        return _response({"code": 0, "data": {"message_id": "reply-message"}})


def _response(payload: dict[str, Any], status: int = 200) -> HTTPResponse:
    return HTTPResponse(status, {}, json.dumps(payload).encode())


def _event(sender_type: str = "user") -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "event-1",
            "event_type": "im.message.receive_v1",
            "app_id": APP_ID,
            "token": VERIFY_TOKEN,
        },
        "event": {
            "sender": {"sender_id": {"open_id": "ou_user"}, "sender_type": sender_type},
            "message": {
                "message_id": "om_message",
                "chat_id": "oc_chat",
                "chat_type": "p2p",
                "message_type": "text",
                "content": json.dumps({"text": "你好，飞书"}, ensure_ascii=False),
            },
        },
    }


def _binding(encrypted: bool = False) -> ChannelBinding:
    return ChannelBinding(
        channel="feishu",
        account_id=APP_ID,
        verify_token=f"literal://{VERIFY_TOKEN}",
        secret_ref=f"literal://{APP_SECRET}",
        encrypt_key_ref=f"literal://{ENCRYPT_KEY}" if encrypted else None,
        api_base_url="https://feishu.test",
    )


class FeishuAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_url_challenge(self) -> None:
        adapter = FeishuAdapter()
        body = json.dumps({"type": "url_verification", "token": VERIFY_TOKEN, "challenge": "challenge-value"}).encode()
        with self.assertLogs("trpc_service.channels.feishu", level="INFO"):
            callback = adapter.handle_callback("acme", _binding(), {}, body, "trace-1")
        self.assertEqual("challenge-value", callback.challenge)

    async def test_parse_text_event_and_ignore_bot(self) -> None:
        adapter = FeishuAdapter()
        body = json.dumps(_event(), ensure_ascii=False).encode()
        with self.assertLogs("trpc_service.channels.feishu", level="INFO") as received_logs:
            callback = adapter.handle_callback("acme", _binding(), {}, body, "trace-1")
        assert callback.message is not None
        self.assertEqual("你好，飞书", callback.message.text)
        self.assertEqual("ou_user", callback.message.external_user_id)
        self.assertEqual("direct", callback.message.chat_type)
        received = json.loads(received_logs.records[0].getMessage())
        self.assertEqual("feishu.received", received["event"])
        self.assertEqual("trace-1", received["trace_id"])

        bot_body = json.dumps(_event("bot"), ensure_ascii=False).encode()
        with self.assertLogs("trpc_service.channels.feishu", level="INFO") as ignored_logs:
            self.assertTrue(adapter.handle_callback("acme", _binding(), {}, bot_body, "trace-2").ignored)
        ignored = [json.loads(record.getMessage()) for record in ignored_logs.records]
        self.assertEqual(["feishu.received", "feishu.ignored"], [record["event"] for record in ignored])
        self.assertEqual("bot_sender", ignored[-1]["reason"])

    async def test_access_token_is_cached_and_reply_uses_message_id(self) -> None:
        client = FakeHTTPClient()
        adapter = FeishuAdapter(client)
        inbound = adapter.handle_callback(
            "acme", _binding(), {}, json.dumps(_event(), ensure_ascii=False).encode(), "trace-1"
        ).message
        assert inbound is not None
        with self.assertLogs("trpc_service.channels.feishu", level="INFO") as reply_logs:
            for index in range(2):
                outbound = adapter.to_outbound(inbound, f"reply {index}")[0]
                receipt = await adapter.send(outbound, _binding())
                self.assertTrue(receipt["ok"])

        auth_calls = [call for call in client.calls if "/auth/" in call[0]]
        reply_calls = [call for call in client.calls if call[0].endswith("/om_message/reply")]
        self.assertEqual(1, len(auth_calls))
        self.assertEqual(2, len(reply_calls))
        self.assertEqual(APP_SECRET, auth_calls[0][2]["app_secret"])
        self.assertEqual("Bearer tenant-token", reply_calls[0][1]["Authorization"])
        reply_uuid = reply_calls[0][2]["uuid"]
        self.assertEqual(reply_uuid, reply_calls[1][2]["uuid"])
        self.assertEqual(reply_uuid, str(UUID(reply_uuid)))
        self.assertLessEqual(len(reply_uuid), 50)
        reply_records = [json.loads(record.getMessage()) for record in reply_logs.records]
        self.assertEqual(["feishu.reply_succeeded"] * 2, [record["event"] for record in reply_records])

    async def test_reply_failure_logs_only_sanitized_field_violations(self) -> None:
        class FailureHTTPClient(FakeHTTPClient):
            async def post(
                self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, Any]
            ) -> HTTPResponse:
                self.calls.append((url, dict(headers), dict(payload)))
                if url.endswith("/open-apis/auth/v3/tenant_access_token/internal"):
                    return _response({"code": 0, "tenant_access_token": "tenant-token", "expire": 7200})
                return _response(
                    {
                        "code": 99992402,
                        "msg": "field validation failed",
                        "error": {
                            "field_violations": [
                                {
                                    "field": "uuid",
                                    "description": "api_key=real-secret\nlength must be <= 50",
                                }
                            ]
                        },
                    },
                    status=400,
                )

        adapter = FeishuAdapter(FailureHTTPClient())
        inbound = adapter.handle_callback(
            "acme", _binding(), {}, json.dumps(_event(), ensure_ascii=False).encode(), "trace-failure"
        ).message
        assert inbound is not None
        outbound = adapter.to_outbound(inbound, "reply")[0]

        with self.assertLogs("trpc_service.channels.feishu", level="WARNING") as failure_logs:
            receipt = await adapter.send(outbound, _binding())

        self.assertFalse(receipt["ok"])
        raw_log = failure_logs.records[-1].getMessage()
        self.assertNotIn("real-secret", raw_log)
        failed = json.loads(raw_log)
        self.assertEqual("feishu.reply_failed", failed["event"])
        self.assertEqual(99992402, failed["code"])
        self.assertEqual("api_key=[REDACTED]\nlength must be <= 50", failed["field_violations"][0]["description"])

    async def test_encrypted_signed_callback(self) -> None:
        body = _encrypted_body(_event(), ENCRYPT_KEY)
        timestamp = str(int(time.time()))
        nonce = "nonce-value"
        signature = hashlib.sha256(timestamp.encode() + nonce.encode() + ENCRYPT_KEY.encode() + body).hexdigest()
        headers = {
            "X-Lark-Request-Timestamp": timestamp,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": signature,
        }
        callback = FeishuAdapter().handle_callback("acme", _binding(encrypted=True), headers, body, "trace-1")
        assert callback.message is not None
        self.assertEqual("om_message", callback.message.external_message_id)

        with self.assertRaises(ValueError):
            FeishuAdapter().handle_callback(
                "acme", _binding(encrypted=True), {**headers, "X-Lark-Signature": "invalid"}, body, "trace-1"
            )

    async def test_runtime_enqueues_and_sends_reply(self) -> None:
        client = FakeHTTPClient()
        adapter = FeishuAdapter(client)
        binding = _binding()
        config = TenantConfig(
            tenant_id="acme",
            name="Acme",
            apps={"default": AgentApp("default", "assistant")},
            channels={"feishu": binding},
        )
        registry = TenantRegistry([config])
        dispatcher = ChannelDispatcher(
            {"feishu": adapter},
            binding_resolver=lambda tenant, channel, account: registry.resolve_binding(tenant, channel, account)[1],
        )
        service = AgentService(registry, dispatcher)
        runtime = ServiceRuntime(ServiceSettings(), registry, service)
        body = json.dumps(_event(), ensure_ascii=False, separators=(",", ":")).encode()
        with self.assertLogs("trpc_service.channels.feishu", level="INFO") as enqueue_logs:
            result = await runtime.ingest("acme", "feishu", json.loads(body), {}, raw_body=body)
        self.assertTrue(result)
        enqueue_records = [json.loads(record.getMessage()) for record in enqueue_logs.records]
        self.assertEqual(["feishu.received", "feishu.enqueued"], [record["event"] for record in enqueue_records])
        self.assertEqual("accepted", enqueue_records[-1]["result"])
        await service.process_one()
        self.assertEqual(1, len(dispatcher.deliveries))
        session_id = make_session_id("acme", "feishu", "oc_chat", "direct", ServiceSettings().session_hmac_key)
        self.assertEqual(2, len(await service.sessions.events("acme", session_id)))
        self.assertTrue(any(call[0].endswith("/om_message/reply") for call in client.calls))

    def test_http_url_challenge(self) -> None:
        binding = _binding()
        config = TenantConfig(
            tenant_id="acme",
            name="Acme",
            apps={"default": AgentApp("default", "assistant")},
            channels={"feishu": binding},
        )
        registry = TenantRegistry([config])
        adapter = FeishuAdapter(FakeHTTPClient())
        dispatcher = ChannelDispatcher(
            {"feishu": adapter},
            binding_resolver=lambda tenant, channel, account: registry.resolve_binding(tenant, channel, account)[1],
        )
        runtime = ServiceRuntime(ServiceSettings(), registry, AgentService(registry, dispatcher))
        with TestClient(create_app(runtime)) as client:
            response = client.post(
                "/webhook/acme/feishu",
                json={
                    "type": "url_verification",
                    "token": VERIFY_TOKEN,
                    "challenge": "challenge-over-http",
                },
            )
        self.assertEqual(200, response.status_code)
        self.assertEqual({"challenge": "challenge-over-http"}, response.json())


def _encrypted_body(payload: dict[str, Any], key_text: str) -> bytes:
    try:
        from cryptography.hazmat.primitives import padding
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError as exc:
        raise unittest.SkipTest("cryptography is not installed") from exc
    plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    key = hashlib.sha256(key_text.encode()).digest()
    iv = os.urandom(16)
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    encrypted = encryptor.update(padded) + encryptor.finalize()
    return json.dumps({"encrypt": base64.b64encode(iv + encrypted).decode()}, separators=(",", ":")).encode()


if __name__ == "__main__":
    unittest.main()
