# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""QQ Bot adapter and gateway integration tests."""

from __future__ import annotations

import json

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from trpc_service.channels import CHAT_GROUP
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.channels import InboundMessage
from trpc_service.channels import OutboundMessage
from trpc_service.channels import QQAdapter
from trpc_service.channels._qq import QQ_EVENT_C2C
from trpc_service.channels._qq import QQ_EVENT_DM
from trpc_service.channels._qq import QQ_EVENT_GROUP
from trpc_service.channels._qq import QQ_EVENT_GROUP_AT
from trpc_service.channels._qq import QQ_EVENT_GUILD_AT
from trpc_service.channels._qq import QQ_SCOPE_C2C
from trpc_service.channels._qq import QQ_SCOPE_DM
from trpc_service.channels._qq import QQ_SCOPE_GROUP
from trpc_service.channels._qq import QQ_SCOPE_GUILD
from trpc_service.channels._qq import sign_validation_response
from trpc_service.channels._qq import verify_webhook_signature
from trpc_service.web.gateway import ChannelRegistry
from trpc_service.web.gateway import LocalIdempotencyStore
from trpc_service.web.gateway import create_gateway_app
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import TenantStatus

APP_SECRET = "qq-app-secret"
TIMESTAMP = "1750000000"


def _private_key(secret: str = APP_SECRET) -> Ed25519PrivateKey:
    raw = secret.encode("utf-8")
    seed = (raw * ((32 + len(raw) - 1) // len(raw)))[:32]
    return Ed25519PrivateKey.from_private_bytes(seed)


def _signed_headers(raw_body: bytes, *, timestamp: str = TIMESTAMP, secret: str = APP_SECRET) -> dict[str, str]:
    signature = _private_key(secret).sign(timestamp.encode("utf-8") + raw_body).hex()
    return {
        "X-Signature-Timestamp": timestamp,
        "X-Signature-Ed25519": signature,
        "Content-Type": "application/json",
    }


def _event(event_type: str, data: dict) -> dict:
    return {"op": 0, "id": "event-1", "t": event_type, "d": data}


def test_qq_validation_challenge_signature():
    response = sign_validation_response(APP_SECRET, "plain-token", "1700000000")

    assert response["plain_token"] == "plain-token"
    _private_key().public_key().verify(bytes.fromhex(response["signature"]), b"1700000000plain-token")
    with pytest.raises(ValueError, match="AppSecret"):
        sign_validation_response("", "plain-token", "1700000000")


async def test_qq_adapter_closes_owned_lazy_http_client(monkeypatch):
    monkeypatch.delenv("ALL_PROXY", raising=False)
    monkeypatch.delenv("all_proxy", raising=False)
    adapter = QQAdapter(app_id="app", app_secret=APP_SECRET)
    client = adapter._client()
    await adapter.close()
    assert client.is_closed is True


async def test_qq_adapter_challenge_response_validation():
    adapter = QQAdapter(app_secret=APP_SECRET)
    challenge = await adapter.challenge_response({"op": 13, "d": {"plain_token": "plain", "event_ts": "123"}})
    assert challenge == sign_validation_response(APP_SECRET, "plain", "123")
    assert await adapter.challenge_response({"op": 0}) is None

    with pytest.raises(ValueError, match="missing 'd'"):
        await adapter.challenge_response({"op": "13", "d": []})
    with pytest.raises(ValueError, match="plain_token"):
        await adapter.challenge_response({"op": 13, "d": {}})


async def test_qq_verifies_exact_raw_body_and_case_insensitive_headers():
    raw = b'{ "op": 0, "d": {"id": "m1"} }'
    headers = _signed_headers(raw)
    adapter = QQAdapter(app_secret=APP_SECRET)

    assert verify_webhook_signature(APP_SECRET, raw, TIMESTAMP, headers["X-Signature-Ed25519"])
    assert await adapter.verify_request(raw, {"op": 0}, headers, {}) is True
    assert await adapter.verify_request(raw + b" ", {"op": 0}, headers, {}) is False
    assert verify_webhook_signature(APP_SECRET, raw, "", "") is False
    assert verify_webhook_signature(APP_SECRET, raw, TIMESTAMP, "not-hex") is False

    canonical = {"op": 0, "d": {"id": "m1"}}
    canonical_raw = json.dumps(canonical, separators=(",", ":")).encode()
    assert await adapter.verify_signature(canonical, _signed_headers(canonical_raw), {}) is True


@pytest.mark.parametrize(
    ("event_type", "event_data", "expected"),
    [
        (
            QQ_EVENT_C2C,
            {
                "id":
                "c2c-1",
                "content":
                "  你好  ",
                "timestamp":
                "2026-01-01T00:00:00Z",
                "author": {
                    "user_openid": "user-openid",
                    "username": "Alice"
                },
                "attachments": [
                    {
                        "url": "https://img",
                        "content_type": "image/png"
                    },
                    {
                        "url": "https://file",
                        "content_type": "application/pdf"
                    },
                    "ignored",
                ],
                "msg_elements": [
                    {
                        "attachments": [{
                            "url": "https://img",
                            "content_type": "image/png"
                        }, {
                            "voice_wav_url": "https://voice"
                        }]
                    },
                    "ignored",
                ],
            },
            ("user-openid", "user-openid", CHAT_PRIVATE, QQ_SCOPE_C2C),
        ),
        (
            QQ_EVENT_GROUP_AT,
            {
                "id": "group-at-1",
                "content": "@机器人 hello",
                "group_openid": "group-openid",
                "author": {
                    "member_openid": "member-openid"
                },
            },
            ("member-openid", "group-openid", CHAT_GROUP, QQ_SCOPE_GROUP),
        ),
        (
            QQ_EVENT_GROUP,
            {
                "id": "group-1",
                "content": "hello",
                "group_openid": "group-openid",
                "author": {
                    "member_openid": "member-openid"
                },
            },
            ("member-openid", "group-openid", CHAT_GROUP, QQ_SCOPE_GROUP),
        ),
        (
            QQ_EVENT_GUILD_AT,
            {
                "id": "guild-1",
                "content": "hello",
                "guild_id": "guild-id",
                "channel_id": "channel-id",
                "author": {
                    "id": "guild-user"
                },
            },
            ("guild-user", "channel-id", CHAT_GROUP, QQ_SCOPE_GUILD),
        ),
        (
            QQ_EVENT_DM,
            {
                "id": "dm-1",
                "content": "hello",
                "guild_id": "guild-id",
                "author": {
                    "id": "guild-user"
                },
            },
            ("guild-user", "guild-id", CHAT_PRIVATE, QQ_SCOPE_DM),
        ),
    ],
)
async def test_qq_parse_supported_message_events(event_type, event_data, expected):
    inbound = await QQAdapter().parse_message(_event(event_type, event_data))

    sender_id, chat_id, chat_type, scope = expected
    assert (inbound.sender_id, inbound.chat_id, inbound.chat_type) == (sender_id, chat_id, chat_type)
    assert inbound.channel == "qq"
    assert inbound.message_id == event_data["id"]
    assert inbound.text == event_data["content"].strip()
    assert inbound.metadata["qq_scope"] == scope
    assert inbound.raw["t"] == event_type
    if event_type == QQ_EVENT_C2C:
        assert inbound.images == ["https://img"]
        assert inbound.files == ["https://file", "https://voice"]
        assert inbound.metadata["sender_name"] == "Alice"


@pytest.mark.parametrize(
    "event_data",
    [
        {
            "id": "m1",
            "group_openid": "g1",
            "author": {
                "member_openid": "u1"
            }
        },
        {
            "id": "m2",
            "author": {
                "user_openid": "u2"
            }
        },
        {
            "id": "m3",
            "channel_id": "c3",
            "author": {
                "id": "u3"
            }
        },
        {
            "id": "m4",
            "guild_id": "g4",
            "author": {
                "id": "u4"
            }
        },
    ],
)
async def test_qq_infers_message_scope_for_bare_event_fixtures(event_data):
    inbound = await QQAdapter().parse_message(event_data)
    assert inbound.message_id == event_data["id"]


async def test_qq_rejects_unsupported_or_incomplete_events():
    adapter = QQAdapter()
    with pytest.raises(ValueError, match="opcode"):
        await adapter.parse_message({"op": 13, "d": {}})
    with pytest.raises(ValueError, match="must be an object"):
        await adapter.parse_message({"op": 0, "d": []})
    with pytest.raises(ValueError, match="unsupported QQ message"):
        await adapter.parse_message({"op": 0, "t": "READY", "d": {}})
    with pytest.raises(ValueError, match="missing sender"):
        await adapter.parse_message(_event(QQ_EVENT_C2C, {"id": "m1", "author": {}}))


async def test_qq_fetches_token_splits_reply_and_caches_token():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/app/getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "generated", "expires_in": "7200"})
        return httpx.Response(200, json={"id": f"reply-{len(requests)}"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = QQAdapter(
        app_id="app-id",
        app_secret=APP_SECRET,
        http_client=client,
        message_limit_chars=3,
        max_retries=0,
    )
    inbound = InboundMessage(
        channel="qq",
        chat_id="group/id",
        chat_type=CHAT_GROUP,
        sender_id="user",
        message_id="source-message",
        metadata={"qq_scope": QQ_SCOPE_GROUP},
    )

    result = await adapter.reply_text(inbound, "abcdefg")

    assert result.ok is True
    token_requests = [request for request in requests if request.url.path.endswith("/app/getAppAccessToken")]
    message_requests = [request for request in requests if "/messages" in request.url.path]
    assert len(token_requests) == 1
    assert json.loads(token_requests[0].content) == {"appId": "app-id", "clientSecret": APP_SECRET}
    assert [request.url.path for request in message_requests] == ["/v2/groups/group/id/messages"] * 3
    assert [json.loads(request.content)["content"] for request in message_requests] == ["abc", "def", "g"]
    assert [json.loads(request.content)["msg_seq"] for request in message_requests] == [1, 2, 3]
    assert all(json.loads(request.content)["msg_id"] == "source-message" for request in message_requests)
    assert all(request.headers["authorization"] == "QQBot generated" for request in message_requests)
    await client.aclose()


@pytest.mark.parametrize(
    ("scope", "target", "path", "has_msg_type"),
    [
        (QQ_SCOPE_C2C, "user/id", "/v2/users/user/id/messages", True),
        (QQ_SCOPE_GROUP, "group/id", "/v2/groups/group/id/messages", True),
        (QQ_SCOPE_GUILD, "channel/id", "/channels/channel/id/messages", False),
        (QQ_SCOPE_DM, "guild/id", "/dms/guild/id/messages", False),
    ],
)
async def test_qq_send_routes_each_conversation_scope(scope, target, path, has_msg_type):
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"message_id": "sent-1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = QQAdapter(access_token="static-token", http_client=client)
    result = await adapter.send_message(
        OutboundMessage(
            chat_id=target,
            text="hello",
            metadata={
                "qq_scope": scope,
                "reply_to_message_id": "origin",
                "msg_seq": 7,
            },
        ))

    assert result.ok is True and result.message_id == "sent-1"
    assert requests[0].url.path == path
    body = json.loads(requests[0].content)
    assert body["content"] == "hello" and body["msg_id"] == "origin"
    assert ("msg_type" in body) is has_msg_type
    if has_msg_type:
        assert body["msg_type"] == 0 and body["msg_seq"] == 7
    await client.aclose()


async def test_qq_send_stream_empty_and_configuration_errors(monkeypatch):
    adapter = QQAdapter()
    assert (await adapter.send_message(OutboundMessage(chat_id="u",
                                                       text=""))).error == "QQ message text must not be empty"
    assert (await adapter.send_message(OutboundMessage(chat_id="u", text="hello"))).ok is False
    static = QQAdapter(access_token="token")
    unsupported = await static.send_message(
        OutboundMessage(chat_id="u", text="hello", metadata={"qq_scope": "unsupported"}))
    assert unsupported.ok is False and "unsupported QQ message scope" in unsupported.error

    async def empty_stream():
        if False:
            yield ""

    assert (await adapter.send_stream("u", empty_stream())).ok is True

    sent = []

    async def fake_send(outbound):
        sent.append(outbound)
        from trpc_service.channels import SendResult
        return SendResult(ok=True, message_id="stream-id")

    monkeypatch.setattr(static, "send_message", fake_send)

    async def chunks():
        yield "hello"
        yield " world"

    assert (await static.send_stream("u", chunks())).message_id == "stream-id"
    assert sent[0].text == "hello world"


async def test_qq_send_hook_captures_platform_payload_without_credentials():
    payloads = []

    async def capture(body):
        payloads.append(body)
        from trpc_service.channels import SendResult
        return SendResult(ok=True, message_id="captured")

    adapter = QQAdapter(send_hook=capture)
    result = await adapter.send_message(
        OutboundMessage(
            chat_id="group",
            text="reply",
            metadata={
                "qq_scope": QQ_SCOPE_GROUP,
                "reply_to_message_id": "origin"
            },
        ))

    assert result.ok is True and result.message_id == "captured"
    assert payloads == [{
        "content": "reply",
        "msg_type": 0,
        "msg_seq": 1,
        "msg_id": "origin",
    }]


async def test_qq_transport_retry_business_error_and_malformed_response():
    calls = 0

    async def retry_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"message": "busy"})
        return httpx.Response(200, json={"code": 40001, "message": "denied"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(retry_handler))
    adapter = QQAdapter(access_token="token", http_client=client, max_retries=1, retry_backoff=0)
    result = await adapter.send_message(OutboundMessage(chat_id="u", text="hello"))
    assert result.ok is False and "qq code=40001 denied" in result.error
    assert calls == 2
    await client.aclose()

    async def malformed_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    malformed_client = httpx.AsyncClient(transport=httpx.MockTransport(malformed_handler))
    malformed = QQAdapter(access_token="token", http_client=malformed_client, max_retries=0)
    result = await malformed.send_message(OutboundMessage(chat_id="u", text="hello"))
    assert result.ok is False and "JSON object" in result.error
    await malformed_client.aclose()


async def test_qq_token_failures_and_expiry_fallback():
    responses = iter([
        httpx.Response(503, json={"message": "busy"}),
        httpx.Response(200, json={}),
        httpx.Response(200, json={
            "access_token": "token",
            "expires_in": "invalid"
        }),
        httpx.Response(200, json={"id": "sent"}),
    ])

    async def handler(request: httpx.Request) -> httpx.Response:
        return next(responses)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    failed_fetch = QQAdapter(app_id="app", app_secret=APP_SECRET, http_client=client, max_retries=0)
    assert (await failed_fetch.send_message(OutboundMessage(chat_id="u", text="one"))).ok is False
    assert (await failed_fetch.send_message(OutboundMessage(chat_id="u", text="two"))).ok is False
    assert (await failed_fetch.send_message(OutboundMessage(chat_id="u", text="three"))).ok is True
    await client.aclose()


class FakeQQWorker:

    def __init__(self, manager):
        self.manager = manager
        self.handled = []

    def resolve_tenant(self, tenant_id, config_revision=None):
        tenant = (self.manager.get_version(tenant_id, config_revision)
                  if config_revision is not None else self.manager.get(tenant_id))
        if tenant is None or tenant.status != TenantStatus.ACTIVE:
            return None
        return tenant

    async def handle(self, tenant_id, channel, inbound):
        self.handled.append((tenant_id, channel, inbound.text))
        return "QQ reply"


def _qq_gateway(http_client: httpx.AsyncClient):
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant_qq",
        name="QQ tenant",
        model=ModelEndpoint(model_name="test-model"),
        channel_configs={
            "qq": QQChannelConfig(app_id="app-id", secret=APP_SECRET),
        },
    )
    manager.register(tenant)
    worker = FakeQQWorker(manager)
    registry = ChannelRegistry(
        factories={
            "qq":
            lambda cfg: QQAdapter(
                app_id=cfg.app_id or "",
                app_secret=cfg.secret.get_secret_value() if cfg.secret else "",
                http_client=http_client,
                max_retries=0,
            )
        })
    app = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=registry,
        idempotency_store=LocalIdempotencyStore(),
        async_dispatch=False,
    )
    return app, worker


async def test_qq_gateway_challenge_signed_callback_reply_and_deduplication():
    openapi_requests = []

    async def openapi_handler(request: httpx.Request) -> httpx.Response:
        openapi_requests.append(request)
        if request.url.path.endswith("/app/getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "gateway-token", "expires_in": 7200})
        return httpx.Response(200, json={"id": "reply-id"})

    qq_client = httpx.AsyncClient(transport=httpx.MockTransport(openapi_handler))
    app, worker = _qq_gateway(qq_client)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        challenge = await client.post(
            "/webhook/tenant_qq/qq",
            json={
                "op": 13,
                "d": {
                    "plain_token": "plain",
                    "event_ts": "123"
                }
            },
        )
        assert challenge.status_code == 200
        assert challenge.json() == sign_validation_response(APP_SECRET, "plain", "123")

        payload = _event(QQ_EVENT_C2C, {
            "id": "message-1",
            "content": "hello agent",
            "author": {
                "user_openid": "openid-1"
            },
        })
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        first = await client.post("/webhook/tenant_qq/qq", content=raw, headers=_signed_headers(raw))
        duplicate = await client.post("/webhook/tenant_qq/qq", content=raw, headers=_signed_headers(raw))

    assert first.status_code == duplicate.status_code == 200
    assert first.json() == duplicate.json() == {"op": 12, "d": 0}
    assert worker.handled == [("tenant_qq", "qq", "hello agent")]
    messages = [request for request in openapi_requests if "/messages" in request.url.path]
    assert len(messages) == 1
    assert messages[0].url.path == "/v2/users/openid-1/messages"
    assert json.loads(messages[0].content) == {
        "content": "QQ reply",
        "msg_type": 0,
        "msg_seq": 1,
        "msg_id": "message-1",
    }
    await qq_client.aclose()


async def test_qq_gateway_rejects_bad_signature_and_malformed_challenge():
    qq_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    app, worker = _qq_gateway(qq_client)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        malformed = await client.post("/webhook/tenant_qq/qq", json={"op": 13, "d": {}})
        unsigned = await client.post(
            "/webhook/tenant_qq/qq",
            json=_event(QQ_EVENT_C2C, {
                "id": "m",
                "author": {
                    "user_openid": "u"
                }
            }),
        )

    assert malformed.status_code == 400
    assert "challenge failed" in malformed.json()["error"]
    assert unsigned.status_code == 401
    assert worker.handled == []
    await qq_client.aclose()


def test_default_channel_registry_builds_qq_adapter_without_secret_leakage():
    tenant = Tenant(
        tenant_id="tenant",
        name="QQ",
        model=ModelEndpoint(model_name="model"),
        channel_configs={"qq": QQChannelConfig(app_id="app", secret=APP_SECRET)},
    )
    adapter = ChannelRegistry().get(tenant, "qq")

    assert isinstance(adapter, QQAdapter)
    assert adapter.app_id == "app"
    assert adapter.app_secret == APP_SECRET
    assert APP_SECRET not in repr(tenant.channel_configs["qq"])
