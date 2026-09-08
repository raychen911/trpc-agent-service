# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Integration tests for the gateway webhook endpoint (no live IM accounts)."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from trpc_service.channels import ChannelAdapter
from trpc_service.channels import InboundMessage
from trpc_service.channels import SendResult
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.web.gateway import ChannelRegistry
from trpc_service.web.gateway import LocalIdempotencyStore
from trpc_service.web.gateway import create_gateway_app
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import AppConfig
from trpc_service.tenant import AppInfo
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import TenantStatus
from trpc_service.tenant import WeComChannelConfig


class FakeAdapter(ChannelAdapter):
    channel = "fake"

    def __init__(self, cfg=None):
        self.replies: list[tuple[str, str]] = []

    async def verify_signature(self, payload, headers, query):
        return headers.get("x-signature", "") == "good"

    async def parse_message(self, payload):
        msg = payload if isinstance(payload, dict) else json.loads(payload)
        return InboundMessage(
            channel=self.channel,
            chat_id="c1",
            chat_type="private",
            sender_id="u1",
            message_id=msg.get("message_id", "m1"),
            text=msg.get("text", ""),
        )

    async def send_message(self, outbound):
        return SendResult(ok=True)

    async def send_stream(self, chat_id, stream):
        return SendResult(ok=True)

    async def reply_text(self, inbound, text):
        self.replies.append((inbound.chat_id, text))
        return SendResult(ok=True)


class FakeWorker:

    def __init__(self, manager):
        self.manager = manager
        self.handled: list[tuple[str, str, str]] = []
        self.handled_metadata: list[dict] = []
        self.metrics = EnterpriseMetrics(meter=False)

    def resolve_tenant(self, tenant_id, config_revision=None):
        tenant = (self.manager.get_version(tenant_id, config_revision)
                  if config_revision is not None else self.manager.get(tenant_id))
        if tenant is None or tenant.status != TenantStatus.ACTIVE:
            return None
        return tenant

    async def handle(self, tenant_id, channel, inbound):
        self.handled.append((tenant_id, channel, inbound.text))
        self.handled_metadata.append(dict(inbound.metadata))
        return "echo: " + inbound.text


class ChallengeAdapter(FakeAdapter):

    async def challenge_response(self, payload):
        if isinstance(payload, dict) and "challenge" in payload:
            return {"challenge": payload["challenge"]}
        return None


def _build_client(adapter_type=FakeAdapter):
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant_a",
        name="t",
        model=ModelEndpoint(model_name="gpt-4o"),
    )
    tenant.channel_configs["fake"] = WeComChannelConfig(
        token="token",
        aes_key="aes",
        corp_id="corp",
        agent_id="1",
    )
    manager.register(tenant)

    worker = FakeWorker(manager)
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter_type(cfg)})
    app = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=registry,
        idempotency_store=LocalIdempotencyStore(),
        async_dispatch=False,
    )
    return TestClient(app), worker


def test_gateway_routes_and_dispatches():
    client, worker = _build_client()
    response = client.post(
        "/webhook/tenant_a/fake",
        json={
            "message_id": "m1",
            "text": "hello"
        },
        headers={"x-signature": "good"},
    )
    assert response.status_code == 200
    assert worker.handled == [("tenant_a", "fake", "hello")]
    snapshot = worker.metrics.snapshot("tenant_a")
    callback = next(item for item in snapshot["counters"] if item["name"] == "agent_callback_total")
    assert callback["attributes"]["outcome"] == "success"
    assert next(item for item in snapshot["histograms"] if item["name"] == "agent_callback_duration_ms")["count"] == 1


def test_gateway_routes_binding_to_configured_agent_app():
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant_a",
        name="t",
        model=ModelEndpoint(model_name="gpt-4o"),
        app_config=AppConfig(
            app_list=[AppInfo(app_id="sales"), AppInfo(app_id="support")],
            default_app_id="sales",
        ),
        channel_configs={
            "support_wecom":
            WeComChannelConfig(
                token="token",
                aes_key="aes",
                corp_id="corp",
                agent_id="1",
                agent_app_id="support",
            )
        },
    )
    manager.register(tenant)
    worker = FakeWorker(manager)
    registry = ChannelRegistry(factories={"wecom": lambda cfg: FakeAdapter(cfg)})
    app = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=registry,
        idempotency_store=LocalIdempotencyStore(),
        async_dispatch=False,
    )

    response = TestClient(app).post(
        "/webhook/tenant_a/support_wecom",
        json={
            "message_id": "binding-1",
            "text": "hello"
        },
        headers={"x-signature": "good"},
    )

    assert response.status_code == 200
    assert worker.handled == [("tenant_a", "support_wecom", "hello")]
    assert worker.handled_metadata == [{
        "channel_binding_id": "support_wecom",
        "agent_app_id": "support",
    }]


def test_gateway_records_platform_challenge_separately_from_user_messages():
    client, worker = _build_client(ChallengeAdapter)

    response = client.post("/webhook/tenant_a/fake", json={"challenge": "verify-me"})

    assert response.status_code == 200
    assert response.json() == {"challenge": "verify-me"}
    assert worker.handled == []
    callback = next(item for item in worker.metrics.snapshot("tenant_a")["counters"]
                    if item["name"] == "agent_callback_total")
    assert callback["attributes"]["outcome"] == "challenge"


def test_gateway_rejects_bad_signature():
    client, worker = _build_client()
    response = client.post(
        "/webhook/tenant_a/fake",
        json={
            "message_id": "m1",
            "text": "hello"
        },
        headers={"x-signature": "bad"},
    )
    assert response.status_code == 401
    assert worker.handled == []
    callback = worker.metrics.snapshot("tenant_a")["counters"][0]
    assert callback["attributes"]["outcome"] == "signature_failed"


def test_gateway_rejects_unknown_tenant():
    client, worker = _build_client()
    response = client.post(
        "/webhook/unknown/fake",
        json={
            "message_id": "m1",
            "text": "hello"
        },
        headers={"x-signature": "good"},
    )
    assert response.status_code == 404
    assert worker.handled == []
    callback = worker.metrics.snapshot("unknown")["counters"][0]
    assert callback["attributes"]["outcome"] == "tenant_not_found"


def test_gateway_dedups_redelivered_message():
    client, worker = _build_client()
    payload = {"message_id": "dup1", "text": "hello"}
    headers = {"x-signature": "good"}
    first = client.post("/webhook/tenant_a/fake", json=payload, headers=headers)
    second = client.post("/webhook/tenant_a/fake", json=payload, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert len(worker.handled) == 1
    outcomes = {
        item["attributes"]["outcome"]: item["value"]
        for item in worker.metrics.snapshot("tenant_a")["counters"] if item["name"] == "agent_callback_total"
    }
    assert outcomes == {"success": 1, "duplicate": 1}


def test_internal_test_message_runs_worker_without_channel_adapter():
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant_a",
        name="t",
        model=ModelEndpoint(model_name="gpt-4o"),
    )
    manager.register(tenant)
    worker = FakeWorker(manager)
    app = create_gateway_app(
        manager=manager,
        worker=worker,
        idempotency_store=LocalIdempotencyStore(),
        async_dispatch=False,
        test_api_key="test-secret",
    )

    response = TestClient(app).post(
        "/internal/test/messages/tenant_a",
        json={
            "user_id": "user-1",
            "chat_id": "chat-1",
            "message_id": "sim-1",
            "text": "hello",
        },
        headers={"X-Test-API-Key": "test-secret"},
    )

    assert response.status_code == 200
    assert response.json()["reply"] == "echo: hello"
    assert response.json()["simulated"] is True
    assert worker.handled == [("tenant_a", "qq", "hello")]
    assert worker.handled_metadata == [{"simulated": True, "qq_scope": "c2c"}]


def test_internal_test_message_is_not_available_without_key():
    client, worker = _build_client()

    response = client.post(
        "/internal/test/messages/tenant_a",
        json={
            "user_id": "user-1",
            "chat_id": "chat-1",
            "message_id": "sim-1",
            "text": "hello",
        },
        headers={"X-Test-API-Key": "test-secret"},
    )

    assert response.status_code == 404
    assert worker.handled == []


def test_internal_test_message_rejects_bad_key_and_unknown_tenant():
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant_a",
        name="t",
        model=ModelEndpoint(model_name="gpt-4o"),
    )
    manager.register(tenant)
    worker = FakeWorker(manager)
    app = create_gateway_app(manager=manager, worker=worker, test_api_key="test-secret")
    client = TestClient(app)
    payload = {
        "user_id": "user-1",
        "chat_id": "chat-1",
        "message_id": "sim-1",
        "text": "hello",
    }

    assert client.post(
        "/internal/test/messages/tenant_a",
        json=payload,
        headers={
            "X-Test-API-Key": "wrong"
        },
    ).status_code == 401
    assert client.post(
        "/internal/test/messages/unknown",
        json=payload,
        headers={
            "X-Test-API-Key": "test-secret"
        },
    ).status_code == 404
    assert worker.handled == []


def test_internal_test_message_rejects_queue_channel_and_worker_failure():
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="tenant_a", name="t", model=ModelEndpoint(model_name="gpt-4o")))
    worker = FakeWorker(manager)
    payload = {
        "user_id": "user-1",
        "chat_id": "chat-1",
        "message_id": "sim-1",
        "text": "hello",
    }
    queue_app = create_gateway_app(manager=manager, worker=worker, queue=object(), test_api_key="secret")
    assert TestClient(queue_app).post(
        "/internal/test/messages/tenant_a",
        json=payload,
        headers={
            "X-Test-API-Key": "secret"
        },
    ).status_code == 503

    app = create_gateway_app(manager=manager, worker=worker, test_api_key="secret")
    unsupported = {**payload, "channel": "wecom"}
    assert TestClient(app).post(
        "/internal/test/messages/tenant_a",
        json=unsupported,
        headers={
            "X-Test-API-Key": "secret"
        },
    ).status_code == 400

    async def fail(*args, **kwargs):
        raise RuntimeError("safe failure")

    worker.handle = fail
    assert TestClient(app, raise_server_exceptions=False).post(
        "/internal/test/messages/tenant_a",
        json=payload,
        headers={
            "X-Test-API-Key": "secret"
        },
    ).status_code == 500
