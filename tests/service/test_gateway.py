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
from trpc_service.web.gateway import ChannelRegistry
from trpc_service.web.gateway import LocalIdempotencyStore
from trpc_service.web.gateway import create_gateway_app
from trpc_service.tenant import ModelEndpoint
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

    def resolve_tenant(self, tenant_id):
        tenant = self.manager.get(tenant_id)
        if tenant is None or tenant.status != TenantStatus.ACTIVE:
            return None
        return tenant

    async def handle(self, tenant_id, channel, inbound):
        self.handled.append((tenant_id, channel, inbound.text))
        return "echo: " + inbound.text


def _build_client():
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
    registry = ChannelRegistry(factories={"fake": lambda cfg: FakeAdapter(cfg)})
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
