# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import httpx
import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import GenerateContentResponseUsageMetadata
from trpc_agent_sdk.types import Part

from trpc_service.agent import AgentWorker
from trpc_service.config import ServiceSettings
from trpc_service.gateway.idempotency import InMemoryIdempotencyStore
from trpc_service.gateway.service import GatewayService
from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.web import build_container
from trpc_service.web import create_app


class FakeRuntime:

    def __init__(self, tenant, app):
        self.tenant = tenant
        self.app = app

    async def run(self, **kwargs):
        yield Event(
            author="assistant",
            content=Content(parts=[Part.from_text(text=f"echo:{kwargs['text']}")]),
            usage_metadata=GenerateContentResponseUsageMetadata(
                prompt_token_count=12,
                candidates_token_count=3,
                total_token_count=15,
            ),
        )


class CapturingAuditSink:

    def __init__(self):
        self.events = []

    async def write(self, event):
        self.events.append(event)


class FakeRuntimeManager:

    def __init__(self, tenant):
        self.tenant = tenant

    async def get(self, tenant_id, app_id, config_version=None):
        assert tenant_id == self.tenant.tenant_id
        assert config_version == self.tenant.version
        return FakeRuntime(self.tenant, self.tenant.apps[app_id])


@pytest.mark.asyncio
async def test_chat_http_main_path(tenant_config):
    tenant_config.apps["assistant"].model.input_cost_per_million_usd = 1.0
    tenant_config.apps["assistant"].model.output_cost_per_million_usd = 2.0
    settings = ServiceSettings(config_file="unused")
    container = build_container(ServiceSettings(), [tenant_config])
    container.settings = settings
    audit = CapturingAuditSink()
    container.audit = audit
    fake_runtimes = FakeRuntimeManager(tenant_config)
    container.gateway = GatewayService(
        container.registry,
        AgentWorker(fake_runtimes, InMemorySessionExecutionGuard()),
        InMemoryIdempotencyStore(),
    )
    app = create_app(container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/v1/chat",
                                     json={
                                         "tenant_id": "tenant-a",
                                         "app_id": "assistant",
                                         "user_id": "student",
                                         "session_id": "lesson-1",
                                         "message": "hello",
                                     })
    assert response.status_code == 200
    assert response.json()["text"] == "echo:hello"
    assert response.json()["user_id"] != "student"
    assert response.json()["usage"] == {
        "input_tokens": 12,
        "output_tokens": 3,
        "total_tokens": 15,
        "cost_usd": pytest.approx(0.000018),
    }
    assert len(audit.events) == 1
    assert audit.events[0].input_tokens == response.json()["usage"]["input_tokens"]
    assert audit.events[0].output_tokens == response.json()["usage"]["output_tokens"]
    assert audit.events[0].cost_usd == pytest.approx(response.json()["usage"]["cost_usd"])


@pytest.mark.asyncio
async def test_admin_requires_token_outside_development(tenant_config):
    settings = ServiceSettings(environment="production", admin_token="admin-secret")
    container = build_container(ServiceSettings(), [tenant_config])
    container.settings = settings
    app = create_app(container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        unauthorized = await client.get("/api/v1/admin/tenants")
        authorized = await client.get("/api/v1/admin/tenants", headers={"X-Admin-Token": "admin-secret"})
    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
