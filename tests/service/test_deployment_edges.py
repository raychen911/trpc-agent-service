# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Deployment assembly tests for local and Redis-backed topologies."""

from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient

from trpc_agent_sdk.agents import BaseAgent
from trpc_service.agent import run_worker
from trpc_service.tool import ChannelUserAuthorizationFilter
from trpc_service.tool import apply_tenant_governance
from trpc_service.tenant import AppConfig, ModelEndpoint, Tenant, TenantConfigManager
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

deployment_app = importlib.import_module("trpc_service.web.app")


class EchoAgent(BaseAgent):

    async def _run_async_impl(self, ctx):
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text="ok")]))


def make_tenant(fallback=None):
    return Tenant(
        tenant_id="deploy",
        name="Deploy",
        app_config=AppConfig(default_instruction="tenant instruction"),
        model=ModelEndpoint(model_name="primary", fallback_model=fallback),
    )


def test_create_agent_primary_and_fallback(monkeypatch):
    monkeypatch.setenv("TRPC_AGENT_API_KEY", "test-key")
    monkeypatch.setattr(deployment_app, "_BUDGET_TRACKER", None)
    primary = deployment_app.create_agent(make_tenant())
    fallback = deployment_app.create_agent(make_tenant("backup"))
    assert primary.name == fallback.name == "deploy"
    assert primary.instruction == "tenant instruction"
    assert primary.model.model_retry_config.num_retries == 2
    assert sum(isinstance(item, ChannelUserAuthorizationFilter) for item in primary.filters) == 1

    governed = apply_tenant_governance(primary, make_tenant())
    assert sum(isinstance(item, ChannelUserAuthorizationFilter) for item in governed.filters) == 1


def test_create_agent_supports_tenant_key_reference_and_anthropic(monkeypatch):
    monkeypatch.setenv("TENANT_DEPLOY_MODEL_KEY", "tenant-secret")
    monkeypatch.setattr(deployment_app, "_BUDGET_TRACKER", None)
    tenant = make_tenant()
    tenant.model.provider = "anthropic"
    tenant.model.api_key_env = "TENANT_DEPLOY_MODEL_KEY"
    tenant.model.retry = 4

    agent = deployment_app.create_agent(tenant)

    assert agent.model.__class__.__name__ == "AnthropicModel"
    assert agent.model.model_retry_config.num_retries == 4


def test_create_agent_rejects_unknown_provider(monkeypatch):
    monkeypatch.setattr(deployment_app, "_BUDGET_TRACKER", None)
    tenant = make_tenant()
    tenant.model.provider = "unsupported"
    with pytest.raises(ValueError, match="unsupported tenant model provider"):
        deployment_app.create_agent(tenant)


def test_shared_governance_and_lock_factories_local_and_redis(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setattr(deployment_app, "_BUDGET_TRACKER", None)
    monkeypatch.setattr(deployment_app, "_CONFIRMATION_MANAGER", None)
    monkeypatch.setattr(deployment_app, "_SESSION_LOCK_MANAGER", None)
    assert deployment_app.create_budget_tracker() is deployment_app.create_budget_tracker()
    assert deployment_app.create_confirmation_manager() is deployment_app.create_confirmation_manager()
    assert deployment_app.create_session_lock_manager() is None
    monkeypatch.setenv("REDIS_URL", "redis://unused/0")
    assert deployment_app.create_memory_service(make_tenant()) is not None

    monkeypatch.setattr(deployment_app, "_BUDGET_TRACKER", None)
    monkeypatch.setattr(deployment_app, "_CONFIRMATION_MANAGER", None)
    monkeypatch.setattr(deployment_app, "_SESSION_LOCK_MANAGER", None)
    assert deployment_app.create_budget_tracker().__class__.__name__ == "RedisBudgetTracker"
    assert deployment_app.create_confirmation_manager().__class__.__name__ == "RedisConfirmationManager"
    assert deployment_app.create_session_lock_manager().__class__.__name__ == "RedisSessionLockManager"


def test_build_app_loads_tenants_and_mounts_admin(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("AGENT_QUEUE_ENABLED", "false")
    monkeypatch.setenv("ADMIN_API_KEY", "admin")
    monkeypatch.setattr(deployment_app, "_CONFIRMATION_MANAGER", None)
    monkeypatch.setattr(deployment_app, "_SESSION_LOCK_MANAGER", None)
    monkeypatch.setattr(deployment_app, "load_tenants", lambda _path: [make_tenant()])
    app = deployment_app.build_app(tenants_path="fixture.yml",
                                   agent_factory=lambda item: EchoAgent(name=item.tenant_id))
    client = TestClient(app)
    assert client.get("/healthz").status_code == 200
    assert client.get("/admin/tenants", headers={"x-admin-api-key": "admin"}).json()[0]["tenant_id"] == "deploy"


def test_stream_worker_assembly_and_main(monkeypatch):
    manager = TenantConfigManager()
    manager.register(make_tenant())
    captured = {}

    class FakeTenantWorker:

        def __init__(self, **kwargs):
            captured["tenant"] = kwargs

    class FakeQueue:

        def __init__(self, **kwargs):
            captured["queue"] = kwargs

    class FakeResults:

        def __init__(self, **kwargs):
            captured["results"] = kwargs

    class FakeStreamWorker:

        def __init__(self, **kwargs):
            captured["stream"] = kwargs

    audit_logger = object()
    audit_sink = object()

    monkeypatch.setenv("REDIS_URL", "redis://worker/0")
    monkeypatch.setattr(run_worker, "TenantWorker", FakeTenantWorker)
    monkeypatch.setattr(run_worker, "StreamQueue", FakeQueue)
    monkeypatch.setattr(run_worker, "RedisTaskResultStore", FakeResults)
    monkeypatch.setattr(run_worker, "StreamWorker", FakeStreamWorker)
    monkeypatch.setattr(run_worker, "create_audit_logger", lambda: (audit_logger, audit_sink))
    result = run_worker.build_stream_worker(manager=manager)
    assert isinstance(result, FakeStreamWorker)
    assert captured["queue"]["redis_url"] == "redis://worker/0"
    assert captured["results"]["redis_url"] == "redis://worker/0"
    assert captured["tenant"]["audit_logger"] is audit_logger
    assert result.audit_sink is audit_sink


def test_create_audit_logger_without_mysql(monkeypatch):
    monkeypatch.delenv("MYSQL_URL", raising=False)
    logger, sink = deployment_app.create_audit_logger()
    assert logger is not None
    assert sink is None


async def test_stream_worker_main_runs(monkeypatch):

    class Runner:
        ran = False

        async def run(self):
            self.ran = True

    runner = Runner()
    monkeypatch.setattr(run_worker, "build_stream_worker", lambda **kwargs: runner)
    monkeypatch.setenv("TENANTS_CONFIG", "tenants.yml")
    await run_worker.main()
    assert runner.ran is True
