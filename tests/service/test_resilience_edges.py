# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Failure-path tests that keep the enterprise CI gate honest.

These cases exercise adapter construction, shared backend lifecycle, retry
reservations and governance edge behavior that happy-path tests do not reach.
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.context import new_agent_context
from trpc_service.log import AuditLogger
from trpc_service.channels import ChannelAdapter, InboundMessage, SendResult
from trpc_service.web.gateway import (
    ChannelRegistry,
    LocalIdempotencyStore,
    RedisIdempotencyStore,
    build_idempotency_store,
    create_gateway_app,
)
from trpc_service.tool import (
    ConfirmationManager,
    ModelPricing,
    RedisBudgetTracker,
    RedisConfirmationManager,
    SensitiveDataRedactor,
    ToolAllowlistFilter,
    ToolConfirmationRequired,
    ToolOutputRedactionFilter,
)
from trpc_service.tool._filters import build_governance
from trpc_service.workspace import TenantStorageRouter
from trpc_service.tenant import (
    BudgetConfig,
    DesensitizeRule,
    DingTalkChannelConfig,
    FeishuChannelConfig,
    ModelEndpoint,
    StorageBackendConfig,
    Tenant,
    TenantConfigManager,
    ToolPermissions,
    WeComChannelConfig,
    WechatCustomerServiceChannelConfig,
)
from trpc_service.agent import RedisSessionLockManager, RedisTaskResultStore
import trpc_service.workspace._router as router_module


def tenant(storage: StorageBackendConfig | None = None) -> Tenant:
    return Tenant(
        tenant_id="edge",
        name="Edge",
        model=ModelEndpoint(model_name="model"),
        storage_config=storage or StorageBackendConfig(),
    )


def edge_channel_config() -> WeComChannelConfig:
    return WeComChannelConfig(token="token", aes_key="aes", corp_id="corp", agent_id="1")


class ClosingService:

    def __init__(self) -> None:
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1


async def test_storage_router_all_builtin_routes_environment_and_close(monkeypatch):
    built: list[tuple[str, str]] = []

    def builder(kind):

        def create(url):
            built.append((kind, url))
            return ClosingService()

        return create

    monkeypatch.setattr(router_module, "_redis_session_builder", builder("session-redis"))
    monkeypatch.setattr(router_module, "_mysql_session_builder", builder("session-mysql"))
    monkeypatch.setattr(router_module, "_redis_memory_builder", builder("memory-redis"))
    monkeypatch.setattr(router_module, "_mysql_memory_builder", builder("memory-mysql"))
    monkeypatch.setenv("REDIS_URL", "redis://shared/0")
    monkeypatch.setenv("MYSQL_URL", "mysql+aiomysql://user:pass@shared/db")

    router = TenantStorageRouter()
    default_service = router.session_service(None)
    assert router.session_service(None) is default_service
    assert router.session_service(tenant()) is default_service

    redis_tenant = tenant(
        StorageBackendConfig(session_backend="redis", memory_backend="redis", redis_url=SecretStr("redis://tenant/1")))
    mysql_tenant = tenant(
        StorageBackendConfig(session_backend="mysql",
                             memory_backend="mysql",
                             mysql_url=SecretStr("mysql+aiomysql://user:pass@tenant/db")))
    redis_session = router.session_service(redis_tenant)
    redis_memory = router.memory_service(redis_tenant)
    mysql_session = router.session_service(mysql_tenant)
    mysql_memory = router.memory_service(mysql_tenant)

    assert ("session-redis", "redis://tenant/1") in built
    assert ("memory-redis", "redis://tenant/1") in built
    assert ("session-mysql", "mysql+aiomysql://user:pass@tenant/db") in built
    assert ("memory-mysql", "mysql+aiomysql://user:pass@tenant/db") in built
    await router.close()
    assert all(service.closed == 1 for service in {
        default_service,
        redis_session,
        redis_memory,
        mysql_session,
        mysql_memory,
    })
    await router.close()


def test_storage_router_rejects_memory_configuration(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("MYSQL_URL", raising=False)
    router = TenantStorageRouter()
    for backend in ("redis", "mysql"):
        target = tenant(StorageBackendConfig(memory_backend=backend))
        with pytest.raises(ValueError):
            router.memory_service(target)
    with pytest.raises(ValidationError):
        StorageBackendConfig(memory_backend="unknown")


class EdgeAdapter(ChannelAdapter):
    channel = "edge"

    async def verify_signature(self, payload, headers, query):
        return True

    async def parse_message(self, payload):
        if payload == b"bad":
            raise ValueError("bad message")
        return InboundMessage(channel="edge", chat_id="c", sender_id="u", message_id="m", text="hello")

    async def send_message(self, outbound):
        return SendResult(ok=True)

    async def send_stream(self, chat_id, stream):
        return SendResult(ok=True)

    async def reply_text(self, inbound, text):
        return SendResult(ok=True)


class EdgeWorker:

    def __init__(self, manager, fail=False):
        self.manager = manager
        self.fail = fail

    def resolve_tenant(self, tenant_id):
        return self.manager.get(tenant_id)

    async def handle(self, tenant_id, channel, inbound):
        if self.fail:
            raise TimeoutError("retry me")
        return "ok"


def gateway_fixture(*, fail=False, queue=None):
    manager = TenantConfigManager()
    item = tenant()
    item.channel_configs["edge"] = edge_channel_config()
    manager.register(item)
    registry = ChannelRegistry({"edge": lambda _cfg: EdgeAdapter()})
    app = create_gateway_app(
        manager=manager,
        worker=EdgeWorker(manager, fail=fail),
        registry=registry,
        async_dispatch=False,
        queue=queue,
    )
    return app, registry, item


def test_gateway_health_registry_factories_and_client_errors():
    app, registry, item = gateway_fixture()
    client = TestClient(app)
    assert client.get("/healthz").json() == {"status": "ok"}
    assert registry.get(item, "edge") is registry.get(item, "edge")
    assert registry.get(item, "missing") is None
    registry.register_factory("unconfigured", lambda _cfg: EdgeAdapter())
    assert registry.get(item, "unconfigured") is None

    missing = client.post("/webhook/edge/missing", content=b"x")
    assert missing.status_code == 404
    malformed = client.post("/webhook/edge/edge", content=b"bad", headers={"content-type": "application/octet-stream"})
    assert malformed.status_code == 400

    wecom = ChannelRegistry().get(tenant(), "wecom")
    assert wecom is None
    configured = tenant()
    configured.channel_configs["wecom"] = WeComChannelConfig(
        token=SecretStr("t"),
        aes_key=SecretStr("a"),
        corp_id="c",
        agent_id="1",
    )
    configured.channel_configs["feishu"] = FeishuChannelConfig(
        app_id="app",
        verification_token=SecretStr("verify"),
        secret=SecretStr("s"),
    )
    configured.channel_configs["wechat_kf"] = WechatCustomerServiceChannelConfig(
        corp_id="c",
        open_kfid="wk",
        token=SecretStr("t"),
        aes_key=SecretStr("a"),
    )
    configured.channel_configs["dingtalk"] = DingTalkChannelConfig(
        app_id="app",
        robot_code="robot",
        secret=SecretStr("s"),
    )
    defaults = ChannelRegistry()
    assert defaults.get(configured, "wecom").channel == "wecom"
    assert defaults.get(configured, "feishu").channel == "feishu"
    assert defaults.get(configured, "wechat_kf").channel == "wechat_kf"
    assert defaults.get(configured, "dingtalk").channel == "dingtalk"
    defaults.invalidate(configured.tenant_id)
    assert defaults.get(configured, "wecom").channel == "wecom"
    defaults.invalidate()


def test_gateway_sync_failure_releases_idempotency_reservation():
    store = LocalIdempotencyStore()
    app, _, _ = gateway_fixture(fail=True)
    # Replace the app's default store by building with the explicit one.
    manager = TenantConfigManager()
    item = tenant()
    item.channel_configs["edge"] = edge_channel_config()
    manager.register(item)
    app = create_gateway_app(
        manager=manager,
        worker=EdgeWorker(manager, fail=True),
        registry=ChannelRegistry({"edge": lambda _cfg: EdgeAdapter()}),
        idempotency_store=store,
        async_dispatch=False,
    )
    client = TestClient(app)
    first = client.post("/webhook/edge/edge", content=b"ok")
    second = client.post("/webhook/edge/edge", content=b"ok")
    assert first.status_code == second.status_code == 503


class RecordingQueue:

    def __init__(self) -> None:
        self.tasks = []

    async def enqueue(self, task):
        self.tasks.append(task)


def test_gateway_queue_success_carries_normalized_task():
    queue = RecordingQueue()
    app, _, _ = gateway_fixture(queue=queue)
    response = TestClient(app).post("/webhook/edge/edge", json={"x": 1})
    assert response.status_code == 200
    assert queue.tasks[0].tenant_id == "edge"


async def test_redis_idempotency_result_store_and_builders(monkeypatch):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("redis.asyncio.from_url", lambda *args, **kwargs: redis)
    idem = RedisIdempotencyStore("redis://unused", ttl_seconds=12)
    assert await idem.check_and_set("k") is False
    assert await idem.check_and_set("k") is True
    await idem.release("k")
    assert await idem.check_and_set("k") is False
    assert isinstance(build_idempotency_store(), LocalIdempotencyStore)
    assert isinstance(build_idempotency_store("redis://unused"), RedisIdempotencyStore)

    results = RedisTaskResultStore(redis_url="redis://unused", ttl_seconds=10)
    assert await results.get("task") is None
    await results.put("task", "answer")
    assert await results.get("task") == "answer"
    await results.close()
    await idem.close()
    with pytest.raises(ValueError):
        RedisTaskResultStore()


async def test_redis_budget_full_lifecycle_and_limits():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    tracker = RedisBudgetTracker(
        client=redis,
        pricing={"m": ModelPricing(input_per_mtok=1, output_per_mtok=2)},
        retention_seconds=60,
    )
    tracker.set_pricing("m", ModelPricing(input_per_mtok=2, output_per_mtok=4))
    item = tenant()
    item.budget = BudgetConfig(daily_token_budget=20, daily_cost_limit=0.00002)
    assert await tracker.reserve(item, 10, "2026-01-01") is True
    assert await tracker.reserve(item, 11, "2026-01-01") is False
    await tracker.release(item.tenant_id, 99, "2026-01-01")
    await tracker.record(item.tenant_id, "m", 5, 5, "2026-01-01")
    usage = await tracker.usage(item.tenant_id, "2026-01-01")
    assert usage["input"] == usage["output"] == 5
    assert usage["reserved"] == 0
    assert await tracker.is_within_budget(item, "2026-01-01") is False

    unlimited = tenant()
    assert await tracker.reserve(unlimited, 10_000) is True
    tracker_without_pricing = RedisBudgetTracker(client=redis)
    await tracker_without_pricing.record("free", "unknown", 1, 2, "2026-01-02")
    assert (await tracker_without_pricing.usage("free", "2026-01-02"))["cost"] == 0
    with pytest.raises(ValueError):
        RedisBudgetTracker()


async def test_redis_confirmation_expiry_missing_and_close(monkeypatch):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    manager = RedisConfirmationManager(client=redis, ttl_seconds=0)
    pending = await manager.request("edge", "danger", {"id": 1}, user_id="u", session_id="s")
    assert (await manager.get(pending.token)).tool_args == {"id": 1}
    assert (await manager.resolve(pending.token, approve=False)).tool_name == "danger"
    assert await manager.get(pending.token) is None
    assert await manager.resolve("missing", approve=True) is None
    with pytest.raises(ValueError):
        RedisConfirmationManager()
    monkeypatch.setattr(redis, "aclose", lambda: asyncio.sleep(0))
    await manager.close()


async def test_governance_resolver_confirmation_audit_and_nested_redaction():
    item = tenant()
    item.tool_permissions = ToolPermissions(tool_whitelist=["safe", "danger"], dangerous_tools=["danger"])
    audit = AuditLogger()
    confirmations = ConfirmationManager(ttl_seconds=30)
    filter_ = ToolAllowlistFilter(
        resolver=lambda tenant_id: item if tenant_id == "edge" else None,
        audit_logger=audit,
        confirmation_manager=confirmations,
    )
    missing = FilterResult()
    await filter_._before(new_agent_context(metadata={"tenant_id": "missing"}), {}, missing)
    assert missing.error is None

    ctx = new_agent_context(metadata={"tenant_id": "edge"})
    confirmation = FilterResult()
    await filter_._before(ctx, {"tool_name": "danger", "id": 1}, confirmation)
    assert isinstance(confirmation.error, ToolConfirmationRequired)
    assert (await audit.query(tenant_id="edge"))[0].decision == "confirm"

    confirmed = FilterResult()
    confirmed_ctx = new_agent_context(metadata={"tenant_id": "edge", "confirmed_tools": ["danger"]})
    await filter_._before(confirmed_ctx, {"tool_name": "danger"}, confirmed)
    assert confirmed.error is None

    unknown = FilterResult()
    await ToolAllowlistFilter(permissions=ToolPermissions(tool_whitelist=["safe"]))._before(ctx, {}, unknown)
    assert isinstance(unknown.error, PermissionError)

    redactor = SensitiveDataRedactor(default_rules=[
        {
            "pattern": "[",
            "replace": "bad"
        },
        {
            "pattern": "secret",
            "replace": "***"
        },
    ])
    assert redactor.redact(None) is None
    assert redactor.redact(7) == 7
    assert redactor.redact_any(None) is None
    nested = redactor.redact_any({"a": ["secret", ("secret", )], "n": 4})
    assert nested == {"a": ["***", ["***"]], "n": 4}

    errored = FilterResult(error=RuntimeError("tool failed"), rsp="secret")
    output = ToolOutputRedactionFilter(redactor=redactor)
    await output._after(ctx, {}, errored)
    assert errored.rsp == "secret"

    item.audit_policy.desensitize_rules = [DesensitizeRule(pattern="account", replace="***")]
    resolved_output = ToolOutputRedactionFilter(resolver=lambda _id: item)
    response = FilterResult(rsp="account secret=abc")
    await resolved_output._after(ctx, {}, response)
    assert "account" not in response.rsp
    assert build_governance(item)["model_filters"] == []
    assert len(build_governance(item, tracker=object())["model_filters"]) == 1


async def test_redis_lock_timeout_and_constructor_validation():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    first = RedisSessionLockManager(client=redis, acquire_timeout=1, retry_interval=0.001)
    second = RedisSessionLockManager(client=redis, acquire_timeout=0, retry_interval=0.001)
    async with first.acquire("same"):
        with pytest.raises(TimeoutError):
            async with second.acquire("same"):
                pass
    with pytest.raises(ValueError):
        RedisSessionLockManager()
    await first.close()
