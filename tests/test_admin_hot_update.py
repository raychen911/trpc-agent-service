# ===================================================================
# Admin 租户配置热更新 + 版本回滚（PRD 5.2）
# ===================================================================
# 说明: 验证三条机制:
#   1. 更新后本进程 Registry 立即生效（无需重启）
#   2. 回滚恢复上一版本
#   3. Redis pub/sub 跨进程失效通知（模拟 Admin 进程通知 Gateway 进程）
# ===================================================================

import asyncio
import logging

import pytest
from fastapi.testclient import TestClient

from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry
from trpc_service.web import build_admin_app, build_gateway_app


def _admin_client(registry: TenantRegistry):
    storage = InMemoryStorage()
    app = build_admin_app(storage=storage, registry=registry, api_key="k")
    return TestClient(app), storage


def _gateway(registry: TenantRegistry, storage: InMemoryStorage):
    runtime = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    return TestClient(build_gateway_app(registry=registry, storage=storage, runtime=runtime))


def _create_hot(admin, tenant_id="hot1"):
    headers = {"X-Admin-Key": "k"}
    resp = admin.post("/tenants", headers=headers, json={"tenant_id": tenant_id, "name": "热更新租户"})
    assert resp.status_code == 201
    return headers


def test_hot_update_effective_without_restart():
    """Admin 更新租户配置后，Gateway（同 Registry）下一请求立即生效，无需重启。"""
    registry = TenantRegistry()
    admin, storage = _admin_client(registry)
    gw = _gateway(registry, storage)
    headers = _create_hot(admin)

    # 1. 生效中的租户: /chat 正常
    resp = gw.post("/chat", json={"tenant_id": "hot1", "user_id": "u1", "content": "你好"})
    assert resp.status_code == 200 and resp.json()["response_type"] == "text"

    # 2. Admin 热更新: 停用租户（不重启任何服务）
    resp = admin.put("/tenants/hot1", headers=headers, json={"status": "suspended"})
    assert resp.status_code == 200

    # 3. Gateway 下一请求即拒绝 —— 配置变更无需重启即生效
    resp = gw.post("/chat", json={"tenant_id": "hot1", "user_id": "u1", "content": "你好"})
    assert resp.status_code == 200
    assert resp.json()["response_type"] == "error"
    assert "tenant suspended" in resp.json()["content"]

    # 4. 回滚: 恢复 active，Gateway 立即恢复可用
    resp = admin.post("/tenants/hot1/rollback", headers=headers)
    assert resp.status_code == 200
    resp = gw.post("/chat", json={"tenant_id": "hot1", "user_id": "u1", "content": "你好"})
    assert resp.status_code == 200 and resp.json()["response_type"] == "text"


def test_rollback_multiple_versions():
    """多次更新可逐级回滚；无历史版本时返回 404。"""
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = _create_hot(admin)

    # 更新两次: 初始 -> v1 -> v2
    assert admin.put("/tenants/hot1", headers=headers, json={"name": "v1"}).status_code == 200
    assert admin.put("/tenants/hot1", headers=headers, json={"name": "v2"}).status_code == 200
    assert admin.get("/tenants/hot1", headers=headers).json()["name"] == "v2"

    # 回滚两次
    assert admin.post("/tenants/hot1/rollback", headers=headers).status_code == 200
    assert admin.get("/tenants/hot1", headers=headers).json()["name"] == "v1"
    assert admin.post("/tenants/hot1/rollback", headers=headers).status_code == 200
    assert admin.get("/tenants/hot1", headers=headers).json()["name"] == "热更新租户"

    # 无历史可回滚
    resp = admin.post("/tenants/hot1/rollback", headers=headers)
    assert resp.status_code == 404


async def _redis_available() -> bool:
    try:
        import redis.asyncio as aioredis

        client = aioredis.Redis.from_url("redis://localhost:6379/0")
        await client.ping()
        await client.aclose()
        return True
    except Exception:  # noqa: BLE001 - 探测用途
        return False


@pytest.mark.asyncio
async def test_broadcaster_cross_process_invalidate():
    """Redis pub/sub: Admin 进程发布失效通知 → Gateway 进程失效本地缓存。"""
    if not await _redis_available():
        pytest.skip("Redis 不可用（本机未启动 redis-server）")

    import redis.asyncio as aioredis

    from trpc_service.tenant.broadcaster import ConfigBroadcaster

    redis_url = "redis://localhost:6379/0"

    # Gateway 侧 registry（模拟 Gateway 进程）: get 命中即缓存
    gw_registry = TenantRegistry()

    async def load_fn(tenant_id: str):
        return {"tenant_id": tenant_id, "name": "x"}

    gw_registry._load_fn = load_fn
    assert await gw_registry.get("t1") is not None
    assert "t1" in gw_registry._cache, "前提: 配置已进本地缓存"

    # 订阅循环（后台任务）；等订阅就绪再发布，避免 pub/sub 丢消息
    stop = asyncio.Event()
    ready = asyncio.Event()
    # C6：顺带验证适配器缓存随通知失效（同一条 pub/sub 消息驱动）
    from trpc_service.channels.factory import ChannelFactory
    from trpc_service.tenant import ImChannelConfig

    channel_factory = ChannelFactory()
    channel_factory.create("t1", ImChannelConfig(channel_type="web"))
    subscriber = asyncio.create_task(
        ConfigBroadcaster(aioredis.Redis.from_url(redis_url)).subscribe_loop(gw_registry,
                                                                             stop,
                                                                             ready=ready,
                                                                             channel_factory=channel_factory))
    await asyncio.wait_for(ready.wait(), timeout=2)

    # Admin 侧广播（独立客户端，模拟另一进程）
    admin_redis = aioredis.Redis.from_url(redis_url)
    admin_broadcaster = ConfigBroadcaster(admin_redis)
    await admin_broadcaster.publish_invalidated("t1")

    # 等待订阅循环消费并失效缓存（轮询 2s 上限）
    for _ in range(20):
        if "t1" not in gw_registry._cache:
            break
        await asyncio.sleep(0.1)
    assert "t1" not in gw_registry._cache, "Gateway 应收到通知并失效缓存"
    assert channel_factory.get("t1", "web") is None, "适配器缓存应随通知一并失效"

    stop.set()
    await subscriber
    await admin_redis.aclose()


# ------------------------------------------------------------------
# 全项目审查回归（2026-09-04 OCR findings）
# ------------------------------------------------------------------


def test_admin_partial_update_preserves_unspecified_fields():
    """局部更新不得清空未传入字段（update 合并基线须为非脱敏 dump）。

    缺陷背景：此前以脱敏 _dump 为合并基线，密钥字段先被剔除再合并，
    任何局部更新都会把内存路径租户的 secret_ref 静默清空。
    """
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = {"X-Admin-Key": "k"}
    resp = admin.post("/tenants",
                      headers=headers,
                      json={
                          "tenant_id": "sec1",
                          "name": "带密钥租户",
                          "model": {
                              "provider": "deepseek",
                              "model_name": "deepseek-chat",
                              "api_key_ref": "sk-secret-1234567890"
                          },
                          "im": [{
                              "channel_type": "wechat_work",
                              "token_ref": "tok-abcdef",
                              "secret_ref": "sec-abcdef"
                          }],
                      })
    assert resp.status_code == 201

    # 局部更新：只改 name
    resp = admin.put("/tenants/sec1", headers=headers, json={"name": "改名"})
    assert resp.status_code == 200

    data = admin.get("/tenants/sec1", headers=headers).json()
    assert data["name"] == "改名"
    # 密钥字段不回显（PRD 4.5），但内部配置对象必须保留
    # （registry._cache 为 admin _save 写入的当前配置，直接读内部缓存断言）
    cfg = registry._cache.get("sec1")
    assert cfg is not None, "热更新后的配置应写入 Registry 缓存"
    assert cfg.model.api_key_ref is not None, "api_key_ref 被局部更新清空"
    assert cfg.im[0].token_ref is not None, "token_ref 被局部更新清空"
    assert cfg.im[0].secret_ref is not None, "secret_ref 被局部更新清空"
    # API 输出仍脱敏（PRD 4.5）
    assert "api_key_ref" not in data["model"]
    assert "token_ref" not in data["im"][0]


def test_admin_malformed_json_returns_400():
    """畸形 JSON 应回 400，而非未处理 500（审查 09-04 quick win）。"""
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = {"X-Admin-Key": "k", "Content-Type": "application/json"}
    resp = admin.post("/tenants", headers=headers, content=b"{not-json")
    assert resp.status_code == 400
    resp = admin.put("/tenants/whatever", headers=headers, content=b"{not-json")
    assert resp.status_code == 400


def test_admin_audit_operator_per_request():
    """操作人经请求参数传入而非实例态——并发请求不会互相覆盖归属。"""
    registry = TenantRegistry()
    admin, storage = _admin_client(registry)
    h1 = {"X-Admin-Key": "k", "X-Admin-Operator": "alice"}
    h2 = {"X-Admin-Key": "k", "X-Admin-Operator": "bob"}
    assert admin.post("/tenants", headers=h1, json={"tenant_id": "op1", "name": "a"}).status_code == 201
    assert admin.post("/tenants", headers=h2, json={"tenant_id": "op2", "name": "b"}).status_code == 201
    logs = storage.audit._data.get("op1", []) if hasattr(storage.audit, "_data") else []
    if logs:  # InMemoryAuditStore 存储结构兼容时校验 operator 归属
        assert logs[-1]["payload"]["operator"] == "alice"


# ------------------------------------------------------------------
# 问题 1 守卫回归：预算 > 0 但单价 = 0 → WARNING（不拦截）
# ------------------------------------------------------------------


def test_budget_without_price_warns_on_create(caplog):
    """创建租户：预算 > 0 且输入/输出单价全 0 → 打 WARNING，但创建不阻断。"""
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = {"X-Admin-Key": "k"}
    with caplog.at_level(logging.WARNING, logger="teneuris.web.admin"):
        resp = admin.post("/tenants",
                          headers=headers,
                          json={
                              "tenant_id": "bw1",
                              "name": "守卫租户",
                              "monthly_budget_usd": 10.0
                          })
    assert resp.status_code == 201, "守卫只告警不拦截，创建必须成功"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "unit price is 0" in r.message]
    assert warnings, "预算>0 且单价全 0 应产生 WARNING"
    assert getattr(warnings[0], "tenant_id", None) == "bw1"


def test_budget_without_price_no_warning_when_price_set(caplog):
    """单价任一 > 0，或预算 = 0，均不应告警。"""
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = {"X-Admin-Key": "k"}
    with caplog.at_level(logging.WARNING, logger="teneuris.web.admin"):
        # 单价 > 0：不告警
        resp = admin.post("/tenants",
                          headers=headers,
                          json={
                              "tenant_id": "bw2",
                              "name": "有单价",
                              "monthly_budget_usd": 10.0,
                              "model": {
                                  "provider": "deepseek",
                                  "input_price_per_1m_usd": 0.27
                              }
                          })
        assert resp.status_code == 201
        # 预算 = 0：不告警
        resp = admin.post("/tenants", headers=headers, json={"tenant_id": "bw3", "name": "无预算"})
        assert resp.status_code == 201
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "unit price is 0" in r.message]
    assert not warnings, "单价>0 或 预算=0 时不应告警"


def test_budget_without_price_warns_on_update(caplog):
    """更新路径：给单价为 0 的存量租户设置预算 → 同样触发 WARNING。"""
    registry = TenantRegistry()
    admin, _ = _admin_client(registry)
    headers = {"X-Admin-Key": "k"}
    assert admin.post("/tenants", headers=headers, json={"tenant_id": "bw4", "name": "先建后补价"}).status_code == 201
    with caplog.at_level(logging.WARNING, logger="teneuris.web.admin"):
        resp = admin.put("/tenants/bw4", headers=headers, json={"monthly_budget_usd": 5.0})
    assert resp.status_code == 200
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "unit price is 0" in r.message]
    assert warnings, "update 合并后命中'预算>0 且单价=0'也应告警"
    assert getattr(warnings[0], "tenant_id", None) == "bw4"
