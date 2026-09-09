# filters 模块单元测试
import pytest

from trpc_service.events import AgentEvent
from trpc_service.filters import build_filter_chain
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry

DEMO_TENANTS = {
    "t1": {
        "tenant_id": "t1",
        "name": "租户A",
        "status": "active",
        "tools": {
            "allowlist": ["search"],
            "dangerous_tools": ["delete_all"]
        },
        "rate_limit_per_min": 100,
        "monthly_budget_usd": 100,
        "used_budget_usd": 50,
    },
    "t_suspend": {
        "tenant_id": "t_suspend",
        "name": "停用",
        "status": "suspended"
    },
    "t_broke": {
        "tenant_id": "t_broke",
        "name": "破产",
        "status": "active",
        "monthly_budget_usd": 10,
        "used_budget_usd": 10,
    },
    "t_acl": {
        "tenant_id":
        "t_acl",
        "name":
        "用户级权限",
        "status":
        "active",
        "im": [{
            "channel_type": "web",
            "webhook_path": "",
            "user_acl": {
                "enabled": True,
                "allowlist": ["alice", "bob"],
                "blocklist": ["mallory"],
            },
        }],
    },
    "t_acl_off": {
        "tenant_id":
        "t_acl_off",
        "name":
        "用户级权限关闭",
        "status":
        "active",
        "im": [{
            "channel_type": "web",
            "webhook_path": "",
            "user_acl": {
                "enabled": False,
                "allowlist": ["alice"],
                "blocklist": ["mallory"],
            },
        }],
    },
}


@pytest.fixture
def chain_ctx():
    storage = InMemoryStorage()

    async def load_fn(tid):
        return DEMO_TENANTS.get(tid)

    registry = TenantRegistry(load_fn=load_fn)
    chain, ctx = build_filter_chain(registry=registry, storage=storage)
    return chain, ctx, storage


@pytest.mark.asyncio
async def test_chain_allows_and_audits(chain_ctx):
    chain, ctx, storage = chain_ctx
    event = AgentEvent(tenant_id="t1", channel_type="web", user_id="u1", content="你好 13812345678")
    result = await chain.run(ctx, event)
    assert result.passed
    assert event.trace_id
    assert "13812345678" not in event.content  # PII 脱敏
    logs = await storage.audit.query_logs("t1", {})
    assert len(logs) == 1
    assert logs[0]["decision"] == "allow"
    assert logs[0]["trace_id"] == event.trace_id


@pytest.mark.asyncio
async def test_chain_blocks_missing_tenant(chain_ctx):
    chain, ctx, _ = chain_ctx
    result = await chain.run(ctx, AgentEvent(tenant_id="nope", content="hi"))
    assert not result.passed
    assert result.error.error_type == "tenant_not_found"


@pytest.mark.asyncio
async def test_chain_blocked_requests_audited(chain_ctx):
    """被阻断/异常流量同样写入审计（PRD 4.4，decision=block）。"""
    chain, ctx, storage = chain_ctx
    # 租户不存在 -> 审计按事件声明的 tenant_id 桶记录
    await chain.run(ctx, AgentEvent(tenant_id="nope", content="hi"))
    blocked_logs = await storage.audit.query_logs("nope", {})
    assert len(blocked_logs) == 1
    assert blocked_logs[0]["decision"] == "block"
    assert blocked_logs[0]["error_type"] == "tenant_not_found"
    # 预算超限 -> 阻断流量记入该租户审计
    await chain.run(ctx, AgentEvent(tenant_id="t_broke", content="hi"))
    broke_logs = await storage.audit.query_logs("t_broke", {"decision": "block"})
    assert len(broke_logs) == 1
    assert broke_logs[0]["error_type"] == "budget_exceeded"


@pytest.mark.asyncio
async def test_chain_blocks_suspended(chain_ctx):
    chain, ctx, _ = chain_ctx
    result = await chain.run(ctx, AgentEvent(tenant_id="t_suspend", content="hi"))
    assert not result.passed
    assert result.error.error_type == "tenant_suspended"


@pytest.mark.asyncio
async def test_chain_blocks_budget(chain_ctx):
    chain, ctx, _ = chain_ctx
    result = await chain.run(ctx, AgentEvent(tenant_id="t_broke", content="hi"))
    assert not result.passed
    assert result.error.error_type == "budget_exceeded"


@pytest.mark.asyncio
async def test_chain_rate_limit(chain_ctx):
    chain, ctx, _ = chain_ctx
    results = [await chain.run(ctx, AgentEvent(tenant_id="t1", content="m")) for _ in range(101)]
    assert results[-1].error is not None
    assert results[-1].error.error_type == "rate_limited"


@pytest.mark.asyncio
async def test_tool_whitelist_and_confirmation(chain_ctx):
    chain, ctx, _ = chain_ctx
    ok = await chain.run(ctx, AgentEvent(tenant_id="t1", content="x", metadata={"tool_name": "search"}))
    assert ok.passed
    blocked = await chain.run(ctx, AgentEvent(tenant_id="t1", content="x", metadata={"tool_name": "hack"}))
    assert not blocked.passed and blocked.error.error_type == "tool_not_allowed"


@pytest.mark.asyncio
async def test_signature_filter(chain_ctx):
    _, _, storage = chain_ctx

    async def load_fn(tid):
        return DEMO_TENANTS.get(tid)

    registry = TenantRegistry(load_fn=load_fn)

    def verifier(body, sig):
        return sig == "valid"

    chain, ctx = build_filter_chain(registry=registry, storage=storage, signature_verifiers={"wechat_work": verifier})
    ok = await chain.run(
        ctx,
        AgentEvent(tenant_id="t1",
                   channel_type="wechat_work",
                   content="x",
                   metadata={
                       "signature": "valid",
                       "raw_body": b"b"
                   }),
    )
    assert ok.passed
    bad = await chain.run(
        ctx,
        AgentEvent(tenant_id="t1",
                   channel_type="wechat_work",
                   content="x",
                   metadata={
                       "signature": "bad",
                       "raw_body": b"b"
                   }),
    )
    assert not bad.passed and bad.error.error_type == "signature_mismatch"


@pytest.mark.asyncio
async def test_user_auth_filter(chain_ctx):
    """IM 用户级权限校验（PRD 4.1）：白名单放行 / 黑名单阻断 / 白名单外阻断。"""
    chain, ctx, _ = chain_ctx
    # 白名单内 -> 放行
    ok = await chain.run(ctx, AgentEvent(tenant_id="t_acl", channel_type="web", user_id="alice", content="hi"))
    assert ok.passed
    # 黑名单 -> user_blocked
    blocked = await chain.run(ctx, AgentEvent(tenant_id="t_acl", channel_type="web", user_id="mallory", content="hi"))
    assert not blocked.passed and blocked.error.error_type == "user_blocked"
    # 白名单外且白名单非空 -> user_not_allowed
    outsider = await chain.run(ctx, AgentEvent(tenant_id="t_acl", channel_type="web", user_id="eve", content="hi"))
    assert not outsider.passed and outsider.error.error_type == "user_not_allowed"
    # 未配置 im 通道 -> 放行（no_channel）
    passthrough = await chain.run(ctx, AgentEvent(tenant_id="t1", channel_type="web", user_id="u1", content="hi"))
    assert passthrough.passed
    # enabled=False -> 即使命中黑名单也放行
    off = await chain.run(ctx, AgentEvent(tenant_id="t_acl_off", channel_type="web", user_id="mallory", content="hi"))
    assert off.passed


# ------------------------------------------------------------------
# 全项目审查回归（2026-09-04 OCR findings）
# ------------------------------------------------------------------


def test_rate_limiter_rebuilds_bucket_on_rate_change():
    """租户 rate_limit_per_min 热更新后，令牌桶须按新速率重建（审查 09-04）。"""
    from trpc_service.filters.rate_limiter import TenantRateLimiter

    limiter = TenantRateLimiter()
    # 旧速率 2/min：连发 2 次耗尽
    assert limiter.allow("t1", 2)[0] is True
    assert limiter.allow("t1", 2)[0] is True
    assert limiter.allow("t1", 2)[0] is False
    # 热更新到 5/min：桶应以新速率重建，立即放行
    ok, _ = limiter.allow("t1", 5)
    assert ok is True, "速率热更新后旧桶应被替换"
    # 相同速率重复调用复用同一桶（不重置）
    limiter2 = TenantRateLimiter()
    limiter2.allow("t2", 1)
    assert limiter2.allow("t2", 1)[0] is False, "相同速率不应重建桶重置配额"


@pytest.mark.asyncio
async def test_budget_gauge_refreshed_on_request(chain_ctx):
    """BudgetFilter 每请求刷新 tenant_budget_usd Gauge（PRD 4.2；09-05 缺口 3）。"""
    from trpc_service.metrics.metrics import get_metrics

    chain, ctx, _ = chain_ctx
    await chain.run(ctx, AgentEvent(tenant_id="t_broke", content="hi"))
    gauge = get_metrics().tenant_budget.labels(tenant_id="t_broke")
    assert gauge._value.get() == 10, "Gauge 应等于租户 monthly_budget_usd"


# ------------------------------------------------------------------
# C5 回归：Redis 固定窗口限流器（多节点共享额度）
# ------------------------------------------------------------------


def _redis_probe():
    import redis.asyncio as aioredis

    return aioredis.Redis.from_url("redis://localhost:6379/15")


@pytest.mark.asyncio
async def test_redis_fixed_window_limiter():
    """固定窗口: 额度内放行，超额阻断并返回等待秒数；租户间额度独立。"""
    import pytest as _pytest

    client = _redis_probe()
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        _pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.filters.rate_limiter import RedisFixedWindowLimiter

    await client.flushdb()
    try:
        limiter = RedisFixedWindowLimiter(client)
        # 额度 2：前两次放行，第三次阻断
        assert await limiter.allow("t_rl", 2) == (True, 0.0)
        assert await limiter.allow("t_rl", 2) == (True, 0.0)
        allowed, wait = await limiter.allow("t_rl", 2)
        assert not allowed and 0 < wait <= 60, "超额应阻断并给等待秒数"

        # 其他租户额度独立
        assert await limiter.allow("t_rl2", 2) == (True, 0.0)

        # rate<=0 不限流
        assert await limiter.allow("t_rl3", 0) == (True, 0.0)
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_filter_accepts_async_limiter():
    """RateLimitFilter 兼容异步共享限流器（duck typing，isawaitable 分支）。"""

    class FakeAsyncLimiter:

        async def allow(self, tenant_id, rate_per_min=None):
            return False, 1.5

    storage = InMemoryStorage()

    async def load_fn(tid):
        return DEMO_TENANTS.get(tid)

    registry = TenantRegistry(load_fn=load_fn)
    chain, ctx = build_filter_chain(registry=registry, storage=storage, limiter=FakeAsyncLimiter())
    result = await chain.run(ctx, AgentEvent(tenant_id="t1", content="hi"))
    assert not result.passed and result.error.error_type == "rate_limited"
