# runtime 模块单元测试
import asyncio
import uuid

import pytest

from trpc_service.events import AgentEvent, ResponseType
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry


@pytest.fixture
def runtime_env():
    storage = InMemoryStorage()

    async def load_fn(tid):
        if tid == "t1":
            return {
                "tenant_id": "t1",
                "name": "租户A",
                "status": "active",
                "app": {
                    "system_prompt": "客服助手"
                },
                "model": {
                    "model_name": "mock"
                },
            }
        return None

    registry = TenantRegistry(load_fn=load_fn)
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    return rt, storage


@pytest.mark.asyncio
async def test_handle_echo_and_trace(runtime_env):
    rt, _ = runtime_env
    resp = await rt.handle(
        AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="你好", trace_id="trc1", channel_type="web"))
    assert resp.response_type == ResponseType.TEXT
    assert "你好" in resp.content
    assert resp.trace_id == "trc1"


@pytest.mark.asyncio
async def test_session_persisted(runtime_env):
    rt, storage = runtime_env
    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="第1条"))
    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="第2条"))
    sess = await storage.session.get_session("t1", "s1")
    assert len(sess["state"]["history"]) == 4  # user+assistant x2


@pytest.mark.asyncio
async def test_summary_persisted(runtime_env):
    rt, storage = runtime_env
    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="你好呀"))
    summary = await storage.summary.get_summary("t1", "s1")
    assert summary and "你好呀" in summary


@pytest.mark.asyncio
async def test_memory_used_in_reply(runtime_env):
    rt, storage = runtime_env
    await storage.memory.add_memory("t1", "u1", {"content": "用户喜欢 Python"})
    resp = await rt.handle(AgentEvent(tenant_id="t1", session_id="s2", user_id="u1", content="Python?"))
    assert "Python" in resp.content


@pytest.mark.asyncio
async def test_unknown_tenant_error(runtime_env):
    rt, _ = runtime_env
    resp = await rt.handle(AgentEvent(tenant_id="nope", session_id="s3", content="hi"))
    assert resp.response_type == ResponseType.ERROR
    assert "租户不存在" in resp.content


@pytest.mark.asyncio
async def test_mock_runner_tool_events():
    from trpc_service.tenant import TenantConfig

    runner = MockAgentRunner()
    tenant = TenantConfig(tenant_id="t", name="x")
    events = [
        e async for e in runner.run(
            tenant=tenant,
            user_id="u",
            session_id="s",
            new_message="hi",
            session_state={"pending_tool": "calculator"},
        )
    ]
    types = [e.type for e in events]
    assert "tool_call" in types
    assert "tool_result" in types
    assert events[-1].type == "done"


# ------------------------------------------------------------------
# 执行审计（PRD 4.4）: Runtime 侧与网关 AuditFilter 并列的执行留痕
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execution_audit_written_on_success(runtime_env):
    """Mock 成功路径落 executed 执行审计（含工具名；网关审计管治理）。"""
    rt, storage = runtime_env
    await rt.handle(
        AgentEvent(tenant_id="t1",
                   session_id="s_audit",
                   user_id="u1",
                   content="跑一下",
                   trace_id="trc-exec-ok",
                   channel_type="web"))

    logs = await storage.audit.query_logs("t1", {"decision": "executed"})
    assert len(logs) == 1
    assert logs[0]["trace_id"] == "trc-exec-ok"
    assert logs[0]["agent_name"] == "租户A"


@pytest.mark.asyncio
async def test_execution_audit_written_on_mock_tool(runtime_env):
    """Mock 演示工具路径: executed 审计的 tool_name 含实际调用的工具。"""
    rt, storage = runtime_env
    # 预置 session 状态声明 pending_tool，Mock Runner 据此演示工具调用
    await storage.session.save_session("t1", {
        "session_id": "s_tool_audit",
        "state": {
            "pending_tool": "calculator"
        },
    })
    await rt.handle(
        AgentEvent(tenant_id="t1",
                   session_id="s_tool_audit",
                   user_id="u1",
                   content="计算一下",
                   trace_id="trc-exec-tool",
                   channel_type="web"))

    logs = await storage.audit.query_logs("t1", {"decision": "executed"})
    assert len(logs) == 1
    assert logs[0]["tool_name"] == "calculator", "执行审计应记录真实调用的工具"


@pytest.mark.asyncio
async def test_execution_audit_not_written_when_disabled(runtime_env):
    """execution_audit_enabled=False（测试/内部开关）时不落执行审计。"""
    rt, storage = runtime_env
    rt._execution_audit_enabled = False
    await rt.handle(AgentEvent(tenant_id="t1", session_id="s_off", user_id="u1", content="关闭"))
    assert await storage.audit.query_logs("t1", {"decision": "executed"}) == []


@pytest.mark.asyncio
async def test_dual_audit_gateway_and_execution_rows():
    """一次完整请求落两层审计: 网关治理 allow + Runtime 执行 executed（PRD 4.4）。

    AuditFilter 在 Filter 链内（执行前）记治理决策；Runtime 执行完记执行
    审计（含真实成本/工具）——两层语义不同、互不覆盖。
    """
    from trpc_service.filters import build_filter_chain
    from trpc_service.runtime.pipeline import process_event
    from trpc_service.storage import InMemoryStorage
    from trpc_service.tenant import TenantRegistry

    storage = InMemoryStorage()

    async def load_fn(tid):
        if tid == "t1":
            return {
                "tenant_id": "t1",
                "name": "租户A",
                "status": "active",
                "app": {
                    "system_prompt": "客服助手"
                },
                "model": {
                    "model_name": "mock"
                },
            }
        return None

    registry = TenantRegistry(load_fn=load_fn)
    chain, ctx = build_filter_chain(registry=registry, storage=storage)
    runtime = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())

    event = AgentEvent(tenant_id="t1",
                       session_id="s_dual",
                       user_id="u1",
                       content="你好",
                       msg_id="dual-001",
                       channel_type="web")
    resp = await process_event(event, chain=chain, ctx=ctx, runtime=runtime)
    assert resp.response_type == ResponseType.TEXT

    all_logs = await storage.audit.query_logs("t1", {})
    decisions = sorted(log["decision"] for log in all_logs)
    assert decisions == ["allow", "executed"], f"应同时存在治理 allow 与执行 executed，实际 {decisions}"
    assert all_logs[0]["trace_id"] == all_logs[1]["trace_id"], "两层审计应共享同一 trace_id"


# ------------------------------------------------------------------
# 后台任务可靠化（PRD 5.2/4.4）: drain + 关闭前排空
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_background_tasks_waits_and_clears():
    """drain 会等后台任务完成并清空 _bg_tasks（关闭前不丢摘要落库）。"""
    storage = InMemoryStorage()

    async def load_fn(tid):
        if tid == "t1":
            return {
                "tenant_id": "t1",
                "name": "租户A",
                "status": "active",
                "app": {
                    "system_prompt": "客服"
                },
                "model": {
                    "model_name": "mock"
                }
            }
        return None

    registry = TenantRegistry(load_fn=load_fn)
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())

    # 手动塞一个后台任务（模拟摘要/预算）
    async def _slow_task():
        await asyncio.sleep(0.05)
        await storage.summary.save_summary("t1", "s_drain", "后台落库内容")

    task = asyncio.create_task(_slow_task())
    rt._bg_tasks.add(task)
    task.add_done_callback(rt._bg_tasks.discard)

    await rt.drain_background_tasks(timeout_s=2)
    assert not rt._bg_tasks, "drain 后 _bg_tasks 应为空"
    assert await storage.summary.get_summary("t1", "s_drain") == "后台落库内容"


@pytest.mark.asyncio
async def test_drain_background_tasks_timeout_safe():
    """超时任务不阻塞 drain（timeout 保护）。"""
    storage = InMemoryStorage()

    async def load_fn(tid):
        return {"tenant_id": "t1", "name": "x", "status": "active", "model": {"model_name": "mock"}}

    registry = TenantRegistry(load_fn=load_fn)
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())

    async def _never_ends():
        await asyncio.sleep(10)

    task = asyncio.create_task(_never_ends())
    rt._bg_tasks.add(task)
    await rt.drain_background_tasks(timeout_s=0.05)  # 不应卡死
    assert not rt._bg_tasks, "超时任务被清理（即使未完成）"


# ------------------------------------------------------------------
# 并发写会话一致性（PRD 2.3-A / 09-04 联调缺陷回归）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_session_writes_no_lost_updates(runtime_env):
    """N 个并发请求写同一 session 不得丢历史（读-改-写须锁内重读为基线）。

    缺陷背景（09-04 联调实测）：修复前 12 并发仅落 10 轮——handle 开头读的
    session 快照在 LLM 调用期间已过期，锁只串行化「写」防不住丢失更新。
    """
    rt, storage = runtime_env
    n = 12
    events = [
        AgentEvent(tenant_id="t1", session_id="s_conc", user_id="u1", content=f"并发消息{i}", channel_type="web")
        for i in range(n)
    ]
    responses = await asyncio.gather(*(rt.handle(e) for e in events))
    assert all(r.response_type == ResponseType.TEXT for r in responses)

    sess = await storage.session.get_session("t1", "s_conc")
    history = sess["state"]["history"]
    assert len(history) == 2 * n, f"历史应 {2 * n} 条，实际 {len(history)} 条（丢失更新）"
    user_contents = sorted(h["content"] for h in history if h["role"] == "user")
    assert user_contents == sorted(f"并发消息{i}" for i in range(n)), "每轮 user 消息都应留痕"
    assert sess["version"] == n


@pytest.mark.asyncio
async def test_concurrent_session_writes_redis_when_available():
    """真实 Redis 后端 + 分布式锁下的并发写一致性（无 Redis 自动跳过）。"""
    import redis.asyncio as aioredis

    from trpc_service.storage import StorageFactory
    from trpc_service.tenant import DataBackendConfig, TenantRegistry

    try:
        client = aioredis.Redis.from_url("redis://localhost:6379/0")
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    storage = await StorageFactory(redis=client).create(
        "t1",
        DataBackendConfig(session="redis", memory="redis", summary="inmemory", audit="inmemory"),
    )

    async def load_fn(tid):
        if tid == "t1":
            return {"tenant_id": "t1", "name": "租户A", "status": "active", "model": {"model_name": "mock"}}
        return None

    rt = Runtime(registry=TenantRegistry(load_fn=load_fn), storage=storage, runner=MockAgentRunner())
    # Redis AOF 跨测试运行持久化：session_id 唯一化保证断言不受历史残留影响
    sid = f"s_redis_conc_{uuid.uuid4().hex[:8]}"
    try:
        n = 12
        events = [
            AgentEvent(tenant_id="t1", session_id=sid, user_id="u1", content=f"并发消息{i}", channel_type="web")
            for i in range(n)
        ]
        await asyncio.gather(*(rt.handle(e) for e in events))

        sess = await storage.session.get_session("t1", sid)
        history = sess["state"]["history"]
        assert len(history) == 2 * n, f"Redis 后端历史应 {2 * n} 条，实际 {len(history)} 条"
        assert sess["version"] == n
    finally:
        await client.aclose()


def test_runner_event_field_defaults_not_shadowed():
    """dataclass 字段不得被同名工厂方法遮蔽（审查 09-04：content/error 曾是 bound method）。

    直接构造 RunnerEvent(type='content') 不传 content 时，字段默认值必须是
    空串/None 而非类方法对象——否则任何消费方拿到的是方法对象。
    """
    from trpc_service.runtime.events import RunnerEvent

    e = RunnerEvent(type="content")
    assert e.content == "" and e.error is None
    # 工厂方法仍可用（改名后）
    assert RunnerEvent.text("hi").content == "hi"
    assert RunnerEvent.failure("boom").error == "boom"


@pytest.mark.asyncio
async def test_pipeline_releases_idempotency_on_failure():
    """治理阻断/执行失败后幂等键须释放，IM 重试才能重新处理（审查 09-04）。"""
    from trpc_service.filters import build_filter_chain
    from trpc_service.runtime.pipeline import process_event
    from trpc_service.storage import InMemoryStorage
    from trpc_service.tenant import TenantRegistry

    storage = InMemoryStorage()

    async def load_fn(tid):
        if tid == "t1":
            return {"tenant_id": "t1", "name": "x", "status": "active", "model": {"model_name": "mock"}}
        return None

    registry = TenantRegistry(load_fn=load_fn)
    chain, ctx = build_filter_chain(registry=registry, storage=storage)
    runtime = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    msg_id = "retry-001"

    # 第一轮：未知租户 → 治理阻断（幂等键应被释放）
    bad = AgentEvent(tenant_id="ghost", session_id="s1", user_id="u1", content="hi", msg_id=msg_id, channel_type="web")
    r1 = await process_event(bad, chain=chain, ctx=ctx, runtime=runtime)
    assert r1.response_type == ResponseType.ERROR

    # 第二轮：同 msg_id 换合法租户重试 → 不应被幂等挡掉
    good = AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="hi", msg_id=msg_id, channel_type="web")
    r2 = await process_event(good, chain=chain, ctx=ctx, runtime=runtime)
    assert r2.response_type == ResponseType.TEXT, f"失败后幂等键应释放，实际: {r2.content}"

    # 第三轮：成功后同 msg_id 重复投递 → 仍应被幂等拦截
    r3 = await process_event(good, chain=chain, ctx=ctx, runtime=runtime)
    assert "duplicate" in r3.content


# ------------------------------------------------------------------
# C1 回归：后端写重试 + Session 后端延迟指标
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_session_retry_then_warning(runtime_env, caplog):
    """session state 首写失败 → 自动重试一次成功，回复不受影响。"""
    import logging

    rt, storage = runtime_env
    calls = {"n": 0}
    orig_update = storage.session.update_state

    async def flaky(tenant_id, session_id, state):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("db jitters")
        return await orig_update(tenant_id, session_id, state)

    storage.session.update_state = flaky
    try:
        resp = await rt.handle(
            AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="hi", channel_type="web"))
    finally:
        storage.session.update_state = orig_update

    assert resp.response_type == ResponseType.TEXT, "写失败重试成功后回复应正常"
    assert calls["n"] == 2, "应恰好重试一次"
    assert any(r.levelno == logging.WARNING and "retrying" in r.message for r in caplog.records), "首写失败应有 WARNING"


@pytest.mark.asyncio
async def test_save_session_retry_exhausted_no_raise(runtime_env, caplog):
    """两次写均失败 → 仅告警不抛，回复仍正常返回。"""
    import logging

    rt, storage = runtime_env

    async def always_fail(tenant_id, session_id, state):
        raise RuntimeError("db down")

    storage.session.update_state = always_fail
    try:
        resp = await rt.handle(
            AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="hi", channel_type="web"))
    finally:
        pass

    assert resp.response_type == ResponseType.TEXT, "写失败不得中断回复"
    assert any(r.levelno == logging.WARNING and "failed after retry" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_session_backend_latency_observed(runtime_env):
    """session 读写延迟应被 observe 到 session_backend_latency 指标。

    注意: prometheus_client 全局注册表不允许同名指标重建，故复用
    get_metrics() 单例，用基线差值断言（避免其他测试的累计干扰）。
    """
    from prometheus_client import REGISTRY

    from trpc_service.metrics.metrics import get_metrics

    rt, storage = runtime_env
    rt._metrics = get_metrics()

    def _count(op: str) -> float:
        v = REGISTRY.get_sample_value("teneuris_session_backend_latency_seconds_count", {
            "op": op,
            "backend": "InMemorySessionStore"
        })
        return v or 0.0

    base_get, base_upd = _count("get"), _count("update")
    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="hi", channel_type="web"))

    assert _count("get") >= base_get + 1, "get 路径应有延迟打点"
    assert _count("update") >= base_upd + 1, "update 路径应有延迟打点"
