# ===================================================================
# 阶段二验收用例（Spec: DEVELOPMENT_LOG「阶段二 Spec」§4）
# ===================================================================
# 说明: 用例 1-7 自动化（用例 8 门禁由 gate-check.sh 人工执行）。
#   真实 LLM 的 HTTP 层由 tests/fakes.py 的 FakeLLMModel 替换——
#   验证的是「Runtime → Runner → 事件流 → 平台存储」自家逻辑，
#   不是模型厂商的行为。
# ===================================================================

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import SecretStr

from tests.fakes import FAKE_MODEL_NAME, FakeLLMModel
from trpc_service.agent.model_factory import build_llm_model, resolve_model_endpoint
from trpc_service.events import AgentEvent
from trpc_service.runtime import FrameworkAgentRunner, Runtime
from trpc_service.runtime.runner import translate_event

if TYPE_CHECKING:
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.sessions import Session
from trpc_service.storage import InMemoryStorage
from trpc_service.storage.framework_adapter import PlatformMemoryService, PlatformSessionService
from trpc_service.tenant import ModelConfig, TenantConfig, TenantRegistry

TENANT_ID = "t1"
SESSION_ID = "s1"
USER_ID = "u1"


def _tenant(tenant_id: str = TENANT_ID, **model_kwargs) -> TenantConfig:
    """构造租户配置（模型段可覆盖）。"""
    cfg = TenantConfig(tenant_id=tenant_id, name=f"租户{tenant_id}")
    if model_kwargs:
        cfg = cfg.model_copy(update={"model": ModelConfig(**model_kwargs)})
    return cfg


def _registry(*tenants: TenantConfig) -> TenantRegistry:
    table = {t.tenant_id: t for t in tenants}

    async def load_fn(tid: str):
        return table.get(tid)

    return TenantRegistry(load_fn=load_fn)


def _runtime(tenant: TenantConfig, *, storage=None, model=None) -> tuple[Runtime, InMemoryStorage, FakeLLMModel]:
    """装配 Runtime + FrameworkAgentRunner（注入假模型）。"""
    storage = storage or InMemoryStorage()
    fake = model or FakeLLMModel()
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    return Runtime(registry=_registry(tenant), storage=storage, runner=runner), storage, fake


# ------------------------------------------------------------------
# 用例 1: 模型实例构造（Spec §4-1）
# ------------------------------------------------------------------


def test_build_llm_model_deepseek():
    """provider=deepseek 应产出 OpenAIModel，且 endpoint 指向 DeepSeek。"""
    from trpc_agent_sdk.models import OpenAIModel

    cfg = ModelConfig(provider="deepseek", model_name="deepseek-chat", api_key_ref=SecretStr("sk-test-key"))
    model = build_llm_model(cfg)

    assert isinstance(model, OpenAIModel)
    assert model.name == "deepseek-chat"
    # endpoint 解析走纯函数断言（不依赖框架的私有属性 _base_url）
    assert resolve_model_endpoint("deepseek") == "https://api.deepseek.com"


def test_resolve_model_endpoint_explicit_overrides_default():
    """显式 base_url 优先级最高（自定义 OpenAI 兼容端点）。"""
    assert resolve_model_endpoint("deepseek", "https://my.proxy/v1") == "https://my.proxy/v1"
    assert resolve_model_endpoint("openai") == "https://api.openai.com/v1"


def test_resolve_model_endpoint_unknown_provider_raises():
    """未知 provider 且无 base_url 必须报错，不得静默打到错误地址。"""
    with pytest.raises(ValueError, match="未知 provider"):
        resolve_model_endpoint("some-unknown-provider")


def test_llm_agent_accepts_model_instance():
    """LlmAgent 必须接受 LLMModel 实例（传 dict 会 ValidationError）。"""
    from trpc_agent_sdk.agents import LlmAgent

    model = build_llm_model(
        ModelConfig(provider="deepseek", model_name="deepseek-chat", api_key_ref=SecretStr("sk-test-key")))
    agent = LlmAgent(name="t1:llm", model=model, instruction="hi")
    assert agent.model is model


def test_build_framework_agent_uses_real_model_instance():
    """agent.build_framework_agent 同样不可传 model dict（回归防护）。

    该函数曾因 app_name / system_prompt / max_rounds / model=dict 四处
    字段名错误而完全不可用（实测 4 个 ValidationError）。
    """
    from trpc_service.agent import build_framework_agent

    tenant = _tenant(provider="deepseek", model_name="deepseek-chat", api_key_ref=SecretStr("sk-test"))
    agent = build_framework_agent(tenant)

    from trpc_agent_sdk.models import OpenAIModel

    assert agent.name == "t1:llm"
    assert isinstance(agent.model, OpenAIModel)
    assert agent.instruction, "instruction（非 system_prompt）应生效"


# ------------------------------------------------------------------
# 用例 2: 事件 final 判定（Spec §4-2）
# ------------------------------------------------------------------


def _fw_event(*, text: str = "", partial: bool = False, with_tool_call: bool = False):
    """构造框架 Event（用于直接单测事件翻译）。

    工具调用挂在 content.parts[].function_call 上（不在 EventActions 上）。
    """
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    parts = []
    if text:
        parts.append(Part(text=text))
    if with_tool_call:
        from google.genai.types import FunctionCall

        parts.append(Part(function_call=FunctionCall(name="calculator", args={"expr": "1+1"})))
    return Event(author="bot", content=Content(role="model", parts=parts), partial=partial)


def test_translate_event_final_flag_not_constant():
    """final 标记必须随事件内容变化（修复前 getattr 绑到方法上恒为 True）。"""
    # 带工具调用的事件: 回合未结束 -> 非 final
    calling = translate_event(_fw_event(text="我来算一下", with_tool_call=True))
    assert calling, "带工具调用的事件应产出 content 事件"
    assert calling[0].is_final is False, "工具调用中却被标成 final"

    # 纯文本收尾事件 -> final
    final = translate_event(_fw_event(text="答案是 2"))
    assert final[0].is_final is True, "收尾事件未标成 final"


def test_translate_event_skips_partial():
    """流式增量必须跳过，否则回复文本会重复累加。"""
    assert translate_event(_fw_event(text="你", partial=True)) == []


def test_translate_event_tool_call_from_pydantic_object():
    """get_function_calls() 返回 FunctionCall 对象，不能用 .get() 取字段。"""
    events = translate_event(_fw_event(text="我来算一下", with_tool_call=True))
    tool_call = next((e for e in events if e.type == "tool_call"), None)
    assert tool_call is not None, "未产出 tool_call 事件"
    assert tool_call.tool_name == "calculator"
    assert tool_call.tool_input == {"expr": "1+1"}


def test_translate_event_error():
    """框架错误事件要转成平台 error 事件。"""
    from trpc_agent_sdk.events import Event

    event = Event(author="bot", error_code="rate_limit", error_message="被限流")
    (translated, ) = translate_event(event)
    assert translated.type == "error"
    assert "rate_limit" in translated.error


# ------------------------------------------------------------------
# 用例 3: 端到端（Spec §4-3）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_framework_runner_end_to_end():
    """完整链路: Runtime → Runner → 事件流 → 平台存储 → 回复。"""
    rt, storage, fake = _runtime(_tenant(model_name=FAKE_MODEL_NAME))

    resp = await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="你好"))

    assert resp.content, "回复不得为空"
    assert "你好" in resp.content, "回复应包含用户输入（证明链路贯通）"
    assert len(fake.calls) == 1, "应恰好调用模型一次"

    session = await storage.session.get_session(TENANT_ID, SESSION_ID)
    assert session, "session 必须落到平台存储"


def test_web_ui_end_to_end():
    """Web UI 入口: POST /chat → Filter 链 → Runtime → 框架 Runner。

    相比上一条，本用例额外覆盖 Channel Adapter 解析、幂等登记、
    Filter 链与 session_id 自动生成，是更完整的一条链路。
    """
    from fastapi.testclient import TestClient

    from trpc_service.web.app import build_gateway_app

    storage = InMemoryStorage()
    tenant = _tenant(model_name=FAKE_MODEL_NAME)
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: FakeLLMModel())
    runtime = Runtime(registry=_registry(tenant), storage=storage, runner=runner)
    app = build_gateway_app(registry=_registry(tenant), storage=storage, runtime=runtime)

    with TestClient(app) as client:
        resp = client.post(
            "/chat",
            json={
                "tenant_id": TENANT_ID,
                "user_id": USER_ID,
                "content": "你好",
                "msg_id": "m1",
            },
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("response_type") == "text", body
    assert "你好" in body.get("content", "")


def test_web_chat_writes_dual_audit_and_token_metrics():
    """一次 /chat: 网关治理 allow + Runtime 执行 executed 两层审计，token 指标递增。

    验收口径（PRD 4.2/4.4）: 审计不因「文档声称」而在——通过真实请求验证
    双行落库，且 /metrics 出现 llm token 计数（成本闭环的真实证据）。
    """
    from fastapi.testclient import TestClient

    from trpc_service.metrics import Metrics
    from trpc_service.web.app import build_gateway_app

    storage = InMemoryStorage()
    tenant = _tenant(model_name=FAKE_MODEL_NAME, input_price_per_1m_usd=1.0, output_price_per_1m_usd=2.0)
    metrics = Metrics(namespace="teneuris_e2e")
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: FakeLLMModel(usage=(800, 200)))
    runtime = Runtime(registry=_registry(tenant), storage=storage, runner=runner, metrics=metrics)
    app = build_gateway_app(registry=_registry(tenant), storage=storage, runtime=runtime)

    with TestClient(app) as client:
        resp = client.post(
            "/chat",
            json={
                "tenant_id": TENANT_ID,
                "user_id": USER_ID,
                "content": "端到端审计",
                "msg_id": "dual-e2e-1",
            },
        )
        assert resp.status_code == 200, resp.text

        logs = asyncio.run(storage.audit.query_logs(TENANT_ID, {}))
        decisions = sorted(log["decision"] for log in logs)
        assert decisions == ["allow", "executed"], f"期望 allow+executed，实际 {decisions}"
        executed = next(log for log in logs if log["decision"] == "executed")
        assert executed["payload"]["input_tokens"] == 800
        assert executed["payload"]["output_tokens"] == 200
        assert executed["cost"] == "0.0012", f"800/1e6*1 + 200/1e6*2 = 0.0012，实际 {executed['cost']}"
        # 指标真实接线（独立 namespace，避免污染全局）
        assert metrics.llm_tokens_input.labels(tenant_id=TENANT_ID, model=FAKE_MODEL_NAME)._value.get() == 800


# ------------------------------------------------------------------
# 用例 4: 会话跨轮保持（Spec §4-4）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_carried_across_turns():
    """同一 session 连问两轮，第二轮模型应看到第一轮历史。"""
    rt, _, fake = _runtime(_tenant(model_name=FAKE_MODEL_NAME))
    base = dict(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID)

    await rt.handle(AgentEvent(content="第一轮", **base))
    await rt.handle(AgentEvent(content="第二轮", **base))

    assert len(fake.calls) == 2
    second_turn_texts = fake.calls[1]["texts"]
    assert any("第一轮" in t for t in second_turn_texts), "第二轮未读到第一轮历史"


# ------------------------------------------------------------------
# 用例 5: 租户隔离（Spec §4-5）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_isolation_same_session_id():
    """两租户使用相同 session_id，会话不得串数据。"""
    tenant_a = _tenant("tenant-a", model_name=FAKE_MODEL_NAME)
    tenant_b = _tenant("tenant-b", model_name=FAKE_MODEL_NAME)
    storage = InMemoryStorage()

    rt_a, _, fake_a = _runtime(tenant_a, storage=storage)
    rt_b, _, fake_b = _runtime(tenant_b, storage=storage)

    await rt_a.handle(AgentEvent(tenant_id="tenant-a", session_id="shared", user_id="u1", content="A的私密"))
    await rt_b.handle(AgentEvent(tenant_id="tenant-b", session_id="shared", user_id="u1", content="B的消息"))

    assert fake_b.calls[0]["texts"] == ["B的消息"], "租户 B 读到了租户 A 的历史"


# ------------------------------------------------------------------
# 用例 6: 缺 key 显式失败（Spec §4-6）
# ------------------------------------------------------------------


def test_missing_api_key_raises(monkeypatch):
    """未配置 api_key 必须显式报错，不得静默回落到 mock。

    显式 delenv 密封化：开发机 .env 有真实 key 时 load_dotenv 会注入环境，
    不清理则本用例永远无法触发（gate-check 曾因此假失败）。
    """
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    with pytest.raises(ValueError, match="api_key"):
        build_llm_model(ModelConfig(provider="deepseek", model_name="deepseek-chat"))


def test_missing_api_key_reads_env(monkeypatch):
    """api_key 可经环境变量注入（PRD 4.5: 不落库）。"""
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-from-env")
    model = build_llm_model(ModelConfig(provider="deepseek", model_name="deepseek-chat"))
    assert model is not None


# ------------------------------------------------------------------
# 用例 7: 后端可切换（Spec §4-7）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_storage_backend_switchable():
    """同一套逻辑在 InMemory 后端下可跑通（Redis 见下一条）。"""
    rt, storage, _ = _runtime(_tenant(model_name=FAKE_MODEL_NAME))
    resp = await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="切换验证"))
    assert "切换验证" in resp.content
    assert await storage.session.get_session(TENANT_ID, SESSION_ID)


async def _redis_available() -> bool:
    try:
        import redis.asyncio as aioredis

        client = aioredis.Redis.from_url("redis://localhost:6379/0")
        await client.ping()
        await client.close()
        return True
    except Exception:  # noqa: BLE001 - 探测用途，任何异常都视为不可用
        return False


@pytest.mark.asyncio
async def test_storage_backend_redis_when_available():
    """多节点场景下 Session 落真实 Redis（PRD 2.3，本机无 Redis 时跳过）。

    开发机未装 redis-server 时自动跳过；Docker 镜像内置 redis-server，
    在那里会自动执行本用例。
    """
    if not await _redis_available():
        pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    import redis.asyncio as aioredis

    from trpc_service.storage import StorageFactory
    from trpc_service.tenant import DataBackendConfig

    client = aioredis.Redis.from_url("redis://localhost:6379/0")
    storage = await StorageFactory(redis=client).create(
        TENANT_ID,
        DataBackendConfig(session="redis", memory="redis", summary="inmemory", audit="inmemory"),
    )
    rt, _, _ = _runtime(_tenant(model_name=FAKE_MODEL_NAME), storage=storage)
    resp = await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="redis验证"))

    assert "redis验证" in resp.content
    assert await storage.session.get_session(TENANT_ID, SESSION_ID), "session 未落到 Redis"
    await storage.close()


# ------------------------------------------------------------------
# 适配层专项（用例 3/4/5 的前置能力）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_platform_session_service_roundtrip():
    """框架 Session 对象经平台存储往返后不丢字段。"""
    svc = PlatformSessionService(InMemoryStorage(), tenant_id=TENANT_ID)

    session = await svc.create_session(app_name="t1:llm", user_id=USER_ID, session_id=SESSION_ID, state={"k": "v"})
    loaded = await svc.get_session(app_name="t1:llm", user_id=USER_ID, session_id=SESSION_ID)

    assert loaded is not None
    assert loaded.id == session.id == SESSION_ID
    assert loaded.state["k"] == "v"


@pytest.mark.asyncio
async def test_platform_session_service_tenant_scoped():
    """适配层必须以 tenant_id 隔离（PRD 1.4）。"""
    storage = InMemoryStorage()
    svc_a = PlatformSessionService(storage, tenant_id="tenant-a")
    svc_b = PlatformSessionService(storage, tenant_id="tenant-b")

    await svc_a.create_session(app_name="t:llm", user_id=USER_ID, session_id="shared", state={"who": "a"})

    assert await svc_b.get_session(app_name="t:llm", user_id=USER_ID, session_id="shared") is None, "租户间串数据"


def test_platform_memory_service_is_async():
    """框架 memory 服务实现为 async，与平台层对齐（Spec §0 实测结论）。"""
    import inspect

    svc = PlatformMemoryService(InMemoryStorage(), tenant_id=TENANT_ID)
    assert inspect.iscoroutinefunction(svc.store_session)
    assert inspect.iscoroutinefunction(svc.search_memory)


# ------------------------------------------------------------------
# 成本闭环（PRD 4.2/4.4/6-10）: usage 透传 -> 执行审计 -> 预算累加
# ------------------------------------------------------------------


def test_translate_event_carries_usage_tokens():
    """收尾事件携带 usage_metadata 时，content 事件必须透传 token（PRD 4.2）。"""
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part
    from trpc_agent_sdk.types._usage import GenerateContentResponseUsageMetadata

    usage = GenerateContentResponseUsageMetadata(prompt_token_count=123, candidates_token_count=45)
    event = Event(author="bot",
                  content=Content(role="model", parts=[Part(text="hi")]),
                  turn_complete=True,
                  usage_metadata=usage)
    (content, ) = [e for e in translate_event(event) if e.type == "content"]
    assert content.input_tokens == 123
    assert content.output_tokens == 45


def test_translate_event_no_usage_is_zero():
    """无 usage_metadata 的事件 token 恒为 0（不抛错）。"""
    (content, ) = [e for e in translate_event(_fw_event(text="hi")) if e.type == "content"]
    assert content.input_tokens == 0
    assert content.output_tokens == 0


@pytest.mark.asyncio
async def test_execution_audit_and_cost_recorded():
    """执行审计: Runner 真实跑完 -> decision=executed 且 token/成本入 payload。

    审计双行语义: 网关 AuditFilter 记治理（allow/block），Runtime 记执行
    （executed/execution_error），两层互不覆盖（PRD 4.4）。
    """
    tenant = _tenant(model_name=FAKE_MODEL_NAME, input_price_per_1m_usd=1.0, output_price_per_1m_usd=2.0)
    storage = InMemoryStorage()
    fake = FakeLLMModel(usage=(1000, 500))
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    rt = Runtime(registry=_registry(tenant), storage=storage, runner=runner)

    resp = await rt.handle(
        AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="成本测试", trace_id="trc-cost"))

    assert resp.content
    logs = await storage.audit.query_logs(TENANT_ID, {"decision": "executed"})
    assert len(logs) == 1, "应落一条执行审计（executed）"
    entry = logs[0]
    assert entry["trace_id"] == "trc-cost"
    assert entry["cost"] == "0.002", f"成本应 = 1000/1e6*1 + 500/1e6*2 = 0.002，实际 {entry['cost']}"
    payload = entry["payload"]
    assert payload["input_tokens"] == 1000
    assert payload["output_tokens"] == 500


@pytest.mark.asyncio
async def test_execution_audit_when_runner_errors():
    """Runner 出错 -> decision=execution_error 且 error_type 留痕（不写 session）。"""
    tenant = _tenant(model_name=FAKE_MODEL_NAME)
    storage = InMemoryStorage()
    fake = FakeLLMModel(fail_with="模型超时")
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    rt = Runtime(registry=_registry(tenant), storage=storage, runner=runner)

    resp = await rt.handle(
        AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="会失败", trace_id="trc-err"))

    assert resp.response_type.value == "error"
    logs = await storage.audit.query_logs(TENANT_ID, {"decision": "execution_error"})
    assert len(logs) == 1, "执行失败也应落审计（execution_error）"
    assert logs[0]["error_type"], "失败原因应留痕"
    assert logs[0]["trace_id"] == "trc-err"


@pytest.mark.asyncio
async def test_budget_tracker_persists_and_invalidates():
    """预算闭环: Runtime 结算 -> BudgetTracker 累加 used_budget_usd + 缓存失效。

    用内存 tenant_store 替身断言原子累加被调用、registry 缓存被失效，
    BudgetFilter 下一请求读到的 used 已增长（PRD 6-10）。
    """
    from trpc_service.tenant.budget import BudgetTracker

    calls: list[float] = []
    invalidated: list[str] = []

    class FakeTenantStore:

        async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
            calls.append(delta_usd)

    registry = _registry(_tenant(model_name=FAKE_MODEL_NAME, input_price_per_1m_usd=1.0, output_price_per_1m_usd=2.0))
    invalidated: list[str] = []
    registry.invalidate = lambda tid: invalidated.append(tid)  # type: ignore[method-assign]
    tracker = BudgetTracker(tenant_store=FakeTenantStore(), registry=registry)

    fake = FakeLLMModel(usage=(1_000_000, 500_000))
    storage = InMemoryStorage()
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    rt = Runtime(registry=registry, storage=storage, runner=runner, budget_tracker=tracker)

    await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="预算"))

    # 等后台预算任务跑完（_bg_tasks 引用挂实例，可 await 后再断言）
    if rt._bg_tasks:
        await asyncio.gather(*list(rt._bg_tasks))

    assert calls, "used_budget_usd 必须被累加"
    assert calls[0] == pytest.approx(2.0), "1M in @1/1e6 + 0.5M out @2/1e6 = 2.0"
    assert invalidated == [TENANT_ID], "累加后必须失效本地缓存，下一请求回源读新预算"


@pytest.mark.asyncio
async def test_budget_tracker_noop_without_cost():
    """价格未配置（0）时 cost=0 -> 不触发预算持久化（tokens 仍入 metrics/审计）。"""
    from trpc_service.tenant.budget import BudgetTracker

    called = False

    class FakeTenantStore:

        async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
            nonlocal called
            called = True

    fake = FakeLLMModel(usage=(1000, 500))  # 有 token 但价格 0
    storage = InMemoryStorage()
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    rt = Runtime(registry=_registry(_tenant(model_name=FAKE_MODEL_NAME)),
                 storage=storage,
                 runner=runner,
                 budget_tracker=BudgetTracker(tenant_store=FakeTenantStore()))

    await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="零价"))
    if rt._bg_tasks:
        await asyncio.gather(*list(rt._bg_tasks))

    assert not called, "无成本时不应写预算表"
    logs = await storage.audit.query_logs(TENANT_ID, {"decision": "executed"})
    assert logs[0]["payload"]["input_tokens"] == 1000, "token 仍应入审计"


@pytest.mark.asyncio
async def test_token_cost_metrics_increment():
    """token/成本指标真实接线（PRD 4.2）: 请求跑完 llm_tokens_* / tenant_cost 递增。"""
    from trpc_service.metrics import Metrics

    metrics = Metrics(namespace="teneuris_test")
    tenant = _tenant(model_name=FAKE_MODEL_NAME, input_price_per_1m_usd=1.0, output_price_per_1m_usd=2.0)
    storage = InMemoryStorage()
    fake = FakeLLMModel(usage=(1000, 500))
    runner = FrameworkAgentRunner(storage=storage, model_factory=lambda _cfg: fake)
    rt = Runtime(registry=_registry(tenant), storage=storage, runner=runner, metrics=metrics)

    await rt.handle(AgentEvent(tenant_id=TENANT_ID, session_id=SESSION_ID, user_id=USER_ID, content="指标"))

    assert metrics.llm_tokens_input.labels(tenant_id=TENANT_ID, model=FAKE_MODEL_NAME)._value.get() == 1000
    assert metrics.llm_tokens_output.labels(tenant_id=TENANT_ID, model=FAKE_MODEL_NAME)._value.get() == 500
    # 成本 1000/1e6*1 + 500/1e6*2 = 0.002
    assert metrics.tenant_cost.labels(tenant_id=TENANT_ID, cost_type="llm")._value.get() == pytest.approx(0.002)


# ------------------------------------------------------------------
# 危险工具运行时门控（PRD 4.1）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dangerous_tool_blocked_without_confirmation():
    """未二次确认的危险工具在运行时被拦截，不执行 spec.func。"""
    from trpc_service.tool.builder import tool_confirmation_required
    from trpc_service.tool.registry import ToolSpec

    executed = {"flag": False}

    async def dangerous_func(*, path: str, **kwargs: Any) -> Any:
        executed["flag"] = True
        return {"deleted": path}

    spec = ToolSpec(name="delete_file",
                    description="危险",
                    func=dangerous_func,
                    dangerous=True,
                    parameters={
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string"
                            }
                        }
                    })
    perms = _tenant().tools.model_copy(update={"dangerous_tools": ["delete_file"], "require_confirmation": True})

    assert tool_confirmation_required(spec, perms, frozenset()), "未确认危险工具应被判定需拦截"
    assert not tool_confirmation_required(spec, perms, frozenset({"delete_file"})), "已确认则应放行"
    # 审查 09-04 默认反转：平台标记 dangerous 即拦截，租户名单是追加而非前置条件
    assert tool_confirmation_required(spec, _tenant().tools, frozenset()), \
        "平台 dangerous 但租户未列入名单也应拦截（默认反转）"
    # 非 dangerous 工具且未列入名单 → 不拦
    safe_spec = ToolSpec(name="echo", description="安全", func=lambda: None, dangerous=False)
    assert not tool_confirmation_required(safe_spec, _tenant().tools, frozenset())


@pytest.mark.asyncio
async def test_make_tool_impl_blocks_unconfirmed_and_runs_confirmed():
    """impl 门控: 未确认 -> blocked 不执行 func；确认集含该工具 -> 正常执行。"""
    from trpc_service.tool.builder import make_tool_impl
    from trpc_service.tool.registry import ToolSpec

    executed = {"count": 0}

    async def dangerous_func(*, path: str, **kwargs: Any) -> Any:
        executed["count"] += 1
        return {"deleted": path}

    spec = ToolSpec(name="delete_file",
                    description="危险",
                    func=dangerous_func,
                    dangerous=True,
                    parameters={
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string"
                            }
                        }
                    })
    tenant = _tenant(model_name=FAKE_MODEL_NAME)
    tenant = tenant.model_copy(
        update={
            "tools": tenant.tools.model_copy(update={
                "allowlist": ["delete_file"],
                "dangerous_tools": ["delete_file"]
            })
        })

    # 未确认: 拦截，func 不被调用
    blocked = await make_tool_impl(spec, tenant, frozenset())(path="/tmp/x")
    assert isinstance(blocked, dict)
    assert blocked.get("blocked") is True
    assert "confirmation" in blocked.get("error", "")
    assert executed["count"] == 0, "未确认的危险工具不应执行"

    # 已确认: 放行执行
    result = await make_tool_impl(spec, tenant, frozenset({"delete_file"}))(path="/tmp/x")
    assert result == {"deleted": "/tmp/x"}
    assert executed["count"] == 1


# ------------------------------------------------------------------
# 并发写会话一致性（PRD 2.3-A / 09-04 联调缺陷回归）
# ------------------------------------------------------------------


def _fw_session(app_name: str) -> "Session":
    """构造独立的框架 Session 对象（模拟并发 Runner 各持一份内存态）。"""
    import time as _time

    from trpc_agent_sdk.sessions import Session
    from trpc_agent_sdk.utils import user_key

    return Session(id=SESSION_ID,
                   app_name=app_name,
                   user_id=USER_ID,
                   state={},
                   last_update_time=_time.time(),
                   save_key=user_key(app_name, USER_ID))


def _fw_evt(author: str, text: str) -> "Event":
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    return Event(author=author, content=Content(parts=[Part(text=text)]), turn_complete=True)


@pytest.mark.asyncio
async def test_platform_session_service_concurrent_append_no_lost_events():
    """N 个并发 Runner 各自 append 事件不得互相冲掉（09-04 联调缺陷回归）。

    缺陷背景：修复前 append_event/update_session 是无锁全量覆盖，6 并发
    只剩 1 条模型回复（其余被覆盖冲掉）。
    """
    storage = InMemoryStorage()
    svc = PlatformSessionService(storage, tenant_id=TENANT_ID)
    n = 6

    async def one(i: int) -> None:
        # 每个并发请求独立构造 Session 对象（同 id），模拟不同节点上的 Runner
        session = _fw_session(f"app{i}")
        user_evt = _fw_evt("user", f"问题{i}")
        await svc.append_event(session, user_evt)
        model_evt = _fw_evt("model", f"回复{i}")
        await svc.append_event(session, model_evt)

    await asyncio.gather(*(one(i) for i in range(n)))

    from trpc_agent_sdk.sessions import Session

    raw = await storage.session.get_session(TENANT_ID, f"{SESSION_ID}:fw")
    merged = Session.model_validate(raw["state"]["_fw"])
    texts = []
    for e in merged.events:
        for p in (e.content.parts if e.content else []):
            if getattr(p, "text", None):
                texts.append(p.text)
    assert len(merged.events) == 2 * n, f"事件应 {2 * n} 条，实际 {len(merged.events)} 条（丢失更新）"
    for i in range(n):
        assert f"问题{i}" in texts and f"回复{i}" in texts


@pytest.mark.asyncio
async def test_platform_session_service_sequential_append_order_kept():
    """顺序追加（单请求多次 append）语义不变：事件按序累积、不重复。"""
    storage = InMemoryStorage()
    svc = PlatformSessionService(storage, tenant_id=TENANT_ID)
    session = _fw_session("app")

    for i in range(5):
        await svc.append_event(session, _fw_evt("model", f"chunk{i}"))

    loaded = await svc.get_session(app_name="app", user_id=USER_ID, session_id=SESSION_ID)
    texts = []
    for e in loaded.events:
        for p in (e.content.parts if e.content else []):
            if getattr(p, "text", None):
                texts.append(p.text)
    assert texts == [f"chunk{i}" for i in range(5)]
