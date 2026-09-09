# tool 模块单元测试
from types import SimpleNamespace

import pytest

from tests.fakes import FakeLLMModel
from trpc_service.tenant import TenantConfig
from trpc_service.tool import (
    ToolContext,
    ToolRegistry,
    ToolSpec,
    get_default_registry,
    run_tool_with_context,
    select_tools,
)


def test_default_registry_contents():
    registry = get_default_registry()
    names = {t.name for t in registry.all()}
    assert {"echo", "get_time", "calculator", "web_search", "delete_file"} <= names
    assert registry.get("delete_file").dangerous


def test_select_tools_permissions():
    t = TenantConfig(tenant_id="t", name="x", tools={"allowlist": ["echo", "calculator"]})
    selected = select_tools(t)
    assert {s.name for s in selected} == {"echo", "calculator"}
    # 空 allowlist = 不限制
    open_tenant = TenantConfig(tenant_id="t2", name="y")
    assert len(select_tools(open_tenant)) == 5


def test_blocklist_priority():
    t = TenantConfig(tenant_id="t", name="x", tools={"allowlist": ["echo", "calculator"], "blocklist": ["echo"]})
    selected = {s.name for s in select_tools(t)}
    assert "echo" not in selected


@pytest.mark.asyncio
async def test_execute_calculator():
    registry = get_default_registry()
    ctx = ToolContext(tenant_id="t1", user_id="u1", session_id="s1")
    result = await run_tool_with_context(registry.get("calculator"), ctx, {"expression": "2*3+1"})
    assert result["result"] == 7


@pytest.mark.asyncio
async def test_calculator_injection_blocked():
    registry = get_default_registry()
    ctx = ToolContext(tenant_id="t1", user_id="u1", session_id="s1")
    result = await run_tool_with_context(registry.get("calculator"), ctx,
                                         {"expression": "__import__('os').system('id')"})
    assert "error" in result


@pytest.mark.asyncio
async def test_echo_tool():
    registry = get_default_registry()
    ctx = ToolContext(tenant_id="t", user_id="u", session_id="s")
    result = await run_tool_with_context(registry.get("echo"), ctx, {"content": "hi"})
    assert result == {"echo": "hi"}


def test_duplicate_register_raises():
    registry = ToolRegistry()

    async def f(**kwargs):
        return None

    registry.register(ToolSpec(name="x", description="", func=f))
    with pytest.raises(ValueError):
        registry.register(ToolSpec(name="x", description="", func=f))


# ------------------------------------------------------------------
# 框架工具接线回归（P0 真实 LLM 验证中暴露的两处缺陷，见 DEVELOPMENT_LOG §2.9）
# ------------------------------------------------------------------


def _build_framework_tools(tools=None, tenant=None):
    """用假模型构建框架 Agent，返回 {工具名: FunctionTool}。"""
    from trpc_service.agent.builder import build_framework_agent

    tenant = tenant or TenantConfig(tenant_id="t1", name="x", tools={"allowlist": ["echo", "calculator"]})
    agent = build_framework_agent(tenant, tools=tools, model_factory=lambda _cfg: FakeLLMModel())
    return {t.name: t for t in agent.tools}


def test_framework_tool_names_are_unique():
    """wrapper 若都叫 impl 会撞名（DeepSeek 报 Tool names must be unique）。"""
    tools = _build_framework_tools()
    assert set(tools) == {"echo", "calculator"}, f"工具名应取自 spec.name，实际: {set(tools)}"


def test_framework_tool_declares_parameters():
    """带参工具必须推导出非空参数声明，否则 LLM 无法传参。"""
    tools = _build_framework_tools()
    decl = tools["calculator"]._get_declaration()
    props = (decl.parameters.properties if decl.parameters else None) or {}
    assert "expression" in props, f"calculator 参数声明为空，LLM 无法传参: {props}"
    # tool_context 是框架内部参数，不得暴露给 LLM
    assert "tool_context" not in props


@pytest.mark.asyncio
async def test_framework_tool_receives_tenant_context():
    """tool_context 必须注入，否则工具执行时租户上下文恒为空。"""
    seen = {}

    async def probe(*, expression: str, **kwargs):
        seen.update(expression=expression, tenant_id=kwargs.get("tenant_id"), user_id=kwargs.get("user_id"))
        return {"ok": True}

    spec = ToolSpec(
        name="probe",
        description="探针",
        func=probe,
        parameters={
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string"
                }
            }
        },
    )
    tools = _build_framework_tools(tools=[spec])
    ctx = SimpleNamespace(tenant_id="t1", user_id="u1", session_id="s1")
    await tools["probe"]._run_async_impl(tool_context=ctx, args={"expression": "1+2"})

    assert seen == {"expression": "1+2", "tenant_id": "t1", "user_id": "u1"}, f"租户上下文未注入: {seen}"


# ------------------------------------------------------------------
# tool_latency 指标接线（PRD 4.2；09-05 缺口 3）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_latency_observed_on_execution():
    """工具真实执行应计入 tool_latency 直方图。"""
    from trpc_service.metrics.metrics import get_metrics
    from trpc_service.tool.builder import make_tool_impl

    tenant = TenantConfig(tenant_id="t_lat", name="x")
    impl = make_tool_impl(get_default_registry().get("echo"), tenant, frozenset())
    ctx = SimpleNamespace(tenant_id="t_lat", user_id="u1", session_id="s1")
    await impl(tool_context=ctx, content="hi")
    hist = get_metrics().tool_latency.labels(tenant_id="t_lat", tool_name="echo")
    assert hist._sum.get() > 0, "工具真实执行应计入 tool_latency"


@pytest.mark.asyncio
async def test_tool_latency_not_observed_when_blocked():
    """危险工具被门控拦截时不执行，不应计入 tool_latency。"""
    from trpc_service.metrics.metrics import get_metrics
    from trpc_service.tool.builder import make_tool_impl

    async def probe(**kwargs):
        return {"ok": True}

    spec = ToolSpec(name="danger_probe", description="", func=probe, dangerous=True)
    tenant = TenantConfig(tenant_id="t_lat2", name="x")
    impl = make_tool_impl(spec, tenant, frozenset())
    ctx = SimpleNamespace(tenant_id="t_lat2", user_id="u1", session_id="s1")
    result = await impl(tool_context=ctx)
    assert result.get("blocked") is True
    hist = get_metrics().tool_latency.labels(tenant_id="t_lat2", tool_name="danger_probe")
    assert hist._sum.get() == 0, "门控拦截不应计入 tool_latency"
