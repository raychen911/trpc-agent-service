# ===================================================================
# tool.builder - 工具按租户过滤与框架适配（平台层新增）
# ===================================================================
# 说明: PRD 0.4「Tool / MCP 复用框架 FunctionTool」——本包职责边界为
#   「工具」本身，不含 Agent 组装:
#   1. select_tools: 按租户 tool_permissions 过滤工具（PRD 1.4）
#   2. make_framework_tool: 平台 ToolSpec -> 框架 FunctionTool（含危险工具门控）
#   3. ToolContext: 平台工具执行上下文（Mock/自测用，模拟框架 InvocationContext）
#
# Agent 组装（模型 + 工具集 + 指令 -> LlmAgent）在 agent/builder.py
# （build_framework_agent）。依赖方向单向: agent -> tool。
# 规范: 白名单过滤在平台层完成（Filter 之外再兜底一层）。
# ===================================================================

from __future__ import annotations

import inspect
import time
from typing import Any, Optional

from ..metrics.metrics import get_metrics
from ..tenant.models import TenantConfig
from .registry import ToolRegistry, ToolSpec, execute_tool, get_default_registry

_SCHEMA_TYPES: dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}
"""JSON Schema 类型 -> Python 注解（供框架据函数签名推导工具参数声明）。"""

TOOL_CONTEXT_PARAM = "tool_context"
"""框架注入工具上下文的参数名（见 FunctionTool._run_async_impl）。"""


def _build_tool_signature(spec: ToolSpec) -> inspect.Signature:
    """按 spec.parameters 合成函数签名，供框架推导工具参数声明。

    实测（trpc-agent-py v1.1.19）三条约束，缺一不可:
    1. `FunctionTool` 只能用 `inspect.signature(func)` 推导参数声明；
       而 impl 是 `(**kwargs)`，会推导出**空 schema**，LLM 无法传参。
       故用 `__signature__` 注入合成签名（inspect.signature 优先读取它）。
    2. 必须显式声明 `tool_context` 参数，框架才注入租户上下文
       （`_run_async_impl`: `if TOOL_CONTEXT in signature.parameters`）；
       不声明则工具执行时 tenant_id/user_id/session_id 恒为空。
    3. `tool_context` 会被 `_get_declaration` 的
       `ignore_params=[TOOL_CONTEXT, INPUT_STREAM]` 自动过滤，不会暴露给 LLM。
    """
    schema = spec.parameters or {}
    properties: dict[str, Any] = schema.get("properties") or {}
    # 未显式声明 required 时，全部参数视为必填（内置工具均为单参数且必填）
    required = set(schema.get("required") or properties)

    params: list[inspect.Parameter] = []
    for name, prop in properties.items():
        annotation = _SCHEMA_TYPES.get((prop or {}).get("type", ""), str)
        if name in required:
            params.append(inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation))
        else:
            params.append(inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation))

    # tool_context: 框架据此注入租户上下文；不出现在给 LLM 的声明里
    params.append(inspect.Parameter(TOOL_CONTEXT_PARAM, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=Any))
    # 保留 **kwargs: impl 真实签名仍是 (**kwargs)，兜底接收未声明参数
    params.append(inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD))
    return inspect.Signature(params)


def tool_confirmation_required(
    spec: "ToolSpec",
    perms: Any,
    confirmed_tools: frozenset[str],
) -> bool:
    """危险工具运行时门控判定（PRD 4.1，纯函数便于单测）。

    判定语义（审查 09-04 默认反转）：平台在工具定义上标记 dangerous 即视为
    需二次确认——租户 `dangerous_tools` 名单是**追加**而非前置条件；此前
    「spec.dangerous 且租户名单都命中才拦」意味着平台声明了危险但租户漏配
    时完全不拦。租户 `require_confirmation=False` 仍是总开关（显式关闭）。

    Args:
        spec: 平台工具定义（dangerous 标记）
        perms: 租户 ToolPermissions（require_confirmation 总开关 + 名单）
        confirmed_tools: 本轮已确认的危险工具名集合

    Returns:
        True = 应拦截（LLM 动态调用该工具时不执行，返回需确认提示）
    """
    if not perms.require_confirmation:
        return False
    flagged = spec.dangerous or perms.requires_confirmation(spec.name)
    return flagged and spec.name not in confirmed_tools


def select_tools(tenant: TenantConfig, registry: Optional[ToolRegistry] = None) -> list[ToolSpec]:
    """按租户工具权限过滤可用工具（allowlist/blocklist/dangerous，PRD 1.4）。"""
    reg = registry or get_default_registry()
    return reg.filter_by_permissions(tenant.tools)


class ToolContext:
    """平台工具执行上下文（自测 / Mock 用，对齐框架 InvocationContext 形态）。"""

    def __init__(self, *, tenant_id: str, user_id: str, session_id: str, trace_id: str = "") -> None:
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.session_id = session_id
        self.trace_id = trace_id


async def run_tool_with_context(
    spec: ToolSpec,
    ctx: ToolContext,
    args: dict[str, Any],
) -> Any:
    """在平台 ToolContext 下执行工具（Mock Runner 的 execute_tool 逻辑）。"""
    return await execute_tool(
        spec,
        tenant_id=ctx.tenant_id,
        user_id=ctx.user_id,
        session_id=ctx.session_id,
        args=args,
    )


def make_knowledge_search_spec(knowledge: Any, tenant_id: str) -> ToolSpec:
    """构造 RAG 检索工具（平台知识库 -> LLM 工具调用链路）。

    与普通工具同构: 经 make_framework_tool 包装为框架 FunctionTool，
    租户白名单门控。
    """

    async def func(*, query: str, **kwargs: Any) -> dict[str, Any]:
        hits = await knowledge.search(tenant_id, query, top_k=3)
        if not hits:
            return {"result": "未命中知识库", "hits": 0}
        lines = "\n".join(f"[{hit['doc_id']}] {hit['content']}" for hit in hits)
        return {"result": lines, "hits": len(hits)}

    return ToolSpec(
        name="knowledge_search",
        description="从本租户知识库检索与用户问题相关的内容（RAG）。涉及平台文档、内部知识时调用。",
        func=func,
        parameters={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string"
                }
            },
        },
    )


def make_tool_impl(spec: ToolSpec, tenant: TenantConfig, confirmed_tools: frozenset[str]):
    """构造工具的框架入口包装 impl（危险工具运行时门控在此拦截，PRD 4.1）。

    返回 async impl(**kwargs)，与 make_tool 共用，便于脱离框架直接单测门控。
    未二次确认的危险工具不执行 spec.func，返回「需确认」提示（不抛异常，
    由 LLM 回填给用户），确认集放行。真实执行路径埋 tool_latency 指标
    （PRD 4.2；被门控拦截的调用不计延迟）。
    """

    async def impl(**kwargs: Any) -> Any:
        if tool_confirmation_required(spec, tenant.tools, confirmed_tools):
            return {
                "error": f"tool requires confirmation: {spec.name}",
                "blocked": True,
                "hint": "该工具属于危险操作，未获用户/管理员二次确认，已拒绝执行。",
            }
        tool_ctx = kwargs.pop("tool_context", None)
        tenant_id = getattr(tool_ctx, "tenant_id", "") or tenant.tenant_id
        user_id = getattr(tool_ctx, "user_id", "") or ""
        session_id = getattr(tool_ctx, "session_id", "") or ""
        started = time.perf_counter()
        try:
            return await spec.func(tenant_id=tenant_id, user_id=user_id, session_id=session_id, **kwargs)
        finally:
            # 执行延迟（含失败路径）：只有真实执行才计入，门控拦截不算
            get_metrics().tool_latency.labels(tenant_id=tenant_id,
                                              tool_name=spec.name).observe(time.perf_counter() - started)

    return impl


def make_framework_tool(spec: ToolSpec, tenant: TenantConfig, confirmed_tools: frozenset[str] = frozenset()) -> Any:
    """把平台 ToolSpec 包装为框架 FunctionTool（工具 -> 框架适配边界）。

    危险工具运行时门控在 make_tool_impl 内拦截（PRD 4.1）。

    Args:
        spec: 平台工具定义
        tenant: 租户配置（门控读取 dangerous_tools / require_confirmation）
        confirmed_tools: 本轮已通过二次确认的危险工具名集合

    Returns:
        框架 FunctionTool 实例；未安装 trpc-agent-py 时抛 ImportError。
    """
    try:
        from trpc_agent_sdk.tools import FunctionTool  # type: ignore
    except ImportError as exc:  # pragma: no cover - 环境相关
        raise ImportError("make_framework_tool 需要安装 trpc-agent-py；自测请使用 MockAgentRunner") from exc

    impl = make_tool_impl(spec, tenant, confirmed_tools)

    # FunctionTool 据函数名推导工具名（见 trpc_agent_sdk.tools.FunctionTool）：
    # 所有 wrapper 若都叫 impl 会撞名（DeepSeek 报 Tool names must be unique），
    # 故用 spec.name 覆盖 __name__，并用 spec.description 覆盖 __doc__ 作为工具描述。
    impl.__name__ = spec.name
    impl.__doc__ = spec.description
    # 补上参数声明，否则 LLM 看不到可传参数（见 _build_tool_signature 实测注释）
    impl.__signature__ = _build_tool_signature(spec)
    # FunctionTool 由框架根据函数签名自动生成声明
    return FunctionTool(impl)
