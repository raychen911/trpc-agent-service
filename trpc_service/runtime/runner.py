# ===================================================================
# runtime.runner - Agent Runner 抽象与实现
# ===================================================================
# 说明: PRD 0.3「复用框架 Runner」——框架 Runner.run_async 产出事件流。
#   平台侧定义 AgentRunner 抽象，两个实现:
#   1. FrameworkAgentRunner: 包装 tRPC-Agent-Python 的 Runner（生产，
#      需安装 trpc-agent-py；框架符号懒加载，未安装时 import 本模块不失败）
#   2. MockAgentRunner: 本地自测回声 Runner（Web UI 自测 / 单测，无需 LLM key）
# 规范: Runner 无状态；输出统一为 RunnerEvent 流（见 runtime/events.py）。
#
# 实测基线（trpc-agent-py v1.1.19，详见 DEVELOPMENT_LOG「阶段二 Spec」§0）:
#   - 正确入口是 Runner.run_async，不是自行构造 InvocationContext 后调
#     agent.run_async（后者跳过 session 落库 / memory 沉淀 / telemetry）
#   - LlmAgent.model 只接受 str | LLMModel | Callable，传 dict 报
#     ValidationError；字段是 name / instruction，不是 app_name / system_prompt
#   - Event.is_final_response 是方法，必须调用，不可当属性取布尔
#   - new_agent_context(timeout=...) 单位是**毫秒**（默认 3000ms）
# ===================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, Optional

from ..agent.model_factory import build_llm_model
from ..storage.base import Storage
from ..storage.manager import StorageManager
from ..tenant.models import TenantConfig
from .events import RunnerEvent

_DEFAULT_TIMEOUT_MS = 120_000
"""默认单次 Agent 运行超时（毫秒）。

框架 AgentContext._timeout 单位为毫秒且默认仅 3000ms（3 秒），
对真实 LLM 调用过短，故平台侧显式放大并允许按部署调参。
"""


class AgentRunner(ABC):
    """Agent 执行器抽象（平台侧归一化事件流）。"""

    name: str = "base"

    @abstractmethod
    def run(
            self,
            *,
            tenant: TenantConfig,
            user_id: str,
            session_id: str,
            new_message: str,
            memories: Optional[list[dict[str, Any]]] = None,
            session_state: Optional[dict[str, Any]] = None,
            confirmed_tools: frozenset[str] = frozenset(),
    ) -> AsyncIterator[RunnerEvent]:
        """执行一次 Agent 推理，产出归一化事件流。

        Args:
            confirmed_tools: 本轮已通过二次确认的危险工具集（PRD 4.1）；
                为空表示无确认，危险工具被运行时门控拦截（见 tool.builder）。
        """


class FrameworkAgentRunner(AgentRunner):
    """tRPC-Agent-Python Runner 包装（生产实现）。

    走框架 `Runner.run_async` 入口，Session / Memory 由平台存储层适配
    （`storage.framework_adapter`），从而保住多后端 + 分布式锁 + 幂等。

    框架符号懒加载: 未安装 trpc-agent-py 时可 import 本模块，首次 run 才报错。
    """

    name = "framework"

    def __init__(
        self,
        storage: Storage,
        *,
        model_factory: Optional[Callable[..., Any]] = None,
        artifact_service: Optional[Any] = None,
        timeout_ms: int = _DEFAULT_TIMEOUT_MS,
        storage_manager: Optional[StorageManager] = None,
    ) -> None:
        """
        Args:
            storage: 平台 Storage（session / memory 后端由此注入）。
            model_factory: `(ModelConfig) -> LLMModel`，默认走真实模型工厂；
                测试可注入假模型以脱离真实 HTTP 依赖。
            artifact_service: 框架 Artifact 服务，None 时由框架默认处理。
            timeout_ms: 单次运行超时（**毫秒**，框架单位）。
            storage_manager: 按租户懒建 Storage 的容器（PRD 2.1）；
                None 时所有租户共用 `storage`。
        """
        self._storage = storage
        self._storage_manager = storage_manager
        self._model_factory = model_factory or build_llm_model
        self._artifact_service = artifact_service
        self._timeout_ms = timeout_ms
        self._services: dict[tuple[int, str], tuple[Any, Any]] = {}

    async def _resolve_storage(self, tenant: TenantConfig) -> Storage:
        """按租户解析 Storage：有 manager 则懒建，否则回落启动时单例。"""
        if self._storage_manager is not None:
            return await self._storage_manager.get(tenant)
        return self._storage

    def _services_for(self, storage: Storage, tenant_id: str) -> tuple[Any, Any]:
        """按 (storage, tenant_id) 缓存 Session / Memory 服务（隔离边界 = 租户）。

        storage 为 key 一部分：不同租户使用不同后端时服务绑定各自的 Storage。
        """
        key = (id(storage), tenant_id)
        cached = self._services.get(key)
        if cached is None:
            from ..storage.framework_adapter import PlatformMemoryService, PlatformSessionService

            cached = (
                PlatformSessionService(storage, tenant_id),
                PlatformMemoryService(storage, tenant_id),
            )
            self._services[key] = cached
        return cached

    async def run(
            self,
            *,
            tenant: TenantConfig,
            user_id: str,
            session_id: str,
            new_message: str,
            memories: Optional[list[dict[str, Any]]] = None,
            session_state: Optional[dict[str, Any]] = None,
            confirmed_tools: frozenset[str] = frozenset(),
    ) -> AsyncIterator[RunnerEvent]:
        from trpc_agent_sdk.configs import RunConfig  # type: ignore
        from trpc_agent_sdk.context import new_agent_context  # type: ignore
        from trpc_agent_sdk.runners import Runner  # type: ignore
        from trpc_agent_sdk.types import Content, Part  # type: ignore

        # 1. 按租户解析 Storage + 取共享服务 + 动态构建 Agent（PRD 0.3-4b）
        #    复用 agent.builder.build_framework_agent: 它按租户权限过滤并
        #    用框架 FunctionTool 包装工具。早先内联构造 LlmAgent 时漏了
        #    tools 参数，导致真实 LLM 也永远不会触发工具调用（阶段二
        #    P0「工具调用链路」无法闭环的根因）。model_factory 透传，
        #    保留测试注入假模型的能力。
        storage = await self._resolve_storage(tenant)
        session_service, memory_service = self._services_for(storage, tenant.tenant_id)
        app_name = f"{tenant.tenant_id}:{tenant.app.agent_type}"
        from ..agent.builder import build_framework_agent

        agent = build_framework_agent(
            tenant,
            model_factory=self._model_factory,
            knowledge=getattr(storage, "knowledge", None),
            confirmed_tools=confirmed_tools,
        )

        # 2. 交给框架 Runner 编排: 由其负责 session 加载/落库、memory 沉淀、
        #    telemetry 埋点（PRD 0.3-4c）
        runner = Runner(
            app_name=app_name,
            agent=agent,
            session_service=session_service,
            memory_service=memory_service,
            artifact_service=self._artifact_service,
            # Storage 生命周期由平台统一管理，不让 Runner 关闭共享后端
            close_session_service_on_close=False,
            close_memory_service_on_close=False,
        )

        # 3. 消费框架事件流 -> 归一化 RunnerEvent
        # save_history_enabled: 框架默认 False（用户消息不入历史），
        #   多轮会话必须开启，否则下一轮读不到上一轮。
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=Content(role="user", parts=[Part(text=new_message)]),
            run_config=RunConfig(save_history_enabled=True),
            agent_context=new_agent_context(timeout=self._timeout_ms),
        ):
            for runner_event in translate_event(event):
                yield runner_event
        yield RunnerEvent.done()


def translate_event(event: Any) -> list[RunnerEvent]:
    """把框架 Event 翻译成平台 RunnerEvent（纯函数，便于单测）。

    翻译规则:
    - 错误事件优先，转成 error 并终止本事件后续处理
    - 流式增量（partial）跳过: 收尾事件会带完整文本，避免重复累加
    - 文本 -> content，final 标记取自 `is_final_response()`（**必须调用**）
    - 工具调用 / 工具结果 -> tool_call / tool_result
    - usage_metadata -> content 事件携带 input/output token（PRD 4.2；
      实测 trpc-agent-py v1.1.19: agent 级 Event 直接暴露 usage_metadata）
    """
    error_code = getattr(event, "error_code", None)
    if error_code:
        message = getattr(event, "error_message", None) or error_code
        return [RunnerEvent.failure(f"{error_code}: {message}")]

    if getattr(event, "partial", False):
        return []

    out: list[RunnerEvent] = []
    in_tokens, out_tokens = read_event_usage(event)

    get_text = getattr(event, "get_text", None)
    text = get_text() if callable(get_text) else ""
    if text:
        is_final = event.is_final_response() if hasattr(event, "is_final_response") else False
        out.append(RunnerEvent.text(text, is_final=bool(is_final), input_tokens=in_tokens, output_tokens=out_tokens))

    # get_function_calls/responses 返回的是 pydantic 对象（FunctionCall /
    # FunctionResponse），不是 dict —— 不可直接 .get()（实测确认）
    calls = event.get_function_calls() if hasattr(event, "get_function_calls") else None
    for call in calls or []:
        out.append(RunnerEvent.tool_call(_field(call, "name") or "", _field(call, "args", "arguments") or {}))

    responses = event.get_function_responses() if hasattr(event, "get_function_responses") else None
    for resp in responses or []:
        out.append(RunnerEvent.tool_result(_field(resp, "name") or "", str(_field(resp, "response", "content") or "")))

    return out


def read_event_usage(event: Any) -> tuple[int, int]:
    """从框架事件读取 token 用量（纯函数，无 usage 时返回 0,0）。

    实测（trpc-agent-py v1.1.19）: agent 级 Event 直接暴露
    `usage_metadata`（GenerateContentResponseUsageMetadata），字段
    `prompt_token_count`（输入）/ `candidates_token_count`（输出）/
    `total_token_count`。LLM 层在构造事件时透传 response.usage_metadata
    （见 agents/core/_llm_processor.py），仅收尾（非 partial）事件携带。

    Args:
        event: 框架 Event（或任意带 usage_metadata 的对象）

    Returns:
        (input_tokens, output_tokens)：缺失/异常一律 0，不抛错。
    """
    try:
        usage = getattr(event, "usage_metadata", None)
        if usage is None:
            return 0, 0
        prompt = getattr(usage, "prompt_token_count", None) or 0
        candidates = getattr(usage, "candidates_token_count", None)
        if candidates is None:
            # 兼容只给 total 的供应商: output = total - prompt
            total = getattr(usage, "total_token_count", None) or 0
            candidates = max(total - prompt, 0)
        return int(prompt), int(candidates)
    except Exception:  # noqa: BLE001 - usage 读取失败不影响事件翻译
        return 0, 0


def _field(obj: Any, *names: str) -> Any:
    """从框架 pydantic 对象或 dict 中取值（兼容两种形态）。

    框架的 get_function_calls() 返回 FunctionCall 对象（字段 name / args），
    部分场景可能是 dict（字段 name / arguments），故按名字依次尝试。
    """
    for name in names:
        if isinstance(obj, dict):
            if name in obj:
                return obj[name]
        else:
            value = getattr(obj, name, None)
            if value is not None:
                return value
    return None


class MockAgentRunner(AgentRunner):
    """回声 Runner（本地自测 / 单测，无 LLM 依赖）。

    行为: 回复 = 系统提示词 + 用户消息 + 可选记忆上下文，
    并演示一次工具调用（metadata.tool_name 存在时）。
    """

    name = "mock"

    def __init__(self, reply_prefix: str = "（本地回声）") -> None:
        self._prefix = reply_prefix

    async def run(
            self,
            *,
            tenant: TenantConfig,
            user_id: str,
            session_id: str,
            new_message: str,
            memories: Optional[list[dict[str, Any]]] = None,
            session_state: Optional[dict[str, Any]] = None,
            confirmed_tools: frozenset[str] = frozenset(),
    ) -> AsyncIterator[RunnerEvent]:
        # 演示工具调用: 事件声明了 tool_name 且被租户允许
        tool_name = (session_state or {}).get("pending_tool")
        if tool_name:
            yield RunnerEvent.tool_call(tool_name, {})
            yield RunnerEvent.tool_result(tool_name, f"工具 {tool_name} 执行完成")

        reply_parts = [self._prefix]
        if memories:
            reply_parts.append("已参考记忆:" + ";".join(str(m.get("content", ""))[:30] for m in memories))
        reply_parts.append(new_message)
        yield RunnerEvent.text(" ".join(reply_parts), is_final=True)
        yield RunnerEvent.done()
