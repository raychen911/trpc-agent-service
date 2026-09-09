# ===================================================================
# agent.summarizer - LLM 会话摘要器（PRD 2.2: 异步生成摘要）
# ===================================================================
# 说明: 替代确定性拼接摘要（历史轮数 + 最近消息）。复用租户模型工厂
#   产出 LLMModel（与 AgentRunner 同源），用一次 generate_async 生成
#   整段会话摘要，不依赖 Runner 编排。
# 实测（trpc-agent-py v1.1.19）: 一次性生成入口是
#   `model.generate_async(LlmRequest(contents=[Content]))`，产出
#   LlmResponse 流；文本从 `response.content.parts[].text` 提取，
#   错误看 `response.error_code`（与 Event 流的错误判定一致）。
# 规范: 失败必须抛异常由调用方回落确定性摘要，绝不静默返回空串。
# ===================================================================

from __future__ import annotations

import asyncio
from typing import Any, Callable

from ..tenant.models import TenantConfig

MAX_MESSAGES = 20
"""送入摘要的历史消息条数上限（防 prompt 无界增长）。"""

MAX_MESSAGE_CHARS = 500
"""单条消息送入摘要的最大字符数（超长截断）。"""


class LlmSummarizer:
    """LLM 会话摘要器。

    模型实例按租户缓存（与 FrameworkAgentRunner._services_for 同策略），
    避免每轮摘要重复构造模型客户端。
    """

    def __init__(self, model_factory: Callable[..., Any], timeout_s: float = 30.0) -> None:
        """
        Args:
            model_factory: `(ModelConfig) -> LLMModel`（复用 agent.model_factory，
                测试注入 FakeLLMModel）。
            timeout_s: 单次摘要生成超时（秒），超时抛异常由调用方回落。
        """
        self._model_factory = model_factory
        self._timeout_s = timeout_s
        self._models: dict[str, Any] = {}

    def _model_for(self, tenant: TenantConfig) -> Any:
        # 缓存键含模型配置指纹（审查 09-04）：此前仅按 tenant_id 缓存，
        # 租户经 Admin 热更模型（provider/model_name）后旧实例不失效。
        # LLM 客户端本身昂贵，按配置指纹换键即可自然切换且无需失效通知。
        key = (f"{tenant.tenant_id}:{tenant.model.provider}:{tenant.model.model_name}:"
               f"{tenant.model.temperature}:{tenant.model.max_tokens}")
        cached = self._models.get(key)
        if cached is None:
            cached = self._model_factory(tenant.model)
            self._models[key] = cached
        return cached

    async def summarize(self, tenant: TenantConfig, messages: list[dict[str, str]]) -> str:
        """把一段对话压缩成摘要文本。

        Args:
            tenant: 租户配置（模型由此构建）。
            messages: 按序对话，元素形如 {"role": "user"|"assistant", "content": str}。

        Returns:
            摘要文本。

        Raises:
            RuntimeError: 模型返回错误事件或输出为空。
            asyncio.TimeoutError: 超过 timeout_s。
        """
        from trpc_agent_sdk.models import LlmRequest  # type: ignore
        from trpc_agent_sdk.types import Content, Part  # type: ignore

        prompt = self._build_prompt(messages)
        request = LlmRequest(contents=[Content(role="user", parts=[Part(text=prompt)])])
        model = self._model_for(tenant)

        texts: list[str] = []

        async def _collect() -> None:
            async for resp in model.generate_async(request):
                if resp.error_code:
                    raise RuntimeError(f"摘要模型调用失败: {resp.error_code}: {resp.error_message}")
                parts = resp.content.parts if resp.content else None
                for part in parts or []:
                    text = getattr(part, "text", None)
                    if text:
                        texts.append(text)

        await asyncio.wait_for(_collect(), timeout=self._timeout_s)
        summary = "".join(texts).strip()
        if not summary:
            raise RuntimeError("摘要模型输出为空")
        return summary

    @staticmethod
    def _build_prompt(messages: list[dict[str, str]]) -> str:
        lines = []
        for msg in messages[-MAX_MESSAGES:]:
            role = "用户" if msg.get("role") == "user" else "助手"
            lines.append(f"{role}: {str(msg.get('content', ''))[:MAX_MESSAGE_CHARS]}")
        dialog = "\n".join(lines)
        return ("请将以下对话压缩为一段简洁的中文摘要（200 字以内），"
                "概括用户意图与关键结论，直接输出摘要本身，不要任何前缀说明：\n\n" + dialog)
