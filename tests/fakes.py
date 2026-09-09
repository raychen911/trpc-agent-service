# ===================================================================
# tests.fakes - 测试替身（仅测试用，不进生产代码）
# ===================================================================
# 说明: 阶段二 Spec §3「验证策略」——无真实 API key 时，用假模型固定
#   输入输出，验证「Runtime → Runner → 事件流 → 平台存储」自家逻辑。
#   真实模型的 HTTP 层不在单测范围内（真 key 到位后补冒烟，不进 CI）。
# 规范: 本文件只被 tests/ 引用；生产代码不得 import，避免伪代码上线。
# ===================================================================

from __future__ import annotations

from typing import Any, AsyncGenerator, Optional

from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, Part

FAKE_MODEL_NAME = "fake-model"
"""假模型名（与真实 provider 不冲突，避免误当作真实调用）。"""


class FakeLLMModel(LLMModel):
    """固定输入输出的假模型，替换真实 LLM 的 HTTP 层。

    产出模式: 先 yield 一个 partial=True 的流式分片（用于验证
    is_final 判定），再 yield 一个 turn_complete=True 的收尾响应。
    回复内容 = 固定前缀 + 用户最后一条消息，便于断言链路贯通。
    """

    def __init__(self,
                 reply_prefix: str = "[fake]",
                 *,
                 fail_with: Optional[str] = None,
                 usage: Optional[tuple[int, int]] = None) -> None:
        super().__init__(model_name=FAKE_MODEL_NAME)
        self._prefix = reply_prefix
        self._fail_with = fail_with
        self._usage = usage
        """收尾响应的 token 用量 (input, output)；None 表示不携带 usage。"""
        self.calls: list[dict[str, Any]] = []
        """记录每次调用，供断言（如"第二轮能看到第一轮历史"）。"""

    @staticmethod
    def supported_models() -> list[str]:
        return [FAKE_MODEL_NAME]

    @classmethod
    def _with_usage(cls, response: LlmResponse, usage: Optional[tuple[int, int]]) -> LlmResponse:
        """给 LlmResponse 附 usage_metadata（实测 trpc-agent-py v1.1.19 支持）。"""
        if usage is None:
            return response
        from trpc_agent_sdk.types._usage import GenerateContentResponseUsageMetadata

        in_tokens, out_tokens = usage
        response.usage_metadata = GenerateContentResponseUsageMetadata(
            prompt_token_count=in_tokens,
            candidates_token_count=out_tokens,
            total_token_count=in_tokens + out_tokens,
        )
        return response

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: Optional[InvocationContext] = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        texts = self._history_texts(request)
        user_text = texts[-1] if texts else ""
        self.calls.append({"user_text": user_text, "texts": texts, "history_len": len(texts)})

        if self._fail_with:
            yield LlmResponse(error_code="fake_error", error_message=self._fail_with)
            return

        reply = f"{self._prefix} {user_text}"
        if stream:
            # 分片 1: 非 final（用于验证 is_final 不再恒真）
            yield LlmResponse(content=Content(role="model", parts=[Part(text=reply[:1])]), partial=True)
            # 分片 2: 收尾（携带 usage，模拟真实供应商在收尾 chunk 上报用量）
            yield self._with_usage(
                LlmResponse(content=Content(role="model", parts=[Part(text=reply[1:])]), turn_complete=True),
                self._usage,
            )
        else:
            yield self._with_usage(
                LlmResponse(content=Content(role="model", parts=[Part(text=reply)]), turn_complete=True),
                self._usage,
            )

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    @staticmethod
    def _texts(request: LlmRequest) -> list[str]:
        """从 LlmRequest 中取出全部文本（含历史）。"""
        out: list[str] = []
        for content in getattr(request, "contents", None) or []:
            for part in getattr(content, "parts", None) or []:
                text = getattr(part, "text", None)
                if text:
                    out.append(text)
        return out

    def _history_texts(self, request: LlmRequest) -> list[str]:
        return self._texts(request)

    def _last_user_text(self, request: LlmRequest) -> str:
        texts = self._texts(request)
        return texts[-1] if texts else ""
