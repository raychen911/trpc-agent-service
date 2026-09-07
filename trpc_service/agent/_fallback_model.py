# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""LLM wrapper that falls back only before any partial response is emitted."""

from __future__ import annotations

from typing import AsyncGenerator
from typing import List
from typing import Optional

from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import LlmRequest
from trpc_agent_sdk.models import LlmResponse


class FallbackLLMModel(LLMModel):
    """Try a secondary model when the primary fails before producing output."""

    def __init__(self, primary: LLMModel, fallback: LLMModel, filters: Optional[list] = None) -> None:
        super().__init__(model_name=primary.name, filters=filters or [])
        self.primary = primary
        self.fallback = fallback

    @classmethod
    def supported_models(cls) -> List[str]:
        return [r".*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        emitted = False
        try:
            async for response in self.primary.generate_async(request, stream=stream, ctx=ctx):
                if response.error_code or response.error_message:
                    if emitted:
                        yield response
                        return
                    break
                emitted = True
                yield response
            else:
                return
        except Exception:
            if emitted:
                raise
        async for response in self.fallback.generate_async(request, stream=stream, ctx=ctx):
            yield response
