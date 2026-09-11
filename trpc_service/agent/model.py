"""Model wrappers used by the production tRPC-Agent integration."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from trpc_agent_sdk.models import LLMModel

from ..metrics.telemetry import span


class FailoverModel(LLMModel):
    """Use a secondary model only before the primary emits visible content."""

    def __init__(self, primary: LLMModel, fallback: LLMModel) -> None:
        super().__init__(model_name=f"{primary.name}|{fallback.name}")
        self.primary = primary
        self.fallback = fallback

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r".*"]

    async def _generate_async_impl(
        self,
        request: Any,
        stream: bool = False,
        ctx: Any | None = None,
    ) -> AsyncIterator[Any]:
        produced_content = False
        try:
            async for response in self.primary.generate_async(request, stream=stream, ctx=ctx):
                if response.has_content():
                    produced_content = True
                if response.error_code and not produced_content:
                    async for fallback_response in self._fallback(request, stream, ctx):
                        yield fallback_response
                    return
                yield response
        except Exception:
            if produced_content:
                raise
            async for fallback_response in self._fallback(request, stream, ctx):
                yield fallback_response

    async def _fallback(self, request: Any, stream: bool, ctx: Any | None) -> AsyncIterator[Any]:
        with span("model.fallback", primary=self.primary.name, fallback=self.fallback.name):
            async for response in self.fallback.generate_async(request, stream=stream, ctx=ctx):
                yield response
