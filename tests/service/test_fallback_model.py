# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Primary/fallback model behavior tests."""

from __future__ import annotations

from trpc_service import FallbackLLMModel
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import LlmRequest
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part


class StubModel(LLMModel):

    def __init__(self, name: str, responses=None, error: Exception = None) -> None:
        super().__init__(model_name=name)
        self.responses = responses or []
        self.error = error
        self.calls = 0

    @classmethod
    def supported_models(cls):
        return [r".*"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.calls += 1
        for response in self.responses:
            yield response
        if self.error is not None:
            raise self.error


async def test_fallback_runs_when_primary_fails_before_output():
    primary = StubModel("primary", error=TimeoutError("timeout"))
    fallback = StubModel("fallback", responses=[LlmResponse(error_message="fallback-result")])
    model = FallbackLLMModel(primary, fallback)

    results = [response async for response in model._generate_async_impl(LlmRequest())]
    assert results[0].error_message == "fallback-result"
    assert primary.calls == 1
    assert fallback.calls == 1


async def test_fallback_does_not_duplicate_after_partial_output():
    primary = StubModel(
        "primary",
        responses=[LlmResponse(content=Content(parts=[Part.from_text(text="partial")]), partial=True)],
        error=ConnectionError("stream broke"),
    )
    fallback = StubModel("fallback", responses=[LlmResponse(error_message="must-not-run")])
    model = FallbackLLMModel(primary, fallback)

    results = [response async for response in model._generate_async_impl(LlmRequest(), stream=True)]
    assert results[-1].error_message == "stream broke"
    assert fallback.calls == 0
