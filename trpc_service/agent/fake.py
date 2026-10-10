"""Deterministic offline agent used by tests and local doctor checks."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, FunctionCall, Part


class DeterministicAgent(BaseAgent):
    response: str = "deterministic-response"
    delay_seconds: float = 0.0

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        yield Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            content=Content(parts=[Part(text=self.response)]),
        )


class DeterministicToolCallModel(LLMModel):
    """Offline model that requests one fixed tool call per invocation.

    The model has no mutable per-invocation state, so one cached agent can be
    used by concurrent workers.  The last request content distinguishes the
    first model turn from the follow-up after the tool response; historical
    tool responses do not affect a new user turn.
    """

    def __init__(self, tool_name: str, *, delay_seconds: float = 0.0) -> None:
        super().__init__("offline-fault-stage-model")
        self._tool_name = tool_name
        self._delay_seconds = delay_seconds

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r".*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        if self._delay_seconds > 0:
            # This delay is only supplied by the explicitly enabled
            # test-only deterministic loader.  It gives the fault acceptance
            # runner time to arm the TOOL marker after the turn/ledger row is
            # visible, without changing production model behavior.
            await asyncio.sleep(self._delay_seconds)
        last_content = request.contents[-1] if request.contents else None
        has_tool_response = bool(
            last_content
            and last_content.parts
            and any(part.function_response is not None for part in last_content.parts)
        )
        if has_tool_response:
            yield LlmResponse(
                content=Content(
                    role="model",
                    parts=[Part(text="offline deterministic response")],
                )
            )
            return
        yield LlmResponse(
            content=Content(
                role="model",
                parts=[
                    Part(
                        function_call=FunctionCall(
                            id="deterministic-fault-stage-call",
                            name=self._tool_name,
                            args={},
                        )
                    )
                ],
            )
        )


__all__ = ["DeterministicAgent", "DeterministicToolCallModel"]
