# mypy: disable-error-code="import-untyped"
"""Deterministic framework test doubles and tenant fixtures."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

from trpc_agent_sdk.context import AgentContext, InvocationContext
from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part, SearchMemoryResponse

from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tenant.models import AgentAppSpec, ModelRoute, ToolPolicy


def echo(value: str) -> str:
    """Return the supplied value."""

    return value


class StreamingFakeModel(LLMModel):
    """Emit a delta and then the SDK's accumulated final response."""

    def __init__(self) -> None:
        super().__init__(model_name="fake-streaming")
        self.requests: list[LlmRequest] = []
        self.drained = False

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"fake-.*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del ctx
        self.validate_request(request)
        self.requests.append(request)
        if stream:
            yield LlmResponse(
                content=Content(role="model", parts=[Part.from_text(text="hel")]),
                partial=True,
            )
        yield LlmResponse(
            content=Content(role="model", parts=[Part.from_text(text="hello")]),
            partial=False,
        )
        self.drained = True


class BlockingFakeModel(LLMModel):
    """Wait forever so the integration's wall-clock timeout must cancel it."""

    def __init__(self) -> None:
        super().__init__(model_name="fake-blocking")
        self.cancelled = False

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"fake-.*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        self.validate_request(request)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if False:  # pragma: no cover - makes this an async generator by construction
            yield LlmResponse()


class ErrorFakeModel(LLMModel):
    """Return an SDK error response containing a message that must not leak."""

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"fake-.*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        self.validate_request(request)
        yield LlmResponse(
            error_code="provider_timeout",
            error_message="secret-provider-token-should-not-leak",
            partial=False,
        )


class EmptyFakeModel(LLMModel):
    """End with a non-user-visible event to exercise the fail-closed path."""

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"fake-.*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        self.validate_request(request)
        yield LlmResponse(content=Content(role="model", parts=[]), partial=False)


class RecordingMemoryService(BaseMemoryService):
    """Record post-turn calls while satisfying the real SDK memory contract."""

    def __init__(self) -> None:
        super().__init__(enabled=True)
        self.store_calls = 0
        self.search_calls = 0
        self.close_calls = 0

    async def store_session(
        self,
        session: Any,
        agent_context: AgentContext | None = None,
    ) -> None:
        del session, agent_context
        self.store_calls += 1

    async def search_memory(
        self,
        key: str,
        query: str,
        limit: int = 10,
        agent_context: AgentContext | None = None,
    ) -> SearchMemoryResponse:
        del key, query, limit, agent_context
        self.search_calls += 1
        return SearchMemoryResponse(memories=[])

    async def close(self) -> None:
        self.close_calls += 1


class RecordingSessionService(InMemorySessionService):
    """Verify a per-turn Runner does not own the shared session backend."""

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        await super().close()


def make_context(**overrides: Any) -> TenantContext:
    values: dict[str, Any] = {
        "tenant_id": "tenant-a",
        "app_id": "support",
        "app_revision": 3,
        "binding_id": "binding-a",
        "binding_revision": 2,
        "principal_id": "principal-a",
        "session_id": "session-a",
        "scope": ConversationScope.PRIVATE,
        "request_id": "request-a",
        "trace_id": "trace-a",
    }
    values.update(overrides)
    return TenantContext(**values)


def make_app(**overrides: Any) -> AgentAppSpec:
    values: dict[str, Any] = {
        "app_id": "support",
        "revision": 3,
        "name": "support_agent",
        "prompt": "Answer the verified user clearly.",
        "model": ModelRoute(
            provider="fake",
            model="fake-streaming",
            timeout_seconds=2,
            token_ceiling=512,
        ),
        "tools": ToolPolicy(
            allowed=frozenset({"echo"}),
            max_calls_per_turn=2,
        ),
    }
    values.update(overrides)
    return AgentAppSpec(**values)
