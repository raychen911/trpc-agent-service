"""Isolated per-tenant agent runtime."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import aclosing

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.artifacts import BaseArtifactService
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import (
    BaseSessionService,
    SessionServiceConfig,
    SummarizerSessionManager,
)
from trpc_agent_sdk.tools import BaseTool, preload_memory_tool
from trpc_agent_sdk.types import Content, FunctionResponse, Part

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.execution_coordinator import (
    SessionExecutionCoordinator,
    SessionExecutionIdentity,
    SessionLease,
)
from trpc_service.storage.state_backend import AgentStateBackend
from trpc_service.config.tenant import TenantConfig
from trpc_service.telemetry.runtime import SPAN_AGENT_TURN, TelemetryRuntime, safe_span
from trpc_service.telemetry.sdk_services import instrument_memory_service, instrument_session_service
from trpc_service.telemetry.tool import tracer_for
from trpc_service.tenant.context import TenantContext

logger = logging.getLogger(__name__)


class _PerRuntimeSummarizerSessionService(BaseSessionService):
    """Smallest per-runtime delegating wrapper that injects the SDK summarizer.

    Why a wrapper (R1B): the Worker's backend ``session_service`` is shared by
    every tenant runtime, so the summarizer cannot be set on it — a shared
    manager would use one tenant's model for all tenants and
    ``set_summarizer_manager`` on a shared object mutates state across
    runtimes.  This wrapper owns a fresh ``SummarizerSessionManager`` bound to
    ITSELF by the base constructor; the manager's persistence step therefore
    flows through this wrapper's ``update_session`` into the real backend,
    while the shared service is never mutated.

    ``store_historical_events`` is forced on the config view the wrapper
    exposes: a durable summary must never replace raw active events without
    the SDK first moving them into ``Session.historical_events``; the Redis and
    SQL backends persist that field (see ``state_backend`` module config).
    """

    def __init__(self, target: BaseSessionService, model: LLMModel) -> None:
        self._target = target
        config = getattr(target, "session_config", None)
        if isinstance(config, SessionServiceConfig):
            if not config.store_historical_events:
                config = config.model_copy(update={"store_historical_events": True})
        else:
            config = SessionServiceConfig(store_historical_events=True)
        super().__init__(summarizer_manager=SummarizerSessionManager(model=model), session_config=config)

    # Everything except the SDK summary entry points delegates unchanged so
    # Event/State persistence behaves exactly like the shared backend service.

    async def create_session(self, **kwargs):
        return await self._target.create_session(**kwargs)

    async def get_session(self, **kwargs):
        return await self._target.get_session(**kwargs)

    async def list_sessions(self, **kwargs):
        return await self._target.list_sessions(**kwargs)

    async def delete_session(self, **kwargs):
        return await self._target.delete_session(**kwargs)

    async def append_event(self, session, event):
        return await self._target.append_event(session, event)

    async def update_session(self, session):
        return await self._target.update_session(session)

    async def close(self):
        return await self._target.close()


class TenantAgentRuntime:
    """Owns one LlmAgent + Runner + shared state services for a tenant config."""

    def __init__(
        self,
        config: TenantConfig,
        model: LLMModel,
        tools: list[BaseTool],
        state_backend: AgentStateBackend,
        artifact_service: BaseArtifactService | None = None,
        coordinator: SessionExecutionCoordinator | None = None,
        telemetry: TelemetryRuntime | None = None,
    ) -> None:
        app_id = config.app.app_id
        if not app_id.isidentifier() or app_id == "user":
            raise TenantAgentConfigurationError()

        self._config = config
        self._state_backend = state_backend
        self._coordinator = coordinator
        # Stage 6B1: the REAL backend services stay unwrapped (close()/
        # check_ready() semantics unchanged); only what the Runner receives is
        # proxied, and only while tracing is enabled.
        self._tracer = tracer_for(telemetry, "trpc-service.agent")

        all_tools = list(tools) + [preload_memory_tool]

        self._agent = LlmAgent(
            name=app_id,
            description=config.app.instruction,
            model=model,
            instruction=config.app.instruction,
            tools=all_tools,
        )

        app_name = f"{config.tenant_id}:{app_id}:v{config.version}"

        session_service = state_backend.session_service
        # R1B: per-runtime SDK summarizer injection (durable Session Summary).
        # The shared backend service is never mutated; non-SDK service doubles
        # (test mocks) keep their exact identity.
        if isinstance(session_service, BaseSessionService):
            session_service = _PerRuntimeSummarizerSessionService(session_service, model=model)
        memory_service = state_backend.memory_service
        if self._tracer is not None:
            session_service = instrument_session_service(session_service, self._tracer)
            memory_service = instrument_memory_service(memory_service, self._tracer)

        self._runner = Runner(
            app_name=app_name,
            agent=self._agent,
            session_service=session_service,
            memory_service=memory_service,
            artifact_service=artifact_service,
            close_session_service_on_close=False,
            close_memory_service_on_close=False,
        )

        self._session_locks: dict[str, asyncio.Lock] = {}
        self._session_lock_waiters: dict[str, int] = {}
        self._session_locks_guard = asyncio.Lock()

    async def _acquire_session_lock(self, session_id: str) -> asyncio.Lock:
        async with self._session_locks_guard:
            if session_id not in self._session_locks:
                self._session_locks[session_id] = asyncio.Lock()
            self._session_lock_waiters[session_id] = self._session_lock_waiters.get(session_id, 0) + 1
            return self._session_locks[session_id]

    async def _release_session_lock(self, session_id: str) -> None:
        async with self._session_locks_guard:
            count = self._session_lock_waiters.get(session_id, 1) - 1
            if count <= 0:
                self._session_locks.pop(session_id, None)
                self._session_lock_waiters.pop(session_id, None)
            else:
                self._session_lock_waiters[session_id] = count

    def _check_identity(self, context: TenantContext) -> None:
        if context.tenant_id != self._config.tenant_id:
            raise TenantAgentConfigurationError()
        if context.app_id != self._config.app.app_id:
            raise TenantAgentConfigurationError()

    async def run(
        self,
        context: TenantContext,
        session_id: str,
        user_input: str,
    ) -> AsyncIterator[Event]:
        self._check_identity(context)
        message = Content(role="user", parts=[Part.from_text(text=user_input)])
        # aclosing propagates consumer cancellation/disconnect down the chain
        # so the agent.turn span (and the SDK generator) end immediately.
        async with aclosing(self._stream(context, session_id, message)) as stream:
            async for event in stream:
                yield event

    async def resume(
        self,
        context: TenantContext,
        session_id: str,
        *,
        function_call_id: str,
        tool_name: str,
        tool_result: dict | None = None,
        execute=None,
    ) -> AsyncIterator[Event]:
        """Resume a paused run by injecting the SDK-official FunctionResponse.

        Exactly one of ``tool_result`` / ``execute`` must be given.
        ``tool_result`` is the fixed rejected verdict (no execution involved).
        ``execute`` is an awaitable factory for the APPROVED tool execution:
        it is deliberately invoked INSIDE the per-session local lock and the
        Redis execution lease (P0-1), so the controlled execution and the
        resume turn are serialized with normal turns on the same session.
        """
        self._check_identity(context)
        if not isinstance(function_call_id, str) or not function_call_id.strip():
            raise TenantAgentConfigurationError()
        if not isinstance(tool_name, str) or not tool_name.strip():
            raise TenantAgentConfigurationError()
        if (tool_result is None) == (execute is None):
            raise TenantAgentConfigurationError()
        if tool_result is not None and not isinstance(tool_result, dict):
            raise TenantAgentConfigurationError()

        async def _build_message():
            result = dict(tool_result) if tool_result is not None else dict(await execute())
            response = FunctionResponse(
                id=function_call_id.strip(),
                name=tool_name.strip(),
                response=result,
            )
            return Content(role="user", parts=[Part(function_response=response)])

        if tool_result is not None:
            payload = await _build_message()
        else:
            # deferred: constructed only after lock+lease are held
            payload = _build_message
        async with aclosing(self._stream(context, session_id, payload)) as stream:
            async for event in stream:
                yield event

    async def _stream(
        self,
        context: TenantContext,
        session_id: str,
        message,
    ) -> AsyncIterator[Event]:
        lock = await self._acquire_session_lock(session_id)
        try:
            async with lock:
                if self._coordinator is not None:
                    identity = SessionExecutionIdentity(
                        tenant_id=context.tenant_id,
                        app_id=context.app_id,
                        config_version=self._config.version,
                        sdk_user_id=context.sdk_user_id,
                        session_id=session_id,
                    )
                    async with self._coordinator.acquire(identity) as lease:
                        logger.warning(
                            "session execution entered session_id=%s tenant=%s",
                            session_id,
                            context.tenant_id,
                        )
                        try:
                            resolved = await message() if callable(message) else message
                            async with aclosing(self._run_turn(context, session_id, resolved, lease)) as turn:
                                async for event in turn:
                                    yield event
                        finally:
                            logger.warning(
                                "session execution exited session_id=%s tenant=%s",
                                session_id,
                                context.tenant_id,
                            )
                else:
                    resolved = await message() if callable(message) else message
                    async with aclosing(self._run_turn(context, session_id, resolved, None)) as turn:
                        async for event in turn:
                            yield event
        finally:
            await self._release_session_lock(session_id)

    async def _run_turn(
        self,
        context: TenantContext,
        session_id: str,
        message: Content,
        lease: SessionLease | None,
    ) -> AsyncIterator[Event]:
        # The agent.turn span covers the Runner iteration itself.  It is a
        # plain context manager inside this async generator, so normal
        # completion, errors, client disconnects and cancellation (the
        # generator is aclosed by the consumer) all end the span via the
        # finally path; event order is untouched.
        with safe_span(self._tracer, SPAN_AGENT_TURN):
            gen = self._runner.run_async(
                user_id=context.sdk_user_id,
                session_id=session_id,
                new_message=message,
            )
            try:
                async for event in gen:
                    if lease is not None:
                        lease.ensure_valid()
                    yield event
            finally:
                await gen.aclose()

    async def close(self) -> None:
        await self._runner.close()


__all__ = ["TenantAgentRuntime"]
