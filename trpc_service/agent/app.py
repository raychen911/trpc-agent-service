"""Application-scoped tenant agent runtime manager."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import aclosing
from typing import TYPE_CHECKING

from trpc_agent_sdk.events import Event

from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.execution_coordinator import SessionExecutionCoordinator
from trpc_service.agent.model_provider import DefaultModelProvider
from trpc_service.agent.model_provider import ModelProvider
from trpc_service.agent.runtime import TenantAgentRuntime
from trpc_service.storage.state_backend import AgentStateBackend
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.config.tenant import TenantConfig
from trpc_service.telemetry.runtime import TelemetryRuntime
from trpc_service.tenant.context import TenantContext

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from trpc_service.storage.backend_capabilities import TenantBackendCapabilitiesResolver

RuntimeKey = tuple[str, str, int]


class AgentApp:
    """Manages cached tenant agent runtimes keyed by (tenant_id, app_id, version)."""

    def __init__(
        self,
        model_provider: ModelProvider,
        tool_registry: AllowedToolRegistry | None = None,
        state_backend: AgentStateBackend | None = None,
        coordinator: SessionExecutionCoordinator | None = None,
        telemetry: TelemetryRuntime | None = None,
        backend_resolver: TenantStateBackendResolver | None = None,
        backend_capabilities_resolver: TenantBackendCapabilitiesResolver | None = None,
    ) -> None:
        self._model_provider = model_provider
        self._tool_registry = tool_registry or AllowedToolRegistry.default(telemetry=telemetry)
        self._telemetry = telemetry
        # Two mutually exclusive ownership modes:
        # - state_backend: a fixed backend injected by callers/tests;
        # - backend_resolver: production R1A mode, per-tenant profile
        #   selection of the Worker's owned Redis/SQL backends.
        self._state_backend = state_backend
        self._backend_resolver = backend_resolver
        self._backend_capabilities_resolver = backend_capabilities_resolver
        self._coordinator = coordinator
        self._cache: dict[RuntimeKey, TenantAgentRuntime] = {}
        self._active_runs: dict[RuntimeKey, int] = {}
        self._retired: set[RuntimeKey] = set()
        self._max_versions: dict[str, int] = {}  # tenant_id -> max observed version
        self._closing = False
        self._close_task: asyncio.Task[None] | None = None

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        state_backend: AgentStateBackend | None = None,
        coordinator: SessionExecutionCoordinator | None = None,
        tool_registry: AllowedToolRegistry | None = None,
        telemetry: TelemetryRuntime | None = None,
        backend_resolver: TenantStateBackendResolver | None = None,
        backend_capabilities_resolver: TenantBackendCapabilitiesResolver | None = None,
    ) -> "AgentApp":
        """Build the AgentApp using the lazy DefaultModelProvider."""
        return cls(
            model_provider=DefaultModelProvider.from_env(environ),
            state_backend=state_backend,
            coordinator=coordinator,
            tool_registry=tool_registry,
            telemetry=telemetry,
            backend_resolver=backend_resolver,
            backend_capabilities_resolver=backend_capabilities_resolver,
        )

    async def run(
        self,
        config: TenantConfig,
        context: TenantContext,
        session_id: str,
        user_input: str,
    ) -> AsyncIterator[Event]:
        """Drive one conversation turn through the cached tenant runtime."""
        runtime = self._get_or_create_runtime(config, context)
        key: RuntimeKey = (context.tenant_id, config.app.app_id, config.version)
        # aclosing propagates consumer aclose()/errors through the whole
        # generator chain so inner per-turn spans/cleanup finish promptly on
        # cancellation and client disconnect (Stage 6B1 requirement).
        async with aclosing(self._tracked(key, runtime.run(context, session_id, user_input))) as stream:
            async for event in stream:
                yield event

    async def resume(
        self,
        config: TenantConfig,
        context: TenantContext,
        session_id: str,
        *,
        function_call_id: str,
        tool_name: str,
        tool_result: dict | None = None,
        execute=None,
    ) -> AsyncIterator[Event]:
        """Resume a paused approval through the runtime pinned to the SAME
        config version that created the approval (stale versions are rejected
        by the shared max-version/retirement guards).

        ``execute`` (approved tool execution) runs inside the runtime's
        per-session lock + Redis lease (P0-1)."""
        runtime = self._get_or_create_runtime(config, context)
        key: RuntimeKey = (context.tenant_id, config.app.app_id, config.version)
        inner = self._tracked(
            key,
            runtime.resume(
                context,
                session_id,
                function_call_id=function_call_id,
                tool_name=tool_name,
                tool_result=tool_result,
                execute=execute,
            ),
        )
        async with aclosing(inner) as stream:
            async for event in stream:
                yield event

    async def _tracked(self, key: RuntimeKey, agen) -> AsyncIterator[Event]:
        """Active-run bookkeeping + stale retirement shared by run()/resume()."""
        self._active_runs[key] = self._active_runs.get(key, 0) + 1
        stale_to_close = self._retire_stale_runtimes(key)
        try:
            for stale_key in stale_to_close:
                await self._close_key(stale_key)
            async with aclosing(agen) as stream:
                async for event in stream:
                    yield event
        finally:
            remaining = self._active_runs.get(key, 0) - 1
            if remaining <= 0:
                self._active_runs.pop(key, None)
            else:
                self._active_runs[key] = remaining
            self._max_versions[key[0]] = max(self._max_versions.get(key[0], 0), key[2])
            if key in self._retired and remaining <= 0:
                await self._close_key(key)

    def _retire_stale_runtimes(self, current_key: RuntimeKey) -> list[RuntimeKey]:
        """Mark same-tenant stale runtimes retired; return idle ones to close.

        Pure synchronous state mutation: safe because the event loop cannot
        interleave between the active-run increment above and this scan.
        """
        to_close: list[RuntimeKey] = []
        tenant_id = current_key[0]
        for key in list(self._cache.keys()):
            if key == current_key or key[0] != tenant_id or key in self._retired:
                continue
            self._retired.add(key)
            if self._active_runs.get(key, 0) == 0:
                to_close.append(key)
        return to_close

    async def _close_key(self, key: RuntimeKey) -> None:
        """Close one runtime exactly once and evict it from the cache.

        Uses cache.pop as the idempotency guard: if the key is not in the
        cache, it has already been closed.  After closing, remove the key
        from _retired so metadata stays bounded.
        """
        runtime = self._cache.pop(key, None)
        if runtime is None:
            self._retired.discard(key)
            return
        try:
            await runtime.close()
        except Exception as exc:
            logger.error("AgentApp runtime close failure: %s", type(exc).__name__)
        self._retired.discard(key)

    def _get_or_create_runtime(self, config: TenantConfig, context: TenantContext) -> TenantAgentRuntime:
        if self._closing:
            raise TenantAgentConfigurationError()

        if context.tenant_id != config.tenant_id or context.app_id != config.app.app_id:
            raise TenantAgentConfigurationError()

        key: RuntimeKey = (context.tenant_id, config.app.app_id, config.version)

        if key in self._retired:
            raise TenantAgentConfigurationError()

        max_version = self._max_versions.get(key[0], 0)
        if key[2] < max_version:
            raise TenantAgentConfigurationError()

        if key in self._cache:
            return self._cache[key]

        model = self._model_provider.get_model(config.app.model_profile)
        # Selection: an explicitly injected fixed backend (test path) wins;
        # production resolves the tenant's own state_backend — never the
        # other one (fail closed, no fallback).
        artifact_service = None
        knowledge_base = None
        if self._state_backend is not None:
            state_backend: AgentStateBackend | None = self._state_backend
        elif self._backend_capabilities_resolver is not None:
            services = self._backend_capabilities_resolver.resolve(config.tenant_id, config.backend_profile)
            # The bundle is SDK-typed Session/Memory, and its concrete state
            # backend remains selected by the existing resolver.
            state_backend = self._backend_capabilities_resolver.resolve_state_backend(config.backend_profile)
            artifact_service = services.artifact
            knowledge_base = services.knowledge
        elif self._backend_resolver is not None:
            state_backend = self._backend_resolver.resolve(config.backend_profile)
        else:
            state_backend = None
        tools = self._tool_registry.build_tools(
            config.app.allowed_tools,
            config.governance.tool_decisions,
            knowledge_base=knowledge_base,
        )
        runtime = TenantAgentRuntime(
            config=config,
            model=model,
            tools=tools,
            state_backend=state_backend,
            artifact_service=artifact_service,
            coordinator=self._coordinator,
            telemetry=self._telemetry,
        )

        self._cache[key] = runtime
        self._max_versions[key[0]] = max(max_version, key[2])
        return runtime

    async def close(self) -> None:
        """Close all cached runtimes exactly once. Safe to call repeatedly.

        Waits for all in-flight requests to complete before closing runtimes.
        Multiple concurrent close() calls will all wait for the same drain
        and close sequence to complete.
        """
        task = self._close_task
        if task is None:
            self._closing = True
            task = asyncio.create_task(self._close_impl())
            self._close_task = task

        # A completed Task can be observed from a later asyncio.run() loop,
        # while awaiting a pending Task from another loop would be invalid.
        if task.done():
            task.result()
            return

        # Caller cancellation must not abandon process-level cleanup.  A later
        # close() call can await the same task and observe its real result.
        await asyncio.shield(task)

    async def _close_impl(self) -> None:
        """Drain active runs and release owned resources once."""
        while any(count > 0 for count in self._active_runs.values()):
            await asyncio.sleep(0.1)

        failures: list[str] = []
        for key in list(self._cache.keys()):
            runtime = self._cache.pop(key, None)
            if runtime is None:
                continue
            try:
                await runtime.close()
            except Exception as exc:
                failures.append(f"{key}: {type(exc).__name__}")

        if failures:
            logger.error("AgentApp close failures: %s", "; ".join(failures))

        # Ownership: production closes the resolver (it owns BOTH backends
        # exactly once); the injected fixed backend is closed only when no
        # resolver was given.  Either way, exactly once.
        backend_owner = self._backend_capabilities_resolver or self._backend_resolver or self._state_backend
        if backend_owner is not None:
            try:
                await backend_owner.close()
            except Exception as exc:
                logger.error("AgentApp state backend close failure: %s", type(exc).__name__)

        if self._coordinator is not None:
            try:
                await self._coordinator.close()
            except Exception as exc:
                logger.error("AgentApp coordinator close failure: %s", type(exc).__name__)


__all__ = ["AgentApp", "RuntimeKey"]
