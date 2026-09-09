# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Tenant runtime construction and version-aware caching."""

from __future__ import annotations

import asyncio
import re
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol
from typing import Any

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import AnthropicModel
from trpc_agent_sdk.models import LiteLLMModel
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.config import AgentAppConfig
from trpc_service.config import EnvironmentSecretResolver
from trpc_service.config import SecretResolver
from trpc_service.config import TenantConfig
from trpc_service.gateway.identity import sdk_app_name
from trpc_service.storage import StorageProviderFactory
from trpc_service.tenant import TenantRegistry
from trpc_service.tenant import TenantBoundaryAgentFilter
from trpc_service.tool import ToolRegistry
from trpc_service.agent.post_turn import TurnFinalizer, configure_summary, IncompleteRunError
from trpc_service.agent.model_observer import instrument_model_call_accounting
from trpc_service.storage.fencing import hold_storage_guards


def _agent_name(name: str) -> str:
    normalized = re.sub(r"\W", "_", name)
    if not normalized or normalized[0].isdigit():
        normalized = f"agent_{normalized}"
    return normalized


class RuntimeProvider(Protocol):
    """Worker-facing subset implemented by real and test runtime managers."""

    async def get(self, tenant_id: str, app_id: str, config_version: int | None = None) -> "TenantRuntime":
        """Return a runtime pinned to one immutable configuration version."""


@dataclass(slots=True)
class TenantRuntime:
    """SDK Runner and its concurrency limit for one Agent App version."""

    tenant: TenantConfig
    app: AgentAppConfig
    runner: Runner
    semaphore: asyncio.Semaphore
    active_runs: int = 0
    retiring: bool = False
    references: int = 0
    artifact_store: Any = None
    write_guards: dict = field(default_factory=dict)

    def execution_scope(self, key):
        return hold_storage_guards(self.write_guards, key)

    async def run(self,
                  *,
                  user_id: str,
                  session_id: str,
                  text: str,
                  agent_context: AgentContext,
                  attachments: list | None = None) -> AsyncIterator[Event]:
        parts = [Part.from_text(text=text)]
        for attachment in attachments or []:
            if self.artifact_store is None:
                raise ValueError("runtime attachment store is not configured")
            metadata, data = await self.artifact_store.get(self.tenant.tenant_id, attachment.attachment_id)
            if metadata.app_id != self.app.app_id:
                raise PermissionError("attachment app mismatch")
            if metadata.mime_type.startswith("text/"):
                parts.append(Part.from_text(text=data.decode("utf-8", errors="replace")))
            else:
                parts.append(Part.from_bytes(data=data, mime_type=metadata.mime_type))
        content = Content(parts=parts)
        async with self.semaphore:
            self.active_runs += 1
            try:
                async for event in self.runner.run_async(
                        user_id=user_id,
                        session_id=session_id,
                        new_message=content,
                        agent_context=agent_context,
                ):
                    yield event
                session = await self.runner.session_service.get_session(app_name=self.runner.app_name,
                                                                        user_id=user_id,
                                                                        session_id=session_id)
                request_id = agent_context.get_metadata("request_id")
                if request_id:
                    if session is None:
                        raise IncompleteRunError("session_missing_after_agent_run")
                    turn = await TurnFinalizer(self.runner, self.app.runtime).finish(session, request_id, agent_context)
                    if turn is None:
                        raise IncompleteRunError("final_event_missing_after_agent_run")
            finally:
                self.active_runs -= 1

    async def cancel(self, *, user_id: str, session_id: str) -> bool:
        """Request cooperative SDK cancellation for a run whose lease was lost."""
        return await self.runner.cancel_run_async(user_id=user_id, session_id=session_id)

    async def replay_result(self, *, request_id: str, user_id: str,
                            session_id: str) -> tuple[str, dict[str, int]] | None:
        """Recover a final persisted response after a crash before Outbox commit."""
        session = await self.runner.session_service.get_session(app_name=self.runner.app_name,
                                                                user_id=user_id,
                                                                session_id=session_id)
        if session is None:
            return None
        finalizer = TurnFinalizer(self.runner, self.app.runtime)
        await finalizer.recover_pending(session)
        turn = await finalizer.finish(session, request_id)
        if turn:
            return turn["text"], turn["usage"]
        matching = [event for event in session.events if event.request_id == request_id]
        if matching:
            raise IncompleteRunError("partial_session_turn_requires_review")
        return None

    async def close(self) -> None:
        await self.runner.close()
        for guard in self.write_guards.values():
            await guard.close()


class TenantRuntimeFactory:
    """Compose SDK Model, Agent, Runner, Session and Memory from a snapshot."""

    def __init__(self,
                 storage_factory: StorageProviderFactory | None = None,
                 secret_resolver: SecretResolver | None = None,
                 tool_registry: ToolRegistry | None = None,
                 artifact_store: Any = None,
                 migration_control: Any = None) -> None:
        self._artifact_store = artifact_store
        self._storage_factory = storage_factory or StorageProviderFactory()
        self._secret_resolver = secret_resolver or EnvironmentSecretResolver()
        self._tool_registry = tool_registry or ToolRegistry()
        self._migration_control = migration_control

    async def create(self, tenant: TenantConfig, app: AgentAppConfig) -> TenantRuntime:
        storage = self._storage_factory.create(tenant.storage)
        return await self._create_with_storage(tenant, app, storage)

    async def create_with_route(self, tenant: TenantConfig, app: AgentAppConfig, route: Any) -> TenantRuntime:

        async def dirty(kind: str, key: str, error: Exception) -> None:
            if self._migration_control and route.job_id:
                await self._migration_control.mark_dirty(route.job_id, kind, key, error)

        storage = self._storage_factory.create_migration_route(tenant.storage,
                                                               route.source_backend,
                                                               route.target_backend,
                                                               route.mode,
                                                               dirty,
                                                               shadow_sample_rate=route.shadow_sample_rate)
        return await self._create_with_storage(tenant, app, storage)

    async def _create_with_storage(self, tenant: TenantConfig, app: AgentAppConfig, storage) -> TenantRuntime:
        if not app.model.model_name:
            raise ValueError(f"model_name is required for {tenant.tenant_id}/{app.app_id}")
        api_key = app.model.api_key.get_secret_value() if app.model.api_key else ""
        if not api_key and app.model.api_key_ref:
            api_key = await self._secret_resolver.resolve(app.model.api_key_ref)
        model_args: dict[str, object] = {
            "api_key": api_key,
            "client_args": {
                "timeout": app.model.timeout_seconds
            },
            "model_retry_config": ModelRetryConfig(num_retries=app.model.max_retries),
        }
        if app.model.base_url:
            model_args["base_url"] = app.model.base_url
        model_classes = {
            "openai-compatible": OpenAIModel,
            "anthropic": AnthropicModel,
            "litellm": LiteLLMModel,
        }
        model = model_classes[app.model.provider](model_name=app.model.model_name, **model_args)
        instrument_model_call_accounting(model)
        agent = LlmAgent(
            name=_agent_name(app.agent_name),
            description=app.description,
            instruction=app.instruction,
            model=model,
            tools=self._tool_registry.resolve(app.tools),
            filters=[TenantBoundaryAgentFilter(tenant.tenant_id, app.app_id)],
        )
        runner = Runner(
            app_name=sdk_app_name(tenant.tenant_id, app.app_id),
            agent=agent,
            session_service=storage.session_service,
            memory_service=storage.memory_service,
            enable_post_turn_processing=False,
            defer_post_turn_processing=False,
        )
        configure_summary(runner, app.runtime, model)
        return TenantRuntime(
            tenant=tenant.model_copy(deep=True),
            app=app.model_copy(deep=True),
            runner=runner,
            semaphore=asyncio.Semaphore(app.runtime.max_concurrent_runs),
            artifact_store=self._artifact_store,
            write_guards=storage.write_guards,
        )

    async def validate(self, tenant: TenantConfig) -> None:
        """Fail publication before active pointer changes, without calling a model."""
        storage = self._storage_factory.create(tenant.storage)
        try:
            for kind, service in ((tenant.storage.session, storage.session_service), (tenant.storage.memory,
                                                                                      storage.memory_service)):
                if kind.value == "external" and not (getattr(service, "supports_fenced_writes", False)
                                                     and storage.write_guards):
                    raise ValueError("external provider must implement native fenced writes and write_guards")
            for app in tenant.apps.values():
                if not app.model.model_name:
                    raise ValueError(f"model_name is required for {tenant.tenant_id}/{app.app_id}")
                self._tool_registry.resolve(app.tools)
                if app.model.api_key_ref:
                    await self._secret_resolver.resolve(app.model.api_key_ref)
        finally:
            for service in (storage.session_service, storage.memory_service):
                close = getattr(service, "close", None)
                if close:
                    await close()
            for guard in storage.write_guards.values():
                await guard.close()


class TenantRuntimeManager:
    """Cache immutable runtimes by tenant, app and configuration version."""

    def __init__(self,
                 registry: TenantRegistry,
                 factory: TenantRuntimeFactory | None = None,
                 max_entries: int = 128,
                 migration_control: Any = None) -> None:
        self._registry = registry
        self._factory = factory or TenantRuntimeFactory()
        self._migration_control = migration_control
        self._runtimes: OrderedDict[tuple[str, str, int, int], TenantRuntime] = OrderedDict()
        self._max_entries = max(1, max_entries)
        self._lock = asyncio.Lock()

    async def get(self,
                  tenant_id: str,
                  app_id: str,
                  config_version: int | None = None,
                  *,
                  storage_route_version: int = 0,
                  _pin: bool = False) -> TenantRuntime:
        tenant = await self._registry.get(tenant_id, config_version)
        app = await self._registry.get_app(tenant_id, app_id, tenant.version)
        key = (tenant_id, app_id, tenant.version, storage_route_version)
        runtime = self._runtimes.get(key)
        if runtime:
            self._runtimes.move_to_end(key)
            if _pin:
                runtime.references += 1
            return runtime
        evicted: list[TenantRuntime] = []
        async with self._lock:
            runtime = self._runtimes.get(key)
            if runtime is None:
                route = (await self._migration_control.get_route(tenant_id, storage_route_version)
                         if self._migration_control and storage_route_version else None)
                runtime = (await self._factory.create_with_route(tenant, app, route)
                           if route else await self._factory.create(tenant, app))
                self._runtimes[key] = runtime
            self._runtimes.move_to_end(key)
            if _pin:
                runtime.references += 1
            while len(self._runtimes) > self._max_entries:
                candidate = next(((item_key, item) for item_key, item in self._runtimes.items()
                                  if item.active_runs == 0 and item.references == 0 and item_key != key), None)
                if candidate is None:
                    break
                self._runtimes.pop(candidate[0])
                evicted.append(candidate[1])
        for item in evicted:
            await item.close()
        return runtime

    @asynccontextmanager
    async def borrow(self, tenant_id: str, app_id: str, config_version: int | None = None):
        """Pin before a Worker waits on a Session lock or runtime semaphore."""
        runtime = await self.get(tenant_id, app_id, config_version, _pin=True)
        try:
            yield runtime
        finally:
            runtime.references -= 1

    @asynccontextmanager
    async def borrow_request(self, request):
        runtime = await self.get(request.tenant_id,
                                 request.app_id,
                                 request.config_version,
                                 storage_route_version=request.storage_route_version,
                                 _pin=True)
        try:
            yield runtime
        finally:
            runtime.references -= 1

    async def invalidate(self, tenant_id: str, *, keep_version: int | None = None) -> None:
        """Mark old snapshots retiring; pinned queued/in-flight requests remain valid."""
        async with self._lock:
            for key, runtime in self._runtimes.items():
                if key[0] == tenant_id and key[2] != keep_version:
                    runtime.retiring = True

    async def close(self) -> None:
        async with self._lock:
            runtimes = list(self._runtimes.values())
            self._runtimes.clear()
        for runtime in runtimes:
            await runtime.close()
