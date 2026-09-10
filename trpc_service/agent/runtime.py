"""Tenant-specific tRPC model and runner construction."""

from __future__ import annotations

import asyncio
import re
from typing import Protocol

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import LLMModel, OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.config.models import AgentAppRecord
from trpc_service.config.secrets import SecretResolutionError, SecretResolver
from trpc_service.config.settings import AppEnvironment, ServiceSettings
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import TenantRepository
from trpc_service.storage.router import TenantStorageRouter
from trpc_service.tool.calculator import calculator


class ModelProvider(Protocol):
    def create(self, app: AgentAppRecord) -> LLMModel: ...


class RunnerProvider(Protocol):
    async def get_runner(self, app: AgentAppRecord) -> Runner: ...


class ModelFactory:
    """Create supported real models from tenant app configuration."""

    def __init__(self, settings: ServiceSettings, secrets: SecretResolver | None = None) -> None:
        self._settings = settings
        self._secrets = secrets or SecretResolver()

    def create(self, app: AgentAppRecord) -> LLMModel:
        config = app.model_config_data
        if "api_key" in config:
            raise ValueError("model configuration must store api_key_ref, not a literal api_key")

        provider = str(config.get("provider", self._settings.model_provider)).casefold()
        if provider != "openai":
            if provider in {"test", "fake", "mock"}:
                raise ValueError("test models must be injected by automated tests")
            raise ValueError(f"unsupported model provider: {provider}")

        model_name = str(
            config.get("model_name") or config.get("model") or self._settings.model_name
        )
        base_url = config.get("base_url", self._settings.model_base_url)
        api_key_ref = config.get("api_key_ref", self._settings.model_api_key_ref)
        if not isinstance(base_url, str) or not base_url.startswith("https://"):
            raise ValueError("OpenAI-compatible model base_url must be HTTPS")
        if not isinstance(api_key_ref, str):
            raise SecretResolutionError("model api_key_ref is required")

        api_key = self._secrets.resolve(api_key_ref)
        return OpenAIModel(model_name=model_name, api_key=api_key, base_url=base_url)


class TenantRunnerFactory:
    """Lazily build and cache a tRPC Runner for each tenant app version."""

    def __init__(
        self,
        settings: ServiceSettings,
        model_provider: ModelProvider | None = None,
        database: Database | None = None,
        secrets: SecretResolver | None = None,
        storage_router: TenantStorageRouter | None = None,
    ) -> None:
        self._settings = settings
        self._model_provider = model_provider or ModelFactory(settings, secrets)
        self._database = database
        self._storage_router = storage_router or (
            TenantStorageRouter(database, secrets) if database is not None else None
        )
        self._owns_storage_router = storage_router is None and self._storage_router is not None
        self._runners: dict[tuple[str, ...], Runner] = {}
        self._lock = asyncio.Lock()

    async def get_runner(self, app: AgentAppRecord) -> Runner:
        tenant = None
        storage_key = ("in-memory",)
        if self._settings.app_env != AppEnvironment.TEST:
            if self._database is None or self._storage_router is None:
                raise ValueError("non-test runner factory requires database-backed storage routing")
            tenant = await TenantRepository(self._database).get(app.tenant_id)
            if tenant is None:
                raise ValueError("tenant does not exist")
            storage_key = (
                tenant.storage_config.session_backend.value,
                tenant.storage_config.redis_url_ref or "",
            )
        key = (app.tenant_id, app.app_id, str(app.active_config_version), *storage_key)
        runner = self._runners.get(key)
        if runner is not None:
            return runner

        async with self._lock:
            runner = self._runners.get(key)
            if runner is not None:
                return runner
            model = self._model_provider.create(app)
            tools = []
            allowed_tools = set(app.tool_policy.get("allow", []))
            if {"calculator", "calculator.calculate"} & allowed_tools:
                tools.append(FunctionTool(calculator))

            agent = LlmAgent(
                name=_safe_agent_name(app.tenant_id, app.app_id),
                description=app.name,
                instruction=app.system_prompt,
                model=model,
                tools=tools,
            )
            if tenant is None:
                session_service = InMemorySessionService()
            else:
                assert self._storage_router is not None
                session_service = self._storage_router.session_service_for(tenant)
            runner = Runner(
                app_name=f"{app.tenant_id}:{app.app_id}",
                agent=agent,
                session_service=session_service,
            )
            self._runners[key] = runner
            return runner

    async def close(self) -> None:
        runners = list(self._runners.values())
        self._runners.clear()
        for runner in runners:
            await runner.close()
        if self._owns_storage_router and self._storage_router is not None:
            await self._storage_router.close()


def _safe_agent_name(tenant_id: str, app_id: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]", "_", f"{tenant_id}_{app_id}")
    return name[:128]


__all__ = ["ModelFactory", "ModelProvider", "RunnerProvider", "TenantRunnerFactory"]
