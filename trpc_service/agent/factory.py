import asyncio
from dataclasses import dataclass
from typing import Any

from google.genai.types import GenerateContentConfig
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.memory import (
    InMemoryMemoryService,
    RedisMemoryService,
    SqlMemoryService,
)
from trpc_agent_sdk.models import AnthropicModel, OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import (
    InMemorySessionService,
    RedisSessionService,
    SqlSessionService,
)

from trpc_service.agent.runtime_config import (
    AgentRuntimeConfig,
    AgentRuntimeConfigRepository,
    RuntimeBackendConfig,
)
from trpc_service.config import SecretResolver
from trpc_service.domain import BackendKind
from trpc_service.governance import ToolGovernanceCallbacks
from trpc_service.metrics import PlatformMetrics
from trpc_service.storage.contracts import AuditStore
from trpc_service.tenant.errors import InvalidStateError


@dataclass(frozen=True, slots=True)
class AgentRuntime:
    tenant_id: str
    agent_app_id: str
    config_version: int
    agent: LlmAgent
    runner: Runner
    input_cost_per_million: float = 0
    output_cost_per_million: float = 0


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Any] = {}

    def register(self, name: str, tool: Any) -> None:
        if name in self._tools:
            raise ValueError(f"tool already registered: {name}")
        self._tools[name] = tool

    def resolve_allowed(self, names: tuple[str, ...]) -> list[Any]:
        missing = [name for name in names if name not in self._tools]
        if missing:
            raise InvalidStateError(f"allowed tools are not registered: {', '.join(missing)}")
        return [self._tools[name] for name in names]


class AgentFactory:
    """Build and cache tRPC-Agent runtimes from immutable active tenant config."""

    def __init__(
        self,
        config_repository: AgentRuntimeConfigRepository,
        secret_resolver: SecretResolver,
        tool_registry: ToolRegistry | None = None,
        audit_store: AuditStore | None = None,
        metrics: PlatformMetrics | None = None,
    ) -> None:
        self._configs = config_repository
        self._secrets = secret_resolver
        self._tools = tool_registry or ToolRegistry()
        self._audit = audit_store
        self._metrics = metrics
        self._cache: dict[tuple[str, str, int], AgentRuntime] = {}
        self._guard = asyncio.Lock()

    async def build(self, tenant_id: str, app_id: str) -> AgentRuntime:
        config = await self._configs.load_active(tenant_id, app_id)
        key = (tenant_id, app_id, config.version)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        async with self._guard:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
            runtime = await self._build_runtime(config)
            self._cache[key] = runtime
            await self._evict_old_versions(tenant_id, app_id, config.version)
            return runtime

    async def invalidate(self, tenant_id: str, app_id: str) -> None:
        for key in tuple(self._cache):
            if key[:2] == (tenant_id, app_id):
                runtime = self._cache.pop(key)
                await runtime.runner.close()

    async def _evict_old_versions(self, tenant_id: str, app_id: str, keep_version: int) -> None:
        for key in tuple(self._cache):
            if key[:2] == (tenant_id, app_id) and key[2] != keep_version:
                runtime = self._cache.pop(key)
                await runtime.runner.close()

    async def close(self) -> None:
        for runtime in tuple(self._cache.values()):
            await runtime.runner.close()
        self._cache.clear()

    async def _build_runtime(self, config: AgentRuntimeConfig) -> AgentRuntime:
        model = await self._build_model(config)
        generation_config = (
            GenerateContentConfig(**config.model.parameters) if config.model.parameters else None
        )
        agent_name = config.app_slug.replace("-", "_")
        tool_governance = ToolGovernanceCallbacks(config.tool_policies, self._audit, self._metrics)
        agent = LlmAgent(
            name=agent_name,
            description=config.description,
            instruction=config.instruction,
            model=model,
            tools=self._tools.resolve_allowed(config.allowed_tools),
            generate_content_config=generation_config,
            before_tool_callback=tool_governance.before_tool,
            after_tool_callback=tool_governance.after_tool,
        )
        session_service = await self._build_session_service(config.backends)
        memory_service = await self._build_memory_service(config.backends)
        runner = Runner(
            app_name=f"{config.tenant_id}:{config.agent_app_id}:v{config.version}",
            agent=agent,
            session_service=session_service,
            memory_service=memory_service,
        )
        pricing = dict(config.application_config.get("pricing", {}))
        return AgentRuntime(
            tenant_id=config.tenant_id,
            agent_app_id=config.agent_app_id,
            config_version=config.version,
            agent=agent,
            runner=runner,
            input_cost_per_million=max(0, float(pricing.get("input_per_million", 0))),
            output_cost_per_million=max(0, float(pricing.get("output_per_million", 0))),
        )

    async def _build_model(self, config: AgentRuntimeConfig) -> Any:
        model = config.model
        if model.api_key_secret_ref is None:
            raise InvalidStateError("model api_key_secret_ref is required")
        api_key = await self._secrets.resolve(model.api_key_secret_ref)
        provider = model.provider.lower()
        kwargs: dict[str, Any] = {"api_key": api_key}
        if model.base_url:
            kwargs["base_url"] = model.base_url
        if provider in {"openai", "openai-compatible"}:
            return OpenAIModel(model_name=model.model_name, **kwargs)
        if provider == "anthropic":
            return AnthropicModel(model_name=model.model_name, **kwargs)
        raise InvalidStateError(f"unsupported model provider: {model.provider}")

    @staticmethod
    def _find_backend(
        backends: tuple[RuntimeBackendConfig, ...], kind: BackendKind
    ) -> RuntimeBackendConfig | None:
        return next((item for item in backends if item.kind == kind), None)

    async def _backend_url(self, backend: RuntimeBackendConfig) -> str:
        if backend.secret_ref:
            return await self._secrets.resolve(backend.secret_ref)
        url = str(backend.options.get("url", ""))
        if url.startswith("sqlite"):
            return url
        raise InvalidStateError(
            f"{backend.kind.value} backend requires secret_ref; only SQLite may use an inline URL"
        )

    async def _build_session_service(self, backends: tuple[RuntimeBackendConfig, ...]) -> Any:
        backend = self._find_backend(backends, BackendKind.SESSION)
        if backend is None or backend.backend_type == "inmemory":
            return InMemorySessionService()
        url = await self._backend_url(backend)
        if backend.backend_type == "redis":
            return RedisSessionService(db_url=url, is_async=True)
        if backend.backend_type in {"sql", "sqlite", "postgresql", "mysql"}:
            return SqlSessionService(db_url=url, is_async=True)
        raise InvalidStateError(f"unsupported session backend: {backend.backend_type}")

    async def _build_memory_service(self, backends: tuple[RuntimeBackendConfig, ...]) -> Any | None:
        backend = self._find_backend(backends, BackendKind.MEMORY)
        if backend is None:
            return None
        if backend.backend_type == "inmemory":
            return InMemoryMemoryService(enabled=True)
        url = await self._backend_url(backend)
        if backend.backend_type == "redis":
            return RedisMemoryService(db_url=url, enabled=True, is_async=True)
        if backend.backend_type in {"sql", "sqlite", "postgresql", "mysql"}:
            return SqlMemoryService(db_url=url, enabled=True, is_async=True)
        raise InvalidStateError(f"unsupported memory backend: {backend.backend_type}")
