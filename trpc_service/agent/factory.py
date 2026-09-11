"""Framework integration seam for tenant-specific agents."""

from __future__ import annotations

import hashlib
import os

from ..config import resolve_secret
from ..storage import StorageRouter
from ..tenant.models import AgentApp, TenantConfig
from ..tool import ToolCatalog
from .model import FailoverModel
from .runner import AgentExecutor, EchoExecutor, TenantExecutorRouter, TRPCAgentExecutor


def create_executor(config: TenantConfig) -> AgentExecutor:
    """Return the deterministic executor used by explicit local demos."""
    _ = config
    return EchoExecutor()


def create_executor_from_env(config: TenantConfig) -> AgentExecutor:
    """Build a real tRPC-Agent executor when model variables are configured."""
    app = next(iter(config.apps.values()))
    api_key = resolve_secret(app.model_api_key_ref) if app.model_api_key_ref else os.getenv("TRPC_AGENT_API_KEY")
    model_name = app.model_name or os.getenv("TRPC_AGENT_MODEL_NAME")
    if not api_key or not model_name:
        return EchoExecutor()
    return _create_trpc_executor(config, app, api_key, model_name)


def create_executor_router_from_env(
    configs: list[TenantConfig],
    *,
    storage_router: StorageRouter | None = None,
    repository: object | None = None,
) -> AgentExecutor:
    """Create an isolated Runner for every configured tenant application."""
    api_key = os.getenv("TRPC_AGENT_API_KEY")
    model_name = os.getenv("TRPC_AGENT_MODEL_NAME")
    catalog = ToolCatalog.from_env()
    key = hashlib.sha256(os.getenv("TRPC_SERVICE_SESSION_HMAC_KEY", "development-only-change-me").encode()).digest()

    def build(config: TenantConfig, app: AgentApp) -> AgentExecutor:
        selected_key = resolve_secret(app.model_api_key_ref) if app.model_api_key_ref else api_key
        selected_model = app.model_name or model_name
        if not selected_key or not selected_model:
            return EchoExecutor()
        return _create_trpc_executor(
            config,
            app,
            selected_key,
            selected_model,
            storage_router=storage_router,
            repository=repository,
            catalog=catalog,
            tool_key=key,
        )

    executors = {
        (config.tenant_id, app.app_id, config.version): build(config, app)
        for config in configs
        for app in config.apps.values()
    }
    return TenantExecutorRouter(
        executors,
        factory=build,
    )


def _create_trpc_executor(
    config: TenantConfig,
    app: AgentApp,
    api_key: str,
    model_name: str,
    *,
    storage_router: StorageRouter | None = None,
    repository: object | None = None,
    catalog: ToolCatalog | None = None,
    tool_key: bytes | None = None,
) -> AgentExecutor:
    try:
        from trpc_agent_sdk.agents import LlmAgent
        from trpc_agent_sdk.models import OpenAIModel
        from trpc_agent_sdk.runners import Runner
        from trpc_agent_sdk.sessions import InMemorySessionService, RedisSessionService
    except ImportError as exc:
        raise RuntimeError("model variables are set, but trpc-agent-py is not installed") from exc

    model = OpenAIModel(
        model_name=model_name,
        api_key=api_key,
        base_url=app.model_base_url or os.getenv("TRPC_AGENT_BASE_URL", ""),
    )
    fallback_model_name = app.fallback_model_name or os.getenv("TRPC_AGENT_FALLBACK_MODEL_NAME")
    if fallback_model_name:
        fallback_api_key = (
            resolve_secret(app.fallback_api_key_ref)
            if app.fallback_api_key_ref
            else os.getenv("TRPC_AGENT_FALLBACK_API_KEY", api_key)
        )
        fallback = OpenAIModel(
            model_name=fallback_model_name,
            api_key=fallback_api_key,
            base_url=(
                app.fallback_base_url
                or os.getenv("TRPC_AGENT_FALLBACK_BASE_URL")
                or app.model_base_url
                or os.getenv("TRPC_AGENT_BASE_URL", "")
            ),
        )
        model = FailoverModel(model, fallback)
    tools = (catalog or ToolCatalog()).build(
        config,
        app,
        tool_key or hashlib.sha256(b"development-only-change-me").digest(),
        repository=repository,
        storage_router=storage_router,
    )
    agent = LlmAgent(
        name=app.name,
        description=f"Agent for tenant {config.tenant_id}",
        model=model,
        instruction=app.instruction,
        tools=tools,
    )
    redis_url = os.getenv("TRPC_AGENT_SESSION_REDIS_URL") or os.getenv("TRPC_SERVICE_REDIS_URL")
    session_backend = os.getenv("TRPC_AGENT_SESSION_BACKEND", "redis" if redis_url else "inmemory").lower()
    if session_backend == "redis":
        if not redis_url:
            raise ValueError("Redis tRPC session backend requires TRPC_AGENT_SESSION_REDIS_URL")
        session_service = RedisSessionService(db_url=redis_url, is_async=True)
    elif session_backend == "inmemory":
        session_service = InMemorySessionService()
    else:
        raise ValueError("TRPC_AGENT_SESSION_BACKEND must be redis or inmemory")
    runner = Runner(
        app_name=f"{config.tenant_id}:{app.app_id}",
        agent=agent,
        session_service=session_service,
    )
    artifact_store = storage_router.route(config).object_store if storage_router is not None else None
    return TRPCAgentExecutor(
        runner,
        artifact_store=artifact_store,
        timeout_seconds=(
            app.timeout_seconds
            if app.timeout_seconds is not None
            else max(1.0, float(os.getenv("TRPC_AGENT_RUN_TIMEOUT_SECONDS", "120")))
        ),
    )
