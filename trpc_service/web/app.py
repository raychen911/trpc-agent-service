# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Reference wiring that assembles the gateway + worker into a runnable app.

Run with::

    TRPC_SERVICE_TENANTS_CONFIG=tenants.yaml uvicorn \
        trpc_service.web.app:app --host 0.0.0.0 --port 8080

The webhook endpoint is ``POST /webhook/{tenant_id}/{channel}``. Set
``TRPC_SERVICE_MYSQL_URL`` for durable configuration/audit storage, and
``TRPC_SERVICE_REDIS_URL`` for shared sessions, queues, and node discovery.
"""

from __future__ import annotations

from functools import partial
from typing import Any
from typing import Callable
from typing import Optional
from pydantic import SecretStr

from trpc_agent_sdk.abc import SessionServiceABC
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.models import AnthropicModel
from trpc_agent_sdk.models import OpenAIModel

from trpc_service._utils import to_agent_name
from trpc_service.agent import RedisSessionLockManager
from trpc_service.agent import TenantWorker
from trpc_service.agent._fallback_model import FallbackLLMModel
from trpc_service.agent._queue import StreamQueue
from trpc_service.log import AuditLogger
from trpc_service.log import SqlAuditSink
from trpc_service.log import install_redacting_log_filter
from trpc_service.metrics._observability import configure_telemetry
from trpc_service.metrics._observability import shutdown_telemetry
from trpc_service.tenant import Tenant
from trpc_service.tenant import AppInfo
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import build_tenant_config_manager
from trpc_service.tenant import load_tenants
from trpc_service.tenant._persistence import mysql_sync_url
from trpc_service.tool import BudgetTracker
from trpc_service.tool import ChannelUserAuthorizationFilter
from trpc_service.tool import ConfirmationManager
from trpc_service.tool import ModelBudgetFilter
from trpc_service.tool import ModelPricing
from trpc_service.tool import RedisBudgetTracker
from trpc_service.tool import RedisConfirmationManager
from trpc_service.web.admin import create_admin_router
from trpc_service.web.gateway import ChannelRegistry
from trpc_service.web.gateway import build_idempotency_store
from trpc_service.web.gateway import build_rate_limiter
from trpc_service.web.gateway import create_gateway_app
from trpc_service.workspace import TenantStorageRouter
from trpc_service.config import DefaultModelPricing
from trpc_service.config import SecretResolver
from trpc_service.config import ServiceSettings
from trpc_service.config import resolve_secret
from trpc_service.config import ProductionTenantPreflight
from trpc_service.runtime import RedisNodeDirectory

_BUDGET_TRACKER: Optional[Any] = None
_CONFIRMATION_MANAGER: Optional[Any] = None


def create_agent(
    tenant: Tenant,
    *,
    settings: Optional[ServiceSettings] = None,
    secret_resolver: Optional[SecretResolver] = None,
) -> LlmAgent:
    """Build an :class:`LlmAgent` from the tenant's model configuration.

    The model API key is resolved from the tenant's ``api_key_ref`` at the
    adapter boundary and never stored as plaintext configuration.
    """
    settings = settings or ServiceSettings.from_env()
    secret_resolver = secret_resolver or SecretResolver(file_root=settings.secret_file_root)
    pricing_policy = DefaultModelPricing(
        deepseek_input_per_mtok=settings.deepseek_input_price_per_mtok,
        deepseek_output_per_mtok=settings.deepseek_output_price_per_mtok,
    )
    budget_tracker = create_budget_tracker(settings)
    budget_tracker.replace_tenant_pricing(
        tenant.tenant_id,
        {
            model_name: ModelPricing(
                input_per_mtok=pricing.input_per_mtok,
                output_per_mtok=pricing.output_per_mtok,
            )
            for model_name, pricing in pricing_policy.for_tenant(tenant).items()
        },
    )
    model_filter = ModelBudgetFilter(budget_tracker, tenant=tenant)
    retry_config = ModelRetryConfig(num_retries=tenant.model.retry)
    api_key_ref = tenant.model.api_key_ref or settings.model_api_key_ref
    api_key = secret_resolver.resolve(api_key_ref, tenant_id=tenant.tenant_id)

    def build_model(model_name: str):
        common = {
            "model_name": model_name,
            "api_key": api_key,
            "base_url": tenant.model.api_endpoint,
            "model_retry_config": retry_config,
        }
        provider = tenant.model.provider.lower().replace("_", "-")
        if provider in {"anthropic", "claude"}:
            return AnthropicModel(**common)
        if provider in {"openai", "openai-compatible", "deepseek"}:
            return OpenAIModel(timeout=tenant.model.timeout, **common)
        raise ValueError(f"unsupported tenant model provider: {tenant.model.provider}")

    primary = build_model(tenant.model.model_name)
    model = primary
    if tenant.model.fallback_model:
        fallback = build_model(tenant.model.fallback_model)
        model = FallbackLLMModel(primary, fallback, filters=[model_filter])
    else:
        primary.add_filters([model_filter])
    selected_app: Optional[AppInfo] = None
    if tenant.app_config.default_app_id is not None:
        selected_app = next(
            (item for item in tenant.app_config.app_list if item.app_id == tenant.app_config.default_app_id),
            None,
        )
    instruction = (selected_app.instruction
                   if selected_app and selected_app.instruction is not None else tenant.app_config.default_instruction)
    agent_identity = tenant.tenant_id
    if selected_app is not None:
        agent_identity = f"{tenant.tenant_id}_{selected_app.app_id}"
    return LlmAgent(
        name=to_agent_name(agent_identity),
        model=model,
        instruction=instruction or "You are a helpful assistant.",
        filters=[ChannelUserAuthorizationFilter(tenant=tenant)],
    )


_STORAGE_ROUTER: Optional[TenantStorageRouter] = None
_SESSION_LOCK_MANAGER: Optional[Any] = None


def create_budget_tracker(settings: Optional[ServiceSettings] = None):
    """Return a process-local tracker for demos or Redis tracker for many nodes."""
    global _BUDGET_TRACKER
    if _BUDGET_TRACKER is None:
        settings = settings or ServiceSettings.from_env()
        redis_url = ServiceSettings.reveal(settings.redis_url)
        _BUDGET_TRACKER = RedisBudgetTracker(redis_url=redis_url) if redis_url else BudgetTracker()
    return _BUDGET_TRACKER


def create_confirmation_manager(settings: Optional[ServiceSettings] = None):
    """Return a confirmation registry shared across workers when Redis is set."""
    global _CONFIRMATION_MANAGER
    if _CONFIRMATION_MANAGER is None:
        settings = settings or ServiceSettings.from_env()
        redis_url = ServiceSettings.reveal(settings.redis_url)
        _CONFIRMATION_MANAGER = (RedisConfirmationManager(redis_url=redis_url) if redis_url else ConfirmationManager())
    return _CONFIRMATION_MANAGER


def _storage_router(
    settings: ServiceSettings,
    secret_resolver: Optional[SecretResolver] = None,
) -> TenantStorageRouter:
    global _STORAGE_ROUTER
    if _STORAGE_ROUTER is None:
        _STORAGE_ROUTER = TenantStorageRouter(
            redis_url=ServiceSettings.reveal(settings.redis_url),
            mysql_url=ServiceSettings.reveal(settings.mysql_url),
            secret_resolver=secret_resolver,
        )
    return _STORAGE_ROUTER


def create_session_service(
    tenant: Optional[Tenant],
    *,
    settings: Optional[ServiceSettings] = None,
    secret_resolver: Optional[SecretResolver] = None,
) -> SessionServiceABC:
    """Return the shared session backend.

    The backend must be a single shared instance so sessions persist across
    turns (and workers). Tenant isolation is layered on top by
    :class:`TenantSessionService` (``app_name`` prefixing), so a single shared
    backend is safe. In multi-node deployments ``TRPC_SERVICE_REDIS_URL`` makes the backend
    shared across nodes (no sticky sessions required).
    """
    settings = settings or ServiceSettings.from_env()
    return _storage_router(settings, secret_resolver).session_service(tenant)


def create_memory_service(
    tenant: Tenant,
    *,
    settings: Optional[ServiceSettings] = None,
    secret_resolver: Optional[SecretResolver] = None,
):
    """Return the tenant-selected shared Memory backend."""
    settings = settings or ServiceSettings.from_env()
    return _storage_router(settings, secret_resolver).memory_service(tenant)


def create_session_lock_manager(settings: Optional[ServiceSettings] = None):
    """Use Redis session locks for multi-node mode and local locks otherwise."""
    global _SESSION_LOCK_MANAGER
    if _SESSION_LOCK_MANAGER is None:
        settings = settings or ServiceSettings.from_env()
        redis_url = ServiceSettings.reveal(settings.redis_url)
        if redis_url:
            _SESSION_LOCK_MANAGER = RedisSessionLockManager(redis_url=redis_url)
    return _SESSION_LOCK_MANAGER


def create_audit_logger(settings: Optional[ServiceSettings] = None) -> tuple[AuditLogger, Optional[SqlAuditSink]]:
    """Create the process-local logger backed by the shared MySQL audit table."""
    settings = settings or ServiceSettings.from_env()
    mysql_url = ServiceSettings.reveal(settings.mysql_url)
    sink = SqlAuditSink(mysql_sync_url(mysql_url), is_async=False) if mysql_url else None
    return AuditLogger(
        sink=sink,
        source=sink.query_entries if sink else None,
    ), sink


def build_app(
    manager: Optional[TenantConfigManager] = None,
    tenants_path: Optional[str] = None,
    agent_factory: Optional[Callable] = None,
    settings: Optional[ServiceSettings] = None,
) -> "create_gateway_app":
    """Assemble the tenant manager, worker and gateway into a FastAPI app.

    ``agent_factory`` defaults to :func:`create_agent` (LLM-backed) but can be
    overridden to inject a mock/test agent factory.
    """
    install_redacting_log_filter()
    configure_telemetry("trpc-agent-gateway")
    settings = settings or ServiceSettings.from_env()
    secret_resolver = SecretResolver(file_root=settings.secret_file_root)
    redis_url = resolve_secret(settings.redis_url, resolver=secret_resolver) or None
    mysql_url = resolve_secret(settings.mysql_url, resolver=secret_resolver) or None
    owns_manager = manager is None
    manager = manager or build_tenant_config_manager(
        mysql_url=mysql_url,
        redis_url=redis_url,
        encryption_key=resolve_secret(settings.tenant_config_encryption_key, resolver=secret_resolver) or None,
    )
    manager.add_preflight_check(ProductionTenantPreflight(settings))
    if tenants_path:
        for tenant in load_tenants(tenants_path):
            if manager.get(tenant.tenant_id) is None:
                manager.register(tenant, reason="bootstrap from TRPC_SERVICE_TENANTS_CONFIG")

    runtime_settings = settings.model_copy(
        update={
            "redis_url": SecretStr(redis_url) if redis_url else None,
            "mysql_url": SecretStr(mysql_url) if mysql_url else None,
        })
    audit_logger, audit_sink = create_audit_logger(runtime_settings)
    resolved_agent_factory = agent_factory or partial(
        create_agent,
        settings=runtime_settings,
        secret_resolver=secret_resolver,
    )
    session_factory = partial(create_session_service, settings=runtime_settings, secret_resolver=secret_resolver)
    memory_factory = partial(create_memory_service, settings=runtime_settings, secret_resolver=secret_resolver)
    worker = TenantWorker(
        manager=manager,
        agent_factory=resolved_agent_factory,
        session_service_factory=session_factory,
        memory_service_factory=memory_factory,
        audit_logger=audit_logger,
        confirmation_manager=create_confirmation_manager(runtime_settings),
        session_lock_manager=create_session_lock_manager(runtime_settings),
    )

    node_directory = RedisNodeDirectory(redis_url=redis_url) if redis_url else None
    queue = (StreamQueue(redis_url=redis_url, node_directory=node_directory)
             if redis_url and settings.queue_enabled else None)

    gateway = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=ChannelRegistry(secret_resolver=secret_resolver),
        idempotency_store=build_idempotency_store(redis_url),
        rate_limiter=build_rate_limiter(redis_url),
        queue=queue,
        owned_resources=[audit_sink, node_directory, manager if owns_manager else None],
        test_api_key=(ServiceSettings.reveal(settings.test_api_key) if settings.test_api_enabled else None),
    )
    gateway.include_router(
        create_admin_router(
            manager=manager,
            audit_logger=audit_logger,
            api_key=ServiceSettings.reveal(settings.admin_api_key),
        ))
    gateway.state.tenant_manager = manager
    gateway.state.audit_sink = audit_sink
    gateway.state.node_directory = node_directory
    gateway.router.add_event_handler("shutdown", shutdown_telemetry)
    return gateway


_PROCESS_SETTINGS = ServiceSettings.from_env()
app = build_app(tenants_path=_PROCESS_SETTINGS.tenants_config, settings=_PROCESS_SETTINGS)
