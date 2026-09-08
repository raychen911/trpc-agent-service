# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Reference wiring that assembles the gateway + worker into a runnable app.

Run with::

    TENANTS_CONFIG=tenants.yaml uvicorn \
        trpc_service.web.app:app --host 0.0.0.0 --port 8080

The webhook endpoint is ``POST /webhook/{tenant_id}/{channel}``. Set
``MYSQL_URL`` for durable tenant configuration and audit storage, and
``REDIS_URL`` for shared sessions, config caching and node invalidation.
"""

from __future__ import annotations

import os
from typing import Any
from typing import Callable
from typing import Optional

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
from trpc_service.web.gateway import create_gateway_app
from trpc_service.workspace import TenantStorageRouter

_BUDGET_TRACKER: Optional[Any] = None
_CONFIRMATION_MANAGER: Optional[Any] = None


def create_agent(tenant: Tenant) -> LlmAgent:
    """Build an :class:`LlmAgent` from the tenant's model configuration.

    The model API key is read from the tenant's ``api_key_env`` reference so
    it is never stored in tenant config; ``TRPC_AGENT_API_KEY`` is the default.
    """
    budget_tracker = create_budget_tracker()
    budget_tracker.replace_tenant_pricing(
        tenant.tenant_id,
        {
            model_name: ModelPricing(
                input_per_mtok=pricing.input_per_mtok,
                output_per_mtok=pricing.output_per_mtok,
            )
            for model_name, pricing in tenant.model.pricing.items()
        },
    )
    model_filter = ModelBudgetFilter(budget_tracker, tenant=tenant)
    retry_config = ModelRetryConfig(num_retries=tenant.model.retry)
    api_key = os.environ.get(tenant.model.api_key_env, "")

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


_STORAGE_ROUTER = TenantStorageRouter()
_SESSION_LOCK_MANAGER: Optional[Any] = None


def create_budget_tracker():
    """Return a process-local tracker for demos or Redis tracker for many nodes."""
    global _BUDGET_TRACKER
    if _BUDGET_TRACKER is None:
        redis_url = os.environ.get("REDIS_URL")
        _BUDGET_TRACKER = RedisBudgetTracker(redis_url=redis_url) if redis_url else BudgetTracker()
    return _BUDGET_TRACKER


def create_confirmation_manager():
    """Return a confirmation registry shared across workers when Redis is set."""
    global _CONFIRMATION_MANAGER
    if _CONFIRMATION_MANAGER is None:
        redis_url = os.environ.get("REDIS_URL")
        _CONFIRMATION_MANAGER = (RedisConfirmationManager(redis_url=redis_url) if redis_url else ConfirmationManager())
    return _CONFIRMATION_MANAGER


def create_session_service(tenant: Optional[Tenant]) -> SessionServiceABC:
    """Return the shared session backend.

    The backend must be a single shared instance so sessions persist across
    turns (and workers). Tenant isolation is layered on top by
    :class:`TenantSessionService` (``app_name`` prefixing), so a single shared
    backend is safe. In a multi-node deployment ``REDIS_URL`` makes the backend
    shared across nodes (no sticky sessions required).
    """
    return _STORAGE_ROUTER.session_service(tenant)


def create_memory_service(tenant: Tenant):
    """Return the tenant-selected shared Memory backend."""
    return _STORAGE_ROUTER.memory_service(tenant)


def create_session_lock_manager():
    """Use Redis session locks for multi-node mode and local locks otherwise."""
    global _SESSION_LOCK_MANAGER
    if _SESSION_LOCK_MANAGER is None:
        redis_url = os.environ.get("REDIS_URL")
        if redis_url:
            _SESSION_LOCK_MANAGER = RedisSessionLockManager(redis_url=redis_url)
    return _SESSION_LOCK_MANAGER


def create_audit_logger() -> tuple[AuditLogger, Optional[SqlAuditSink]]:
    """Create the process-local logger backed by the shared MySQL audit table."""
    mysql_url = os.environ.get("MYSQL_URL")
    sink = SqlAuditSink(mysql_sync_url(mysql_url), is_async=False) if mysql_url else None
    return AuditLogger(
        sink=sink,
        source=sink.query_entries if sink else None,
    ), sink


def build_app(manager: Optional[TenantConfigManager] = None,
              tenants_path: Optional[str] = None,
              agent_factory: Optional[Callable] = None) -> "create_gateway_app":
    """Assemble the tenant manager, worker and gateway into a FastAPI app.

    ``agent_factory`` defaults to :func:`create_agent` (LLM-backed) but can be
    overridden to inject a mock/test agent factory.
    """
    install_redacting_log_filter()
    configure_telemetry("trpc-agent-gateway")
    manager = manager or build_tenant_config_manager(
        mysql_url=os.environ.get("MYSQL_URL"),
        redis_url=os.environ.get("REDIS_URL"),
        encryption_key=os.environ.get("TENANT_CONFIG_ENCRYPTION_KEY"),
    )
    if tenants_path:
        for tenant in load_tenants(tenants_path):
            if manager.get(tenant.tenant_id) is None:
                manager.register(tenant, reason="bootstrap from TENANTS_CONFIG")

    audit_logger, audit_sink = create_audit_logger()
    worker = TenantWorker(
        manager=manager,
        agent_factory=agent_factory or create_agent,
        session_service_factory=create_session_service,
        memory_service_factory=create_memory_service,
        audit_logger=audit_logger,
        confirmation_manager=create_confirmation_manager(),
        session_lock_manager=create_session_lock_manager(),
    )

    redis_url = os.environ.get("REDIS_URL")
    queue_enabled = os.environ.get("AGENT_QUEUE_ENABLED", "1").lower() not in {"0", "false", "no"}
    queue = StreamQueue(redis_url=redis_url) if redis_url and queue_enabled else None

    gateway = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=ChannelRegistry(),
        idempotency_store=build_idempotency_store(redis_url),
        queue=queue,
    )
    gateway.include_router(
        create_admin_router(
            manager=manager,
            audit_logger=audit_logger,
            api_key=os.environ.get("ADMIN_API_KEY"),
        ))
    gateway.state.tenant_manager = manager
    gateway.state.audit_sink = audit_sink
    gateway.router.add_event_handler("shutdown", shutdown_telemetry)
    return gateway


app = build_app(tenants_path=os.environ.get("TENANTS_CONFIG"))
