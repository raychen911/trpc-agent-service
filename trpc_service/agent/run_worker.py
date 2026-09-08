# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Standalone worker process entrypoint.

Consumes tasks from Redis Streams (enqueued by the gateway) and runs them.
Run with::

    TRPC_SERVICE_REDIS_URL=redis://redis:6379 TRPC_SERVICE_TENANTS_CONFIG=tenants.yaml \
        python -m trpc_service.agent.run_worker
"""

from __future__ import annotations

import asyncio
import os
from functools import partial
from typing import Optional
from pydantic import SecretStr

os.environ.setdefault("OTEL_SERVICE_NAME", "trpc-agent-worker")

from trpc_service.web.gateway import ChannelRegistry
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import build_tenant_config_manager
from trpc_service.tenant import load_tenants
from trpc_service.agent import StreamWorker
from trpc_service.agent import StreamQueue
from trpc_service.agent import TenantWorker
from trpc_service.agent import RedisTaskResultStore

from trpc_service.web.app import create_agent
from trpc_service.web.app import create_audit_logger
from trpc_service.web.app import create_memory_service
from trpc_service.web.app import create_confirmation_manager
from trpc_service.web.app import create_session_lock_manager
from trpc_service.web.app import create_session_service
from trpc_service.log import install_redacting_log_filter
from trpc_service.metrics._observability import configure_telemetry
from trpc_service.metrics._observability import shutdown_telemetry
from trpc_service.config import SecretResolver
from trpc_service.config import ServiceSettings
from trpc_service.config import resolve_secret
from trpc_service.config import ProductionTenantPreflight
from trpc_service.messaging import SqlMessageStore
from trpc_service.runtime import RedisNodeDirectory


def build_stream_worker(manager: Optional[TenantConfigManager] = None,
                        tenants_path: Optional[str] = None,
                        settings: Optional[ServiceSettings] = None) -> StreamWorker:
    """Assemble the consumer-side worker for a standalone process."""
    install_redacting_log_filter()
    configure_telemetry("trpc-agent-worker")
    settings = settings or ServiceSettings.from_env()
    secret_resolver = SecretResolver(file_root=settings.secret_file_root)
    redis_url = resolve_secret(settings.redis_url, resolver=secret_resolver) or None
    mysql_url = resolve_secret(settings.mysql_url, resolver=secret_resolver) or None
    if not redis_url:
        raise ValueError("worker requires TRPC_SERVICE_REDIS_URL")
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

    runtime_settings = settings.model_copy(update={
        "redis_url": SecretStr(redis_url),
        "mysql_url": SecretStr(mysql_url) if mysql_url else None,
    })
    audit_logger, audit_sink = create_audit_logger(runtime_settings)
    worker = TenantWorker(
        manager=manager,
        agent_factory=partial(create_agent, settings=runtime_settings, secret_resolver=secret_resolver),
        session_service_factory=partial(
            create_session_service,
            settings=runtime_settings,
            secret_resolver=secret_resolver,
        ),
        memory_service_factory=partial(
            create_memory_service,
            settings=runtime_settings,
            secret_resolver=secret_resolver,
        ),
        audit_logger=audit_logger,
        confirmation_manager=create_confirmation_manager(runtime_settings),
        session_lock_manager=create_session_lock_manager(runtime_settings),
    )
    queue = StreamQueue(
        redis_url=redis_url,
        consumer=os.environ.get("TRPC_SERVICE_NODE_ID"),
        node_directory=RedisNodeDirectory(redis_url=redis_url),
    )
    result_store = RedisTaskResultStore(redis_url=redis_url)
    if settings.durable_delivery_enabled and not mysql_url:
        raise ValueError("durable delivery requires TRPC_SERVICE_MYSQL_URL")
    message_store = SqlMessageStore(mysql_url) if settings.durable_delivery_enabled else None
    stream_worker = StreamWorker(
        queue=queue,
        worker=worker,
        registry=ChannelRegistry(secret_resolver=secret_resolver),
        result_store=result_store,
        message_store=message_store,
    )
    # Keep the sink reachable for graceful shutdown hooks and diagnostics.
    stream_worker.audit_sink = audit_sink
    stream_worker.message_store = message_store
    return stream_worker


async def main() -> None:
    settings = ServiceSettings.from_env()
    worker = build_stream_worker(tenants_path=settings.tenants_config, settings=settings)
    try:
        await worker.run()
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    asyncio.run(main())
