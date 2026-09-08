# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Standalone worker process entrypoint.

Consumes tasks from Redis Streams (enqueued by the gateway) and runs them.
Run with::

    REDIS_URL=redis://redis:6379 TENANTS_CONFIG=tenants.yaml \
        python -m trpc_service.agent.run_worker
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional

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


def build_stream_worker(manager: Optional[TenantConfigManager] = None,
                        tenants_path: Optional[str] = None) -> StreamWorker:
    """Assemble the consumer-side worker for a standalone process."""
    install_redacting_log_filter()
    configure_telemetry("trpc-agent-worker")
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
        agent_factory=create_agent,
        session_service_factory=create_session_service,
        memory_service_factory=create_memory_service,
        audit_logger=audit_logger,
        confirmation_manager=create_confirmation_manager(),
        session_lock_manager=create_session_lock_manager(),
    )
    queue = StreamQueue(
        redis_url=os.environ.get("REDIS_URL"),
        consumer=os.environ.get("AGENT_WORKER_ID"),
    )
    result_store = RedisTaskResultStore(redis_url=os.environ.get("REDIS_URL"))
    stream_worker = StreamWorker(
        queue=queue,
        worker=worker,
        registry=ChannelRegistry(),
        result_store=result_store,
    )
    # Keep the sink reachable for graceful shutdown hooks and diagnostics.
    stream_worker.audit_sink = audit_sink
    return stream_worker


async def main() -> None:
    worker = build_stream_worker(tenants_path=os.environ.get("TENANTS_CONFIG"))
    try:
        await worker.run()
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    asyncio.run(main())
