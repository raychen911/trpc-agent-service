"""Build and run the standalone queued Agent worker process."""

from __future__ import annotations

import socket
from uuid import uuid4

from trpc_service.agent.execution import AgentExecutionService
from trpc_service.agent.runtime import TenantRunnerFactory
from trpc_service.config.secrets import SecretResolver
from trpc_service.config.settings import ExecutionBackend, ServiceSettings
from trpc_service.metrics import ServiceMetrics
from trpc_service.storage.database import Database
from trpc_service.storage.lock import RedisSessionLockManager
from trpc_service.storage.router import TenantStorageRouter
from trpc_service.telemetry import configure_telemetry
from trpc_service.worker.redis_runtime import RedisWorkerRuntime
from trpc_service.worker.service import WorkerService


async def run_worker(settings: ServiceSettings) -> None:
    if settings.execution_backend != ExecutionBackend.REDIS:
        raise RuntimeError("standalone worker requires execution_backend=redis")
    if settings.queue_redis_url_ref is None:
        raise RuntimeError("queue_redis_url_ref is required")

    database = Database(settings.database_url)
    secrets = SecretResolver()
    redis_url = secrets.resolve(settings.queue_redis_url_ref)
    storage_router = TenantStorageRouter(database, secrets, artifact_root=settings.artifact_root)
    runners = TenantRunnerFactory(
        settings,
        database=database,
        secrets=secrets,
        storage_router=storage_router,
    )
    lock = RedisSessionLockManager(
        redis_url,
        ttl_seconds=settings.agent_timeout_seconds + 30,
        acquire_timeout=min(30, settings.queue_result_timeout_seconds),
    )
    execution = AgentExecutionService(
        database,
        runners,
        storage_router=storage_router,
        metrics=ServiceMetrics(),
        timeout_seconds=settings.agent_timeout_seconds,
        distributed_lock=lock,
    )
    runtime = RedisWorkerRuntime(
        database,
        redis_url,
        WorkerService(execution),
        stream=settings.queue_stream,
        group=settings.queue_consumer_group,
        consumer=f"{socket.gethostname()}-{uuid4().hex[:8]}",
        claim_idle_ms=settings.queue_claim_idle_ms,
        max_attempts=settings.queue_max_attempts,
    )
    configure_telemetry("trpc-agent-worker", settings.otel_console_exporter)
    await database.initialize()
    try:
        await runtime.run()
    finally:
        await runtime.close()
        await lock.close()
        await runners.close()
        await storage_router.close()
        await database.dispose()


__all__ = ["run_worker"]
