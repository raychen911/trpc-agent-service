"""Command-line entry point for API and stateless Worker process roles."""

import asyncio
from copy import deepcopy
import logging
import logging.config
from pathlib import Path
import signal
import sys
import uvicorn
from uvicorn.config import LOGGING_CONFIG

from trpc_service.config import get_settings
from trpc_service.agent.nodes import PostgreSQLRuntimeNodeRegistry, RuntimeNodeHeartbeatService
from trpc_service.agent.scaling import (
    LocalWorkerCapacity,
    LocalWorkerProcessLauncher,
    KubernetesWorkerCapacity,
    PostgreSQLWorkerPoolStore,
    WorkerCapacity,
    WorkerPoolController,
)
from trpc_service.storage import build_engine, build_session_factory
from trpc_service.container import build_application_container


def _safe_log_config(settings=None) -> dict[str, object]:  # type: ignore[no-untyped-def]
    """Build one JSON logging configuration with optional local rotation."""

    config = deepcopy(LOGGING_CONFIG)
    formatters = config.get("formatters", {})
    if isinstance(formatters, dict):
        default_formatter = formatters.get("default")
        access_formatter = formatters.get("access")
        if isinstance(default_formatter, dict):
            default_formatter.clear()
            default_formatter["()"] = "trpc_service.log.JsonLogFormatter"
        if isinstance(access_formatter, dict):
            access_formatter.clear()
            access_formatter["()"] = "trpc_service.log.JsonLogFormatter"
    handler_name = "default"
    if settings is not None and settings.log_file is not None:
        settings.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers = config.get("handlers", {})
        if isinstance(handlers, dict):
            handlers["json_file"] = {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": str(settings.log_file),
                "maxBytes": settings.log_max_bytes,
                "backupCount": settings.log_backup_count,
                "encoding": "utf-8",
                "delay": True,
            }
            handler_name = "json_file"
    loggers = config.get("loggers", {})
    if isinstance(loggers, dict):
        for logger_config in loggers.values():
            if isinstance(logger_config, dict) and "handlers" in logger_config:
                logger_config["handlers"] = [handler_name]
    config["root"] = {
        "handlers": [handler_name],
        "level": settings.log_level.upper() if settings is not None else "INFO",
    }
    return config


async def _run_background_role() -> None:
    """Run Worker or Channel roles without opening an HTTP listener."""

    settings = get_settings()
    logging.config.dictConfig(_safe_log_config(settings))
    if settings.runtime_role == "worker" and settings.worker_concurrency < 1:
        raise ValueError("worker runtime role requires positive worker concurrency")
    if settings.runtime_role == "channel" and settings.worker_concurrency != 0:
        raise ValueError("channel runtime role must not run Agent Worker slots")
    engine = build_engine(settings)
    session_factory = build_session_factory(engine)
    container = build_application_container(
        settings=settings,
        session_factory=session_factory,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    try:
        await container.storage_composition.initialize()
        await container.start()
        await stop.wait()
    finally:
        try:
            await container.close()
        finally:
            try:
                await container.storage_composition.close()
            finally:
                await engine.dispose()


async def _run_supervisor_role() -> None:
    """Reconcile runtime Worker capacity to the administrator's desired count."""

    settings = get_settings()
    logging.config.dictConfig(_safe_log_config(settings))
    if settings.worker_concurrency != 0:
        raise ValueError("supervisor runtime role must not execute Agent tasks")
    engine = build_engine(settings)
    sessions = build_session_factory(engine)
    registry = PostgreSQLRuntimeNodeRegistry(
        sessions,
        stale_after_seconds=settings.node_stale_after_seconds,
    )
    capacity: WorkerCapacity
    if settings.worker_scaler_mode == "kubernetes":
        namespace = settings.kubernetes_namespace_file.read_text(encoding="utf-8").strip()
        capacity = KubernetesWorkerCapacity(
            namespace=namespace,
            deployment=settings.kubernetes_worker_deployment,
            api_url=settings.kubernetes_api_url,
            token_file=settings.kubernetes_token_file,
            ca_file=settings.kubernetes_ca_file,
        )
    else:
        executable = Path(sys.executable).with_name("trpc-agent-service")
        capacity = LocalWorkerCapacity(
            LocalWorkerProcessLauncher(
                executable=executable,
                run_dir=settings.worker_run_dir,
                log_dir=settings.worker_log_dir,
                concurrency_per_node=settings.worker_concurrency_per_node,
            ))
    controller = WorkerPoolController(
        PostgreSQLWorkerPoolStore(sessions),
        capacity,
        initial_desired_nodes=settings.local_worker_nodes,
        reconcile_interval_seconds=settings.worker_pool_reconcile_interval_seconds,
    )
    heartbeat = RuntimeNodeHeartbeatService(
        registry,
        node_id=settings.resolved_node_id,
        role="supervisor",
        worker_concurrency=0,
        heartbeat_interval_seconds=settings.node_heartbeat_interval_seconds,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    task = asyncio.create_task(controller.run(), name="worker-pool-controller")
    try:
        await heartbeat.start()
        await stop.wait()
        controller.stop_reconciling()
        await task
    finally:
        try:
            await controller.close()
        finally:
            try:
                await heartbeat.close()
            finally:
                await engine.dispose()


def main() -> None:
    """Start Uvicorn with settings resolved from environment and local files."""

    settings = get_settings()
    if settings.runtime_role == "supervisor":
        asyncio.run(_run_supervisor_role())
        return
    if settings.runtime_role in {"worker", "channel"}:
        asyncio.run(_run_background_role())
        return
    uvicorn.run(
        "trpc_service.web.app:create_default_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        log_config=_safe_log_config(settings),
    )


if __name__ == "__main__":
    main()
