#!/usr/bin/env python3
"""Expose the authoritative SessionReady backlog for Kubernetes autoscaling.

Redis ``trpc:session-ready:v2`` contains reconstructable wake-up notices, so
stream length and consumer-group PEL size are not business backlog.  This
exporter reads the bounded PostgreSQL function created by migration 0016 and
publishes one namespace-scoped gauge for the HPA external-metrics provider.

The exporter deliberately stays live when PostgreSQL is unavailable.  Its
readiness and metrics endpoints return HTTP 503 until the authoritative query
succeeds; an outage is never represented as a zero backlog.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, cast

import asyncpg
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

METRICS_PORT = 9100
METRIC_NAME = "trpc_session_ready_backlog"
BACKLOG_QUERY = "SELECT public.count_session_ready_backlog()"
DATABASE_DSN_ENV = "TRPC_SERVICE_METRICS_DATABASE_DSN"
NAMESPACE_ENV = "TRPC_BACKLOG_NAMESPACE"
_NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExporterConfig:
    """Validated non-secret exporter settings."""

    database_dsn: str
    namespace: str


def _configuration() -> ExporterConfig:
    """Read and validate the two settings required by the exporter."""

    database_dsn = os.getenv(DATABASE_DSN_ENV, "").strip()
    namespace = os.getenv(NAMESPACE_ENV, "").strip().lower()
    if not database_dsn:
        raise ValueError(f"{DATABASE_DSN_ENV} is required")
    if len(namespace) > 63 or _NAMESPACE_RE.fullmatch(namespace) is None:
        raise ValueError(f"{NAMESPACE_ENV} must be a DNS label")
    # asyncpg accepts the PostgreSQL URI scheme, not SQLAlchemy's driver
    # suffix.  Keeping this normalization local avoids putting connection
    # details into any report or metric label.
    database_dsn = database_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    return ExporterConfig(database_dsn=database_dsn, namespace=namespace)


class BacklogExporter:
    """Small asyncpg-backed reader with fail-closed endpoint semantics."""

    def __init__(
        self,
        config: ExporterConfig | None,
        *,
        configuration_error: BaseException | None = None,
        pool: Any | None = None,
    ) -> None:
        self._config = config
        self._configuration_error = configuration_error
        self._pool = pool
        self._pool_lock = asyncio.Lock()

    @classmethod
    def from_environment(cls) -> BacklogExporter:
        """Build an exporter that remains probeable when config is invalid."""

        try:
            return cls(_configuration())
        except ValueError as error:
            # Keep the HTTP process alive so Kubernetes can observe a stable
            # not-ready state.  The exception text is never returned to a
            # caller, and a missing config cannot produce a zero metric.
            return cls(None, configuration_error=error)

    @property
    def namespace(self) -> str:
        if self._config is None:
            raise RuntimeError("backlog exporter configuration is unavailable")
        return self._config.namespace

    async def _pool_or_create(self) -> Any:
        if self._configuration_error is not None or self._config is None:
            raise RuntimeError("backlog exporter configuration is unavailable")
        if self._pool is not None:
            return self._pool
        async with self._pool_lock:
            if self._pool is None:
                self._pool = await asyncpg.create_pool(
                    self._config.database_dsn,
                    min_size=1,
                    max_size=2,
                    command_timeout=5,
                    server_settings={
                        "application_name": "trpc-session-ready-backlog-exporter",
                    },
                )
        return self._pool

    async def read_backlog(self) -> int:
        """Read one authoritative count; errors never become ``0``."""

        try:
            pool = await self._pool_or_create()
            async with pool.acquire() as connection:
                value = await connection.fetchval(BACKLOG_QUERY)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RuntimeError("backlog function returned an invalid count")
            return cast(int, value)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _LOGGER.warning(
                "session-ready backlog query unavailable",
                extra={"error_type": type(error).__name__},
            )
            raise RuntimeError("session-ready backlog is unavailable") from error

    async def close(self) -> None:
        """Close the pool, if it was successfully created."""

        pool = self._pool
        if pool is None:
            return
        close = getattr(pool, "close", None)
        if callable(close):
            await close()
        self._pool = None


def _unavailable() -> Response:
    """Return a content-free failure response without a stale gauge."""

    return Response(content="backlog_unavailable\n", status_code=503, media_type="text/plain")


def _metrics_response(value: int, namespace: str) -> Response:
    """Render exactly one bounded namespace-scoped Prometheus gauge."""

    registry = CollectorRegistry()
    gauge = Gauge(
        METRIC_NAME,
        "Number of executable SessionReady mailboxes waiting in PostgreSQL.",
        ("namespace",),
        registry=registry,
    )
    gauge.labels(namespace=namespace).set(value)
    return Response(
        content=generate_latest(registry),
        media_type=CONTENT_TYPE_LATEST,
    )


def create_app(exporter: BacklogExporter | None = None) -> FastAPI:
    """Create the metrics/health ASGI application."""

    state = exporter or BacklogExporter.from_environment()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await state.close()

    app = FastAPI(title="SessionReady backlog exporter", lifespan=lifespan)

    @app.get("/metrics")
    async def metrics() -> Response:
        try:
            value = await state.read_backlog()
        except RuntimeError:
            return _unavailable()
        return _metrics_response(value, state.namespace)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(response: Response) -> dict[str, str]:
        try:
            await state.read_backlog()
        except RuntimeError:
            response.status_code = 503
            return {"status": "unavailable"}
        return {"status": "ok"}

    return app


def main() -> None:
    """Run the exporter on its fixed in-cluster metrics port."""

    import uvicorn

    uvicorn.run(
        create_app(),
        host="0.0.0.0",  # noqa: S104 - the pod must be reachable by Prometheus
        port=METRICS_PORT,
        log_config=None,
    )


if __name__ == "__main__":
    main()
