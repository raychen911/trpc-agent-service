"""Composition root for the process-local validation service."""

from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Callable, Mapping

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from trpc_service.audit.models import TenantScope
from trpc_service.channels.local_http import LocalHttpChannelAdapter
from trpc_service.config.settings import load_settings, load_runtime_settings
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.observability.health import (
    ROLE_CRITICAL_DEPENDENCIES,
    HealthMonitor,
)
from trpc_service.observability.service import DiagnosticQueryService, TelemetryRecorder
from trpc_service.storage.inmemory import EnvironmentSecretResolver, InMemoryPlatformAdapters
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.storage.locks import SessionLockManager
from trpc_service.worker.service import AgentExecutor
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_session import SharedSessionBackendFactory
from trpc_service.storage.shared import SharedPlatformAdapters
from trpc_service.storage.redis_leases import RedisSessionLeaseManager
from trpc_service.storage.redis_adapter_leases import RedisAdapterOwnershipRepository
from trpc_service.metrics.shared import SharedMetricsRecorder
from trpc_service.recovery.reconciler import RecoveryReconciler
from trpc_service.storage.contracts import AuditUnavailable, StateBackendUnavailable
import asyncio


def adapter_readiness_payload(state: object) -> dict[str, object]:
    """Render ownership state without echoing the channel identity digest."""

    return {
        "readiness": state.phase.value,
        "owner_node_id": state.owner_node_id,
        "generation": state.generation,
        "expires_in_ms": state.expires_in_ms,
    }


def _up_probes_for(role: str, *, telemetry_health: Callable[[], str] | None = None) -> dict[str, Callable[[], str]]:
    """Static up-probes for in-process dependencies of ``role``.

    The local profile runs every critical dependency in-process, so the
    probes report ``up``; telemetry is wired to the exporter health so an
    outage degrades (never un-readies) the role.
    """

    probes: dict[str, Callable[[], str]] = {
        dependency: (lambda: "up") for dependency in ROLE_CRITICAL_DEPENDENCIES.get(role, ())
    }
    if telemetry_health is not None:
        probes["telemetry"] = telemetry_health
    return probes


async def _health_status_access(runtime: "LocalRuntime | SharedRuntime", *, authorized: bool) -> int:
    """Minimal, pseudonymous audit of one /health/status access (FR-016)."""

    runtime.health_access_audit.append(
        {
            "action": "health_status_access",
            "authorized": authorized,
            "observed_at": runtime.now().isoformat(),
        }
    )
    return len(runtime.health_access_audit)


@dataclass(slots=True)
class LocalRuntime:
    adapters: InMemoryPlatformAdapters
    metrics: InMemoryMetricsRecorder
    secrets: EnvironmentSecretResolver
    worker: AgentExecutor
    gateway: GatewayService
    now: Callable[[], datetime]
    telemetry: TelemetryRecorder
    diagnostics: DiagnosticQueryService
    health: HealthMonitor
    ops_token: str | None
    health_access_audit: list = field(default_factory=list)

    def tenant_scope(self, tenant_id: str) -> TenantScope:
        return TenantScope(tenant_id=tenant_id)

    async def record_health_status_access(self, *, authorized: bool = True) -> int:
        return await _health_status_access(self, authorized=authorized)

    async def close(self) -> None:
        await self.worker.close()


def build_runtime(environ: Mapping[str, str], *, now: Callable[[], datetime] | None = None) -> LocalRuntime:
    clock = now or (lambda: datetime.now(timezone.utc))
    settings = load_settings(environ)
    adapters = InMemoryPlatformAdapters(settings)
    metrics = InMemoryMetricsRecorder()
    secrets = EnvironmentSecretResolver(environ)
    worker = AgentExecutor(SessionBackendFactory())
    telemetry = TelemetryRecorder()
    gateway = GatewayService(
        adapters, metrics, worker, SessionLockManager(),
        telemetry=telemetry, now=clock,
    )
    health = HealthMonitor(
        role="gateway",
        node_id="local-gateway",
        probes=_up_probes_for(
            "gateway",
            telemetry_health=lambda: {
                "ok": "up", "retrying": "degraded", "down": "down",
                "unused": "up",
            }.get(telemetry.exporter_health, "up"),
        ),
        clock=clock,
    )
    return LocalRuntime(
        adapters, metrics, secrets, worker, gateway, clock,
        telemetry, DiagnosticQueryService(telemetry),
        health, environ.get("TRPC_OPS_TOKEN") or None,
    )


def _phase8_health_routes(runtime_getter: Callable[[object], "LocalRuntime | SharedRuntime"]) -> list[Route]:
    """Vendor-neutral health endpoints (FR-012/FR-016, DEC-003)."""

    async def live(_request: object) -> JSONResponse:
        # Answering the probe is, by construction, proof of progress.
        return JSONResponse({"status": "alive"})

    async def ready(request: object) -> JSONResponse:
        runtime = runtime_getter(request)
        snapshot = await runtime.health.readiness()
        if snapshot.readiness == "ready":
            return JSONResponse(
                {
                    "status": "ready",
                    "role": snapshot.role,
                    "reason_codes": list(snapshot.reason_codes),
                }
            )
        return JSONResponse(
            {
                "status": "unready",
                "role": snapshot.role,
                "reason_codes": list(snapshot.reason_codes),
            },
            status_code=503,
        )

    async def status(request: object) -> JSONResponse:
        runtime = runtime_getter(request)
        token = request.headers.get("x-ops-token", "")
        if runtime.ops_token is None or token != runtime.ops_token:
            await runtime.record_health_status_access(authorized=False)
            return JSONResponse({"status": "forbidden"}, status_code=403)
        await runtime.record_health_status_access(authorized=True)
        platform = await runtime.health.platform()
        return JSONResponse(
            {
                "state": platform.state,
                "available_paths": list(platform.available_paths),
                "unavailable_paths": list(platform.unavailable_paths),
                "role_counts": platform.role_counts,
                "reason_codes": list(platform.reason_codes),
            }
        )

    return [
        Route("/health/live", live),
        Route("/health/ready", ready),
        Route("/health/status", status),
    ]


def create_app(environ: Mapping[str, str], *, now: Callable[[], datetime] | None = None) -> Starlette:
    runtime = build_runtime(environ, now=now)
    adapter = LocalHttpChannelAdapter(runtime)

    @asynccontextmanager
    async def lifespan(_app: Starlette):
        try:
            yield
        finally:
            await runtime.close()

    async def health(_request: object) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    app = Starlette(
        lifespan=lifespan,
        routes=[
            Route("/healthz", health),
            *_phase8_health_routes(lambda _request: runtime),
            Route("/v1/local/messages", adapter.handle, methods=["POST"]),
        ],
    )
    app.state.runtime = runtime
    return app


@dataclass(slots=True)
class SharedRuntime:
    adapters: SharedPlatformAdapters
    metrics: InMemoryMetricsRecorder
    secrets: EnvironmentSecretResolver
    worker: AgentExecutor
    gateway: GatewayService
    now: Callable[[], datetime]
    recovery: RecoveryReconciler
    recovery_task: asyncio.Task[None] | None = None
    telemetry: TelemetryRecorder | None = None
    diagnostics: DiagnosticQueryService | None = None
    health: HealthMonitor | None = None
    ops_token: str | None = None
    health_access_audit: list = field(default_factory=list)

    async def record_health_status_access(self, *, authorized: bool = True) -> int:
        return await _health_status_access(self, authorized=authorized)

    async def start_recovery(self) -> None:
        if self.recovery_task is None:
            self.recovery_task = asyncio.create_task(self._recovery_loop())

    async def _recovery_loop(self) -> None:
        while True:
            try:
                for tenant_id in ("tenant-alpha", "tenant-beta"):
                    await self.recovery.run_once(tenant_id)
            except (AuditUnavailable, StateBackendUnavailable):
                # A transient dependency outage must not kill the worker's
                # recovery capability. /readyz independently reports the
                # dependency state and the loop retries after the interval.
                pass
            await asyncio.sleep(0.5)

    async def close(self) -> None:
        if self.recovery_task is not None:
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                pass
        await self.worker.close()
        await self.adapters.close()


async def build_shared_runtime(environ: Mapping[str, str], *, now: Callable[[], datetime] | None = None) -> SharedRuntime:
    clock = now or (lambda: datetime.now(timezone.utc))
    settings = load_runtime_settings(environ)
    adapters = await SharedPlatformAdapters.create(settings, settings.node)
    metrics = SharedMetricsRecorder(node_id=settings.node.node_id)
    secrets = EnvironmentSecretResolver(environ)
    worker = AgentExecutor(SharedSessionBackendFactory(adapters.redis, require_fence=True))
    locks = RedisSessionLeaseManager(
        adapters.redis, node=settings.node, lease_ms=settings.lease.lease_ms,
        heartbeat_ms=settings.lease.heartbeat_ms, wait_ms=settings.lease.acquire_wait_ms,
        metrics=metrics,
    )
    adapters.audit.fence_validator = locks.validate_fence
    telemetry = TelemetryRecorder()
    gateway = GatewayService(adapters, metrics, worker, locks, telemetry=telemetry, now=clock)
    recovery = RecoveryReconciler(adapters.audit, adapters.idempotency)
    health = HealthMonitor(
        role="worker",
        node_id=settings.node.node_id,
        probes=_up_probes_for(
            "worker",
            telemetry_health=lambda: {
                "ok": "up", "retrying": "degraded", "down": "down",
                "unused": "up",
            }.get(telemetry.exporter_health, "up"),
        ),
        clock=clock,
    )
    return SharedRuntime(
        adapters, metrics, secrets, worker, gateway, clock, recovery,
        telemetry=telemetry, diagnostics=DiagnosticQueryService(telemetry),
        health=health, ops_token=environ.get("TRPC_OPS_TOKEN") or None,
    )


def create_shared_app(environ: Mapping[str, str] | None = None) -> Starlette:
    """Build one stateless Worker process over the configured shared backends."""

    source = dict(environ or {})
    # This composition root is explicitly for the shared backend. Requiring
    # every in-process caller to repeat the profile selector made otherwise
    # complete Redis/PostgreSQL settings silently load as LOCAL.
    source["TRPC_RUNTIME_PROFILE"] = "shared"

    @asynccontextmanager
    async def lifespan(app: Starlette):
        runtime = await build_shared_runtime(source)
        await runtime.start_recovery()
        app.state.runtime = runtime
        app.state.adapter = LocalHttpChannelAdapter(runtime)
        try:
            yield
        finally:
            await runtime.close()

    async def health(_request: object) -> JSONResponse:
        return JSONResponse({"status": "ok", "profile": "shared"})

    async def ready(request: object) -> JSONResponse:
        runtime = request.app.state.runtime
        available = await runtime.adapters.readiness() and (
            runtime.recovery_task is not None and not runtime.recovery_task.done()
        )
        return JSONResponse({"status": "ready" if available else "unavailable"}, status_code=200 if available else 503)

    async def messages(request: object) -> JSONResponse:
        return await request.app.state.adapter.handle(request)

    async def adapter_readiness(request: object) -> JSONResponse:
        identity_digest = request.query_params.get("identity_digest", "")
        if (
            len(identity_digest) != 64
            or any(character not in "0123456789abcdef" for character in identity_digest)
        ):
            return JSONResponse({"status": "invalid_request"}, status_code=400)
        try:
            state = await RedisAdapterOwnershipRepository(
                request.app.state.runtime.adapters.redis
            ).inspect(identity_digest)
        except StateBackendUnavailable:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse(adapter_readiness_payload(state))

    def shared_runtime(request: object) -> "SharedRuntime":
        runtime = request.app.state.runtime
        if runtime.health is None or runtime.ops_token is None:
            # Health wiring is created with the runtime in the lifespan; the
            # shared composition stores token from the original environ.
            runtime.ops_token = runtime.ops_token or source.get("TRPC_OPS_TOKEN") or None
        return runtime

    return Starlette(lifespan=lifespan, routes=[
        Route("/healthz", health), Route("/readyz", ready),
        *_phase8_health_routes(shared_runtime),
        Route("/v1/channels/readiness", adapter_readiness),
        Route("/v1/local/messages", messages, methods=["POST"]),
    ])
