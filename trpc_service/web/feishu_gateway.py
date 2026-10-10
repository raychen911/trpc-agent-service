"""Fast encrypted callback gateway for Feishu application bots."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.envelopes import InboundEnvelope
from trpc_service.channels.feishu import FeishuCallback, FeishuVerificationError
from trpc_service.metrics.prometheus import CALLBACK_LATENCY, CALLBACKS
from trpc_service.metrics.telemetry import get_tracer, mark_span_error
from trpc_service.queue.emergency import EmergencyQueue
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import BindingRoute
from trpc_service.storage.protocols import RuntimeRepository
from trpc_service.tenant.models import Channel, ChannelBinding


class FeishuCallbackAdapter(Protocol):
    def verify_and_parse(
        self, request: WebhookRequest, binding: ChannelBinding
    ) -> FeishuCallback: ...


@dataclass(slots=True)
class _CachedRoute:
    route: BindingRoute
    expires_at: float


class FeishuGatewayService:
    def __init__(
        self,
        repository: RuntimeRepository,
        runtime: TenantRuntime,
        adapter: FeishuCallbackAdapter,
        *,
        emergency_queue: EmergencyQueue | None = None,
        binding_cache_seconds: int = 300,
        max_body_bytes: int = 2 * 1024 * 1024,
        allow_stale_binding_cache: bool = True,
    ) -> None:
        self._repository = repository
        self._runtime = runtime
        self._adapter = adapter
        self._emergency = emergency_queue
        self._cache_seconds = binding_cache_seconds
        self._max_body_bytes = max_body_bytes
        self._allow_stale_binding_cache = allow_stale_binding_cache
        self._routes: dict[str, _CachedRoute] = {}

    async def binding(self, binding_id: str) -> BindingRoute:
        try:
            route = await self._repository.resolve_binding(binding_id)
        except (asyncpg.PostgresError, TimeoutError, OSError, ConnectionError):
            cached = self._routes.get(binding_id)
            if (
                not self._allow_stale_binding_cache
                or cached is None
                or cached.expires_at <= time.monotonic()
                or not cached.route.binding.enabled
                or not cached.route.tenant_active
            ):
                raise
            return cached.route
        if (
            route is None
            or route.binding.channel != Channel.FEISHU
            or not route.binding.enabled
            or not route.tenant_active
        ):
            raise LookupError("binding not found")
        self._routes[binding_id] = _CachedRoute(route, time.monotonic() + self._cache_seconds)
        return route

    async def accept(self, route: BindingRoute, envelope: InboundEnvelope) -> None:
        prepared = self._runtime.prepare(route, envelope)
        try:
            await self._runtime.accept_prepared(prepared)
        except (asyncpg.PostgresError, TimeoutError, OSError, ConnectionError):
            if self._emergency is None:
                raise
            await self._emergency.enqueue(prepared)

    def parse(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        route: BindingRoute,
    ) -> FeishuCallback:
        return self._adapter.verify_and_parse(
            WebhookRequest(method="POST", headers=headers, body=body),
            route.binding,
        )


def create_feishu_gateway_router(service: FeishuGatewayService) -> APIRouter:
    router = APIRouter(prefix="/v1/channels/feishu", tags=["gateway"])

    @router.post("/{binding_id}/callback")
    async def callback(binding_id: str, request: Request) -> dict[str, object]:
        started = time.perf_counter()
        channel = Channel.FEISHU.value
        outcome = "error"
        tracer = get_tracer()
        with tracer.start_as_current_span("im.callback", attributes={"channel": channel}) as span:
            try:
                body = await _read_bounded_body(request, service._max_body_bytes)
                route = await service.binding(binding_id)
                verified = service.parse(
                    body=body,
                    headers=dict(request.headers),
                    route=route,
                )
                if verified.envelope is not None:
                    await service.accept(route, verified.envelope)
                outcome = "accepted"
                return verified.acknowledgement
            except (LookupError, FeishuVerificationError):
                outcome = "rejected"
                mark_span_error(span, "callback_verification_failed")
                raise HTTPException(
                    status_code=403, detail="callback verification failed"
                ) from None
            except (asyncpg.PostgresError, TimeoutError, OSError, ConnectionError):
                outcome = "unavailable"
                mark_span_error(span, "postgres_unavailable")
                raise HTTPException(
                    status_code=503, detail="durable acceptance unavailable"
                ) from None
            except HTTPException:
                raise
            except Exception as exc:
                mark_span_error(span, type(exc).__name__)
                raise
            finally:
                span.set_attribute("outcome", outcome)
                CALLBACKS.labels(channel=channel, outcome=outcome).inc()
                CALLBACK_LATENCY.labels(channel=channel).observe(time.perf_counter() - started)

    return router


async def _read_bounded_body(request: Request, max_bytes: int) -> bytes:
    """Read a callback incrementally so a false Content-Length cannot exhaust memory."""

    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(status_code=413, detail="callback body too large")
        except ValueError:
            raise HTTPException(status_code=413, detail="callback body too large") from None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > max_bytes:
            raise HTTPException(status_code=413, detail="callback body too large")
        chunks.append(chunk)
    return b"".join(chunks)


__all__ = ["FeishuGatewayService", "create_feishu_gateway_router"]
