# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""FastAPI gateway: tenant resolution, signature verification, routing.

Routes inbound IM webhooks of the form ``POST /webhook/{tenant_id}/{channel}``
to the correct tenant worker. Per-tenant channel adapters are built lazily from
the tenant's :class:`ChannelConfig` and cached.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from contextlib import suppress
from typing import Any
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .._dispatch import run_and_reply
from ..testing import create_test_message_router
from trpc_service.agent._queue import TaskMessage
from trpc_service.metrics._observability import callback_span
from trpc_service.metrics._observability import inject_trace_headers
from trpc_service.metrics._observability import operation_span
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.log import safe_error_message
from trpc_service.runtime import RuntimeResources
from trpc_service.tenant import TenantConfigManager
from trpc_service.agent import TenantWorker
from ._idempotency import LocalIdempotencyStore
from ._idempotency import build_idempotency_store
from ._rate_limit import LocalRateLimiter
from ._rate_limit import build_rate_limiter
from ._registry import ChannelRegistry

logger = logging.getLogger(__name__)


def create_gateway_app(
    *,
    manager: TenantConfigManager,
    worker: TenantWorker,
    registry: Optional[ChannelRegistry] = None,
    idempotency_store: Optional[LocalIdempotencyStore] = None,
    rate_limiter: Optional[LocalRateLimiter] = None,
    async_dispatch: bool = True,
    queue: Any = None,
    metrics: Optional[EnterpriseMetrics] = None,
    test_api_key: Optional[str] = None,
    owned_resources: Optional[list[Any]] = None,
) -> FastAPI:
    """Build the gateway FastAPI application.

    ``async_dispatch=True`` (default) returns ``200`` immediately and processes
    the message in the background, satisfying platforms that require a fast
    callback ACK (e.g. WeCom's ~5s). Set it to ``False`` for tests so dispatch
    completes before the response is returned.

    When ``queue`` (a :class:`StreamQueue`) is provided, the gateway enqueues
    the task for a separate worker process instead of dispatching in-process.
    """
    registry = registry or ChannelRegistry()
    manager.subscribe(lambda tenant_id, _tenant: registry.invalidate(tenant_id))
    store = idempotency_store or build_idempotency_store()
    limiter = rate_limiter or build_rate_limiter()
    metrics = metrics or getattr(worker, "metrics", None) or get_enterprise_metrics()
    background_tasks: set[asyncio.Task] = set()
    resources = RuntimeResources(*(owned_resources or []), queue, registry, limiter, store)

    app = FastAPI(title="tRPC-Agent Enterprise Gateway")

    async def _worker_status() -> tuple[bool, str]:
        if queue is None:
            metrics.set_gauge("agent_worker_available", 1, mode="in_process")
            return True, "in_process"
        checker = getattr(queue, "has_active_workers", None)
        if not callable(checker):
            # Backwards compatibility for custom queue implementations. The
            # built-in Redis StreamQueue always exposes the liveness check.
            return True, "unsupported"
        try:
            available = bool(await checker())
        except Exception:  # noqa: BLE001 - readiness must not expose Redis details
            metrics.set_gauge("agent_worker_available", 0, mode="queue")
            logger.exception("failed to check queue Worker availability")
            return False, "worker_healthcheck_failed"
        metrics.set_gauge("agent_worker_available", int(available), mode="queue")
        return available, "ready" if available else "worker_unavailable"

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        available, reason = await _worker_status()
        status_code = 200 if available else 503
        return JSONResponse({
            "status": "ready" if available else "not_ready",
            "reason": reason
        },
                            status_code=status_code)

    async def _handle_webhook(tenant_id: str, channel: str, request: Request) -> tuple[JSONResponse, str]:
        tenant = worker.resolve_tenant(tenant_id)
        if tenant is None:
            return JSONResponse({"error": "tenant not found or disabled"}, status_code=404), "tenant_not_found"

        adapter = registry.get(tenant, channel)
        if adapter is None:
            return JSONResponse({"error": f"channel '{channel}' not configured"}, status_code=404), "channel_not_found"

        body = await request.body()
        content_type = request.headers.get("content-type", "")
        if "json" in content_type:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = body
        else:
            payload = body

        query = dict(request.query_params)
        headers = dict(request.headers)

        try:
            with operation_span("channel.challenge", **{"tenant.id": tenant_id, "channel": channel}):
                challenge = await adapter.challenge_response(payload)
        except Exception as exc:  # noqa: BLE001 - malformed challenge is a client error
            return JSONResponse({"error": f"challenge failed: {safe_error_message(exc)}"},
                                status_code=400), "challenge_failed"
        if challenge is not None:
            return JSONResponse(challenge, status_code=200), "challenge"

        with operation_span("channel.verify", **{"tenant.id": tenant_id, "channel": channel}):
            verified = await adapter.verify_request(body, payload, headers, query)
        if not verified:
            return JSONResponse({"error": "signature verification failed"}, status_code=401), "signature_failed"

        try:
            with operation_span("channel.parse", **{"tenant.id": tenant_id, "channel": channel}):
                inbound = await adapter.parse_message(payload)
        except Exception as exc:  # noqa: BLE001 - malformed payloads are client errors
            return JSONResponse({"error": f"parse failed: {safe_error_message(exc)}"}, status_code=400), "parse_failed"

        if not inbound.message_id.strip():
            # Some platform event types omit a provider message id. Hashing the
            # exact callback body preserves retry de-duplication without making
            # every id-less callback collide on one empty key.
            inbound.message_id = f"callback-{hashlib.sha256(body).hexdigest()}"
            inbound.metadata = {
                **inbound.metadata,
                "message_id_synthesized": True,
            }

        binding = tenant.channel_configs[channel]
        inbound.metadata = {
            **inbound.metadata,
            "channel_binding_id": channel,
        }
        if binding.agent_app_id is not None:
            inbound.metadata["agent_app_id"] = binding.agent_app_id

        workers_available, worker_reason = await _worker_status()
        if not workers_available:
            metrics.increment(
                "agent_worker_unavailable_total",
                tenant_id=tenant_id,
                channel=channel,
                reason=worker_reason,
            )
            return JSONResponse({"error": worker_reason}, status_code=503), worker_reason

        dedup_key = f"{tenant_id}:{channel}:{inbound.message_id}"
        with operation_span("idempotency.check", **{"tenant.id": tenant_id, "channel": channel}):
            duplicate = await store.check_and_set(dedup_key)
        if duplicate:
            return JSONResponse(adapter.callback_response(duplicate=True), status_code=200), "duplicate"

        rate_limit = tenant.im_access_policy.callback_requests_per_minute
        if rate_limit is not None:
            try:
                with operation_span("rate_limit.check", **{"tenant.id": tenant_id, "channel": channel}):
                    allowed = await limiter.allow(f"{tenant_id}:{channel}", rate_limit)
            except Exception:  # noqa: BLE001 - fail closed when shared governance is unavailable
                await store.release(dedup_key)
                logger.exception("rate-limit backend failed for tenant=%s channel=%s", tenant_id, channel)
                return JSONResponse({"error": "rate_limit_backend_unavailable"},
                                    status_code=503), "rate_limit_backend_unavailable"
            if not allowed:
                await store.release(dedup_key)
                metrics.increment("agent_callback_rate_limited_total", tenant_id=tenant_id, channel=channel)
                return JSONResponse(
                    {"error": "tenant callback rate limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": "60"},
                ), "rate_limited"

        async def _dispatch() -> None:
            await run_and_reply(
                tenant_id=tenant_id,
                channel=channel,
                inbound=inbound,
                worker=worker,
                registry=registry,
            )

        async def _background_dispatch() -> None:
            try:
                await _dispatch()
            except asyncio.CancelledError:
                with suppress(Exception):
                    await store.release(dedup_key)
                raise
            except Exception:  # noqa: BLE001 - background failures are logged and released
                await store.release(dedup_key)
                logger.exception("background dispatch failed for tenant=%s channel=%s", tenant_id, channel)

        if queue is not None:
            # Decoupled mode: enqueue for a separate worker process.
            try:
                with operation_span("callback.enqueue", **{"tenant.id": tenant_id, "channel": channel}):
                    await queue.enqueue(
                        TaskMessage.from_inbound(
                            tenant_id,
                            channel,
                            inbound,
                            trace_headers=inject_trace_headers(),
                            config_revision=manager.current_version(tenant_id),
                        ))
                metrics.increment(
                    "agent_callback_enqueue_total",
                    tenant_id=tenant_id,
                    channel=channel,
                    outcome="success",
                )
            except Exception as exc:  # noqa: BLE001 - return retryable response to IM platform
                metrics.increment(
                    "agent_callback_enqueue_total",
                    tenant_id=tenant_id,
                    channel=channel,
                    outcome="error",
                    error_type=type(exc).__name__,
                )
                await store.release(dedup_key)
                logger.exception("failed to enqueue callback")
                return JSONResponse({"error": type(exc).__name__}, status_code=503), "enqueue_failed"
        elif async_dispatch:
            # Keep a strong reference to the task until it completes: asyncio may
            # otherwise garbage-collect a task with no external references.
            task = asyncio.create_task(_background_dispatch())
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
        else:
            try:
                await _dispatch()
            except Exception as exc:  # noqa: BLE001 - allow caller to retry
                await store.release(dedup_key)
                return JSONResponse({"error": type(exc).__name__}, status_code=503), "dispatch_failed"

        return JSONResponse(adapter.callback_response(), status_code=200), "success"

    @app.post("/webhook/{tenant_id}/{channel}")
    async def webhook(tenant_id: str, channel: str, request: Request) -> JSONResponse:
        started = time.perf_counter()
        outcome = "error"
        try:
            with callback_span(tenant_id, channel):
                response, outcome = await _handle_webhook(tenant_id, channel, request)
                return response
        finally:
            metrics.increment("agent_callback_total", tenant_id=tenant_id, channel=channel, outcome=outcome)
            metrics.observe(
                "agent_callback_duration_ms",
                (time.perf_counter() - started) * 1000,
                tenant_id=tenant_id,
                channel=channel,
                outcome=outcome,
            )

    if test_api_key:
        app.include_router(create_test_message_router(worker=worker, api_key=test_api_key, queue=queue))

    async def _shutdown_gateway_resources() -> None:
        tasks = list(background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await resources.close()

    app.router.add_event_handler("shutdown", _shutdown_gateway_resources)

    return app
