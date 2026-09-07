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
import json
import logging
import time
from typing import Any
from typing import Callable
from typing import Optional

from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse

from .._dispatch import run_and_reply
from trpc_service.agent._queue import TaskMessage
from trpc_service.metrics._observability import callback_span
from trpc_service.metrics._observability import inject_trace_headers
from trpc_service.metrics._observability import operation_span
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.log import safe_error_message
from trpc_service.channels import ChannelAdapter
from trpc_service.channels import DingTalkAdapter
from trpc_service.channels import FeishuAdapter
from trpc_service.channels import QQAdapter
from trpc_service.channels import WecomAdapter
from trpc_service.channels import WechatCustomerServiceAdapter
from trpc_service.tenant import ChannelConfig
from trpc_service.tenant import DingTalkChannelConfig
from trpc_service.tenant import FeishuChannelConfig
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import WeComChannelConfig
from trpc_service.tenant import WechatCustomerServiceChannelConfig
from trpc_service.agent import TenantWorker
from ._idempotency import LocalIdempotencyStore
from ._idempotency import build_idempotency_store

ChannelAdapterFactory = Callable[[ChannelConfig], ChannelAdapter]

logger = logging.getLogger(__name__)


def _wecom_factory(cfg: WeComChannelConfig) -> WecomAdapter:
    return WecomAdapter(
        token=cfg.token.get_secret_value(),
        encoding_aes_key=cfg.aes_key.get_secret_value(),
        corp_id=cfg.corp_id,
        agent_id=cfg.agent_id,
        access_token=cfg.access_token.get_secret_value() if cfg.access_token else None,
        corp_secret=cfg.secret.get_secret_value() if cfg.secret else None,
    )


def _wechat_kf_factory(cfg: WechatCustomerServiceChannelConfig) -> WechatCustomerServiceAdapter:
    return WechatCustomerServiceAdapter(
        corp_id=cfg.corp_id,
        open_kfid=cfg.open_kfid,
        token=cfg.token.get_secret_value(),
        webhook_url=cfg.webhook_url,
    )


def _dingtalk_factory(cfg: DingTalkChannelConfig) -> DingTalkAdapter:
    return DingTalkAdapter(
        client_id=cfg.app_id,
        robot_code=cfg.robot_code,
        secret=cfg.secret.get_secret_value(),
        webhook_url=cfg.webhook_url,
    )


def _feishu_factory(cfg: FeishuChannelConfig) -> FeishuAdapter:
    return FeishuAdapter(
        app_id=cfg.app_id,
        verification_token=(cfg.verification_token.get_secret_value() if cfg.verification_token else ""),
        encrypt_key=cfg.encrypt_key.get_secret_value() if cfg.encrypt_key else "",
        secret=cfg.secret.get_secret_value() if cfg.secret else None,
        webhook_url=cfg.webhook_url,
    )


def _qq_factory(cfg: QQChannelConfig) -> QQAdapter:
    return QQAdapter(
        app_id=cfg.app_id,
        app_secret=cfg.secret.get_secret_value(),
        access_token=cfg.access_token.get_secret_value() if cfg.access_token else None,
    )


def default_channel_factories() -> dict[str, ChannelAdapterFactory]:
    """Return the built-in adapter factories keyed by channel type."""
    return {
        "wecom": _wecom_factory,
        "wechat_kf": _wechat_kf_factory,
        "dingtalk": _dingtalk_factory,
        "feishu": _feishu_factory,
        "qq": _qq_factory,
    }


class ChannelRegistry:
    """Maps ``(tenant_id, channel)`` to a cached adapter instance."""

    def __init__(self, factories: Optional[dict[str, ChannelAdapterFactory]] = None) -> None:
        self._factories = factories or default_channel_factories()
        self._cache: dict[tuple[str, str], ChannelAdapter] = {}

    def register_factory(self, channel: str, factory: ChannelAdapterFactory) -> None:
        self._factories[channel] = factory

    def invalidate(self, tenant_id: Optional[str] = None) -> None:
        """Drop cached adapters after a tenant configuration change."""
        if tenant_id is None:
            self._cache.clear()
            return
        for key in [key for key in self._cache if key[0] == tenant_id]:
            self._cache.pop(key, None)

    def get(self, tenant: Tenant, channel: str) -> Optional[ChannelAdapter]:
        key = (tenant.tenant_id, channel)
        if key in self._cache:
            return self._cache[key]
        factory = self._factories.get(channel)
        if factory is None:
            return None
        cfg = tenant.channel_configs.get(channel)
        if cfg is None:
            return None
        adapter = factory(cfg)
        self._cache[key] = adapter
        return adapter


def create_gateway_app(
    *,
    manager: TenantConfigManager,
    worker: TenantWorker,
    registry: Optional[ChannelRegistry] = None,
    idempotency_store: Optional[LocalIdempotencyStore] = None,
    async_dispatch: bool = True,
    queue: Any = None,
    metrics: Optional[EnterpriseMetrics] = None,
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
    metrics = metrics or getattr(worker, "metrics", None) or get_enterprise_metrics()
    background_tasks: set[asyncio.Task] = set()

    app = FastAPI(title="tRPC-Agent Enterprise Gateway")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

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

        dedup_key = f"{tenant_id}:{channel}:{inbound.message_id}"
        with operation_span("idempotency.check", **{"tenant.id": tenant_id, "channel": channel}):
            duplicate = await store.check_and_set(dedup_key)
        if duplicate:
            return JSONResponse(adapter.callback_response(duplicate=True), status_code=200), "duplicate"

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

    return app
