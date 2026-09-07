# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Shared dispatch helper: run the agent turn and reply via the channel adapter.

Used by both the in-process gateway path and the out-of-process stream worker,
so the two paths stay behaviourally identical.
"""

from __future__ import annotations

import time
from typing import Any

from trpc_service.metrics import operation_span


async def run_turn(*, tenant_id: str, channel: str, inbound: Any, worker: Any) -> str:
    return await worker.handle(tenant_id, channel, inbound)


async def reply_to_channel(*, tenant_id: str, channel: str, inbound: Any, text: str, worker: Any,
                           registry: Any) -> None:
    metrics = getattr(worker, "metrics", None)
    if not text:
        if metrics is not None:
            metrics.increment(
                "agent_im_delivery_total",
                tenant_id=tenant_id,
                channel=channel,
                outcome="skipped_empty",
            )
            metrics.observe(
                "agent_im_delivery_parts",
                0,
                tenant_id=tenant_id,
                channel=channel,
                outcome="skipped_empty",
            )
        return
    tenant = worker.resolve_tenant(tenant_id)
    if tenant is None:
        if metrics is not None:
            metrics.increment(
                "agent_im_delivery_total",
                tenant_id=tenant_id,
                channel=channel,
                outcome="tenant_unavailable",
            )
        return
    adapter = registry.get(tenant, channel)
    if adapter is None:
        if metrics is not None:
            metrics.increment(
                "agent_im_delivery_total",
                tenant_id=tenant_id,
                channel=channel,
                outcome="adapter_missing",
            )
        raise RuntimeError(f"channel adapter disappeared: {tenant_id}/{channel}")
    started = time.perf_counter()
    outcome = "error"
    error_type = None
    try:
        with operation_span("im.reply", **{"tenant.id": tenant_id, "channel": channel}):
            result = await adapter.reply_text(inbound, text)
        if result is not None and not result.ok:
            error_type = "SendResultError"
            raise RuntimeError(result.error or "channel reply failed")
        outcome = "success"
    except Exception as exc:
        error_type = error_type or type(exc).__name__
        raise
    finally:
        if metrics is not None:
            attributes = {
                "tenant_id": tenant_id,
                "channel": channel,
                "outcome": outcome,
                "error_type": error_type,
            }
            metrics.increment("agent_im_delivery_total", **attributes)
            metrics.observe(
                "agent_im_delivery_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )
            metrics.observe("agent_im_delivery_parts", 1, **attributes)


async def run_and_reply(*, tenant_id: str, channel: str, inbound: Any, worker: Any, registry: Any) -> bool:
    """Run one Agent turn and send its result through the channel adapter."""
    with operation_span("worker.process_inline", **{"tenant.id": tenant_id, "channel": channel}):
        text = await run_turn(tenant_id=tenant_id, channel=channel, inbound=inbound, worker=worker)
        await reply_to_channel(
            tenant_id=tenant_id,
            channel=channel,
            inbound=inbound,
            text=text,
            worker=worker,
            registry=registry,
        )
        return True
