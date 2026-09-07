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

from typing import Any

from trpc_service.metrics._observability import callback_span


async def run_turn(*, tenant_id: str, channel: str, inbound: Any, worker: Any) -> str:
    return await worker.handle(tenant_id, channel, inbound)


async def reply_to_channel(*, tenant_id: str, channel: str, inbound: Any, text: str, worker: Any,
                           registry: Any) -> None:
    if not text:
        return
    tenant = worker.resolve_tenant(tenant_id)
    if tenant is None:
        return
    adapter = registry.get(tenant, channel)
    if adapter is None:
        raise RuntimeError(f"channel adapter disappeared: {tenant_id}/{channel}")
    result = await adapter.reply_text(inbound, text)
    metrics = getattr(worker, "metrics", None)
    if result is not None and not result.ok:
        if metrics is not None:
            metrics.increment("agent_im_delivery_total", tenant_id=tenant_id, channel=channel, outcome="error")
        raise RuntimeError(result.error or "channel reply failed")
    if metrics is not None:
        metrics.increment("agent_im_delivery_total", tenant_id=tenant_id, channel=channel, outcome="success")


async def run_and_reply(*, tenant_id: str, channel: str, inbound: Any, worker: Any, registry: Any) -> bool:
    """Run one Agent turn and send its result through the channel adapter."""
    with callback_span(tenant_id, channel):
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
