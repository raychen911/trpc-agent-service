from __future__ import annotations

import asyncio
import os
from typing import Any, cast
from uuid import uuid4

import pytest
import redis.asyncio as redis_async

from trpc_service.queue.redis_streams import RedisStreamQueue
from trpc_service.storage.models import OutboxRecord

pytestmark = pytest.mark.integration


def _required_redis_url() -> str:
    value = os.getenv("TRPC_TEST_REDIS_URL")
    if not value:
        pytest.skip("TRPC_TEST_REDIS_URL is not set")
    return value


@pytest.mark.asyncio
async def test_real_redis_pel_heartbeat_and_owner_transfer() -> None:
    """Exercise PEL ownership with two real Redis consumers at low volume."""

    redis_url = _required_redis_url()
    client_a = redis_async.from_url(redis_url, decode_responses=False)
    client_b = redis_async.from_url(redis_url, decode_responses=False)
    suffix = uuid4().hex
    stream = f"trpc:test:queue:{suffix}"
    group = f"trpc-test-workers-{suffix}"
    outbox_id = f"outbox-{suffix}"
    queue_a = RedisStreamQueue(
        cast(Any, client_a),
        stream=stream,
        group=group,
        reclaim_after_ms=300,
    )
    queue_b = RedisStreamQueue(
        cast(Any, client_b),
        stream=stream,
        group=group,
        reclaim_after_ms=300,
    )
    heartbeat_stop = asyncio.Event()
    heartbeat_task: asyncio.Task[bool] | None = None
    try:
        await queue_a.ensure_group()
        await queue_b.ensure_group()
        await queue_a.publish(
            OutboxRecord(
                outbox_id=outbox_id,
                tenant_id="tenant-pel-test",
                event_type="inbound.accepted",
                aggregate_id=f"inbound-{suffix}",
                payload={"text": "pel heartbeat"},
            )
        )

        delivered = await queue_a.consume(consumer="owner-a", block_ms=100)
        assert len(delivered) == 1
        message = delivered[0]

        heartbeat_task = asyncio.create_task(queue_a.heartbeat(message, "owner-a", heartbeat_stop))
        # The heartbeat interval is reclaim_after_ms / 3.  Waiting for more
        # than one reclaim window proves that B cannot claim an active turn.
        await asyncio.sleep(0.75)
        assert await queue_b.consume(consumer="owner-b", block_ms=1) == ()
        assert not heartbeat_task.done()

        heartbeat_stop.set()
        assert await heartbeat_task is True
        heartbeat_task = None

        # Once the owner stops refreshing its PEL entry, the other consumer
        # can reclaim it after the configured idle window.
        await asyncio.sleep(0.45)
        reclaimed = await queue_b.consume(consumer="owner-b", block_ms=1)
        assert len(reclaimed) == 1
        assert reclaimed[0].stream_id == message.stream_id

        # Ownership has moved to B.  A stale owner must not refresh or steal
        # the delivery back through the exact-message defer path.
        assert await queue_a.defer(message, "owner-a") is False
        await queue_b.ack(reclaimed[0])
    finally:
        if heartbeat_task is not None:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        await client_a.delete(stream, f"trpc:published:{outbox_id}")
        await client_a.aclose()
        await client_b.aclose()
