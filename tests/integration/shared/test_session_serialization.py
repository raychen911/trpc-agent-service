from __future__ import annotations

import asyncio

import pytest
from redis.asyncio import Redis

from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_leases import RedisSessionLeaseManager


@pytest.mark.shared_backend
async def test_same_session_serializes_fifty_contenders_while_other_sessions_overlap(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    manager = RedisSessionLeaseManager(redis, namespace=shared_namespace)
    active = 0
    peak = 0

    async def same_session(index: int) -> None:
        nonlocal active, peak
        lease = await manager.acquire("tenant-alpha", "agent-alpha", "sess_" + "1" * 64,
                                      f"{index:064x}", NodeIdentity(node_id=f"node-{index % 2}"), 500, 2000)
        async with lease:
            active += 1; peak = max(peak, active)
            await asyncio.sleep(0.002)
            active -= 1

    await asyncio.gather(*(same_session(i) for i in range(50)))
    assert peak == 1

    parallel_groups = 0
    for group in range(20):
        entered = 0
        both = asyncio.Event()

        async def distinct(side: int) -> None:
            nonlocal entered
            marker = f"{group * 2 + side + 2:064x}"
            lease = await manager.acquire(
                "tenant-alpha", "agent-alpha", "sess_" + marker,
                marker, NodeIdentity(node_id=f"node-{side}"), 500, 100,
            )
            async with lease:
                entered += 1
                if entered == 2:
                    both.set()
                await asyncio.wait_for(both.wait(), 0.3)

        await asyncio.gather(distinct(0), distinct(1))
        parallel_groups += entered == 2
    assert parallel_groups == 20
    await redis.aclose()
