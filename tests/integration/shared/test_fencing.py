from __future__ import annotations

import asyncio

import pytest
from redis.asyncio import Redis

from trpc_service.storage.contracts import LeaseLost
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_leases import RedisSessionLeaseManager


@pytest.mark.shared_backend
async def test_expired_generation_never_revives_after_new_owner_takes_over(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    manager = RedisSessionLeaseManager(redis, namespace=shared_namespace)
    session = "sess_" + "9" * 64
    old = await manager.acquire("tenant-alpha", "agent-alpha", session, "1" * 64,
                                NodeIdentity(node_id="old"), 60, 10)
    await asyncio.sleep(0.08)
    new = await manager.acquire("tenant-alpha", "agent-alpha", session, "2" * 64,
                                NodeIdentity(node_id="new"), 200, 100)
    assert new.fence.generation > old.fence.generation
    with pytest.raises(LeaseLost):
        await old.renew(500)
    assert await new.pttl() > 0
    await new.release()
    await redis.aclose()
