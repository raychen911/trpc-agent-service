import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from trpc_service.storage.models import IdempotencyKey
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


@pytest.mark.shared_backend
async def test_only_expired_pre_start_owner_can_be_replaced(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    a = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="a", owner_lease_ms=50)
    b = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="b", owner_lease_ms=50)
    for index in range(20):
        key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id=f"takeover-{index}")
        first = await a.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))
        await asyncio.sleep(0.07)
        second = await b.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))
        assert second.disposition.value == "acquired"
        assert (await b.get(key)).generation == 2
    await redis.aclose()


@pytest.mark.shared_backend
async def test_twenty_post_start_interruptions_are_never_replayed(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    a = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="a", owner_lease_ms=40)
    b = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="b", owner_lease_ms=40)
    for index in range(20):
        key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id=f"post-start-{index}")
        trace = uuid4(); claim = await a.claim(key, "a" * 64, trace, datetime.now(timezone.utc))
        await a.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
        await asyncio.sleep(0.055)
        observed = await b.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))
        assert observed.disposition.value == "outcome_unknown"
        assert (await b.get(key)).generation == 1
    await redis.aclose()
