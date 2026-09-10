from datetime import datetime, timezone
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from tests.contract.support.repository_contracts import assert_idempotency_contract
from trpc_service.storage.inmemory import InMemoryIdempotencyRepository
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


async def test_inmemory_uses_vendor_neutral_idempotency_contract() -> None:
    await assert_idempotency_contract(InMemoryIdempotencyRepository())


@pytest.mark.shared_backend
async def test_redis_uses_same_vendor_neutral_idempotency_contract(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    await assert_idempotency_contract(RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="contract"))
    await redis.aclose()
