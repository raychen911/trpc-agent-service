"""Real Redis integration and fault tests.

Prerequisite: TRPC_TEST_REDIS_URL. These tests create unique keys/streams and do
not call a model or IM. Expected: cross-client idempotency, stale reclaim and
observable lease loss all work. If failing, inspect gateway/queue.py,
gateway/idempotency.py and storage/guard.py first.
"""

import asyncio
import os
import uuid

import pytest
from redis.asyncio import from_url

from trpc_service.gateway import AgentRequest
from trpc_service.gateway import AgentTaskEnvelope
from trpc_service.gateway import RedisIdempotencyStore
from trpc_service.gateway import RedisStreamAgentTaskQueue
from trpc_service.storage import RedisSessionExecutionGuard
from trpc_service.storage import SessionLockLostError

REDIS_URL = os.getenv("TRPC_TEST_REDIS_URL")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not REDIS_URL, reason="TRPC_TEST_REDIS_URL is not set")]


@pytest.mark.asyncio
async def test_real_redis_idempotency_and_stream_reclaim():
    suffix = uuid.uuid4().hex
    idempotency = RedisIdempotencyStore(REDIS_URL, prefix=f"test:{suffix}:idem")
    first = await idempotency.reserve("key", "r1", ttl_seconds=60, payload_hash="h")
    duplicate = await idempotency.reserve("key", "r2", ttl_seconds=60, payload_hash="h")
    assert first.request_id == duplicate.request_id == "r1"

    queue = RedisStreamAgentTaskQueue(REDIS_URL,
                                      stream=f"test:{suffix}:tasks",
                                      group=f"test:{suffix}:group",
                                      dead_stream=f"test:{suffix}:dead")
    request = AgentRequest(request_id="r1",
                           tenant_id="t",
                           config_version=1,
                           app_id="a",
                           user_id="u",
                           session_id="s",
                           text="hello")
    await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key="key"))
    delivery = await queue.receive("crashed-worker", timeout_seconds=1)
    assert delivery is not None
    await asyncio.sleep(0.02)
    reclaimed = await queue.reclaim_stale("replacement-worker", idle_ms=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].envelope.request.request_id == "r1"
    await queue.ack(reclaimed[0])
    await queue.close()
    await idempotency.close()


@pytest.mark.asyncio
async def test_real_redis_retry_atomically_replaces_pending_delivery():
    suffix = uuid.uuid4().hex
    queue = RedisStreamAgentTaskQueue(REDIS_URL,
                                      stream=f"test:{suffix}:tasks",
                                      group=f"test:{suffix}:group",
                                      dead_stream=f"test:{suffix}:dead")
    request = AgentRequest(request_id="retry",
                           tenant_id="t",
                           config_version=1,
                           app_id="a",
                           user_id="u",
                           session_id="s",
                           text="hello")
    await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key="key"))
    original = await queue.receive("first-worker", timeout_seconds=1)
    assert original is not None

    await queue.retry(original)

    pending = await queue._redis.xpending(queue._stream, queue._group)
    assert pending["pending"] == 0
    retry = await queue.receive("replacement-worker", timeout_seconds=1)
    assert retry is not None
    assert retry.envelope.request.request_id == "retry"
    assert retry.envelope.attempts == 1
    await queue.ack(retry)
    assert (await queue._redis.xpending(queue._stream, queue._group))["pending"] == 0
    await queue.close()


@pytest.mark.asyncio
async def test_real_redis_consumer_cleanup_preserves_pending_recovery():
    suffix = uuid.uuid4().hex
    queue = RedisStreamAgentTaskQueue(REDIS_URL,
                                      stream=f"test:{suffix}:tasks",
                                      group=f"test:{suffix}:group",
                                      dead_stream=f"test:{suffix}:dead")
    request = AgentRequest(request_id="consumer-cleanup",
                           tenant_id="t",
                           config_version=1,
                           app_id="a",
                           user_id="u",
                           session_id="s",
                           text="hello")
    await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key="key"))
    delivery = await queue.receive("worker-with-pending", timeout_seconds=1)
    assert delivery is not None

    # Never delete a Consumer while its delivery still needs XAUTOCLAIM.
    assert not await queue.unregister_consumer("worker-with-pending")
    consumers = await queue._redis.xinfo_consumers(queue._stream, queue._group)
    assert any(item["name"] == "worker-with-pending" and item["pending"] == 1 for item in consumers)

    await queue.ack(delivery)
    assert await queue.unregister_consumer("worker-with-pending")
    consumers = await queue._redis.xinfo_consumers(queue._stream, queue._group)
    assert all(item["name"] != "worker-with-pending" for item in consumers)
    await queue.close()


@pytest.mark.fault
@pytest.mark.asyncio
async def test_real_redis_lease_deletion_is_observable():
    suffix = uuid.uuid4().hex
    client = from_url(REDIS_URL, decode_responses=True)
    guard = RedisSessionExecutionGuard(REDIS_URL, prefix=f"test:{suffix}:lock", client=client)
    with pytest.raises(SessionLockLostError):
        async with guard.hold("session", wait_timeout=1, lease_seconds=1) as lease:
            await client.delete(f"test:{suffix}:lock:session")
            await asyncio.wait_for(lease.lost.wait(), timeout=2)
    await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_budget_concurrent_reservation_and_idempotent_settlement():
    from trpc_service.config import TenantConfig
    from trpc_service.tenant.budget import RedisBudgetLedger
    from trpc_service.tenant import BudgetExceededError
    tenant = TenantConfig(tenant_id=f"budget-test-{uuid.uuid4().hex[:12]}", budget={"daily_requests": 10})
    ledger = RedisBudgetLedger(REDIS_URL)

    async def reserve():
        try:
            await ledger.reserve(tenant, input_tokens=2)
            return True
        except BudgetExceededError:
            return False

    results = await asyncio.gather(*(reserve() for _ in range(20)))
    assert sum(results) == 10
    await ledger.settle_actual(tenant, request_id="r", input_tokens=3, output_tokens=1, cost_usd=0)
    await ledger.settle_actual(tenant, request_id="r", input_tokens=3, output_tokens=1, cost_usd=0)
    from datetime import date
    key = f"trpc-service:budget:{tenant.tenant_id}:{date.today().isoformat()}"
    assert int(await ledger._redis.hget(key, "input")) == 23
    await ledger.close()
