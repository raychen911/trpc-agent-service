from __future__ import annotations

import asyncio

import pytest
from redis.asyncio import Redis

from trpc_service.storage.contracts import LeaseLost
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_leases import RedisSessionLeaseManager
from trpc_service.metrics.shared import SharedMetricsRecorder
from types import SimpleNamespace


@pytest.mark.shared_backend
async def test_lease_uses_backend_ttl_generation_and_current_owner_renewal(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    manager = RedisSessionLeaseManager(redis, namespace=shared_namespace)
    first = await manager.acquire("tenant-alpha", "agent-alpha", "sess_" + "a" * 64,
                                  "b" * 64, NodeIdentity(node_id="node-a"), 120, 20)
    assert first.fence.generation == 1
    assert await first.pttl() > 0
    await first.renew(120)
    await asyncio.sleep(0.14)
    second = await manager.acquire("tenant-alpha", "agent-alpha", "sess_" + "a" * 64,
                                   "c" * 64, NodeIdentity(node_id="node-b"), 120, 100)
    assert second.fence.generation == 2
    with pytest.raises(LeaseLost):
        await first.renew(120)
    await second.release()
    await redis.aclose()


@pytest.mark.shared_backend
async def test_message_lease_emits_anonymous_wait_and_node_dimensions(
    shared_redis_url: str, shared_namespace: str,
) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    metrics = SharedMetricsRecorder(node_id="node-a")
    node = NodeIdentity(node_id="node-a")
    manager = RedisSessionLeaseManager(
        redis, namespace=shared_namespace, node=node, metrics=metrics,
    )
    context = SimpleNamespace(tenant_id="tenant-alpha", agent_id="agent-alpha")
    identity = SimpleNamespace(platform_session_id="sess_" + "f" * 64)
    key = SimpleNamespace(
        tenant_id="tenant-alpha", binding_id="binding-alpha",
        external_message_id="lease-metric",
    )
    lease = await manager.acquire_for_message(context, identity, key)
    event = metrics.events[-1]
    assert event["node_id"] == "node-a" and event["backend"] == "redis"
    assert event["outcome"] == "acquired"
    assert event["tenant"] != "tenant-alpha"
    assert event["session"] != identity.platform_session_id
    await lease.release()
    await redis.aclose()
