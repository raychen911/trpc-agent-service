"""Node liveness and deterministic routing tests."""

from __future__ import annotations

import fakeredis.aioredis as faioredis

from trpc_service.runtime import InMemoryNodeDirectory
from trpc_service.runtime import NodeInfo
from trpc_service.runtime import RedisNodeDirectory
from trpc_service.runtime import RendezvousRouter


async def test_in_memory_directory_expires_filters_and_removes_nodes():
    now = [100.0]
    directory = InMemoryNodeDirectory(clock=lambda: now[0])
    await directory.heartbeat(NodeInfo(node_id="w2", role="worker"), 5)
    await directory.heartbeat(NodeInfo(node_id="w1", role="worker"), 5)
    await directory.heartbeat(NodeInfo(node_id="g1", role="gateway"), 5)

    assert [node.node_id for node in await directory.healthy("worker")] == ["w1", "w2"]
    await directory.remove("w1")
    assert [node.node_id for node in await directory.healthy()] == ["g1", "w2"]
    now[0] = 106
    assert await directory.healthy() == []


async def test_redis_directory_roundtrip_filter_missing_payload_and_remove():
    client = faioredis.FakeRedis(decode_responses=True)
    directory = RedisNodeDirectory(client=client, prefix="test:nodes")
    await directory.heartbeat(NodeInfo(node_id="worker-a", role="worker", capacity=3), 30)
    await directory.heartbeat(NodeInfo(node_id="gateway-a", role="gateway"), 30)
    await client.zadd("test:nodes:index", {"missing": 9999999999})

    workers = await directory.healthy("worker")
    assert [(node.node_id, node.capacity) for node in workers] == [("worker-a", 3)]
    assert await client.zscore("test:nodes:index", "missing") is None
    await directory.remove("worker-a")
    assert await directory.healthy("worker") == []
    await directory.close()


def test_redis_directory_requires_connection_and_router_is_stable_and_load_aware():
    try:
        RedisNodeDirectory()
    except ValueError as exc:
        assert "requires redis_url" in str(exc)
    else:
        raise AssertionError("missing Redis connection must fail")

    nodes = [
        NodeInfo(node_id="a", role="worker", capacity=1),
        NodeInfo(node_id="b", role="worker", capacity=5),
    ]
    first = RendezvousRouter.choose("tenant:session", nodes)
    assert first == RendezvousRouter.choose("tenant:session", list(reversed(nodes)))
    assert RendezvousRouter.choose("key", []) is None
    choices = [RendezvousRouter.choose(f"session-{index}", nodes).node_id for index in range(100)]
    assert choices.count("b") > choices.count("a")
