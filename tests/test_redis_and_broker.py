from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest

from tenant_agent.ids import IdentityDeriver
from tenant_agent.models import MemoryRecord, SummaryRecord
from tenant_agent.services import broker as broker_module
from tenant_agent.services.broker import (
    PUBLISH_SCRIPT,
    BrokerCapacityError,
    BrokerMessage,
    InlineBroker,
    RedisStreamsBroker,
)
from tenant_agent.services.dispatcher import GatewayRouter
from tenant_agent.storage import redis as redis_module
from tenant_agent.storage.base import ConcurrentWriteError
from tenant_agent.storage.redis import (
    APPEND_EVENT_SCRIPT,
    PUT_SUMMARY_SCRIPT,
    RELEASE_LOCK_SCRIPT,
    RENEW_LOCK_SCRIPT,
    RedisPlane,
)
from tests.helpers import make_envelope, make_tenant


def fake_redis_plane() -> RedisPlane:
    plane = RedisPlane("redis://unused")
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    plane.redis = fake
    plane._append_event = fake.register_script(APPEND_EVENT_SCRIPT)
    plane._put_summary = fake.register_script(PUT_SUMMARY_SCRIPT)
    plane._renew_lock = fake.register_script(RENEW_LOCK_SCRIPT)
    plane._release_lock = fake.register_script(RELEASE_LOCK_SCRIPT)
    return plane


def test_redis_cluster_clients_are_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    broker_fake = fakeredis.aioredis.FakeRedis(decode_responses=True)

    class DataCluster:
        @classmethod
        def from_url(cls, url: str, **kwargs: object) -> object:
            del cls, url, kwargs
            return data_fake

    class BrokerCluster:
        @classmethod
        def from_url(cls, url: str, **kwargs: object) -> object:
            del cls, url, kwargs
            return broker_fake

    monkeypatch.setattr(redis_module, "RedisCluster", DataCluster)
    monkeypatch.setattr(broker_module, "RedisCluster", BrokerCluster)
    plane = RedisPlane("redis://cluster", cluster=True)
    broker = RedisStreamsBroker(
        url="redis://cluster",
        stream="{tap}:jobs",
        group="workers",
        consumer="node",
        claim_idle_ms=100,
        cluster=True,
    )
    assert plane.cluster is True
    assert plane.redis is data_fake
    assert broker.redis is broker_fake
    with pytest.raises(ValueError, match="hash tag"):
        RedisStreamsBroker(
            url="redis://cluster",
            stream="untagged-stream",
            group="workers",
            consumer="node",
            claim_idle_ms=100,
            cluster=True,
        )


@pytest.mark.asyncio
async def test_redis_session_event_summary_memory_and_lease() -> None:
    plane = fake_redis_plane()
    await plane.initialize()
    assert await plane.healthcheck()
    snapshot = await plane.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    current, event = await plane.append_event(
        snapshot=snapshot,
        event_id="event-1",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"status": "received"},
        trace_id="0" * 32,
    )
    duplicate_current, duplicate_event = await plane.append_event(
        snapshot=snapshot,
        event_id="event-1",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"status": "received"},
        trace_id="0" * 32,
    )
    assert duplicate_event == event
    assert duplicate_current == current
    with pytest.raises(ConcurrentWriteError):
        await plane.append_event(
            snapshot=snapshot,
            event_id="event-2",
            kind="user_message",
            actor_id="user",
            payload={},
            state_delta={},
            trace_id="0" * 32,
        )
    assert [item.event_id for item in await plane.list_events("alpha", "session")] == ["event-1"]
    assert [item.session_id async for item in plane.iter_sessions("alpha")] == ["session"]

    summary = SummaryRecord(
        tenant_id="alpha",
        session_id="session",
        version=1,
        through_event_sequence=1,
        content="hello",
    )
    with pytest.raises(ConcurrentWriteError, match="cannot cover events"):
        await plane.put_summary(summary.model_copy(update={"through_event_sequence": 2}))
    await plane.put_summary(summary)
    await plane.put_summary(summary.model_copy(update={"version": 0}))
    assert await plane.get_summary("alpha", "session") == summary
    assert [item.version async for item in plane.iter_summaries("alpha")] == [1]
    with pytest.raises(ConcurrentWriteError, match="summary version"):
        await plane.put_summary(summary.model_copy(update={"content": "conflict"}))

    memory = MemoryRecord(
        memory_id="memory-1",
        tenant_id="alpha",
        user_id="user",
        content="Alice likes blue",
    )
    await plane.put_memory(memory)
    await plane.put_memory(memory.model_copy(update={"tenant_id": "beta", "content": "Beta remembers red"}))
    await plane.put_memory(memory.model_copy(update={"revision": 0, "content": "stale"}))
    assert [item.memory_id for item in await plane.search_memory("alpha", "user", "blue")] == ["memory-1"]
    assert [item.memory_id for item in await plane.search_memory("beta", "user", "red")] == ["memory-1"]
    assert await plane.search_memory("alpha", "user", "red") == ()
    assert [item.memory_id async for item in plane.iter_memories("alpha")] == ["memory-1"]
    with pytest.raises(ConcurrentWriteError, match="memory revision"):
        await plane.put_memory(memory.model_copy(update={"content": "conflict"}))

    for index in range(20):
        base = memory.model_copy(update={"memory_id": f"race-{index}"})
        await asyncio.gather(
            plane.put_memory(base.model_copy(update={"revision": 3, "content": "newest"})),
            plane.put_memory(base.model_copy(update={"revision": 2, "content": "older"})),
        )
        stored = await plane.search_memory("alpha", "user", "newest", limit=100)
        assert any(item.memory_id == base.memory_id and item.revision == 3 for item in stored)

    entered = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def first() -> None:
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="session",
            owner="one",
            wait_timeout=2,
            lease_seconds=1,
        ):
            order.append("one")
            entered.set()
            await release.wait()

    async def second() -> None:
        await entered.wait()
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="session",
            owner="two",
            wait_timeout=2,
            lease_seconds=1,
        ):
            order.append("two")

    first_task = asyncio.create_task(first())
    second_task = asyncio.create_task(second())
    await entered.wait()
    await asyncio.sleep(0.05)
    release.set()
    await asyncio.gather(first_task, second_task)
    assert order == ["one", "two"]
    await plane.close()


@pytest.mark.asyncio
async def test_inline_broker_retry_and_ack() -> None:
    broker = InlineBroker()
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant), tenant
    )
    broker_id = await broker.publish(routed)
    message = await broker.receive(timeout_ms=100)
    assert message is not None and message.broker_id == broker_id
    await broker.fail(message)
    retried = await broker.receive(timeout_ms=100)
    assert retried is not None and retried.attempts == 1
    await broker.ack(retried)
    assert await broker.receive(timeout_ms=1) is None

    bounded = InlineBroker(max_queue_size=1)
    await bounded.publish(routed)
    with pytest.raises(BrokerCapacityError, match="global"):
        await bounded.publish(routed)

    await broker.publish(routed)
    deferred = await broker.receive(timeout_ms=100)
    assert deferred is not None
    await broker.defer(deferred, delay_seconds=0)
    redelivered = await broker.receive(timeout_ms=100)
    assert redelivered == BrokerMessage(
        deferred.broker_id,
        deferred.routed,
        attempts=deferred.attempts + 1,
    )
    await broker.ack(redelivered)

    await broker.publish(routed)
    terminal = await broker.receive(timeout_ms=100)
    assert terminal is not None
    await broker.fail(terminal, terminal=True)
    assert tuple(broker.dead_letters) == (BrokerMessage(terminal.broker_id, terminal.routed, attempts=1),)
    assert await broker.receive(timeout_ms=1) is None
    assert [item.broker_id for item in await broker.list_dead("alpha", limit=10)] == [terminal.broker_id]
    requeued_id = await broker.requeue_dead("alpha", terminal.broker_id)
    assert requeued_id != terminal.broker_id
    requeued = await broker.receive(timeout_ms=100)
    assert requeued is not None and requeued.routed == terminal.routed
    await broker.ack(requeued)
    assert await broker.list_dead("alpha", limit=10) == ()


@pytest.mark.asyncio
async def test_redis_streams_broker_publish_receive_and_dead_letter() -> None:
    broker = RedisStreamsBroker(
        url="redis://unused",
        stream="jobs",
        group="workers",
        consumer="node-1",
        claim_idle_ms=100,
        max_attempts=2,
    )
    await broker.redis.aclose()
    broker.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await broker.initialize()
    assert await broker.healthcheck()
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant), tenant
    )
    await broker.publish(routed)
    assert "MAXLEN" not in PUBLISH_SCRIPT.upper()
    assert await broker.redis.hget(broker.tenant_counts_key, "alpha") == "1"
    message = await broker.receive(timeout_ms=100)
    assert message is not None and message.routed == routed
    await broker.defer(message, delay_seconds=0.02)
    assert await broker.redis.xlen("jobs") == 0
    assert await broker.redis.zcard(broker.delayed_key) == 1
    pending = await broker.redis.xpending("jobs", "workers")
    assert pending["pending"] == 0
    assert await broker.receive(timeout_ms=1) is None
    await asyncio.sleep(0.03)
    deferred = await broker.receive(timeout_ms=100)
    assert deferred is not None and deferred.attempts == 1
    await broker.defer(deferred, delay_seconds=0)
    dead_stream = f"{broker.dead_letter_stream}:alpha"
    dead = await broker.redis.xrange(dead_stream)
    assert len(dead) == 1
    assert await broker.redis.hget(broker.tenant_counts_key, "alpha") is None
    dead_id = dead[0][0]
    assert [item.broker_id for item in await broker.list_dead("alpha", limit=10)] == [dead_id]
    requeued_id = await broker.requeue_dead("alpha", dead_id)
    assert requeued_id != dead_id
    assert await broker.redis.xlen(dead_stream) == 0
    requeued = await broker.receive(timeout_ms=100)
    assert requeued is not None and requeued.routed == routed
    await broker.ack(requeued)

    await broker.publish(routed)
    duplicate_wait = await broker.receive(timeout_ms=100)
    assert duplicate_wait is not None
    await broker.defer(duplicate_wait, delay_seconds=0, count_attempt=False)
    duplicate_wait = await broker.receive(timeout_ms=100)
    assert duplicate_wait is not None and duplicate_wait.attempts == 0
    await broker.ack(duplicate_wait)
    await broker.close()


@pytest.mark.asyncio
async def test_redis_streams_broker_enforces_global_and_tenant_admission() -> None:
    broker = RedisStreamsBroker(
        url="redis://unused",
        stream="jobs",
        group="workers",
        consumer="node-1",
        claim_idle_ms=100,
        global_queue_limit=2,
        tenant_queue_limit=1,
    )
    await broker.redis.aclose()
    broker.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await broker.initialize()
    router = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key"))
    alpha = make_tenant("alpha")
    beta = make_tenant("beta")
    gamma = make_tenant("gamma")

    await broker.publish(router.route(make_envelope(alpha), alpha))
    with pytest.raises(BrokerCapacityError, match="tenant"):
        await broker.publish(router.route(make_envelope(alpha), alpha))
    await broker.publish(router.route(make_envelope(beta), beta))
    with pytest.raises(BrokerCapacityError, match="global"):
        await broker.publish(router.route(make_envelope(gamma), gamma))

    first = await broker.receive(timeout_ms=100)
    assert first is not None and first.routed.inbound.tenant_id == "alpha"
    await broker.defer(first, delay_seconds=60)
    assert await broker.redis.zcard(broker.delayed_key) == 1
    assert await broker.redis.hget(broker.tenant_counts_key, "alpha") == "1"
    with pytest.raises(BrokerCapacityError, match="global"):
        await broker.publish(router.route(make_envelope(gamma), gamma))
    await broker.close()
