# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tests for the gateway→worker Redis Streams queue (using fakeredis)."""

from __future__ import annotations

import asyncio
import json

import fakeredis.aioredis as faioredis
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from trpc_agent_sdk.agents import BaseAgent
from trpc_service import ChannelRegistry
from trpc_service import EnterpriseMetrics
from trpc_service import InboundMessage
from trpc_service import StreamQueue
from trpc_service import StreamWorker
from trpc_service import TaskMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import create_gateway_app
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.channels import ChannelAdapter
from trpc_service.channels import SendResult
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_service.tenant import WeComChannelConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part


def _metric_values(metrics, name):
    return {
        tuple(sorted(item["attributes"].items())): item["value"]
        for item in metrics.snapshot()["counters"] if item["name"] == name
    }


def _has_attributes(metrics, name, **expected):
    expected = {key: str(value) for key, value in expected.items()}
    return any(item["name"] == name and all(item["attributes"].get(key) == value for key, value in expected.items())
               for item in metrics.snapshot()["counters"])


class EchoAgent(BaseAgent):

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx):
        user_text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text=f"echo:{user_text}")]), partial=False)


class FakeAdapter(ChannelAdapter):
    channel = "fake"

    def __init__(self, cfg=None):
        self.replies: list[str] = []

    async def verify_signature(self, payload, headers, query):
        return True

    async def parse_message(self, payload):
        msg = payload if isinstance(payload, dict) else json.loads(payload)
        return InboundMessage(channel=self.channel,
                              chat_id="c1",
                              chat_type=CHAT_PRIVATE,
                              sender_id="u1",
                              message_id=msg.get("message_id", "m1"),
                              text=msg.get("text", ""))

    async def send_message(self, outbound):
        return SendResult(ok=True)

    async def send_stream(self, chat_id, stream):
        return SendResult(ok=True)

    async def reply_text(self, inbound, text):
        self.replies.append(text)
        return SendResult(ok=True)


class FailOnceAdapter(FakeAdapter):

    def __init__(self):
        super().__init__()
        self.attempts = 0

    async def reply_text(self, inbound, text):
        self.attempts += 1
        if self.attempts == 1:
            return SendResult(ok=False, error="temporary failure")
        return await super().reply_text(inbound, text)


def _make_tenant(manager):
    tenant = Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m"))
    tenant.channel_configs["fake"] = WeComChannelConfig(
        token="token",
        aes_key="aes",
        corp_id="corp",
        agent_id="1",
    )
    manager.register(tenant)
    return tenant


# ------------------------------------------------------------ TaskMessage roundtrip


def test_task_message_roundtrip():
    inbound = InboundMessage(channel="wecom",
                             chat_id="g1",
                             chat_type="group",
                             sender_id="u1",
                             message_id="m1",
                             text="hi",
                             metadata={"k": "v"})
    task = TaskMessage.from_inbound("t_a", "wecom", inbound)
    restored = TaskMessage.model_validate_json(task.model_dump_json()).to_inbound()
    assert restored.channel == "wecom"
    assert restored.chat_id == "g1"
    assert restored.chat_type == "group"
    assert restored.sender_id == "u1"
    assert restored.text == "hi"
    assert restored.metadata == {"k": "v"}


# ------------------------------------------------------------ StreamQueue roundtrip


async def test_stream_queue_enqueue_read_ack():
    client = faioredis.FakeRedis(decode_responses=True)
    metrics = EnterpriseMetrics(meter=False)
    queue = StreamQueue(client=client, metrics=metrics)
    await queue.ensure_group()
    await queue.ensure_group()

    mid = await queue.enqueue(TaskMessage(tenant_id="t", channel="wecom", inbound={"chat_id": "c"}))
    messages = await queue.read(count=1, block=0)

    assert len(messages) == 1
    entries = messages[0][1]
    assert len(entries) == 1
    got_mid, fields = entries[0]
    assert got_mid == mid
    assert "payload" in fields

    assert await queue.ack(mid) == 1
    assert await queue.read(count=1, block=0) == []
    operations = {dict(labels)["operation"] for labels in _metric_values(metrics, "agent_queue_operation_total")}
    assert {"ensure_group", "enqueue", "read", "ack"} <= operations
    await queue.close()


async def test_stream_queue_treats_blocking_read_timeout_as_empty_poll():

    class TimeoutClient:

        async def xreadgroup(self, *args, **kwargs):
            raise RedisTimeoutError("read timed out")

    queue = StreamQueue(client=TimeoutClient(), metrics=EnterpriseMetrics(meter=False))

    assert await queue.read(count=1, block=5000) == []

    try:
        await queue.read(count=1, block=0)
    except RedisTimeoutError:
        pass
    else:
        raise AssertionError("non-blocking Redis timeout must be propagated")


# ------------------------------------------------------------ gateway enqueue path


def test_gateway_enqueues_when_queue_provided():
    manager = TenantConfigManager()
    _make_tenant(manager)
    shared = InMemorySessionService()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
    )
    adapter = FakeAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})

    class FakeQueue:

        def __init__(self):
            self.enqueued = []

        async def enqueue(self, task):
            self.enqueued.append(task)
            return "id-1"

    queue = FakeQueue()
    app = create_gateway_app(manager=manager, worker=worker, registry=registry, queue=queue)
    client = TestClient(app)
    response = client.post("/webhook/t_a/fake", json={"message_id": "m1", "text": "hi"})

    assert response.status_code == 200
    assert len(queue.enqueued) == 1
    assert queue.enqueued[0].tenant_id == "t_a"
    assert queue.enqueued[0].config_revision == 1
    assert len(queue.enqueued[0].turn_id) == 32
    # The worker must NOT have run in-process (decoupled mode).
    assert adapter.replies == []
    assert _has_attributes(worker.metrics, "agent_callback_enqueue_total", outcome="success")


def test_gateway_releases_idempotency_when_enqueue_fails():
    manager = TenantConfigManager()
    _make_tenant(manager)
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: InMemorySessionService(),
    )
    adapter = FakeAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})

    class FailOnceQueue:

        def __init__(self):
            self.calls = 0

        async def enqueue(self, task):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("redis unavailable")
            return "id-2"

    queue = FailOnceQueue()
    app = create_gateway_app(manager=manager, worker=worker, registry=registry, queue=queue)
    client = TestClient(app, raise_server_exceptions=False)
    payload = {"message_id": "retry-enqueue", "text": "hi"}

    assert client.post("/webhook/t_a/fake", json=payload).status_code == 503
    assert client.post("/webhook/t_a/fake", json=payload).status_code == 200
    assert queue.calls == 2
    assert _has_attributes(worker.metrics,
                           "agent_callback_enqueue_total",
                           outcome="error",
                           error_type="ConnectionError")
    assert _has_attributes(worker.metrics, "agent_callback_enqueue_total", outcome="success")


def test_gateway_rejects_callback_until_a_queue_worker_is_active():
    manager = TenantConfigManager()
    _make_tenant(manager)
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda tenant: EchoAgent(name="t_a"),
        session_service_factory=lambda tenant: InMemorySessionService(),
    )
    adapter = FakeAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})

    class AvailabilityQueue:

        def __init__(self):
            self.available = False
            self.enqueued = []

        async def has_active_workers(self):
            if self.available is None:
                raise RedisConnectionError("health check unavailable")
            return self.available

        async def enqueue(self, task):
            self.enqueued.append(task)
            return "id"

    queue = AvailabilityQueue()
    app = create_gateway_app(manager=manager, worker=worker, registry=registry, queue=queue)
    client = TestClient(app)
    payload = {"message_id": "wait-for-worker", "text": "hi"}

    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503
    unavailable = client.post("/webhook/t_a/fake", json=payload)
    assert unavailable.status_code == 503
    assert unavailable.json() == {"error": "worker_unavailable"}
    assert queue.enqueued == []
    assert _has_attributes(worker.metrics,
                           "agent_worker_unavailable_total",
                           tenant_id="t_a",
                           channel="fake",
                           reason="worker_unavailable")

    queue.available = True
    assert client.get("/readyz").status_code == 200
    assert client.post("/webhook/t_a/fake", json=payload).status_code == 200
    assert len(queue.enqueued) == 1

    queue.available = None
    assert client.get("/readyz").status_code == 503
    failed_check = client.post(
        "/webhook/t_a/fake",
        json={
            "message_id": "healthcheck-error",
            "text": "hi"
        },
    )
    assert failed_check.status_code == 503
    assert failed_check.json() == {"error": "worker_healthcheck_failed"}


# ------------------------------------------------------------ StreamWorker consume


async def test_stream_worker_processes_and_acks():
    client = faioredis.FakeRedis(decode_responses=True)
    metrics = EnterpriseMetrics(meter=False)
    queue = StreamQueue(client=client, metrics=metrics)
    await queue.ensure_group()

    manager = TenantConfigManager()
    _make_tenant(manager)
    shared = InMemorySessionService()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
        metrics=metrics,
    )
    adapter = FakeAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})

    inbound = InboundMessage(channel="fake",
                             chat_id="c1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m1",
                             text="hi")
    await queue.enqueue(TaskMessage.from_inbound("t_a", "fake", inbound))

    stream_worker = StreamWorker(queue=queue, worker=worker, registry=registry)
    assert await stream_worker.run_once(count=1, block=0) == 1
    assert adapter.replies == ["echo:hi"]
    assert _has_attributes(metrics, "agent_worker_task_total", outcome="success")
    assert _has_attributes(metrics, "agent_result_cache_total", outcome="miss")
    assert _has_attributes(metrics, "agent_im_delivery_total", outcome="success")

    # The message was acked, so a second read returns nothing.
    assert await stream_worker.run_once(count=1, block=0) == 0
    await queue.close()


async def test_stream_queue_uses_unique_consumers_and_can_heartbeat_pending_task():
    client = faioredis.FakeRedis(decode_responses=True)
    first = StreamQueue(client=client)
    second = StreamQueue(client=client)
    assert first.consumer_name != second.consumer_name

    await first.ensure_group()
    message_id = await first.enqueue(
        TaskMessage(
            tenant_id="t",
            channel="fake",
            inbound={
                "chat_id": "c",
                "sender_id": "u",
                "message_id": "m"
            },
        ))
    assert await first.read(count=1, block=0)
    assert await first.touch(message_id) is True
    assert await second.touch(message_id) is False
    pending = await client.xpending_range("agent:tasks", "agent-workers", message_id, message_id, 1)
    assert pending[0]["consumer"] == first.consumer_name


async def test_stream_queue_publishes_and_removes_worker_liveness():
    client = faioredis.FakeRedis(decode_responses=True)
    worker_queue = StreamQueue(client=client, consumer="worker-one")
    gateway_queue = StreamQueue(client=client, consumer="gateway")

    assert await gateway_queue.has_active_workers() is False
    await worker_queue.publish_worker_heartbeat(ttl_seconds=30)
    assert await gateway_queue.has_active_workers() is True
    await worker_queue.remove_worker_heartbeat()
    assert await gateway_queue.has_active_workers() is False


async def test_stream_worker_heartbeats_while_agent_turn_is_running():

    class HeartbeatQueue:

        def __init__(self):
            self.touches = 0
            self.acked = []

        async def touch(self, message_id):
            self.touches += 1
            return True

        async def ack(self, message_id):
            self.acked.append(message_id)

    class SlowWorker:

        def __init__(self):
            self.metrics = EnterpriseMetrics(meter=False)

        async def handle(self, tenant_id, channel, inbound):
            await asyncio.sleep(0.025)
            return ""

    queue = HeartbeatQueue()
    task = TaskMessage.from_inbound(
        "t_a",
        "fake",
        InboundMessage(
            channel="fake",
            chat_id="c",
            sender_id="u",
            message_id="heartbeat",
        ),
    )
    stream_worker = StreamWorker(
        queue=queue,
        worker=SlowWorker(),
        registry=object(),
        heartbeat_interval_ms=5,
    )

    assert await stream_worker._process("1-0", task.model_dump_json()) is True
    assert queue.touches >= 2
    assert queue.acked == ["1-0"]


async def test_stream_worker_stops_when_pending_task_ownership_is_lost():

    class LostQueue:

        def __init__(self):
            self.acked = []

        async def touch(self, message_id):
            return False

        async def ack(self, message_id):
            self.acked.append(message_id)

    class SlowWorker:

        def __init__(self):
            self.metrics = EnterpriseMetrics(meter=False)

        async def handle(self, tenant_id, channel, inbound):
            await asyncio.sleep(1)
            return "late"

    queue = LostQueue()
    worker = SlowWorker()
    task = TaskMessage.from_inbound(
        "t_a",
        "fake",
        InboundMessage(channel="fake", chat_id="c", sender_id="u", message_id="lost"),
    )
    stream_worker = StreamWorker(
        queue=queue,
        worker=worker,
        registry=object(),
        heartbeat_interval_ms=1,
    )

    assert await stream_worker._process("1-0", task.model_dump_json()) is False
    assert queue.acked == []
    assert _has_attributes(worker.metrics, "agent_worker_task_total", outcome="ownership_lost")


async def test_stream_worker_recovers_queue_connection_errors(monkeypatch):

    class RecoveryQueue:

        def __init__(self):
            self.ensure_calls = 0
            self.heartbeat_calls = 0
            self.remove_calls = 0

        async def ensure_group(self):
            self.ensure_calls += 1

        async def publish_worker_heartbeat(self, ttl_seconds):
            self.heartbeat_calls += 1

        async def remove_worker_heartbeat(self):
            self.remove_calls += 1

    class Worker:
        metrics = EnterpriseMetrics(meter=False)

    queue = RecoveryQueue()
    stream_worker = StreamWorker(
        queue=queue,
        worker=Worker(),
        registry=object(),
        reconnect_delay_seconds=0,
    )
    run_calls = 0

    async def run_once(*, count, block):
        nonlocal run_calls
        run_calls += 1
        if run_calls == 1:
            raise RedisConnectionError("redis unavailable")
        raise asyncio.CancelledError

    monkeypatch.setattr(stream_worker, "run_once", run_once)

    with pytest.raises(asyncio.CancelledError):
        await stream_worker.run(block=5000)

    assert queue.ensure_calls == 2
    assert queue.heartbeat_calls == 2
    assert queue.remove_calls == 1
    assert _has_attributes(Worker.metrics, "agent_worker_reconnect_total", error_type="ConnectionError")


async def test_failed_task_is_not_acked_and_is_reclaimed():
    client = faioredis.FakeRedis(decode_responses=True)
    metrics = EnterpriseMetrics(meter=False)
    queue = StreamQueue(client=client, metrics=metrics)
    await queue.ensure_group()
    manager = TenantConfigManager()
    _make_tenant(manager)
    shared = InMemorySessionService()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
        metrics=metrics,
    )
    adapter = FailOnceAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})
    inbound = InboundMessage(channel="fake",
                             chat_id="c1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m-retry",
                             text="hi")
    message_id = await queue.enqueue(TaskMessage.from_inbound("t_a", "fake", inbound))
    stream_worker = StreamWorker(
        queue=queue,
        worker=worker,
        registry=registry,
        min_idle_ms=0,
        max_attempts=3,
    )

    assert await stream_worker.run_once(count=1, block=0) == 1
    assert await queue.delivery_count(message_id) == 1
    assert await stream_worker.run_once(count=1, block=0) == 1
    assert adapter.replies == ["echo:hi"]
    assert await queue.delivery_count(message_id) == 0
    assert _has_attributes(metrics, "agent_worker_retry_total", error_type="RuntimeError")
    assert _has_attributes(metrics, "agent_result_cache_total", outcome="miss")
    assert _has_attributes(metrics, "agent_result_cache_total", outcome="hit")
    assert _has_attributes(metrics, "agent_im_delivery_total", outcome="error")
    assert _has_attributes(metrics, "agent_im_delivery_total", outcome="success")
    await queue.close()


async def test_poison_task_moves_to_dead_letter_stream():
    client = faioredis.FakeRedis(decode_responses=True)
    metrics = EnterpriseMetrics(meter=False)
    queue = StreamQueue(client=client, metrics=metrics)
    await queue.ensure_group()
    message_id = await client.xadd("agent:tasks", {"payload": "not-json"})
    stream_worker = StreamWorker(
        queue=queue,
        worker=object(),
        registry=object(),
        min_idle_ms=0,
        max_attempts=1,
        metrics=metrics,
    )

    assert await stream_worker.run_once(count=1, block=0) == 1
    assert await queue.delivery_count(message_id) == 0
    dead = await client.xrange("agent:tasks:dead")
    assert len(dead) == 1
    assert dead[0][1]["source_id"] == message_id
    assert _has_attributes(metrics, "agent_queue_dlq_total")
    assert _has_attributes(metrics, "agent_worker_task_total", outcome="dead_letter")
    await queue.close()
