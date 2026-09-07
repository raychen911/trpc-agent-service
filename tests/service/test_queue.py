# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tests for the gateway→worker Redis Streams queue (using fakeredis)."""

from __future__ import annotations

import json

import fakeredis.aioredis as faioredis
from fastapi.testclient import TestClient

from trpc_agent_sdk.agents import BaseAgent
from trpc_service import ChannelRegistry
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
    queue = StreamQueue(client=client)
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
    await queue.close()


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
    # The worker must NOT have run in-process (decoupled mode).
    assert adapter.replies == []


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


# ------------------------------------------------------------ StreamWorker consume


async def test_stream_worker_processes_and_acks():
    client = faioredis.FakeRedis(decode_responses=True)
    queue = StreamQueue(client=client)
    await queue.ensure_group()

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

    # The message was acked, so a second read returns nothing.
    assert await stream_worker.run_once(count=1, block=0) == 0
    await queue.close()


async def test_failed_task_is_not_acked_and_is_reclaimed():
    client = faioredis.FakeRedis(decode_responses=True)
    queue = StreamQueue(client=client)
    await queue.ensure_group()
    manager = TenantConfigManager()
    _make_tenant(manager)
    shared = InMemorySessionService()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
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
    await queue.close()


async def test_poison_task_moves_to_dead_letter_stream():
    client = faioredis.FakeRedis(decode_responses=True)
    queue = StreamQueue(client=client)
    await queue.ensure_group()
    message_id = await client.xadd("agent:tasks", {"payload": "not-json"})
    stream_worker = StreamWorker(
        queue=queue,
        worker=object(),
        registry=object(),
        min_idle_ms=0,
        max_attempts=1,
    )

    assert await stream_worker.run_once(count=1, block=0) == 1
    assert await queue.delivery_count(message_id) == 0
    dead = await client.xrange("agent:tasks:dead")
    assert len(dead) == 1
    assert dead[0][1]["source_id"] == message_id
    await queue.close()
