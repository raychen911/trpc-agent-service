# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest

from trpc_service.config import ChannelType
from trpc_service.gateway import AgentRequest
from trpc_service.gateway import AgentTaskEnvelope
from trpc_service.gateway import InMemoryAgentTaskQueue
from trpc_service.gateway import InMemoryIdempotencyStore
from trpc_service.gateway import InMemoryOutboxStore
from trpc_service.gateway import OutboundMessage
from trpc_service.gateway import OutboxState
from trpc_service.gateway.dispatcher import AgentTaskProcessor
from trpc_service.gateway.models import AgentStreamEvent
from trpc_service.gateway.models import StreamEventType
from trpc_service.gateway.service import GatewayService
from trpc_service.tenant import InMemoryTenantRegistry


@pytest.mark.asyncio
async def test_idempotency_returns_original_owner():
    store = InMemoryIdempotencyStore()
    first = await store.reserve("key", "r1", ttl_seconds=60)
    second = await store.reserve("key", "r2", ttl_seconds=60)
    assert first.request_id == second.request_id == "r1"
    await store.complete("key", "r1", ttl_seconds=60)
    assert (await store.reserve("key", "r3", ttl_seconds=60)).state == "completed"


@pytest.mark.asyncio
async def test_queue_retry_increments_attempts():
    queue = InMemoryAgentTaskQueue()
    request = AgentRequest(request_id="r",
                           tenant_id="t",
                           config_version=1,
                           app_id="a",
                           user_id="u",
                           session_id="s",
                           text="hi")
    await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key="key"))
    delivery = await queue.receive("worker", timeout_seconds=0.1)
    assert delivery is not None
    await queue.retry(delivery)
    retried = await queue.receive("worker", timeout_seconds=0.1)
    assert retried is not None and retried.envelope.attempts == 1
    await queue.ack(retried)


@pytest.mark.asyncio
async def test_outbox_is_idempotent_and_reaches_delivered():
    store = InMemoryOutboxStore()
    message = OutboundMessage(outbound_id="o",
                              request_id="r",
                              tenant_id="t",
                              binding_id="b",
                              channel=ChannelType.TELEGRAM,
                              external_conversation_id="c",
                              text="ok")
    assert await store.add(message)
    assert not await store.add(message)
    records = await store.claim()
    assert len(records) == 1
    await store.delivered("o")
    assert (await store.get("o")).state == OutboxState.DELIVERED


class FakeWorker:

    async def stream(self, request):
        yield AgentStreamEvent(request_id=request.request_id, sequence=0, type=StreamEventType.STARTED)
        yield AgentStreamEvent(request_id=request.request_id, sequence=1, type=StreamEventType.DELTA, text="reply")
        yield AgentStreamEvent(request_id=request.request_id, sequence=2, type=StreamEventType.COMPLETED)


@pytest.mark.asyncio
async def test_task_processor_commits_outbox_before_idempotency(tenant_config):
    idempotency = InMemoryIdempotencyStore()
    gateway = GatewayService(InMemoryTenantRegistry([tenant_config]), FakeWorker(), idempotency)
    queue = InMemoryAgentTaskQueue()
    outbox = InMemoryOutboxStore()
    request = AgentRequest(
        request_id="request-1",
        tenant_id="tenant-a",
        config_version=1,
        app_id="assistant",
        user_id="user",
        session_id="session",
        text="hello",
        channel=ChannelType.TELEGRAM,
        binding_id="binding",
        metadata={"external_conversation_id": "chat"},
    )
    await idempotency.reserve("dedupe", request.request_id, ttl_seconds=60)
    await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key="dedupe"))
    processor = AgentTaskProcessor(queue, gateway, outbox, consumer="worker")
    assert await processor.process_one(timeout_seconds=0.1)
    assert (await idempotency.reserve("dedupe", "other", ttl_seconds=60)).state == "completed"
    assert len(await outbox.claim()) == 1
