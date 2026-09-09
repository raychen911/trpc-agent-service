"""Core V3 reliability tests.

Purpose: prove payload-aware idempotency, durable request state, same-session
serialization and reclaim contracts. All components are in-memory fakes; no
model fee, Redis, PostgreSQL or IM connection is used.
"""

import asyncio

import pytest

from trpc_service.gateway import IdempotencyConflictError
from trpc_service.gateway import InMemoryIdempotencyStore
from trpc_service.gateway import InMemoryRequestStore
from trpc_service.gateway import RequestRecord
from trpc_service.gateway import RequestState
from trpc_service.gateway import AgentRequest
from trpc_service.gateway import InMemoryAgentTaskQueue
from trpc_service.gateway import RequestRepairService
from trpc_service.gateway import InMemoryOrderingStore
from trpc_service.storage import InMemorySessionExecutionGuard

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_same_key_same_payload_reuses_request_and_conflict_is_rejected():
    store = InMemoryIdempotencyStore()
    first = await store.reserve("key", "r1", ttl_seconds=60, payload_hash="a")
    duplicate = await store.reserve("key", "r2", ttl_seconds=60, payload_hash="a")
    assert first.request_id == duplicate.request_id == "r1"
    with pytest.raises(IdempotencyConflictError):
        await store.reserve("key", "r3", ttl_seconds=60, payload_hash="b")


@pytest.mark.asyncio
async def test_request_state_and_attempts_are_observable():
    store = InMemoryRequestStore()
    await store.create(RequestRecord(request_id="r1", tenant_id="t1", state=RequestState.RESERVED))
    await store.transition("t1", "r1", RequestState.QUEUED)
    record = await store.transition("t1", "r1", RequestState.RUNNING, increment_attempts=True)
    assert record.state == RequestState.RUNNING
    assert record.attempts == 1
    assert record.model_attempts == 0
    assert record.successful_model_calls == 0
    assert record.recovery_count == 0


@pytest.mark.asyncio
async def test_twenty_same_session_entries_are_serialized():
    guard = InMemorySessionExecutionGuard()
    active = maximum = 0

    async def run():
        nonlocal active, maximum
        async with guard.hold("session", wait_timeout=1, lease_seconds=1) as lease:
            lease.assert_owned()
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1

    await asyncio.gather(*(run() for _ in range(20)))
    assert maximum == 1


@pytest.mark.asyncio
async def test_stale_request_repair_requeues_durable_payload():
    store = InMemoryRequestStore()
    queue = InMemoryAgentTaskQueue()
    request = AgentRequest(request_id="r2",
                           tenant_id="t1",
                           config_version=1,
                           app_id="app",
                           user_id="u",
                           session_id="s",
                           text="hello",
                           metadata={"admission_complete": True})
    await store.create(
        RequestRecord(request_id="r2",
                      tenant_id="t1",
                      state=RequestState.RESERVED,
                      request=request,
                      idempotency_key="key"))
    repaired = await RequestRepairService(store, queue).repair_stale(older_than_seconds=0)
    delivery = await queue.receive("worker", timeout_seconds=0.1)
    assert repaired == 1
    assert delivery is not None
    assert delivery.envelope.request.request_id == "r2"
    assert (await store.get("t1", "r2")).recovery_count == 1


@pytest.mark.asyncio
async def test_request_repair_does_not_duplicate_stream_owned_states():
    store = InMemoryRequestStore()
    queue = InMemoryAgentTaskQueue()
    for state in (RequestState.QUEUED, RequestState.RUNNING, RequestState.RETRYABLE_FAILED):
        request = AgentRequest(request_id=state.value,
                               tenant_id="t1",
                               config_version=1,
                               app_id="app",
                               user_id="u",
                               session_id="s",
                               text="hello",
                               metadata={"admission_complete": True})
        await store.create(
            RequestRecord(request_id=state.value,
                          tenant_id="t1",
                          state=state,
                          request=request,
                          idempotency_key=state.value))

    repaired = await RequestRepairService(store, queue).repair_stale(older_than_seconds=0)

    assert repaired == 0
    assert await queue.receive("worker", timeout_seconds=0.01) is None


@pytest.mark.asyncio
async def test_channel_ordering_marks_late_arrival_without_hiding_it():
    ordering = InMemoryOrderingStore()
    assert await ordering.observe("telegram:bot", 10) is False
    assert await ordering.observe("telegram:bot", 9) is True
    assert await ordering.observe("telegram:bot", 11) is False
