from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from tenant_agent.models import (
    MemoryRecord,
    OutboundMessage,
    SummaryRecord,
    UsageDelta,
)
from tenant_agent.storage.base import ConcurrentWriteError, OutboxItem
from tenant_agent.storage.memory import InMemoryPlane


@pytest.mark.asyncio
async def test_optimistic_revision_prevents_lost_update() -> None:
    plane = InMemoryPlane()
    snapshot = await plane.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )

    async def append(event_id: str) -> object:
        return await plane.append_event(
            snapshot=snapshot,
            event_id=event_id,
            kind="user_message",
            actor_id="user",
            payload={"text": event_id},
            state_delta={"last": event_id},
            trace_id="0" * 32,
        )

    results = await asyncio.gather(append("e1"), append("e2"), return_exceptions=True)
    assert sum(isinstance(item, ConcurrentWriteError) for item in results) == 1
    current = await plane.get_session("alpha", "session")
    assert current is not None
    assert current.revision == 1
    assert current.last_event_sequence == 1


@pytest.mark.asyncio
async def test_event_then_state_then_summary_order_is_enforced() -> None:
    plane = InMemoryPlane()
    snapshot = await plane.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    with pytest.raises(ConcurrentWriteError):
        await plane.put_summary(
            SummaryRecord(
                tenant_id="alpha",
                session_id="session",
                version=1,
                through_event_sequence=1,
                content="future",
            )
        )
    snapshot, event = await plane.append_event(
        snapshot=snapshot,
        event_id="e1",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"status": "received"},
        trace_id="0" * 32,
    )
    await plane.put_summary(
        SummaryRecord(
            tenant_id="alpha",
            session_id="session",
            version=1,
            through_event_sequence=event.sequence,
            content="hello",
        )
    )
    assert snapshot.state["status"] == "received"
    assert (await plane.get_summary("alpha", "session")).through_event_sequence == 1  # type: ignore[union-attr]
    with pytest.raises(ConcurrentWriteError, match="summary version"):
        await plane.put_summary(
            SummaryRecord(
                tenant_id="alpha",
                session_id="session",
                version=1,
                through_event_sequence=1,
                content="conflict",
            )
        )


@pytest.mark.asyncio
async def test_memory_is_visible_immediately_after_write() -> None:
    plane = InMemoryPlane()
    await plane.put_memory(
        MemoryRecord(
            memory_id="m1",
            tenant_id="alpha",
            user_id="u1",
            content="Alice prefers blue",
        )
    )
    rows = await plane.search_memory("alpha", "u1", "blue")
    assert [row.memory_id for row in rows] == ["m1"]
    assert await plane.search_memory("beta", "u1", "blue") == ()
    with pytest.raises(ConcurrentWriteError, match="memory revision"):
        await plane.put_memory(
            MemoryRecord(
                memory_id="m1",
                tenant_id="alpha",
                user_id="u1",
                content="conflicting value",
            )
        )


@pytest.mark.asyncio
async def test_receipt_and_outbox_complete_atomically_and_reclaim_expired_work() -> None:
    plane = InMemoryPlane()
    now = datetime.now(UTC)
    claim = await plane.claim_receipt(
        tenant_id="alpha",
        dedupe_key="d1",
        owner="worker-1",
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim.acquired
    reservation = await plane.reserve_usage(
        tenant_id="alpha",
        reservation_id="d1",
        period="2026-08",
        reserved_tokens=100,
        reserved_cost_usd=1.0,
        token_limit=1_000,
        cost_limit_usd=10.0,
        expires_at=now + timedelta(minutes=5),
    )
    assert reservation.acquired
    response = OutboundMessage(
        tenant_id="alpha",
        binding_id="web-binding-001",
        channel="web",
        external_chat_id="chat",
        text="done",
    )
    outbox = OutboxItem(
        outbox_id="o1",
        tenant_id="alpha",
        kind="im-delivery",
        payload={"message": response.model_dump(mode="json")},
        status="pending",
        attempts=0,
        available_at=now,
    )
    await plane.complete_receipt_with_outbox(
        tenant_id="alpha",
        dedupe_key="d1",
        owner="worker-1",
        response=(response,),
        items=(outbox,),
        usage_period="2026-08",
        usage_delta=UsageDelta(input_tokens=3, output_tokens=2, cost_usd=0.1),
        usage_reservation_id="d1",
    )
    assert (await plane.get_usage("alpha", "2026-08")).total_tokens == 5
    assert plane._usage_reservations == {}  # type: ignore[attr-defined]
    with pytest.raises(ConcurrentWriteError):
        await plane.fail_receipt(
            tenant_id="alpha",
            dedupe_key="d1",
            owner="worker-1",
            error_type="LateFailure",
        )
    duplicate = await plane.claim_receipt(
        tenant_id="alpha",
        dedupe_key="d1",
        owner="worker-2",
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert not duplicate.acquired
    assert duplicate.receipt.response[0].text == "done"

    claimed = await plane.claim_outbox("delivery-1", limit=10, now=now)
    assert claimed[0].outbox_id == "o1"
    reclaimed = await plane.claim_outbox("delivery-2", limit=10, now=now + timedelta(seconds=301))
    assert reclaimed[0].owner == "delivery-2"
    await plane.complete_outbox("o1", "delivery-2")

    dead_item = OutboxItem(
        outbox_id="dead-o1",
        tenant_id="alpha",
        kind="im-delivery",
        payload={"next_segment": 2},
        status="pending",
        attempts=0,
        available_at=now,
    )
    await plane.enqueue_outbox(dead_item)
    await plane.claim_outbox("delivery-dead", limit=1, now=now)
    await plane.retry_outbox(
        "dead-o1",
        "delivery-dead",
        error_type="PermanentDeliveryError",
        available_at=now,
        terminal=True,
    )
    assert [item.outbox_id for item in await plane.list_dead_outbox("alpha", limit=10)] == ["dead-o1"]
    assert await plane.list_dead_outbox("beta", limit=10) == ()
    requeued = await plane.requeue_dead_outbox("alpha", "dead-o1")
    assert requeued.status == "retry" and requeued.attempts == 0
    assert requeued.payload["next_segment"] == 2
    with pytest.raises(ConcurrentWriteError, match="only dead"):
        await plane.requeue_dead_outbox("alpha", "dead-o1")
    with pytest.raises(KeyError):
        await plane.requeue_dead_outbox("beta", "dead-o1")

    scoped_claim = await plane.claim_receipt(
        tenant_id="alpha",
        dedupe_key="scope-check",
        owner="worker-1",
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert scoped_claim.acquired
    with pytest.raises(ValueError, match="tenant scope"):
        await plane.complete_receipt(
            tenant_id="alpha",
            dedupe_key="scope-check",
            owner="worker-1",
            response=(response.model_copy(update={"tenant_id": "beta"}),),
        )
    assert await plane.prune_operational_records(
        before=now + timedelta(days=1),
        limit=10,
    ) == {"receipts": 1, "outbox": 1, "usage_reservations": 0}
    assert ("alpha", "scope-check") in plane._receipts  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_session_lease_serializes_turns() -> None:
    plane = InMemoryPlane()
    order: list[str] = []

    async def task(name: str) -> None:
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="s1",
            owner=name,
            wait_timeout=1,
            lease_seconds=1,
        ):
            order.append(f"{name}:start")
            await asyncio.sleep(0.01)
            order.append(f"{name}:end")

    await asyncio.gather(task("one"), task("two"))
    assert order in (
        ["one:start", "one:end", "two:start", "two:end"],
        ["two:start", "two:end", "one:start", "one:end"],
    )
    assert plane._session_locks == {}  # type: ignore[attr-defined]

    for index in range(100):
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id=f"ephemeral-{index}",
            owner="test",
            wait_timeout=1,
            lease_seconds=1,
        ):
            pass
    assert plane._session_locks == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_tenant_concurrency_slots_enforce_limit_and_expire() -> None:
    plane = InMemoryPlane()
    now = datetime.now(UTC)
    assert await plane.acquire_tenant_slot(
        tenant_id="alpha",
        owner="one",
        limit=1,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert not await plane.acquire_tenant_slot(
        tenant_id="alpha",
        owner="two",
        limit=1,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert await plane.acquire_tenant_slot(
        tenant_id="alpha",
        owner="one",
        limit=1,
        lease_expires_at=now + timedelta(seconds=60),
    )
    await plane.release_tenant_slot(tenant_id="alpha", owner="one")
    assert await plane.acquire_tenant_slot(
        tenant_id="alpha",
        owner="two",
        limit=1,
        lease_expires_at=now - timedelta(seconds=1),
    )
    assert await plane.acquire_tenant_slot(
        tenant_id="alpha",
        owner="three",
        limit=1,
        lease_expires_at=now + timedelta(seconds=30),
    )
