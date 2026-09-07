from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tenant_agent.models import (
    ArtifactRecord,
    AuditRecord,
    ConfigVersion,
    KnowledgeRecord,
    MemoryRecord,
    OutboundMessage,
    SummaryRecord,
    UsageDelta,
)
from tenant_agent.services.config import configuration_checksum
from tenant_agent.storage.base import (
    ConcurrentWriteError,
    OutboxItem,
    SessionLeaseTimeout,
    UsageReservationResult,
)
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_tenant


@pytest.mark.asyncio
async def test_sql_config_activation_and_binding_lookup(tmp_path: Path) -> None:
    plane = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'control.db').as_posix()}")
    await plane.initialize()
    assert await plane.healthcheck()
    async with plane.engine.connect() as connection:
        assert (await connection.exec_driver_sql("PRAGMA journal_mode")).scalar_one() == "wal"
        assert (await connection.exec_driver_sql("PRAGMA busy_timeout")).scalar_one() == 30_000
    tenant = make_tenant()
    version = ConfigVersion(
        tenant_id=tenant.tenant_id,
        revision=tenant.revision,
        config=tenant,
        status="draft",
        created_by="test",
        checksum_sha256=configuration_checksum(tenant),
    )
    await plane.save_config_version(version)
    await plane.activate_config(tenant.tenant_id, tenant.revision, datetime.now(UTC))

    active = await plane.get_active_tenant("alpha")
    routed = await plane.get_tenant_by_binding("web", tenant.channels[0].binding_id)
    assert active == tenant
    assert routed == tenant
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
    await plane.release_tenant_slot(tenant_id="alpha", owner="one")
    await plane.close()


@pytest.mark.asyncio
async def test_two_sql_adapter_instances_serialize_same_session(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{(tmp_path / 'shared.db').as_posix()}"
    first = SqlPlane(url)
    second = SqlPlane(url)
    await first.initialize()
    await second.initialize()
    snapshot = await first.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    order: list[str] = []
    first_entered = asyncio.Event()
    release_first = asyncio.Event()

    async def locked(plane: SqlPlane, owner: str) -> None:
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="session",
            owner=owner,
            wait_timeout=2,
            lease_seconds=0.15,
        ):
            order.append(f"{owner}:start")
            if owner == "one":
                first_entered.set()
                await release_first.wait()
            order.append(f"{owner}:end")

    first_task = asyncio.create_task(locked(first, "one"))
    await first_entered.wait()
    second_task = asyncio.create_task(locked(second, "two"))
    await asyncio.sleep(0.35)
    assert order == ["one:start"]
    release_first.set()
    await asyncio.gather(first_task, second_task)
    assert order == ["one:start", "one:end", "two:start", "two:end"]

    updated, _ = await first.append_event(
        snapshot=snapshot,
        event_id="event-1",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"status": "ok"},
        trace_id="0" * 32,
    )
    with pytest.raises(ConcurrentWriteError):
        await second.append_event(
            snapshot=snapshot,
            event_id="event-2",
            kind="user_message",
            actor_id="user",
            payload={"text": "stale"},
            state_delta={},
            trace_id="0" * 32,
        )
    assert updated.revision == 1
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_sql_receipt_and_outbox_transaction(tmp_path: Path) -> None:
    plane = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'receipt.db').as_posix()}")
    await plane.initialize()
    now = datetime.now(UTC)
    claim = await plane.claim_receipt(
        tenant_id="alpha",
        dedupe_key="dedupe",
        owner="owner",
        lease_expires_at=now - timedelta(seconds=1),
    )
    assert claim.acquired
    reservation = await plane.reserve_usage(
        tenant_id="alpha",
        reservation_id="dedupe",
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
        text="reply",
    )
    item = OutboxItem(
        outbox_id="outbox",
        tenant_id="alpha",
        kind="im-delivery",
        payload={"message": response.model_dump(mode="json")},
        status="pending",
        attempts=0,
        available_at=now,
    )
    await plane.complete_receipt_with_outbox(
        tenant_id="alpha",
        dedupe_key="dedupe",
        owner="owner",
        response=(response,),
        items=(item,),
        usage_period="2026-08",
        usage_delta=UsageDelta(input_tokens=3, output_tokens=2, cost_usd=0.1),
        usage_reservation_id="dedupe",
    )
    assert (await plane.get_usage("alpha", "2026-08")).total_tokens == 5
    with pytest.raises(ConcurrentWriteError):
        await plane.fail_receipt(
            tenant_id="alpha",
            dedupe_key="dedupe",
            owner="owner",
            error_type="LateFailure",
        )
    assert not (
        await plane.claim_receipt(
            tenant_id="alpha",
            dedupe_key="dedupe",
            owner="other",
            lease_expires_at=now + timedelta(seconds=30),
        )
    ).acquired
    completed_replay = await plane.claim_receipt(
        tenant_id="alpha",
        dedupe_key="dedupe",
        owner="late-replay",
        lease_expires_at=now + timedelta(minutes=10),
    )
    assert not completed_replay.acquired
    assert completed_replay.receipt.status.value == "completed"
    assert completed_replay.receipt.response[0].text == "reply"
    claimed = await plane.claim_outbox("delivery", limit=5, now=now)
    assert [row.outbox_id for row in claimed] == ["outbox"]
    checkpoint = dict(claimed[0].payload)
    checkpoint["next_segment"] = 1
    await plane.checkpoint_outbox(
        "outbox",
        "delivery",
        payload=checkpoint,
    )
    await plane.retry_outbox(
        "outbox",
        "delivery",
        error_type="Temporary",
        available_at=now,
        terminal=False,
    )
    resumed = await plane.claim_outbox("delivery-2", limit=5, now=now)
    assert resumed[0].payload["next_segment"] == 1
    await plane.complete_outbox("outbox", "delivery-2")

    dead_item = OutboxItem(
        outbox_id="dead-outbox",
        tenant_id="alpha",
        kind="im-delivery",
        payload={"next_segment": 3},
        status="pending",
        attempts=0,
        available_at=now,
    )
    await plane.enqueue_outbox(dead_item)
    assert (await plane.claim_outbox("dead-owner", limit=1, now=now))[0].outbox_id == "dead-outbox"
    await plane.retry_outbox(
        "dead-outbox",
        "dead-owner",
        error_type="PermanentDeliveryError",
        available_at=now,
        terminal=True,
    )
    assert [item.outbox_id for item in await plane.list_dead_outbox("alpha", limit=10)] == ["dead-outbox"]
    assert await plane.list_dead_outbox("beta", limit=10) == ()
    requeued = await plane.requeue_dead_outbox("alpha", "dead-outbox")
    assert requeued.status == "retry" and requeued.attempts == 0
    assert requeued.payload["next_segment"] == 3
    with pytest.raises(ConcurrentWriteError, match="only dead"):
        await plane.requeue_dead_outbox("alpha", "dead-outbox")
    with pytest.raises(KeyError):
        await plane.requeue_dead_outbox("beta", "dead-outbox")
    assert await plane.prune_operational_records(
        before=now + timedelta(days=1),
        limit=10,
    ) == {"receipts": 1, "outbox": 1, "usage_reservations": 0}
    await plane.close()


@pytest.mark.asyncio
async def test_two_sql_adapters_cannot_claim_the_same_outbox_item(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{(tmp_path / 'outbox-claim.db').as_posix()}"
    first = SqlPlane(url)
    second = SqlPlane(url)
    await first.initialize()
    await second.initialize()
    now = datetime.now(UTC)
    await first.enqueue_outbox(
        OutboxItem(
            outbox_id="single-claim",
            tenant_id="alpha",
            kind="im-delivery",
            payload={},
            status="pending",
            attempts=0,
            available_at=now,
        )
    )

    first_claim, second_claim = await asyncio.gather(
        first.claim_outbox("one", limit=1, now=now),
        second.claim_outbox("two", limit=1, now=now),
    )
    assert len(first_claim) + len(second_claim) == 1
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_two_sql_adapters_cannot_overreserve_one_tenant_budget(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{(tmp_path / 'usage-reservation.db').as_posix()}"
    first = SqlPlane(url)
    second = SqlPlane(url)
    await first.initialize()
    await second.initialize()
    now = datetime.now(UTC)

    async def reserve(plane: SqlPlane, reservation_id: str) -> UsageReservationResult:
        return await plane.reserve_usage(
            tenant_id="alpha",
            reservation_id=reservation_id,
            period="2026-08",
            reserved_tokens=60,
            reserved_cost_usd=0.6,
            token_limit=100,
            cost_limit_usd=1.0,
            expires_at=now + timedelta(minutes=5),
        )

    one, two = await asyncio.gather(reserve(first, "one"), reserve(second, "two"))
    assert sum(item.acquired for item in (one, two)) == 1
    rejected = two if one.acquired else one
    assert rejected.reason in {"monthly_token_budget", "monthly_cost_budget"}
    winner = "one" if one.acquired else "two"
    await first.release_usage_reservation("alpha", winner)
    assert (
        await second.reserve_usage(
            tenant_id="alpha",
            reservation_id="after-release",
            period="2026-08",
            reserved_tokens=60,
            reserved_cost_usd=0.6,
            token_limit=100,
            cost_limit_usd=1.0,
            expires_at=now + timedelta(minutes=5),
        )
    ).acquired
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_sql_all_resource_crud_and_failure_paths(tmp_path: Path) -> None:
    plane = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'all.db').as_posix()}")
    await plane.initialize()
    assert await plane.get_session("alpha", "missing") is None
    session = await plane.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    session, first_event = await plane.append_event(
        snapshot=session,
        event_id="event-1",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"step": 1},
        trace_id="1" * 32,
    )
    duplicate_session, duplicate_event = await plane.append_event(
        snapshot=session.model_copy(update={"revision": 0}),
        event_id="event-1",
        kind="user_message",
        actor_id="user",
        payload={},
        state_delta={},
        trace_id="1" * 32,
    )
    assert duplicate_event == first_event
    assert duplicate_session.revision == 1
    assert await plane.list_events("alpha", "session", after_sequence=1) == ()
    assert [item.session_id async for item in plane.iter_sessions("alpha")] == ["session"]

    summary = SummaryRecord(
        tenant_id="alpha",
        session_id="session",
        version=1,
        through_event_sequence=1,
        content="hello summary",
    )
    with pytest.raises(ConcurrentWriteError, match="cannot cover events"):
        await plane.put_summary(summary.model_copy(update={"through_event_sequence": 2}))
    await plane.put_summary(summary)
    await plane.put_summary(summary.model_copy(update={"version": 0, "content": "stale"}))
    assert await plane.get_summary("alpha", "session") == summary
    assert [item.version async for item in plane.iter_summaries("alpha")] == [1]
    with pytest.raises(ConcurrentWriteError, match="summary version"):
        await plane.put_summary(summary.model_copy(update={"content": "conflict"}))

    memory = MemoryRecord(
        memory_id="memory",
        tenant_id="alpha",
        user_id="user",
        content="Alice likes blue",
    )
    await plane.put_memory(memory)
    newer_memory = memory.model_copy(update={"revision": 2, "content": "Alice likes green"})
    await plane.put_memory(newer_memory)
    await plane.put_memory(memory.model_copy(update={"content": "stale"}))
    assert [item.content for item in await plane.search_memory("alpha", "user", "green")] == [
        "Alice likes green"
    ]
    wildcard_memory = MemoryRecord(
        memory_id="wildcard",
        tenant_id="alpha",
        user_id="user",
        content="Completion is 100%_verified",
    )
    await plane.put_memory(wildcard_memory)
    await plane.put_memory(
        wildcard_memory.model_copy(update={"tenant_id": "beta", "content": "Beta tenant only"})
    )
    assert [item.memory_id for item in await plane.search_memory("alpha", "user", "100%_verified")] == [
        "wildcard"
    ]
    assert [item.memory_id for item in await plane.search_memory("alpha", "user", "%")] == ["wildcard"]
    assert await plane.search_memory("alpha", "user", "green%") == ()
    assert [item.memory_id for item in await plane.search_memory("beta", "user", "Beta")] == ["wildcard"]
    assert await plane.search_memory("alpha", "user", "Beta") == ()
    assert sorted([item.revision async for item in plane.iter_memories("alpha")]) == [1, 2]
    with pytest.raises(ConcurrentWriteError, match="memory revision"):
        await plane.put_memory(newer_memory.model_copy(update={"content": "conflict"}))

    content = b"sql artifact"
    artifact = ArtifactRecord(
        tenant_id="alpha",
        session_id="session",
        artifact_id="artifact",
        filename="artifact.txt",
        content_type="text/plain",
        size_bytes=len(content),
        checksum_sha256=hashlib.sha256(content).hexdigest(),
        storage_uri="sql://artifact",
    )
    await plane.put_artifact(artifact, content)
    await plane.put_artifact(artifact, content)
    assert await plane.get_artifact("alpha", "artifact") == (artifact, content)
    assert [item.artifact_id async for item in plane.iter_artifacts("alpha")] == ["artifact"]
    with pytest.raises(ValueError):
        await plane.put_artifact(
            artifact.model_copy(update={"artifact_id": "bad", "checksum_sha256": "0" * 64}),
            content,
        )
    conflicting_content = b"changed sql artifact"
    with pytest.raises(ConcurrentWriteError, match="artifact version"):
        await plane.put_artifact(
            artifact.model_copy(
                update={
                    "size_bytes": len(conflicting_content),
                    "checksum_sha256": hashlib.sha256(conflicting_content).hexdigest(),
                }
            ),
            conflicting_content,
        )

    first_chunk = KnowledgeRecord(
        tenant_id="alpha",
        document_id="doc",
        chunk_id="one",
        text="one",
        embedding=(1.0, 0.0),
        metadata={"kind": "a"},
    )
    second_chunk = KnowledgeRecord(
        tenant_id="alpha",
        document_id="doc",
        chunk_id="two",
        text="two",
        embedding=(0.0, 1.0),
        metadata={"kind": "b"},
    )
    await plane.put_knowledge(first_chunk)
    await plane.put_knowledge(second_chunk)
    updated_chunk = first_chunk.model_copy(update={"text": "updated"})
    await plane.put_knowledge(updated_chunk)
    matches = await plane.search_knowledge("alpha", (1.0, 0.0), limit=2, metadata_filter={"kind": "a"})
    assert [item.text for item in matches] == ["updated"]
    assert {item.chunk_id async for item in plane.iter_knowledge("alpha")} == {"one", "two"}

    audit = AuditRecord(
        audit_id="audit",
        tenant_id="alpha",
        channel="web",
        user_id="user",
        session_id="session",
        agent_name="agent",
        decision="allowed",
        latency_ms=1,
        trace_id="2" * 32,
    )
    await plane.append_audit(audit)
    await plane.append_audit(audit)
    assert await plane.query_audit("alpha", limit=1) == (audit,)
    assert await plane.query_audit("alpha", before=audit.occurred_at) == ()

    assert (await plane.get_usage("alpha", "2026-08")).total_tokens == 0
    usage = await plane.add_usage(
        "alpha", "2026-08", UsageDelta(input_tokens=3, output_tokens=2, cost_usd=0.1)
    )
    assert usage.total_tokens == 5
    assert (await plane.get_usage("alpha", "2026-08")).cost_usd == pytest.approx(0.1)

    now = datetime.now(UTC)
    retry_item = OutboxItem(
        outbox_id="retry",
        tenant_id="alpha",
        kind="im-delivery",
        payload={},
        status="pending",
        attempts=0,
        available_at=now,
    )
    await plane.enqueue_outbox(retry_item)
    await plane.enqueue_outbox(retry_item)
    await plane.claim_outbox("owner", limit=1, now=now)
    await plane.retry_outbox(
        "retry",
        "owner",
        error_type="Temporary",
        available_at=now + timedelta(seconds=1),
        terminal=False,
    )
    assert await plane.claim_outbox("other", limit=1, now=now) == ()
    reclaimed = await plane.claim_outbox("other", limit=1, now=now + timedelta(seconds=2))
    await plane.complete_outbox(reclaimed[0].outbox_id, "other")
    with pytest.raises(ConcurrentWriteError):
        await plane.complete_outbox("retry", "wrong")

    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> None:
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="locked",
            owner="holder",
            wait_timeout=1,
            lease_seconds=2,
        ):
            entered.set()
            await release.wait()

    holder = asyncio.create_task(hold())
    await entered.wait()
    with pytest.raises(SessionLeaseTimeout):
        async with plane.acquire_session(
            tenant_id="alpha",
            session_id="locked",
            owner="waiter",
            wait_timeout=0.01,
            lease_seconds=1,
        ):
            pass
    release.set()
    await holder
    await plane.close()
