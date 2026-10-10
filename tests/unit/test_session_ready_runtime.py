from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import envelope, repository
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.mailbox import InMemorySessionMailboxStore
from trpc_service.storage.models import StoredEvent, TurnCommit
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict


async def _runtime_repository():
    value = repository()
    runtime = TenantRuntime(value, routing_key=b"p" * 32)
    first = await runtime.accept("binding-unpredictable-a", envelope("v1"))
    return value, first


@pytest.mark.asyncio
async def test_v1_and_v2_acceptance_paths_are_mutually_exclusive() -> None:
    repo, first = await _runtime_repository()
    assert await repo.mailbox.get("tenant-a", first.context.session_id) is None
    second_envelope = envelope("v2")
    second = await repo.accept_inbound_v2(
        context=first.context,
        envelope=second_envelope,
        trace_headers={"traceparent": "trace"},
    )
    assert not second.duplicate
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None and mailbox.accepted_sequence == 1
    assert len(repo.mailbox.outbox) == 1
    duplicate = await repo.accept_inbound_v2(
        context=first.context,
        envelope=second_envelope,
        trace_headers={},
    )
    assert duplicate.duplicate
    assert len(repo.mailbox.outbox) == 1


@pytest.mark.asyncio
async def test_claim_returns_executable_turn_and_only_one_active_claim() -> None:
    repo, first = await _runtime_repository()
    accepted = await repo.accept_inbound_v2(
        context=first.context,
        envelope=envelope("claim"),
        trace_headers={},
    )
    claim = await repo.claim_session_ready(
        "tenant-a",
        first.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
    )
    assert claim.claimed
    assert claim.acceptance is not None and claim.acceptance.inbound_id == accepted.inbound_id
    assert claim.execution_lease is not None
    assert claim.execution_lease.snapshot.session_id == first.context.session_id
    running = await repo.claim_session_ready(
        "tenant-a",
        first.context.session_id,
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
    )
    assert running.status.value == "RUNNING"
    assert running.execution_lease is None


@pytest.mark.asyncio
async def test_commit_backlog_emits_only_next_generation_and_retry_wait_has_no_wakeup() -> None:
    repo, first = await _runtime_repository()
    first_v2 = await repo.accept_inbound_v2(
        context=first.context, envelope=envelope("backlog-1"), trace_headers={}
    )
    await repo.accept_inbound_v2(
        context=first.context, envelope=envelope("backlog-2"), trace_headers={}
    )
    claim = await repo.claim_session_ready(
        "tenant-a", first.context.session_id, owner_id="worker", lease_for=timedelta(seconds=30)
    )
    assert claim.execution_lease is not None
    await repo.commit_session_ready(
        TurnCommit(
            context=first_v2.context,
            lease=claim.execution_lease,
            state={},
            events=(StoredEvent(event_id="event-1", author="agent", timestamp=1, event={}),),
        )
    )
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None and mailbox.status.value == "QUEUED"
    assert mailbox.queue_generation == 2
    assert len(repo.mailbox.outbox) == 2

    next_claim = await repo.claim_session_ready(
        "tenant-a", first.context.session_id, owner_id="worker", lease_for=timedelta(seconds=30)
    )
    assert next_claim.execution_lease is not None
    await repo.retry_session_ready(
        next_claim.execution_lease, error_type="timeout", delay=timedelta(seconds=10)
    )
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None and mailbox.status.value == "RETRY_WAIT"
    assert len(repo.mailbox.outbox) == 2


@pytest.mark.asyncio
async def test_permanent_failure_unblocks_next_item_and_stale_epoch_is_rejected() -> None:
    repo, first = await _runtime_repository()
    await repo.accept_inbound_v2(
        context=first.context, envelope=envelope("poison"), trace_headers={}
    )
    await repo.accept_inbound_v2(
        context=first.context, envelope=envelope("after"), trace_headers={}
    )
    claim = await repo.claim_session_ready(
        "tenant-a", first.context.session_id, owner_id="worker", lease_for=timedelta(seconds=30)
    )
    assert claim.execution_lease is not None
    stale = claim.execution_lease.model_copy(
        update={"fencing_token": claim.execution_lease.fencing_token + 1}
    )
    with pytest.raises(FencingConflict):
        await repo.commit_session_ready(
            TurnCommit(context=first.context, lease=stale, state={}, events=())
        )
    await repo.fail_session_ready(claim.execution_lease, error_type="poison")
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None and mailbox.status.value == "QUEUED"
    assert mailbox.resolved_sequence == 1 and mailbox.queue_generation == 2
    assert len(repo.mailbox.outbox) == 2


@pytest.mark.asyncio
async def test_postgres_v2_accept_uses_sequential_mailbox_writes_and_no_v1_event() -> None:
    from tests.unit.test_postgres_repository import Connection, Pool, inbound_row

    memory, first = await _runtime_repository()
    persisted = inbound_row(first)
    now = first.accepted_at
    mailbox = {
        "tenant_id": "tenant-a",
        "session_id": first.context.session_id,
        "status": "IDLE",
        "accepted_sequence": 0,
        "resolved_sequence": 0,
        "processing_sequence": None,
        "processing_inbound_id": None,
        "queue_generation": 0,
        "lease_owner": None,
        "lease_epoch": 0,
        "lease_expires_at": None,
        "retry_count": 0,
        "attempt": 0,
        "priority": 0,
        "retry_at": None,
        "updated_at": now,
    }
    item = {
        "tenant_id": "tenant-a",
        "session_id": first.context.session_id,
        "sequence": 1,
        "inbound_id": first.inbound_id,
        "trace_id": first.context.trace_id,
        "priority": 0,
        "retry_at": None,
    }
    changed = {**mailbox, "status": "QUEUED", "accepted_sequence": 1, "queue_generation": 1}
    connection = Connection(
        fetchrows=[persisted, mailbox, item, changed],
        fetchvals=[now],
    )
    await PostgresRuntimeRepository(Pool(connection)).accept_inbound_v2(
        context=first.context,
        envelope=envelope("pg-v2"),
        trace_headers={"traceparent": "trace"},
    )
    statements = [args[0] for kind, args in connection.calls]
    assert any("INSERT INTO session_mailboxes" in statement for statement in statements)
    assert any("session.ready.v2" in statement for statement in statements)
    assert not any("inbound.accepted" in statement for statement in statements)
    assert "$2::text" in next(
        statement for statement in statements if "session_mailbox_items" in statement
    )
    del memory


@pytest.mark.asyncio
async def test_reconcile_replays_lost_ready_without_generation_churn() -> None:
    store = InMemorySessionMailboxStore()
    base = datetime(2026, 8, 21, tzinfo=UTC)
    await store.accept(
        "tenant-a",
        "session-a",
        "inbound-a",
        now=base,
        trace_id="trace-a",
    )
    assert len(store.outbox) == 1
    reconciled = await store.reconcile("tenant-a", "session-a", now=base + timedelta(seconds=6))
    assert reconciled is not None
    assert reconciled.queue_generation == 1
    assert len(store.outbox) == 1


@pytest.mark.asyncio
async def test_committed_redelivery_self_heals_unresolved_mailbox_item() -> None:
    repo, first = await _runtime_repository()
    accepted = await repo.accept_inbound_v2(
        context=first.context, envelope=envelope("committed-redelivery"), trace_headers={}
    )
    repo._committed_inbound.add(("tenant-a", accepted.inbound_id))
    claim = await repo.claim_session_ready(
        "tenant-a", first.context.session_id, owner_id="worker", lease_for=timedelta(seconds=30)
    )
    assert claim.status.value == "EMPTY"
    assert claim.acceptance is not None and claim.acceptance.duplicate
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None and mailbox.status.value == "IDLE"
