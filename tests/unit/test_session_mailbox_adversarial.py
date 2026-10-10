from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.conftest import envelope, repository
from tests.unit.test_postgres_repository import Connection, Pool, inbound_row
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.mailbox import (
    InMemorySessionMailboxStore,
    PostgresSessionMailboxStore,
)
from trpc_service.storage.models import MailboxStatus
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict

BASE = datetime(2026, 8, 23, tzinfo=UTC)


def _mailbox_row(
    accepted,
    *,
    status: str,
    retry_at: datetime | None,
    accepted_sequence: int = 1,
    resolved_sequence: int = 0,
    queue_generation: int = 1,
) -> dict[str, object]:
    return {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "status": status,
        "accepted_sequence": accepted_sequence,
        "resolved_sequence": resolved_sequence,
        "processing_sequence": None,
        "processing_inbound_id": None,
        "queue_generation": queue_generation,
        "lease_owner": None,
        "lease_epoch": 0,
        "lease_expires_at": None,
        "retry_count": 0,
        "attempt": 0,
        "priority": 0,
        "retry_at": retry_at,
        "updated_at": BASE,
    }


@pytest.mark.asyncio
async def test_accept_and_expired_recovery_race_keeps_every_sequence_runnable() -> None:
    """Accepting behind an expired turn must not lose the recovery wake-up."""

    store = InMemorySessionMailboxStore()
    await store.accept("tenant-a", "session-a", "inbound-1", now=BASE)
    first = await store.claim(
        "tenant-a",
        "session-a",
        owner_id="old-worker",
        lease_for=timedelta(seconds=1),
        now=BASE,
    )
    assert first is not None
    await store.accept("tenant-a", "session-a", "inbound-2", now=BASE + timedelta(seconds=1))

    await asyncio.gather(
        store.recover("tenant-a", "session-a", now=BASE + timedelta(seconds=2)),
        store.accept("tenant-a", "session-a", "inbound-3", now=BASE + timedelta(seconds=2)),
    )

    processed: list[int] = []
    for index in range(1, 4):
        lease = await store.claim(
            "tenant-a",
            "session-a",
            owner_id=f"worker-{index}",
            lease_for=timedelta(seconds=30),
            now=BASE + timedelta(seconds=3 + index),
        )
        assert lease is not None
        processed.append(lease.sequence)
        await store.commit(lease, now=BASE + timedelta(seconds=3 + index))

    assert processed == [1, 2, 3]
    mailbox = await store.get("tenant-a", "session-a")
    assert mailbox is not None
    assert mailbox.accepted_sequence == mailbox.resolved_sequence == 3
    assert mailbox.status == MailboxStatus.IDLE
    assert mailbox.processing_sequence is None

    # Recovery emits exactly one new wake-up for the expired head.  The
    # concurrent acceptance may append work, but must not emit a second head
    # wake-up while the session is already queued.
    assert len(store.outbox) == 4


@pytest.mark.asyncio
async def test_expired_takeover_fences_old_epoch_even_after_new_message_arrives() -> None:
    store = InMemorySessionMailboxStore()
    await store.accept("tenant-a", "session-a", "inbound-1", now=BASE)
    old = await store.claim(
        "tenant-a", "session-a", owner_id="worker-a", lease_for=timedelta(seconds=1), now=BASE
    )
    assert old is not None
    await store.accept("tenant-a", "session-a", "inbound-2", now=BASE + timedelta(seconds=1))
    replacement = await store.claim(
        "tenant-a",
        "session-a",
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
        now=BASE + timedelta(seconds=2),
    )
    assert replacement is not None
    assert replacement.sequence == 1
    assert replacement.epoch == old.epoch + 1
    with pytest.raises(FencingConflict):
        await store.commit(old, now=BASE + timedelta(seconds=2))
    await store.commit(replacement, now=BASE + timedelta(seconds=2))


@pytest.mark.asyncio
async def test_new_message_does_not_unlock_a_future_retry_wait_head() -> None:
    store = InMemorySessionMailboxStore()
    retry_at = BASE + timedelta(seconds=30)
    waiting = await store.accept("tenant-a", "session-a", "inbound-1", retry_at=retry_at, now=BASE)
    assert waiting.status == MailboxStatus.RETRY_WAIT
    before_outbox = len(store.outbox)

    still_waiting = await store.accept(
        "tenant-a", "session-a", "inbound-2", now=BASE + timedelta(seconds=1)
    )
    assert still_waiting.status == MailboxStatus.RETRY_WAIT
    assert still_waiting.retry_at == retry_at
    assert len(store.outbox) == before_outbox

    ready = await store.accept(
        "tenant-a", "session-a", "inbound-3", now=BASE + timedelta(seconds=31)
    )
    assert ready.status == MailboxStatus.QUEUED
    assert ready.retry_at is None
    assert len(store.outbox) == before_outbox + 1


@pytest.mark.asyncio
async def test_terminal_failure_resolves_head_and_wakes_next_item() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"adversarial-terminal" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    first = await runtime.accept("binding-unpredictable-a", envelope("terminal-head"))
    await repo.accept_inbound_v2(
        context=first.context,
        envelope=envelope("terminal-next"),
        trace_headers={},
    )
    mailbox = await repo.mailbox.get(first.context.tenant_id, first.context.session_id)
    assert mailbox is not None
    claim = await repo.claim_session_ready(
        first.context.tenant_id,
        first.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None

    await repo.fail_session_ready(claim.execution_lease, error_type="permanent")
    failed = await repo.mailbox.get(first.context.tenant_id, first.context.session_id)
    assert failed is not None
    assert failed.resolved_sequence == 1
    assert failed.status == MailboxStatus.QUEUED
    assert len(repo.mailbox.outbox) == 2

    next_claim = await repo.claim_session_ready(
        first.context.tenant_id,
        first.context.session_id,
        owner_id="worker-2",
        lease_for=timedelta(seconds=30),
        expected_generation=failed.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert next_claim.execution_lease is not None
    assert next_claim.execution_lease.inbound_id != claim.execution_lease.inbound_id


@pytest.mark.asyncio
async def test_postgres_mailbox_store_due_retry_wait_reemits_ready() -> None:
    accepted = await _acceptance_for_postgres()
    retry_at = BASE - timedelta(seconds=1)
    mailbox = _mailbox_row(
        accepted,
        status=MailboxStatus.RETRY_WAIT.value,
        retry_at=retry_at,
        queue_generation=4,
    )
    changed = dict(
        mailbox,
        accepted_sequence=2,
        status=MailboxStatus.QUEUED.value,
        queue_generation=5,
        retry_at=None,
    )
    connection = Connection(
        fetchrows=[mailbox, None, changed],
        fetchvals=[BASE],
    )
    result = await PostgresSessionMailboxStore(Pool(connection)).accept(
        accepted.context.tenant_id,
        accepted.context.session_id,
        str(uuid4()),
        retry_at=BASE + timedelta(seconds=30),
        now=BASE,
    )
    assert result.status == MailboxStatus.QUEUED
    assert result.queue_generation == 5
    assert any(
        "session.ready.v2" in args[0] for kind, args in connection.calls if kind == "execute"
    )


@pytest.mark.asyncio
async def test_postgres_runtime_due_retry_wait_reemits_ready() -> None:
    accepted = await _acceptance_for_postgres()
    retry_at = BASE - timedelta(seconds=1)
    mailbox = _mailbox_row(
        accepted,
        status=MailboxStatus.RETRY_WAIT.value,
        retry_at=retry_at,
        queue_generation=7,
    )
    inbound = dict(inbound_row(accepted), inbound_id=uuid4())
    item = {"priority": 0, "trace_id": accepted.context.trace_id}
    changed = dict(
        mailbox,
        accepted_sequence=2,
        status=MailboxStatus.QUEUED.value,
        queue_generation=8,
        retry_at=None,
    )
    connection = Connection(
        fetchrows=[inbound, mailbox, item, changed],
        fetchvals=[BASE],
    )
    result = await PostgresRuntimeRepository(Pool(connection)).accept_inbound_v2(
        context=accepted.context,
        envelope=accepted.envelope,
        trace_headers={},
        retry_at=BASE + timedelta(seconds=30),
    )
    assert not result.duplicate
    update = next(
        args
        for kind, args in connection.calls
        if kind == "fetchrow" and "UPDATE session_mailboxes" in args[0]
    )
    assert update[3:6] == (MailboxStatus.QUEUED.value, None, 1)
    assert any(
        "session.ready.v2" in args[0] for kind, args in connection.calls if kind == "execute"
    )


async def _acceptance_for_postgres():
    from tests.unit.test_postgres_repository import acceptance

    return await acceptance()
