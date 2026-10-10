from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.unit.test_postgres_repository import Connection, Pool
from trpc_service.storage.mailbox import InMemorySessionMailboxStore, PostgresSessionMailboxStore
from trpc_service.storage.models import MailboxClaimStatus, MailboxLease, MailboxStatus
from trpc_service.storage.protocols import FencingConflict

BASE = datetime(2026, 8, 23, tzinfo=UTC)


@pytest.mark.asyncio
async def test_mailbox_accept_during_processing_preserves_wakeup_generation() -> None:
    store = InMemorySessionMailboxStore()
    first = await store.accept("tenant-a", "session-a", "inbound-1", now=BASE)
    first_lease = await store.claim(
        "tenant-a", "session-a", owner_id="worker-a", lease_for=timedelta(seconds=30), now=BASE
    )
    assert first.accepted_sequence == 1
    assert first_lease is not None and first_lease.sequence == 1

    queued = await store.accept(
        "tenant-a", "session-a", "inbound-2", now=BASE + timedelta(seconds=1)
    )
    assert queued.status == MailboxStatus.RUNNING
    assert queued.accepted_sequence == 2
    assert queued.queue_generation == first.queue_generation
    assert len(store.outbox) == 1

    committed = await store.commit(first_lease, now=BASE + timedelta(seconds=2))
    assert committed.status == MailboxStatus.QUEUED
    assert committed.resolved_sequence == 1
    assert committed.accepted_sequence == 2
    assert committed.queue_generation == first.queue_generation + 1
    assert len(store.outbox) == 2

    second_lease = await store.claim(
        "tenant-a",
        "session-a",
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
        now=BASE + timedelta(seconds=3),
    )
    assert second_lease is not None and second_lease.sequence == 2


@pytest.mark.asyncio
async def test_mailbox_has_one_active_claim_and_expired_takeover_is_fenced() -> None:
    store = InMemorySessionMailboxStore()
    await store.accept("tenant-a", "session-a", "inbound-1", now=BASE)
    old = await store.claim(
        "tenant-a", "session-a", owner_id="worker-a", lease_for=timedelta(seconds=5), now=BASE
    )
    assert old is not None

    running = await store.claim_session(
        "tenant-a",
        "session-a",
        owner_id="worker-b",
        lease_for=timedelta(seconds=5),
        now=BASE + timedelta(seconds=1),
    )
    assert running.status == MailboxClaimStatus.RUNNING
    assert (
        await store.claim(
            "tenant-a",
            "session-a",
            owner_id="worker-b",
            lease_for=timedelta(seconds=5),
            now=BASE + timedelta(seconds=1),
        )
        is None
    )

    replacement = await store.claim(
        "tenant-a",
        "session-a",
        owner_id="worker-b",
        lease_for=timedelta(seconds=5),
        now=BASE + timedelta(seconds=6),
    )
    assert replacement is not None
    assert replacement.epoch == old.epoch + 1
    with pytest.raises(FencingConflict):
        await store.commit(old, now=BASE + timedelta(seconds=6))

    committed = await store.commit(replacement, now=BASE + timedelta(seconds=7))
    assert committed.status == MailboxStatus.IDLE


@pytest.mark.asyncio
async def test_mailbox_claim_uses_ready_generation_and_priority_is_non_negative() -> None:
    store = InMemorySessionMailboxStore()
    with pytest.raises(ValueError, match="priority"):
        await store.accept("tenant-a", "session-a", "inbound-bad", priority=-1, now=BASE)
    accepted = await store.accept("tenant-a", "session-a", "inbound-1", now=BASE)
    stale = await store.claim_session(
        "tenant-a",
        "session-a",
        owner_id="worker-a",
        lease_for=timedelta(seconds=5),
        expected_generation=accepted.queue_generation + 1,
        now=BASE,
    )
    assert stale.status == MailboxClaimStatus.STALE
    claimed = await store.claim_session(
        "tenant-a",
        "session-a",
        owner_id="worker-a",
        lease_for=timedelta(seconds=5),
        expected_generation=accepted.queue_generation,
        now=BASE,
    )
    assert claimed.claimed


@pytest.mark.asyncio
async def test_mailbox_retry_recover_and_reconcile_keep_sequence_monotonic() -> None:
    store = InMemorySessionMailboxStore()
    await store.accept("tenant-a", "session-a", "inbound-1", priority=4, now=BASE)
    lease = await store.claim(
        "tenant-a", "session-a", owner_id="worker-a", lease_for=timedelta(seconds=5), now=BASE
    )
    assert lease is not None
    retry_at = BASE + timedelta(seconds=10)
    waiting = await store.retry(lease, retry_at=retry_at, now=BASE + timedelta(seconds=1))
    assert waiting.status == MailboxStatus.RETRY_WAIT
    assert waiting.resolved_sequence == 0
    assert (
        await store.claim(
            "tenant-a",
            "session-a",
            owner_id="worker-b",
            lease_for=timedelta(seconds=5),
            now=BASE + timedelta(seconds=2),
        )
        is None
    )

    ready = await store.reconcile("tenant-a", "session-a", now=BASE + timedelta(seconds=11))
    assert ready is not None and ready.status == MailboxStatus.QUEUED
    recovered = await store.claim(
        "tenant-a",
        "session-a",
        owner_id="worker-b",
        lease_for=timedelta(seconds=5),
        now=BASE + timedelta(seconds=11),
    )
    assert recovered is not None and recovered.attempt == 2
    assert recovered.retry_count == 1


@pytest.mark.asyncio
async def test_postgres_mailbox_renew_is_owner_epoch_and_server_clock_fenced() -> None:
    expires_at = BASE + timedelta(seconds=30)
    connection = Connection(fetchrows=[{"lease_expires_at": expires_at}])
    store = PostgresSessionMailboxStore(Pool(connection))
    lease = MailboxLease(
        tenant_id="tenant-a",
        session_id="session-a",
        inbound_id=str(uuid4()),
        sequence=1,
        owner_id="worker-a",
        epoch=3,
        expires_at=BASE + timedelta(seconds=10),
    )
    renewed = await store.renew(lease, lease_for=timedelta(seconds=30))
    assert renewed.expires_at == expires_at
    query = next(call[1][0] for call in connection.calls if call[0] == "fetchrow")
    assert "status='RUNNING'" in query
    assert "lease_owner=$6" in query and "lease_epoch=$7" in query
    assert "clock_timestamp()" in query


@pytest.mark.asyncio
async def test_postgres_mailbox_stale_epoch_commit_is_rejected() -> None:
    lease = MailboxLease(
        tenant_id="tenant-a",
        session_id="session-a",
        inbound_id=str(uuid4()),
        sequence=1,
        owner_id="worker-a",
        epoch=2,
        expires_at=BASE + timedelta(seconds=10),
    )
    connection = Connection(
        fetchrows=[
            {
                "status": "RUNNING",
                "processing_sequence": 1,
                "processing_inbound_id": uuid4(),
                "lease_owner": "worker-a",
                "lease_epoch": 3,
                "lease_expires_at": BASE + timedelta(seconds=30),
            }
        ],
        fetchvals=[BASE],
    )
    store = PostgresSessionMailboxStore(Pool(connection))
    with pytest.raises(FencingConflict):
        await store.commit(lease)
