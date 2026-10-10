from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from tests.unit.test_mailbox_store_branch_coverage import (
    Connection,
    Pool,
    item_row,
    mailbox_row,
)
from trpc_service.storage.mailbox import (
    InMemorySessionMailboxStore,
    PostgresSessionMailboxStore,
)
from trpc_service.storage.models import (
    MailboxClaimStatus,
    MailboxLease,
    MailboxStatus,
    SessionMailbox,
)
from trpc_service.storage.protocols import FencingConflict

BASE = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
LEASE_FOR = timedelta(seconds=10)
TENANT = "tenant-state"
SESSION = "session-state"


async def _accept(
    store: InMemorySessionMailboxStore,
    inbound_id: str = "inbound-1",
    *,
    now: datetime = BASE,
    retry_at: datetime | None = None,
    priority: int = 0,
) -> None:
    await store.accept(
        TENANT,
        SESSION,
        inbound_id,
        now=now,
        retry_at=retry_at,
        priority=priority,
    )


async def _claim(
    store: InMemorySessionMailboxStore,
    *,
    owner: str = "worker-a",
    now: datetime = BASE,
) -> MailboxLease:
    result = await store.claim(
        TENANT,
        SESSION,
        owner_id=owner,
        lease_for=LEASE_FOR,
        now=now,
    )
    assert result is not None
    return result


@pytest.mark.asyncio
async def test_inmemory_validation_duplicate_and_empty_outcomes_are_explicit() -> None:
    store = InMemorySessionMailboxStore()
    with pytest.raises(ValueError, match="identifiers"):
        await store.accept("", SESSION, "inbound-1", now=BASE)
    with pytest.raises(ValueError, match="priority"):
        await store.accept(TENANT, SESSION, "inbound-1", priority=True, now=BASE)
    with pytest.raises(ValueError, match="lease"):
        await store.claim(
            TENANT,
            SESSION,
            owner_id="worker-a",
            lease_for=timedelta(0),
            now=BASE,
        )
    with pytest.raises(ValueError, match="identifiers"):
        await store.claim(
            TENANT,
            SESSION,
            owner_id="",
            lease_for=LEASE_FOR,
            now=BASE,
        )

    inbound_id = str(uuid4())
    first = await store.accept(TENANT, SESSION, inbound_id, now=BASE)
    duplicate = await store.accept(TENANT, SESSION, inbound_id, now=BASE + timedelta(seconds=1))
    assert duplicate == first
    assert duplicate.accepted_sequence == 1
    assert len(store.outbox) == 1

    missing = await store.claim_session(
        TENANT,
        "missing-session",
        owner_id="worker-a",
        lease_for=LEASE_FOR,
        now=BASE,
    )
    assert missing.status == MailboxClaimStatus.EMPTY


@pytest.mark.asyncio
async def test_inmemory_claim_handles_idle_future_retry_and_corrupt_missing_items() -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store)
    lease = await _claim(store)
    await store.commit(lease, now=BASE + timedelta(seconds=1))

    # A fully resolved mailbox is normalized to IDLE and has no new claim.
    assert (
        await store.claim(
            TENANT,
            SESSION,
            owner_id="worker-b",
            lease_for=LEASE_FOR,
            now=BASE + timedelta(seconds=1),
        )
        is None
    )
    mailbox = await store.get(TENANT, SESSION)
    assert mailbox is not None and mailbox.status == MailboxStatus.IDLE

    waiting = InMemorySessionMailboxStore()
    future = BASE + timedelta(minutes=1)
    await _accept(waiting, retry_at=future)
    assert (
        await waiting.claim(
            TENANT,
            SESSION,
            owner_id="worker-a",
            lease_for=LEASE_FOR,
            now=BASE,
        )
        is None
    )
    waiting_mailbox = await waiting.get(TENANT, SESSION)
    assert waiting_mailbox is not None
    assert waiting_mailbox.status == MailboxStatus.RETRY_WAIT
    assert waiting_mailbox.retry_at == future

    # These states model a broken/cut-over row.  Claim must fail closed rather
    # than inventing an inbound item or advancing the resolved sequence.
    missing_next = InMemorySessionMailboxStore()
    missing_next._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.QUEUED,
        accepted_sequence=1,
        resolved_sequence=0,
        updated_at=BASE,
    )
    assert (
        await missing_next.claim(
            TENANT,
            SESSION,
            owner_id="worker-a",
            lease_for=LEASE_FOR,
            now=BASE,
        )
        is None
    )

    missing_processing = InMemorySessionMailboxStore()
    missing_processing._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.RUNNING,
        accepted_sequence=1,
        resolved_sequence=0,
        processing_sequence=1,
        processing_inbound_id="inbound-1",
        lease_owner="old-worker",
        lease_epoch=1,
        lease_expires_at=BASE - timedelta(seconds=1),
        updated_at=BASE,
    )
    assert (
        await missing_processing.claim(
            TENANT,
            SESSION,
            owner_id="worker-a",
            lease_for=LEASE_FOR,
            now=BASE,
        )
        is None
    )


@pytest.mark.asyncio
async def test_inmemory_claim_session_stale_running_and_empty_states() -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store)
    mailbox = await store.get(TENANT, SESSION)
    assert mailbox is not None
    stale = await store.claim_session(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=LEASE_FOR,
        expected_generation=mailbox.queue_generation + 1,
        now=BASE,
    )
    assert stale.status == MailboxClaimStatus.STALE

    first = await _claim(store)
    running = await store.claim_session(
        TENANT,
        SESSION,
        owner_id="worker-b",
        lease_for=LEASE_FOR,
        expected_generation=mailbox.queue_generation,
        now=BASE + timedelta(seconds=1),
    )
    assert running.status == MailboxClaimStatus.RUNNING
    assert running.lease is None
    await store.commit(first, now=BASE + timedelta(seconds=2))


@pytest.mark.asyncio
async def test_inmemory_claim_session_detects_generation_changed_during_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store)
    mailbox = await store.get(TENANT, SESSION)
    assert mailbox is not None

    async def claim_and_change_generation(*_args: object, **_kwargs: object) -> None:
        current = store._mailboxes[(TENANT, SESSION)]
        store._mailboxes[(TENANT, SESSION)] = current.model_copy(
            update={"queue_generation": current.queue_generation + 1}
        )
        return None

    monkeypatch.setattr(store, "claim", claim_and_change_generation)
    result = await store.claim_session(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=LEASE_FOR,
        expected_generation=mailbox.queue_generation,
        now=BASE,
    )
    assert result.status == MailboxClaimStatus.STALE
    assert result.mailbox.queue_generation == mailbox.queue_generation + 1


@pytest.mark.asyncio
async def test_inmemory_renew_batch_fences_stale_members_and_preserves_longer_expiry() -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store)
    lease = await _claim(store)
    renewed = await store.renew(
        lease,
        lease_for=timedelta(seconds=1),
        now=BASE + timedelta(seconds=1),
    )
    assert renewed.expires_at > lease.expires_at
    assert renewed.expires_at > BASE + timedelta(seconds=1)
    assert await store.renew_many((), lease_for=LEASE_FOR, now=BASE) == ()

    with pytest.raises(FencingConflict):
        await store.renew_many(
            (lease.model_copy(update={"owner_id": "other-worker"}),),
            lease_for=LEASE_FOR,
            now=BASE + timedelta(seconds=2),
        )
    await store.commit(renewed, now=BASE + timedelta(seconds=3))


@pytest.mark.asyncio
async def test_inmemory_commit_transitions_to_idle_queued_or_retry_wait() -> None:
    idle = InMemorySessionMailboxStore()
    await _accept(idle)
    first = await _claim(idle)
    committed = await idle.commit(first, now=BASE + timedelta(seconds=1))
    assert committed.status == MailboxStatus.IDLE
    assert committed.resolved_sequence == committed.accepted_sequence == 1

    queued = InMemorySessionMailboxStore()
    await _accept(queued, now=BASE)
    first = await _claim(queued)
    await queued.accept(TENANT, SESSION, "inbound-2", now=BASE)
    committed = await queued.commit(first, now=BASE + timedelta(seconds=1))
    assert committed.status == MailboxStatus.QUEUED
    assert committed.resolved_sequence == 1
    assert committed.queue_generation == 2
    assert len(queued.outbox) == 2

    waiting = InMemorySessionMailboxStore()
    await _accept(waiting, now=BASE)
    first = await _claim(waiting)
    retry_at = BASE + timedelta(minutes=1)
    await waiting.accept(TENANT, SESSION, "inbound-2", retry_at=retry_at, now=BASE)
    committed = await waiting.commit(first, now=BASE + timedelta(seconds=1))
    assert committed.status == MailboxStatus.RETRY_WAIT
    assert committed.retry_at == retry_at
    assert len(waiting.outbox) == 1


@pytest.mark.asyncio
async def test_inmemory_fenced_mutations_reject_a_stale_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store)
    lease = await _claim(store)
    current = await store.get(TENANT, SESSION)
    assert current is not None
    mismatched = current.model_copy(update={"processing_sequence": 2})
    monkeypatch.setattr(store, "_require_owned", lambda *_args: mismatched)
    with pytest.raises(FencingConflict):
        await store.commit(lease, now=BASE)
    with pytest.raises(FencingConflict):
        await store.retry(lease, retry_at=None, now=BASE)


@pytest.mark.asyncio
async def test_inmemory_reschedule_retry_and_retry_without_wakeup_preserve_rules() -> None:
    store = InMemorySessionMailboxStore()
    await _accept(store, priority=1)
    lease = await _claim(store)
    due = await store.reschedule(
        lease,
        retry_at=BASE - timedelta(seconds=1),
        priority=4,
        now=BASE,
    )
    assert due.status == MailboxStatus.QUEUED
    assert due.retry_count == 0
    assert due.priority == 4
    assert len(store.outbox) == 2

    lease = await _claim(store, now=BASE + timedelta(seconds=1))
    future = BASE + timedelta(minutes=1)
    waiting = await store.retry(lease, retry_at=future, now=BASE + timedelta(seconds=2))
    assert waiting.status == MailboxStatus.RETRY_WAIT
    assert waiting.retry_count == 1
    assert waiting.retry_at == future

    # The worker retry path deliberately leaves recovery to the scheduler and
    # therefore must not create a ready event or make the head runnable.
    due_lease = await store.claim(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=LEASE_FOR,
        now=BASE + timedelta(minutes=2),
    )
    assert due_lease is not None
    no_wakeup = await store.retry_without_wakeup(
        due_lease,
        retry_at=BASE + timedelta(minutes=3),
        now=BASE + timedelta(minutes=2),
    )
    assert no_wakeup.status == MailboxStatus.RETRY_WAIT
    assert len(store.outbox) == 2


@pytest.mark.asyncio
async def test_inmemory_recover_covers_noop_idle_waiting_and_ready() -> None:
    absent = InMemorySessionMailboxStore()
    assert await absent.recover(TENANT, SESSION, now=BASE) is None

    active = InMemorySessionMailboxStore()
    await _accept(active)
    active_lease = await _claim(active)
    assert await active.recover(TENANT, SESSION, now=BASE) is None
    await active.commit(active_lease, now=BASE + timedelta(seconds=1))
    assert await active.recover(TENANT, SESSION, now=BASE + timedelta(seconds=1)) is None

    missing = InMemorySessionMailboxStore()
    missing._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.RUNNING,
        accepted_sequence=1,
        resolved_sequence=0,
        processing_sequence=1,
        processing_inbound_id="missing",
        lease_owner="old-worker",
        lease_epoch=1,
        lease_expires_at=BASE - timedelta(seconds=1),
        updated_at=BASE,
    )
    recovered = await missing.recover(TENANT, SESSION, now=BASE)
    assert recovered is not None and recovered.status == MailboxStatus.IDLE
    assert recovered.processing_sequence is None

    waiting = InMemorySessionMailboxStore()
    future = BASE + timedelta(minutes=1)
    await _accept(waiting, retry_at=future)
    # The lease cannot be claimed before the retry deadline; model an expired
    # worker lease around the already persisted head to exercise recovery.
    waiting._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.RUNNING,
        accepted_sequence=1,
        resolved_sequence=0,
        processing_sequence=1,
        processing_inbound_id="inbound-1",
        lease_owner="old-worker",
        lease_epoch=1,
        lease_expires_at=BASE - timedelta(seconds=1),
        retry_at=future,
        updated_at=BASE,
    )
    recovered = await waiting.recover(TENANT, SESSION, now=BASE)
    assert recovered is not None and recovered.status == MailboxStatus.RETRY_WAIT
    assert recovered.retry_at == future

    ready = InMemorySessionMailboxStore()
    await _accept(ready)
    ready._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.RUNNING,
        accepted_sequence=1,
        resolved_sequence=0,
        processing_sequence=1,
        processing_inbound_id="inbound-1",
        lease_owner="old-worker",
        lease_epoch=1,
        lease_expires_at=BASE - timedelta(seconds=1),
        updated_at=BASE,
    )
    before = len(ready.outbox)
    recovered = await ready.recover(TENANT, SESSION, now=BASE)
    assert recovered is not None and recovered.status == MailboxStatus.QUEUED
    assert len(ready.outbox) == before + 1


@pytest.mark.asyncio
async def test_inmemory_reconcile_repairs_missing_retry_and_ready_states() -> None:
    absent = InMemorySessionMailboxStore()
    assert await absent.reconcile(TENANT, SESSION, now=BASE) is None

    recent = InMemorySessionMailboxStore()
    await _accept(recent)
    # Accept makes the queued wake-up recent; reconciliation should not emit a
    # duplicate event while the original outbox record is still fresh.
    before = len(recent.outbox)
    preserved = await recent.reconcile(TENANT, SESSION, now=BASE + timedelta(seconds=1))
    assert preserved is not None and preserved.status == MailboxStatus.QUEUED
    assert len(recent.outbox) == before

    old = InMemorySessionMailboxStore()
    await _accept(old)
    old._mailboxes[(TENANT, SESSION)] = (await old.get(TENANT, SESSION)).model_copy(  # type: ignore[union-attr]
        update={"updated_at": BASE - timedelta(minutes=1)}
    )
    before = len(old.outbox)
    repaired = await old.reconcile(TENANT, SESSION, now=BASE)
    assert repaired is not None and repaired.status == MailboxStatus.QUEUED
    # A stale QUEUED mailbox replays its existing generation; it must not
    # manufacture a new outbox row on every reconciler pass.
    assert len(old.outbox) == before

    waiting = InMemorySessionMailboxStore()
    future = BASE + timedelta(minutes=1)
    await _accept(waiting, retry_at=future)
    repaired = await waiting.reconcile(TENANT, SESSION, now=BASE)
    assert repaired is not None and repaired.status == MailboxStatus.RETRY_WAIT
    assert repaired.retry_at == future

    active = InMemorySessionMailboxStore()
    await _accept(active)
    active_lease = await _claim(active)
    current = await active.reconcile(TENANT, SESSION, now=BASE + timedelta(seconds=1))
    assert current is not None and current.status == MailboxStatus.RUNNING
    assert current.processing_sequence == active_lease.sequence

    missing = InMemorySessionMailboxStore()
    missing._mailboxes[(TENANT, SESSION)] = SessionMailbox(
        tenant_id=TENANT,
        session_id=SESSION,
        status=MailboxStatus.QUEUED,
        accepted_sequence=1,
        resolved_sequence=0,
        updated_at=BASE - timedelta(minutes=1),
    )
    repaired = await missing.reconcile(TENANT, SESSION, now=BASE)
    assert repaired is not None and repaired.status == MailboxStatus.IDLE


@pytest.mark.asyncio
async def test_inmemory_recovery_schedulers_validate_limits_and_count_due_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemorySessionMailboxStore()
    with pytest.raises(ValueError, match="positive"):
        await store.sweep_expired_leases(owner_id="recovery", limit=0)
    with pytest.raises(ValueError, match="positive"):
        await store.schedule_retries(owner_id="recovery", limit=0)
    with pytest.raises(ValueError, match="positive"):
        await store.reconcile_sessions(owner_id="recovery", limit=0)

    await _accept(store)
    lease = await _claim(store)
    await store.retry(
        lease,
        retry_at=BASE - timedelta(seconds=1),
        now=BASE,
    )
    # A due retry is immediately queued by retry(); create the RETRY_WAIT
    # state that the independent scheduler is responsible for waking.
    scheduler_now = datetime.now(UTC)
    due = scheduler_now - timedelta(seconds=1)
    current = await store.get(TENANT, SESSION)
    assert current is not None
    store._mailboxes[(TENANT, SESSION)] = current.model_copy(
        update={"status": MailboxStatus.RETRY_WAIT, "retry_at": due}
    )
    store._items[(TENANT, SESSION)][1] = store._items[(TENANT, SESSION)][1].model_copy(
        update={"retry_at": due}
    )
    assert await store.schedule_retries(owner_id="recovery", limit=1) == 1

    lease = await _claim(store, now=scheduler_now + timedelta(seconds=1))
    await store.retry(
        lease,
        retry_at=scheduler_now + timedelta(minutes=1),
        now=scheduler_now + timedelta(seconds=1),
    )
    # The future retry is not eligible until its due time.
    assert await store.schedule_retries(owner_id="recovery", limit=1) == 0
    assert await store.reconcile_sessions(owner_id="recovery", limit=1) == 1

    # Expired lease recovery is handled independently from retry scheduling.
    expired_lease = await store.claim(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=timedelta(seconds=1),
        now=scheduler_now + timedelta(minutes=2),
    )
    assert expired_lease is not None
    current = await store.get(TENANT, SESSION)
    assert current is not None
    store._mailboxes[(TENANT, SESSION)] = current.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    assert (
        await store.sweep_expired_leases(
            owner_id="recovery",
            limit=1,
        )
        == 1
    )

    # Exercise both the non-expired branch and the scheduler limit break.
    sweep = InMemorySessionMailboxStore()
    await sweep.accept(TENANT, "active", "active-inbound", now=BASE)
    await sweep.accept(TENANT, "expired-1", "expired-inbound-1", now=BASE)
    await sweep.accept(TENANT, "expired-2", "expired-inbound-2", now=BASE)
    for session_id in ("expired-1", "expired-2"):
        current = await sweep.get(TENANT, session_id)
        assert current is not None
        sweep._mailboxes[(TENANT, session_id)] = current.model_copy(
            update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
        )
    assert await sweep.sweep_expired_leases(owner_id="recovery", limit=1) == 1

    retries = InMemorySessionMailboxStore()
    due = datetime.now(UTC) - timedelta(seconds=1)
    for session_id in ("retry-1", "retry-2"):
        await retries.accept(TENANT, session_id, f"{session_id}-inbound", now=BASE)
        current = await retries.get(TENANT, session_id)
        assert current is not None
        retries._mailboxes[(TENANT, session_id)] = current.model_copy(
            update={"status": MailboxStatus.RETRY_WAIT, "retry_at": due}
        )
        item = retries._items[(TENANT, session_id)][1]
        retries._items[(TENANT, session_id)][1] = item.model_copy(update={"retry_at": due})
    assert await retries.schedule_retries(owner_id="recovery", limit=1) == 1

    no_retry = InMemorySessionMailboxStore()
    await no_retry.accept(TENANT, SESSION, "inbound-no-retry", now=BASE)
    temporary_patch = pytest.MonkeyPatch()
    temporary_patch.setattr(no_retry, "reconcile", lambda *_args, **_kwargs: _none_async())
    try:
        current = await no_retry.get(TENANT, SESSION)
        assert current is not None
        no_retry._mailboxes[(TENANT, SESSION)] = current.model_copy(
            update={"status": MailboxStatus.RETRY_WAIT, "retry_at": due}
        )
        no_retry._items[(TENANT, SESSION)][1] = no_retry._items[(TENANT, SESSION)][1].model_copy(
            update={"retry_at": due}
        )
        assert await no_retry.schedule_retries(owner_id="recovery", limit=1) == 0
    finally:
        temporary_patch.undo()

    no_recovery = InMemorySessionMailboxStore()
    await no_recovery.accept(TENANT, SESSION, "inbound-no-recovery", now=BASE)
    current = await no_recovery.get(TENANT, SESSION)
    assert current is not None
    no_recovery._mailboxes[(TENANT, SESSION)] = current.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )

    async def no_recover(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(no_recovery, "recover", no_recover)
    assert await no_recovery.sweep_expired_leases(owner_id="recovery", limit=1) == 0

    sessions = InMemorySessionMailboxStore()
    await sessions.accept(TENANT, "reconcile-1", "reconcile-inbound-1", now=BASE)
    await sessions.accept(TENANT, "reconcile-2", "reconcile-inbound-2", now=BASE)
    assert await sessions.reconcile_sessions(owner_id="recovery", limit=1) == 1

    no_reconcile = InMemorySessionMailboxStore()
    await no_reconcile.accept(TENANT, SESSION, "inbound-no-reconcile", now=BASE)

    async def no_reconcile_result(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(no_reconcile, "reconcile", no_reconcile_result)
    assert await no_reconcile.reconcile_sessions(owner_id="recovery", limit=1) == 0


async def _none_async() -> None:
    return None


@pytest.mark.asyncio
async def test_postgres_mailbox_get_and_reconcile_no_row_are_safe() -> None:
    empty = Connection(fetchrows=[None])
    assert await PostgresSessionMailboxStore(Pool(empty)).get(TENANT, SESSION) is None
    no_row = Connection(fetchrows=[None])
    assert await PostgresSessionMailboxStore(Pool(no_row)).reconcile(TENANT, SESSION) is None


@pytest.mark.asyncio
async def test_postgres_accept_duplicate_and_rejects_non_uuid_ids() -> None:
    duplicate_row = mailbox_row(status=MailboxStatus.RUNNING.value, generation=3)
    duplicate = Connection(fetchrows=[duplicate_row, duplicate_row, {"sequence": 1}])
    result = await PostgresSessionMailboxStore(Pool(duplicate)).accept(
        TENANT,
        SESSION,
        str(uuid4()),
    )
    assert result.status == MailboxStatus.RUNNING
    assert result.queue_generation == 3
    assert not any("UPDATE session_mailboxes" in str(args[0]) for kind, args in duplicate.calls)

    with pytest.raises(ValueError, match="UUID"):
        await PostgresSessionMailboxStore(Pool(Connection())).accept(
            TENANT,
            SESSION,
            "not-a-uuid",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "current_retry_at", "new_retry_at", "expected_status"),
    [
        (
            MailboxStatus.RETRY_WAIT.value,
            BASE + timedelta(minutes=1),
            BASE + timedelta(minutes=2),
            MailboxStatus.RETRY_WAIT,
        ),
        (
            MailboxStatus.IDLE.value,
            None,
            BASE + timedelta(minutes=2),
            MailboxStatus.RETRY_WAIT,
        ),
        (MailboxStatus.RUNNING.value, None, None, MailboxStatus.RUNNING),
    ],
)
async def test_postgres_accept_preserves_head_timer_and_non_idle_state(
    status: str,
    current_retry_at: datetime | None,
    new_retry_at: datetime | None,
    expected_status: MailboxStatus,
) -> None:
    current = mailbox_row(status=status, retry_at=current_retry_at, generation=4)
    updated = mailbox_row(
        status=expected_status.value,
        retry_at=(new_retry_at if expected_status == MailboxStatus.RETRY_WAIT else None),
        generation=4,
    )
    connection = Connection(
        fetchrows=[current, None, updated],
        fetchvals=[BASE],
    )
    result = await PostgresSessionMailboxStore(Pool(connection)).accept(
        TENANT,
        SESSION,
        str(uuid4()),
        retry_at=new_retry_at,
    )
    assert result.status == expected_status
    assert not any("session.ready.v2" in str(args[0]) for kind, args in connection.calls)


@pytest.mark.asyncio
async def test_postgres_claim_session_returns_claimed_and_stale_after_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expired = mailbox_row(expires=BASE - timedelta(seconds=1))
    inbound_id = uuid4()
    updated_item: dict[str, Any] = {
        "attempt": 1,
        "retry_count": 0,
        "priority": 0,
        "retry_at": None,
        "inbound_id": inbound_id,
    }
    claimed_row = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=updated_item["inbound_id"],
        owner="worker-a",
        epoch=1,
        expires=BASE + LEASE_FOR,
    )
    connection = Connection(
        fetchrows=[
            mailbox_row(),
            expired,
            {**item_row(inbound_id=inbound_id), "attempt": 0, "retry_count": 0},
            updated_item,
            {"lease_expires_at": BASE + LEASE_FOR},
            claimed_row,
        ],
        fetchvals=[BASE, BASE],
    )
    result = await PostgresSessionMailboxStore(Pool(connection)).claim_session(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=LEASE_FOR,
    )
    assert result.status == MailboxClaimStatus.CLAIMED
    assert result.lease is not None

    race_connection = Connection(
        fetchrows=[mailbox_row(generation=1), mailbox_row(generation=2)],
        fetchvals=[BASE],
    )
    store = PostgresSessionMailboxStore(Pool(race_connection))

    async def no_claim(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(store, "_claim_locked", no_claim)
    raced = await store.claim_session(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=LEASE_FOR,
        expected_generation=1,
    )
    assert raced.status == MailboxClaimStatus.STALE


@pytest.mark.asyncio
async def test_postgres_reconcile_missing_head_becomes_idle() -> None:
    current = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        expires=BASE - timedelta(seconds=1),
    )
    updated = mailbox_row(status=MailboxStatus.IDLE.value, accepted=1, resolved=0)
    connection = Connection(fetchrows=[current, None, None, updated], fetchvals=[BASE])
    result = await PostgresSessionMailboxStore(Pool(connection)).reconcile(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.IDLE
    assert not any("session.ready.v2" in str(args[0]) for kind, args in connection.calls)


@pytest.mark.asyncio
async def test_postgres_claim_without_item_and_renew_failure_fail_closed() -> None:
    no_item = Connection(
        fetchrows=[mailbox_row(expires=BASE - timedelta(seconds=1)), None],
        fetchvals=[BASE],
    )
    assert (
        await PostgresSessionMailboxStore(Pool(no_item)).claim(
            TENANT,
            SESSION,
            owner_id="worker-a",
            lease_for=LEASE_FOR,
        )
        is None
    )
    assert not any("SET status='RETRY_WAIT'" in str(args[0]) for kind, args in no_item.calls)

    lease = MailboxLease(
        tenant_id=TENANT,
        session_id=SESSION,
        inbound_id=str(uuid4()),
        sequence=1,
        owner_id="worker-a",
        epoch=1,
        expires_at=BASE + LEASE_FOR,
    )
    with pytest.raises(FencingConflict):
        await PostgresSessionMailboxStore(Pool(Connection(fetchrows=[None]))).renew(
            lease,
            lease_for=LEASE_FOR,
        )


@pytest.mark.asyncio
async def test_postgres_reschedule_delegates_to_fenced_release() -> None:
    owned = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=uuid4(),
        owner="worker-a",
        epoch=3,
        expires=BASE + LEASE_FOR,
    )
    inbound_id = owned["processing_inbound_id"]
    lease = MailboxLease(
        tenant_id=TENANT,
        session_id=SESSION,
        inbound_id=str(inbound_id),
        sequence=1,
        owner_id="worker-a",
        epoch=3,
        expires_at=BASE + LEASE_FOR,
    )
    result_retry_at = BASE + timedelta(minutes=1)
    result_row = mailbox_row(
        status=MailboxStatus.RETRY_WAIT.value,
        retry_at=result_retry_at,
    )
    connection = Connection(
        fetchrows=[
            owned,
            {
                "retry_count": 0,
                "priority": 0,
                "retry_at": result_retry_at,
                "trace_id": "t",
                "inbound_id": inbound_id,
            },
            result_row,
        ],
        fetchvals=[BASE, BASE],
    )
    result = await PostgresSessionMailboxStore(Pool(connection)).reschedule(
        lease,
        retry_at=result_retry_at,
    )
    assert result.status == MailboxStatus.RETRY_WAIT
    assert result.retry_at == result_retry_at
