from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from trpc_service.storage.mailbox import PostgresSessionMailboxStore
from trpc_service.storage.models import MailboxClaimStatus, MailboxLease, MailboxStatus
from trpc_service.storage.protocols import FencingConflict

BASE = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
TENANT = "tenant-a"
SESSION = "session-a"
INBOUND = UUID("11111111-1111-1111-1111-111111111111")


class Connection:
    """Small asyncpg-shaped script for testing SQL branch semantics offline."""

    def __init__(
        self,
        *,
        fetchrows: Iterable[object] = (),
        fetchvals: Iterable[object] = (),
        fetches: Iterable[object] = (),
        executes: Iterable[str] = (),
    ) -> None:
        self.fetchrows = list(fetchrows)
        self.fetchvals = list(fetchvals)
        self.fetches = list(fetches)
        self.executes = list(executes)
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> Connection:
        return self

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def fetchrow(self, *args: object) -> object:
        self.calls.append(("fetchrow", args))
        return self.fetchrows.pop(0) if self.fetchrows else None

    async def fetchval(self, *args: object) -> object:
        self.calls.append(("fetchval", args))
        value = self.fetchvals.pop(0) if self.fetchvals else None
        if isinstance(value, BaseException):
            raise value
        return value

    async def fetch(self, *args: object) -> object:
        self.calls.append(("fetch", args))
        return self.fetches.pop(0) if self.fetches else []

    async def execute(self, *args: object) -> str:
        self.calls.append(("execute", args))
        return self.executes.pop(0) if self.executes else "UPDATE 1"


class Acquire:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class Pool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def acquire(self) -> Acquire:
        return Acquire(self.connection)


def mailbox_row(
    *,
    status: str = MailboxStatus.QUEUED.value,
    accepted: int = 1,
    resolved: int = 0,
    processing: int | None = None,
    processing_inbound: UUID | None = None,
    generation: int = 1,
    owner: str | None = None,
    epoch: int = 0,
    expires: datetime | None = None,
    retry_count: int = 0,
    attempt: int = 0,
    priority: int = 0,
    retry_at: datetime | None = None,
    updated_at: datetime = BASE,
) -> dict[str, object]:
    return {
        "tenant_id": TENANT,
        "session_id": SESSION,
        "status": status,
        "accepted_sequence": accepted,
        "resolved_sequence": resolved,
        "processing_sequence": processing,
        "processing_inbound_id": processing_inbound,
        "queue_generation": generation,
        "lease_owner": owner,
        "lease_epoch": epoch,
        "lease_expires_at": expires,
        "retry_count": retry_count,
        "attempt": attempt,
        "priority": priority,
        "retry_at": retry_at,
        "updated_at": updated_at,
    }


def item_row(
    *,
    inbound_id: UUID = INBOUND,
    retry_at: datetime | None = None,
    trace_id: str = "trace-1",
    priority: int = 3,
) -> dict[str, object]:
    return {
        "inbound_id": inbound_id,
        "trace_id": trace_id,
        "priority": priority,
        "retry_at": retry_at,
    }


def lease(*, owner: str = "worker-a", epoch: int = 3) -> MailboxLease:
    return MailboxLease(
        tenant_id=TENANT,
        session_id=SESSION,
        inbound_id=str(INBOUND),
        sequence=1,
        owner_id=owner,
        epoch=epoch,
        expires_at=BASE + timedelta(seconds=30),
        attempt=1,
        retry_count=1,
        priority=3,
    )


def store(connection: Connection) -> PostgresSessionMailboxStore:
    return PostgresSessionMailboxStore(Pool(connection))


@pytest.mark.asyncio
async def test_claim_success_and_no_work_branches_use_database_clock() -> None:
    expires = BASE + timedelta(seconds=30)
    updated = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="worker-a",
        epoch=1,
        expires=expires,
        attempt=1,
        retry_count=0,
        priority=2,
    )
    connection = Connection(
        fetchrows=[
            mailbox_row(expires=BASE - timedelta(seconds=1)),
            item_row(priority=2),
            {
                "attempt": 1,
                "retry_count": 0,
                "priority": 2,
                "retry_at": None,
                "inbound_id": INBOUND,
            },
            {"lease_expires_at": expires},
        ],
        fetchvals=[BASE],
    )
    claimed = await store(connection).claim(
        TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
    )
    assert claimed is not None
    assert claimed.sequence == 1
    assert claimed.inbound_id == str(INBOUND)
    assert claimed.expires_at == expires
    assert any(
        kind == "fetchval" and "clock_timestamp()" in str(args[0])
        for kind, args in connection.calls
    )
    assert updated["status"] == MailboxStatus.RUNNING.value

    active = Connection(
        fetchrows=[
            mailbox_row(
                status=MailboxStatus.RUNNING.value,
                processing=1,
                processing_inbound=INBOUND,
                owner="other",
                epoch=2,
                expires=BASE + timedelta(minutes=1),
            )
        ],
        fetchvals=[BASE],
    )
    assert (
        await store(active).claim(
            TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
        )
        is None
    )


@pytest.mark.asyncio
async def test_claim_marks_resolved_idle_and_future_head_retry_wait() -> None:
    idle = Connection(
        fetchrows=[mailbox_row(accepted=1, resolved=1, status=MailboxStatus.QUEUED.value)],
        fetchvals=[BASE],
    )
    assert (
        await store(idle).claim(
            TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
        )
        is None
    )
    assert any(
        kind == "execute" and "SET status='IDLE'" in str(args[0]) for kind, args in idle.calls
    )

    future = BASE + timedelta(minutes=5)
    waiting = Connection(
        fetchrows=[mailbox_row(expires=BASE - timedelta(seconds=1)), None],
        fetchvals=[BASE, future],
    )
    assert (
        await store(waiting).claim(
            TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
        )
        is None
    )
    update = next(
        args
        for kind, args in waiting.calls
        if kind == "execute" and "session_mailboxes" in str(args[0])
    )
    assert "SET status='RETRY_WAIT'" in str(update[0])
    assert future in update


@pytest.mark.asyncio
async def test_claim_session_returns_stale_running_and_empty_explicitly() -> None:
    stale_connection = Connection(
        fetchrows=[mailbox_row(generation=2)],
        fetchvals=[BASE],
    )
    stale = await store(stale_connection).claim_session(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
    )
    assert stale.status == MailboxClaimStatus.STALE
    assert len(stale_connection.calls) == 3

    active_row = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="worker-b",
        epoch=2,
        expires=BASE + timedelta(minutes=1),
    )
    running_connection = Connection(
        fetchrows=[active_row, active_row, active_row],
        fetchvals=[BASE, BASE],
    )
    running = await store(running_connection).claim_session(
        TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
    )
    assert running.status == MailboxClaimStatus.RUNNING
    assert running.lease is None

    empty_connection = Connection(fetchrows=[None, None], fetchvals=[BASE])
    empty = await store(empty_connection).claim_session(
        TENANT, SESSION, owner_id="worker-a", lease_for=timedelta(seconds=30)
    )
    assert empty.status == MailboxClaimStatus.EMPTY


@pytest.mark.asyncio
async def test_renew_many_is_atomic_by_tenant_and_fails_closed_on_stale_member() -> None:
    first_expiry = BASE + timedelta(seconds=30)
    second_expiry = BASE + timedelta(seconds=31)
    connection = Connection(
        fetchrows=[{"lease_expires_at": first_expiry}, {"lease_expires_at": second_expiry}]
    )
    renewed = await store(connection).renew_many(
        (lease(), lease(owner="worker-b", epoch=4)),
        lease_for=timedelta(seconds=30),
    )
    assert tuple(value.expires_at for value in renewed) == (first_expiry, second_expiry)
    assert sum(kind == "fetchrow" for kind, _args in connection.calls) == 2
    assert all(
        "lease_owner=$6" in str(args[0]) and "clock_timestamp()" in str(args[0])
        for kind, args in connection.calls
        if kind == "fetchrow"
    )

    mismatch = Connection(fetchrows=[{"lease_expires_at": first_expiry}])
    with pytest.raises(ValueError, match="one tenant"):
        await store(mismatch).renew_many(
            (lease(), lease(owner="worker-b").model_copy(update={"tenant_id": "tenant-b"})),
            lease_for=timedelta(seconds=30),
        )

    stale = Connection(fetchrows=[None])
    with pytest.raises(FencingConflict):
        await store(stale).renew_many((lease(),), lease_for=timedelta(seconds=30))

    empty = Connection()
    assert await store(empty).renew_many((), lease_for=timedelta(seconds=30)) == ()
    assert empty.calls == []


@pytest.mark.asyncio
async def test_retry_due_emits_ready_and_future_retry_wait_does_not() -> None:
    owned = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="worker-a",
        epoch=3,
        expires=BASE + timedelta(seconds=30),
    )
    returned = mailbox_row(
        status=MailboxStatus.QUEUED.value,
        generation=8,
        retry_count=2,
        priority=4,
        retry_at=None,
    )
    due_connection = Connection(
        fetchrows=[
            owned,
            {
                "retry_count": 2,
                "priority": 4,
                "retry_at": None,
                "trace_id": "trace",
                "inbound_id": INBOUND,
            },
            returned,
        ],
        fetchvals=[BASE, BASE],
    )
    due = await store(due_connection).retry(
        lease(), retry_at=BASE - timedelta(seconds=1), priority=4
    )
    assert due.status == MailboxStatus.QUEUED
    assert due.retry_count == 2
    assert any(
        kind == "execute" and "session.ready.v2" in str(args[0])
        for kind, args in due_connection.calls
    )

    future = BASE + timedelta(minutes=5)
    future_connection = Connection(
        fetchrows=[
            owned,
            {
                "retry_count": 2,
                "priority": 3,
                "retry_at": future,
                "trace_id": "trace",
                "inbound_id": INBOUND,
            },
            mailbox_row(status=MailboxStatus.RETRY_WAIT.value, retry_at=future),
        ],
        fetchvals=[BASE, BASE],
    )
    waiting = await store(future_connection).retry(lease(), retry_at=future)
    assert waiting.status == MailboxStatus.RETRY_WAIT
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0])
        for kind, args in future_connection.calls
    )

    no_wakeup_connection = Connection(
        fetchrows=[
            owned,
            {
                "retry_count": 2,
                "priority": 3,
                "retry_at": BASE,
                "trace_id": "trace",
                "inbound_id": INBOUND,
            },
            mailbox_row(status=MailboxStatus.RETRY_WAIT.value, retry_at=BASE),
        ],
        fetchvals=[BASE, BASE],
    )
    no_wakeup = await store(no_wakeup_connection).retry_without_wakeup(lease(), retry_at=BASE)
    assert no_wakeup.status == MailboxStatus.RETRY_WAIT
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0])
        for kind, args in no_wakeup_connection.calls
    )


@pytest.mark.asyncio
async def test_recover_expired_handles_missing_future_and_ready_items() -> None:
    assert await store(Connection(fetchrows=[None])).recover(TENANT, SESSION) is None

    expired = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="old",
        epoch=2,
        expires=BASE - timedelta(seconds=1),
    )
    missing = Connection(
        fetchrows=[expired, None, None, mailbox_row(status=MailboxStatus.IDLE.value)],
        fetchvals=[BASE],
        fetches=[],
    )
    result = await store(missing).recover(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.IDLE
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0]) for kind, args in missing.calls
    )

    future = BASE + timedelta(minutes=5)
    waiting = Connection(
        fetchrows=[
            expired,
            None,
            item_row(retry_at=future),
            mailbox_row(status=MailboxStatus.RETRY_WAIT.value, retry_at=future),
        ],
        fetchvals=[BASE],
    )
    result = await store(waiting).recover(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.RETRY_WAIT
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0]) for kind, args in waiting.calls
    )

    ready = Connection(
        fetchrows=[
            expired,
            None,
            item_row(),
            mailbox_row(status=MailboxStatus.QUEUED.value, generation=4),
        ],
        fetchvals=[BASE],
    )
    result = await store(ready).recover(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.QUEUED
    assert any(
        kind == "execute" and "session.ready.v2" in str(args[0]) for kind, args in ready.calls
    )


@pytest.mark.asyncio
async def test_reconcile_preserves_active_and_recent_ready_but_repairs_lost_wakeup() -> None:
    active_row = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="worker-a",
        epoch=3,
        expires=BASE + timedelta(minutes=1),
    )
    active = Connection(
        fetchrows=[
            active_row,
            {
                "lease_owner": "worker-a",
                "lease_epoch": 3,
                "lease_expires_at": BASE + timedelta(minutes=1),
            },
        ],
        fetchvals=[BASE],
    )
    result = await store(active).reconcile(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.RUNNING
    assert len(active.calls) == 4

    recent = Connection(
        fetchrows=[mailbox_row(updated_at=BASE - timedelta(seconds=1)), None, item_row()],
        fetchvals=[BASE],
    )
    result = await store(recent).reconcile(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.QUEUED
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0]) for kind, args in recent.calls
    )

    old = Connection(
        fetchrows=[
            mailbox_row(updated_at=BASE - timedelta(minutes=1)),
            None,
            item_row(),
            mailbox_row(generation=2),
        ],
        fetchvals=[BASE],
    )
    result = await store(old).reconcile(TENANT, SESSION)
    assert result is not None and result.queue_generation == 2
    assert any(kind == "execute" and "session.ready.v2" in str(args[0]) for kind, args in old.calls)

    future = BASE + timedelta(minutes=5)
    future_connection = Connection(
        fetchrows=[
            mailbox_row(updated_at=BASE - timedelta(minutes=1)),
            None,
            item_row(retry_at=future),
            mailbox_row(status=MailboxStatus.RETRY_WAIT.value, retry_at=future),
        ],
        fetchvals=[BASE],
    )
    result = await store(future_connection).reconcile(TENANT, SESSION)
    assert result is not None and result.status == MailboxStatus.RETRY_WAIT
    assert not any(
        kind == "execute" and "session_mailboxes" in str(args[0])
        for kind, args in future_connection.calls
    )


@pytest.mark.asyncio
async def test_commit_resolves_head_and_wakes_or_waits_for_next_item() -> None:
    owned = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="worker-a",
        epoch=3,
        expires=BASE + timedelta(seconds=30),
    )
    idle = mailbox_row(status=MailboxStatus.IDLE.value, accepted=1, resolved=1, generation=3)
    no_next = Connection(fetchrows=[owned, None, idle], fetchvals=[BASE, BASE])
    result = await store(no_next).commit(lease())
    assert result.status == MailboxStatus.IDLE
    assert not any(
        kind == "execute" and "session.ready.v2" in str(args[0]) for kind, args in no_next.calls
    )

    next_item = item_row(inbound_id=UUID("22222222-2222-2222-2222-222222222222"), priority=5)
    queued = mailbox_row(status=MailboxStatus.QUEUED.value, accepted=2, resolved=1, generation=4)
    due = Connection(fetchrows=[owned, next_item, queued], fetchvals=[BASE, BASE])
    result = await store(due).commit(lease())
    assert result.status == MailboxStatus.QUEUED
    assert any(kind == "execute" and "session.ready.v2" in str(args[0]) for kind, args in due.calls)

    future = BASE + timedelta(minutes=5)
    waiting = mailbox_row(
        status=MailboxStatus.RETRY_WAIT.value, accepted=2, resolved=1, generation=3, retry_at=future
    )
    future_connection = Connection(
        fetchrows=[owned, item_row(retry_at=future), waiting],
        fetchvals=[BASE, BASE],
    )
    result = await store(future_connection).commit(lease())
    assert result.status == MailboxStatus.RETRY_WAIT
    assert not any(
        kind == "execute" and "session.ready.v2" in str(args[0])
        for kind, args in future_connection.calls
    )


@pytest.mark.asyncio
async def test_recovery_scheduler_functions_validate_limits_and_use_named_sql() -> None:
    connection = Connection(fetchvals=[4, 5, 6])
    mailbox_store = store(connection)
    assert await mailbox_store.sweep_expired_leases(owner_id="recovery-a", limit=10) == 4
    assert await mailbox_store.schedule_retries(owner_id="recovery-a", limit=10) == 5
    assert await mailbox_store.reconcile_sessions(owner_id="recovery-a", limit=10) == 6
    statements = [str(args[0]) for kind, args in connection.calls if kind == "fetchval"]
    assert "sweep_expired_session_leases" in statements[0]
    assert "schedule_session_mailbox_retries" in statements[1]
    assert "reconcile_session_mailboxes" in statements[2]
    reconcile_call = next(
        args
        for kind, args in connection.calls
        if kind == "fetchval" and "reconcile" in str(args[0])
    )
    assert reconcile_call[1:] == (10, 30)
    for method in (
        mailbox_store.sweep_expired_leases,
        mailbox_store.schedule_retries,
        mailbox_store.reconcile_sessions,
    ):
        with pytest.raises(ValueError, match="between 1 and 1000"):
            await method(owner_id="recovery-a", limit=0)
        with pytest.raises(ValueError, match="between 1 and 1000"):
            await method(owner_id="recovery-a", limit=1001)


@pytest.mark.asyncio
async def test_fenced_commit_rejects_wrong_owner_epoch_sequence_and_expiry() -> None:
    current = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="other",
        epoch=4,
        expires=BASE + timedelta(seconds=30),
    )
    for wrong in (
        current,
        {**current, "status": MailboxStatus.QUEUED.value},
        {**current, "processing_sequence": 2},
        {**current, "processing_inbound_id": UUID("33333333-3333-3333-3333-333333333333")},
        {**current, "lease_epoch": 9},
        {**current, "lease_expires_at": BASE - timedelta(seconds=1)},
    ):
        connection = Connection(fetchrows=[wrong], fetchvals=[BASE])
        with pytest.raises(FencingConflict):
            await store(connection).commit(lease())


@pytest.mark.asyncio
async def test_accept_and_ready_outbox_helper_preserve_db_clock_and_event_shape() -> None:
    inbound_id = uuid4()
    updated = mailbox_row(status=MailboxStatus.QUEUED.value, generation=1)
    connection = Connection(
        fetchrows=[
            mailbox_row(status=MailboxStatus.IDLE.value, accepted=0, generation=0),
            None,
            updated,
        ],
        fetchvals=[BASE],
    )
    result = await store(connection).accept(TENANT, SESSION, str(inbound_id), priority=5)
    assert result.status == MailboxStatus.QUEUED
    assert result.queue_generation == 1
    update_sql = next(
        str(args[0])
        for kind, args in connection.calls
        if kind == "fetchrow" and "UPDATE session_mailboxes" in str(args[0])
    )
    assert "clock_timestamp()" in update_sql
    outbox_sql = next(
        str(args[0])
        for kind, args in connection.calls
        if kind == "execute" and "outbox_events" in str(args[0])
    )
    assert "session.ready.v2" in outbox_sql

    helper_connection = Connection()
    await PostgresSessionMailboxStore._emit_ready_outbox(
        helper_connection,
        {"tenant_id": TENANT, "session_id": SESSION, "queue_generation": 9},
        {"inbound_id": INBOUND, "priority": 7, "trace_id": "trace-helper"},
    )
    args = next(args for kind, args in helper_connection.calls if kind == "execute")
    assert args[1:5] == (TENANT, SESSION, 9, 7)
    assert args[5] == "trace-helper"
