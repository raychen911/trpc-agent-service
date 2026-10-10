from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.unit.test_postgres_repository import (
    Connection,
    Pool,
    inbound_row,
    session_row,
)
from tests.unit.test_postgres_repository import (
    acceptance as make_acceptance,
)
from trpc_service.storage.models import (
    MailboxClaimStatus,
    SessionLease,
    SessionSnapshot,
    StoredEvent,
    TurnCommit,
)
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)


def mailbox_row(
    accepted,
    *,
    status: str = "RUNNING",
    accepted_sequence: int = 1,
    resolved_sequence: int = 0,
    sequence: int | None = 1,
    inbound_id: str | None = None,
    owner: str | None = "worker-a",
    epoch: int = 4,
    expires: datetime | None = NOW + timedelta(minutes=1),
    generation: int = 1,
    retry_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "status": status,
        "accepted_sequence": accepted_sequence,
        "resolved_sequence": resolved_sequence,
        "processing_sequence": sequence,
        "processing_inbound_id": inbound_id or accepted.inbound_id,
        "queue_generation": generation,
        "lease_owner": owner,
        "lease_epoch": epoch,
        "lease_expires_at": expires,
        "retry_count": 0,
        "attempt": 1,
        "priority": 0,
        "retry_at": retry_at,
        "updated_at": NOW,
    }


def item_row(accepted, *, sequence: int = 1) -> dict[str, object]:
    return {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "sequence": sequence,
        "inbound_id": accepted.inbound_id,
        "trace_id": accepted.context.trace_id,
        "priority": 0,
        "retry_count": 0,
        "attempt": 1,
        "retry_at": None,
    }


def ready_event(accepted, *, event_id: str | None = None, generation: int = 1) -> dict[str, object]:
    return {
        "outbox_id": event_id or str(uuid4()),
        "tenant_id": accepted.context.tenant_id,
        "aggregate_type": "session",
        "aggregate_id": accepted.context.session_id,
        "event_type": "session.ready.v2",
        "payload_json": {"generation": generation},
    }


def turn_row(accepted, lease: SessionLease, *, status: str = "processing") -> dict[str, object]:
    return {
        "turn_id": lease.turn_id,
        "inbound_id": accepted.inbound_id,
        "status": status,
        "fencing_token": lease.fencing_token,
    }


def session_row_for(accepted, lease: SessionLease) -> dict[str, object]:
    return session_row(
        accepted,
        owner=lease.worker_id,
        expires=NOW + timedelta(minutes=1),
        epoch=lease.fencing_token,
    )


def make_lease(accepted, *, worker_id: str = "worker-a", epoch: int = 4) -> SessionLease:
    return SessionLease(
        tenant_id=accepted.context.tenant_id,
        session_id=accepted.context.session_id,
        turn_id=str(uuid4()),
        inbound_id=accepted.inbound_id,
        worker_id=worker_id,
        fencing_token=epoch,
        expires_at=NOW + timedelta(minutes=1),
        snapshot=SessionSnapshot(
            tenant_id=accepted.context.tenant_id,
            app_id=accepted.context.app_id,
            session_id=accepted.context.session_id,
            principal_id=accepted.context.principal_id,
        ),
    )


@pytest.mark.asyncio
async def test_v2_claim_fails_closed_for_missing_or_malformed_event_id() -> None:
    accepted = await make_acceptance()
    missing_connection = Connection()
    missing = await PostgresRuntimeRepository(Pool(missing_connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
    )
    assert missing.status == MailboxClaimStatus.STALE
    assert not missing_connection.calls

    malformed_connection = Connection()
    malformed = await PostgresRuntimeRepository(Pool(malformed_connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_event_id="not-a-uuid",
    )
    assert malformed.status == MailboxClaimStatus.STALE
    assert not any(kind == "fetchrow" for kind, _ in malformed_connection.calls)


@pytest.mark.asyncio
async def test_v2_claim_rejects_stale_event_and_active_mailbox() -> None:
    accepted = await make_acceptance()
    stale_event = ready_event(accepted, generation=2)
    stale_connection = Connection(fetchrows=[stale_event])
    stale = await PostgresRuntimeRepository(Pool(stale_connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(stale_event["outbox_id"]),
    )
    assert stale.status == MailboxClaimStatus.STALE

    event = ready_event(accepted)
    active = mailbox_row(accepted, status="RUNNING")
    active_connection = Connection(fetchrows=[event, active], fetchvals=[NOW])
    running = await PostgresRuntimeRepository(Pool(active_connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event["outbox_id"]),
    )
    assert running.status == MailboxClaimStatus.RUNNING
    assert running.execution_lease is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_field",
    [
        "tenant_id",
        "aggregate_type",
        "aggregate_id",
        "event_type",
        "generation_bool",
        "generation_string",
        "generation_zero",
        "generation_missing",
        "missing_event",
        "expected_generation",
    ],
)
async def test_v2_claim_rejects_every_unauthenticated_ready_event_shape(
    invalid_field: str,
) -> None:
    """A Redis wake-up must not authorize a claim unless every event field matches."""

    accepted = await make_acceptance()
    event = ready_event(accepted)
    expected_generation = 1
    if invalid_field == "tenant_id":
        event["tenant_id"] = "tenant-other"
    elif invalid_field == "aggregate_type":
        event["aggregate_type"] = "other"
    elif invalid_field == "aggregate_id":
        event["aggregate_id"] = "other-session"
    elif invalid_field == "event_type":
        event["event_type"] = "session.ready.other"
    elif invalid_field == "generation_bool":
        event["payload_json"] = {"generation": True}
    elif invalid_field == "generation_string":
        event["payload_json"] = {"generation": "1"}
    elif invalid_field == "generation_zero":
        event["payload_json"] = {"generation": 0}
    elif invalid_field == "generation_missing":
        event["payload_json"] = {}
    elif invalid_field == "missing_event":
        pass
    else:
        expected_generation = 2

    connection = Connection(fetchrows=[None if invalid_field == "missing_event" else event])
    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=expected_generation,
        expected_event_id=str(event["outbox_id"]),
    )

    assert claim.status == MailboxClaimStatus.STALE
    assert claim.execution_lease is None
    assert not any(
        kind == "execute" and "set_config" not in args[0] for kind, args in connection.calls
    )


@pytest.mark.asyncio
async def test_v2_claim_marks_missing_mailbox_stale_for_authenticated_generation() -> None:
    accepted = await make_acceptance()
    event = ready_event(accepted)
    connection = Connection(fetchrows=[event, None])

    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event["outbox_id"]),
    )

    assert claim.status == MailboxClaimStatus.STALE
    assert claim.mailbox.session_id == accepted.context.session_id


@pytest.mark.asyncio
async def test_v2_claim_takes_over_expired_mailbox_and_fences_new_turn() -> None:
    accepted = await make_acceptance()
    event = ready_event(accepted)
    item = item_row(accepted)
    inbound = inbound_row(accepted)
    session = session_row(accepted, owner="old-worker", expires=NOW - timedelta(seconds=1), epoch=4)
    mailbox = mailbox_row(
        accepted,
        status="QUEUED",
        sequence=None,
        owner=None,
        expires=None,
    )
    claimed = mailbox_row(
        accepted,
        status="RUNNING",
        owner="new-worker",
        epoch=5,
    )
    connection = Connection(
        fetchrows=[event, mailbox, item, inbound, None, session, claimed],
        fetchvals=[NOW],
    )

    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="new-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event["outbox_id"]),
    )

    assert claim.status == MailboxClaimStatus.CLAIMED
    assert claim.execution_lease is not None
    assert claim.execution_lease.fencing_token == 5
    assert claim.execution_lease.worker_id == "new-worker"
    assert any("UPDATE sessions" in args[0] for kind, args in connection.calls if kind == "execute")
    assert any(
        "UPDATE session_mailboxes" in args[0]
        for kind, args in connection.calls
        if kind == "fetchrow"
    )


@pytest.mark.asyncio
async def test_v2_committed_redelivery_clears_only_matching_session_fence() -> None:
    """Duplicate repair must not clear an unrelated active session lease."""

    accepted = await make_acceptance()
    event = ready_event(accepted)
    mailbox = mailbox_row(
        accepted,
        status="RUNNING",
        owner="old-worker",
        epoch=4,
        expires=NOW - timedelta(seconds=1),
    )
    item = item_row(accepted)
    inbound = inbound_row(accepted, status="committed")
    resolved = mailbox_row(
        accepted,
        status="IDLE",
        sequence=None,
        owner=None,
        epoch=4,
        expires=None,
    )
    connection = Connection(
        fetchrows=[event, mailbox, item, inbound, None, resolved],
        fetchvals=[NOW],
    )

    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="new-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event["outbox_id"]),
    )

    assert claim.status == MailboxClaimStatus.EMPTY
    session_updates = [
        args
        for kind, args in connection.calls
        if kind == "execute" and "UPDATE sessions" in args[0]
    ]
    assert len(session_updates) == 1
    assert "lease_owner IS NOT DISTINCT FROM $3" in session_updates[0][0]
    assert "lease_epoch=$4" in session_updates[0][0]
    assert session_updates[0][1:] == (
        accepted.context.tenant_id,
        accepted.context.session_id,
        "old-worker",
        4,
    )


@pytest.mark.asyncio
async def test_v2_renew_uses_owner_epoch_and_rejects_expired_or_fenced_lease() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    renewed_until = NOW + timedelta(minutes=2)
    connection = Connection(fetchvals=[renewed_until, renewed_until])
    renewed = await PostgresRuntimeRepository(Pool(connection)).renew_session_ready(
        lease, lease_for=timedelta(seconds=30)
    )
    assert renewed.expires_at == renewed_until
    sql = [args[0] for kind, args in connection.calls if kind == "fetchval"]
    assert len(sql) == 2
    assert all("lease_owner=$" in statement and "lease_epoch=$" in statement for statement in sql)

    rejected_connection = Connection(fetchvals=[None])
    with pytest.raises(FencingConflict, match="mailbox lease"):
        await PostgresRuntimeRepository(Pool(rejected_connection)).renew_session_ready(
            lease, lease_for=timedelta(seconds=30)
        )
    with pytest.raises(ValueError, match="positive"):
        await PostgresRuntimeRepository(Pool(Connection())).renew_session_ready(
            lease, lease_for=timedelta(0)
        )


@pytest.mark.asyncio
async def test_v2_commit_rejects_stale_lease_before_writing_any_state() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    mailbox = mailbox_row(accepted, owner="other-worker")
    connection = Connection(fetchrows=[mailbox], fetchvals=[NOW])
    with pytest.raises(FencingConflict, match="commit"):
        await PostgresRuntimeRepository(Pool(connection)).commit_session_ready(
            TurnCommit(context=accepted.context, lease=lease, state={}, events=())
        )
    # The tenant transaction always sets the RLS context.  No business write
    # may happen after the fence check fails.
    assert not any(
        kind == "execute" and "set_config" not in args[0] for kind, args in connection.calls
    )


@pytest.mark.asyncio
async def test_v2_commit_resolves_head_and_emits_post_turn_without_next_ready() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    mailbox = mailbox_row(accepted)
    inbound = inbound_row(accepted, status="processing")
    session = {**session_row_for(accepted, lease), "next_sequence": 1}
    idle_mailbox = mailbox_row(
        accepted,
        status="IDLE",
        accepted_sequence=1,
        resolved_sequence=1,
        sequence=None,
        inbound_id=None,
        owner=None,
        epoch=4,
        expires=None,
    )
    connection = Connection(
        fetchrows=[
            mailbox,
            session,
            turn_row(accepted, lease),
            item_row(accepted),
            inbound,
            {"session_id": accepted.context.session_id},
            None,
            idle_mailbox,
        ],
        fetchvals=[NOW, NOW],
    )
    result = await PostgresRuntimeRepository(Pool(connection)).commit_session_ready(
        TurnCommit(
            context=accepted.context,
            lease=lease,
            state={"answer": "ok"},
            events=(StoredEvent(event_id=str(uuid4()), author="agent", timestamp=1, event={}),),
        )
    )
    assert result.turn_id == lease.turn_id
    assert result.first_sequence == 1
    assert result.last_sequence == 1
    sql = [args[0] for kind, args in connection.calls if kind == "execute"]
    assert any(
        "UPDATE session_turns" in statement and "committed" in statement for statement in sql
    )
    assert any(
        "UPDATE inbound_messages" in statement and "committed" in statement for statement in sql
    )
    assert any("UPDATE session_mailbox_items" in statement for statement in sql)
    assert any("post_turn.ready" in statement for statement in sql)


class ExpiringCommitConnection(Connection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rolled_back = False

    async def __aexit__(self, exc_type, exc, traceback):
        self.rolled_back = exc_type is not None
        return False


@pytest.mark.asyncio
async def test_v2_commit_rechecks_database_clock_after_event_writes() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    connection = ExpiringCommitConnection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
            None,  # final fenced UPDATE sessions observes the expired clock
        ],
        fetchvals=[NOW, NOW],
    )
    with pytest.raises(FencingConflict, match="expired before commit"):
        await PostgresRuntimeRepository(Pool(connection)).commit_session_ready(
            TurnCommit(
                context=accepted.context,
                lease=lease,
                state={"answer": "late"},
                events=(StoredEvent(event_id=str(uuid4()), author="agent", timestamp=1, event={}),),
            )
        )
    assert connection.rolled_back
    assert not any(
        kind == "execute" and "UPDATE session_turns" in args[0] for kind, args in connection.calls
    )


@pytest.mark.asyncio
async def test_v2_commit_rolls_back_when_final_mailbox_fence_update_affects_zero_rows() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    connection = ExpiringCommitConnection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
            {"session_id": accepted.context.session_id},
            None,  # no following item
            None,  # final mailbox owner/epoch/expiry check affects zero rows
        ],
        fetchvals=[NOW, NOW],
    )
    with pytest.raises(FencingConflict, match="mailbox lease expired before commit"):
        await PostgresRuntimeRepository(Pool(connection)).commit_session_ready(
            TurnCommit(
                context=accepted.context,
                lease=lease,
                state={"answer": "late-mailbox"},
                events=(StoredEvent(event_id=str(uuid4()), author="agent", timestamp=1, event={}),),
            )
        )

    assert connection.rolled_back
    assert not any(
        kind == "execute" and "post_turn.ready" in args[0] for kind, args in connection.calls
    )


@pytest.mark.asyncio
async def test_v2_retry_rejects_expired_lease_and_success_enters_retry_wait() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    stale_mailbox = mailbox_row(accepted, expires=NOW - timedelta(seconds=1))
    stale_connection = Connection(fetchrows=[stale_mailbox], fetchvals=[NOW])
    with pytest.raises(FencingConflict, match="retry"):
        await PostgresRuntimeRepository(Pool(stale_connection)).retry_session_ready(
            lease, error_type="timeout", delay=timedelta(seconds=5)
        )

    mailbox = mailbox_row(accepted)
    inbound = inbound_row(accepted, status="processing")
    turn = turn_row(accepted, lease)
    session = session_row_for(accepted, lease)
    connection = Connection(
        fetchrows=[mailbox, session, turn, item_row(accepted), inbound],
        fetchvals=[NOW],
    )
    await PostgresRuntimeRepository(Pool(connection)).retry_session_ready(
        lease, error_type="timeout", delay=timedelta(seconds=5)
    )
    sql = [args[0] for kind, args in connection.calls if kind == "execute"]
    assert any("status='failed'" in statement for statement in sql)
    assert any("status='accepted'" in statement for statement in sql)
    assert any("status='RETRY_WAIT'" in statement for statement in sql)

    with pytest.raises(ValueError, match="non-negative"):
        await PostgresRuntimeRepository(Pool(Connection())).retry_session_ready(
            lease, error_type="bad", delay=timedelta(seconds=-1)
        )


@pytest.mark.asyncio
async def test_v2_fail_resolves_head_and_rejects_wrong_fence() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    wrong = lease.model_copy(update={"fencing_token": lease.fencing_token + 1})
    wrong_mailbox = mailbox_row(accepted, epoch=lease.fencing_token)
    wrong_connection = Connection(fetchrows=[wrong_mailbox], fetchvals=[NOW])
    with pytest.raises(FencingConflict, match="fail"):
        await PostgresRuntimeRepository(Pool(wrong_connection)).fail_session_ready(
            wrong, error_type="poison"
        )

    mailbox = mailbox_row(accepted)
    inbound = inbound_row(accepted, status="processing")
    turn = turn_row(accepted, lease)
    session = session_row_for(accepted, lease)
    updated = mailbox_row(
        accepted,
        status="IDLE",
        accepted_sequence=1,
        resolved_sequence=1,
        sequence=None,
        inbound_id=None,
        owner=None,
        expires=None,
    )
    connection = Connection(
        fetchrows=[mailbox, session, turn, item_row(accepted), inbound, None, updated],
        fetchvals=[NOW],
    )
    await PostgresRuntimeRepository(Pool(connection)).fail_session_ready(lease, error_type="poison")
    sql = [args[0] for _, args in connection.calls]
    assert any("status='failed'" in statement for statement in sql)
    assert any("resolved_sequence" in statement for statement in sql)


class RollbackConnection(Connection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rolled_back = False
        self._execute_count = 0

    async def __aexit__(self, exc_type, exc, traceback):
        self.rolled_back = exc_type is not None
        return False

    async def execute(self, *args):
        self.calls.append(("execute", args))
        self._execute_count += 1
        if self._execute_count == 2:
            raise RuntimeError("injected commit write failure")
        return "UPDATE 1"


@pytest.mark.asyncio
async def test_v2_commit_exception_is_rolled_back_by_transaction_boundary() -> None:
    accepted = await make_acceptance()
    lease = make_lease(accepted)
    connection = RollbackConnection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
        ],
        fetchvals=[NOW, NOW],
    )
    with pytest.raises(RuntimeError, match="injected"):
        await PostgresRuntimeRepository(Pool(connection)).commit_session_ready(
            TurnCommit(
                context=accepted.context,
                lease=lease,
                state={},
                events=(StoredEvent(event_id=str(uuid4()), author="agent", timestamp=1, event={}),),
            )
        )
    assert connection.rolled_back


@pytest.mark.asyncio
async def test_postgres_scheduler_paths_use_mailbox_session_turn_lock_order() -> None:
    """The v1/v2 overlap must acquire shared rows in one SQL order.

    This uses the repository's recording connection rather than a mock call
    list: each assertion is against the actual SELECT statement emitted by
    the implementation.  A real mailbox row is supplied to the v1 calls so
    the optional legacy path exercises the mailbox-first branch.
    """

    accepted = await make_acceptance()
    lease = make_lease(accepted)
    mailbox = {"status": "IDLE"}

    v1_commit_connection = Connection(
        fetchrows=[
            mailbox,
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            session_row_for(accepted, lease),
        ],
        fetchvals=[NOW],
    )
    await PostgresRuntimeRepository(Pool(v1_commit_connection)).commit(
        TurnCommit(context=accepted.context, lease=lease, state={}, events=())
    )
    v1_commit_locks = [
        statement
        for kind, args in v1_commit_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert "FROM session_mailboxes" in v1_commit_locks[0]
    assert "FROM sessions" in v1_commit_locks[1]
    assert "FROM session_turns" in v1_commit_locks[2]

    v1_fail_connection = Connection(
        fetchrows=[mailbox, session_row_for(accepted, lease), turn_row(accepted, lease)]
    )
    await PostgresRuntimeRepository(Pool(v1_fail_connection)).fail(lease, error_type="test")
    v1_fail_locks = [
        statement
        for kind, args in v1_fail_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert "FROM session_mailboxes" in v1_fail_locks[0]
    assert "FROM sessions" in v1_fail_locks[1]
    assert "FROM session_turns" in v1_fail_locks[2]

    v1_acquire_connection = Connection(
        fetchrows=[
            {
                "status": "IDLE",
                "accepted_sequence": 0,
                "resolved_sequence": 0,
                "lease_expires_at": None,
            },
            session_row(accepted),
            None,
            {"accepted_at": NOW},
        ],
        fetchvals=[NOW, None],
        fetches=[[]],
    )
    acquired = await PostgresRuntimeRepository(Pool(v1_acquire_connection)).acquire(
        acceptance=accepted, worker_id="worker-a", lease_for=timedelta(seconds=30)
    )
    assert acquired is not None
    v1_acquire_locks = [
        statement
        for kind, args in v1_acquire_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert "FROM session_mailboxes" in v1_acquire_locks[0]
    assert "FROM sessions" in v1_acquire_locks[1]
    assert "FROM session_turns" in v1_acquire_locks[2]

    v2_connection = Connection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
            {"session_id": accepted.context.session_id},
            None,
            mailbox_row(
                accepted,
                status="IDLE",
                accepted_sequence=1,
                resolved_sequence=1,
                sequence=None,
                inbound_id=None,
                owner=None,
                expires=None,
            ),
        ],
        fetchvals=[NOW, NOW],
    )
    await PostgresRuntimeRepository(Pool(v2_connection)).commit_session_ready(
        TurnCommit(context=accepted.context, lease=lease, state={}, events=())
    )
    v2_locks = [
        statement
        for kind, args in v2_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert "FROM session_mailboxes" in v2_locks[0]
    assert "FROM sessions" in v2_locks[1]
    assert "FROM session_turns" in v2_locks[2]
    assert "FROM session_mailbox_items" in v2_locks[3]
    assert "FROM inbound_messages" in v2_locks[4]
    final_session_update = next(
        statement
        for kind, args in v2_connection.calls
        if kind == "fetchrow"
        and "UPDATE sessions" in (statement := args[0])
        and "RETURNING session_id" in statement
    )
    assert "lease_owner=$5" in final_session_update
    assert "lease_epoch=$6" in final_session_update
    assert "lease_expires_at > clock_timestamp()" in final_session_update
    final_mailbox_update = next(
        statement
        for kind, args in v2_connection.calls
        if kind == "fetchrow"
        and "UPDATE session_mailboxes" in (statement := args[0])
        and "RETURNING *" in statement
    )
    assert "lease_owner=$7" in final_mailbox_update
    assert "lease_epoch=$8" in final_mailbox_update
    assert "lease_expires_at > clock_timestamp()" in final_mailbox_update

    v2_retry_connection = Connection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
        ],
        fetchvals=[NOW],
    )
    await PostgresRuntimeRepository(Pool(v2_retry_connection)).retry_session_ready(
        lease, error_type="timeout", delay=timedelta(seconds=1)
    )
    v2_retry_locks = [
        statement
        for kind, args in v2_retry_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert [
        "session_mailboxes",
        "sessions",
        "session_turns",
        "session_mailbox_items",
        "inbound_messages",
    ] == [
        next(
            table
            for table in (
                "session_mailboxes",
                "sessions",
                "session_turns",
                "session_mailbox_items",
                "inbound_messages",
            )
            if f"FROM {table}" in statement
        )
        for statement in v2_retry_locks
    ]

    v2_fail_connection = Connection(
        fetchrows=[
            mailbox_row(accepted),
            session_row_for(accepted, lease),
            turn_row(accepted, lease),
            item_row(accepted),
            inbound_row(accepted, status="processing"),
            None,
            mailbox_row(
                accepted,
                status="IDLE",
                accepted_sequence=1,
                resolved_sequence=1,
                sequence=None,
                inbound_id=None,
                owner=None,
                expires=None,
            ),
        ],
        fetchvals=[NOW],
    )
    await PostgresRuntimeRepository(Pool(v2_fail_connection)).fail_session_ready(
        lease, error_type="poison"
    )
    v2_fail_locks = [
        statement
        for kind, args in v2_fail_connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in (statement := args[0])
    ]
    assert [
        "session_mailboxes",
        "sessions",
        "session_turns",
        "session_mailbox_items",
        "inbound_messages",
        "session_mailbox_items",
    ] == [
        next(
            table
            for table in (
                "session_mailboxes",
                "sessions",
                "session_turns",
                "session_mailbox_items",
                "inbound_messages",
            )
            if f"FROM {table}" in statement
        )
        for statement in v2_fail_locks
    ]
