from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.unit.test_postgres_repository import (
    Connection,
    Pool,
    session_row,
)
from tests.unit.test_postgres_repository import acceptance as make_acceptance
from trpc_service.storage.models import MailboxClaimStatus
from trpc_service.storage.postgres import PostgresRuntimeRepository


def _mailbox_row(
    accepted,
    *,
    status: str,
    accepted_sequence: int,
    resolved_sequence: int,
    lease_owner: str | None = None,
    lease_expires_at: datetime | None = None,
    lease_epoch: int = 0,
) -> dict[str, object]:
    return {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "status": status,
        "accepted_sequence": accepted_sequence,
        "resolved_sequence": resolved_sequence,
        "processing_sequence": None,
        "processing_inbound_id": None,
        "queue_generation": max(accepted_sequence, 1),
        "lease_owner": lease_owner,
        "lease_epoch": lease_epoch,
        "lease_expires_at": lease_expires_at,
        "retry_count": 0,
        "attempt": 0,
        "priority": 0,
        "retry_at": None,
        "updated_at": datetime.now(UTC),
    }


def _sql_calls(connection: Connection) -> list[str]:
    return [args[0] for kind, args in connection.calls if kind == "execute"]


def _assert_no_scheduler_state_mutation(connection: Connection) -> None:
    """Ignore tenant-context setup, then reject all lease/turn/input writes."""

    forbidden = (
        "UPDATE session_mailboxes",
        "UPDATE sessions",
        "INSERT INTO session_turns",
        "UPDATE session_turns",
        "UPDATE inbound_messages",
    )
    assert not any(
        any(fragment in statement for fragment in forbidden) for statement in _sql_calls(connection)
    )


def _assert_mailbox_lock_precedes_session_lock(connection: Connection) -> None:
    fetchrow_sql = [
        args[0] for kind, args in connection.calls if kind == "fetchrow" and "FOR UPDATE" in args[0]
    ]
    mailbox_lock = next(
        index
        for index, statement in enumerate(fetchrow_sql)
        if "FROM session_mailboxes" in statement
    )
    session_lock = next(
        index for index, statement in enumerate(fetchrow_sql) if "FROM sessions" in statement
    )
    assert mailbox_lock < session_lock


@pytest.mark.asyncio
async def test_v2_claim_returns_running_for_an_active_v1_session_lease() -> None:
    accepted = await make_acceptance()
    now = datetime.now(UTC)
    event_id = uuid4()
    inbound_id = uuid4()
    mailbox = _mailbox_row(
        accepted,
        status="QUEUED",
        accepted_sequence=1,
        resolved_sequence=0,
    )
    item = {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "sequence": 1,
        "inbound_id": inbound_id,
        "trace_id": accepted.context.trace_id,
        "priority": 0,
        "retry_count": 0,
        "attempt": 0,
        "retry_at": None,
    }
    inbound = {
        "tenant_id": accepted.context.tenant_id,
        "inbound_id": inbound_id,
        "app_id": accepted.context.app_id,
        "principal_id": accepted.context.principal_id,
        "status": "accepted",
    }
    ready_event = {
        "outbox_id": event_id,
        "tenant_id": accepted.context.tenant_id,
        "aggregate_type": "session",
        "aggregate_id": accepted.context.session_id,
        "event_type": "session.ready.v2",
        "payload_json": {"generation": 1},
    }
    connection = Connection(
        fetchrows=[
            ready_event,
            mailbox,
            item,
            inbound,
            None,
            session_row(
                accepted,
                owner="v1-worker",
                expires=now + timedelta(minutes=1),
                epoch=7,
            ),
        ],
        fetchvals=[now],
    )

    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="v2-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event_id),
    )

    assert claim.status == MailboxClaimStatus.RUNNING
    assert claim.lease is None
    assert claim.execution_lease is None
    _assert_no_scheduler_state_mutation(connection)
    _assert_mailbox_lock_precedes_session_lock(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("mailbox_status", ["QUEUED", "RUNNING"])
async def test_v1_acquire_refuses_unresolved_or_active_v2_mailbox(
    mailbox_status: str,
) -> None:
    accepted = await make_acceptance()
    now = datetime.now(UTC)
    active = mailbox_status == "RUNNING"
    mailbox = _mailbox_row(
        accepted,
        status=mailbox_status,
        accepted_sequence=1,
        resolved_sequence=1 if active else 0,
        lease_owner="v2-worker" if active else None,
        lease_expires_at=now + timedelta(minutes=1) if active else None,
        lease_epoch=8 if active else 0,
    )
    connection = Connection(
        fetchrows=[mailbox],
        fetchvals=[now] if active else [],
    )

    lease = await PostgresRuntimeRepository(Pool(connection)).acquire(
        acceptance=accepted,
        worker_id="v1-worker",
        lease_for=timedelta(seconds=30),
    )

    assert lease is None
    _assert_no_scheduler_state_mutation(connection)
    assert not any("INSERT INTO sessions" in statement for statement in _sql_calls(connection))


@pytest.mark.asyncio
async def test_v1_fence_locks_mailbox_before_session() -> None:
    accepted = await make_acceptance()
    now = datetime.now(UTC)
    connection = Connection(
        fetchrows=[
            _mailbox_row(
                accepted,
                status="IDLE",
                accepted_sequence=0,
                resolved_sequence=0,
            ),
            session_row(
                accepted,
                owner="v2-worker",
                expires=now + timedelta(minutes=1),
                epoch=9,
            ),
            None,
        ],
        fetchvals=[now],
    )

    lease = await PostgresRuntimeRepository(Pool(connection)).acquire(
        acceptance=accepted,
        worker_id="v1-worker",
        lease_for=timedelta(seconds=30),
    )

    assert lease is None
    _assert_no_scheduler_state_mutation(connection)
    _assert_mailbox_lock_precedes_session_lock(connection)
