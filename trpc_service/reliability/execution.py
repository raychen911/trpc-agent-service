"""SQL-backed session lease, monotonically increasing fencing, and revision CAS."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from trpc_service.persistence import Database
from trpc_service.persistence.models import ExecutionAttemptRow
from trpc_service.persistence.models import SessionEventRow
from trpc_service.persistence.models import SessionLeaseRow
from trpc_service.persistence.models import SessionRow
from trpc_service.persistence.models import utcnow
from trpc_service.storage import ConflictError


class SessionBusyError(ConflictError):
    """No execution started; the caller may safely wait for the session."""


@dataclass(frozen=True)
class ExecutionLease:
    tenant_id: str
    app_id: str
    user_id: str
    session_id: str
    owner_id: str
    execution_id: str
    fencing_token: int


@dataclass(frozen=True)
class EventSnapshot:
    event_id: str
    seq_no: int
    payload: dict


@dataclass(frozen=True)
class SessionSnapshot:
    revision: int
    state: dict
    events: tuple[EventSnapshot, ...]


class SessionCoordinator:
    def __init__(self, database: Database) -> None:
        self._database = database

    def start_attempt(self, execution_id: str, worker_id: str, config_version: int, storage_revision: int) -> str:
        attempt_id = uuid.uuid4().hex
        with self._database.sessions.begin() as session:
            session.add(
                ExecutionAttemptRow(
                    attempt_id=attempt_id,
                    execution_id=execution_id,
                    worker_id=worker_id,
                    config_version=config_version,
                    storage_revision=storage_revision,
                    status="running",
                ))
        return attempt_id

    def finish_attempt(self, attempt_id: str, status: str, error_type: str | None = None) -> None:
        if status not in {"succeeded", "failed_retryable", "failed_final", "unknown_outcome"}:
            raise ValueError("invalid execution attempt terminal status")
        with self._database.sessions.begin() as session:
            row = session.get(ExecutionAttemptRow, attempt_id)
            if row is None:
                raise LookupError("execution attempt not found")
            if row.status != "running":
                raise ConflictError("execution attempt is already terminal")
            row.status = status
            row.error_type = error_type
            row.ended_at = utcnow()

    def recover_running_attempts(self, execution_id: str, status: str, error_type: str | None = None) -> None:
        """Close attempts left running by a crash, using durable final channel events."""
        if status not in {"succeeded", "failed_final", "unknown_outcome"}:
            raise ValueError("invalid recovered execution status")
        with self._database.sessions.begin() as session:
            rows = session.scalars(select(ExecutionAttemptRow).where(
                ExecutionAttemptRow.execution_id == execution_id, ExecutionAttemptRow.status == "running"))
            for row in rows:
                row.status = status
                row.error_type = error_type
                row.ended_at = utcnow()

    async def acquire(
        self,
        tenant_id: str,
        app_id: str,
        user_id: str,
        session_id: str,
        owner_id: str,
        lease_seconds: float,
        execution_id: str | None = None,
    ) -> ExecutionLease:
        now = utcnow()
        with self._database.sessions.begin() as session:
            row = session.scalar(
                select(SessionLeaseRow).where(
                    SessionLeaseRow.tenant_id == tenant_id,
                    SessionLeaseRow.session_id == session_id,
                ).with_for_update())
            if row is None:
                row = SessionLeaseRow(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    owner_id=owner_id,
                    fencing_token=1,
                    expires_at=now + timedelta(seconds=lease_seconds),
                )
                session.add(row)
            elif row.expires_at.replace(tzinfo=timezone_utc(row.expires_at)) > now and row.owner_id != owner_id:
                raise SessionBusyError("session lease is held by another worker")
            else:
                row.owner_id = owner_id
                row.fencing_token += 1
                row.expires_at = now + timedelta(seconds=lease_seconds)
            try:
                session.flush()
            except IntegrityError as error:
                raise SessionBusyError("session lease was concurrently created") from error
            token = row.fencing_token
        return ExecutionLease(tenant_id, app_id, user_id, session_id, owner_id, execution_id or uuid.uuid4().hex, token)

    async def renew(self, lease: ExecutionLease, lease_seconds: float) -> None:
        now = utcnow()
        with self._database.sessions.begin() as session:
            result = session.execute(update(SessionLeaseRow).where(
                SessionLeaseRow.tenant_id == lease.tenant_id,
                SessionLeaseRow.session_id == lease.session_id,
                SessionLeaseRow.owner_id == lease.owner_id,
                SessionLeaseRow.fencing_token == lease.fencing_token,
                SessionLeaseRow.expires_at > now,
            ).values(expires_at=now + timedelta(seconds=lease_seconds)))
            if result.rowcount != 1:
                raise ConflictError("session lease lost")

    def has_attempt(self, execution_id: str) -> bool:
        with self._database.sessions() as session:
            return session.scalar(select(ExecutionAttemptRow.attempt_id).where(
                ExecutionAttemptRow.execution_id == execution_id).limit(1)) is not None

    async def commit(
        self,
        lease: ExecutionLease,
        *,
        expected_revision: int,
        event_id: str,
        event_payload: dict,
        state_delta: dict | None = None,
        event_type: str = "event",
        author: str | None = None,
        invocation_id: str | None = None,
    ) -> int:
        with self._database.sessions.begin() as session:
            active = session.scalar(
                select(SessionLeaseRow).where(
                    SessionLeaseRow.tenant_id == lease.tenant_id,
                    SessionLeaseRow.session_id == lease.session_id,
                ).with_for_update())
            if (active is None or active.owner_id != lease.owner_id or active.fencing_token != lease.fencing_token
                    or active.expires_at.replace(tzinfo=timezone_utc(active.expires_at)) <= utcnow()):
                raise ConflictError("fencing token is stale")
            existing_event = session.get(SessionEventRow, (lease.tenant_id, lease.session_id, event_id))
            if existing_event is not None:
                if existing_event.content_json != event_payload:
                    raise ConflictError("event id was reused with different content")
                current = session.get(
                    SessionRow,
                    (lease.tenant_id, lease.app_id, lease.user_id, lease.session_id),
                )
                assert current is not None
                return current.revision
            row = session.get(SessionRow, (lease.tenant_id, lease.app_id, lease.user_id, lease.session_id))
            if row is None:
                row = SessionRow(
                    tenant_id=lease.tenant_id,
                    app_id=lease.app_id,
                    user_id=lease.user_id,
                    session_id=lease.session_id,
                )
                session.add(row)
                session.flush()
            if row.revision != expected_revision:
                raise ConflictError(f"revision conflict: expected {expected_revision}, actual {row.revision}")
            if lease.fencing_token < row.last_fencing_token:
                raise ConflictError("fencing token is stale")
            seq_no = row.next_event_seq
            new_state = dict(row.state_json)
            new_state.update(state_delta or {})
            session.add(
                SessionEventRow(
                    tenant_id=lease.tenant_id,
                    session_id=lease.session_id,
                    event_id=event_id,
                    app_id=lease.app_id,
                    user_id=lease.user_id,
                    seq_no=seq_no,
                    execution_id=lease.execution_id,
                    invocation_id=invocation_id,
                    author=author,
                    event_type=event_type,
                    content_json=event_payload,
                    state_delta_json=state_delta or {},
                ))
            row.revision += 1
            row.next_event_seq += 1
            row.state_json = new_state
            row.last_fencing_token = lease.fencing_token
            return row.revision

    async def release(self, lease: ExecutionLease) -> None:
        """Expire a lease only when the caller still owns its fencing token."""

        with self._database.sessions.begin() as session:
            active = session.scalar(
                select(SessionLeaseRow).where(
                    SessionLeaseRow.tenant_id == lease.tenant_id,
                    SessionLeaseRow.session_id == lease.session_id,
                ).with_for_update())
            if active is None:
                return
            if active.owner_id != lease.owner_id or active.fencing_token != lease.fencing_token:
                raise ConflictError("fencing token is stale")
            active.expires_at = utcnow()

    def snapshot(self, tenant_id: str, app_id: str, user_id: str, session_id: str) -> SessionSnapshot:
        with self._database.sessions() as session:
            row = session.get(SessionRow, (tenant_id, app_id, user_id, session_id))
            if row is None:
                return SessionSnapshot(0, {}, ())
            events = session.scalars(
                select(SessionEventRow).where(
                    SessionEventRow.tenant_id == tenant_id,
                    SessionEventRow.session_id == session_id,
                ).order_by(SessionEventRow.seq_no))
            return SessionSnapshot(
                row.revision,
                dict(row.state_json),
                tuple(EventSnapshot(item.event_id, item.seq_no, dict(item.content_json)) for item in events),
            )

    def events_for_execution(self, tenant_id: str, session_id: str, execution_id: str) -> tuple[EventSnapshot, ...]:
        with self._database.sessions() as session:
            events = session.scalars(
                select(SessionEventRow).where(
                    SessionEventRow.tenant_id == tenant_id,
                    SessionEventRow.session_id == session_id,
                    SessionEventRow.execution_id == execution_id,
                ).order_by(SessionEventRow.seq_no))
            return tuple(EventSnapshot(item.event_id, item.seq_no, dict(item.content_json)) for item in events)


def timezone_utc(value):
    """SQLite returns naive datetimes even for timezone-aware columns."""

    from datetime import timezone

    return value.tzinfo or timezone.utc
