"""Message receipt and audit repository for idempotent message execution."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, Protocol

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryDataError,
    ExecutionAuditRepositoryUnavailableError,
    append_events_in_transaction,
    validate_events_for_receipt,
)
from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.schema import message_audit_events, message_receipts
from trpc_service.transport.models import WorkerErrorCode, WorkerTask


class ReceiptAction(StrEnum):
    """What the caller should do after claiming a receipt."""

    EXECUTE = "execute"
    REPLAY = "replay"
    IN_PROGRESS = "in_progress"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class MessageClaim:
    """Result of claiming a message receipt."""

    action: ReceiptAction
    receipt_id: uuid.UUID | None
    response_text: str | None
    error_code: WorkerErrorCode | None


@dataclass(frozen=True)
class MessageAuditEvent:
    """Immutable audit event for a message receipt transition."""

    audit_id: uuid.UUID
    receipt_id: uuid.UUID
    tenant_id: str
    app_id: str
    channel: str
    user_id: str
    session_id: str
    message_id: str
    event_type: str
    request_id: uuid.UUID
    config_version: int
    error_code: str | None
    latency_ms: int | None
    message_digest: str
    response_digest: str | None
    occurred_at: object


class MessageReceiptRepository(Protocol):
    """Async protocol for message receipt claim/complete/fail with audit."""

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        ...

    async def complete(
            self,
            receipt_id: uuid.UUID,
            response_text: str,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        ...

    async def fail(
            self,
            receipt_id: uuid.UUID,
            error_code: WorkerErrorCode,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        ...

    async def list_audit(
        self,
        tenant_id: str,
        message_id: str,
        limit: int,
    ) -> tuple[MessageAuditEvent, ...]:
        ...

    async def count_processing_by_tenant(self, tenant_id: str) -> int:
        """Count receipts currently in ``processing`` state for a tenant.

        R1B: the offline state migration refuses to run while any message
        execution for the tenant is still in flight.
        """
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


class MessageReceiptRepositoryConfigurationError(ValueError):
    """Repository could not be configured."""


class MessageReceiptRepositoryUnavailableError(RuntimeError):
    """Repository backend is not reachable or not ready."""


class MessageReceiptRepositoryDataError(RuntimeError):
    """Repository data is corrupt or conflicts with existing data."""


def _validate_terminal_execution_events(
    receipt_id: uuid.UUID,
    receipt: Mapping,
    events: tuple[ExecutionAuditEvent, ...],
) -> None:
    # Shared with the approval pause/finalize boundary: every identity field
    # must match the receipt row before the terminal transition commits.
    try:
        validate_events_for_receipt(receipt_id, receipt, events)
    except ExecutionAuditRepositoryDataError as exc:
        raise MessageReceiptRepositoryDataError(str(exc)) from None


def compute_message_digest(message_text: str) -> str:
    """Compute SHA-256 hex digest of the raw message text."""
    return hashlib.sha256(message_text.encode("utf-8")).hexdigest()


def compute_response_digest(response_text: str) -> str:
    """Compute SHA-256 hex digest of the response text."""
    return hashlib.sha256(response_text.encode("utf-8")).hexdigest()


class SqlMessageReceiptRepository:
    """PostgreSQL-backed message receipt repository with atomic audit."""

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> SqlMessageReceiptRepository:
        """Create a repository using ``TRPC_DATABASE_URL`` from the environment."""
        from trpc_service.storage.database import DatabaseConfigurationError

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError as exc:
            raise MessageReceiptRepositoryConfigurationError(str(exc)) from exc
        engine = create_database_engine(settings)
        return cls(engine)

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        """Attempt to claim a receipt for the given task and message.

        Returns a MessageClaim indicating whether to execute, replay, or reject.
        """
        self._require_open()
        receipt_id = uuid.uuid4()
        message_digest = compute_message_digest(message_text)
        now = sa.text("now()")

        try:
            async with self._engine.begin() as conn:
                insert_stmt = postgresql_insert(message_receipts).values(
                    receipt_id=receipt_id,
                    tenant_id=task.tenant_id,
                    channel=task.channel,
                    user_id=task.user_id,
                    session_id=task.session_id,
                    message_id=task.message_id,
                    app_id=task.app_id,
                    config_version=task.config_version,
                    request_id=task.request_id,
                    message_digest=message_digest,
                    state="processing",
                    started_at=now,
                ).on_conflict_do_nothing(constraint="message_receipts_business_key")
                result = await conn.execute(insert_stmt)
                if result.rowcount == 0:
                    existing = await conn.execute(
                        sa.select(
                            message_receipts.c.receipt_id,
                            message_receipts.c.state,
                            message_receipts.c.message_digest,
                            message_receipts.c.response_text,
                            message_receipts.c.error_code,
                        ).where(
                            message_receipts.c.tenant_id == task.tenant_id,
                            message_receipts.c.channel == task.channel,
                            message_receipts.c.user_id == task.user_id,
                            message_receipts.c.session_id == task.session_id,
                            message_receipts.c.message_id == task.message_id,
                        ))
                    row = existing.first()
                    if row is None:
                        raise MessageReceiptRepositoryDataError("receipt insert failed but no existing row")
                    existing_digest = row._mapping["message_digest"]
                    if existing_digest != message_digest:
                        return MessageClaim(
                            action=ReceiptAction.CONFLICT,
                            receipt_id=None,
                            response_text=None,
                            error_code=WorkerErrorCode.IDEMPOTENCY_CONFLICT,
                        )
                    state = row._mapping["state"]
                    if state == "processing":
                        return MessageClaim(
                            action=ReceiptAction.IN_PROGRESS,
                            receipt_id=row._mapping["receipt_id"],
                            response_text=None,
                            error_code=WorkerErrorCode.MESSAGE_IN_PROGRESS,
                        )
                    if state == "completed":
                        return MessageClaim(
                            action=ReceiptAction.REPLAY,
                            receipt_id=row._mapping["receipt_id"],
                            response_text=row._mapping["response_text"],
                            error_code=None,
                        )
                    if state == "failed":
                        error_code_str = row._mapping["error_code"]
                        return MessageClaim(
                            action=ReceiptAction.REPLAY,
                            receipt_id=row._mapping["receipt_id"],
                            response_text=None,
                            error_code=WorkerErrorCode(error_code_str) if error_code_str else None,
                        )
                    raise MessageReceiptRepositoryDataError(f"unknown receipt state: {state}")

                audit_stmt = message_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    receipt_id=receipt_id,
                    tenant_id=task.tenant_id,
                    app_id=task.app_id,
                    channel=task.channel,
                    user_id=task.user_id,
                    session_id=task.session_id,
                    message_id=task.message_id,
                    event_type="accepted",
                    request_id=task.request_id,
                    config_version=task.config_version,
                    message_digest=message_digest,
                    occurred_at=now,
                )
                await conn.execute(audit_stmt)
                return MessageClaim(
                    action=ReceiptAction.EXECUTE,
                    receipt_id=receipt_id,
                    response_text=None,
                    error_code=None,
                )
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database query failed") from exc

    async def complete(
            self,
            receipt_id: uuid.UUID,
            response_text: str,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        """Mark a processing receipt as completed with the final response.

        ``execution_events`` (Stage 6B2) are appended in this same
        transaction: if a required governance audit cannot be written, the
        terminal transition aborts and the receipt stays ``processing`` —
        never a fake "completed" without its trail.
        """
        self._require_open()
        response_digest = compute_response_digest(response_text)
        now = sa.text("now()")

        try:
            async with self._engine.begin() as conn:
                receipt_row = (await conn.execute(
                    sa.select(
                        message_receipts.c.tenant_id,
                        message_receipts.c.app_id,
                        message_receipts.c.channel,
                        message_receipts.c.user_id,
                        message_receipts.c.session_id,
                        message_receipts.c.message_id,
                        message_receipts.c.request_id,
                        message_receipts.c.config_version,
                        message_receipts.c.message_digest,
                        message_receipts.c.state,
                    ).where(message_receipts.c.receipt_id == receipt_id))).first()
                if receipt_row is None:
                    raise MessageReceiptRepositoryDataError("receipt not found")
                if receipt_row._mapping["state"] != "processing":
                    raise MessageReceiptRepositoryDataError("receipt is not in processing state")
                _validate_terminal_execution_events(receipt_id, receipt_row._mapping, execution_events)

                update_stmt = (message_receipts.update().where(message_receipts.c.receipt_id == receipt_id).where(
                    message_receipts.c.state == "processing").values(
                        state="completed",
                        response_text=response_text,
                        finished_at=now,
                        latency_ms=latency_ms,
                    ))
                result = await conn.execute(update_stmt)
                if result.rowcount != 1:
                    raise MessageReceiptRepositoryDataError("receipt update failed")

                audit_stmt = message_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    receipt_id=receipt_id,
                    tenant_id=receipt_row._mapping["tenant_id"],
                    app_id=receipt_row._mapping["app_id"],
                    channel=receipt_row._mapping["channel"],
                    user_id=receipt_row._mapping["user_id"],
                    session_id=receipt_row._mapping["session_id"],
                    message_id=receipt_row._mapping["message_id"],
                    event_type="completed",
                    request_id=receipt_row._mapping["request_id"],
                    config_version=receipt_row._mapping["config_version"],
                    message_digest=receipt_row._mapping["message_digest"],
                    response_digest=response_digest,
                    latency_ms=latency_ms,
                    occurred_at=now,
                )
                await conn.execute(audit_stmt)
                await append_events_in_transaction(conn, execution_events)
        except MessageReceiptRepositoryDataError:
            raise
        except ExecutionAuditRepositoryDataError:
            raise MessageReceiptRepositoryDataError("execution audit data is invalid") from None
        except ExecutionAuditRepositoryUnavailableError:
            raise MessageReceiptRepositoryUnavailableError("execution audit database query failed") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database query failed") from exc

    async def fail(
            self,
            receipt_id: uuid.UUID,
            error_code: WorkerErrorCode,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        """Mark a processing receipt as failed with the error code.

        ``execution_events`` are appended in this same transaction (see
        :meth:`complete` — required audits fail closed together with the
        terminal transition).
        """
        self._require_open()
        now = sa.text("now()")

        try:
            async with self._engine.begin() as conn:
                receipt_row = (await conn.execute(
                    sa.select(
                        message_receipts.c.tenant_id,
                        message_receipts.c.app_id,
                        message_receipts.c.channel,
                        message_receipts.c.user_id,
                        message_receipts.c.session_id,
                        message_receipts.c.message_id,
                        message_receipts.c.request_id,
                        message_receipts.c.config_version,
                        message_receipts.c.message_digest,
                        message_receipts.c.state,
                    ).where(message_receipts.c.receipt_id == receipt_id))).first()
                if receipt_row is None:
                    raise MessageReceiptRepositoryDataError("receipt not found")
                if receipt_row._mapping["state"] != "processing":
                    raise MessageReceiptRepositoryDataError("receipt is not in processing state")
                _validate_terminal_execution_events(receipt_id, receipt_row._mapping, execution_events)

                update_stmt = (message_receipts.update().where(message_receipts.c.receipt_id == receipt_id).where(
                    message_receipts.c.state == "processing").values(
                        state="failed",
                        error_code=error_code.value,
                        finished_at=now,
                        latency_ms=latency_ms,
                    ))
                result = await conn.execute(update_stmt)
                if result.rowcount != 1:
                    raise MessageReceiptRepositoryDataError("receipt update failed")

                audit_stmt = message_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    receipt_id=receipt_id,
                    tenant_id=receipt_row._mapping["tenant_id"],
                    app_id=receipt_row._mapping["app_id"],
                    channel=receipt_row._mapping["channel"],
                    user_id=receipt_row._mapping["user_id"],
                    session_id=receipt_row._mapping["session_id"],
                    message_id=receipt_row._mapping["message_id"],
                    event_type="failed",
                    request_id=receipt_row._mapping["request_id"],
                    config_version=receipt_row._mapping["config_version"],
                    error_code=error_code.value,
                    message_digest=receipt_row._mapping["message_digest"],
                    latency_ms=latency_ms,
                    occurred_at=now,
                )
                await conn.execute(audit_stmt)
                await append_events_in_transaction(conn, execution_events)
        except MessageReceiptRepositoryDataError:
            raise
        except ExecutionAuditRepositoryDataError:
            raise MessageReceiptRepositoryDataError("execution audit data is invalid") from None
        except ExecutionAuditRepositoryUnavailableError:
            raise MessageReceiptRepositoryUnavailableError("execution audit database query failed") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database query failed") from exc

    async def list_audit(
        self,
        tenant_id: str,
        message_id: str,
        limit: int,
    ) -> tuple[MessageAuditEvent, ...]:
        """Return audit events for a tenant and message ID, newest first."""
        self._require_open()
        stmt = (sa.select(message_audit_events).where(
            message_audit_events.c.tenant_id == tenant_id,
            message_audit_events.c.message_id == message_id,
        ).order_by(message_audit_events.c.occurred_at.desc()).limit(limit))

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(stmt)
                rows = result.fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database query failed") from exc

        events = []
        for row in rows:
            events.append(
                MessageAuditEvent(
                    audit_id=row._mapping["audit_id"],
                    receipt_id=row._mapping["receipt_id"],
                    tenant_id=row._mapping["tenant_id"],
                    app_id=row._mapping["app_id"],
                    channel=row._mapping["channel"],
                    user_id=row._mapping["user_id"],
                    session_id=row._mapping["session_id"],
                    message_id=row._mapping["message_id"],
                    event_type=row._mapping["event_type"],
                    request_id=row._mapping["request_id"],
                    config_version=row._mapping["config_version"],
                    error_code=row._mapping["error_code"],
                    latency_ms=row._mapping["latency_ms"],
                    message_digest=row._mapping["message_digest"],
                    response_digest=row._mapping["response_digest"],
                    occurred_at=row._mapping["occurred_at"],
                ))
        return tuple(events)

    async def count_processing_by_tenant(self, tenant_id: str) -> int:
        """Count receipts currently in ``processing`` state for a tenant."""
        self._require_open()
        stmt = sa.select(sa.func.count()).select_from(message_receipts).where(
            message_receipts.c.tenant_id == tenant_id,
            message_receipts.c.state == "processing",
        )
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(stmt)
                return int(result.scalar_one())
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database query failed") from exc

    async def check_ready(self) -> None:
        """Verify the database is reachable."""
        if self._closed:
            raise MessageReceiptRepositoryUnavailableError("repository is closed")
        try:
            await check_database_readiness(self._engine)
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise MessageReceiptRepositoryUnavailableError("database is not reachable") from exc

    async def close(self) -> None:
        """Dispose the engine pool if owned. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._owns_engine:
            await self._engine.dispose()

    def _require_open(self) -> None:
        if self._closed:
            raise MessageReceiptRepositoryUnavailableError("repository is closed")


__all__ = [
    "MessageAuditEvent",
    "MessageClaim",
    "MessageReceiptRepository",
    "MessageReceiptRepositoryConfigurationError",
    "MessageReceiptRepositoryDataError",
    "MessageReceiptRepositoryUnavailableError",
    "ReceiptAction",
    "SqlMessageReceiptRepository",
    "compute_message_digest",
    "compute_response_digest",
]
