"""PostgreSQL tool-approval repository (Stage 6A2).

Single source of truth for the human-approval state machine:
pending -> executing -> completed | rejected | failed.

- ``pause_first_request``: ONE transaction that verifies the original
  message-receipt row (identity + 'processing'), creates the pending approval
  + 'created' audit, and completes that receipt with the fixed pending reply
  + message audit.  The (receipt, function-call) unique key makes a
  redelivered pause return the existing row instead of double-creating.
- ``claim_decision``: row-lock CAS — exactly one EXECUTE; in-flight claims
  observe IN_PROGRESS; terminal claims REPLAY only for the identical
  (decision, decision_message_id); any identity mismatch or missing row is
  the single NOT_AVAILABLE outcome (no detail leak).
- ``finalize``: ONE transaction moving approval executing -> terminal (with
  audit) AND the decision receipt processing -> completed/failed (with message
  audit).  Any failure rolls the whole thing back, leaving the approval
  'executing' — fail-closed, never auto-reset.
- Stage 6D ``terminate_orphan``: the ONLY way an 'executing' row created by a
  crashed controlled execution can reach a terminal state — an explicit,
  Admin-authenticated, one-shot disposition to 'failed' in ONE transaction
  (approval + audit + decision receipt + message audit).  It never re-runs
  the tool and never resurrects state; age/identity guards reject anything
  that is not a provably orphaned record.

Audit rows carry digests and decision metadata only — never tool args or
reply text.  Database failure maps to a fixed Unavailable error; no
in-memory fallback.
"""

from __future__ import annotations

import uuid
from typing import Any, Mapping, Protocol

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.governance.approval import (
    ApprovalAction,
    ApprovalAuditEvent,
    ApprovalClaim,
    ApprovalDecision,
    ApprovalRequest,
    OrphanTermination,
    OrphanTerminationAction,
    OrphanedApproval,
    compute_args_digest,
)
from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryDataError,
    ExecutionAuditRepositoryUnavailableError,
    append_events_in_transaction,
    validate_events_for_receipt,
)
from trpc_service.storage.message_repository import compute_response_digest
from trpc_service.storage.schema import (
    message_audit_events,
    message_receipts,
    tool_approval_audit_events,
    tool_approval_requests,
)

_TERMINAL_STATES = frozenset({"completed", "rejected", "failed"})

#: Stage 6D disposition marker: the decision receipt of an orphaned
#: execution books the EXISTING fixed code — an orphaned controlled
#: execution "could not be completed", which is exactly what it is.
_ORPHAN_DISPOSITION_ERROR_CODE = "approval_execution_failed"


async def _write_terminal_execution_events(
    conn,
    receipt_id: uuid.UUID,
    identity: Mapping,
    events: tuple[ExecutionAuditEvent, ...],
) -> None:
    """Stage 6B2: write the receipt-bound audit facts in this SAME
    transaction, mapping audit errors onto the approval repository's fixed
    error taxonomy (fail-closed: a required audit aborts the terminal
    transition)."""
    try:
        validate_events_for_receipt(receipt_id, identity, events)
        await append_events_in_transaction(conn, events)
    except ExecutionAuditRepositoryDataError as exc:
        raise ToolApprovalRepositoryDataError(str(exc)) from None
    except ExecutionAuditRepositoryUnavailableError:
        raise ToolApprovalRepositoryUnavailableError("execution audit database query failed") from None


class ToolApprovalRepositoryConfigurationError(ValueError):
    """Repository could not be configured."""


class ToolApprovalRepositoryUnavailableError(RuntimeError):
    """Repository backend is not reachable or not ready."""


class ToolApprovalRepositoryDataError(RuntimeError):
    """Repository data is corrupt or an illegal transition was attempted."""


class ToolApprovalRepository(Protocol):
    """Async protocol for approval persistence, CAS and atomic finalize."""

    async def pause_first_request(
            self,
            *,
            approval_id: uuid.UUID,
            tenant_id: str,
            app_id: str,
            config_version: int,
            channel: str,
            user_id: str,
            session_id: str,
            receipt_id: uuid.UUID,
            function_call_id: str,
            tool_name: str,
            tool_args: Mapping[str, Any],
            pending_response: str,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> ApprovalRequest:
        ...

    async def claim_decision(
        self,
        approval_id: uuid.UUID,
        *,
        tenant_id: str,
        channel: str,
        user_id: str,
        session_id: str,
        decision: ApprovalDecision,
        decision_message_id: str,
    ) -> ApprovalClaim:
        ...

    async def finalize(
            self,
            approval_id: uuid.UUID,
            *,
            receipt_id: uuid.UUID,
            terminal_state: str,
            response_text: str | None,
            error_code: str | None,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        ...

    async def get(self, approval_id: uuid.UUID) -> ApprovalRequest | None:
        ...

    async def list_stale_executing(self, *, tenant_id: str, stale_seconds: int,
                                   limit: int) -> tuple[OrphanedApproval, ...]:
        """Executions stuck in 'executing' longer than the operator-declared
        threshold (Stage 6D disposition query)."""
        ...

    async def terminate_orphan(self, approval_id: uuid.UUID, *, tenant_id: str,
                               stale_seconds: int) -> OrphanTermination:
        """One-shot, atomic disposition of a provably orphaned 'executing'
        approval to 'failed'.  NEVER executes the tool; rejects active,
        pending, inconsistent and foreign-tenant records."""
        ...

    async def get_tool_args(self, approval_id: uuid.UUID) -> dict[str, Any] | None:
        """Restricted read of the stored tool args for the approved execution."""
        ...

    async def list_audit(self, approval_id: uuid.UUID, limit: int) -> tuple[ApprovalAuditEvent, ...]:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


def _require_nonblank(value: str, name: str) -> str:
    stripped = value.strip()
    if not stripped or len(stripped) > 200:
        raise ValueError(f"invalid {name}")
    return stripped


def _row_to_request(row: sa.engine.Row) -> ApprovalRequest:
    m = row._mapping
    return ApprovalRequest(
        approval_id=m["approval_id"],
        tenant_id=m["tenant_id"],
        app_id=m["app_id"],
        config_version=m["config_version"],
        channel=m["channel"],
        user_id=m["user_id"],
        session_id=m["session_id"],
        receipt_id=m["receipt_id"],
        function_call_id=m["function_call_id"],
        tool_name=m["tool_name"],
        args_digest=m["args_digest"],
        state=m["state"],
        decision=m["decision"],
        decision_message_id=m["decision_message_id"],
        response_text=m["response_text"],
    )


_REQUEST_COLUMNS = (
    tool_approval_requests.c.approval_id,
    tool_approval_requests.c.tenant_id,
    tool_approval_requests.c.app_id,
    tool_approval_requests.c.config_version,
    tool_approval_requests.c.channel,
    tool_approval_requests.c.user_id,
    tool_approval_requests.c.session_id,
    tool_approval_requests.c.receipt_id,
    tool_approval_requests.c.function_call_id,
    tool_approval_requests.c.tool_name,
    tool_approval_requests.c.args_digest,
    tool_approval_requests.c.state,
    tool_approval_requests.c.decision,
    tool_approval_requests.c.decision_message_id,
    tool_approval_requests.c.response_text,
)

_RECEIPT_COLUMNS = (
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
)


class SqlToolApprovalRepository:
    """Concrete PostgreSQL implementation of :class:`ToolApprovalRepository`."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SqlToolApprovalRepository:
        from trpc_service.storage.database import DatabaseConfigurationError

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError as exc:
            raise ToolApprovalRepositoryConfigurationError(str(exc)) from exc
        engine = create_database_engine(settings)
        return cls(engine)

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    def _require_open(self) -> None:
        if self._closed:
            raise ToolApprovalRepositoryUnavailableError("repository is closed")

    # ------------------------------------------------------------------ pause

    async def pause_first_request(
            self,
            *,
            approval_id: uuid.UUID,
            tenant_id: str,
            app_id: str,
            config_version: int,
            channel: str,
            user_id: str,
            session_id: str,
            receipt_id: uuid.UUID,
            function_call_id: str,
            tool_name: str,
            tool_args: Mapping[str, Any],
            pending_response: str,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> ApprovalRequest:
        self._require_open()
        if not isinstance(approval_id, uuid.UUID) or not isinstance(receipt_id, uuid.UUID):
            raise ValueError("invalid identifier")
        _require_nonblank(function_call_id, "function_call_id")
        _require_nonblank(tool_name, "tool_name")
        if not isinstance(tool_args, Mapping):
            raise ValueError("invalid tool_args")
        if not isinstance(pending_response, str) or not pending_response:
            raise ValueError("invalid pending_response")
        args_dict = dict(tool_args)
        args_digest = compute_args_digest(args_dict)
        now = sa.text("now()")
        try:
            async with self._engine.begin() as conn:
                # The receipt row is the identity ground truth: verify EVERY
                # identity field before creating anything (P2) and lock it.
                pre_row = (await conn.execute(
                    sa.select(*_RECEIPT_COLUMNS).where(message_receipts.c.receipt_id == receipt_id).with_for_update()
                )).first()
                if pre_row is None:
                    raise ToolApprovalRepositoryDataError("receipt not found for pause")
                pm = pre_row._mapping
                if (pm["tenant_id"] != tenant_id or pm["app_id"] != app_id or pm["channel"] != channel
                        or pm["user_id"] != user_id or pm["session_id"] != session_id
                        or pm["config_version"] != config_version):
                    raise ToolApprovalRepositoryDataError("receipt identity mismatch")
                existing = (await conn.execute(
                    sa.select(*_REQUEST_COLUMNS).where(
                        tool_approval_requests.c.receipt_id == receipt_id,
                        tool_approval_requests.c.function_call_id == function_call_id,
                    ))).first()
                if existing is not None:
                    # redelivered pause: only legal if it is the SAME approval
                    # and the receipt was already completed atomically with it
                    if existing._mapping["approval_id"] != approval_id or pm["state"] != "completed":
                        raise ToolApprovalRepositoryDataError("approval already exists for another id")
                    return _row_to_request(existing)
                if pm["state"] != "processing":
                    raise ToolApprovalRepositoryDataError("receipt is not processing")

                result = await conn.execute(
                    postgresql_insert(tool_approval_requests).values(
                        approval_id=approval_id,
                        tenant_id=tenant_id,
                        app_id=app_id,
                        config_version=config_version,
                        channel=channel,
                        user_id=user_id,
                        session_id=session_id,
                        receipt_id=receipt_id,
                        function_call_id=function_call_id,
                        tool_name=tool_name,
                        tool_args=args_dict,
                        args_digest=args_digest,
                        state="pending",
                    ).on_conflict_do_nothing(constraint="tool_approval_requests_receipt_call_uk"))
                if result.rowcount == 0:
                    # lost the unique-key race against an identical concurrent
                    # pause: re-read and return the winner
                    raced = (await conn.execute(
                        sa.select(*_REQUEST_COLUMNS).where(
                            tool_approval_requests.c.receipt_id == receipt_id,
                            tool_approval_requests.c.function_call_id == function_call_id,
                        ))).first()
                    if raced is None or raced._mapping["approval_id"] != approval_id:
                        raise ToolApprovalRepositoryDataError("approval already exists for another id")
                    return _row_to_request(raced)

                await conn.execute(tool_approval_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    approval_id=approval_id,
                    tenant_id=tenant_id,
                    event_type="created",
                    decision=None,
                    message_id=None,
                    args_digest=args_digest,
                    occurred_at=now,
                ))
                await self._complete_receipt(conn, receipt_id, pending_response, latency_ms, now)
                await _write_terminal_execution_events(conn, receipt_id, pm, execution_events)
                row = (await conn.execute(
                    sa.select(*_REQUEST_COLUMNS).where(tool_approval_requests.c.approval_id == approval_id))).first()
                if row is None:
                    raise ToolApprovalRepositoryDataError("approval row missing after pause")
                return _row_to_request(row)
        except ValueError:
            raise
        except ToolApprovalRepositoryDataError:
            raise
        except IntegrityError as exc:
            raise ToolApprovalRepositoryDataError("approval data conflict") from exc
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    @staticmethod
    async def _complete_receipt(conn, receipt_id: uuid.UUID, response_text: str, latency_ms: int, now) -> None:
        upd = await conn.execute(message_receipts.update().where(message_receipts.c.receipt_id == receipt_id).where(
            message_receipts.c.state == "processing").values(
                state="completed",
                response_text=response_text,
                finished_at=now,
                latency_ms=latency_ms,
            ))
        if upd.rowcount != 1:
            raise ToolApprovalRepositoryDataError("receipt completion failed")
        rm = (await conn.execute(sa.select(*_RECEIPT_COLUMNS).where(message_receipts.c.receipt_id == receipt_id)
                                 )).first()._mapping
        await conn.execute(message_audit_events.insert().values(
            audit_id=uuid.uuid4(),
            receipt_id=receipt_id,
            tenant_id=rm["tenant_id"],
            app_id=rm["app_id"],
            channel=rm["channel"],
            user_id=rm["user_id"],
            session_id=rm["session_id"],
            message_id=rm["message_id"],
            event_type="completed",
            request_id=rm["request_id"],
            config_version=rm["config_version"],
            message_digest=rm["message_digest"],
            response_digest=compute_response_digest(response_text),
            latency_ms=latency_ms,
            occurred_at=now,
        ))

    # ------------------------------------------------------------------- CAS

    async def claim_decision(
        self,
        approval_id: uuid.UUID,
        *,
        tenant_id: str,
        channel: str,
        user_id: str,
        session_id: str,
        decision: ApprovalDecision,
        decision_message_id: str,
    ) -> ApprovalClaim:
        self._require_open()
        if decision not in ("approve", "reject"):
            raise ValueError("invalid decision")
        dmid = _require_nonblank(decision_message_id, "decision_message_id")
        now = sa.text("now()")
        try:
            async with self._engine.begin() as conn:
                row = (await conn.execute(
                    sa.select(*_REQUEST_COLUMNS).where(
                        tool_approval_requests.c.approval_id == approval_id,
                        tool_approval_requests.c.tenant_id == tenant_id,
                        tool_approval_requests.c.channel == channel,
                        tool_approval_requests.c.user_id == user_id,
                        tool_approval_requests.c.session_id == session_id,
                    ).with_for_update())).first()
                if row is None:
                    return ApprovalClaim(
                        action=ApprovalAction.NOT_AVAILABLE,
                        approval_id=None,
                        state=None,
                        response_text=None,
                    )
                req = _row_to_request(row)
                if req.state == "pending":
                    upd = await conn.execute(tool_approval_requests.update().where(
                        tool_approval_requests.c.approval_id == approval_id).where(
                            tool_approval_requests.c.state == "pending").values(
                                state="executing",
                                decision=decision,
                                decision_message_id=dmid,
                                decided_at=now,
                            ))
                    if upd.rowcount != 1:
                        raise ToolApprovalRepositoryDataError("approval claim race lost after lock")
                    await conn.execute(tool_approval_audit_events.insert().values(
                        audit_id=uuid.uuid4(),
                        approval_id=approval_id,
                        tenant_id=tenant_id,
                        event_type="decided",
                        decision=decision,
                        message_id=dmid,
                        args_digest=req.args_digest,
                        occurred_at=now,
                    ))
                    return ApprovalClaim(
                        action=ApprovalAction.EXECUTE,
                        approval_id=approval_id,
                        state="executing",
                        response_text=None,
                    )
                if req.state == "executing":
                    return ApprovalClaim(
                        action=ApprovalAction.IN_PROGRESS,
                        approval_id=approval_id,
                        state=req.state,
                        response_text=None,
                    )
                if req.decision == decision and req.decision_message_id == dmid:
                    return ApprovalClaim(
                        action=ApprovalAction.REPLAY,
                        approval_id=approval_id,
                        state=req.state,
                        response_text=req.response_text,
                    )
                return ApprovalClaim(
                    action=ApprovalAction.CONFLICT,
                    approval_id=approval_id,
                    state=req.state,
                    response_text=None,
                )
        except ValueError:
            raise
        except ToolApprovalRepositoryDataError:
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    # -------------------------------------------------------------- finalize

    async def finalize(
            self,
            approval_id: uuid.UUID,
            *,
            receipt_id: uuid.UUID,
            terminal_state: str,
            response_text: str | None,
            error_code: str | None,
            latency_ms: int,
            execution_events: tuple[ExecutionAuditEvent, ...] = (),
    ) -> None:
        self._require_open()
        if terminal_state not in _TERMINAL_STATES:
            raise ValueError("invalid terminal state")
        if terminal_state in ("completed", "rejected"):
            if not isinstance(response_text, str) or not response_text:
                raise ValueError("terminal state requires response_text")
            receipt_state = "completed"
            receipt_error = None
        else:
            if not error_code:
                raise ValueError("failed requires error_code")
            response_text = None
            receipt_state = "failed"
            receipt_error = error_code
        now = sa.text("now()")
        try:
            async with self._engine.begin() as conn:
                appr = (await conn.execute(
                    sa.select(
                        tool_approval_requests.c.tenant_id,
                        tool_approval_requests.c.state,
                        tool_approval_requests.c.decision,
                        tool_approval_requests.c.args_digest,
                    ).where(tool_approval_requests.c.approval_id == approval_id).with_for_update())).first()
                if appr is None:
                    raise ToolApprovalRepositoryDataError("approval not found")
                am = appr._mapping
                if am["state"] != "executing":
                    raise ToolApprovalRepositoryDataError("approval is not executing")
                if terminal_state == "completed" and am["decision"] != "approve":
                    raise ToolApprovalRepositoryDataError("terminal state conflicts with decision")
                if terminal_state == "rejected" and am["decision"] != "reject":
                    raise ToolApprovalRepositoryDataError("terminal state conflicts with decision")

                upd = await conn.execute(
                    tool_approval_requests.update().where(tool_approval_requests.c.approval_id == approval_id).where(
                        tool_approval_requests.c.state == "executing").values(
                            state=terminal_state,
                            response_text=response_text,
                            finished_at=now,
                        ))
                if upd.rowcount != 1:
                    raise ToolApprovalRepositoryDataError("approval finalize failed")
                await conn.execute(tool_approval_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    approval_id=approval_id,
                    tenant_id=am["tenant_id"],
                    event_type=terminal_state,
                    decision=None,
                    message_id=None,
                    args_digest=am["args_digest"],
                    occurred_at=now,
                ))

                receipt_row = (await conn.execute(
                    sa.select(*_RECEIPT_COLUMNS).where(message_receipts.c.receipt_id == receipt_id).with_for_update()
                )).first()
                if receipt_row is None:
                    raise ToolApprovalRepositoryDataError("decision receipt not found")
                if receipt_row._mapping["state"] != "processing":
                    raise ToolApprovalRepositoryDataError("decision receipt is not processing")

                r_values: dict[str, Any] = {
                    "state": receipt_state,
                    "finished_at": now,
                    "latency_ms": latency_ms,
                }
                if receipt_state == "completed":
                    r_values["response_text"] = response_text
                else:
                    r_values["error_code"] = receipt_error
                r_upd = await conn.execute(
                    message_receipts.update().where(message_receipts.c.receipt_id == receipt_id).where(
                        message_receipts.c.state == "processing").values(**r_values))
                if r_upd.rowcount != 1:
                    raise ToolApprovalRepositoryDataError("decision receipt finalize failed")
                rm = receipt_row._mapping
                await conn.execute(message_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    receipt_id=receipt_id,
                    tenant_id=rm["tenant_id"],
                    app_id=rm["app_id"],
                    channel=rm["channel"],
                    user_id=rm["user_id"],
                    session_id=rm["session_id"],
                    message_id=rm["message_id"],
                    event_type="completed" if receipt_state == "completed" else "failed",
                    request_id=rm["request_id"],
                    config_version=rm["config_version"],
                    error_code=receipt_error,
                    message_digest=rm["message_digest"],
                    response_digest=compute_response_digest(response_text) if response_text else None,
                    latency_ms=latency_ms,
                    occurred_at=now,
                ))
                await _write_terminal_execution_events(conn, receipt_id, rm, execution_events)
        except ValueError:
            raise
        except ToolApprovalRepositoryDataError:
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    # -------------------------------------------- Stage 6D orphan disposition

    async def list_stale_executing(self, *, tenant_id: str, stale_seconds: int,
                                   limit: int) -> tuple[OrphanedApproval, ...]:
        self._require_open()
        tenant_id = _require_nonblank(tenant_id, "tenant_id")
        if isinstance(stale_seconds, bool) or not isinstance(stale_seconds, int) or stale_seconds < 1:
            raise ValueError("invalid stale_seconds")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("invalid limit")
        age = sa.func.extract("epoch", sa.func.now() - tool_approval_requests.c.decided_at)
        stmt = (sa.select(
            *_REQUEST_COLUMNS,
            tool_approval_requests.c.decided_at,
            age.label("age_seconds"),
        ).where(
            tool_approval_requests.c.tenant_id == tenant_id,
            tool_approval_requests.c.state == "executing",
            tool_approval_requests.c.decided_at.is_not(None),
            age >= stale_seconds,
        ).order_by(tool_approval_requests.c.decided_at.asc()).limit(limit))
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(stmt)).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc
        orphans: list[OrphanedApproval] = []
        for row in rows:
            m = row._mapping
            orphans.append(
                OrphanedApproval(
                    approval_id=m["approval_id"],
                    tenant_id=m["tenant_id"],
                    session_id=m["session_id"],
                    function_call_id=m["function_call_id"],
                    tool_name=m["tool_name"],
                    args_digest=m["args_digest"],
                    decision=m["decision"],
                    state=m["state"],
                    decided_at=m["decided_at"],
                    age_seconds=int(m["age_seconds"]),
                ))
        return tuple(orphans)

    async def terminate_orphan(self, approval_id: uuid.UUID, *, tenant_id: str,
                               stale_seconds: int) -> OrphanTermination:
        self._require_open()
        if not isinstance(approval_id, uuid.UUID):
            raise ValueError("invalid identifier")
        tenant_id = _require_nonblank(tenant_id, "tenant_id")
        if isinstance(stale_seconds, bool) or not isinstance(stale_seconds, int) or stale_seconds < 1:
            raise ValueError("invalid stale_seconds")
        now = sa.text("now()")
        try:
            async with self._engine.begin() as conn:
                row = (await conn.execute(
                    sa.select(
                        tool_approval_requests.c.tenant_id,
                        tool_approval_requests.c.state,
                        tool_approval_requests.c.decision,
                        tool_approval_requests.c.decision_message_id,
                        tool_approval_requests.c.args_digest,
                        tool_approval_requests.c.channel,
                        tool_approval_requests.c.user_id,
                        tool_approval_requests.c.session_id,
                        sa.func.extract("epoch",
                                        sa.func.now() - tool_approval_requests.c.decided_at).label("age_seconds"),
                    ).where(tool_approval_requests.c.approval_id == approval_id).with_for_update())).first()
                if row is None or row._mapping["tenant_id"] != tenant_id:
                    # same fixed non-leaking outcome as claim_decision: never
                    # distinguishes missing from cross-tenant
                    return OrphanTermination(
                        action=OrphanTerminationAction.NOT_AVAILABLE,
                        approval_id=None,
                        state=None,
                    )
                am = row._mapping
                state = am["state"]
                if state == "failed":
                    # idempotent replay of a previous disposition (or of a
                    # genuine execution failure): same terminal fact, rewrite
                    # nothing.
                    return OrphanTermination(
                        action=OrphanTerminationAction.ALREADY_TERMINATED,
                        approval_id=approval_id,
                        state="failed",
                    )
                if state == "pending":
                    return OrphanTermination(
                        action=OrphanTerminationAction.PENDING,
                        approval_id=approval_id,
                        state=state,
                    )
                if state != "executing":
                    # completed/rejected went through the normal terminal
                    # path — a disposition call on them is a mismatch.
                    return OrphanTermination(
                        action=OrphanTerminationAction.INCONSISTENT,
                        approval_id=approval_id,
                        state=state,
                    )
                age = am["age_seconds"]
                dmid = am["decision_message_id"]
                if age is None or dmid is None or int(age) < stale_seconds:
                    return OrphanTermination(
                        action=OrphanTerminationAction.ACTIVE,
                        approval_id=approval_id,
                        state="executing",
                    )
                # The decision receipt is the crash twin of the approval: it
                # was claimed (processing) BEFORE the approval moved to
                # executing, and finalize is atomic — so a live 'executing'
                # MUST have its decision receipt still 'processing'.
                receipt_row = (await conn.execute(
                    sa.select(
                        message_receipts.c.receipt_id,
                        message_receipts.c.state,
                        message_receipts.c.app_id,
                        message_receipts.c.request_id,
                        message_receipts.c.config_version,
                        message_receipts.c.message_digest,
                    ).where(
                        message_receipts.c.tenant_id == tenant_id,
                        message_receipts.c.channel == am["channel"],
                        message_receipts.c.user_id == am["user_id"],
                        message_receipts.c.session_id == am["session_id"],
                        message_receipts.c.message_id == dmid,
                    ).with_for_update())).first()
                if receipt_row is None or receipt_row._mapping["state"] != "processing":
                    return OrphanTermination(
                        action=OrphanTerminationAction.INCONSISTENT,
                        approval_id=approval_id,
                        state="executing",
                    )
                rm = receipt_row._mapping

                upd = await conn.execute(
                    tool_approval_requests.update().where(tool_approval_requests.c.approval_id == approval_id).where(
                        tool_approval_requests.c.state == "executing").values(
                            state="failed",
                            response_text=None,
                            finished_at=now,
                        ))
                if upd.rowcount != 1:
                    raise ToolApprovalRepositoryDataError("orphan disposition failed")
                await conn.execute(tool_approval_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    approval_id=approval_id,
                    tenant_id=tenant_id,
                    event_type="failed",
                    decision=None,
                    message_id=None,
                    args_digest=am["args_digest"],
                    occurred_at=now,
                ))
                latency_ms = int(age) * 1000
                r_upd = await conn.execute(
                    message_receipts.update().where(message_receipts.c.receipt_id == rm["receipt_id"]).where(
                        message_receipts.c.state == "processing").values(
                            state="failed",
                            error_code=_ORPHAN_DISPOSITION_ERROR_CODE,
                            finished_at=now,
                            latency_ms=latency_ms,
                        ))
                if r_upd.rowcount != 1:
                    raise ToolApprovalRepositoryDataError("orphan receipt disposition failed")
                await conn.execute(message_audit_events.insert().values(
                    audit_id=uuid.uuid4(),
                    receipt_id=rm["receipt_id"],
                    tenant_id=tenant_id,
                    app_id=rm["app_id"],
                    channel=am["channel"],
                    user_id=am["user_id"],
                    session_id=am["session_id"],
                    message_id=dmid,
                    event_type="failed",
                    request_id=rm["request_id"],
                    config_version=rm["config_version"],
                    error_code=_ORPHAN_DISPOSITION_ERROR_CODE,
                    message_digest=rm["message_digest"],
                    response_digest=None,
                    latency_ms=latency_ms,
                    occurred_at=now,
                ))
                return OrphanTermination(
                    action=OrphanTerminationAction.TERMINATED,
                    approval_id=approval_id,
                    state="failed",
                )
        except ValueError:
            raise
        except ToolApprovalRepositoryDataError:
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    # --------------------------------------------------------------- readers

    async def get(self, approval_id: uuid.UUID) -> ApprovalRequest | None:
        self._require_open()
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(*_REQUEST_COLUMNS).where(tool_approval_requests.c.approval_id == approval_id))).first()
                return None if row is None else _row_to_request(row)
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    async def get_tool_args(self, approval_id: uuid.UUID) -> dict[str, Any] | None:
        self._require_open()
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(
                        tool_approval_requests.c.tool_args).where(tool_approval_requests.c.approval_id == approval_id)
                )).first()
                if row is None:
                    return None
                raw = row._mapping["tool_args"]
                if not isinstance(raw, dict):
                    raise ToolApprovalRepositoryDataError("approval args are not an object")
                return dict(raw)
        except ToolApprovalRepositoryDataError:
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc

    async def list_audit(self, approval_id: uuid.UUID, limit: int) -> tuple[ApprovalAuditEvent, ...]:
        self._require_open()
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(
                    sa.select(tool_approval_audit_events).where(
                        tool_approval_audit_events.c.approval_id == approval_id).order_by(
                            tool_approval_audit_events.c.occurred_at.desc()).limit(limit))).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database query failed") from exc
        events = []
        for row in rows:
            m = row._mapping
            events.append(
                ApprovalAuditEvent(
                    audit_id=m["audit_id"],
                    approval_id=m["approval_id"],
                    tenant_id=m["tenant_id"],
                    event_type=m["event_type"],
                    decision=m["decision"],
                    message_id=m["message_id"],
                    args_digest=m["args_digest"],
                    occurred_at=m["occurred_at"],
                ))
        return tuple(events)

    async def check_ready(self) -> None:
        self._require_open()
        try:
            await check_database_readiness(self._engine)
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise ToolApprovalRepositoryUnavailableError("database is not reachable") from exc

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_engine:
            await self._engine.dispose()


__all__ = [
    "SqlToolApprovalRepository",
    "ToolApprovalRepository",
    "ToolApprovalRepositoryConfigurationError",
    "ToolApprovalRepositoryDataError",
    "ToolApprovalRepositoryUnavailableError",
]
