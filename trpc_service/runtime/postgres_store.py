"""Shared PostgreSQL implementation of the durable runtime port.

This is deliberately a row-oriented implementation: Inbox/Outbox, leases,
budget reservations, Tool intents, and Session events are database facts rather
than a cache or a process-local snapshot.  Every public runtime operation opens
one transaction with ``SET LOCAL app.tenant_id`` through :class:`PostgresConnections`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from trpc_service.db.postgres import PostgresConnections

from .errors import (
    BudgetExceeded,
    ExecutionDivergence,
    ExecutionUnavailable,
    InvalidTransition,
    LeaseLost,
    MigrationBusy,
    NotFound,
    SecurityRejected,
    StaleFence,
    TenantMismatch,
)
from .models import (
    Acceptance,
    AuditRecord,
    BudgetAccount,
    CommitInput,
    CommitResult,
    ExecutionClaim,
    ExecutionMode,
    InboundEnvelope,
    InboxRecord,
    InboxStatus,
    MemoryIntent,
    MigrationStatus,
    OutboxRecord,
    OutboxStatus,
    SessionEvent,
    SessionRecord,
    SessionSummary,
    StorageMigration,
    StorageRoute,
    TenantContext,
    TenantRuntimeState,
    ToolCapability,
    ToolExecution,
    ToolStatus,
    content_hash,
    stable_id,
    to_primitive,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _json(value: Any) -> Any:
    """Adapt a value to PostgreSQL jsonb only when this backend is used."""

    from psycopg.types.json import Jsonb

    return Jsonb(value)


_AUDIT_TEXT_FIELDS = {
    "channel",
    "subject_id",
    "agent_name",
    "tool_name",
    "policy_version",
    "reason_code",
    "error_type",
    "input_hash",
    "output_hash",
    "encrypted_detail_ref",
}
_AUDIT_INTEGER_FIELDS = {"latency_ms", "token_in", "token_out", "cost_micros"}


def _audit_fields(metadata: Mapping[str, Any]) -> dict[str, object]:
    """Whitelist hash/identifier audit fields, excluding raw input and output."""

    fields: dict[str, object] = {}
    for key in _AUDIT_TEXT_FIELDS:
        value = metadata.get(key)
        if isinstance(value, str):
            fields[key] = value[:512]
    for key in _AUDIT_INTEGER_FIELDS:
        value = metadata.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            fields[key] = value
    return fields


class PostgresRuntimeStore:
    """A transaction-per-operation runtime store backed by PostgreSQL rows."""

    def __init__(self, database_url: str, database_role: str | None = None) -> None:
        self._connections = PostgresConnections(database_url, database_role)

    def bootstrap_tenant(self, tenant_id: str, *, storage_profile: dict[str, Any] | None = None):
        """Return an already-provisioned tenant state.

        ``PostgresControlPlane.create_tenant`` creates the state and initial
        route atomically with the tenant row.  This compatibility method keeps
        the ServiceContainer bootstrap call explicit while refusing to invent a
        tenant outside the control-plane transaction.
        """

        del storage_profile
        with self.transaction(TenantContext(tenant_id=tenant_id, actor_id="bootstrap")) as tx:
            return tx.runtime_state()

    @contextmanager
    def transaction(self, context: TenantContext) -> Iterator[PostgresRuntimeTransaction]:
        with self._connections.tenant(context) as connection:
            yield PostgresRuntimeTransaction(connection, context)


class PostgresRuntimeTransaction:
    def __init__(self, connection: Any, context: TenantContext) -> None:
        self.connection = connection
        self.context = context

    def _tenant(self) -> str:
        if not self.context.tenant_id:
            raise TenantMismatch("TenantContext requires tenant_id")
        return self.context.tenant_id

    @staticmethod
    def _state(row: dict[str, Any]) -> TenantRuntimeState:
        return TenantRuntimeState(
            tenant_id=str(row["tenant_id"]),
            routing_epoch=int(row["routing_epoch"]),
            security_epoch=int(row["security_epoch"]),
            credential_revocation_epoch=int(row["credential_revocation_epoch"]),
            execution_mode=ExecutionMode(str(row["execution_mode"])),
            tool_denylist=set(row["tool_denylist"] or []),
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _inbox(row: dict[str, Any]) -> InboxRecord:
        return InboxRecord(
            tenant_id=str(row["tenant_id"]),
            inbox_id=str(row["inbox_id"]),
            channel_binding_id=str(row["channel_binding_id"]),
            agent_id=str(row["agent_id"]),
            session_id=str(row["session_id"]),
            config_version=int(row["config_version"]),
            subject_id=row["subject_id"],
            idempotency_key=str(row["idempotency_key"]),
            external_message_id=row["external_message_id"],
            status=InboxStatus(str(row["status"])),
            execution_id=row["execution_id"],
            execution_attempt=int(row["execution_attempt"]),
            claimed_lease_fence=row["claimed_lease_fence"],
            claimed_routing_epoch=row["claimed_routing_epoch"],
            claimed_security_epoch=row["claimed_security_epoch"],
            request_id=str(row["request_id"]),
            trace_id=str(row["trace_id"]),
            payload=dict(row["payload"] or {}),
            payload_hash=str(row["payload_hash"]),
            received_at=row["received_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _outbox(row: dict[str, Any]) -> OutboxRecord:
        return OutboxRecord(
            tenant_id=str(row["tenant_id"]),
            outbox_id=str(row["outbox_id"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=str(row["aggregate_id"]),
            inbox_id=row["inbox_id"],
            event_type=str(row["event_type"]),
            payload=dict(row["payload"] or {}),
            idempotency_key=str(row["idempotency_key"]),
            trace_id=str(row["trace_id"]),
            status=OutboxStatus(str(row["status"])),
            attempts=int(row["attempts"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            available_at=row["available_at"],
            created_at=row["created_at"],
            published_at=row["published_at"],
        )

    @staticmethod
    def _session(row: dict[str, Any]) -> SessionRecord:
        return SessionRecord(
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            agent_id=str(row["agent_id"]),
            channel_binding_id=str(row["channel_binding_id"]),
            config_version=int(row["config_version"]),
            state=dict(row["state"] or {}),
            version=int(row["version"]),
            last_event_seq=int(row["last_event_seq"]),
            active_inbox_id=row["active_inbox_id"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            lease_fence=int(row["lease_fence"]),
            status=str(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _event(row: dict[str, Any]) -> SessionEvent:
        return SessionEvent(
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            seq=int(row["seq"]),
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            role=row["role"],
            subject_id=row["subject_id"],
            external_message_id=row["external_message_id"],
            payload=dict(row["payload"] or {}),
            trace_id=str(row["trace_id"]),
            occurred_at=row["occurred_at"],
        )

    @staticmethod
    def _summary(row: dict[str, Any]) -> SessionSummary:
        return SessionSummary(
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            summary_id=str(row["summary_id"]),
            based_on_seq=int(row["based_on_seq"]),
            content=str(row["content"]),
            content_hash=str(row["content_hash"]),
            model_ref=row["model_ref"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _memory(row: dict[str, Any]) -> MemoryIntent:
        return MemoryIntent(
            tenant_id=str(row["tenant_id"]),
            memory_id=str(row["memory_id"]),
            session_id=str(row["session_id"]),
            subject_id=row["subject_id"],
            memory_type=str(row["memory_type"]),
            content=str(row["content"]),
            content_hash=str(row["content_hash"]),
            acl=dict(row["acl"] or {}),
            source_event_id=row["source_event_id"],
            version=int(row["version"]),
            expires_at=row["expires_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _tool(row: dict[str, Any]) -> ToolExecution:
        result_ref = row["result_ref"]
        try:
            result = json.loads(result_ref) if result_ref else None
        except (TypeError, json.JSONDecodeError):
            result = None
        return ToolExecution(
            tenant_id=str(row["tenant_id"]),
            tool_call_id=str(row["tool_call_id"]),
            inbox_id=str(row["inbox_id"]),
            execution_id=str(row["execution_id"]),
            session_id=str(row["session_id"]),
            tool_step=int(row["tool_step"]),
            tool_name=str(row["tool_name"]),
            arguments_hash=str(row["arguments_hash"]),
            retry_capability=ToolCapability(str(row["retry_capability"])),
            provider_idempotency_key=row["provider_idempotency_key"],
            lease_fence=int(row["lease_fence"]),
            routing_epoch=int(row["routing_epoch"]),
            security_epoch=int(row["security_epoch"]),
            status=ToolStatus(str(row["status"])),
            trace_id=str(row["trace_id"]),
            result=result,
            last_error_code=row["last_error_code"],
            provider_operation_id=row["provider_operation_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _route(row: dict[str, Any]) -> StorageRoute:
        return StorageRoute(
            tenant_id=str(row["tenant_id"]),
            routing_epoch=int(row["routing_epoch"]),
            profile=dict(row["profile"] or {}),
            status=MigrationStatus(str(row["route_status"])),
            source_watermark=row["source_watermark"],
            target_watermark=row["target_watermark"],
            created_at=row["created_at"],
            activated_at=row["activated_at"],
        )

    @staticmethod
    def _migration(row: dict[str, Any]) -> StorageMigration:
        return StorageMigration(
            tenant_id=str(row["tenant_id"]),
            migration_id=str(row["migration_id"]),
            source_profile=dict(row["source_profile"] or {}),
            target_profile=dict(row["target_profile"] or {}),
            status=MigrationStatus(str(row["status"])),
            source_routing_epoch=int(row["source_routing_epoch"]),
            target_routing_epoch=row["target_routing_epoch"],
            source_watermark=row["source_watermark"],
            target_watermark=row["target_watermark"],
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _state_row(self, *, lock: bool = False) -> dict[str, Any]:
        suffix = " FOR UPDATE" if lock else ""
        row = self.connection.execute(
            f"SELECT * FROM tenant_runtime_state WHERE tenant_id = %s{suffix}", (self._tenant(),)
        ).fetchone()
        if row is None:
            raise NotFound(f"tenant {self._tenant()!r} is not provisioned")
        return row

    def _inbox_row(self, inbox_id: str, *, lock: bool = False) -> dict[str, Any]:
        suffix = " FOR UPDATE" if lock else ""
        row = self.connection.execute(
            f"SELECT * FROM inbox WHERE tenant_id = %s AND inbox_id = %s{suffix}",
            (self._tenant(), inbox_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"inbox {inbox_id!r} was not found")
        return row

    def _session_row(self, session_id: str, *, lock: bool = False) -> dict[str, Any]:
        suffix = " FOR UPDATE" if lock else ""
        row = self.connection.execute(
            f"SELECT * FROM session WHERE tenant_id = %s AND session_id = %s{suffix}",
            (self._tenant(), session_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"session {session_id!r} was not found")
        return row

    def _tool_row(self, tool_call_id: str, *, lock: bool = False) -> dict[str, Any]:
        suffix = " FOR UPDATE" if lock else ""
        row = self.connection.execute(
            f"SELECT * FROM tool_execution WHERE tenant_id = %s AND tool_call_id = %s{suffix}",
            (self._tenant(), tool_call_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"tool intent {tool_call_id!r} was not found")
        return row

    def runtime_state(self) -> TenantRuntimeState:
        return self._state(self._state_row())

    def set_execution_mode(self, mode: ExecutionMode) -> TenantRuntimeState:
        row = self.connection.execute(
            """
            UPDATE tenant_runtime_state
            SET execution_mode = %s, security_epoch = security_epoch + 1, updated_at = %s
            WHERE tenant_id = %s RETURNING *
            """,
            (mode.value, _now(), self._tenant()),
        ).fetchone()
        if row is None:
            raise NotFound("tenant runtime state was not found")
        return self._state(row)

    def revoke_tool(self, tool_name: str) -> TenantRuntimeState:
        state = self._state_row(lock=True)
        denylist = set(state["tool_denylist"] or [])
        denylist.add(tool_name)
        row = self.connection.execute(
            """
            UPDATE tenant_runtime_state
            SET tool_denylist = %s, security_epoch = security_epoch + 1, updated_at = %s
            WHERE tenant_id = %s RETURNING *
            """,
            (_json(sorted(denylist)), _now(), self._tenant()),
        ).fetchone()
        return self._state(row)

    def accept_inbound(self, envelope: InboundEnvelope) -> Acceptance:
        tenant_id = self._tenant()
        if envelope.tenant_id != tenant_id:
            raise TenantMismatch("InboundEnvelope tenant does not match TenantContext")
        state = self.runtime_state()
        if state.execution_mode in {ExecutionMode.SUSPENDED, ExecutionMode.EMERGENCY_STOP}:
            raise SecurityRejected(f"tenant execution mode is {state.execution_mode.value}")
        now = _now()
        inbox_id = stable_id(
            "inb", tenant_id, envelope.channel_binding_id, envelope.idempotency_key
        )
        row = self.connection.execute(
            """
            INSERT INTO inbox
            (tenant_id, inbox_id, channel_binding_id, agent_id, config_version, subject_id,
             idempotency_key, external_message_id, session_id, status, request_id, trace_id,
             payload_hash, payload, received_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
            RETURNING *
            """,
            (
                tenant_id,
                inbox_id,
                envelope.channel_binding_id,
                envelope.agent_id,
                envelope.config_version,
                envelope.subject_id,
                envelope.idempotency_key,
                envelope.external_message_id,
                envelope.session_id,
                envelope.request_id or self.context.request_id,
                envelope.trace_id or self.context.trace_id,
                content_hash(envelope.payload),
                _json(dict(envelope.payload)),
                envelope.received_at,
                now,
            ),
        ).fetchone()
        if row is None:
            existing = self._inbox_row(inbox_id)
            outbox = self.connection.execute(
                """
                SELECT * FROM outbox WHERE tenant_id = %s AND idempotency_key = %s
                """,
                (tenant_id, f"inbound:{existing['inbox_id']}"),
            ).fetchone()
            if outbox is None:
                raise ExecutionDivergence("duplicate Inbox is missing its durable dispatch Outbox")
            return Acceptance(self._inbox(existing), self._outbox(outbox), True)
        inbox = self._inbox(row)
        outbox = self._insert_outbox(
            aggregate_type="inbox",
            aggregate_id=inbox.inbox_id,
            inbox_id=inbox.inbox_id,
            event_type="inbound.dispatch",
            payload={
                "tenant_id": tenant_id,
                "inbox_id": inbox.inbox_id,
                "session_id": inbox.session_id,
                "trace_id": inbox.trace_id,
                "request_id": inbox.request_id,
            },
            idempotency_key=f"inbound:{inbox.inbox_id}",
            trace_id=inbox.trace_id,
            now=now,
        )
        return Acceptance(inbox, outbox, False)

    def claim_outbox(self, owner: str, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        if limit <= 0 or lease_seconds <= 0:
            return []
        now = _now()
        rows = self.connection.execute(
            """
            SELECT * FROM outbox
            WHERE tenant_id = %s AND available_at <= %s
              AND (status = 'pending' OR (status = 'processing' AND lease_expires_at <= %s))
            ORDER BY available_at, created_at, outbox_id
            LIMIT %s FOR UPDATE SKIP LOCKED
            """,
            (self._tenant(), now, now, limit),
        ).fetchall()
        leased: list[OutboxRecord] = []
        expiry = now + timedelta(seconds=lease_seconds)
        for row in rows:
            updated = self.connection.execute(
                """
                UPDATE outbox SET status = 'processing', lease_owner = %s, lease_expires_at = %s,
                attempts = attempts + 1 WHERE tenant_id = %s AND outbox_id = %s RETURNING *
                """,
                (owner, expiry, self._tenant(), row["outbox_id"]),
            ).fetchone()
            leased.append(self._outbox(updated))
        return leased

    def mark_outbox_published(self, outbox_id: str, owner: str) -> None:
        row = self.connection.execute(
            """
            UPDATE outbox SET status = 'published', published_at = %s,
            lease_owner = NULL, lease_expires_at = NULL
            WHERE tenant_id = %s AND outbox_id = %s AND status = 'processing' AND lease_owner = %s
            RETURNING outbox_id
            """,
            (_now(), self._tenant(), outbox_id, owner),
        ).fetchone()
        if row is None:
            raise LeaseLost("outbox is no longer leased by dispatcher")

    def mark_outbox_delivered(self, outbox_id: str) -> None:
        row = self.connection.execute(
            """
            UPDATE outbox SET status = 'delivered', delivered_at = %s
            WHERE tenant_id = %s AND outbox_id = %s AND status IN ('published', 'delivered')
            RETURNING outbox_id, inbox_id, event_type
            """,
            (_now(), self._tenant(), outbox_id),
        ).fetchone()
        if row is None:
            raise InvalidTransition("reply delivery must follow Outbox publication")
        if row["event_type"] == "reply.dispatch" and row["inbox_id"]:
            self.connection.execute(
                """
                UPDATE inbox SET status = 'delivered', updated_at = %s
                WHERE tenant_id = %s AND inbox_id = %s AND status = 'reply_pending'
                """,
                (_now(), self._tenant(), row["inbox_id"]),
            )

    def release_outbox(self, outbox_id: str, owner: str, delay_seconds: int = 0) -> None:
        row = self.connection.execute(
            """
            UPDATE outbox SET status = 'pending', available_at = %s,
            lease_owner = NULL, lease_expires_at = NULL
            WHERE tenant_id = %s AND outbox_id = %s AND status = 'processing' AND lease_owner = %s
            RETURNING outbox_id
            """,
            (_now() + timedelta(seconds=max(delay_seconds, 0)), self._tenant(), outbox_id, owner),
        ).fetchone()
        if row is None:
            raise LeaseLost("outbox is no longer leased by dispatcher")

    def requeue_outbox(self, outbox_id: str) -> None:
        self.connection.execute(
            """
            UPDATE outbox SET status = 'pending', available_at = %s,
            lease_owner = NULL, lease_expires_at = NULL
            WHERE tenant_id = %s AND outbox_id = %s AND status <> 'delivered'
            """,
            (_now(), self._tenant(), outbox_id),
        )

    def put_budget_account(self, account: BudgetAccount) -> BudgetAccount:
        if account.tenant_id != self._tenant():
            raise TenantMismatch("budget account belongs to another tenant")
        if min(account.limit_units, account.spent_units, account.reserved_units) < 0:
            raise ValueError("budget units cannot be negative")
        if account.spent_units + account.reserved_units > account.limit_units:
            raise BudgetExceeded("budget account is already over its limit")
        row = self.connection.execute(
            """
            INSERT INTO budget_account
            (tenant_id, budget_name, unit, period_start, period_end, limit_units,
             reserved_units, spent_units, version, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, budget_name, period_start) DO UPDATE SET
              unit = EXCLUDED.unit, period_end = EXCLUDED.period_end, limit_units = EXCLUDED.limit_units,
              reserved_units = EXCLUDED.reserved_units, spent_units = EXCLUDED.spent_units,
              version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                account.tenant_id,
                account.budget_name,
                account.unit,
                account.period_start,
                account.period_end,
                account.limit_units,
                account.reserved_units,
                account.spent_units,
                account.version,
                _now(),
            ),
        ).fetchone()
        return self._account(row)

    def reap_expired_reservations(self) -> int:
        now = _now()
        reservations = self.connection.execute(
            """
            SELECT * FROM budget_reservation
            WHERE tenant_id = %s AND status = 'reserved' AND expires_at <= %s
            FOR UPDATE
            """,
            (self._tenant(), now),
        ).fetchall()
        for reservation in reservations:
            updated = self.connection.execute(
                """
                UPDATE budget_account
                SET reserved_units = reserved_units - %s, version = version + 1, updated_at = %s
                WHERE tenant_id = %s AND budget_name = %s AND period_start = %s
                  AND reserved_units >= %s
                RETURNING budget_name
                """,
                (
                    reservation["estimated_units"],
                    now,
                    self._tenant(),
                    reservation["budget_name"],
                    reservation["period_start"],
                    reservation["estimated_units"],
                ),
            ).fetchone()
            if updated is None:
                raise ExecutionDivergence("expired budget reservation has no matching reserved balance")
            self.connection.execute(
                """
                UPDATE budget_reservation SET status = 'expired', settled_at = %s
                WHERE tenant_id = %s AND reservation_id = %s
                """,
                (now, self._tenant(), reservation["reservation_id"]),
            )
        return len(reservations)

    @staticmethod
    def _account(row: dict[str, Any]) -> BudgetAccount:
        return BudgetAccount(
            tenant_id=str(row["tenant_id"]),
            budget_name=str(row["budget_name"]),
            unit=str(row["unit"]),
            limit_units=int(row["limit_units"]),
            spent_units=int(row["spent_units"]),
            reserved_units=int(row["reserved_units"]),
            version=int(row["version"]),
            period_start=row["period_start"],
            period_end=row["period_end"],
        )

    def claim_execution(
        self, inbox_id: str, worker_id: str, lease_seconds: int, budget_estimates: dict[str, int]
    ) -> ExecutionClaim:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        tenant_id = self._tenant()
        state = self._state(self._state_row(lock=True))
        if state.execution_mode != ExecutionMode.NORMAL:
            raise SecurityRejected(
                f"new claims are disabled while tenant is {state.execution_mode.value}"
            )
        inbox = self._inbox(self._inbox_row(inbox_id, lock=True))
        now = _now()
        self.reap_expired_reservations()
        self.connection.execute(
            """
            INSERT INTO session
            (tenant_id, session_id, agent_id, channel_binding_id, conversation_type,
             conversation_key_hash, id_rule_version, config_version, state, version,
             last_event_seq, lease_fence, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'direct', %s, 1, %s, '{}'::jsonb, 0, 0, 0, 'active', %s, %s)
            ON CONFLICT (tenant_id, session_id) DO NOTHING
            """,
            (
                tenant_id,
                inbox.session_id,
                inbox.agent_id,
                inbox.channel_binding_id,
                content_hash(inbox.session_id),
                inbox.config_version,
                now,
                now,
            ),
        )
        session = self._session(self._session_row(inbox.session_id, lock=True))
        claimable = inbox.status in {
            InboxStatus.RECEIVED,
            InboxStatus.QUEUED,
            InboxStatus.RETRYABLE,
        }
        current_expired = (
            inbox.status == InboxStatus.CLAIMED
            and session.active_inbox_id == inbox.inbox_id
            and session.lease_expires_at is not None
            and session.lease_expires_at <= now
        )
        if not claimable and not current_expired:
            raise ExecutionUnavailable(f"inbox {inbox_id} has status {inbox.status.value}")
        if session.active_inbox_id is not None and session.active_inbox_id != inbox.inbox_id:
            raise ExecutionUnavailable("session is owned by another active inbox")
        if session.lease_owner and session.lease_expires_at and session.lease_expires_at > now:
            raise ExecutionUnavailable("session lease is still active")
        execution_id = inbox.execution_id or stable_id("exe", tenant_id, inbox.inbox_id)
        expiry = now + timedelta(seconds=lease_seconds)
        fence = session.lease_fence + 1
        claimed = self.connection.execute(
            """
            UPDATE inbox SET execution_id = %s, execution_attempt = execution_attempt + 1,
            status = 'claimed', claimed_lease_fence = %s, claimed_routing_epoch = %s,
            claimed_security_epoch = %s, updated_at = %s
            WHERE tenant_id = %s AND inbox_id = %s RETURNING *
            """,
            (
                execution_id,
                fence,
                state.routing_epoch,
                state.security_epoch,
                now,
                tenant_id,
                inbox_id,
            ),
        ).fetchone()
        updated_session = self.connection.execute(
            """
            UPDATE session SET active_inbox_id = %s, lease_owner = %s, lease_expires_at = %s,
            lease_fence = %s, updated_at = %s WHERE tenant_id = %s AND session_id = %s RETURNING *
            """,
            (inbox_id, worker_id, expiry, fence, now, tenant_id, session.session_id),
        ).fetchone()
        self._reserve_budgets(execution_id, budget_estimates, now, lease_seconds)
        claimed_inbox = self._inbox(claimed)
        claimed_session = self._session(updated_session)
        self.connection.execute(
            """
            INSERT INTO execution_attempt
            (tenant_id, execution_id, attempt_no, inbox_id, session_id, worker_id, lease_fence,
             routing_epoch, security_epoch, status, lease_expires_at, claimed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'claimed', %s, %s)
            """,
            (
                tenant_id,
                execution_id,
                claimed_inbox.execution_attempt,
                inbox_id,
                claimed_session.session_id,
                worker_id,
                fence,
                state.routing_epoch,
                state.security_epoch,
                expiry,
                now,
            ),
        )
        return ExecutionClaim(
            tenant_id=tenant_id,
            inbox_id=inbox_id,
            execution_id=execution_id,
            session_id=claimed_session.session_id,
            worker_id=worker_id,
            lease_fence=fence,
            routing_epoch=state.routing_epoch,
            security_epoch=state.security_epoch,
            session_version=claimed_session.version,
            lease_expires_at=expiry,
            attempt_no=claimed_inbox.execution_attempt,
            config_version=claimed_session.config_version,
        )

    def renew_execution(self, claim: ExecutionClaim, lease_seconds: int) -> ExecutionClaim:
        self.assert_execution(claim)
        expiry = _now() + timedelta(seconds=lease_seconds)
        updated = self.connection.execute(
            """
            UPDATE session SET lease_expires_at = %s, updated_at = %s
            WHERE tenant_id = %s AND session_id = %s AND active_inbox_id = %s
              AND lease_owner = %s AND lease_fence = %s RETURNING *
            """,
            (
                expiry,
                _now(),
                self._tenant(),
                claim.session_id,
                claim.inbox_id,
                claim.worker_id,
                claim.lease_fence,
            ),
        ).fetchone()
        if updated is None:
            raise StaleFence("session lease was lost")
        self.connection.execute(
            """
            UPDATE execution_attempt SET status = 'running', lease_expires_at = %s
            WHERE tenant_id = %s AND execution_id = %s AND attempt_no = %s
            """,
            (expiry, self._tenant(), claim.execution_id, claim.attempt_no),
        )
        self.connection.execute(
            """
            UPDATE budget_reservation SET expires_at = %s
            WHERE tenant_id = %s AND execution_id = %s AND status = 'reserved'
            """,
            (expiry, self._tenant(), claim.execution_id),
        )
        return ExecutionClaim(
            tenant_id=claim.tenant_id,
            inbox_id=claim.inbox_id,
            execution_id=claim.execution_id,
            session_id=claim.session_id,
            worker_id=claim.worker_id,
            lease_fence=claim.lease_fence,
            routing_epoch=claim.routing_epoch,
            security_epoch=claim.security_epoch,
            session_version=claim.session_version,
            lease_expires_at=expiry,
            attempt_no=claim.attempt_no,
            config_version=claim.config_version,
        )

    def assert_execution(self, claim: ExecutionClaim) -> None:
        if claim.tenant_id != self._tenant():
            raise TenantMismatch("claim tenant does not match TenantContext")
        state = self._state(self._state_row(lock=True))
        inbox = self._inbox(self._inbox_row(claim.inbox_id, lock=True))
        session = self._session(self._session_row(claim.session_id, lock=True))
        reason: str | None = None
        if state.execution_mode != ExecutionMode.NORMAL:
            reason = f"tenant execution mode is {state.execution_mode.value}"
            error_type: type[Exception] = SecurityRejected
        elif (
            state.routing_epoch != claim.routing_epoch
            or state.security_epoch != claim.security_epoch
        ):
            reason = "tenant routing or security epoch changed"
            error_type = StaleFence
        elif (
            inbox.execution_id != claim.execution_id
            or inbox.status != InboxStatus.CLAIMED
            or inbox.claimed_lease_fence != claim.lease_fence
            or inbox.claimed_routing_epoch != claim.routing_epoch
            or inbox.claimed_security_epoch != claim.security_epoch
        ):
            reason = "inbox no longer belongs to this execution"
            error_type = StaleFence
        elif (
            session.active_inbox_id != claim.inbox_id
            or session.lease_owner != claim.worker_id
            or session.lease_fence != claim.lease_fence
            or session.lease_expires_at is None
            or session.lease_expires_at <= _now()
        ):
            reason = "session lease was lost"
            error_type = StaleFence
        else:
            return
        self._mark_attempt_lost(claim)
        raise error_type(reason)

    def prepare_tool(
        self,
        claim: ExecutionClaim,
        tool_step: int,
        tool_name: str,
        arguments_hash: str,
        capability: ToolCapability,
    ) -> ToolExecution:
        self.assert_execution(claim)
        if tool_name in self.runtime_state().tool_denylist:
            raise SecurityRejected(f"tool {tool_name!r} has been revoked")
        existing = self.connection.execute(
            """
            SELECT * FROM tool_execution
            WHERE tenant_id = %s AND execution_id = %s AND tool_step = %s FOR UPDATE
            """,
            (self._tenant(), claim.execution_id, tool_step),
        ).fetchone()
        if existing is not None:
            tool = self._tool(existing)
            if tool.tool_name != tool_name or tool.arguments_hash != arguments_hash:
                raise ExecutionDivergence("same deterministic tool step has different arguments")
            if tool.lease_fence != claim.lease_fence and tool.status != ToolStatus.SUCCEEDED:
                if tool.status in {ToolStatus.PREPARED, ToolStatus.CONFIRMED}:
                    next_status = tool.status
                    reason = tool.last_error_code
                elif tool.retry_capability == ToolCapability.IDEMPOTENT:
                    next_status = ToolStatus.PREPARED
                    reason = "tool_lease_takeover_idempotent_retry"
                elif tool.retry_capability == ToolCapability.QUERYABLE:
                    next_status = ToolStatus.RECONCILING
                    reason = "tool_lease_takeover_requires_reconciliation"
                else:
                    next_status = ToolStatus.MANUAL_REVIEW
                    reason = "tool_lease_takeover_non_retriable"
                row = self.connection.execute(
                    """
                    UPDATE tool_execution
                    SET lease_fence = %s, routing_epoch = %s, security_epoch = %s,
                        status = %s, last_error_code = %s, updated_at = %s
                    WHERE tenant_id = %s AND tool_call_id = %s
                    RETURNING *
                    """,
                    (
                        claim.lease_fence,
                        claim.routing_epoch,
                        claim.security_epoch,
                        next_status.value,
                        reason,
                        _now(),
                        self._tenant(),
                        tool.tool_call_id,
                    ),
                ).fetchone()
                return self._tool(row)
            return tool
        now = _now()
        tool_call_id = stable_id("tool", claim.inbox_id, claim.execution_id, tool_step)
        row = self.connection.execute(
            """
            INSERT INTO tool_execution
            (tenant_id, tool_call_id, inbox_id, execution_id, session_id, tool_step, tool_name,
             arguments_hash, retry_capability, provider_idempotency_key, lease_fence,
             routing_epoch, security_epoch, status, trace_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'prepared', %s, %s, %s)
            RETURNING *
            """,
            (
                self._tenant(),
                tool_call_id,
                claim.inbox_id,
                claim.execution_id,
                claim.session_id,
                tool_step,
                tool_name,
                arguments_hash,
                capability.value,
                tool_call_id if capability == ToolCapability.IDEMPOTENT else None,
                claim.lease_fence,
                claim.routing_epoch,
                claim.security_epoch,
                self._inbox(self._inbox_row(claim.inbox_id)).trace_id,
                now,
                now,
            ),
        ).fetchone()
        return self._tool(row)

    def start_tool(self, claim: ExecutionClaim, tool_call_id: str) -> ToolExecution:
        self.assert_execution(claim)
        tool = self._tool(self._tool_row(tool_call_id, lock=True))
        if tool.execution_id != claim.execution_id or tool.lease_fence != claim.lease_fence:
            raise StaleFence("tool intent belongs to another execution fence")
        if tool.tool_name in self.runtime_state().tool_denylist:
            raise SecurityRejected(f"tool {tool.tool_name!r} has been revoked")
        if tool.status not in {ToolStatus.PREPARED, ToolStatus.CONFIRMED}:
            return tool
        row = self.connection.execute(
            """
            UPDATE tool_execution SET status = 'running', updated_at = %s
            WHERE tenant_id = %s AND tool_call_id = %s RETURNING *
            """,
            (_now(), self._tenant(), tool_call_id),
        ).fetchone()
        return self._tool(row)

    def finish_tool(
        self,
        claim: ExecutionClaim,
        tool_call_id: str,
        status: ToolStatus,
        result: dict | None = None,
        error_code: str | None = None,
        provider_operation_id: str | None = None,
    ) -> ToolExecution:
        if status not in {
            ToolStatus.SUCCEEDED,
            ToolStatus.FAILED,
            ToolStatus.UNKNOWN,
            ToolStatus.RECONCILING,
            ToolStatus.MANUAL_REVIEW,
        }:
            raise ValueError("tool completion must be a terminal/recovery status")
        tool = self._tool(self._tool_row(tool_call_id, lock=True))
        if tool.execution_id != claim.execution_id or tool.inbox_id != claim.inbox_id:
            raise TenantMismatch("tool result does not belong to execution")
        if tool.status not in {ToolStatus.RUNNING, ToolStatus.RECONCILING, ToolStatus.UNKNOWN}:
            return tool
        row = self.connection.execute(
            """
            UPDATE tool_execution SET status = %s, result_ref = %s, last_error_code = %s,
            provider_operation_id = %s, updated_at = %s
            WHERE tenant_id = %s AND tool_call_id = %s RETURNING *
            """,
            (
                status.value,
                json.dumps(result, sort_keys=True, separators=(",", ":"))
                if result is not None
                else None,
                error_code,
                provider_operation_id,
                _now(),
                self._tenant(),
                tool_call_id,
            ),
        ).fetchone()
        return self._tool(row)

    def resolve_tool(self, tool_call_id: str, status: ToolStatus, note: str) -> ToolExecution:
        tool = self._tool(self._tool_row(tool_call_id, lock=True))
        if tool.status not in {
            ToolStatus.UNKNOWN,
            ToolStatus.MANUAL_REVIEW,
            ToolStatus.RECONCILING,
        }:
            raise InvalidTransition("only an ambiguous Tool operation may be manually resolved")
        now = _now()
        row = self.connection.execute(
            """
            UPDATE tool_execution SET status = %s, last_error_code = %s, resolved_by = %s, updated_at = %s
            WHERE tenant_id = %s AND tool_call_id = %s RETURNING *
            """,
            (
                status.value,
                note or tool.last_error_code,
                self.context.actor_id,
                now,
                self._tenant(),
                tool_call_id,
            ),
        ).fetchone()
        resolved = self._tool(row)
        self.connection.execute(
            """
            INSERT INTO audit_log (tenant_id, audit_id, occurred_at, session_id, tool_name, decision,
            reason_code, trace_id, request_id)
            VALUES (%s, %s, %s, %s, %s, 'tool_manual_resolution', %s, %s, %s)
            """,
            (
                self._tenant(),
                stable_id("audit", "tool-resolution", tool_call_id, now.isoformat()),
                now,
                resolved.session_id,
                resolved.tool_name,
                status.value,
                resolved.trace_id,
                self.context.request_id,
            ),
        )
        return resolved

    def tool_recovery_action(self, tool_call_id: str) -> str:
        tool = self._tool(self._tool_row(tool_call_id))
        if tool.status == ToolStatus.SUCCEEDED:
            return "reuse_result"
        if tool.retry_capability == ToolCapability.IDEMPOTENT:
            return "retry_with_idempotency_key"
        if tool.retry_capability == ToolCapability.QUERYABLE:
            return "reconcile"
        return "manual_review"

    def commit_execution(self, claim: ExecutionClaim, commit: CommitInput) -> CommitResult:
        self.assert_execution(claim)
        session = self._session(self._session_row(claim.session_id, lock=True))
        inbox = self._inbox(self._inbox_row(claim.inbox_id, lock=True))
        if (
            commit.expected_session_version != session.version
            or claim.session_version != session.version
        ):
            raise StaleFence("session CAS version changed")
        now = _now()
        events: list[SessionEvent] = []
        for offset, draft in enumerate(commit.events, start=1):
            event_id = draft.event_id or stable_id(
                "evt", claim.execution_id, session.last_event_seq + offset, draft.event_type
            )
            event = self.connection.execute(
                """
                INSERT INTO session_event
                (tenant_id, session_id, seq, event_id, event_type, role, subject_id,
                 external_message_id, payload, trace_id, occurred_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    self._tenant(),
                    session.session_id,
                    session.last_event_seq + offset,
                    event_id,
                    draft.event_type,
                    draft.role,
                    draft.subject_id,
                    draft.external_message_id,
                    _json(dict(draft.payload)),
                    inbox.trace_id,
                    draft.occurred_at,
                    now,
                ),
            ).fetchone()
            events.append(self._event(event))
        self._settle_budgets(claim.execution_id, dict(commit.actual_budget_units), now)
        updated_session = self.connection.execute(
            """
            UPDATE session SET state = %s, version = version + 1, last_event_seq = %s,
            active_inbox_id = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
            WHERE tenant_id = %s AND session_id = %s AND version = %s AND lease_fence = %s
            RETURNING *
            """,
            (
                _json(dict(commit.new_state)),
                session.last_event_seq + len(events),
                now,
                self._tenant(),
                session.session_id,
                session.version,
                claim.lease_fence,
            ),
        ).fetchone()
        if updated_session is None:
            raise StaleFence("session CAS update failed")
        committed_session = self._session(updated_session)
        summary: SessionSummary | None = None
        if events:
            summary_rows = self.connection.execute(
                """
                SELECT role, payload FROM session_event
                WHERE tenant_id = %s AND session_id = %s AND role IN ('user', 'assistant')
                ORDER BY seq DESC LIMIT 24
                """,
                (self._tenant(), committed_session.session_id),
            ).fetchall()
            lines: list[str] = []
            for summary_row in reversed(summary_rows):
                text = (summary_row["payload"] or {}).get("text")
                if isinstance(text, str) and text.strip():
                    lines.append(f"{summary_row['role']}: {' '.join(text.split())[:800]}")
            if lines:
                summary_content = "\n".join(lines)[-6000:]
                based_on_seq = events[-1].seq
                summary_id = stable_id(
                    "sum", committed_session.session_id, based_on_seq, content_hash(summary_content)
                )
                row = self.connection.execute(
                    """
                    INSERT INTO session_summary
                    (tenant_id, session_id, summary_id, based_on_seq, content, content_hash, model_ref, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, session_id, based_on_seq) DO NOTHING
                    RETURNING *
                    """,
                    (
                        self._tenant(),
                        committed_session.session_id,
                        summary_id,
                        based_on_seq,
                        summary_content,
                        content_hash(summary_content),
                        (
                            str(committed_session.state.get("model"))
                            if isinstance(committed_session.state.get("model"), str)
                            else None
                        ),
                        now,
                    ),
                ).fetchone()
                if row is None:
                    row = self.connection.execute(
                        """
                        SELECT * FROM session_summary
                        WHERE tenant_id = %s AND session_id = %s AND based_on_seq = %s
                        """,
                        (self._tenant(), committed_session.session_id, based_on_seq),
                    ).fetchone()
                summary = self._summary(row)
        memory_outboxes: list[OutboxRecord] = []
        for position, draft in enumerate(commit.memories):
            memory_id = draft.memory_id or stable_id(
                "mem", claim.execution_id, position, draft.memory_type, content_hash(draft.content)
            )
            prior = self.connection.execute(
                """
                SELECT * FROM memory WHERE tenant_id = %s AND memory_id = %s FOR UPDATE
                """,
                (self._tenant(), memory_id),
            ).fetchone()
            version = int(prior["version"]) + 1 if prior else 1
            if prior:
                self.connection.execute(
                    """
                    UPDATE memory SET session_id = %s, subject_id = %s, memory_type = %s, content = %s,
                    encrypted_content_ref = NULL, content_hash = %s, acl = %s, source_event_id = %s,
                    version = %s, expires_at = %s, updated_at = %s
                    WHERE tenant_id = %s AND memory_id = %s
                    """,
                    (
                        committed_session.session_id,
                        draft.subject_id or inbox.subject_id,
                        draft.memory_type,
                        draft.content,
                        content_hash(draft.content),
                        _json(dict(draft.acl)),
                        draft.source_event_id,
                        version,
                        draft.expires_at,
                        now,
                        self._tenant(),
                        memory_id,
                    ),
                )
            else:
                self.connection.execute(
                    """
                    INSERT INTO memory
                    (tenant_id, memory_id, session_id, subject_id, memory_type, content,
                     content_hash, acl, source_event_id, version, expires_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        self._tenant(),
                        memory_id,
                        committed_session.session_id,
                        draft.subject_id or inbox.subject_id,
                        draft.memory_type,
                        draft.content,
                        content_hash(draft.content),
                        _json(dict(draft.acl)),
                        draft.source_event_id,
                        version,
                        draft.expires_at,
                        now,
                        now,
                    ),
                )
            memory_outboxes.append(
                self._insert_outbox(
                    aggregate_type="memory",
                    aggregate_id=memory_id,
                    inbox_id=inbox.inbox_id,
                    event_type="memory.project",
                    payload={
                        "tenant_id": self._tenant(),
                        "memory_id": memory_id,
                        "requested_version": version,
                    },
                    idempotency_key=f"memory:{memory_id}:{version}",
                    trace_id=inbox.trace_id,
                    now=now,
                )
            )
        reply_outbox: OutboxRecord | None = None
        if commit.reply is not None:
            delivery_key = commit.reply.delivery_key or stable_id(
                "reply", claim.execution_id, committed_session.version
            )
            reply_outbox = self._insert_outbox(
                aggregate_type="session",
                aggregate_id=committed_session.session_id,
                inbox_id=inbox.inbox_id,
                event_type="reply.dispatch",
                payload={
                    "tenant_id": self._tenant(),
                    "session_id": committed_session.session_id,
                    "inbox_id": inbox.inbox_id,
                    "channel_binding_id": commit.reply.channel_binding_id
                    or inbox.channel_binding_id,
                    "recipient_id": commit.reply.recipient_id or inbox.subject_id,
                    "blocks": [dict(block) for block in commit.reply.blocks],
                    "delivery_key": delivery_key,
                    "metadata": dict(commit.reply.metadata),
                },
                idempotency_key=f"reply:{delivery_key}",
                trace_id=inbox.trace_id,
                now=now,
            )
            final_status = InboxStatus.REPLY_PENDING
        else:
            final_status = InboxStatus.COMMITTED
        final_inbox = self.connection.execute(
            """
            UPDATE inbox SET status = %s, updated_at = %s
            WHERE tenant_id = %s AND inbox_id = %s RETURNING *
            """,
            (final_status.value, now, self._tenant(), inbox.inbox_id),
        ).fetchone()
        self.connection.execute(
            """
            UPDATE execution_attempt SET status = 'committed', finished_at = %s
            WHERE tenant_id = %s AND execution_id = %s AND attempt_no = %s
            """,
            (now, self._tenant(), claim.execution_id, claim.attempt_no),
        )
        audit = _audit_fields(commit.audit_metadata)
        self.connection.execute(
            """
            INSERT INTO audit_log
            (tenant_id, audit_id, occurred_at, channel, subject_id, session_id, agent_name, tool_name,
             decision, reason_code, policy_version, latency_ms, error_type, input_hash, output_hash,
             token_in, token_out, cost_micros, trace_id, request_id, encrypted_detail_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self._tenant(),
                stable_id("audit", claim.execution_id, committed_session.version),
                now,
                audit.get("channel"),
                audit.get("subject_id"),
                committed_session.session_id,
                audit.get("agent_name"),
                audit.get("tool_name"),
                commit.audit_decision,
                audit.get("reason_code"),
                audit.get("policy_version"),
                audit.get("latency_ms"),
                audit.get("error_type"),
                audit.get("input_hash"),
                audit.get("output_hash"),
                audit.get("token_in"),
                audit.get("token_out"),
                audit.get("cost_micros"),
                inbox.trace_id,
                inbox.request_id,
                audit.get("encrypted_detail_ref"),
            ),
        )
        return CommitResult(
            session=committed_session,
            inbox=self._inbox(final_inbox),
            events=events,
            summary=summary,
            reply_outbox=reply_outbox,
            memory_outboxes=memory_outboxes,
        )

    def get_memory(self, memory_id: str) -> MemoryIntent | None:
        row = self.connection.execute(
            "SELECT * FROM memory WHERE tenant_id = %s AND memory_id = %s",
            (self._tenant(), memory_id),
        ).fetchone()
        return self._memory(row) if row else None

    def put_knowledge_document(self, document: dict) -> dict[str, Any]:
        """Upsert operator-submitted text and enqueue a durable projection event."""

        document_id = str(document.get("document_id") or "")
        knowledge_base_id = str(document.get("knowledge_base_id") or "")
        content = str(document.get("content") or "")
        checksum = str(document.get("checksum") or "")
        acl = document.get("acl") or {}
        if not document_id or not knowledge_base_id or not content.strip() or not checksum:
            raise ValueError("knowledge document requires id, base, content, and checksum")
        if len(content) > 200_000 or not isinstance(acl, Mapping):
            raise ValueError("knowledge document content or ACL is invalid")
        conflict = self.connection.execute(
            """
            SELECT document_id FROM knowledge_document
            WHERE tenant_id = %s AND knowledge_base_id = %s AND checksum = %s
              AND document_id <> %s
            FOR UPDATE
            """,
            (self._tenant(), knowledge_base_id, checksum, document_id),
        ).fetchone()
        if conflict is not None:
            raise ExecutionDivergence("knowledge content already has a different document id")
        now = _now()
        row = self.connection.execute(
            """
            INSERT INTO knowledge_document
            (tenant_id, document_id, knowledge_base_id, object_uri, content, checksum, acl,
             version, index_status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 'pending', %s, %s)
            ON CONFLICT (tenant_id, document_id) DO UPDATE
            SET knowledge_base_id = EXCLUDED.knowledge_base_id,
                object_uri = EXCLUDED.object_uri,
                content = EXCLUDED.content,
                checksum = EXCLUDED.checksum,
                acl = EXCLUDED.acl,
                version = knowledge_document.version + 1,
                index_status = 'pending',
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                self._tenant(),
                document_id,
                knowledge_base_id,
                str(document.get("object_uri") or f"db://knowledge/{document_id}"),
                content,
                checksum,
                _json(dict(acl)),
                now,
                now,
            ),
        ).fetchone()
        result = dict(row)
        result["acl"] = dict(result.get("acl") or {})
        self._insert_outbox(
            aggregate_type="knowledge",
            aggregate_id=document_id,
            inbox_id=None,
            event_type="knowledge.project",
            payload={
                "tenant_id": self._tenant(),
                "document_id": document_id,
                "requested_version": int(result["version"]),
            },
            idempotency_key=f"knowledge:{document_id}:{result['version']}",
            trace_id=self.context.trace_id,
            now=now,
        )
        return result

    def list_outbox(self) -> list[OutboxRecord]:
        rows = self.connection.execute(
            "SELECT * FROM outbox WHERE tenant_id = %s ORDER BY created_at, outbox_id",
            (self._tenant(),),
        ).fetchall()
        return [self._outbox(row) for row in rows]

    def list_audit(self) -> list[AuditRecord]:
        rows = self.connection.execute(
            "SELECT * FROM audit_log WHERE tenant_id = %s ORDER BY occurred_at, audit_id",
            (self._tenant(),),
        ).fetchall()
        return [
            AuditRecord(
                tenant_id=str(row["tenant_id"]),
                audit_id=str(row["audit_id"]),
                decision=str(row["decision"]),
                trace_id=str(row["trace_id"]),
                request_id=str(row["request_id"]),
                session_id=row["session_id"],
                reason_code=row["reason_code"],
                channel=row["channel"],
                subject_id=row["subject_id"],
                agent_name=row["agent_name"],
                tool_name=row["tool_name"],
                policy_version=row["policy_version"],
                latency_ms=row["latency_ms"],
                error_type=row["error_type"],
                input_hash=row["input_hash"],
                output_hash=row["output_hash"],
                token_in=row["token_in"],
                token_out=row["token_out"],
                cost_micros=row["cost_micros"],
                encrypted_detail_ref=row["encrypted_detail_ref"],
                occurred_at=row["occurred_at"],
            )
            for row in rows
        ]

    def record_audit(
        self,
        decision: str,
        audit_id: str,
        *,
        session_id: str | None = None,
        metadata: dict | None = None,
    ) -> AuditRecord:
        """Insert one redacted compliance fact, idempotently by ``audit_id``."""

        if not decision or not audit_id:
            raise ValueError("audit decision and audit_id are required")
        now = _now()
        audit = _audit_fields(metadata or {})
        # ``audit_log`` is range-partitioned by timestamp, so its primary key
        # must include the partition column and cannot deduplicate one audit
        # fact across all partitions.  ``audit_dedup`` is a small unpartitioned
        # tenant-scoped write gate.  Workload roles need INSERT only there; they
        # never need SELECT access to the compliance ledger merely to retry one
        # at-least-once worker event.
        inserted = self.connection.execute(
            """
            INSERT INTO audit_dedup (tenant_id, audit_id, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (tenant_id, audit_id) DO NOTHING
            """,
            (self._tenant(), audit_id, now),
        )
        if inserted.rowcount == 0:
            return AuditRecord(
                tenant_id=self._tenant(),
                audit_id=audit_id,
                decision=decision[:128],
                trace_id=self.context.trace_id,
                request_id=self.context.request_id,
                session_id=session_id,
                **audit,
                occurred_at=now,
            )
        row = self.connection.execute(
            """
            INSERT INTO audit_log
            (tenant_id, audit_id, occurred_at, channel, subject_id, session_id, agent_name, tool_name,
             decision, reason_code, policy_version, latency_ms, error_type, input_hash, output_hash,
             token_in, token_out, cost_micros, trace_id, request_id, encrypted_detail_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self._tenant(),
                audit_id,
                now,
                audit.get("channel"),
                audit.get("subject_id"),
                session_id,
                audit.get("agent_name"),
                audit.get("tool_name"),
                decision[:128],
                audit.get("reason_code"),
                audit.get("policy_version"),
                audit.get("latency_ms"),
                audit.get("error_type"),
                audit.get("input_hash"),
                audit.get("output_hash"),
                audit.get("token_in"),
                audit.get("token_out"),
                audit.get("cost_micros"),
                self.context.trace_id,
                self.context.request_id,
                audit.get("encrypted_detail_ref"),
            ),
        )
        if row.rowcount != 1:  # defensive: RLS/permissions must not silently drop audit writes.
            raise RuntimeError("audit record was not persisted")
        return AuditRecord(
            tenant_id=self._tenant(),
            audit_id=audit_id,
            decision=decision[:128],
            trace_id=self.context.trace_id,
            request_id=self.context.request_id,
            session_id=session_id,
            **audit,
            occurred_at=now,
        )

    @staticmethod
    def list_audit_record(row: dict[str, Any]) -> AuditRecord:
        return AuditRecord(
            tenant_id=str(row["tenant_id"]),
            audit_id=str(row["audit_id"]),
            decision=str(row["decision"]),
            trace_id=str(row["trace_id"]),
            request_id=str(row["request_id"]),
            session_id=row["session_id"],
            reason_code=row["reason_code"],
            channel=row["channel"],
            subject_id=row["subject_id"],
            agent_name=row["agent_name"],
            tool_name=row["tool_name"],
            policy_version=row["policy_version"],
            latency_ms=row["latency_ms"],
            error_type=row["error_type"],
            input_hash=row["input_hash"],
            output_hash=row["output_hash"],
            token_in=row["token_in"],
            token_out=row["token_out"],
            cost_micros=row["cost_micros"],
            encrypted_detail_ref=row["encrypted_detail_ref"],
            occurred_at=row["occurred_at"],
        )

    def current_route(self) -> StorageRoute | None:
        row = self.connection.execute(
            """
            SELECT route.* FROM storage_route AS route
            JOIN tenant_runtime_state AS state
              ON state.tenant_id = route.tenant_id AND state.routing_epoch = route.routing_epoch
            WHERE route.tenant_id = %s
            """,
            (self._tenant(),),
        ).fetchone()
        return self._route(row) if row else None

    def initiate_migration(
        self, target_profile: dict, migration_id: str | None = None
    ) -> StorageMigration:
        state = self._state(self._state_row(lock=True))
        if state.execution_mode != ExecutionMode.NORMAL:
            raise MigrationBusy("cannot begin migration while tenant is not normal")
        active = self.connection.execute(
            """
            SELECT migration_id FROM storage_migration WHERE tenant_id = %s
            AND status IN ('preparing', 'backfilling', 'catching_up', 'draining', 'verifying', 'readonly')
            FOR UPDATE
            """,
            (self._tenant(),),
        ).fetchone()
        if active:
            raise MigrationBusy("another storage migration is in progress")
        route = self.current_route()
        if route is None:
            raise NotFound("current storage route was not found")
        now = _now()
        migration_id = migration_id or stable_id(
            "mig", self._tenant(), content_hash(target_profile), now.isoformat()
        )
        existing = self.connection.execute(
            "SELECT * FROM storage_migration WHERE tenant_id = %s AND migration_id = %s",
            (self._tenant(), migration_id),
        ).fetchone()
        if existing:
            return self._migration(existing)
        row = self.connection.execute(
            """
            INSERT INTO storage_migration
            (tenant_id, migration_id, source_profile, target_profile, status, source_routing_epoch, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'preparing', %s, %s, %s) RETURNING *
            """,
            (
                self._tenant(),
                migration_id,
                _json(route.profile),
                _json(target_profile),
                state.routing_epoch,
                now,
                now,
            ),
        ).fetchone()
        return self._migration(row)

    def migration(self, migration_id: str) -> StorageMigration:
        row = self.connection.execute(
            "SELECT * FROM storage_migration WHERE tenant_id = %s AND migration_id = %s",
            (self._tenant(), migration_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"migration {migration_id!r} was not found")
        return self._migration(row)

    def transition_migration(
        self,
        migration_id: str,
        action: str,
        *,
        source_watermark: str | None = None,
        target_watermark: str | None = None,
        verified: bool | None = None,
    ) -> StorageMigration:
        state = self._state(self._state_row(lock=True))
        row = self.connection.execute(
            "SELECT * FROM storage_migration WHERE tenant_id = %s AND migration_id = %s FOR UPDATE",
            (self._tenant(), migration_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"migration {migration_id!r} was not found")
        migration = self._migration(row)
        now = _now()
        status = migration.status
        if action == "start_backfill" and status == MigrationStatus.PREPARING:
            status = MigrationStatus.BACKFILLING
        elif action == "catch_up" and status == MigrationStatus.BACKFILLING:
            status = MigrationStatus.CATCHING_UP
        elif action == "record_catch_up" and status == MigrationStatus.CATCHING_UP:
            status = MigrationStatus.CATCHING_UP
        elif action == "begin_drain" and status == MigrationStatus.CATCHING_UP:
            status = MigrationStatus.DRAINING
            self._set_drain_state(ExecutionMode.DRAINING, now)
        elif action == "verify" and status == MigrationStatus.DRAINING:
            if self._has_live_leases(now):
                raise MigrationBusy("cannot verify while tenant has live executions")
            if verified:
                status = MigrationStatus.VERIFYING
            else:
                status = MigrationStatus.FAILED
                self._set_drain_state(ExecutionMode.NORMAL, now)
        elif action == "cutover" and status == MigrationStatus.VERIFYING:
            if self._has_live_leases(now):
                raise MigrationBusy("cannot cut over while tenant has live executions")
            next_epoch = state.routing_epoch + 1
            self.connection.execute(
                """
                UPDATE storage_route SET route_status = 'readonly'
                WHERE tenant_id = %s AND routing_epoch = %s
                """,
                (self._tenant(), state.routing_epoch),
            )
            self.connection.execute(
                """
                INSERT INTO storage_route
                (tenant_id, routing_epoch, profile, route_status, source_watermark, target_watermark, created_at, activated_at)
                VALUES (%s, %s, %s, 'active', %s, %s, %s, %s)
                """,
                (
                    self._tenant(),
                    next_epoch,
                    _json(migration.target_profile),
                    source_watermark or migration.source_watermark,
                    target_watermark or migration.target_watermark,
                    now,
                    now,
                ),
            )
            self.connection.execute(
                """
                UPDATE tenant_runtime_state SET routing_epoch = %s, security_epoch = security_epoch + 1,
                execution_mode = 'normal', updated_at = %s WHERE tenant_id = %s
                """,
                (next_epoch, now, self._tenant()),
            )
            status = MigrationStatus.ACTIVE
            migration.target_routing_epoch = next_epoch
        elif action == "begin_rollback" and status == MigrationStatus.ACTIVE:
            status = MigrationStatus.READONLY
            self._set_drain_state(ExecutionMode.DRAINING, now)
        elif action == "complete_rollback" and status == MigrationStatus.READONLY:
            if self._has_live_leases(now):
                raise MigrationBusy("cannot roll back while tenant has live executions")
            next_epoch = state.routing_epoch + 1
            self.connection.execute(
                "UPDATE storage_route SET route_status = 'retired' WHERE tenant_id = %s AND routing_epoch = %s",
                (self._tenant(), state.routing_epoch),
            )
            self.connection.execute(
                """
                INSERT INTO storage_route
                (tenant_id, routing_epoch, profile, route_status, created_at, activated_at)
                VALUES (%s, %s, %s, 'active', %s, %s)
                """,
                (self._tenant(), next_epoch, _json(migration.source_profile), now, now),
            )
            self.connection.execute(
                """
                UPDATE tenant_runtime_state SET routing_epoch = %s, security_epoch = security_epoch + 1,
                execution_mode = 'normal', updated_at = %s WHERE tenant_id = %s
                """,
                (next_epoch, now, self._tenant()),
            )
            status = MigrationStatus.RETIRED
        elif action == "cancel" and status in {
            MigrationStatus.PREPARING,
            MigrationStatus.BACKFILLING,
            MigrationStatus.CATCHING_UP,
        }:
            status = MigrationStatus.RETIRED
        else:
            raise InvalidTransition(f"cannot {action} migration in state {status.value}")
        error = (
            "target watermark verification failed"
            if action == "verify" and not verified
            else migration.error
        )
        updated = self.connection.execute(
            """
            UPDATE storage_migration SET status = %s, target_routing_epoch = %s,
            source_watermark = COALESCE(%s, source_watermark), target_watermark = COALESCE(%s, target_watermark),
            error = %s, updated_at = %s WHERE tenant_id = %s AND migration_id = %s RETURNING *
            """,
            (
                status.value,
                migration.target_routing_epoch,
                source_watermark,
                target_watermark,
                error,
                now,
                self._tenant(),
                migration_id,
            ),
        ).fetchone()
        return self._migration(updated)

    def snapshot(self, *, include_audit: bool = True) -> dict[str, Any]:
        tenant_id = self._tenant()
        state = to_primitive(self.runtime_state())
        tables = {
            "inboxes": ("inbox", self._inbox),
            "outbox": ("outbox", self._outbox),
            "sessions": ("session", self._session),
            "events": ("session_event", self._event),
            "summaries": ("session_summary", self._summary),
            "memories": ("memory", self._memory),
            "knowledge": ("knowledge_document", lambda row: dict(row)),
            "tools": ("tool_execution", self._tool),
            "migrations": ("storage_migration", self._migration),
        }
        output: dict[str, Any] = {"runtime_state": state}
        for name, (table, mapper) in tables.items():
            rows = self.connection.execute(
                f"SELECT * FROM {table} WHERE tenant_id = %s", (tenant_id,)
            ).fetchall()
            output[name] = [to_primitive(mapper(row)) for row in rows]
        output["audit"] = [to_primitive(row) for row in self.list_audit()] if include_audit else []
        return output

    def _reserve_budgets(
        self, execution_id: str, estimates: dict[str, int], now: datetime, lease_seconds: int
    ) -> None:
        for budget_name in sorted(estimates):
            estimate = int(estimates[budget_name])
            if estimate < 0:
                raise ValueError("budget estimate cannot be negative")
            account = self.connection.execute(
                """
                SELECT * FROM budget_account
                WHERE tenant_id = %s AND budget_name = %s AND period_start <= %s AND period_end > %s
                ORDER BY period_start DESC LIMIT 1 FOR UPDATE
                """,
                (self._tenant(), budget_name, now, now),
            ).fetchone()
            if account is None:
                raise BudgetExceeded(f"hard budget account {budget_name!r} is unavailable")
            reservation = self.connection.execute(
                """
                SELECT * FROM budget_reservation WHERE tenant_id = %s AND execution_id = %s
                AND budget_name = %s AND period_start = %s FOR UPDATE
                """,
                (self._tenant(), execution_id, budget_name, account["period_start"]),
            ).fetchone()
            if reservation and reservation["status"] in {"reserved", "unknown"}:
                continue
            updated = self.connection.execute(
                """
                UPDATE budget_account SET reserved_units = reserved_units + %s, version = version + 1, updated_at = %s
                WHERE tenant_id = %s AND budget_name = %s AND period_start = %s
                  AND spent_units + reserved_units + %s <= limit_units RETURNING *
                """,
                (estimate, now, self._tenant(), budget_name, account["period_start"], estimate),
            ).fetchone()
            if updated is None:
                raise BudgetExceeded(f"hard budget {budget_name!r} would be exceeded")
            reservation_id = stable_id("res", self._tenant(), execution_id, budget_name)
            self.connection.execute(
                """
                INSERT INTO budget_reservation
                (tenant_id, reservation_id, budget_name, period_start, execution_id, estimated_units,
                 status, expires_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'reserved', %s, %s)
                ON CONFLICT (tenant_id, execution_id, budget_name, period_start) DO UPDATE
                SET status = 'reserved', expires_at = EXCLUDED.expires_at
                """,
                (
                    self._tenant(),
                    reservation_id,
                    budget_name,
                    account["period_start"],
                    execution_id,
                    estimate,
                    now + timedelta(seconds=lease_seconds),
                    now,
                ),
            )

    def _settle_budgets(self, execution_id: str, actuals: dict[str, int], now: datetime) -> None:
        reservations = self.connection.execute(
            """
            SELECT * FROM budget_reservation WHERE tenant_id = %s AND execution_id = %s
            AND status = 'reserved' FOR UPDATE
            """,
            (self._tenant(), execution_id),
        ).fetchall()
        for reservation in reservations:
            actual = int(actuals.get(reservation["budget_name"], reservation["estimated_units"]))
            if actual < 0:
                raise ValueError("actual budget usage cannot be negative")
            additional = max(0, actual - int(reservation["estimated_units"]))
            account = self.connection.execute(
                """
                UPDATE budget_account
                SET reserved_units = reserved_units - %s, spent_units = spent_units + %s,
                    version = version + 1, updated_at = %s
                WHERE tenant_id = %s AND budget_name = %s AND period_start = %s
                  AND spent_units + reserved_units + %s <= limit_units
                RETURNING *
                """,
                (
                    reservation["estimated_units"],
                    actual,
                    now,
                    self._tenant(),
                    reservation["budget_name"],
                    reservation["period_start"],
                    additional,
                ),
            ).fetchone()
            if account is None:
                raise BudgetExceeded(
                    f"actual usage exceeds hard budget {reservation['budget_name']!r}"
                )
            self.connection.execute(
                """
                UPDATE budget_reservation SET actual_units = %s, status = 'settled', settled_at = %s
                WHERE tenant_id = %s AND reservation_id = %s
                """,
                (actual, now, self._tenant(), reservation["reservation_id"]),
            )

    def _insert_outbox(
        self,
        *,
        aggregate_type: str,
        aggregate_id: str,
        inbox_id: str | None,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        trace_id: str,
        now: datetime,
    ) -> OutboxRecord:
        outbox_id = stable_id("obx", self._tenant(), event_type, idempotency_key)
        row = self.connection.execute(
            """
            INSERT INTO outbox
            (tenant_id, outbox_id, aggregate_type, aggregate_id, inbox_id, event_type, payload,
             idempotency_key, trace_id, status, attempts, available_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', 0, %s, %s)
            ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING *
            """,
            (
                self._tenant(),
                outbox_id,
                aggregate_type,
                aggregate_id,
                inbox_id,
                event_type,
                _json(payload),
                idempotency_key,
                trace_id,
                now,
                now,
            ),
        ).fetchone()
        if row is None:
            row = self.connection.execute(
                "SELECT * FROM outbox WHERE tenant_id = %s AND idempotency_key = %s",
                (self._tenant(), idempotency_key),
            ).fetchone()
        return self._outbox(row)

    def _mark_attempt_lost(self, claim: ExecutionClaim) -> None:
        self.connection.execute(
            """
            UPDATE execution_attempt SET status = 'lost_fence', finished_at = %s
            WHERE tenant_id = %s AND execution_id = %s AND attempt_no = %s
              AND status IN ('claimed', 'running')
            """,
            (_now(), self._tenant(), claim.execution_id, claim.attempt_no),
        )

    def _has_live_leases(self, now: datetime) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM session WHERE tenant_id = %s AND lease_owner IS NOT NULL
            AND lease_expires_at > %s LIMIT 1
            """,
            (self._tenant(), now),
        ).fetchone()
        return row is not None

    def _set_drain_state(self, mode: ExecutionMode, now: datetime) -> None:
        self.connection.execute(
            """
            UPDATE tenant_runtime_state SET execution_mode = %s, security_epoch = security_epoch + 1,
            updated_at = %s WHERE tenant_id = %s
            """,
            (mode.value, now, self._tenant()),
        )
