"""A serializable in-memory RuntimeStore for focused tests and the local demo.

It deliberately serializes transactions under a re-entrant lock and snapshots
the state before each transaction.  That gives the same all-or-nothing boundary
that the SQL repository must provide, including Inbox + Outbox acceptance and
the final Session commit.  It is not intended as a production persistence layer.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import timedelta
from threading import RLock
from typing import Any

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
    AttemptStatus,
    AuditRecord,
    BudgetAccount,
    BudgetReservation,
    CommitInput,
    CommitResult,
    ExecutionAttempt,
    ExecutionClaim,
    ExecutionMode,
    InboundEnvelope,
    InboxRecord,
    InboxStatus,
    MemoryIntent,
    MigrationStatus,
    OutboxRecord,
    OutboxStatus,
    ReservationStatus,
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
    utcnow,
)


@dataclass
class _RuntimeData:
    states: dict[str, TenantRuntimeState] = field(default_factory=dict)
    inboxes: dict[tuple[str, str], InboxRecord] = field(default_factory=dict)
    inbox_by_idempotency: dict[tuple[str, str], str] = field(default_factory=dict)
    outboxes: dict[tuple[str, str], OutboxRecord] = field(default_factory=dict)
    outbox_by_idempotency: dict[tuple[str, str], str] = field(default_factory=dict)
    sessions: dict[tuple[str, str], SessionRecord] = field(default_factory=dict)
    events: dict[tuple[str, str], list[SessionEvent]] = field(default_factory=dict)
    summaries: dict[tuple[str, str], list[SessionSummary]] = field(default_factory=dict)
    budgets: dict[tuple[str, str], BudgetAccount] = field(default_factory=dict)
    reservations: dict[tuple[str, str], BudgetReservation] = field(default_factory=dict)
    reservations_by_execution: dict[tuple[str, str, str], str] = field(default_factory=dict)
    attempts: dict[tuple[str, str], list[ExecutionAttempt]] = field(default_factory=dict)
    tools: dict[tuple[str, str], ToolExecution] = field(default_factory=dict)
    tool_by_step: dict[tuple[str, str, int], str] = field(default_factory=dict)
    memories: dict[tuple[str, str], MemoryIntent] = field(default_factory=dict)
    knowledge_documents: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    audits: dict[str, list[AuditRecord]] = field(default_factory=dict)
    routes: dict[tuple[str, int], StorageRoute] = field(default_factory=dict)
    migrations: dict[tuple[str, str], StorageMigration] = field(default_factory=dict)


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


def _audit_fields(metadata: object) -> dict[str, object]:
    """Whitelist typed audit metadata; raw model/channel payloads never enter audit rows."""

    if not isinstance(metadata, Mapping):
        return {}
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


class InMemoryRuntimeStore:
    """Thread-safe, transactionally serialized durable-runtime store."""

    def __init__(self, *, now=utcnow) -> None:
        self._data = _RuntimeData()
        self._lock = RLock()
        self._now = now

    def now(self):
        return self._now()

    def bootstrap_tenant(
        self, tenant_id: str, *, storage_profile: dict[str, Any] | None = None
    ) -> TenantRuntimeState:
        """Create a tenant/runtime-state pair; admin/bootstrap use only."""

        with self._lock:
            if tenant_id in self._data.states:
                return deepcopy(self._data.states[tenant_id])
            state = TenantRuntimeState(tenant_id=tenant_id, updated_at=self.now())
            self._data.states[tenant_id] = state
            self._data.routes[(tenant_id, 1)] = StorageRoute(
                tenant_id=tenant_id,
                routing_epoch=1,
                profile=deepcopy(storage_profile or {"profile": "memory-default"}),
                status=MigrationStatus.ACTIVE,
                activated_at=self.now(),
            )
            return deepcopy(state)

    @contextmanager
    def transaction(self, context: TenantContext) -> Iterator[InMemoryRuntimeTransaction]:
        """Run a tenant transaction and roll back every mutation on an exception."""

        with self._lock:
            if context.tenant_id not in self._data.states:
                raise NotFound(f"tenant {context.tenant_id!r} is not provisioned")
            snapshot = deepcopy(self._data)
            transaction = InMemoryRuntimeTransaction(self, context)
            try:
                yield transaction
            except BaseException:
                self._data = snapshot
                raise

    # Read-only helpers are intentionally useful for demos/tests.  Production
    # repositories should expose their own tenant-scoped query methods.
    def snapshot(self, context: TenantContext) -> dict[str, Any]:
        with self.transaction(context) as tx:
            return tx.snapshot()


class InMemoryRuntimeTransaction:
    def __init__(self, store: InMemoryRuntimeStore, context: TenantContext) -> None:
        self._store = store
        self.context = context

    def _now(self):
        return self._store.now()

    def _tenant(self) -> str:
        if self.context.tenant_id not in self._store._data.states:
            raise NotFound(f"tenant {self.context.tenant_id!r} is not provisioned")
        return self.context.tenant_id

    def _key(self, record_id: str) -> tuple[str, str]:
        return (self._tenant(), record_id)

    def runtime_state(self) -> TenantRuntimeState:
        return deepcopy(self._store._data.states[self._tenant()])

    def set_execution_mode(self, mode: ExecutionMode) -> TenantRuntimeState:
        state = self._store._data.states[self._tenant()]
        state.execution_mode = mode
        state.security_epoch += 1
        state.updated_at = self._now()
        return deepcopy(state)

    def revoke_tool(self, tool_name: str) -> TenantRuntimeState:
        state = self._store._data.states[self._tenant()]
        state.tool_denylist.add(tool_name)
        state.security_epoch += 1
        state.updated_at = self._now()
        return deepcopy(state)

    def accept_inbound(self, envelope: InboundEnvelope) -> Acceptance:
        tenant_id = self._tenant()
        if envelope.tenant_id != tenant_id:
            raise TenantMismatch("InboundEnvelope tenant does not match TenantContext")
        state = self._store._data.states[tenant_id]
        if state.execution_mode in {ExecutionMode.SUSPENDED, ExecutionMode.EMERGENCY_STOP}:
            raise SecurityRejected(f"tenant execution mode is {state.execution_mode.value}")

        index_key = (tenant_id, envelope.idempotency_key)
        existing_id = self._store._data.inbox_by_idempotency.get(index_key)
        if existing_id:
            inbox = self._store._data.inboxes[(tenant_id, existing_id)]
            outbox_id = self._store._data.outbox_by_idempotency[
                (tenant_id, f"inbound:{inbox.inbox_id}")
            ]
            return Acceptance(
                deepcopy(inbox), deepcopy(self._store._data.outboxes[(tenant_id, outbox_id)]), True
            )

        now = self._now()
        inbox_id = stable_id(
            "inb", tenant_id, envelope.channel_binding_id, envelope.idempotency_key
        )
        outbox_id = stable_id("obx", tenant_id, "inbound.dispatch", inbox_id)
        inbox = InboxRecord(
            tenant_id=tenant_id,
            inbox_id=inbox_id,
            channel_binding_id=envelope.channel_binding_id,
            agent_id=envelope.agent_id,
            session_id=envelope.session_id,
            config_version=envelope.config_version,
            subject_id=envelope.subject_id,
            idempotency_key=envelope.idempotency_key,
            external_message_id=envelope.external_message_id,
            status=InboxStatus.QUEUED,
            execution_id=None,
            execution_attempt=0,
            claimed_lease_fence=None,
            claimed_routing_epoch=None,
            claimed_security_epoch=None,
            request_id=envelope.request_id or self.context.request_id,
            trace_id=envelope.trace_id or self.context.trace_id,
            payload=deepcopy(dict(envelope.payload)),
            payload_hash=content_hash(envelope.payload),
            received_at=envelope.received_at,
            updated_at=now,
        )
        outbox = OutboxRecord(
            tenant_id=tenant_id,
            outbox_id=outbox_id,
            aggregate_type="inbox",
            aggregate_id=inbox_id,
            inbox_id=inbox_id,
            event_type="inbound.dispatch",
            payload={
                "tenant_id": tenant_id,
                "inbox_id": inbox_id,
                "session_id": envelope.session_id,
                "trace_id": inbox.trace_id,
                "request_id": inbox.request_id,
            },
            idempotency_key=f"inbound:{inbox_id}",
            trace_id=inbox.trace_id,
            created_at=now,
            available_at=now,
        )
        self._store._data.inboxes[(tenant_id, inbox_id)] = inbox
        self._store._data.inbox_by_idempotency[index_key] = inbox_id
        self._store._data.outboxes[(tenant_id, outbox_id)] = outbox
        self._store._data.outbox_by_idempotency[(tenant_id, outbox.idempotency_key)] = outbox_id
        return Acceptance(deepcopy(inbox), deepcopy(outbox), False)

    def claim_outbox(self, owner: str, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        if limit <= 0 or lease_seconds <= 0:
            return []
        now = self._now()
        tenant_id = self._tenant()
        candidates = sorted(
            (
                outbox
                for (record_tenant, _), outbox in self._store._data.outboxes.items()
                if record_tenant == tenant_id
                and outbox.available_at <= now
                and (
                    outbox.status == OutboxStatus.PENDING
                    or (
                        outbox.status == OutboxStatus.PROCESSING
                        and outbox.lease_expires_at is not None
                        and outbox.lease_expires_at <= now
                    )
                )
            ),
            key=lambda row: (row.available_at, row.created_at, row.outbox_id),
        )[:limit]
        for outbox in candidates:
            outbox.status = OutboxStatus.PROCESSING
            outbox.lease_owner = owner
            outbox.lease_expires_at = now + timedelta(seconds=lease_seconds)
            outbox.attempts += 1
        return deepcopy(candidates)

    def mark_outbox_published(self, outbox_id: str, owner: str) -> None:
        outbox = self._require_outbox(outbox_id)
        if outbox.status != OutboxStatus.PROCESSING or outbox.lease_owner != owner:
            raise LeaseLost("outbox is no longer leased by dispatcher")
        outbox.status = OutboxStatus.PUBLISHED
        outbox.published_at = self._now()
        outbox.lease_owner = None
        outbox.lease_expires_at = None

    def mark_outbox_delivered(self, outbox_id: str) -> None:
        outbox = self._require_outbox(outbox_id)
        if outbox.status not in {OutboxStatus.PUBLISHED, OutboxStatus.DELIVERED}:
            raise InvalidTransition("reply delivery must follow Outbox publication")
        outbox.status = OutboxStatus.DELIVERED
        if outbox.event_type == "reply.dispatch" and outbox.inbox_id:
            inbox = self._store._data.inboxes.get((self._tenant(), outbox.inbox_id))
            if inbox is not None and inbox.status == InboxStatus.REPLY_PENDING:
                inbox.status = InboxStatus.DELIVERED
                inbox.updated_at = self._now()

    def release_outbox(self, outbox_id: str, owner: str, delay_seconds: int = 0) -> None:
        outbox = self._require_outbox(outbox_id)
        if outbox.status != OutboxStatus.PROCESSING or outbox.lease_owner != owner:
            raise LeaseLost("outbox is no longer leased by dispatcher")
        outbox.status = OutboxStatus.PENDING
        outbox.available_at = self._now() + timedelta(seconds=max(delay_seconds, 0))
        outbox.lease_owner = None
        outbox.lease_expires_at = None

    def requeue_outbox(self, outbox_id: str) -> None:
        outbox = self._require_outbox(outbox_id)
        if outbox.status == OutboxStatus.DELIVERED:
            return
        outbox.status = OutboxStatus.PENDING
        outbox.available_at = self._now()
        outbox.lease_owner = None
        outbox.lease_expires_at = None

    def put_budget_account(self, account: BudgetAccount) -> BudgetAccount:
        if account.tenant_id != self._tenant():
            raise TenantMismatch("budget account belongs to another tenant")
        if account.limit_units < 0 or account.spent_units < 0 or account.reserved_units < 0:
            raise ValueError("budget units cannot be negative")
        if account.spent_units + account.reserved_units > account.limit_units:
            raise BudgetExceeded("budget account is already over its limit")
        self._store._data.budgets[(account.tenant_id, account.budget_name)] = deepcopy(account)
        return deepcopy(account)

    def reap_expired_reservations(self) -> int:
        now = self._now()
        released = 0
        for (tenant_id, _), reservation in self._store._data.reservations.items():
            if (
                tenant_id != self._tenant()
                or reservation.status != ReservationStatus.RESERVED
                or reservation.expires_at > now
            ):
                continue
            account = self._store._data.budgets[(tenant_id, reservation.budget_name)]
            account.reserved_units -= reservation.estimated_units
            account.version += 1
            reservation.status = ReservationStatus.EXPIRED
            reservation.settled_at = now
            released += 1
        return released

    def claim_execution(
        self, inbox_id: str, worker_id: str, lease_seconds: int, budget_estimates: dict[str, int]
    ) -> ExecutionClaim:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        tenant_id = self._tenant()
        state = self._store._data.states[tenant_id]
        if state.execution_mode != ExecutionMode.NORMAL:
            raise SecurityRejected(
                f"new claims are disabled while tenant is {state.execution_mode.value}"
            )
        inbox = self._require_inbox(inbox_id)
        now = self._now()
        self.reap_expired_reservations()
        session = self._get_or_create_session(inbox)
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
            if session.lease_expires_at is None or session.lease_expires_at > now:
                raise ExecutionUnavailable("session is owned by another active inbox")
            raise ExecutionUnavailable("expired session must be recovered through its active inbox")
        if (
            session.lease_owner is not None
            and session.lease_expires_at is not None
            and session.lease_expires_at > now
        ):
            raise ExecutionUnavailable("session lease is still active")

        execution_id = inbox.execution_id or stable_id("exe", tenant_id, inbox.inbox_id)
        self._reserve_budgets(execution_id, budget_estimates, now, lease_seconds)
        session.lease_fence += 1
        session.active_inbox_id = inbox.inbox_id
        session.lease_owner = worker_id
        session.lease_expires_at = now + timedelta(seconds=lease_seconds)
        session.updated_at = now
        inbox.execution_id = execution_id
        inbox.execution_attempt += 1
        inbox.status = InboxStatus.CLAIMED
        inbox.claimed_lease_fence = session.lease_fence
        inbox.claimed_routing_epoch = state.routing_epoch
        inbox.claimed_security_epoch = state.security_epoch
        inbox.updated_at = now
        attempt = ExecutionAttempt(
            tenant_id=tenant_id,
            execution_id=execution_id,
            attempt_no=inbox.execution_attempt,
            inbox_id=inbox.inbox_id,
            session_id=session.session_id,
            worker_id=worker_id,
            lease_fence=session.lease_fence,
            routing_epoch=state.routing_epoch,
            security_epoch=state.security_epoch,
            status=AttemptStatus.CLAIMED,
            lease_expires_at=session.lease_expires_at,
            claimed_at=now,
        )
        self._store._data.attempts.setdefault((tenant_id, execution_id), []).append(attempt)
        return ExecutionClaim(
            tenant_id=tenant_id,
            inbox_id=inbox.inbox_id,
            execution_id=execution_id,
            session_id=session.session_id,
            worker_id=worker_id,
            lease_fence=session.lease_fence,
            routing_epoch=state.routing_epoch,
            security_epoch=state.security_epoch,
            session_version=session.version,
            lease_expires_at=session.lease_expires_at,
            attempt_no=inbox.execution_attempt,
            config_version=session.config_version,
        )

    def renew_execution(self, claim: ExecutionClaim, lease_seconds: int) -> ExecutionClaim:
        self.assert_execution(claim)
        session = self._require_session(claim.session_id)
        now = self._now()
        session.lease_expires_at = now + timedelta(seconds=lease_seconds)
        session.updated_at = now
        attempts = self._store._data.attempts[(claim.tenant_id, claim.execution_id)]
        attempts[-1].lease_expires_at = session.lease_expires_at
        attempts[-1].status = AttemptStatus.RUNNING
        for (tenant_id, _), reservation in self._store._data.reservations.items():
            if (
                tenant_id == self._tenant()
                and reservation.execution_id == claim.execution_id
                and reservation.status == ReservationStatus.RESERVED
            ):
                reservation.expires_at = session.lease_expires_at
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
            lease_expires_at=session.lease_expires_at,
            attempt_no=claim.attempt_no,
            config_version=claim.config_version,
        )

    def assert_execution(self, claim: ExecutionClaim) -> None:
        if claim.tenant_id != self._tenant():
            raise TenantMismatch("claim tenant does not match TenantContext")
        state = self._store._data.states[claim.tenant_id]
        inbox = self._require_inbox(claim.inbox_id)
        session = self._require_session(claim.session_id)
        if state.execution_mode != ExecutionMode.NORMAL:
            self._mark_attempt_lost(claim)
            raise SecurityRejected(f"tenant execution mode is {state.execution_mode.value}")
        if (
            state.routing_epoch != claim.routing_epoch
            or state.security_epoch != claim.security_epoch
        ):
            self._mark_attempt_lost(claim)
            raise StaleFence("tenant routing or security epoch changed")
        if (
            inbox.execution_id != claim.execution_id
            or inbox.status != InboxStatus.CLAIMED
            or inbox.claimed_lease_fence != claim.lease_fence
            or inbox.claimed_routing_epoch != claim.routing_epoch
            or inbox.claimed_security_epoch != claim.security_epoch
        ):
            self._mark_attempt_lost(claim)
            raise StaleFence("inbox no longer belongs to this execution")
        if (
            session.active_inbox_id != claim.inbox_id
            or session.lease_owner != claim.worker_id
            or session.lease_fence != claim.lease_fence
            or session.lease_expires_at is None
            or session.lease_expires_at <= self._now()
        ):
            self._mark_attempt_lost(claim)
            raise StaleFence("session lease was lost")

    def prepare_tool(
        self,
        claim: ExecutionClaim,
        tool_step: int,
        tool_name: str,
        arguments_hash: str,
        capability: ToolCapability,
    ) -> ToolExecution:
        self.assert_execution(claim)
        if tool_name in self._store._data.states[claim.tenant_id].tool_denylist:
            raise SecurityRejected(f"tool {tool_name!r} has been revoked")
        existing_id = self._store._data.tool_by_step.get(
            (claim.tenant_id, claim.execution_id, tool_step)
        )
        if existing_id:
            existing = self._store._data.tools[(claim.tenant_id, existing_id)]
            if existing.arguments_hash != arguments_hash or existing.tool_name != tool_name:
                raise ExecutionDivergence("same deterministic tool step has different arguments")
            if existing.lease_fence != claim.lease_fence and existing.status != ToolStatus.SUCCEEDED:
                # A new fenced execution owns the same deterministic tool
                # intent. Never create a second intent: prepared work may run,
                # idempotent work may retry with its original provider key,
                # queryable work must reconcile, and non-retriable work stops
                # for an operator.
                if existing.status in {ToolStatus.PREPARED, ToolStatus.CONFIRMED}:
                    next_status = existing.status
                    reason = existing.last_error_code
                elif existing.retry_capability == ToolCapability.IDEMPOTENT:
                    next_status = ToolStatus.PREPARED
                    reason = "tool_lease_takeover_idempotent_retry"
                elif existing.retry_capability == ToolCapability.QUERYABLE:
                    next_status = ToolStatus.RECONCILING
                    reason = "tool_lease_takeover_requires_reconciliation"
                else:
                    next_status = ToolStatus.MANUAL_REVIEW
                    reason = "tool_lease_takeover_non_retriable"
                existing.lease_fence = claim.lease_fence
                existing.routing_epoch = claim.routing_epoch
                existing.security_epoch = claim.security_epoch
                existing.status = next_status
                existing.last_error_code = reason
                existing.updated_at = self._now()
            return deepcopy(existing)
        now = self._now()
        tool_call_id = stable_id("tool", claim.inbox_id, claim.execution_id, tool_step)
        provider_key = tool_call_id if capability == ToolCapability.IDEMPOTENT else None
        execution = ToolExecution(
            tenant_id=claim.tenant_id,
            tool_call_id=tool_call_id,
            inbox_id=claim.inbox_id,
            execution_id=claim.execution_id,
            session_id=claim.session_id,
            tool_step=tool_step,
            tool_name=tool_name,
            arguments_hash=arguments_hash,
            retry_capability=capability,
            provider_idempotency_key=provider_key,
            lease_fence=claim.lease_fence,
            routing_epoch=claim.routing_epoch,
            security_epoch=claim.security_epoch,
            status=ToolStatus.PREPARED,
            trace_id=self._require_inbox(claim.inbox_id).trace_id,
            created_at=now,
            updated_at=now,
        )
        self._store._data.tools[(claim.tenant_id, tool_call_id)] = execution
        self._store._data.tool_by_step[(claim.tenant_id, claim.execution_id, tool_step)] = (
            tool_call_id
        )
        return deepcopy(execution)

    def start_tool(self, claim: ExecutionClaim, tool_call_id: str) -> ToolExecution:
        self.assert_execution(claim)
        tool = self._require_tool(tool_call_id)
        if tool.execution_id != claim.execution_id or tool.lease_fence != claim.lease_fence:
            raise StaleFence("tool intent belongs to another execution fence")
        if tool.tool_name in self._store._data.states[claim.tenant_id].tool_denylist:
            raise SecurityRejected(f"tool {tool.tool_name!r} has been revoked")
        if tool.status in {ToolStatus.PREPARED, ToolStatus.CONFIRMED}:
            tool.status = ToolStatus.RUNNING
            tool.updated_at = self._now()
        return deepcopy(tool)

    def finish_tool(
        self,
        claim: ExecutionClaim,
        tool_call_id: str,
        status: ToolStatus,
        result: dict | None = None,
        error_code: str | None = None,
        provider_operation_id: str | None = None,
    ) -> ToolExecution:
        """Persist an external result even after a lease loss to prevent re-effects.

        A stale worker may *record* the outcome of its already-started request but
        cannot create another intent, start a Tool, or commit a Session.
        """

        if status not in {
            ToolStatus.SUCCEEDED,
            ToolStatus.FAILED,
            ToolStatus.UNKNOWN,
            ToolStatus.RECONCILING,
            ToolStatus.MANUAL_REVIEW,
        }:
            raise ValueError("tool completion must be a terminal/recovery status")
        tool = self._require_tool(tool_call_id)
        if tool.execution_id != claim.execution_id or tool.inbox_id != claim.inbox_id:
            raise TenantMismatch("tool result does not belong to execution")
        if tool.status not in {ToolStatus.RUNNING, ToolStatus.RECONCILING, ToolStatus.UNKNOWN}:
            return deepcopy(tool)
        tool.status = status
        tool.result = deepcopy(result) if result is not None else None
        tool.last_error_code = error_code
        tool.provider_operation_id = provider_operation_id
        tool.updated_at = self._now()
        return deepcopy(tool)

    def resolve_tool(self, tool_call_id: str, status: ToolStatus, note: str) -> ToolExecution:
        """Record an explicit human resolution without replaying an external call."""

        tool = self._require_tool(tool_call_id)
        if tool.status not in {
            ToolStatus.UNKNOWN,
            ToolStatus.MANUAL_REVIEW,
            ToolStatus.RECONCILING,
        }:
            raise InvalidTransition("only an ambiguous Tool operation may be manually resolved")
        tool.status = status
        tool.last_error_code = note or tool.last_error_code
        tool.updated_at = self._now()
        self._store._data.audits.setdefault(self._tenant(), []).append(
            AuditRecord(
                tenant_id=self._tenant(),
                audit_id=stable_id(
                    "audit", "tool-resolution", tool_call_id, tool.updated_at.isoformat()
                ),
                decision="tool_manual_resolution",
                reason_code=status.value,
                trace_id=tool.trace_id,
                request_id=self.context.request_id,
                session_id=tool.session_id,
            )
        )
        return deepcopy(tool)

    def tool_recovery_action(self, tool_call_id: str):
        tool = self._require_tool(tool_call_id)
        if tool.status == ToolStatus.SUCCEEDED:
            return "reuse_result"
        if tool.retry_capability == ToolCapability.IDEMPOTENT:
            return "retry_with_idempotency_key"
        if tool.retry_capability == ToolCapability.QUERYABLE:
            return "reconcile"
        return "manual_review"

    def _summarize_session(
        self, session: SessionRecord, appended_events: list[SessionEvent], now
    ) -> SessionSummary | None:
        """Create a deterministic summary from committed conversational events only."""

        all_events = self._store._data.events.get((session.tenant_id, session.session_id), [])
        lines: list[str] = []
        for event in all_events[-24:]:
            if event.role not in {"user", "assistant"}:
                continue
            text = event.payload.get("text")
            if isinstance(text, str) and text.strip():
                lines.append(f"{event.role}: {' '.join(text.split())[:800]}")
        if not lines or not appended_events:
            return None
        content = "\n".join(lines)[-6000:]
        based_on_seq = appended_events[-1].seq
        return SessionSummary(
            tenant_id=session.tenant_id,
            session_id=session.session_id,
            summary_id=stable_id("sum", session.session_id, based_on_seq, content_hash(content)),
            based_on_seq=based_on_seq,
            content=content,
            content_hash=content_hash(content),
            model_ref=(
                str(session.state.get("model")) if isinstance(session.state.get("model"), str) else None
            ),
            created_at=now,
        )

    def commit_execution(self, claim: ExecutionClaim, commit: CommitInput) -> CommitResult:
        self.assert_execution(claim)
        session = self._require_session(claim.session_id)
        if (
            commit.expected_session_version != session.version
            or claim.session_version != session.version
        ):
            raise StaleFence("session CAS version changed")
        now = self._now()
        inbox = self._require_inbox(claim.inbox_id)
        events: list[SessionEvent] = []
        seen_event_ids = {
            event.event_id
            for event in self._store._data.events.get((claim.tenant_id, session.session_id), [])
        }
        for offset, draft in enumerate(commit.events, start=1):
            event_id = draft.event_id or stable_id(
                "evt", claim.execution_id, session.last_event_seq + offset, draft.event_type
            )
            if event_id in seen_event_ids:
                raise ExecutionDivergence(f"session event {event_id} already exists")
            events.append(
                SessionEvent(
                    tenant_id=claim.tenant_id,
                    session_id=session.session_id,
                    seq=session.last_event_seq + offset,
                    event_id=event_id,
                    event_type=draft.event_type,
                    role=draft.role,
                    subject_id=draft.subject_id,
                    external_message_id=draft.external_message_id,
                    payload=deepcopy(dict(draft.payload)),
                    trace_id=inbox.trace_id,
                    occurred_at=draft.occurred_at,
                )
            )
        self._settle_budgets(claim.execution_id, commit.actual_budget_units, now)
        self._store._data.events.setdefault((claim.tenant_id, session.session_id), []).extend(
            events
        )
        session.last_event_seq += len(events)
        session.version += 1
        session.state = deepcopy(dict(commit.new_state))
        session.active_inbox_id = None
        session.lease_owner = None
        session.lease_expires_at = None
        session.updated_at = now
        summary = self._summarize_session(session, events, now)
        if summary is not None:
            self._store._data.summaries.setdefault(
                (claim.tenant_id, session.session_id), []
            ).append(summary)
        memory_outboxes: list[OutboxRecord] = []
        for position, draft in enumerate(commit.memories):
            memory_id = draft.memory_id or stable_id(
                "mem", claim.execution_id, position, draft.memory_type, content_hash(draft.content)
            )
            previous = self._store._data.memories.get((claim.tenant_id, memory_id))
            version = (previous.version + 1) if previous else 1
            intent = MemoryIntent(
                tenant_id=claim.tenant_id,
                memory_id=memory_id,
                session_id=session.session_id,
                subject_id=draft.subject_id or inbox.subject_id,
                memory_type=draft.memory_type,
                content=draft.content,
                content_hash=content_hash(draft.content),
                acl=deepcopy(dict(draft.acl)),
                source_event_id=draft.source_event_id,
                version=version,
                expires_at=draft.expires_at,
                created_at=previous.created_at if previous else now,
                updated_at=now,
            )
            self._store._data.memories[(claim.tenant_id, memory_id)] = intent
            memory_outboxes.append(
                self._insert_outbox(
                    aggregate_type="memory",
                    aggregate_id=memory_id,
                    inbox_id=inbox.inbox_id,
                    event_type="memory.project",
                    payload={
                        "tenant_id": claim.tenant_id,
                        "memory_id": memory_id,
                        "requested_version": version,
                    },
                    idempotency_key=f"memory:{memory_id}:{version}",
                    trace_id=inbox.trace_id,
                )
            )
        reply_outbox: OutboxRecord | None = None
        if commit.reply is not None:
            delivery_key = commit.reply.delivery_key or stable_id(
                "reply", claim.execution_id, session.version
            )
            reply_outbox = self._insert_outbox(
                aggregate_type="session",
                aggregate_id=session.session_id,
                inbox_id=inbox.inbox_id,
                event_type="reply.dispatch",
                payload={
                    "tenant_id": claim.tenant_id,
                    "session_id": session.session_id,
                    "inbox_id": inbox.inbox_id,
                    "channel_binding_id": commit.reply.channel_binding_id
                    or inbox.channel_binding_id,
                    "recipient_id": commit.reply.recipient_id or inbox.subject_id,
                    "blocks": [deepcopy(dict(block)) for block in commit.reply.blocks],
                    "delivery_key": delivery_key,
                    "metadata": deepcopy(dict(commit.reply.metadata)),
                },
                idempotency_key=f"reply:{delivery_key}",
                trace_id=inbox.trace_id,
            )
            inbox.status = InboxStatus.REPLY_PENDING
        else:
            inbox.status = InboxStatus.COMMITTED
        inbox.updated_at = now
        attempts = self._store._data.attempts[(claim.tenant_id, claim.execution_id)]
        attempt = next((row for row in attempts if row.attempt_no == claim.attempt_no), None)
        if attempt is not None:
            attempt.status = AttemptStatus.COMMITTED
            attempt.finished_at = now
        audit = AuditRecord(
            tenant_id=claim.tenant_id,
            audit_id=stable_id("audit", claim.execution_id, session.version),
            decision=commit.audit_decision,
            trace_id=inbox.trace_id,
            request_id=inbox.request_id,
            session_id=session.session_id,
            **_audit_fields(commit.audit_metadata),
        )
        self._store._data.audits.setdefault(claim.tenant_id, []).append(audit)
        return CommitResult(
            session=deepcopy(session),
            inbox=deepcopy(inbox),
            events=deepcopy(events),
            summary=deepcopy(summary),
            reply_outbox=deepcopy(reply_outbox),
            memory_outboxes=deepcopy(memory_outboxes),
        )

    def get_memory(self, memory_id: str) -> MemoryIntent | None:
        value = self._store._data.memories.get(self._key(memory_id))
        return deepcopy(value) if value else None

    def put_knowledge_document(self, document: dict) -> dict:
        """Store canonical text and queue an idempotent retrieval projection."""

        tenant_id = self._tenant()
        document_id = str(document.get("document_id") or "")
        knowledge_base_id = str(document.get("knowledge_base_id") or "")
        content = str(document.get("content") or "")
        checksum = str(document.get("checksum") or "")
        acl = document.get("acl") or {}
        if not document_id or not knowledge_base_id or not content.strip() or not checksum:
            raise ValueError("knowledge document requires id, base, content, and checksum")
        if len(content) > 200_000 or not isinstance(acl, Mapping):
            raise ValueError("knowledge document content or ACL is invalid")
        for (row_tenant, row_id), row in self._store._data.knowledge_documents.items():
            if (
                row_tenant == tenant_id
                and row_id != document_id
                and row["knowledge_base_id"] == knowledge_base_id
                and row["checksum"] == checksum
            ):
                raise ExecutionDivergence("knowledge content already has a different document id")
        now = self._now()
        key = (tenant_id, document_id)
        previous = self._store._data.knowledge_documents.get(key)
        version = int(previous["version"]) + 1 if previous else 1
        row = {
            "tenant_id": tenant_id,
            "document_id": document_id,
            "knowledge_base_id": knowledge_base_id,
            "object_uri": str(document.get("object_uri") or f"db://knowledge/{document_id}"),
            "content": content,
            "checksum": checksum,
            "acl": deepcopy(dict(acl)),
            "version": version,
            "index_status": "pending",
            "created_at": previous["created_at"] if previous else now,
            "updated_at": now,
        }
        self._store._data.knowledge_documents[key] = row
        self._insert_outbox(
            aggregate_type="knowledge",
            aggregate_id=document_id,
            inbox_id=None,
            event_type="knowledge.project",
            payload={
                "tenant_id": tenant_id,
                "document_id": document_id,
                "requested_version": version,
            },
            idempotency_key=f"knowledge:{document_id}:{version}",
            trace_id=self.context.trace_id,
        )
        return deepcopy(row)

    def list_outbox(self) -> list[OutboxRecord]:
        tenant_id = self._tenant()
        return deepcopy(
            [
                value
                for (row_tenant, _), value in self._store._data.outboxes.items()
                if row_tenant == tenant_id
            ]
        )

    def list_audit(self) -> list[AuditRecord]:
        return deepcopy(self._store._data.audits.get(self._tenant(), []))

    def record_audit(
        self,
        decision: str,
        audit_id: str,
        *,
        session_id: str | None = None,
        metadata: dict | None = None,
    ) -> AuditRecord:
        """Append a redacted compliance fact outside a successful execution commit.

        This is used for denials, failed executions, and outbound delivery
        outcomes.  Callers may supply only the typed, allow-listed fields used
        by ``_audit_fields``; raw channel/model payloads never reach the ledger.
        """

        if not decision or not audit_id:
            raise ValueError("audit decision and audit_id are required")
        audit = AuditRecord(
            tenant_id=self._tenant(),
            audit_id=audit_id,
            decision=decision[:128],
            trace_id=self.context.trace_id,
            request_id=self.context.request_id,
            session_id=session_id,
            **_audit_fields(metadata or {}),
            occurred_at=self._now(),
        )
        records = self._store._data.audits.setdefault(self._tenant(), [])
        existing = next((item for item in records if item.audit_id == audit_id), None)
        if existing is not None:
            return deepcopy(existing)
        records.append(audit)
        return deepcopy(audit)

    def current_route(self) -> StorageRoute | None:
        state = self._store._data.states[self._tenant()]
        route = self._store._data.routes.get((state.tenant_id, state.routing_epoch))
        return deepcopy(route) if route else None

    def initiate_migration(
        self, target_profile: dict, migration_id: str | None = None
    ) -> StorageMigration:
        tenant_id = self._tenant()
        state = self._store._data.states[tenant_id]
        if state.execution_mode != ExecutionMode.NORMAL:
            raise MigrationBusy("cannot begin migration while tenant is not normal")
        if any(
            row.tenant_id == tenant_id
            and row.status
            in {
                MigrationStatus.PREPARING,
                MigrationStatus.BACKFILLING,
                MigrationStatus.CATCHING_UP,
                MigrationStatus.DRAINING,
                MigrationStatus.VERIFYING,
                MigrationStatus.READONLY,
            }
            for row in self._store._data.migrations.values()
        ):
            raise MigrationBusy("another storage migration is in progress")
        current = self._store._data.routes[(tenant_id, state.routing_epoch)]
        migration_id = migration_id or stable_id(
            "mig", tenant_id, content_hash(target_profile), self._now().isoformat()
        )
        key = (tenant_id, migration_id)
        if key in self._store._data.migrations:
            return deepcopy(self._store._data.migrations[key])
        migration = StorageMigration(
            tenant_id=tenant_id,
            migration_id=migration_id,
            source_profile=deepcopy(current.profile),
            target_profile=deepcopy(target_profile),
            status=MigrationStatus.PREPARING,
            source_routing_epoch=state.routing_epoch,
            created_at=self._now(),
            updated_at=self._now(),
        )
        self._store._data.migrations[key] = migration
        return deepcopy(migration)

    def migration(self, migration_id: str) -> StorageMigration:
        migration = self._store._data.migrations.get(self._key(migration_id))
        if migration is None:
            raise NotFound(f"migration {migration_id!r} was not found")
        return deepcopy(migration)

    def transition_migration(
        self,
        migration_id: str,
        action: str,
        *,
        source_watermark: str | None = None,
        target_watermark: str | None = None,
        verified: bool | None = None,
    ) -> StorageMigration:
        tenant_id = self._tenant()
        state = self._store._data.states[tenant_id]
        migration = self._store._data.migrations.get((tenant_id, migration_id))
        if migration is None:
            raise NotFound(f"migration {migration_id!r} was not found")
        now = self._now()
        if action == "start_backfill" and migration.status == MigrationStatus.PREPARING:
            migration.status = MigrationStatus.BACKFILLING
        elif action == "catch_up" and migration.status == MigrationStatus.BACKFILLING:
            migration.status = MigrationStatus.CATCHING_UP
            migration.source_watermark = source_watermark
            migration.target_watermark = target_watermark
        elif action == "record_catch_up" and migration.status == MigrationStatus.CATCHING_UP:
            migration.source_watermark = source_watermark
            migration.target_watermark = target_watermark
        elif action == "begin_drain" and migration.status == MigrationStatus.CATCHING_UP:
            state.execution_mode = ExecutionMode.DRAINING
            state.security_epoch += 1
            state.updated_at = now
            migration.status = MigrationStatus.DRAINING
        elif action == "verify" and migration.status == MigrationStatus.DRAINING:
            if self._has_live_leases(tenant_id, now):
                raise MigrationBusy("cannot verify while tenant has live executions")
            if not verified:
                migration.status = MigrationStatus.FAILED
                migration.error = "target watermark verification failed"
                state.execution_mode = ExecutionMode.NORMAL
                state.security_epoch += 1
            else:
                migration.status = MigrationStatus.VERIFYING
                migration.source_watermark = source_watermark or migration.source_watermark
                migration.target_watermark = target_watermark or migration.target_watermark
        elif action == "cutover" and migration.status == MigrationStatus.VERIFYING:
            if self._has_live_leases(tenant_id, now):
                raise MigrationBusy("cannot cut over while tenant has live executions")
            old_route = self._store._data.routes[(tenant_id, state.routing_epoch)]
            old_route.status = MigrationStatus.READONLY
            state.routing_epoch += 1
            state.security_epoch += 1
            state.execution_mode = ExecutionMode.NORMAL
            state.updated_at = now
            new_route = StorageRoute(
                tenant_id=tenant_id,
                routing_epoch=state.routing_epoch,
                profile=deepcopy(migration.target_profile),
                status=MigrationStatus.ACTIVE,
                source_watermark=migration.source_watermark,
                target_watermark=migration.target_watermark,
                activated_at=now,
            )
            self._store._data.routes[(tenant_id, state.routing_epoch)] = new_route
            migration.target_routing_epoch = state.routing_epoch
            migration.status = MigrationStatus.ACTIVE
        elif action == "begin_rollback" and migration.status == MigrationStatus.ACTIVE:
            state.execution_mode = ExecutionMode.DRAINING
            state.security_epoch += 1
            state.updated_at = now
            migration.status = MigrationStatus.READONLY
        elif action == "complete_rollback" and migration.status == MigrationStatus.READONLY:
            if self._has_live_leases(tenant_id, now):
                raise MigrationBusy("cannot roll back while tenant has live executions")
            active_route = self._store._data.routes[(tenant_id, state.routing_epoch)]
            active_route.status = MigrationStatus.RETIRED
            state.routing_epoch += 1
            state.security_epoch += 1
            state.execution_mode = ExecutionMode.NORMAL
            state.updated_at = now
            self._store._data.routes[(tenant_id, state.routing_epoch)] = StorageRoute(
                tenant_id=tenant_id,
                routing_epoch=state.routing_epoch,
                profile=deepcopy(migration.source_profile),
                status=MigrationStatus.ACTIVE,
                activated_at=now,
            )
            migration.status = MigrationStatus.RETIRED
        elif action == "cancel" and migration.status in {
            MigrationStatus.PREPARING,
            MigrationStatus.BACKFILLING,
            MigrationStatus.CATCHING_UP,
        }:
            migration.status = MigrationStatus.RETIRED
        else:
            raise InvalidTransition(f"cannot {action} migration in state {migration.status.value}")
        migration.updated_at = now
        return deepcopy(migration)

    def snapshot(self, *, include_audit: bool = True) -> dict[str, Any]:
        tenant_id = self._tenant()
        return {
            "runtime_state": to_primitive(self._store._data.states[tenant_id]),
            "inboxes": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.inboxes.items()
                if row_tenant == tenant_id
            ],
            "outbox": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.outboxes.items()
                if row_tenant == tenant_id
            ],
            "sessions": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.sessions.items()
                if row_tenant == tenant_id
            ],
            "events": [
                to_primitive(value)
                for (row_tenant, _), values in self._store._data.events.items()
                if row_tenant == tenant_id
                for value in values
            ],
            "summaries": [
                to_primitive(value)
                for (row_tenant, _), values in self._store._data.summaries.items()
                if row_tenant == tenant_id
                for value in values
            ],
            "memories": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.memories.items()
                if row_tenant == tenant_id
            ],
            "knowledge": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.knowledge_documents.items()
                if row_tenant == tenant_id
            ],
            "tools": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.tools.items()
                if row_tenant == tenant_id
            ],
            "audit": (
                [to_primitive(value) for value in self._store._data.audits.get(tenant_id, [])]
                if include_audit
                else []
            ),
            "migrations": [
                to_primitive(value)
                for (row_tenant, _), value in self._store._data.migrations.items()
                if row_tenant == tenant_id
            ],
        }

    def _require_inbox(self, inbox_id: str) -> InboxRecord:
        value = self._store._data.inboxes.get(self._key(inbox_id))
        if value is None:
            raise NotFound(f"inbox {inbox_id!r} was not found")
        return value

    def _require_outbox(self, outbox_id: str) -> OutboxRecord:
        value = self._store._data.outboxes.get(self._key(outbox_id))
        if value is None:
            raise NotFound(f"outbox {outbox_id!r} was not found")
        return value

    def _require_session(self, session_id: str) -> SessionRecord:
        value = self._store._data.sessions.get(self._key(session_id))
        if value is None:
            raise NotFound(f"session {session_id!r} was not found")
        return value

    def _require_tool(self, tool_call_id: str) -> ToolExecution:
        value = self._store._data.tools.get(self._key(tool_call_id))
        if value is None:
            raise NotFound(f"tool intent {tool_call_id!r} was not found")
        return value

    def _get_or_create_session(self, inbox: InboxRecord) -> SessionRecord:
        key = (inbox.tenant_id, inbox.session_id)
        session = self._store._data.sessions.get(key)
        if session is None:
            session = SessionRecord(
                tenant_id=inbox.tenant_id,
                session_id=inbox.session_id,
                agent_id=inbox.agent_id,
                channel_binding_id=inbox.channel_binding_id,
                config_version=inbox.config_version,
                created_at=self._now(),
                updated_at=self._now(),
            )
            self._store._data.sessions[key] = session
        return session

    def _reserve_budgets(
        self, execution_id: str, estimates: dict[str, int], now, lease_seconds: int
    ) -> None:
        tenant_id = self._tenant()
        for budget_name in sorted(estimates):
            estimate = estimates[budget_name]
            if estimate < 0:
                raise ValueError("budget estimate cannot be negative")
            account = self._store._data.budgets.get((tenant_id, budget_name))
            if account is None:
                raise BudgetExceeded(f"hard budget account {budget_name!r} is unavailable")
            reservation_index = (tenant_id, execution_id, budget_name)
            existing_id = self._store._data.reservations_by_execution.get(reservation_index)
            if existing_id:
                existing = self._store._data.reservations[(tenant_id, existing_id)]
                if existing.status in {ReservationStatus.RESERVED, ReservationStatus.UNKNOWN}:
                    continue
            if account.spent_units + account.reserved_units + estimate > account.limit_units:
                raise BudgetExceeded(f"hard budget {budget_name!r} would be exceeded")
            account.reserved_units += estimate
            account.version += 1
            reservation_id = stable_id("res", tenant_id, execution_id, budget_name)
            reservation = BudgetReservation(
                tenant_id=tenant_id,
                reservation_id=reservation_id,
                budget_name=budget_name,
                execution_id=execution_id,
                estimated_units=estimate,
                status=ReservationStatus.RESERVED,
                expires_at=now + timedelta(seconds=lease_seconds),
                created_at=now,
            )
            self._store._data.reservations[(tenant_id, reservation_id)] = reservation
            self._store._data.reservations_by_execution[reservation_index] = reservation_id

    def _settle_budgets(self, execution_id: str, actuals: dict[str, int] | Any, now) -> None:
        tenant_id = self._tenant()
        reservations = [
            reservation
            for (row_tenant, _), reservation in self._store._data.reservations.items()
            if row_tenant == tenant_id
            and reservation.execution_id == execution_id
            and reservation.status == ReservationStatus.RESERVED
        ]
        for reservation in reservations:
            actual = int(actuals.get(reservation.budget_name, reservation.estimated_units))
            if actual < 0:
                raise ValueError("actual budget usage cannot be negative")
            account = self._store._data.budgets[(tenant_id, reservation.budget_name)]
            # A model that exceeds its maximum reservation cannot be committed
            # without a second conditional reservation.  This fails closed.
            additional = max(0, actual - reservation.estimated_units)
            if account.spent_units + account.reserved_units + additional > account.limit_units:
                raise BudgetExceeded(
                    f"actual usage exceeds hard budget {reservation.budget_name!r}"
                )
            account.reserved_units -= reservation.estimated_units
            account.spent_units += actual
            account.version += 1
            reservation.actual_units = actual
            reservation.status = ReservationStatus.SETTLED
            reservation.settled_at = now

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
    ) -> OutboxRecord:
        tenant_id = self._tenant()
        existing_id = self._store._data.outbox_by_idempotency.get((tenant_id, idempotency_key))
        if existing_id:
            return deepcopy(self._store._data.outboxes[(tenant_id, existing_id)])
        outbox_id = stable_id("obx", tenant_id, event_type, idempotency_key)
        now = self._now()
        outbox = OutboxRecord(
            tenant_id=tenant_id,
            outbox_id=outbox_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            inbox_id=inbox_id,
            event_type=event_type,
            payload=deepcopy(payload),
            idempotency_key=idempotency_key,
            trace_id=trace_id,
            created_at=now,
            available_at=now,
        )
        self._store._data.outboxes[(tenant_id, outbox_id)] = outbox
        self._store._data.outbox_by_idempotency[(tenant_id, idempotency_key)] = outbox_id
        return deepcopy(outbox)

    def _mark_attempt_lost(self, claim: ExecutionClaim) -> None:
        attempts = self._store._data.attempts.get((claim.tenant_id, claim.execution_id), [])
        for attempt in attempts:
            if attempt.attempt_no == claim.attempt_no and attempt.status in {
                AttemptStatus.CLAIMED,
                AttemptStatus.RUNNING,
            }:
                attempt.status = AttemptStatus.LOST_FENCE
                attempt.finished_at = self._now()

    def _has_live_leases(self, tenant_id: str, now) -> bool:
        return any(
            session.lease_owner is not None
            and session.lease_expires_at is not None
            and session.lease_expires_at > now
            for (row_tenant, _), session in self._store._data.sessions.items()
            if row_tenant == tenant_id
        )
