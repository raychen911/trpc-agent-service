"""Public orchestration service for Inbox/Outbox, workers, tools and migrations."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from .models import (
    Acceptance,
    BudgetAccount,
    CommitInput,
    CommitResult,
    ExecutionClaim,
    ExecutionMode,
    InboundEnvelope,
    OutboxRecord,
    TenantContext,
    TenantRuntimeState,
    ToolCapability,
    ToolExecution,
    ToolRecoveryAction,
    ToolStatus,
    content_hash,
)
from .ports import MessageBus, RuntimeStore


@dataclass(slots=True)
class InMemoryMessageBus:
    """At-least-once test transport; duplicate delivery is intentionally allowed."""

    published: list[tuple[str, OutboxRecord]] = field(default_factory=list)
    fail_next_publish: bool = False

    def publish(self, event: OutboxRecord, partition_key: str) -> None:
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise OSError("injected message bus failure")
        self.published.append((partition_key, deepcopy(event)))

    def duplicate_last(self) -> None:
        if self.published:
            self.published.append(deepcopy(self.published[-1]))


class PlatformRuntime:
    """Small, explicit service API shared by Gateway, Worker and Admin surfaces.

    It never publishes before the Inbox/Outbox transaction has returned, and it
    never delegates correctness to the message bus.  A SQL-backed ``RuntimeStore``
    must execute each `transaction` block using tenant-scoped SQL transactions.
    """

    def __init__(self, store: RuntimeStore, bus: MessageBus | None = None) -> None:
        self.store = store
        self.bus = bus

    def accept_inbound(self, context: TenantContext, envelope: InboundEnvelope) -> Acceptance:
        with self.store.transaction(context) as tx:
            return tx.accept_inbound(envelope)

    def dispatch_once(
        self,
        context: TenantContext,
        dispatcher_id: str,
        *,
        limit: int = 100,
        lease_seconds: int = 30,
        retry_delay_seconds: int = 0,
    ) -> list[OutboxRecord]:
        """Publish a leased batch and persist publication after each bus ACK.

        A crash between `publish` and `mark_outbox_published` yields a duplicate
        later, which is intentional.  There is no direct publish on acceptance.
        """

        if self.bus is None:
            raise RuntimeError("a MessageBus is required to dispatch Outbox records")
        with self.store.transaction(context) as tx:
            batch = tx.claim_outbox(dispatcher_id, limit, lease_seconds)
        published: list[OutboxRecord] = []
        for event in batch:
            partition = str(event.payload.get("session_id") or event.aggregate_id)
            try:
                self.bus.publish(event, partition)
            except Exception:
                with self.store.transaction(context) as tx:
                    tx.release_outbox(event.outbox_id, dispatcher_id, retry_delay_seconds)
                continue
            with self.store.transaction(context) as tx:
                tx.mark_outbox_published(event.outbox_id, dispatcher_id)
            published.append(event)
        return published

    def claim_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        worker_id: str,
        *,
        lease_seconds: int = 60,
        budget_estimates: Mapping[str, int] | None = None,
    ) -> ExecutionClaim:
        with self.store.transaction(context) as tx:
            return tx.claim_execution(
                inbox_id, worker_id, lease_seconds, dict(budget_estimates or {})
            )

    def renew_execution(
        self, context: TenantContext, claim: ExecutionClaim, *, lease_seconds: int = 60
    ) -> ExecutionClaim:
        with self.store.transaction(context) as tx:
            return tx.renew_execution(claim, lease_seconds)

    def assert_execution(self, context: TenantContext, claim: ExecutionClaim) -> None:
        with self.store.transaction(context) as tx:
            tx.assert_execution(claim)

    def commit_execution(
        self, context: TenantContext, claim: ExecutionClaim, commit: CommitInput
    ) -> CommitResult:
        with self.store.transaction(context) as tx:
            return tx.commit_execution(claim, commit)

    def put_knowledge_document(self, context: TenantContext, document: Mapping[str, Any]) -> dict:
        """Persist a tenant knowledge fact and queue its retrieval projection."""

        with self.store.transaction(context) as tx:
            return tx.put_knowledge_document(dict(document))

    def put_budget_account(self, context: TenantContext, account: BudgetAccount) -> BudgetAccount:
        with self.store.transaction(context) as tx:
            return tx.put_budget_account(account)

    def reap_expired_reservations(self, context: TenantContext) -> int:
        """Release only unpaid reservations whose execution lease has expired."""

        with self.store.transaction(context) as tx:
            return tx.reap_expired_reservations()

    def runtime_state(self, context: TenantContext) -> TenantRuntimeState:
        with self.store.transaction(context) as tx:
            return tx.runtime_state()

    def set_execution_mode(self, context: TenantContext, mode: ExecutionMode) -> TenantRuntimeState:
        with self.store.transaction(context) as tx:
            return tx.set_execution_mode(mode)

    def suspend(self, context: TenantContext, *, emergency: bool = False) -> TenantRuntimeState:
        return self.set_execution_mode(
            context, ExecutionMode.EMERGENCY_STOP if emergency else ExecutionMode.SUSPENDED
        )

    def revoke_tool(self, context: TenantContext, tool_name: str) -> TenantRuntimeState:
        with self.store.transaction(context) as tx:
            return tx.revoke_tool(tool_name)

    def prepare_tool(
        self,
        context: TenantContext,
        claim: ExecutionClaim,
        *,
        tool_step: int,
        tool_name: str,
        arguments: Mapping[str, Any],
        capability: ToolCapability,
    ) -> ToolExecution:
        with self.store.transaction(context) as tx:
            return tx.prepare_tool(claim, tool_step, tool_name, content_hash(arguments), capability)

    def start_tool(
        self, context: TenantContext, claim: ExecutionClaim, tool_call_id: str
    ) -> ToolExecution:
        with self.store.transaction(context) as tx:
            return tx.start_tool(claim, tool_call_id)

    def finish_tool(
        self,
        context: TenantContext,
        claim: ExecutionClaim,
        tool_call_id: str,
        *,
        status: ToolStatus,
        result: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        provider_operation_id: str | None = None,
    ) -> ToolExecution:
        with self.store.transaction(context) as tx:
            return tx.finish_tool(
                claim,
                tool_call_id,
                status,
                dict(result) if result is not None else None,
                error_code,
                provider_operation_id,
            )

    def tool_recovery_action(self, context: TenantContext, tool_call_id: str) -> ToolRecoveryAction:
        with self.store.transaction(context) as tx:
            return ToolRecoveryAction(tx.tool_recovery_action(tool_call_id))

    def resolve_tool(
        self, context: TenantContext, tool_call_id: str, *, status: ToolStatus, note: str = ""
    ) -> ToolExecution:
        with self.store.transaction(context) as tx:
            return tx.resolve_tool(tool_call_id, status, note)

    def initiate_migration(
        self,
        context: TenantContext,
        target_profile: Mapping[str, Any],
        *,
        migration_id: str | None = None,
    ):
        with self.store.transaction(context) as tx:
            return tx.initiate_migration(dict(target_profile), migration_id)

    def migration_action(
        self,
        context: TenantContext,
        migration_id: str,
        action: str,
        *,
        source_watermark: str | None = None,
        target_watermark: str | None = None,
        verified: bool | None = None,
    ):
        with self.store.transaction(context) as tx:
            return tx.transition_migration(
                migration_id,
                action,
                source_watermark=source_watermark,
                target_watermark=target_watermark,
                verified=verified,
            )

    def get_migration(self, context: TenantContext, migration_id: str):
        with self.store.transaction(context) as tx:
            return tx.migration(migration_id)

    def current_route(self, context: TenantContext):
        with self.store.transaction(context) as tx:
            return tx.current_route()

    def snapshot(self, context: TenantContext, *, include_audit: bool = True) -> dict[str, Any]:
        """Demo/debug helper; never use as a cross-tenant admin query."""

        with self.store.transaction(context) as tx:
            return tx.snapshot(include_audit=include_audit)

    def record_audit(
        self,
        context: TenantContext,
        decision: str,
        audit_id: str,
        *,
        session_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """Persist a redacted non-success audit fact in the tenant transaction."""

        with self.store.transaction(context) as tx:
            return tx.record_audit(
                decision,
                audit_id,
                session_id=session_id,
                metadata=dict(metadata or {}),
            )

    def mark_outbox_delivered(self, context: TenantContext, outbox_id: str) -> None:
        with self.store.transaction(context) as tx:
            tx.mark_outbox_delivered(outbox_id)

    def requeue_outbox(self, context: TenantContext, outbox_id: str) -> None:
        """Return a non-delivered event to the durable dispatcher queue."""

        with self.store.transaction(context) as tx:
            tx.requeue_outbox(outbox_id)


__all__ = ["InMemoryMessageBus", "PlatformRuntime"]
