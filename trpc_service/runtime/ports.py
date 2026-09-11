"""Ports for durable facts and the at-least-once transport.

``RuntimeStore`` is intentionally synchronous: database implementations should
make each method one transaction (or use a transaction-bound repository).  This
keeps invariants explicit and lets FastAPI dispatch blocking database work in its
normal worker pool.  The provided in-memory implementation is suitable only for
tests/demo, never as a multi-process fact store.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol

from .models import (
    Acceptance,
    AuditRecord,
    BudgetAccount,
    CommitInput,
    CommitResult,
    ExecutionClaim,
    InboundEnvelope,
    MemoryIntent,
    OutboxRecord,
    StorageMigration,
    StorageRoute,
    TenantContext,
    TenantRuntimeState,
    ToolCapability,
    ToolExecution,
    ToolStatus,
)


class RuntimeTransaction(Protocol):
    """Tenant-scoped transaction; every method is scoped to its context."""

    context: TenantContext

    def runtime_state(self) -> TenantRuntimeState: ...

    def accept_inbound(self, envelope: InboundEnvelope) -> Acceptance: ...

    def claim_outbox(self, owner: str, limit: int, lease_seconds: int) -> list[OutboxRecord]: ...

    def mark_outbox_published(self, outbox_id: str, owner: str) -> None: ...

    def mark_outbox_delivered(self, outbox_id: str) -> None: ...

    def release_outbox(self, outbox_id: str, owner: str, delay_seconds: int = 0) -> None: ...

    def requeue_outbox(self, outbox_id: str) -> None: ...

    def claim_execution(
        self, inbox_id: str, worker_id: str, lease_seconds: int, budget_estimates: dict[str, int]
    ) -> ExecutionClaim: ...

    def renew_execution(self, claim: ExecutionClaim, lease_seconds: int) -> ExecutionClaim: ...

    def assert_execution(self, claim: ExecutionClaim) -> None: ...

    def commit_execution(self, claim: ExecutionClaim, commit: CommitInput) -> CommitResult: ...

    def prepare_tool(
        self,
        claim: ExecutionClaim,
        tool_step: int,
        tool_name: str,
        arguments_hash: str,
        capability: ToolCapability,
    ) -> ToolExecution: ...

    def start_tool(self, claim: ExecutionClaim, tool_call_id: str) -> ToolExecution: ...

    def finish_tool(
        self,
        claim: ExecutionClaim,
        tool_call_id: str,
        status: ToolStatus,
        result: dict | None = None,
        error_code: str | None = None,
        provider_operation_id: str | None = None,
    ) -> ToolExecution: ...

    def resolve_tool(self, tool_call_id: str, status: ToolStatus, note: str) -> ToolExecution: ...

    def put_budget_account(self, account: BudgetAccount) -> BudgetAccount: ...

    def reap_expired_reservations(self) -> int: ...

    def get_memory(self, memory_id: str) -> MemoryIntent | None: ...

    def put_knowledge_document(self, document: dict) -> dict: ...

    def list_outbox(self) -> list[OutboxRecord]: ...

    def list_audit(self) -> list[AuditRecord]: ...

    def record_audit(
        self,
        decision: str,
        audit_id: str,
        *,
        session_id: str | None = None,
        metadata: dict | None = None,
    ) -> AuditRecord: ...

    def current_route(self) -> StorageRoute | None: ...

    def initiate_migration(
        self, target_profile: dict, migration_id: str | None = None
    ) -> StorageMigration: ...

    def migration(self, migration_id: str) -> StorageMigration: ...

    def transition_migration(
        self,
        migration_id: str,
        action: str,
        *,
        source_watermark: str | None = None,
        target_watermark: str | None = None,
        verified: bool | None = None,
    ) -> StorageMigration: ...

    def snapshot(self, *, include_audit: bool = True) -> dict: ...


class RuntimeStore(Protocol):
    def transaction(self, context: TenantContext) -> AbstractContextManager[RuntimeTransaction]: ...


class MessageBus(Protocol):
    """The bus may deliver the same event more than once; consumers use Inbox IDs."""

    def publish(self, event: OutboxRecord, partition_key: str) -> None: ...


class Clock(Protocol):
    def now(self): ...
