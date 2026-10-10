"""Repository and adapter contracts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol

from trpc_agent_sdk.sessions import BaseSessionService

from trpc_service.channels.envelopes import DeliveryReceipt, InboundEnvelope
from trpc_service.storage.artifacts import ArtifactMetadata
from trpc_service.storage.models import (
    Acceptance,
    BindingRoute,
    CommitResult,
    DeliveryAttempt,
    MailboxLease,
    OutboxRecord,
    SessionClaim,
    SessionLease,
    SessionMailbox,
    SessionSnapshot,
    SummarySnapshot,
    TurnCommit,
)
from trpc_service.tenant.models import Channel, ChannelBinding, TenantConfig, TenantContext


class FencingConflict(RuntimeError):
    """The caller no longer owns the current session epoch."""


class DeliveryInProgress(RuntimeError):
    """An earlier provider call is still unresolved for this outbound row.

    ``attempt_number`` is included when the repository can identify the
    durable in-flight attempt.  A new dispatcher owner may then deliberately
    resolve that attempt as ``ambiguous`` (after its outbox claim wins),
    instead of starting a second provider request.  ``None`` preserves the
    compatibility behaviour for older repositories that do not expose the
    attempt ledger.
    """

    def __init__(self, message: str, *, attempt_number: int | None = None) -> None:
        super().__init__(message)
        self.attempt_number = attempt_number


class SessionMailboxStore(Protocol):
    """Atomic per-session ordering and fencing transitions."""

    async def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None: ...

    async def accept(
        self,
        tenant_id: str,
        session_id: str,
        inbound_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
    ) -> SessionMailbox: ...

    async def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        acceptance: Acceptance | None = None,
    ) -> SessionClaim: ...

    async def claim(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
    ) -> MailboxLease | None: ...

    async def renew(self, lease: MailboxLease, *, lease_for: timedelta) -> MailboxLease: ...

    async def renew_many(
        self, leases: tuple[MailboxLease, ...], *, lease_for: timedelta
    ) -> tuple[MailboxLease, ...]: ...

    async def commit(self, lease: MailboxLease) -> SessionMailbox: ...

    async def reschedule(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox: ...

    async def retry(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox: ...

    async def recover(self, tenant_id: str, session_id: str) -> SessionMailbox | None: ...

    async def reconcile(self, tenant_id: str, session_id: str) -> SessionMailbox | None: ...

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int: ...

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int: ...

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int: ...


class RuntimeRepository(Protocol):
    async def resolve_binding(self, binding_id: str) -> BindingRoute | None: ...

    async def list_bindings(self, channel: Channel) -> tuple[ChannelBinding, ...]: ...

    async def get_config(self, tenant_id: str, app_id: str, version: int) -> TenantConfig: ...

    async def get_acceptance(self, tenant_id: str, inbound_id: str) -> Acceptance | None: ...

    async def get_session_snapshot(
        self, tenant_id: str, session_id: str
    ) -> SessionSnapshot | None: ...

    async def claim_session_ready(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        expected_event_id: str | None = None,
        acceptance: Acceptance | None = None,
    ) -> SessionClaim: ...

    async def renew_session_ready(
        self, lease: SessionLease, *, lease_for: timedelta
    ) -> SessionLease: ...

    async def commit_session_ready(self, commit: TurnCommit) -> CommitResult: ...

    async def retry_session_ready(
        self, lease: SessionLease, *, error_type: str, delay: timedelta
    ) -> None: ...

    async def fail_session_ready(self, lease: SessionLease, *, error_type: str) -> None: ...

    async def accept_inbound(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
    ) -> Acceptance: ...

    async def accept_inbound_v2(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
        priority: int = 0,
        retry_at: datetime | None = None,
    ) -> Acceptance: ...

    async def acquire(
        self,
        *,
        acceptance: Acceptance,
        worker_id: str,
        lease_for: timedelta,
    ) -> SessionLease | None: ...

    async def renew(self, lease: SessionLease, *, lease_for: timedelta) -> SessionLease: ...

    async def commit(self, commit: TurnCommit) -> CommitResult: ...

    async def fail(self, lease: SessionLease, *, error_type: str) -> None: ...

    async def claim_outbox(
        self,
        *,
        event_type: str,
        owner_id: str,
        limit: int,
        lease_for: timedelta,
    ) -> tuple[OutboxRecord, ...]: ...

    async def mark_outbox_published(
        self, tenant_id: str, outbox_id: str, *, owner_id: str
    ) -> None: ...

    async def release_outbox(
        self,
        tenant_id: str,
        outbox_id: str,
        *,
        owner_id: str,
        delay: timedelta,
        error_type: str,
    ) -> None: ...

    async def dead_letter_outbox(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        reason: str,
    ) -> None: ...

    async def record_delivery(
        self, tenant_id: str, receipt: DeliveryReceipt, *, retrying: bool = False
    ) -> None: ...

    async def begin_delivery(self, record: OutboxRecord, *, owner_id: str) -> DeliveryAttempt: ...

    async def finish_delivery(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        attempt_number: int,
        receipt: DeliveryReceipt,
        retry_delay: timedelta = timedelta(seconds=1),
    ) -> None: ...


class ProjectionStore(Protocol):
    async def put_session(
        self, tenant_id: str, session_id: str, *, sequence: int, value: dict[str, Any]
    ) -> None: ...

    async def get_session(
        self, tenant_id: str, session_id: str, *, minimum_sequence: int
    ) -> dict[str, Any] | None: ...


class SessionStore(Protocol):
    """Tenant-scoped authoritative session reads used by a turn/projector."""

    async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None: ...

    def open_turn(self, snapshot: SessionSnapshot) -> BaseSessionService: ...


class MemoryStore(Protocol):
    """Authoritative, tenant-scoped memory records.

    Implementations must commit to PostgreSQL (or an explicitly selected
    equivalent) before projecting to a vector or external memory service.
    """

    async def put(
        self,
        tenant_id: str,
        principal_id: str,
        value: Mapping[str, object],
        *,
        memory_id: str | None = None,
        session_id: str | None = None,
        source_sequence: int | None = None,
    ) -> str: ...

    async def list_recent(
        self, tenant_id: str, principal_id: str, *, limit: int = 100
    ) -> tuple[dict[str, object], ...]: ...


class SummaryStore(Protocol):
    """Monotonic session-summary store with optimistic compare-and-set."""

    async def get(self, tenant_id: str, session_id: str) -> SummarySnapshot | None: ...

    async def put(
        self,
        tenant_id: str,
        session_id: str,
        *,
        up_to_sequence: int,
        summary: Mapping[str, object],
        expected_version: int | None = None,
    ) -> bool: ...


class ArtifactStore(Protocol):
    async def stage(
        self, tenant_id: str, artifact_id: str, content: bytes, *, checksum: str
    ) -> str: ...

    async def commit(self, tenant_id: str, artifact_id: str, staged_key: str) -> str: ...

    async def discard(self, staged_key: str) -> None: ...


class ArtifactIngestionStore(ArtifactStore, Protocol):
    """Artifact object stores that accept downloaded provider media bytes."""

    async def ingest_media(
        self,
        tenant_id: str,
        channel: str,
        external_message_id: str,
        provider_media_id: str,
        content: bytes,
        *,
        checksum: str | None = None,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactMetadata: ...

    async def read(self, tenant_id: str, object_key: str) -> bytes: ...


class KnowledgeStore(Protocol):
    async def upsert(
        self,
        tenant_id: str,
        item_id: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> None: ...

    async def search(
        self,
        tenant_id: str,
        embedding: list[float],
        *,
        limit: int = 5,
    ) -> tuple[dict[str, object], ...]: ...


class AuditStore(Protocol):
    """Structured, redacted audit sink.  Content is intentionally absent."""

    async def append(
        self,
        tenant_id: str,
        *,
        decision: str,
        trace_id: str,
        channel: str | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_name: str | None = None,
        tool_name: str | None = None,
        latency_ms: int | None = None,
        error_type: str | None = None,
        cost_units: int = 0,
        config_version: int | None = None,
        policy_version: int | None = None,
        idempotency_key: str | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> str: ...


__all__ = [
    "ArtifactIngestionStore",
    "ArtifactStore",
    "AuditStore",
    "DeliveryInProgress",
    "FencingConflict",
    "KnowledgeStore",
    "MemoryStore",
    "ProjectionStore",
    "RuntimeRepository",
    "SessionMailboxStore",
    "SessionStore",
    "SummaryStore",
]
