"""Narrow storage interfaces shared by all backend adapters."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from tenant_agent.models import (
    ArtifactRecord,
    AuditRecord,
    ConfigVersion,
    KnowledgeRecord,
    MemoryRecord,
    OutboundMessage,
    ProcessingReceipt,
    SessionEvent,
    SessionSnapshot,
    SummaryRecord,
    TenantConfig,
    UsageDelta,
)


class StorageError(RuntimeError):
    pass


class ConcurrentWriteError(StorageError):
    pass


class SessionLeaseTimeout(StorageError):
    pass


class TenantNotFound(StorageError):
    pass


class BindingNotFound(StorageError):
    pass


def lexical_terms(text: str) -> set[str]:
    """Normalize lightweight lexical-memory tokens without losing literal percent signs."""

    return set(re.findall(r"[\w%]+", text.casefold()))


def same_artifact_payload(
    left: ArtifactRecord,
    left_content: bytes,
    right: ArtifactRecord,
    right_content: bytes,
) -> bool:
    """Compare immutable artifact fields while allowing backend-specific URIs/timestamps."""

    fields = (
        "tenant_id",
        "session_id",
        "artifact_id",
        "filename",
        "content_type",
        "size_bytes",
        "checksum_sha256",
        "version",
    )
    return all(getattr(left, field) == getattr(right, field) for field in fields) and (
        left_content == right_content
    )


@dataclass(frozen=True, slots=True)
class ReceiptClaim:
    acquired: bool
    receipt: ProcessingReceipt


@dataclass(frozen=True, slots=True)
class UsageSnapshot:
    tenant_id: str
    period: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass(frozen=True, slots=True)
class UsageReservation:
    tenant_id: str
    reservation_id: str
    period: str
    reserved_tokens: int
    reserved_cost_usd: float
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class UsageReservationResult:
    acquired: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class OutboxItem:
    outbox_id: str
    tenant_id: str
    kind: str
    payload: dict[str, Any]
    status: str
    attempts: int
    available_at: datetime
    owner: str | None = None
    last_error_type: str | None = None
    lease_expires_at: datetime | None = None


class Lifecycle(Protocol):
    async def initialize(self) -> None: ...

    async def close(self) -> None: ...


class ConfigRepository(Lifecycle, Protocol):
    async def save_config_version(self, version: ConfigVersion) -> None: ...

    async def activate_config(self, tenant_id: str, revision: int, activated_at: datetime) -> None: ...

    async def get_active_tenant(self, tenant_id: str) -> TenantConfig | None: ...

    async def get_config_version(self, tenant_id: str, revision: int) -> ConfigVersion | None: ...

    async def list_config_versions(self, tenant_id: str) -> Sequence[ConfigVersion]: ...

    async def get_tenant_by_binding(self, channel: str, binding_id: str) -> TenantConfig | None: ...

    async def list_active_tenants(self) -> Sequence[TenantConfig]: ...

    async def list_configured_tenants(self) -> Sequence[TenantConfig]: ...

    async def prune_operational_records(
        self,
        *,
        before: datetime,
        limit: int,
    ) -> dict[str, int]: ...


class SessionRepository(Lifecycle, Protocol):
    async def get_or_create_session(
        self,
        *,
        tenant_id: str,
        app_id: str,
        session_id: str,
        user_id: str,
        channel: str,
    ) -> SessionSnapshot: ...

    async def get_session(self, tenant_id: str, session_id: str) -> SessionSnapshot | None: ...

    async def append_event(
        self,
        *,
        snapshot: SessionSnapshot,
        event_id: str,
        kind: str,
        actor_id: str,
        payload: dict[str, Any],
        state_delta: dict[str, Any],
        trace_id: str,
    ) -> tuple[SessionSnapshot, SessionEvent]: ...

    async def list_events(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_sequence: int = 0,
    ) -> Sequence[SessionEvent]: ...

    async def get_event(self, tenant_id: str, session_id: str, event_id: str) -> SessionEvent | None: ...

    def iter_sessions(self, tenant_id: str) -> AsyncIterator[SessionSnapshot]: ...


class SummaryRepository(Lifecycle, Protocol):
    async def put_summary(self, summary: SummaryRecord) -> None: ...

    async def get_summary(self, tenant_id: str, session_id: str) -> SummaryRecord | None: ...

    def iter_summaries(self, tenant_id: str) -> AsyncIterator[SummaryRecord]: ...


class MemoryRepository(Lifecycle, Protocol):
    async def put_memory(self, memory: MemoryRecord) -> None: ...

    async def search_memory(
        self, tenant_id: str, user_id: str, query: str, *, limit: int = 10
    ) -> Sequence[MemoryRecord]: ...

    def iter_memories(self, tenant_id: str) -> AsyncIterator[MemoryRecord]: ...


class ArtifactRepository(Lifecycle, Protocol):
    async def put_artifact(self, record: ArtifactRecord, content: bytes) -> None: ...

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactRecord, bytes] | None: ...

    def iter_artifacts(self, tenant_id: str) -> AsyncIterator[ArtifactRecord]: ...


class KnowledgeRepository(Lifecycle, Protocol):
    async def put_knowledge(self, record: KnowledgeRecord) -> None: ...

    async def search_knowledge(
        self,
        tenant_id: str,
        query_embedding: Sequence[float],
        *,
        limit: int = 10,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Sequence[KnowledgeRecord]: ...

    def iter_knowledge(self, tenant_id: str) -> AsyncIterator[KnowledgeRecord]: ...


class AuditRepository(Lifecycle, Protocol):
    async def append_audit(self, record: AuditRecord) -> None: ...

    async def query_audit(
        self,
        tenant_id: str,
        *,
        limit: int = 100,
        before: datetime | None = None,
        oldest_first: bool = False,
    ) -> Sequence[AuditRecord]: ...

    async def prune_audit(self, tenant_id: str, *, before: datetime, limit: int = 500) -> int: ...

    async def delete_audit_ids(self, tenant_id: str, *, audit_ids: Sequence[str]) -> int: ...


class ReceiptRepository(Lifecycle, Protocol):
    async def claim_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        lease_expires_at: datetime,
    ) -> ReceiptClaim: ...

    async def complete_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        response: Sequence[OutboundMessage],
    ) -> None: ...

    async def complete_receipt_with_outbox(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        response: Sequence[OutboundMessage],
        items: Sequence[OutboxItem],
        usage_period: str | None = None,
        usage_delta: UsageDelta | None = None,
        usage_reservation_id: str | None = None,
    ) -> None: ...

    async def fail_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        error_type: str,
        usage_reservation_id: str | None = None,
    ) -> None: ...


class UsageRepository(Lifecycle, Protocol):
    async def get_usage(self, tenant_id: str, period: str) -> UsageSnapshot: ...

    async def add_usage(self, tenant_id: str, period: str, delta: UsageDelta) -> UsageSnapshot: ...

    async def reserve_usage(
        self,
        *,
        tenant_id: str,
        reservation_id: str,
        period: str,
        reserved_tokens: int,
        reserved_cost_usd: float,
        token_limit: int,
        cost_limit_usd: float,
        expires_at: datetime,
    ) -> UsageReservationResult: ...

    async def release_usage_reservation(self, tenant_id: str, reservation_id: str) -> None: ...


class ConcurrencyRepository(Lifecycle, Protocol):
    async def acquire_tenant_slot(
        self,
        *,
        tenant_id: str,
        owner: str,
        limit: int,
        lease_expires_at: datetime,
    ) -> bool: ...

    async def release_tenant_slot(self, *, tenant_id: str, owner: str) -> None: ...


class OutboxRepository(Lifecycle, Protocol):
    async def enqueue_outbox(self, item: OutboxItem) -> None: ...

    async def claim_outbox(
        self, owner: str, *, limit: int, now: datetime, kinds: tuple[str, ...] | None = None
    ) -> Sequence[OutboxItem]: ...

    async def complete_outbox(self, outbox_id: str, owner: str) -> None: ...

    async def checkpoint_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        payload: dict[str, Any],
    ) -> None: ...

    async def retry_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        error_type: str,
        available_at: datetime,
        terminal: bool,
    ) -> None: ...

    async def list_dead_outbox(self, tenant_id: str, *, limit: int) -> Sequence[OutboxItem]: ...

    async def requeue_dead_outbox(self, tenant_id: str, outbox_id: str) -> OutboxItem: ...


class LeaseProvider(Lifecycle, Protocol):
    def acquire_session(
        self,
        *,
        tenant_id: str,
        session_id: str,
        owner: str,
        wait_timeout: float,
        lease_seconds: float,
    ) -> AbstractAsyncContextManager[None]: ...


@dataclass(frozen=True, slots=True)
class TenantDataPlane:
    sessions: SessionRepository
    memories: MemoryRepository
    summaries: SummaryRepository
    artifacts: ArtifactRepository
    knowledge: KnowledgeRepository
    audit: AuditRepository
    receipts: ReceiptRepository
    usage: UsageRepository
    concurrency: ConcurrencyRepository
    outbox: OutboxRepository
    leases: LeaseProvider
