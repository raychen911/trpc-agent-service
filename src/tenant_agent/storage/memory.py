"""Correct single-process backend used for tests and the minimal deployment."""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from tenant_agent.models import (
    ArtifactRecord,
    AuditRecord,
    ConfigVersion,
    KnowledgeRecord,
    MemoryRecord,
    OutboundMessage,
    ProcessingReceipt,
    ReceiptStatus,
    SessionEvent,
    SessionSnapshot,
    SummaryRecord,
    TenantConfig,
    UsageDelta,
)
from tenant_agent.storage.base import (
    ConcurrentWriteError,
    OutboxItem,
    ReceiptClaim,
    SessionLeaseTimeout,
    UsageReservation,
    UsageReservationResult,
    UsageSnapshot,
    lexical_terms,
    same_artifact_payload,
)


@dataclass(slots=True)
class _SessionLockEntry:
    lock: asyncio.Lock
    users: int = 0


class InMemoryPlane:
    """Implements every storage port; never use it across multiple nodes."""

    backend_name = "inmemory"

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._session_locks: dict[tuple[str, str], _SessionLockEntry] = {}
        self._config_versions: dict[tuple[str, int], ConfigVersion] = {}
        self._active_revision: dict[str, int] = {}
        self._bindings: dict[tuple[str, str], str] = {}
        self._sessions: dict[tuple[str, str], SessionSnapshot] = {}
        self._events: defaultdict[tuple[str, str], list[SessionEvent]] = defaultdict(list)
        self._summaries: dict[tuple[str, str], SummaryRecord] = {}
        self._memories: dict[tuple[str, str, str], MemoryRecord] = {}
        self._artifacts: dict[tuple[str, str], tuple[ArtifactRecord, bytes]] = {}
        self._knowledge: dict[tuple[str, str, str], KnowledgeRecord] = {}
        self._audit: defaultdict[str, list[AuditRecord]] = defaultdict(list)
        self._receipts: dict[tuple[str, str], ProcessingReceipt] = {}
        self._usage: dict[tuple[str, str], UsageSnapshot] = {}
        self._usage_reservations: dict[tuple[str, str], UsageReservation] = {}
        self._tenant_slots: dict[tuple[str, str], datetime] = {}
        self._outbox: dict[str, OutboxItem] = {}
        self._outbox_completed_at: dict[str, datetime] = {}

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def healthcheck(self) -> bool:
        return True

    async def save_config_version(self, version: ConfigVersion) -> None:
        async with self._guard:
            key = (version.tenant_id, version.revision)
            existing = self._config_versions.get(key)
            if existing and existing.checksum_sha256 != version.checksum_sha256:
                raise ConcurrentWriteError("an immutable configuration revision already exists")
            self._config_versions[key] = version

    async def activate_config(self, tenant_id: str, revision: int, activated_at: datetime) -> None:
        async with self._guard:
            key = (tenant_id, revision)
            requested = self._config_versions.get(key)
            if requested is None:
                raise KeyError(f"unknown configuration revision {tenant_id}/{revision}")
            for binding in requested.config.channels:
                if not binding.enabled:
                    continue
                owner = self._bindings.get((binding.channel.value, binding.binding_id))
                if owner and owner != tenant_id:
                    raise ConcurrentWriteError("channel binding is already assigned to another tenant")
            previous_revision = self._active_revision.get(tenant_id)
            if previous_revision is not None and previous_revision != revision:
                previous = self._config_versions[(tenant_id, previous_revision)]
                self._config_versions[(tenant_id, previous_revision)] = previous.model_copy(
                    update={"status": "superseded"}
                )
            active = requested.model_copy(update={"status": "active", "activated_at": activated_at})
            self._config_versions[key] = active
            self._active_revision[tenant_id] = revision
            for binding_key, binding_tenant in tuple(self._bindings.items()):
                if binding_tenant == tenant_id:
                    self._bindings.pop(binding_key, None)
            for binding in active.config.channels:
                if binding.enabled:
                    binding_key = (binding.channel.value, binding.binding_id)
                    self._bindings[binding_key] = tenant_id

    async def get_active_tenant(self, tenant_id: str) -> TenantConfig | None:
        revision = self._active_revision.get(tenant_id)
        if revision is None:
            return None
        config = self._config_versions[(tenant_id, revision)].config
        return config if config.status.value == "active" else None

    async def get_config_version(self, tenant_id: str, revision: int) -> ConfigVersion | None:
        return self._config_versions.get((tenant_id, revision))

    async def list_config_versions(self, tenant_id: str) -> Sequence[ConfigVersion]:
        versions = [value for (owner, _), value in self._config_versions.items() if owner == tenant_id]
        return sorted(versions, key=lambda item: item.revision, reverse=True)

    async def get_tenant_by_binding(self, channel: str, binding_id: str) -> TenantConfig | None:
        tenant_id = self._bindings.get((channel, binding_id))
        return await self.get_active_tenant(tenant_id) if tenant_id else None

    async def list_active_tenants(self) -> Sequence[TenantConfig]:
        tenants = [await self.get_active_tenant(tenant_id) for tenant_id in self._active_revision]
        return [tenant for tenant in tenants if tenant is not None and tenant.status.value == "active"]

    async def list_configured_tenants(self) -> Sequence[TenantConfig]:
        return [
            self._config_versions[(tenant_id, revision)].config
            for tenant_id, revision in sorted(self._active_revision.items())
        ]

    async def prune_operational_records(
        self,
        *,
        before: datetime,
        limit: int,
    ) -> dict[str, int]:
        async with self._guard:
            receipt_keys = [
                key
                for key, receipt in sorted(
                    self._receipts.items(),
                    key=lambda item: item[1].updated_at,
                )
                if receipt.status is not ReceiptStatus.PROCESSING and receipt.updated_at < before
            ][:limit]
            for key in receipt_keys:
                self._receipts.pop(key, None)
            outbox_ids = [
                outbox_id
                for outbox_id, completed_at in sorted(
                    self._outbox_completed_at.items(),
                    key=lambda item: item[1],
                )
                if completed_at < before
            ][:limit]
            for outbox_id in outbox_ids:
                self._outbox.pop(outbox_id, None)
                self._outbox_completed_at.pop(outbox_id, None)
            reservation_keys = [
                key
                for key, reservation in self._usage_reservations.items()
                if reservation.expires_at < before
            ][:limit]
            for key in reservation_keys:
                self._usage_reservations.pop(key, None)
            return {
                "receipts": len(receipt_keys),
                "outbox": len(outbox_ids),
                "usage_reservations": len(reservation_keys),
            }

    async def get_or_create_session(
        self,
        *,
        tenant_id: str,
        app_id: str,
        session_id: str,
        user_id: str,
        channel: str,
    ) -> SessionSnapshot:
        key = (tenant_id, session_id)
        async with self._guard:
            existing = self._sessions.get(key)
            if existing:
                if existing.app_id != app_id or existing.user_id != user_id:
                    raise ConcurrentWriteError("session identity is immutable")
                return existing
            snapshot = SessionSnapshot(
                tenant_id=tenant_id,
                app_id=app_id,
                session_id=session_id,
                user_id=user_id,
                channel=channel,
            )
            self._sessions[key] = snapshot
            return snapshot

    async def get_session(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        return self._sessions.get((tenant_id, session_id))

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
    ) -> tuple[SessionSnapshot, SessionEvent]:
        key = (snapshot.tenant_id, snapshot.session_id)
        async with self._guard:
            current = self._sessions.get(key)
            if current is None:
                raise KeyError("session does not exist")
            if current.revision != snapshot.revision:
                raise ConcurrentWriteError(
                    f"expected session revision {snapshot.revision}, found {current.revision}"
                )
            if any(event.event_id == event_id for event in self._events[key]):
                event = next(event for event in self._events[key] if event.event_id == event_id)
                return current, event
            sequence = current.last_event_sequence + 1
            event = SessionEvent(
                event_id=event_id,
                tenant_id=current.tenant_id,
                session_id=current.session_id,
                sequence=sequence,
                kind=kind,
                actor_id=actor_id,
                payload=payload,
                state_delta=state_delta,
                trace_id=trace_id,
            )
            state = dict(current.state)
            state.update(state_delta)
            updated = current.model_copy(
                update={
                    "state": state,
                    "revision": current.revision + 1,
                    "last_event_sequence": sequence,
                    "updated_at": datetime.now(UTC),
                }
            )
            self._events[key].append(event)
            self._sessions[key] = updated
            return updated, event

    async def list_events(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_sequence: int = 0,
    ) -> Sequence[SessionEvent]:
        return tuple(
            event for event in self._events[(tenant_id, session_id)] if event.sequence > after_sequence
        )

    async def get_event(self, tenant_id: str, session_id: str, event_id: str) -> SessionEvent | None:
        return next(
            (event for event in self._events[(tenant_id, session_id)] if event.event_id == event_id),
            None,
        )

    async def iter_sessions(self, tenant_id: str) -> AsyncIterator[SessionSnapshot]:
        for (owner, _), session in tuple(self._sessions.items()):
            if owner == tenant_id:
                yield session

    async def put_summary(self, summary: SummaryRecord) -> None:
        key = (summary.tenant_id, summary.session_id)
        async with self._guard:
            session = self._sessions.get(key)
            if session is not None and summary.through_event_sequence > session.last_event_sequence:
                raise ConcurrentWriteError("summary cannot cover events that have not committed")
            current = self._summaries.get(key)
            if current and current.version > summary.version:
                return
            if current and current.version == summary.version:
                if (
                    current.through_event_sequence != summary.through_event_sequence
                    or current.content != summary.content
                ):
                    raise ConcurrentWriteError("summary version is immutable")
                return
            self._summaries[key] = summary
            if session is not None:
                self._sessions[key] = session.model_copy(
                    update={"summary_version": max(session.summary_version, summary.version)}
                )

    async def get_summary(self, tenant_id: str, session_id: str) -> SummaryRecord | None:
        return self._summaries.get((tenant_id, session_id))

    async def iter_summaries(self, tenant_id: str) -> AsyncIterator[SummaryRecord]:
        for (owner, _), summary in tuple(self._summaries.items()):
            if owner == tenant_id:
                yield summary

    async def put_memory(self, memory: MemoryRecord) -> None:
        key = (memory.tenant_id, memory.user_id, memory.memory_id)
        async with self._guard:
            current = self._memories.get(key)
            if current and current.revision > memory.revision:
                return
            if current and current.revision == memory.revision:
                if current.content != memory.content or current.metadata != memory.metadata:
                    raise ConcurrentWriteError("memory revision is immutable")
                return
            self._memories[key] = memory

    async def search_memory(
        self, tenant_id: str, user_id: str, query: str, *, limit: int = 10
    ) -> Sequence[MemoryRecord]:
        query_terms = lexical_terms(query)
        candidates: list[tuple[int, MemoryRecord]] = []
        for (owner, subject, _), memory in self._memories.items():
            if owner == tenant_id and subject == user_id:
                score = len(query_terms & lexical_terms(memory.content))
                if query_terms and score == 0:
                    continue
                candidates.append((score, memory))
        candidates.sort(key=lambda item: (item[0], item[1].updated_at), reverse=True)
        return tuple(memory for _, memory in candidates[:limit])

    async def iter_memories(self, tenant_id: str) -> AsyncIterator[MemoryRecord]:
        for (owner, _, _), memory in tuple(self._memories.items()):
            if owner == tenant_id:
                yield memory

    async def put_artifact(self, record: ArtifactRecord, content: bytes) -> None:
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("artifact size or checksum does not match its content")
        async with self._guard:
            key = (record.tenant_id, record.artifact_id)
            current = self._artifacts.get(key)
            if current and current[0].version > record.version:
                return
            if current and current[0].version == record.version:
                if not same_artifact_payload(current[0], current[1], record, content):
                    raise ConcurrentWriteError("artifact version is immutable")
                return
            self._artifacts[key] = (record, bytes(content))

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactRecord, bytes] | None:
        return self._artifacts.get((tenant_id, artifact_id))

    async def iter_artifacts(self, tenant_id: str) -> AsyncIterator[ArtifactRecord]:
        for (owner, _), (record, _) in tuple(self._artifacts.items()):
            if owner == tenant_id:
                yield record

    async def put_knowledge(self, record: KnowledgeRecord) -> None:
        async with self._guard:
            self._knowledge[(record.tenant_id, record.document_id, record.chunk_id)] = record

    async def search_knowledge(
        self,
        tenant_id: str,
        query_embedding: Sequence[float],
        *,
        limit: int = 10,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Sequence[KnowledgeRecord]:
        candidates: list[tuple[float, KnowledgeRecord]] = []
        for (owner, _, _), record in self._knowledge.items():
            if owner != tenant_id or not record.embedding:
                continue
            if metadata_filter and any(record.metadata.get(k) != v for k, v in metadata_filter.items()):
                continue
            score = _cosine(query_embedding, record.embedding)
            candidates.append((score, record))
        candidates.sort(key=lambda item: item[0], reverse=True)
        return tuple(record for _, record in candidates[:limit])

    async def iter_knowledge(self, tenant_id: str) -> AsyncIterator[KnowledgeRecord]:
        for (owner, _, _), record in tuple(self._knowledge.items()):
            if owner == tenant_id:
                yield record

    async def append_audit(self, record: AuditRecord) -> None:
        async with self._guard:
            self._audit[record.tenant_id].append(record)

    async def query_audit(
        self,
        tenant_id: str,
        *,
        limit: int = 100,
        before: datetime | None = None,
        oldest_first: bool = False,
    ) -> Sequence[AuditRecord]:
        rows = self._audit[tenant_id]
        if before:
            rows = [row for row in rows if row.occurred_at < before]
        return tuple(
            sorted(
                rows,
                key=lambda row: (row.occurred_at, row.audit_id),
                reverse=not oldest_first,
            )[:limit]
        )

    async def prune_audit(self, tenant_id: str, *, before: datetime, limit: int = 500) -> int:
        async with self._guard:
            rows = self._audit[tenant_id]
            selected_ids = [
                row.audit_id
                for row in sorted(rows, key=lambda row: (row.occurred_at, row.audit_id))
                if row.occurred_at < before
            ][:limit]
            selected = set(selected_ids)
            retained = [row for row in rows if row.audit_id not in selected]
            deleted = len(rows) - len(retained)
            self._audit[tenant_id] = retained
            return deleted

    async def delete_audit_ids(self, tenant_id: str, *, audit_ids: Sequence[str]) -> int:
        selected = set(audit_ids)
        async with self._guard:
            rows = self._audit[tenant_id]
            retained = [row for row in rows if row.audit_id not in selected]
            deleted = len(rows) - len(retained)
            self._audit[tenant_id] = retained
            return deleted

    async def claim_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        lease_expires_at: datetime,
    ) -> ReceiptClaim:
        now = datetime.now(UTC)
        async with self._guard:
            key = (tenant_id, dedupe_key)
            existing = self._receipts.get(key)
            can_reclaim = (
                existing is None
                or existing.status is ReceiptStatus.FAILED
                or (existing.status is ReceiptStatus.PROCESSING and existing.lease_expires_at <= now)
            )
            if can_reclaim:
                receipt = ProcessingReceipt(
                    tenant_id=tenant_id,
                    dedupe_key=dedupe_key,
                    status=ReceiptStatus.PROCESSING,
                    owner=owner,
                    lease_expires_at=lease_expires_at,
                )
                self._receipts[key] = receipt
                return ReceiptClaim(True, receipt)
            assert existing is not None
            return ReceiptClaim(False, existing)

    async def complete_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        response: Sequence[OutboundMessage],
    ) -> None:
        if any(item.tenant_id != tenant_id for item in response):
            raise ValueError("receipt response tenant scope mismatch")
        async with self._guard:
            key = (tenant_id, dedupe_key)
            current = self._receipts[key]
            if current.owner != owner or current.status is not ReceiptStatus.PROCESSING:
                raise ConcurrentWriteError("receipt is not owned by this worker")
            self._receipts[key] = current.model_copy(
                update={
                    "status": ReceiptStatus.COMPLETED,
                    "response": tuple(response),
                    "updated_at": datetime.now(UTC),
                }
            )

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
    ) -> None:
        if any(item.tenant_id != tenant_id for item in response) or any(
            item.tenant_id != tenant_id for item in items
        ):
            raise ValueError("receipt/outbox tenant scope mismatch")
        async with self._guard:
            key = (tenant_id, dedupe_key)
            current = self._receipts[key]
            if current.owner != owner or current.status is not ReceiptStatus.PROCESSING:
                raise ConcurrentWriteError("receipt is not owned by this worker")
            for item in items:
                self._outbox.setdefault(item.outbox_id, item)
            if usage_period is not None and usage_delta is not None:
                usage_key = (tenant_id, usage_period)
                usage = self._usage.get(usage_key, UsageSnapshot(tenant_id, usage_period))
                self._usage[usage_key] = replace(
                    usage,
                    input_tokens=usage.input_tokens + usage_delta.input_tokens,
                    output_tokens=usage.output_tokens + usage_delta.output_tokens,
                    cost_usd=usage.cost_usd + usage_delta.cost_usd,
                )
            if usage_reservation_id is not None:
                self._usage_reservations.pop((tenant_id, usage_reservation_id), None)
            self._receipts[key] = current.model_copy(
                update={
                    "status": ReceiptStatus.COMPLETED,
                    "response": tuple(response),
                    "updated_at": datetime.now(UTC),
                }
            )

    async def fail_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        error_type: str,
        usage_reservation_id: str | None = None,
    ) -> None:
        async with self._guard:
            key = (tenant_id, dedupe_key)
            current = self._receipts[key]
            if current.owner != owner or current.status is not ReceiptStatus.PROCESSING:
                raise ConcurrentWriteError("receipt is not owned by this worker")
            self._receipts[key] = current.model_copy(
                update={
                    "status": ReceiptStatus.FAILED,
                    "error_type": error_type,
                    "updated_at": datetime.now(UTC),
                }
            )
            if usage_reservation_id is not None:
                self._usage_reservations.pop((tenant_id, usage_reservation_id), None)

    async def get_usage(self, tenant_id: str, period: str) -> UsageSnapshot:
        return self._usage.get((tenant_id, period), UsageSnapshot(tenant_id, period))

    async def add_usage(self, tenant_id: str, period: str, delta: UsageDelta) -> UsageSnapshot:
        async with self._guard:
            current = self._usage.get((tenant_id, period), UsageSnapshot(tenant_id, period))
            updated = replace(
                current,
                input_tokens=current.input_tokens + delta.input_tokens,
                output_tokens=current.output_tokens + delta.output_tokens,
                cost_usd=current.cost_usd + delta.cost_usd,
            )
            self._usage[(tenant_id, period)] = updated
            return updated

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
    ) -> UsageReservationResult:
        if reserved_tokens < 0 or reserved_cost_usd < 0:
            raise ValueError("usage reservations cannot be negative")
        now = datetime.now(UTC)
        async with self._guard:
            for key, reservation in tuple(self._usage_reservations.items()):
                if reservation.expires_at <= now:
                    self._usage_reservations.pop(key, None)
            key = (tenant_id, reservation_id)
            existing = self._usage_reservations.get(key)
            if existing is not None:
                if (
                    existing.period != period
                    or existing.reserved_tokens != reserved_tokens
                    or not math.isclose(existing.reserved_cost_usd, reserved_cost_usd)
                ):
                    raise ConcurrentWriteError("usage reservation identity is immutable")
                return UsageReservationResult(True)
            usage = self._usage.get((tenant_id, period), UsageSnapshot(tenant_id, period))
            active = [
                reservation
                for reservation in self._usage_reservations.values()
                if reservation.tenant_id == tenant_id and reservation.period == period
            ]
            total_tokens = usage.total_tokens + sum(item.reserved_tokens for item in active)
            if total_tokens + reserved_tokens > token_limit:
                return UsageReservationResult(False, "monthly_token_budget")
            total_cost = usage.cost_usd + sum(item.reserved_cost_usd for item in active)
            if total_cost + reserved_cost_usd > cost_limit_usd:
                return UsageReservationResult(False, "monthly_cost_budget")
            self._usage_reservations[key] = UsageReservation(
                tenant_id=tenant_id,
                reservation_id=reservation_id,
                period=period,
                reserved_tokens=reserved_tokens,
                reserved_cost_usd=reserved_cost_usd,
                expires_at=expires_at,
            )
            return UsageReservationResult(True)

    async def release_usage_reservation(self, tenant_id: str, reservation_id: str) -> None:
        async with self._guard:
            self._usage_reservations.pop((tenant_id, reservation_id), None)

    async def acquire_tenant_slot(
        self,
        *,
        tenant_id: str,
        owner: str,
        limit: int,
        lease_expires_at: datetime,
    ) -> bool:
        now = datetime.now(UTC)
        async with self._guard:
            for key, expiry in tuple(self._tenant_slots.items()):
                if key[0] == tenant_id and expiry <= now:
                    self._tenant_slots.pop(key, None)
            key = (tenant_id, owner)
            if key in self._tenant_slots:
                self._tenant_slots[key] = lease_expires_at
                return True
            active = sum(slot_tenant == tenant_id for slot_tenant, _ in self._tenant_slots)
            if active >= limit:
                return False
            self._tenant_slots[key] = lease_expires_at
            return True

    async def release_tenant_slot(self, *, tenant_id: str, owner: str) -> None:
        async with self._guard:
            self._tenant_slots.pop((tenant_id, owner), None)

    async def enqueue_outbox(self, item: OutboxItem) -> None:
        async with self._guard:
            self._outbox.setdefault(item.outbox_id, item)

    async def claim_outbox(
        self, owner: str, *, limit: int, now: datetime, kinds: tuple[str, ...] | None = None
    ) -> Sequence[OutboxItem]:
        async with self._guard:
            candidates = sorted(
                (
                    item
                    for item in self._outbox.values()
                    if (kinds is None or item.kind in kinds)
                    and (
                        (item.status in {"pending", "retry"} and item.available_at <= now)
                        or (
                            item.status == "processing"
                            and item.lease_expires_at is not None
                            and item.lease_expires_at <= now
                        )
                    )
                ),
                key=lambda item: item.available_at,
            )[:limit]
            claimed: list[OutboxItem] = []
            for item in candidates:
                value = replace(
                    item,
                    status="processing",
                    owner=owner,
                    attempts=item.attempts + 1,
                    lease_expires_at=now + timedelta(seconds=300),
                )
                self._outbox[item.outbox_id] = value
                claimed.append(value)
            return tuple(claimed)

    async def complete_outbox(self, outbox_id: str, owner: str) -> None:
        async with self._guard:
            current = self._outbox[outbox_id]
            if current.owner != owner:
                raise ConcurrentWriteError("outbox item is not owned by this worker")
            self._outbox[outbox_id] = replace(current, status="completed", lease_expires_at=None)
            self._outbox_completed_at[outbox_id] = datetime.now(UTC)

    async def checkpoint_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        payload: dict[str, Any],
    ) -> None:
        async with self._guard:
            current = self._outbox[outbox_id]
            if current.owner != owner or current.status != "processing":
                raise ConcurrentWriteError("outbox item is not owned by this worker")
            self._outbox[outbox_id] = replace(
                current,
                payload=payload,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=300),
            )

    async def retry_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        error_type: str,
        available_at: datetime,
        terminal: bool,
    ) -> None:
        async with self._guard:
            current = self._outbox[outbox_id]
            if current.owner != owner:
                raise ConcurrentWriteError("outbox item is not owned by this worker")
            self._outbox[outbox_id] = replace(
                current,
                status="dead" if terminal else "retry",
                owner=None,
                last_error_type=error_type,
                available_at=available_at,
                lease_expires_at=None,
            )

    async def list_dead_outbox(self, tenant_id: str, *, limit: int) -> Sequence[OutboxItem]:
        async with self._guard:
            rows = sorted(
                (
                    item
                    for item in self._outbox.values()
                    if item.tenant_id == tenant_id and item.status == "dead"
                ),
                key=lambda item: (item.available_at, item.outbox_id),
                reverse=True,
            )
            return tuple(rows[:limit])

    async def requeue_dead_outbox(self, tenant_id: str, outbox_id: str) -> OutboxItem:
        async with self._guard:
            current = self._outbox.get(outbox_id)
            if current is None or current.tenant_id != tenant_id:
                raise KeyError("unknown outbox item")
            if current.status != "dead":
                raise ConcurrentWriteError("only dead outbox items can be requeued")
            requeued = replace(
                current,
                status="retry",
                attempts=0,
                available_at=datetime.now(UTC),
                owner=None,
                last_error_type=None,
                lease_expires_at=None,
            )
            self._outbox[outbox_id] = requeued
            return requeued

    @asynccontextmanager
    async def acquire_session(
        self,
        *,
        tenant_id: str,
        session_id: str,
        owner: str,
        wait_timeout: float,
        lease_seconds: float,
    ) -> AsyncIterator[None]:
        del owner, lease_seconds
        key = (tenant_id, session_id)
        async with self._guard:
            entry = self._session_locks.get(key)
            if entry is None:
                entry = _SessionLockEntry(asyncio.Lock())
                self._session_locks[key] = entry
            entry.users += 1
        try:
            await asyncio.wait_for(entry.lock.acquire(), timeout=wait_timeout)
        except TimeoutError as exc:
            async with self._guard:
                entry.users -= 1
                if entry.users == 0 and self._session_locks.get(key) is entry:
                    self._session_locks.pop(key, None)
            raise SessionLeaseTimeout("timed out waiting for the session lease") from exc
        try:
            yield
        finally:
            entry.lock.release()
            async with self._guard:
                entry.users -= 1
                if entry.users == 0 and self._session_locks.get(key) is entry:
                    self._session_locks.pop(key, None)


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        return float("-inf")
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)
