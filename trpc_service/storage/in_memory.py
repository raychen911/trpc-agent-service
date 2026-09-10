"""Thread-safe in-memory storage backend for local demos and tests."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from threading import RLock
from time import monotonic, sleep
from uuid import uuid4

from trpc_service.storage.base import (
    AuditRecord,
    IdempotencyRecord,
    IdempotencyStatus,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
    now_utc,
)
from trpc_service.storage.compensation import InMemoryCompensationStore
from trpc_service.storage.durable import InMemoryInboxOutbox
from trpc_service.storage.locking import SessionLease, SessionLeaseLost, SessionLockTimeout
from trpc_service.storage.mailbox import InMemoryMailboxStore
from trpc_service.storage.session_mailbox import InMemorySessionMailboxStore
from trpc_service.storage.tool_governance import InMemoryToolGovernanceStore


class InMemorySessionStore:
    def __init__(self) -> None:
        self._events: dict[tuple[str, str], list[SessionEvent]] = {}
        self._states: dict[tuple[str, str], SessionState] = {}
        self._leases: dict[tuple[str, str], SessionLease] = {}
        self._fencing_tokens: dict[tuple[str, str], int] = {}
        self._lock = RLock()

    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int:
        with self._lock:
            self._assert_fencing(event.tenant_id, event.session_id, fencing_token)
            key = (event.tenant_id, event.session_id)
            events = self._events.setdefault(key, [])
            if event.idempotency_key:
                for existing in events:
                    if existing.idempotency_key == event.idempotency_key and existing.event_type == event.event_type:
                        return existing.seq
            snapshot = deepcopy(event)
            snapshot.seq = len(events) + 1
            if not snapshot.event_id:
                snapshot.event_id = str(uuid4())
            events.append(snapshot)
            state = self._states.setdefault(key, SessionState(tenant_id=event.tenant_id, session_id=event.session_id))
            state.latest_event_seq = snapshot.seq
            return snapshot.seq

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0) -> list[SessionEvent]:
        with self._lock:
            events = self._events.get((tenant_id, session_id), [])
            return [deepcopy(event) for event in events if event.seq > after_seq]

    def load_state(self, tenant_id: str, session_id: str) -> SessionState:
        with self._lock:
            return deepcopy(
                self._states.get(
                    (tenant_id, session_id),
                    SessionState(tenant_id=tenant_id, session_id=session_id),
                )
            )

    def compare_and_set_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict,
        fencing_token: int | None = None,
    ) -> bool:
        with self._lock:
            self._assert_fencing(tenant_id, session_id, fencing_token)
            key = (tenant_id, session_id)
            current = self._states.setdefault(key, SessionState(tenant_id=tenant_id, session_id=session_id))
            if current.state_version != expected_version:
                return False
            current.state = deepcopy(state)
            current.state_version += 1
            return True

    def _assert_fencing(self, tenant_id: str, session_id: str, fencing_token: int | None) -> None:
        if not fencing_token:
            return
        current = self._leases.get((tenant_id, session_id))
        if (
            current is None
            or current.fencing_token != int(fencing_token)
            or current.expires_at <= datetime.now(UTC)
        ):
            raise SessionLeaseLost(f"session fencing token rejected: {tenant_id}/{session_id}")

    def restore_state(
        self,
        tenant_id: str,
        session_id: str,
        state: dict,
        state_version: int,
    ) -> None:
        with self._lock:
            key = (tenant_id, session_id)
            current = self._states.setdefault(key, SessionState(tenant_id=tenant_id, session_id=session_id))
            current.state = deepcopy(state)
            current.state_version = int(state_version)

    def acquire_session_lease(self, tenant_id: str, session_id: str, timeout: float) -> SessionLease:
        key = (tenant_id, session_id)
        owner = str(uuid4())
        deadline = monotonic() + max(0.0, timeout)
        ttl = max(1.0, float(__import__("os").getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        while True:
            with self._lock:
                now = datetime.now(UTC)
                current = self._leases.get(key)
                if current is None or current.expires_at <= now:
                    fence = self._fencing_tokens.get(key, 0) + 1
                    self._fencing_tokens[key] = fence
                    lease = SessionLease(
                        tenant_id,
                        session_id,
                        owner,
                        fence,
                        now + timedelta(seconds=ttl),
                    )
                    self._leases[key] = lease
                    return lease
            if monotonic() >= deadline:
                raise SessionLockTimeout(f"session lease timeout: {tenant_id}/{session_id}")
            sleep(0.01)

    def release_session_lease(self, tenant_id: str, session_id: str, lease: SessionLease) -> None:
        with self._lock:
            current = self._leases.get((tenant_id, session_id))
            if current and current.owner == lease.owner and current.fencing_token == lease.fencing_token:
                self._leases.pop((tenant_id, session_id), None)

    def renew_session_lease(self, lease: SessionLease) -> SessionLease:
        ttl = max(1.0, float(__import__("os").getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        with self._lock:
            current = self._leases.get((lease.tenant_id, lease.session_id))
            if current is None or current.owner != lease.owner or current.fencing_token != lease.fencing_token:
                raise SessionLeaseLost(f"session lease lost: {lease.tenant_id}/{lease.session_id}")
            renewed = SessionLease(
                lease.tenant_id,
                lease.session_id,
                lease.owner,
                lease.fencing_token,
                datetime.now(UTC) + timedelta(seconds=ttl),
            )
            self._leases[(lease.tenant_id, lease.session_id)] = renewed
            return renewed

    def validate_session_lease(self, lease: SessionLease) -> None:
        with self._lock:
            current = self._leases.get((lease.tenant_id, lease.session_id))
            if (
                current is None
                or current.owner != lease.owner
                or current.fencing_token != lease.fencing_token
                or current.expires_at <= datetime.now(UTC)
            ):
                raise SessionLeaseLost(f"session fencing token rejected: {lease.tenant_id}/{lease.session_id}")


class InMemoryMemoryStore:
    def __init__(self) -> None:
        self._items: dict[str, MemoryItem] = {}
        self._lock = RLock()

    def put(self, item: MemoryItem) -> None:
        with self._lock:
            self._items[f"{item.tenant_id}:{item.memory_id}"] = deepcopy(item)

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]:
        words = {word.lower() for word in query.split() if word}
        with self._lock:
            candidates = [
                item
                for item in self._items.values()
                if item.tenant_id == tenant_id and (scope_keys is None or item.scope_key in set(scope_keys))
            ]
        scored: list[tuple[int, MemoryItem]] = []
        for item in candidates:
            content = item.content.lower()
            score = sum(1 for word in words if word in content)
            if score or not words:
                scored.append((score, item))
        scored.sort(key=lambda pair: (-pair[0], pair[1].created_at))
        return [deepcopy(item) for _, item in scored[:limit]]


class InMemorySummaryStore:
    def __init__(self) -> None:
        self._summaries: dict[tuple[str, str], Summary] = {}
        self._lock = RLock()

    def put(self, summary: Summary) -> None:
        with self._lock:
            key = (summary.tenant_id, summary.session_id)
            current = self._summaries.get(key)
            if current and current.source_event_seq > summary.source_event_seq:
                return
            next_version = 1 if current is None else current.summary_version + 1
            snapshot = deepcopy(summary)
            snapshot.summary_version = next_version
            self._summaries[key] = snapshot

    def latest(self, tenant_id: str, session_id: str) -> Summary | None:
        with self._lock:
            summary = self._summaries.get((tenant_id, session_id))
            return deepcopy(summary) if summary else None


class InMemoryAuditLogStore:
    def __init__(self) -> None:
        self._records: list[AuditRecord] = []
        self._lock = RLock()

    def append(self, record: AuditRecord) -> None:
        with self._lock:
            self._records.append(deepcopy(record))

    def list_by_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditRecord]:
        with self._lock:
            records = [deepcopy(record) for record in self._records if record.tenant_id == tenant_id]
        return records[-limit:]


class InMemoryIdempotencyStore:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], IdempotencyRecord] = {}
        self._lock = RLock()

    def start(
        self,
        tenant_id: str,
        key: str,
        trace_id: str,
        lease_seconds: int = 180,
    ) -> IdempotencyRecord:
        with self._lock:
            record_key = (tenant_id, key)
            current = self._records.get(record_key)
            if current:
                if current.status == IdempotencyStatus.FAILED or (
                    current.status == IdempotencyStatus.PROCESSING
                    and (now_utc() - current.updated_at).total_seconds() >= lease_seconds
                ):
                    current.trace_id = trace_id
                    current.status = IdempotencyStatus.PROCESSING
                    current.attempt += 1
                    current.updated_at = now_utc()
                return deepcopy(current)
            record = IdempotencyRecord(
                tenant_id=tenant_id,
                key=key,
                status=IdempotencyStatus.PROCESSING,
                trace_id=trace_id,
            )
            self._records[record_key] = record
            return deepcopy(record)

    def complete(self, tenant_id: str, key: str, response_ref: str, result: dict) -> IdempotencyRecord:
        with self._lock:
            record = self._records[(tenant_id, key)]
            record.status = IdempotencyStatus.COMPLETED
            record.response_ref = response_ref
            record.result = deepcopy(result)
            record.updated_at = now_utc()
            return deepcopy(record)

    def fail(self, tenant_id: str, key: str, error_type: str) -> IdempotencyRecord:
        with self._lock:
            record = self._records[(tenant_id, key)]
            record.status = IdempotencyStatus.FAILED
            record.result = {"error_type": error_type}
            record.updated_at = now_utc()
            return deepcopy(record)

    def get(self, tenant_id: str, key: str) -> IdempotencyRecord | None:
        with self._lock:
            record = self._records.get((tenant_id, key))
            return deepcopy(record) if record else None

    def claim_delivery(self, tenant_id: str, key: str) -> bool:
        with self._lock:
            record = self._records.get((tenant_id, key))
            if not record or record.status != IdempotencyStatus.COMPLETED:
                return False
            result = dict(record.result or {})
            if result.get("delivered") or result.get("delivery_claimed"):
                return False
            result["delivery_claimed"] = True
            record.result = result
            record.updated_at = now_utc()
            return True

    def release_delivery(self, tenant_id: str, key: str) -> None:
        with self._lock:
            record = self._records.get((tenant_id, key))
            if not record:
                return
            result = dict(record.result or {})
            result.pop("delivery_claimed", None)
            record.result = result
            record.updated_at = now_utc()


class InMemoryStorage:
    backend_name = "memory"

    def __init__(self) -> None:
        self.session = InMemorySessionStore()
        self.memory = InMemoryMemoryStore()
        self.summary = InMemorySummaryStore()
        self.audit = InMemoryAuditLogStore()
        self.idempotency = InMemoryIdempotencyStore()
        self.compensation = InMemoryCompensationStore()
        self.inbox_outbox = InMemoryInboxOutbox()
        self.mailbox = InMemoryMailboxStore()
        self.session_mailbox_v2 = InMemorySessionMailboxStore()
        self.tool_governance = InMemoryToolGovernanceStore()

    def close(self) -> None:
        """Keep the storage bundle lifecycle uniform for local callers."""
