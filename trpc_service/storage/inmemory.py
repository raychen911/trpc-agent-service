import asyncio
import copy
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from trpc_service.storage.contracts import (
    AuditEntry,
    EventRecord,
    IdempotencyRecord,
    OutboxInput,
    OutboxRecord,
    RateLimitDecision,
    SessionIdentity,
    SessionSnapshot,
    SummaryRecord,
    TurnCommit,
    TurnCommitResult,
)
from trpc_service.storage.exceptions import (
    DuplicateMessageError,
    LockNotAcquiredError,
    VersionConflictError,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _session_key(identity: SessionIdentity) -> tuple[str, str, str]:
    return identity.tenant_id, identity.agent_app_id, identity.session_id


class _InMemoryLock(AbstractAsyncContextManager[str]):
    def __init__(self, lock: asyncio.Lock, key: str, timeout: float) -> None:
        self._lock = lock
        self._key = key
        self._timeout = timeout

    async def __aenter__(self) -> str:
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=self._timeout)
        except asyncio.TimeoutError as error:
            raise LockNotAcquiredError(f"timed out acquiring lock {self._key}") from error
        return self._key

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._lock.release()


class InMemoryCoordinationStore:
    """Process-local lock, idempotency, rate-limit, and TTL state backend."""

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._locks: dict[str, asyncio.Lock] = {}
        self._idempotency: dict[str, tuple[float, IdempotencyRecord]] = {}
        self._state: dict[str, tuple[float, Mapping[str, Any]]] = {}
        self._rate_limits: dict[str, deque[float]] = defaultdict(deque)

    def acquire(
        self, key: str, *, ttl_seconds: float = 30, wait_timeout_seconds: float = 5
    ) -> AbstractAsyncContextManager[str]:
        del ttl_seconds  # A process-local lock is released by its context manager.
        lock = self._locks.setdefault(key, asyncio.Lock())
        return _InMemoryLock(lock, key, wait_timeout_seconds)

    async def claim(self, key: str, *, ttl_seconds: int = 86_400) -> bool:
        async with self._guard:
            self._purge_expired(key)
            if key in self._idempotency:
                return False
            self._idempotency[key] = (
                time.monotonic() + ttl_seconds,
                IdempotencyRecord(key=key, status="processing", result=None),
            )
            return True

    async def complete(
        self, key: str, result: Mapping[str, Any], *, ttl_seconds: int = 86_400
    ) -> None:
        async with self._guard:
            self._idempotency[key] = (
                time.monotonic() + ttl_seconds,
                IdempotencyRecord(
                    key=key,
                    status="completed",
                    result=copy.deepcopy(dict(result)),
                ),
            )

    async def get(self, key: str) -> IdempotencyRecord | None:
        async with self._guard:
            self._purge_expired(key)
            item = self._idempotency.get(key)
            return copy.deepcopy(item[1]) if item else None

    async def abandon(self, key: str) -> None:
        async with self._guard:
            item = self._idempotency.get(key)
            if item and item[1].status == "processing":
                self._idempotency.pop(key, None)

    def _purge_expired(self, key: str) -> None:
        item = self._idempotency.get(key)
        if item and item[0] <= time.monotonic():
            self._idempotency.pop(key, None)

    async def check(self, key: str, *, limit: int, window_seconds: int) -> RateLimitDecision:
        if limit < 1 or window_seconds < 1:
            raise ValueError("limit and window_seconds must be positive")
        async with self._guard:
            now = time.monotonic()
            window_start = now - window_seconds
            timestamps = self._rate_limits[key]
            while timestamps and timestamps[0] <= window_start:
                timestamps.popleft()
            if len(timestamps) >= limit:
                retry_after = max(0.0, timestamps[0] + window_seconds - now)
                return RateLimitDecision(False, 0, retry_after)
            timestamps.append(now)
            return RateLimitDecision(True, limit - len(timestamps), 0.0)

    async def set_state(self, key: str, value: Mapping[str, Any], *, ttl_seconds: int) -> None:
        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be positive")
        async with self._guard:
            self._state[key] = (
                time.monotonic() + ttl_seconds,
                copy.deepcopy(dict(value)),
            )

    async def get_state(self, key: str) -> Mapping[str, Any] | None:
        async with self._guard:
            item = self._state.get(key)
            if item is None:
                return None
            if item[0] <= time.monotonic():
                self._state.pop(key, None)
                return None
            return copy.deepcopy(item[1])

    async def delete_state(self, key: str) -> None:
        async with self._guard:
            self._state.pop(key, None)


class InMemoryConversationStore:
    """Reference backend with the same CAS and commit ordering as SQL."""

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._sessions: dict[tuple[str, str, str], SessionSnapshot] = {}
        self._events: dict[tuple[str, str, str], list[EventRecord]] = defaultdict(list)
        self._summaries: dict[tuple[str, str, str], list[SummaryRecord]] = defaultdict(list)
        self._external_messages: set[tuple[str, str, str]] = set()
        self._outbox: dict[str, OutboxRecord] = {}
        self._outbox_dedupe: dict[str, str] = {}
        self._audits: list[tuple[str, AuditEntry]] = []
        self._memories: dict[tuple[str, str, str, str], tuple[str, int]] = {}

    async def get_session(self, identity: SessionIdentity) -> SessionSnapshot | None:
        async with self._guard:
            snapshot = self._sessions.get(_session_key(identity))
            return copy.deepcopy(snapshot)

    async def create_session(self, identity: SessionIdentity) -> SessionSnapshot:
        async with self._guard:
            key = _session_key(identity)
            existing = self._sessions.get(key)
            if existing is not None:
                return copy.deepcopy(existing)
            snapshot = SessionSnapshot(identity, {}, 0, _utcnow())
            self._sessions[key] = snapshot
            return copy.deepcopy(snapshot)

    async def compare_and_swap_state(
        self,
        identity: SessionIdentity,
        expected_version: int,
        next_state: Mapping[str, Any],
    ) -> SessionSnapshot:
        async with self._guard:
            key = _session_key(identity)
            current = self._sessions.get(key)
            actual_version = current.version if current else 0
            if actual_version != expected_version:
                raise VersionConflictError(
                    f"expected session version {expected_version}, got {actual_version}"
                )
            snapshot = SessionSnapshot(
                identity,
                copy.deepcopy(dict(next_state)),
                expected_version + 1,
                _utcnow(),
            )
            self._sessions[key] = snapshot
            return copy.deepcopy(snapshot)

    async def commit_turn(self, commit: TurnCommit) -> TurnCommitResult:
        """Atomically apply Event -> State -> Summary -> Outbox in that exact order."""

        async with self._guard:
            key = _session_key(commit.identity)
            current = self._sessions.get(key)
            actual_version = current.version if current else 0
            if actual_version != commit.expected_version:
                raise VersionConflictError(
                    f"expected session version {commit.expected_version}, got {actual_version}"
                )
            external_key = None
            if commit.event.channel and commit.event.external_message_id:
                external_key = (
                    commit.identity.tenant_id,
                    commit.event.channel,
                    commit.event.external_message_id,
                )
                if external_key in self._external_messages:
                    raise DuplicateMessageError("external message has already been committed")

            next_version = actual_version + 1
            now = _utcnow()
            event = EventRecord(
                id=str(uuid.uuid4()),
                tenant_id=commit.identity.tenant_id,
                session_id=commit.identity.session_id,
                sequence_no=next_version,
                event_type=commit.event.event_type,
                role=commit.event.role,
                payload=copy.deepcopy(dict(commit.event.payload)),
                trace_id=commit.event.trace_id,
                channel=commit.event.channel,
                external_message_id=commit.event.external_message_id,
                created_at=now,
            )
            session = SessionSnapshot(
                commit.identity,
                copy.deepcopy(dict(commit.next_state)),
                next_version,
                now,
            )
            summary = None
            if commit.summary_content is not None:
                summary = SummaryRecord(
                    id=str(uuid.uuid4()),
                    tenant_id=commit.identity.tenant_id,
                    session_id=commit.identity.session_id,
                    version=next_version,
                    through_sequence=next_version,
                    content=commit.summary_content,
                    created_at=now,
                )
            generated_outbox = []
            staged_memories: dict[tuple[str, str, str, str], tuple[str, int]] = {}
            for memory in commit.memories:
                memory_key = (
                    commit.identity.tenant_id,
                    commit.identity.agent_app_id,
                    commit.identity.user_id,
                    memory.memory_key,
                )
                previous = staged_memories.get(memory_key) or self._memories.get(memory_key)
                memory_version = previous[1] + 1 if previous else 1
                staged_memories[memory_key] = (memory.text, memory_version)
                generated_outbox.append(
                    OutboxInput(
                        topic="memory.upsert",
                        dedupe_key=(
                            f"memory:{commit.identity.tenant_id}:"
                            f"{commit.identity.agent_app_id}:{commit.identity.user_id}:"
                            f"{memory.memory_key}:v{memory_version}"
                        ),
                        payload={
                            "agent_app_id": commit.identity.agent_app_id,
                            "user_id": commit.identity.user_id,
                            "memory_id": memory.memory_key,
                            "text": memory.text,
                            "metadata": dict(memory.metadata),
                        },
                    )
                )

            outbox_records: list[OutboxRecord] = []
            for item in (*commit.outbox, *generated_outbox):
                existing_id = self._outbox_dedupe.get(item.dedupe_key)
                if existing_id:
                    outbox_records.append(self._outbox[existing_id])
                    continue
                record = OutboxRecord(
                    id=str(uuid.uuid4()),
                    tenant_id=commit.identity.tenant_id,
                    topic=item.topic,
                    payload=copy.deepcopy(dict(item.payload)),
                    dedupe_key=item.dedupe_key,
                    attempts=0,
                    status="pending",
                    available_at=now,
                    created_at=now,
                )
                outbox_records.append(record)

            # The mutations below intentionally mirror the durable SQL transaction order.
            self._events[key].append(event)
            self._sessions[key] = session
            if summary is not None:
                self._summaries[key].append(summary)
            self._memories.update(staged_memories)
            for record in outbox_records:
                self._outbox.setdefault(record.id, record)
                self._outbox_dedupe.setdefault(record.dedupe_key, record.id)
            if external_key is not None:
                self._external_messages.add(external_key)
            return TurnCommitResult(session, event, summary, tuple(outbox_records))

    async def list_events(
        self, identity: SessionIdentity, after_sequence: int = -1
    ) -> Sequence[EventRecord]:
        async with self._guard:
            return tuple(
                copy.deepcopy(event)
                for event in self._events[_session_key(identity)]
                if event.sequence_no > after_sequence
            )

    async def latest_summary(self, identity: SessionIdentity) -> SummaryRecord | None:
        async with self._guard:
            summaries = self._summaries[_session_key(identity)]
            return copy.deepcopy(summaries[-1]) if summaries else None

    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[OutboxRecord]:
        del worker_id
        async with self._guard:
            now = _utcnow()
            claimed = []
            for record_id, record in self._outbox.items():
                if len(claimed) >= limit:
                    break
                if record.status in {"pending", "failed"} and record.available_at <= now:
                    updated = replace(record, status="processing", attempts=record.attempts + 1)
                    self._outbox[record_id] = updated
                    claimed.append(updated)
            return tuple(claimed)

    async def mark_processed(self, record_id: str) -> None:
        async with self._guard:
            self._outbox[record_id] = replace(self._outbox[record_id], status="processed")

    async def mark_failed(self, record_id: str, error: str) -> None:
        del error
        async with self._guard:
            record = self._outbox[record_id]
            delay = min(300, 2 ** min(record.attempts, 8))
            self._outbox[record_id] = replace(
                record,
                status="failed",
                available_at=datetime.fromtimestamp(time.time() + delay, timezone.utc),
            )

    async def append_audit(self, entry: AuditEntry) -> str:
        async with self._guard:
            record_id = str(uuid.uuid4())
            self._audits.append((record_id, copy.deepcopy(entry)))
            return record_id
