"""
In-Memory Storage Adapter Implementation.
"""

import asyncio
import time
from typing import Dict, List, Optional
from trpc_service.config.models import (
    Session,
    SessionEvent,
    MemoryItem,
    SummaryItem,
    ArtifactItem,
    AuditLogEntry,
)
from trpc_service.storage.base import BaseStorageAdapter


class InMemoryStorageAdapter(BaseStorageAdapter):
    """
    In-Memory implementation of BaseStorageAdapter.
    Suitable for local development, unit tests, and lightweight deployments.
    """

    def __init__(self):
        self._locks: Dict[str, float] = {}  # session_id -> expiration timestamp
        self._idempotency_keys: Dict[str, float] = {}  # key -> expiration timestamp
        self._sessions: Dict[str, Session] = {}  # key: f"{tenant_id}:{session_id}"
        self._events: Dict[str, List[SessionEvent]] = {}  # key: f"{tenant_id}:{session_id}"
        self._memories: Dict[str, List[MemoryItem]] = {}  # key: f"{tenant_id}:{user_id}"
        self._summaries: Dict[str, List[SummaryItem]] = {}  # key: f"{tenant_id}:{session_id}"
        self._artifacts: Dict[str, ArtifactItem] = {}
        self._audit_logs: Dict[str, List[AuditLogEntry]] = {}  # key: tenant_id
        self._sync_lock = asyncio.Lock()

    def _session_key(self, tenant_id: str, session_id: str) -> str:
        return f"{tenant_id}:{session_id}"

    async def acquire_session_lock(self, session_id: str, ttl_seconds: float = 30.0) -> bool:
        async with self._sync_lock:
            now = time.time()
            if session_id in self._locks:
                if now < self._locks[session_id]:
                    return False  # Lock is still active
            self._locks[session_id] = now + ttl_seconds
            return True

    async def release_session_lock(self, session_id: str) -> bool:
        async with self._sync_lock:
            if session_id in self._locks:
                del self._locks[session_id]
                return True
            return False

    async def check_and_set_idempotency(self, idempotency_key: str, ttl_seconds: float = 60.0) -> bool:
        async with self._sync_lock:
            now = time.time()
            if idempotency_key in self._idempotency_keys:
                if now < self._idempotency_keys[idempotency_key]:
                    return False  # Already processed
            self._idempotency_keys[idempotency_key] = now + ttl_seconds
            return True

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        key = self._session_key(tenant_id, session_id)
        return self._sessions.get(key)

    async def save_session(self, session: Session) -> Session:
        key = self._session_key(session.tenant_id, session.session_id)
        self._sessions[key] = session
        return session

    async def append_events(
        self,
        tenant_id: str,
        session_id: str,
        events: List[SessionEvent],
        expected_version: int,
    ) -> bool:
        async with self._sync_lock:
            key = self._session_key(tenant_id, session_id)
            curr_session = self._sessions.get(key)
            if curr_session and curr_session.current_version != expected_version:
                return False  # Optimistic concurrency conflict

            if key not in self._events:
                self._events[key] = []

            start_seq = len(self._events[key])
            for idx, evt in enumerate(events):
                evt.sequence_num = start_seq + idx + 1
                self._events[key].append(evt)

            if curr_session:
                curr_session.current_version += len(events)
            return True

    async def get_events(
        self,
        tenant_id: str,
        session_id: str,
        limit: int = 50,
    ) -> List[SessionEvent]:
        key = self._session_key(tenant_id, session_id)
        evts = self._events.get(key, [])
        return evts[-limit:]

    async def add_memory(self, memory: MemoryItem) -> MemoryItem:
        key = f"{memory.tenant_id}:{memory.user_id}"
        if key not in self._memories:
            self._memories[key] = []
        self._memories[key].append(memory)
        return memory

    async def search_memory(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        top_k: int = 5,
    ) -> List[MemoryItem]:
        key = f"{tenant_id}:{user_id}"
        items = self._memories.get(key, [])
        if not query or not items:
            return items[-top_k:]
        # Simple substring / keyword relevance matching for in-memory
        query_lower = query.lower()
        matched = [m for m in items if any(w in m.content.lower() for w in query_lower.split())]
        if not matched:
            return items[-top_k:]
        return matched[-top_k:]

    async def save_summary(self, summary: SummaryItem) -> SummaryItem:
        key = self._session_key(summary.tenant_id, summary.session_id)
        if key not in self._summaries:
            self._summaries[key] = []
        self._summaries[key].append(summary)
        return summary

    async def get_latest_summary(self, tenant_id: str, session_id: str) -> Optional[SummaryItem]:
        key = self._session_key(tenant_id, session_id)
        summaries = self._summaries.get(key, [])
        return summaries[-1] if summaries else None

    async def save_artifact(self, artifact: ArtifactItem) -> ArtifactItem:
        self._artifacts[artifact.artifact_id] = artifact
        return artifact

    async def record_audit_log(self, entry: AuditLogEntry) -> None:
        if entry.tenant_id not in self._audit_logs:
            self._audit_logs[entry.tenant_id] = []
        self._audit_logs[entry.tenant_id].append(entry)

    async def query_audit_logs(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[AuditLogEntry]:
        logs = self._audit_logs.get(tenant_id, [])
        return logs[-limit:]
