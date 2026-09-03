"""
Redis Storage Adapter Implementation for low-latency active session caching and distributed locking.
"""

import json
import time
from datetime import datetime, timezone
from typing import List, Optional
import logging
from trpc_service.config.models import (
    Session,
    SessionEvent,
    MemoryItem,
    SummaryItem,
    ArtifactItem,
    AuditLogEntry,
    EventType,
    AuditDecision,
)
from trpc_service.storage.base import BaseStorageAdapter

logger = logging.getLogger(__name__)


class RedisStorageAdapter(BaseStorageAdapter):
    """
    Redis-backed storage adapter providing high-speed distributed locking,
    idempotency checks, and active session state caching.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self._client = None
        self._fallback_store = {}  # In-memory dictionary fallback if Redis is unreachable in dev/test

    def _get_client(self):
        if self._client is None:
            try:
                import redis
                self._client = redis.from_url(self.redis_url, decode_responses=True)
                self._client.ping()
            except Exception as e:
                logger.warning("Could not connect to Redis at %s (%s). Using fallback in-memory state.", self.redis_url, e)
                self._client = False
        return self._client

    async def acquire_session_lock(self, session_id: str, ttl_seconds: float = 30.0) -> bool:
        client = self._get_client()
        key = f"lock:session:{session_id}"
        if client:
            return bool(client.set(key, "1", nx=True, ex=int(ttl_seconds)))
        # Fallback
        now = time.time()
        if key in self._fallback_store and self._fallback_store[key] > now:
            return False
        self._fallback_store[key] = now + ttl_seconds
        return True

    async def release_session_lock(self, session_id: str) -> bool:
        client = self._get_client()
        key = f"lock:session:{session_id}"
        if client:
            return bool(client.delete(key))
        if key in self._fallback_store:
            del self._fallback_store[key]
            return True
        return False

    async def check_and_set_idempotency(self, idempotency_key: str, ttl_seconds: float = 60.0) -> bool:
        client = self._get_client()
        key = f"idempotency:{idempotency_key}"
        if client:
            return bool(client.set(key, "1", nx=True, ex=int(ttl_seconds)))
        now = time.time()
        if key in self._fallback_store and self._fallback_store[key] > now:
            return False
        self._fallback_store[key] = now + ttl_seconds
        return True

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        client = self._get_client()
        key = f"session:{tenant_id}:{session_id}"
        if client:
            data = client.get(key)
            if data:
                return Session.model_validate_json(data)
            return None
        raw = self._fallback_store.get(key)
        return Session.model_validate_json(raw) if raw else None

    async def save_session(self, session: Session) -> Session:
        client = self._get_client()
        key = f"session:{session.tenant_id}:{session.session_id}"
        data = session.model_dump_json()
        if client:
            client.set(key, data, ex=86400 * 7)  # 7 days active TTL
        else:
            self._fallback_store[key] = data
        return session

    async def append_events(
        self,
        tenant_id: str,
        session_id: str,
        events: List[SessionEvent],
        expected_version: int,
    ) -> bool:
        curr_session = await self.get_session(tenant_id, session_id)
        if curr_session and curr_session.current_version != expected_version:
            return False  # Concurrency conflict

        client = self._get_client()
        key = f"events:{tenant_id}:{session_id}"
        current_evts = await self.get_events(tenant_id, session_id, limit=1000)
        start_seq = len(current_evts)

        for idx, evt in enumerate(events):
            evt.sequence_num = start_seq + idx + 1
            dumped = evt.model_dump_json()
            if client:
                client.rpush(key, dumped)
            else:
                if key not in self._fallback_store:
                    self._fallback_store[key] = []
                self._fallback_store[key].append(dumped)

        if curr_session:
            curr_session.current_version += len(events)
            curr_session.updated_at = datetime.now(timezone.utc)
            await self.save_session(curr_session)
        return True

    async def get_events(
        self,
        tenant_id: str,
        session_id: str,
        limit: int = 50,
    ) -> List[SessionEvent]:
        client = self._get_client()
        key = f"events:{tenant_id}:{session_id}"
        if client:
            items = client.lrange(key, -limit, -1)
        else:
            items = self._fallback_store.get(key, [])[-limit:]
        return [SessionEvent.model_validate_json(item) for item in items]

    async def add_memory(self, memory: MemoryItem) -> MemoryItem:
        client = self._get_client()
        key = f"memory:{memory.tenant_id}:{memory.user_id}"
        dumped = memory.model_dump_json()
        if client:
            client.rpush(key, dumped)
        else:
            if key not in self._fallback_store:
                self._fallback_store[key] = []
            self._fallback_store[key].append(dumped)
        return memory

    async def search_memory(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        top_k: int = 5,
    ) -> List[MemoryItem]:
        client = self._get_client()
        key = f"memory:{tenant_id}:{user_id}"
        if client:
            items = client.lrange(key, 0, -1)
        else:
            items = self._fallback_store.get(key, [])
        all_mem = [MemoryItem.model_validate_json(i) for i in items]
        return all_mem[-top_k:]

    async def save_summary(self, summary: SummaryItem) -> SummaryItem:
        client = self._get_client()
        key = f"summary:{summary.tenant_id}:{summary.session_id}"
        data = summary.model_dump_json()
        if client:
            client.set(key, data)
        else:
            self._fallback_store[key] = data
        return summary

    async def get_latest_summary(self, tenant_id: str, session_id: str) -> Optional[SummaryItem]:
        client = self._get_client()
        key = f"summary:{tenant_id}:{session_id}"
        if client:
            data = client.get(key)
        else:
            data = self._fallback_store.get(key)
        return SummaryItem.model_validate_json(data) if data else None

    async def save_artifact(self, artifact: ArtifactItem) -> ArtifactItem:
        return artifact

    async def record_audit_log(self, entry: AuditLogEntry) -> None:
        client = self._get_client()
        key = f"audit:{entry.tenant_id}"
        data = entry.model_dump_json()
        if client:
            client.rpush(key, data)
        else:
            if key not in self._fallback_store:
                self._fallback_store[key] = []
            self._fallback_store[key].append(data)

    async def query_audit_logs(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[AuditLogEntry]:
        client = self._get_client()
        key = f"audit:{tenant_id}"
        if client:
            items = client.lrange(key, -limit, -1)
        else:
            items = self._fallback_store.get(key, [])[-limit:]
        return [AuditLogEntry.model_validate_json(i) for i in items]
