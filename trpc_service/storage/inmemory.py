# ===================================================================
# storage.inmemory - InMemory 存储实现（仅单机开发）
# ===================================================================
# 说明: PRD 2.1 明确「仅单机开发，多节点禁用」——不提供跨节点数据同步，
#   用于本地自测（Web UI IM）与单元测试。生产环境请使用 Redis/SQL 实现。
# 规范: key 全部带 tenant_id 前缀，模拟行级隔离。
# ===================================================================

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any, Optional

from .base import (
    ArtifactStore,
    AuditStore,
    DistributedLock,
    IdempotencyStore,
    KnowledgeStore,
    MemoryStore,
    SessionStore,
    Storage,
    SummaryStore,
)
from .knowledge_inmemory import InMemoryKnowledgeStore


class InMemorySessionStore(SessionStore):
    """进程内 Session 存储。"""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}

    def _key(self, tenant_id: str, session_id: str) -> str:
        return f"{tenant_id}:{session_id}"

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[dict[str, Any]]:
        return self._data.get(self._key(tenant_id, session_id))

    async def save_session(self, tenant_id: str, session: dict[str, Any]) -> None:
        key = self._key(tenant_id, session.get("session_id", ""))
        self._data[key] = dict(session)

    async def update_state(self, tenant_id: str, session_id: str, state: dict[str, Any]) -> dict[str, Any]:
        key = self._key(tenant_id, session_id)
        session = self._data.setdefault(key, {"session_id": session_id, "state": {}, "version": 0})
        session["state"] = dict(state)
        session["version"] = int(session.get("version", 0)) + 1
        return session

    async def delete_session(self, tenant_id: str, session_id: str) -> None:
        self._data.pop(self._key(tenant_id, session_id), None)


class InMemoryMemoryStore(MemoryStore):
    """进程内 Memory 存储（不做向量检索，按内容关键词打分）。"""

    def __init__(self) -> None:
        self._data: dict[str, list[dict[str, Any]]] = {}

    def _key(self, tenant_id: str, user_id: str) -> str:
        return f"{tenant_id}:{user_id}"

    async def add_memory(self, tenant_id: str, user_id: str, memory: dict[str, Any]) -> None:
        key = self._key(tenant_id, user_id)
        entry = dict(memory)
        entry.setdefault("memory_id", uuid.uuid4().hex)
        entry.setdefault("created_at", time.time())
        self._data.setdefault(key, []).append(entry)

    async def search_memory(self, tenant_id: str, user_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        entries = self._data.get(self._key(tenant_id, user_id), [])
        if not query:
            return entries[-top_k:]
        # 简单关键词命中打分（生产用向量库，PRD 2.2）
        query_terms = set(query.lower().split())

        def _score(entry: dict[str, Any]) -> int:
            content = str(entry.get("content", "")).lower()
            return sum(1 for term in query_terms if term in content)

        ranked = sorted(entries, key=_score, reverse=True)
        return [e for e in ranked if _score(e) > 0][:top_k] or entries[-top_k:]


class InMemorySummaryStore(SummaryStore):
    """进程内 Summary 存储（每 session 一条）。"""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def _key(self, tenant_id: str, session_id: str) -> str:
        return f"{tenant_id}:{session_id}"

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        return self._data.get(self._key(tenant_id, session_id))

    async def save_summary(self, tenant_id: str, session_id: str, content: str) -> None:
        self._data[self._key(tenant_id, session_id)] = content

    async def delete_summary(self, tenant_id: str, session_id: str) -> None:
        self._data.pop(self._key(tenant_id, session_id), None)

    async def list_summaries(self, tenant_id: str) -> list[tuple[str, str]]:
        prefix = f"{tenant_id}:"
        return [(k[len(prefix):], v) for k, v in self._data.items() if k.startswith(prefix)]


class InMemoryAuditStore(AuditStore):
    """进程内审计日志存储。"""

    def __init__(self) -> None:
        self._logs: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    async def write_log(self, tenant_id: str, log: dict[str, Any]) -> None:
        entry = dict(log)
        entry.setdefault("tenant_id", tenant_id)
        entry.setdefault("created_at", time.time())
        with self._lock:
            self._logs.append(entry)

    async def query_logs(self, tenant_id: str, filters: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            matched = [
                e for e in self._logs
                if e.get("tenant_id") == tenant_id and all(e.get(k) == v for k, v in filters.items())
            ]
        return matched[-limit:][::-1]


class InMemoryIdempotencyStore(IdempotencyStore):
    """进程内幂等去重（模拟 Redis SET NX EX）。"""

    def __init__(self) -> None:
        self._seen: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def try_acquire(self, key: str, ttl_seconds: int = 86400) -> bool:
        async with self._lock:
            now = time.time()
            expired = [k for k, t in self._seen.items() if now - t > ttl_seconds]
            for k in expired:
                self._seen.pop(k, None)
            if key in self._seen:
                return False
            self._seen[key] = now
            return True

    async def release(self, key: str) -> None:
        async with self._lock:
            self._seen.pop(key, None)


class InMemoryDistributedLock(DistributedLock):
    """进程内锁（单机开发用；多节点请用 RedisLock）。"""

    def __init__(self) -> None:
        self._held: dict[str, float] = {}
        self._lock = threading.Lock()

    async def acquire(self, key: str, timeout: float = 10.0) -> bool:
        with self._lock:
            now = time.time()
            held_until = self._held.get(key, 0)
            if held_until > now:
                return False
            self._held[key] = now + timeout
            return True

    async def release(self, key: str) -> None:
        with self._lock:
            self._held.pop(key, None)


class InMemoryArtifactStore(ArtifactStore):
    """内存 Artifact 存储（占位；生产换 S3/MinIO，接口不变，PRD 2.2）。"""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}

    def _key(self, tenant_id: str, artifact_id: str) -> str:
        return f"{tenant_id}:{artifact_id}"

    async def put_artifact(self,
                           tenant_id: str,
                           session_id: str,
                           name: str,
                           data: bytes,
                           metadata: Optional[dict[str, Any]] = None) -> str:
        artifact_id = f"{session_id}/{name}"
        self._data[self._key(tenant_id, artifact_id)] = data
        return artifact_id

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[bytes]:
        return self._data.get(self._key(tenant_id, artifact_id))

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> None:
        self._data.pop(self._key(tenant_id, artifact_id), None)


class InMemoryStorage(Storage):
    """InMemory 组合存储（PRD 2.1: 仅单机开发，多节点禁用）。"""

    def __init__(self) -> None:
        self._session = InMemorySessionStore()
        self._memory = InMemoryMemoryStore()
        self._knowledge = InMemoryKnowledgeStore()
        self._summary = InMemorySummaryStore()
        self._audit = InMemoryAuditStore()
        self._idempotency = InMemoryIdempotencyStore()
        self._lock = InMemoryDistributedLock()
        self._artifact = InMemoryArtifactStore()

    @property
    def session(self) -> SessionStore:
        return self._session

    @property
    def memory(self) -> MemoryStore:
        return self._memory

    @property
    def knowledge(self) -> KnowledgeStore:
        return self._knowledge

    @property
    def summary(self) -> SummaryStore:
        return self._summary

    @property
    def audit(self) -> AuditStore:
        return self._audit

    @property
    def artifact(self) -> ArtifactStore:
        return self._artifact

    @property
    def idempotency(self) -> IdempotencyStore:
        return self._idempotency

    @property
    def lock(self) -> DistributedLock:
        return self._lock

    async def close(self) -> None:
        pass
