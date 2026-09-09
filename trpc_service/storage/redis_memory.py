# ===================================================================
# storage.redis_memory - Redis Memory 存储
# ===================================================================
# 说明: Memory 写入 Redis list（tenant:user 隔离），支持 TTL 清理与
#   关键词打分检索（生产向量检索由 VectorStore 承载，PRD 2.2）。
# ===================================================================

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

from .base import MemoryStore

_MEMORY_KEY = "memory:{tenant}:{user}"
_MAX_MEMORIES = 500
"""每 (tenant, user) 记忆条数上限（LTRIM 裁剪，审查 09-04 补界）。"""


class RedisMemoryStore(MemoryStore):
    """基于 Redis list 的 Memory 存储（按租户/用户隔离）。"""

    def __init__(self, redis: Any, ttl_seconds: Optional[int] = None) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    def _key(self, tenant_id: str, user_id: str) -> str:
        return _MEMORY_KEY.format(tenant=tenant_id, user=user_id)

    async def add_memory(self, tenant_id: str, user_id: str, memory: dict[str, Any]) -> None:
        key = self._key(tenant_id, user_id)
        entry = dict(memory)
        entry.setdefault("memory_id", uuid.uuid4().hex)
        entry.setdefault("created_at", time.time())
        await self._redis.rpush(key, json.dumps(entry, ensure_ascii=False))
        # 上限裁剪（审查 09-04）：活跃用户 list 无界增长会拖垮内存且检索
        # 只看最近窗口，保留最近 _MAX_MEMORIES 条即可。
        await self._redis.ltrim(key, -_MAX_MEMORIES, -1)
        if self._ttl:
            await self._redis.expire(key, self._ttl)

    async def search_memory(self, tenant_id: str, user_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        key = self._key(tenant_id, user_id)
        raw_items = await self._redis.lrange(key, -100, -1)  # 最近 100 条
        entries = [json.loads(item) for item in raw_items]
        if not query:
            return entries[-top_k:]
        query_terms = set(query.lower().split())

        def _score(entry: dict[str, Any]) -> int:
            content = str(entry.get("content", "")).lower()
            return sum(1 for term in query_terms if term in content)

        ranked = sorted(entries, key=_score, reverse=True)
        return [e for e in ranked if _score(e) > 0][:top_k] or entries[-top_k:]
