# ===================================================================
# storage.knowledge_redis - Knowledge 域 Redis 实现（多节点共享 RAG）
# ===================================================================
# 说明: PRD 2.1 Knowledge 域的共享后端实现——文档切块存 Redis hash
#   （key 前缀 tenant 隔离），检索沿用关键词重叠打分（与 InMemory 同算法）。
#   动机: InMemory 每进程独立实例，多节点部署下各节点知识库互不可见且
#   进程重启即丢（联调 2026-09-06 实测「搜索功能暂时不可用」的根因）；
#   Redis 与 Session/Memory 同为共享热存，CLI/Admin 录入后所有节点立即可检。
#   生产接入向量库（pgvector 等）替换本实现即可，接口不变（见 PRD §2.2）。
# ===================================================================

from __future__ import annotations

import json
from typing import Any

from .knowledge_inmemory import _tokens
from .base import KnowledgeStore

_KNOWLEDGE_KEY = "knowledge:{tenant}:{doc}"


class RedisKnowledgeStore(KnowledgeStore):
    """基于 Redis hash 的知识库存储（多节点共享，租户 key 前缀隔离）。"""

    def __init__(self, redis: Any) -> None:
        """Args: redis: redis.asyncio.Redis 客户端实例（与 Session/Memory 共享连接）。"""
        self._redis = redis

    def _key(self, tenant_id: str, doc_id: str) -> str:
        return _KNOWLEDGE_KEY.format(tenant=tenant_id, doc=doc_id)

    async def add_document(self, tenant_id: str, doc_id: str, chunks: list[dict[str, Any]]) -> None:
        key = self._key(tenant_id, doc_id)
        mapping: dict[str, str] = {}
        for chunk in chunks:
            cid = str(chunk.get("id") or f"{doc_id}:{len(mapping)}")
            mapping[cid] = json.dumps(
                {
                    "content": str(chunk.get("content", "")),
                    "metadata": dict(chunk.get("metadata") or {}),
                },
                ensure_ascii=False)
        # 整文档原子替换: 先删后写（重复 add 同一 doc_id 幂等）
        await self._redis.delete(key)
        if mapping:
            await self._redis.hset(key, mapping=mapping)

    async def search(self, tenant_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        query_tokens = _tokens(query)
        if not query_tokens:
            return []
        scored: list[dict[str, Any]] = []
        pattern = self._key(tenant_id, "*")
        async for key in self._redis.scan_iter(match=pattern):
            raw = await self._redis.hgetall(key)
            if not raw:
                continue
            key_s = key.decode() if isinstance(key, bytes) else key
            # key = knowledge:{tenant}:{doc}
            doc_id = key_s.split(":", 2)[2]
            for chunk_id, value in raw.items():
                value_s = value.decode() if isinstance(value, bytes) else value
                try:
                    data = json.loads(value_s)
                except ValueError:
                    continue
                content = str(data.get("content", ""))
                content_tokens = _tokens(content)
                overlap = len(query_tokens & content_tokens)
                if overlap == 0:
                    continue
                # 与 InMemory 同算法: 重叠数 / 查询词数，叠加长度惩罚
                score = overlap / len(query_tokens) * min(1.0, 10.0 / max(1, len(content)))
                scored.append({
                    "doc_id": doc_id,
                    "chunk_id": chunk_id.decode() if isinstance(chunk_id, bytes) else chunk_id,
                    "content": content,
                    "score": round(score, 4),
                })
        scored.sort(key=lambda hit: hit["score"], reverse=True)
        return scored[:top_k]

    async def delete_document(self, tenant_id: str, doc_id: str) -> None:
        await self._redis.delete(self._key(tenant_id, doc_id))
