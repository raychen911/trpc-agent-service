# ===================================================================
# storage.knowledge_vector - Knowledge 域向量检索实现（多节点共享 RAG）
# ===================================================================
# 说明: PRD 2.1 Knowledge=vector 后端——本地哈希 embedding（纯 Python，
#   零新依赖）+ cosine 相似度 top-k 检索，向量随 chunk 存 Redis hash
#   （key 前缀 tenant 隔离，与 Session/Memory 同为共享热存）。
#   embedding 口径: 文本 token（与 InMemory/Redis 分词一致）逐个哈希到
#   固定维度并按出现次数累加，L2 归一化——确定性强（同文本同向量）、
#   可离线运行；语义泛化弱于真实 embedding 模型，生产可替换为
#   pgvector / 远端 embedding API（接口不变，见 PRD §2.2）。
# ===================================================================

from __future__ import annotations

import json
import math
from typing import Any

from .knowledge_inmemory import _tokens
from .base import KnowledgeStore

_KNOWLEDGE_KEY = "knowledge:{tenant}:{doc}"

_EMBED_DIM = 256
"""哈希向量维度；256 维足够区分文档级相似度且序列化开销小。"""


def _embed(text: str) -> list[float]:
    """本地哈希 embedding：token → 哈希桶累加 → L2 归一化。

    确定性（同文本必得同向量，跨节点检索口径一致）；空文本返回零向量
    （cosine 恒为 0，不会被召回）。
    """
    vec = [0.0] * _EMBED_DIM
    for tok in _tokens(text):
        # FNV-1a 变体：简单稳定、无依赖；取模映射到桶
        h = 2166136261
        for ch in tok:
            h = (h ^ ord(ch)) * 16777619 & 0xFFFFFFFF
        idx = h % _EMBED_DIM
        # 符号位打散，减少同桶 token 互相抵消的系统性偏差
        vec[idx] += 1.0 if (h >> 8) & 1 else -1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0:
        return vec
    return [v / norm for v in vec]


def _cosine(a: list[float], b: list[float]) -> float:
    """两个已 L2 归一化向量的内积即 cosine；长度不符按 0 处理（口径变更兜底）。"""
    if not a or not b or len(a) != len(b):
        return 0.0
    return sum(x * y for x, y in zip(a, b))


class HashVectorKnowledgeStore(KnowledgeStore):
    """哈希向量知识库存储（多节点共享，租户 key 前缀隔离，cosine top-k）。"""

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
            content = str(chunk.get("content", ""))
            mapping[cid] = json.dumps(
                {
                    "content": content,
                    "metadata": dict(chunk.get("metadata") or {}),
                    "vec": _embed(content),
                },
                ensure_ascii=False)
        # 整文档原子替换: 先删后写（重复 add 同一 doc_id 幂等）
        await self._redis.delete(key)
        if mapping:
            await self._redis.hset(key, mapping=mapping)

    async def search(self, tenant_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        query_vec = _embed(query)
        if not any(query_vec):
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
                vec = data.get("vec")
                score = _cosine(query_vec, vec) if isinstance(vec, list) else 0.0
                if score <= 0:
                    continue
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
