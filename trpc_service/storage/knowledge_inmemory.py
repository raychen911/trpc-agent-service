# ===================================================================
# storage.knowledge_inmemory - Knowledge 域 InMemory 实现（RAG 占位）
# ===================================================================
# 说明: PRD 2.1 Knowledge 域的最小可用实现——按租户存文档切块，检索用
#   关键词重叠打分（非 embedding）。用途: 验证「上传文档 -> 检索命中 ->
#   LLM 基于知识回答」的完整链路（阶段三最小验证）；生产接入向量库
#   （pgvector 等）替换本实现即可，接口不变（见 PRD §2.2）。
# ===================================================================

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .base import KnowledgeStore


def _tokens(text: str) -> set[str]:
    """简单中文/英文分词（占位实现；生产换 embedding 后无需分词）。"""
    # 中文按单字 + 连续 ASCII 词切分，去掉停用词级别的噪音
    ascii_words = re.findall(r"[a-zA-Z0-9_]+", text.lower())
    chars = [c for c in text if '\u4e00' <= c <= '\u9fff']
    return set(ascii_words + chars)


class InMemoryKnowledgeStore(KnowledgeStore):
    """进程内知识库存储（关键词检索占位）。"""

    def __init__(self) -> None:
        # tenant_id -> doc_id -> {chunk_id: {"content": str, "metadata": dict}}
        self._docs: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(dict)

    async def add_document(self, tenant_id: str, doc_id: str, chunks: list[dict[str, Any]]) -> None:
        stored: dict[str, dict[str, Any]] = {}
        for chunk in chunks:
            cid = str(chunk.get("id") or f"{doc_id}:{len(stored)}")
            stored[cid] = {
                "content": str(chunk.get("content", "")),
                "metadata": dict(chunk.get("metadata") or {}),
            }
        self._docs[tenant_id][doc_id] = stored

    async def search(self, tenant_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        query_tokens = _tokens(query)
        if not query_tokens:
            return []
        scored: list[dict[str, Any]] = []
        for doc_id, chunks in (self._docs.get(tenant_id) or {}).items():
            for chunk_id, chunk in chunks.items():
                content = chunk["content"]
                content_tokens = _tokens(content)
                overlap = len(query_tokens & content_tokens)
                if overlap == 0:
                    continue
                # 归一化打分: 重叠数 / 查询词数，叠加长度惩罚（短文本命中更准）
                score = overlap / len(query_tokens) * min(1.0, 10.0 / max(1, len(content)))
                scored.append({
                    "doc_id": doc_id,
                    "chunk_id": chunk_id,
                    "content": content,
                    "score": round(score, 4),
                })
        scored.sort(key=lambda hit: hit["score"], reverse=True)
        return scored[:top_k]

    async def delete_document(self, tenant_id: str, doc_id: str) -> None:
        (self._docs.get(tenant_id) or {}).pop(doc_id, None)
