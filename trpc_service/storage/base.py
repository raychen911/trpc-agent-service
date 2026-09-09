# ===================================================================
# storage.base - Storage Adapter 抽象接口（平台层新增）
# ===================================================================
# 说明: 上层业务（Session/Memory/Summary/Audit）不感知具体后端（PRD 2.1）。
#   对应 PRD 2.1 的 SessionStore / MemoryStore / AuditStore 三个抽象，
#   增补 IdempotencyStore（幂等）与分布式锁（PRD 2.3-A/E）。
# 规范: 所有 Store 方法必须携带 tenant_id，实现层强制 key/行级隔离。
# ===================================================================

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Optional


class SessionStore(ABC):
    """Session 存储抽象（PRD 2.1）。"""

    @abstractmethod
    async def get_session(self, tenant_id: str, session_id: str) -> Optional[dict[str, Any]]:
        ...

    @abstractmethod
    async def save_session(self, tenant_id: str, session: dict[str, Any]) -> None:
        ...

    @abstractmethod
    async def update_state(self, tenant_id: str, session_id: str, state: dict[str, Any]) -> dict[str, Any]:
        ...

    @abstractmethod
    async def delete_session(self, tenant_id: str, session_id: str) -> None:
        ...


class MemoryStore(ABC):
    """Memory 存储抽象（PRD 2.1）。"""

    @abstractmethod
    async def add_memory(self, tenant_id: str, user_id: str, memory: dict[str, Any]) -> None:
        ...

    @abstractmethod
    async def search_memory(self, tenant_id: str, user_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        ...


class KnowledgeStore(ABC):
    """知识库存储抽象（PRD 2.1 Knowledge 域 / RAG 检索）。

    平台实现层: InMemory 关键词检索占位；生产接入向量库（pgvector 等），
    见 PRD §2.2（知识库向量选型）。
    """

    @abstractmethod
    async def add_document(self, tenant_id: str, doc_id: str, chunks: list[dict[str, Any]]) -> None:
        """写入文档切块。chunks 元素: {"id": str, "content": str, "metadata": dict}"""

    @abstractmethod
    async def search(self, tenant_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        """语义/关键词检索，返回按相关度降序的命中列表。
        元素: {"doc_id": str, "chunk_id": str, "content": str, "score": float}"""

    @abstractmethod
    async def delete_document(self, tenant_id: str, doc_id: str) -> None:
        ...


class SummaryStore(ABC):
    """Summary 存储抽象（PRD 2.1 / 2.2，每 session 一条，低频更新）。"""

    @abstractmethod
    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        ...

    @abstractmethod
    async def save_summary(self, tenant_id: str, session_id: str, content: str) -> None:
        ...

    @abstractmethod
    async def delete_summary(self, tenant_id: str, session_id: str) -> None:
        ...

    @abstractmethod
    async def list_summaries(self, tenant_id: str) -> list[tuple[str, str]]:
        """列出租户全部摘要 [(session_id, content)]（后端迁移 / 校验用，PRD 2.3-D）。"""


class AuditStore(ABC):
    """审计日志存储抽象（PRD 2.1 / 4.4）。"""

    @abstractmethod
    async def write_log(self, tenant_id: str, log: dict[str, Any]) -> None:
        ...

    @abstractmethod
    async def query_logs(self, tenant_id: str, filters: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
        ...


class ArtifactStore(ABC):
    """Artifact 产物存储抽象（PRD 2.1 / 2.2）。

    存模型输出文件 / 中间产物（如生成的报表、图片等）；生产推荐
    S3/MinIO（`artifacts/{tenant_id}/{session_id}/{artifact_id}`），
    当前实现为 InMemory 占位（接口不变，可换后端）。
    """

    @abstractmethod
    async def put_artifact(self,
                           tenant_id: str,
                           session_id: str,
                           name: str,
                           data: bytes,
                           metadata: Optional[dict[str, Any]] = None) -> str:
        """上传产物，返回 artifact_id（如 `{session_id}/{name}`）。"""

    @abstractmethod
    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[bytes]:
        ...

    @abstractmethod
    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> None:
        ...


class IdempotencyStore(ABC):
    """IM 消息幂等去重存储（PRD 2.3-E）。"""

    @abstractmethod
    async def try_acquire(self, key: str, ttl_seconds: int = 86400) -> bool:
        """尝试登记消息（NX 语义）。已存在返回 False（重复消息）。"""

    @abstractmethod
    async def release(self, key: str) -> None:
        ...


class DistributedLock(ABC):
    """Redis 分布式锁（PRD 2.3-A）。

    语义约定（各实现必须一致）：
    - ``acquire(key, timeout)`` 的 ``timeout`` 是**租约 TTL**（持锁上限时长），
      不是等待时间——拿不到锁立即返回 ``False``，不阻塞；
    - ``release`` 仅当仍持有该锁（token 匹配）时才删除，防误删他人锁。
    """

    @abstractmethod
    async def acquire(self, key: str, timeout: float = 10.0) -> bool:
        ...

    @abstractmethod
    async def release(self, key: str) -> None:
        ...


async def acquire_lock_with_retry(lock: DistributedLock,
                                  key: str,
                                  *,
                                  ttl: float = 10.0,
                                  wait_seconds: float = 5.0,
                                  interval: float = 0.05) -> bool:
    """带等待重试的锁获取（PRD 2.3-A 并发写一致性的前置）。

    ``DistributedLock.acquire`` 拿不到立即返回 False（timeout 是租约 TTL），
    本 helper 在 ``wait_seconds`` 总预算内以 ``interval`` 间隔反复尝试，
    供「读-改-写」临界区使用——写 session 这类毫秒级临界区几乎总能等到锁。

    Returns:
        True=持有锁（调用方负责 finally release）；False=等待超时仍未获得，
        调用方按各自策略降级（写入路径尽力写 + 告警，撤回路径放弃标记）。
    """
    deadline = time.monotonic() + wait_seconds
    while True:
        if await lock.acquire(key, timeout=ttl):
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(interval)


class Storage(ABC):
    """组合存储根接口: 平台层访问入口。"""

    @property
    @abstractmethod
    def session(self) -> SessionStore:
        ...

    @property
    @abstractmethod
    def memory(self) -> MemoryStore:
        ...

    @property
    @abstractmethod
    def knowledge(self) -> KnowledgeStore:
        ...

    @property
    @abstractmethod
    def summary(self) -> SummaryStore:
        ...

    @property
    @abstractmethod
    def audit(self) -> AuditStore:
        ...

    @property
    @abstractmethod
    def artifact(self) -> ArtifactStore:
        ...

    @property
    @abstractmethod
    def idempotency(self) -> IdempotencyStore:
        ...

    @property
    @abstractmethod
    def lock(self) -> DistributedLock:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...
