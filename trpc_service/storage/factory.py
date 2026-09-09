# ===================================================================
# storage.factory - Store 工厂（按租户 data_backend_config 创建）
# ===================================================================
# 说明: 不同租户可选择不同数据后端（PRD 2.1/2.2），平台层按租户配置
#   组合出 Storage 实例。Redis 客户端与 SQL 引擎全局复用（连接池），
#   内存存储单租户独立实例（多节点禁用）。
# 规范: 工厂返回的 Storage 已按租户 data_backend_config 选好后端，
#   上层业务不感知实现差异。
# ===================================================================

from __future__ import annotations

from typing import Any, Optional

from .base import Storage
from .inmemory import InMemoryStorage, InMemorySummaryStore
from .redis_store import (
    RedisDistributedLock,
    RedisIdempotencyStore,
    RedisSessionStore,
    RedisSummaryStore,
)

# 内存实现按需导入，避免未安装依赖时 import 失败
from .sql_store import SqlAuditStore, SqlSummaryStore


class StorageFactory:
    """按租户配置创建 Storage 组合实例。"""

    def __init__(
        self,
        redis: Optional[Any] = None,
        sql_engine: Optional[Any] = None,
        memory_stores: Optional[dict[str, "InMemoryStorage"]] = None,
    ) -> None:
        """Args:
            redis: 全局 redis.asyncio 客户端（连接池复用）
            sql_engine: 全局 SQL 异步引擎（复用）
            memory_stores: 租户 -> InMemoryStorage 映射（单机开发）
        """
        self._redis = redis
        self._sql_engine = sql_engine
        self._memory_stores = memory_stores if memory_stores is not None else {}

    async def create(self, tenant_id: str, backend_config: Any) -> Storage:
        """按租户 data_backend_config 创建 Storage。

        Args:
            tenant_id: 租户标识（用于内存存储隔离）
            backend_config: TenantConfig.backends（DataBackendConfig）

        Returns:
            Storage: 组合存储实例
        """
        session_backend = getattr(backend_config, "session", "inmemory")
        memory_backend = getattr(backend_config, "memory", "inmemory")
        summary_backend = getattr(backend_config, "summary", "sql")
        audit_backend = getattr(backend_config, "audit", "inmemory")
        knowledge_backend = getattr(backend_config, "knowledge", "inmemory")

        # 未实现后端显式报错而非静默降级（PRD 2.2 口径，同 knowledge=vector /
        # artifact=s3）——避免「文档声称 ≠ 代码行为」：session=sql 若静默落
        # InMemory，多节点会话一致性会静默失效（审查 09-04 缺陷 #1）。
        if session_backend == "sql":
            raise NotImplementedError("租户配置要求 session=sql，但该后端尚未实现（生产演进，见 PRD §2.2）；"
                                      "请改用 redis 或 inmemory")

        # Session + 平台共享基础设施（幂等/锁随 session 后端形态，PRD 2.1）
        # memory_storage: 单机 InMemoryStorage 持有者（session 非 redis 时存在，
        #   供 session/audit/knowledge 复用同一实例保证隔离一致）。
        # 审查 09-04 缺陷 #2 修复：此前 memory_storage 仅在「session 与 memory
        #   都非 redis」分支赋值，{session: inmemory, memory: redis} 会在
        #   audit/knowledge 段 UnboundLocalError。
        memory_storage: Optional[InMemoryStorage] = None
        if session_backend == "redis":
            if self._redis is None:
                raise RuntimeError("租户配置要求 Redis 后端，但未配置 Redis 客户端")
            session_store: Any = RedisSessionStore(self._redis)
            idem_store: Any = RedisIdempotencyStore(self._redis)
            lock_store: Any = RedisDistributedLock(self._redis)
        else:
            memory_storage = self._memory_stores.get(tenant_id)
            if memory_storage is None:
                memory_storage = InMemoryStorage()
                self._memory_stores[tenant_id] = memory_storage
            session_store = memory_storage.session
            idem_store = memory_storage.idempotency
            lock_store = memory_storage.lock

        # Memory 后端（独立选择，PRD 2.2）
        if memory_backend == "redis":
            if self._redis is None:
                raise RuntimeError("租户配置要求 Redis 后端，但未配置 Redis 客户端")
            from .redis_memory import RedisMemoryStore

            memory_store: Any = RedisMemoryStore(self._redis)
        elif memory_storage is not None:
            memory_store = memory_storage.memory
        else:
            # session=redis 但 memory=inmemory：审计/knowledge 无持有者可复用，
            # memory 也用独立实例（与既有行为一致）
            from .inmemory import InMemoryMemoryStore

            memory_store = InMemoryMemoryStore()

        if audit_backend == "sql":
            if self._sql_engine is None:
                raise RuntimeError("租户配置要求 SQL 审计后端，但未配置 SQL 引擎")
            audit_store: Any = SqlAuditStore(self._sql_engine)
        elif memory_storage is not None:
            audit_store = memory_storage.audit
        else:
            from .inmemory import InMemoryAuditStore

            audit_store = InMemoryAuditStore()

        # Summary 后端（独立选择，PRD 2.2: SQL / Redis / InMemory）
        if summary_backend == "sql":
            if self._sql_engine is None:
                raise RuntimeError("租户配置要求 SQL summary 后端，但未配置 SQL 引擎")
            summary_store: Any = SqlSummaryStore(self._sql_engine)
        elif summary_backend == "redis":
            if self._redis is None:
                raise RuntimeError("租户配置要求 Redis summary 后端，但未配置 Redis 客户端")
            summary_store = RedisSummaryStore(self._redis)
        else:
            summary_store = InMemorySummaryStore()

        # Knowledge 后端（RAG 检索，PRD 2.1）: inmemory（单机）/ redis（多节点
        # 共享关键词检索）/ vector（多节点共享向量检索：本地哈希 embedding +
        # cosine top-k，零外部依赖；生产可替换 pgvector / embedding API，接口
        # 不变，见 PRD §2.2）。vector 无 Redis 时显式报错而非静默降级。
        if knowledge_backend == "vector":
            if self._redis is None:
                raise RuntimeError("租户配置要求 knowledge=vector，但未配置 Redis 客户端；"
                                   "请配置 redis 或改用 inmemory")
            from .knowledge_vector import HashVectorKnowledgeStore

            knowledge_store: Any = HashVectorKnowledgeStore(self._redis)
        elif knowledge_backend == "redis":
            if self._redis is None:
                raise RuntimeError("租户配置要求 Redis knowledge 后端，但未配置 Redis 客户端")
            from .knowledge_redis import RedisKnowledgeStore

            knowledge_store: Any = RedisKnowledgeStore(self._redis)
        elif memory_storage is not None:
            # 单机路径复用租户 InMemoryStorage 的 knowledge 实例（隔离一致）
            knowledge_store = memory_storage.knowledge
        else:
            from .knowledge_inmemory import InMemoryKnowledgeStore

            knowledge_store = InMemoryKnowledgeStore()

        # Artifact 后端（PRD 2.1/2.2）: 当前 InMemory 实现；s3（MinIO/S3）为
        # 生产演进，未实现时显式报错而非静默降级（同 Knowledge 口径）。
        artifact_backend = getattr(backend_config, "artifact", "inmemory")
        if artifact_backend == "s3":
            raise NotImplementedError("租户配置要求 artifact=s3，但该后端尚未实现（生产演进，见 PRD §2.2）；"
                                      "请改用 inmemory")
        from .inmemory import InMemoryArtifactStore

        artifact_store = InMemoryArtifactStore()

        return _CompositeStorage(session_store, memory_store, summary_store, audit_store, idem_store, lock_store,
                                 knowledge_store, artifact_store)


class _CompositeStorage(Storage):
    """组合存储实现（工厂产出）。"""

    def __init__(self, session, memory, summary, audit, idempotency, lock, knowledge, artifact) -> None:
        self._session = session
        self._memory = memory
        self._summary = summary
        self._audit = audit
        self._idempotency = idempotency
        self._lock = lock
        self._knowledge = knowledge
        self._artifact = artifact

    @property
    def session(self):
        return self._session

    @property
    def memory(self):
        return self._memory

    @property
    def knowledge(self):
        return self._knowledge

    @property
    def summary(self):
        return self._summary

    @property
    def audit(self):
        return self._audit

    @property
    def artifact(self):
        return self._artifact

    @property
    def idempotency(self):
        return self._idempotency

    @property
    def lock(self):
        return self._lock

    async def close(self) -> None:
        close_audit = getattr(self._audit, "close", None)
        if close_audit is not None:
            await close_audit()
