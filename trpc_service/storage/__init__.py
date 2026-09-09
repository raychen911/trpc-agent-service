# ===================================================================
# storage - Storage Adapter（平台层新增）
# ===================================================================
# 说明: 统一数据访问抽象（PRD 2.x），屏蔽 Redis / SQL / 向量库 / 对象存储差异。
#   base: 抽象接口（SessionStore / MemoryStore / AuditStore / IdempotencyStore / Lock）
#   inmemory: 单机开发用（多节点禁用）
#   redis_store / redis_memory: 生产推荐（Session / 幂等 / 锁 / Memory）
#   sql_store: 审计 / Summary / 租户持久化（强一致）
#   factory: 按租户 data_backend_config 组合后端
# 规范: 上层业务不感知具体后端；key/行级隔离由实现层保证。
# ===================================================================

from .base import (
    AuditStore,
    DistributedLock,
    IdempotencyStore,
    MemoryStore,
    SessionStore,
    Storage,
    SummaryStore,
)
from .factory import StorageFactory
from .inmemory import (
    InMemoryAuditStore,
    InMemoryDistributedLock,
    InMemoryIdempotencyStore,
    InMemoryMemoryStore,
    InMemorySessionStore,
    InMemoryStorage,
    InMemorySummaryStore,
)
from .redis_store import (
    RedisDistributedLock,
    RedisIdempotencyStore,
    RedisSessionStore,
    RedisSummaryStore,
)
from .sql_store import SqlAuditStore, SqlSummaryStore, SqlTenantStore, create_sql_engine

__all__ = [
    "AuditStore",
    "DistributedLock",
    "IdempotencyStore",
    "InMemoryAuditStore",
    "InMemoryDistributedLock",
    "InMemoryIdempotencyStore",
    "InMemoryMemoryStore",
    "InMemorySessionStore",
    "InMemoryStorage",
    "InMemorySummaryStore",
    "MemoryStore",
    "RedisDistributedLock",
    "RedisIdempotencyStore",
    "RedisSessionStore",
    "RedisSummaryStore",
    "SessionStore",
    "SqlAuditStore",
    "SqlSummaryStore",
    "SqlTenantStore",
    "Storage",
    "StorageFactory",
    "SummaryStore",
    "create_sql_engine",
]
