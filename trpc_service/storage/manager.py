# ===================================================================
# storage.manager - 按租户懒建 Storage 的容器（生产运行时）
# ===================================================================
# 说明: PRD 2.1「不同租户可选不同数据后端」。网关进程持有 StorageManager:
#   每个租户按其 TenantConfig.backends（session/memory/summary/audit/
#   knowledge/artifact 各自后端）经 StorageFactory 懒建并缓存 Storage；
#   idempotency/lock 属平台共享基础设施，Redis key 含 tenant_id 前缀隔离，
#   不随租户 backends 变化（由网关启动配置决定）。
# 规范: 同一租户配置变更（Admin 热更新）后应失效缓存，下一请求按新 backends
#   重建；进程退出时 close 全部缓存的 Storage。
# ===================================================================

from __future__ import annotations

from typing import Any, Optional

from ..log.logger import get_logger
from ..tenant.models import DataBackendConfig, TenantConfig
from .base import Storage

log = get_logger("storage.manager")


class StorageManager:
    """按租户 backends 懒建并缓存 Storage 的容器。

    Args:
        factory: StorageFactory（持有全局 redis 客户端 / SQL 引擎连接池）
        default_backends: 无租户配置时的兜底后端（如 demo 租户）
    """

    def __init__(self, factory: Any, default_backends: Optional[DataBackendConfig] = None) -> None:
        self._factory = factory
        self._default_backends = default_backends or DataBackendConfig()
        self._cache: dict[str, Storage] = {}

    async def get(self, tenant: TenantConfig) -> Storage:
        """按租户配置获取 Storage（懒建 + 缓存）。"""
        cached = self._cache.get(tenant.tenant_id)
        if cached is not None:
            return cached
        storage = await self._factory.create(tenant.tenant_id, tenant.backends)
        self._cache[tenant.tenant_id] = storage
        log.info("storage created for tenant", extra={"tenant_id": tenant.tenant_id})
        return storage

    def invalidate(self, tenant_id: str) -> None:
        """租户配置变更后失效缓存（下一请求按新 backends 重建）。"""
        self._cache.pop(tenant_id, None)

    def invalidate_all(self) -> None:
        self._cache.clear()

    async def close(self) -> None:
        """关闭全部缓存 Storage（进程退出路径）。"""
        for storage in list(self._cache.values()):
            close = getattr(storage, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception as exc:  # noqa: BLE001 - 关闭尽力而为
                    log.warning("storage close failed", extra={"error": str(exc)})
        self._cache.clear()
