# ===================================================================
# tenant.registry - 租户配置注册表（本地 LRU 缓存）
# ===================================================================
# 说明: Worker 从 ctx 取 tenant_id 后经 Registry 加载租户配置（PRD 1.4 配置隔离）。
#   启动加载全量配置到本地 LRU，减少 DB 查询；Redis 发布订阅可做失效通知。
# 规范: 未命中时回源加载（load_fn）；缓存使用 LRU 防单租户刷爆内存。
# ===================================================================

from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Awaitable, Callable, Optional

from .models import TenantConfig, tenant_from_dict

LoadFn = Callable[[str], Awaitable[dict]]
"""回源加载函数: tenant_id -> 租户原始 dict（来自 SQL/Redis）。"""


class TenantRegistry:
    """租户配置注册表: LRU 缓存 + 异步回源加载 + 主动失效。"""

    def __init__(
        self,
        load_fn: Optional[LoadFn] = None,
        capacity: int = 1024,
    ) -> None:
        self._load_fn = load_fn
        self._cache: "OrderedDict[str, TenantConfig]" = OrderedDict()
        self._capacity = max(1, capacity)
        self._lock = asyncio.Lock()

    async def get(self, tenant_id: str) -> Optional[TenantConfig]:
        """获取租户配置；缓存未命中时回源加载并写入 LRU。"""
        cached = self._cache.get(tenant_id)
        if cached is not None:
            self._cache.move_to_end(tenant_id)
            return cached
        if self._load_fn is None:
            return None
        async with self._lock:
            # 双检，避免并发回源重复加载
            cached = self._cache.get(tenant_id)
            if cached is not None:
                return cached
            raw = await self._load_fn(tenant_id)
            if not raw:
                return None
            config = tenant_from_dict(raw)
            self._put(tenant_id, config)
            return config

    async def get_or_raise(self, tenant_id: str) -> TenantConfig:
        """获取租户配置，不存在则抛 KeyError（上游转 404）。"""
        config = await self.get(tenant_id)
        if config is None:
            raise KeyError(f"tenant not found: {tenant_id}")
        return config

    def _put(self, tenant_id: str, config: TenantConfig) -> None:
        self._cache[tenant_id] = config
        self._cache.move_to_end(tenant_id)
        while len(self._cache) > self._capacity:
            self._cache.popitem(last=False)

    def put(self, tenant_id: str, config: TenantConfig) -> None:
        """主动写入/更新缓存（Admin API 配置下发 / 热更新）。"""
        self._put(tenant_id, config)

    def invalidate(self, tenant_id: str) -> None:
        """失效单租户缓存（租户配置变更 / 回滚）。"""
        self._cache.pop(tenant_id, None)

    def invalidate_all(self) -> None:
        """清空全部缓存（启动 / 全量回滚）。"""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)
