# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Environment-friendly construction of the durable tenant manager."""

from __future__ import annotations

from typing import Optional

from ._manager import TenantConfigManager
from ._persistence import MySqlTenantRepository
from ._redis_cache import RedisTenantConfigCache


def build_tenant_config_manager(
    mysql_url: Optional[str] = None,
    redis_url: Optional[str] = None,
    encryption_key: Optional[str] = None,
    listen_for_changes: bool = True,
) -> TenantConfigManager:
    """Build a durable manager when MySQL is configured, else an in-memory one.

    MySQL is deliberately mandatory for durable mode. Redis is an optional
    cache/invalidation layer and is never treated as the source of truth.
    """
    if not mysql_url:
        return TenantConfigManager()
    repository = MySqlTenantRepository(mysql_url, encryption_key)
    cache = RedisTenantConfigCache(redis_url, repository.codec) if redis_url else None
    return TenantConfigManager(repository, cache, listen_for_changes)
