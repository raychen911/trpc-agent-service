"""StorageAdapter：按租户选择 InMemory / Redis / SQL 后端。"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.memory import InMemoryMemoryService
from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService

from trpc_service.config.tenant_config import StorageBackend, TenantConfig

# 采集延迟的 Session 服务方法（Runner 高频调用路径）
_TIMED_SESSION_METHODS = (
    "create_session",
    "get_session",
    "list_sessions",
    "delete_session",
    "append_event",
    "update_session",
)


class TimedSessionService(BaseSessionService):
    """Session 后端延迟采集代理（须为 SessionServiceABC 子类：InvocationContext 做实例校验）。"""

    def __init__(self, inner: BaseSessionService, tenant_id: str, backend: str):
        self._inner = inner
        self._tenant_id = tenant_id
        self._backend = backend

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def _timed(self, method: str, call, *args, **kwargs):
        start = time.monotonic()
        try:
            return await call(*args, **kwargs)
        finally:
            from trpc_service.metrics.collector import metrics_collector

            metrics_collector.inc_backend_latency(
                self._tenant_id, self._backend, method, (time.monotonic() - start) * 1000
            )

    async def create_session(self, *args, **kwargs):
        return await self._timed("create_session", self._inner.create_session, *args, **kwargs)

    async def get_session(self, *args, **kwargs):
        return await self._timed("get_session", self._inner.get_session, *args, **kwargs)

    async def list_sessions(self, *args, **kwargs):
        return await self._timed("list_sessions", self._inner.list_sessions, *args, **kwargs)

    async def delete_session(self, *args, **kwargs):
        return await self._timed("delete_session", self._inner.delete_session, *args, **kwargs)

    async def append_event(self, *args, **kwargs):
        return await self._timed("append_event", self._inner.append_event, *args, **kwargs)

    async def update_session(self, *args, **kwargs):
        return await self._timed("update_session", self._inner.update_session, *args, **kwargs)

    async def create_session_summary(self, *args, **kwargs):
        return await self._inner.create_session_summary(*args, **kwargs)

    async def get_session_summary(self, *args, **kwargs):
        return await self._inner.get_session_summary(*args, **kwargs)

    async def close(self):
        await self._inner.close()


@dataclass
class StorageAdapter:
    """一个租户的会话/记忆服务组合。"""

    session_service: BaseSessionService
    memory_service: Optional[BaseMemoryService] = None
    backend: str = StorageBackend.IN_MEMORY

    async def close(self) -> None:
        if self.memory_service is not None:
            try:
                await self.memory_service.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            await self.session_service.close()
        except Exception:  # noqa: BLE001
            pass


def create_storage(tenant_config: TenantConfig) -> StorageAdapter:
    """按租户存储配置装配 Session/Memory 服务。"""
    backend = tenant_config.storage.session_backend
    if backend == StorageBackend.REDIS:
        url = tenant_config.storage.redis_url
        if not url:
            raise ValueError(f"租户 {tenant_config.tenant_id} 选择 Redis 后端但未配置 redis_url")
        return _wrap_timed(
            RedisSessionService(db_url=url, is_async=True),
            RedisMemoryService(db_url=url, enabled=True, is_async=True),
            tenant_config, backend,
        )
    if backend == StorageBackend.SQL:
        url = tenant_config.storage.sql_url
        if not url:
            raise ValueError(
                f"租户 {tenant_config.tenant_id} 选择 SQL 后端但未配置 sql_url"
                "（在 tenants.yaml 或环境变量中填写，如 mysql+pymysql://user:pass@host/db）"
            )
        # is_async=False：框架 _get_session 给 update_time 赋 SQL 表达式（func.now()），
        # flush 后该属性必然过期，后续同步读属性在异步引擎上抛 MissingGreenlet
        # （框架异步 SQL 路径的已知缺陷）。同步模式是官方支持路径，无此问题；
        # 代价是事件循环内的阻塞 IO，演示/单机规模可接受，多节点生产需关注。
        return _wrap_timed(
            SqlSessionService(db_url=url, is_async=False),
            SqlMemoryService(db_url=url, enabled=True, is_async=False),
            tenant_config, backend,
        )
    # 默认内存后端
    return _wrap_timed(
        InMemorySessionService(),
        InMemoryMemoryService(enabled=True),
        tenant_config, StorageBackend.IN_MEMORY,
    )


def _wrap_timed(session_service, memory_service, tenant_config: TenantConfig, backend: str) -> StorageAdapter:
    """装配并包一层延迟采集代理（对 SDK Runner 透明）。"""
    label = backend.value if hasattr(backend, "value") else str(backend)
    return StorageAdapter(
        session_service=TimedSessionService(session_service, tenant_config.tenant_id, label),
        memory_service=memory_service,
        backend=backend,
    )
