# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant-scoped session and memory services."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError
from trpc_agent_sdk.abc import MemoryServiceABC
from trpc_agent_sdk.abc import SearchMemoryResponse
from trpc_service.workspace import TenantMemoryService
from trpc_service.workspace import TenantSessionService
from trpc_service.workspace import TenantStorageRouter
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import StorageBackendConfig
from trpc_service.tenant import Tenant
from trpc_agent_sdk.sessions import InMemorySessionService
import trpc_service.workspace._router as router_module


async def test_session_isolation_between_tenants():
    base = InMemorySessionService()
    svc_a = TenantSessionService(base, "tenant_a")
    svc_b = TenantSessionService(base, "tenant_b")

    s_a = await svc_a.create_session(app_name="myapp", user_id="u1", session_id="s1", state={"k": "a"})
    s_b = await svc_b.create_session(app_name="myapp", user_id="u1", session_id="s1", state={"k": "b"})

    # app_name is tenant-scoped at the storage boundary.
    assert s_a.app_name == "tenant_a:myapp"
    assert s_b.app_name == "tenant_b:myapp"
    assert s_a.state["k"] == "a"
    assert s_b.state["k"] == "b"

    got_a = await svc_a.get_session(app_name="myapp", user_id="u1", session_id="s1")
    got_b = await svc_b.get_session(app_name="myapp", user_id="u1", session_id="s1")
    assert got_a is not None and got_a.state["k"] == "a"
    assert got_b is not None and got_b.state["k"] == "b"


async def test_session_listing_is_tenant_scoped():
    base = InMemorySessionService()
    svc_a = TenantSessionService(base, "tenant_a")
    svc_b = TenantSessionService(base, "tenant_b")

    await svc_a.create_session(app_name="myapp", user_id="u1", session_id="s1")
    await svc_b.create_session(app_name="myapp", user_id="u1", session_id="s2")

    listed_a = await svc_a.list_sessions(app_name="myapp", user_id="u1")
    assert [s.id for s in listed_a.sessions] == ["s1"]


async def test_scoping_is_idempotent():
    base = InMemorySessionService()
    svc = TenantSessionService(base, "tenant_a")
    await svc.create_session(app_name="myapp", user_id="u1", session_id="s1")
    # A second create with an already-scoped app_name must not double-prefix.
    await svc.create_session(app_name="tenant_a:myapp", user_id="u1", session_id="s1")
    got = await svc.get_session(app_name="myapp", user_id="u1", session_id="s1")
    assert got is not None


class RecordingMemory(MemoryServiceABC):
    """Minimal memory backend that records the keys it is called with."""

    def __init__(self, enabled: bool = True) -> None:
        super().__init__(enabled=enabled)
        self.search_keys: list[str] = []
        self.stored: list = []
        self.closed = False

    async def store_session(self, session, agent_context=None) -> None:
        self.stored.append(session)

    async def search_memory(self, key, query, limit=10, agent_context=None) -> SearchMemoryResponse:
        self.search_keys.append(key)
        return SearchMemoryResponse()

    async def close(self) -> None:
        self.closed = True


async def test_memory_search_key_is_tenant_scoped():
    backend = RecordingMemory()
    mem = TenantMemoryService(backend, "tenant_a")

    await mem.search_memory("myapp/u1", "hello")
    assert backend.search_keys == ["tenant_a:myapp/u1"]

    # Already-scoped keys must not be double-prefixed.
    await mem.search_memory("tenant_a:myapp/u1", "hello")
    assert backend.search_keys == ["tenant_a:myapp/u1", "tenant_a:myapp/u1"]


async def test_memory_delegates_store_and_close():
    backend = RecordingMemory()
    mem = TenantMemoryService(backend, "tenant_a")

    assert mem.enabled is True
    await mem.store_session("fake-session")
    assert backend.stored == ["fake-session"]

    await mem.close()
    assert backend.closed is True


def _tenant_with_storage(tenant_id: str, session_backend: str, memory_backend: str) -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        name=tenant_id,
        model=ModelEndpoint(model_name="m"),
        storage_config=StorageBackendConfig(
            session_backend=session_backend,
            memory_backend=memory_backend,
        ),
    )


def test_storage_router_selects_and_caches_tenant_backends(monkeypatch):
    router = TenantStorageRouter()
    redis_session, redis_memory, mysql_session, mysql_memory = object(), object(), object(), object()
    monkeypatch.setattr(router_module, "_redis_session_builder", lambda _url: redis_session)
    monkeypatch.setattr(router_module, "_redis_memory_builder", lambda _url: redis_memory)
    monkeypatch.setattr(router_module, "_mysql_session_builder", lambda _url: mysql_session)
    monkeypatch.setattr(router_module, "_mysql_memory_builder", lambda _url: mysql_memory)
    redis_tenant = _tenant_with_storage("tenant_a", "redis", "redis")
    redis_tenant.storage_config.redis_url = SecretStr("redis://tenant/0")
    mysql_tenant = _tenant_with_storage("tenant_b", "mysql", "mysql")
    mysql_tenant.storage_config.mysql_url = SecretStr("mysql+aiomysql://user:pass@host/db")

    assert router.session_service(redis_tenant) is redis_session
    assert router.session_service(redis_tenant) is redis_session
    assert router.memory_service(redis_tenant) is redis_memory
    assert router.session_service(mysql_tenant) is mysql_session
    assert router.memory_service(mysql_tenant) is mysql_memory


def test_storage_router_rejects_missing_and_unknown_backends(monkeypatch):
    router = TenantStorageRouter()
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("MYSQL_URL", raising=False)
    tenant = _tenant_with_storage("tenant_a", "redis", "redis")

    try:
        router.session_service(tenant)
    except ValueError as exc:
        assert "requires" in str(exc)
    else:
        raise AssertionError("missing Redis URL must be rejected")

    with pytest.raises(ValidationError):
        StorageBackendConfig(session_backend="does-not-exist")
    mysql = _tenant_with_storage("tenant_b", "mysql", "mysql")
    with pytest.raises(ValueError, match="MYSQL_URL"):
        router.session_service(mysql)
    assert router_module._is_async_mysql_url("mysql+aiomysql://host/db") is True
    assert router_module._is_async_mysql_url("mysql+pymysql://host/db") is False
    assert router_module._mysql_runtime_url("mysql+aiomysql://host/db") == ("mysql+pymysql://host/db")
    assert router_module._mysql_runtime_url("mysql+asyncmy://host/db") == ("mysql+pymysql://host/db")
    with pytest.raises(ValueError, match="mysql://"):
        router_module._validate_mysql_url("postgresql://host/db")
