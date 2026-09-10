"""Tenant-selectable SQLite and Redis storage integration tests."""

from __future__ import annotations

import os

import pytest
from redis.asyncio import Redis
from trpc_agent_sdk.sessions import RedisSessionService, SqlSessionService

from trpc_service.config.models import MemoryRecord, TenantRecord, TenantStorageConfig
from trpc_service.config.secrets import SecretResolver
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import MemoryRepository, TenantRepository
from trpc_service.storage.router import TenantStorageRouter


@pytest.mark.asyncio
async def test_sqlite_tenant_uses_sql_session_and_memory() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    router = TenantStorageRouter(database)
    tenant = TenantRecord(tenant_id="sqlite-tenant", name="SQLite tenant")
    try:
        await TenantRepository(database).create(tenant)
        session_service = router.session_service_for(tenant)
        memory_store = router.memory_store_for(tenant)
        await memory_store.create(
            MemoryRecord(
                memory_id="sqlite-memory",
                tenant_id=tenant.tenant_id,
                principal_id="same-user",
                content="stored in sqlite",
            )
        )

        assert isinstance(session_service, SqlSessionService)
        assert [
            item.content
            for item in await memory_store.list_for_principal(tenant.tenant_id, "same-user")
        ] == ["stored in sqlite"]
        created = await session_service.create_session(
            app_name="sqlite-tenant:assistant",
            user_id="same-user",
            session_id="sqlite-session",
            state={"turn": 1},
        )
        stored = await session_service.get_session(
            app_name="sqlite-tenant:assistant",
            user_id="same-user",
            session_id=created.id,
        )
        assert stored is not None and stored.state == {"turn": 1}
        await session_service.close()
    finally:
        await router.close()
        await database.dispose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_tenant_isolated_from_sqlite_fact_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TRPC_TEST_REDIS_URL is not configured")
    monkeypatch.setenv("TEST_REDIS_URL", redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    await redis_client.flushdb()

    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    router = TenantStorageRouter(database, SecretResolver())
    tenant = TenantRecord(
        tenant_id="redis-tenant",
        name="Redis tenant",
        storage_config=TenantStorageConfig(
            session_backend="redis",
            memory_backend="redis",
            redis_url_ref="env://TEST_REDIS_URL",
        ),
    )
    session_service = None
    try:
        await TenantRepository(database).create(tenant)
        assert await router.ping_for(tenant) is True

        memory_store = router.memory_store_for(tenant)
        await memory_store.create(
            MemoryRecord(
                memory_id="redis-memory",
                tenant_id=tenant.tenant_id,
                principal_id="same-user",
                content="stored in redis",
            )
        )
        assert [
            item.content
            for item in await memory_store.list_for_principal(tenant.tenant_id, "same-user")
        ] == ["stored in redis"]
        assert (
            await MemoryRepository(database).list_for_principal(tenant.tenant_id, "same-user") == []
        )

        session_service = router.session_service_for(tenant)
        assert isinstance(session_service, RedisSessionService)
        created = await session_service.create_session(
            app_name="redis-tenant:assistant",
            user_id="same-user",
            session_id="redis-session",
            state={"backend": "redis"},
        )
        stored = await session_service.get_session(
            app_name="redis-tenant:assistant",
            user_id="same-user",
            session_id=created.id,
        )
        assert stored is not None and stored.state == {"backend": "redis"}
    finally:
        if session_service is not None:
            await session_service.close()
        await router.close()
        await database.dispose()
        await redis_client.aclose()
