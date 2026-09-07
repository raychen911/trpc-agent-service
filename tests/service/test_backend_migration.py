# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Tests for real-service tenant migration adapters and merge verification."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from trpc_service.workspace import LocalMigrationBackend
from trpc_service.workspace import StorageRecord
from trpc_service.workspace import TenantBackendMigrationAdapter
from trpc_service.workspace import TenantDataMigrator
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import StorageBackendConfig
from trpc_service.tenant import Tenant
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

import trpc_service.workspace._backend_migration as migration_module


def event(event_id: str = "e1", timestamp: float = 100.0) -> Event:
    return Event(
        id=event_id,
        author="assistant",
        timestamp=timestamp,
        content=Content(parts=[Part.from_text(text="hello")]),
    )


def session(tenant_id: str = "tenant_a", session_id: str = "s1") -> Session:
    return Session(
        app_name=f"{tenant_id}:default",
        user_id="u1",
        id=session_id,
        state={
            "answer": 42,
            "app:shared": "yes",
            "user:language": "zh"
        },
        events=[event()],
        historical_events=[event("old", 50.0)],
        conversation_count=2,
        last_update_time=100.0,
        save_key=f"{tenant_id}:default/u1",
    )


def record(kind: str, item: Session) -> StorageRecord:
    return StorageRecord(
        tenant_id=item.app_name.split(":", 1)[0],
        kind=kind,
        record_id=f"{item.app_name}/{item.user_id}/{item.id}",
        payload=migration_module._session_payload(item),
    )


async def test_tenant_data_migrator_merge_verifies_source_subset():
    source = LocalMigrationBackend([
        StorageRecord(tenant_id="tenant_a", kind="session", record_id="s1", payload={"v": 1}),
        StorageRecord(tenant_id="tenant_a", kind="memory", record_id="m1", payload={"v": 2}),
    ])
    target = LocalMigrationBackend([
        StorageRecord(tenant_id="tenant_a", kind="session", record_id="extra", payload={"v": 9}),
    ])
    report = await TenantDataMigrator(source, target, batch_size=1).migrate("tenant_a", ["session", "memory"])
    assert report.verified is True
    assert report.copied_by_kind == {"session": 1, "memory": 1}
    with pytest.raises(ValueError, match="positive"):
        TenantDataMigrator(source, target, batch_size=0)


async def test_mysql_adapter_round_trip_session_and_memory(tmp_path):
    db_url = f"sqlite+pysqlite:///{tmp_path / 'migration.db'}"
    tenant = Tenant(
        tenant_id="tenant_a",
        name="A",
        model=ModelEndpoint(model_name="m"),
        storage_config=StorageBackendConfig(mysql_url=db_url, redis_url="redis://unused"),
    )
    adapter = TenantBackendMigrationAdapter(tenant, "mysql")
    source_session = session()
    await adapter.upsert(record("session", source_session))
    await adapter.upsert(record("memory", source_session))

    sessions, next_cursor = await adapter.scan("tenant_a", "session", None, 1)
    memories, _ = await adapter.scan("tenant_a", "memory", None, 10)
    assert next_cursor is None
    assert sessions[0].payload["state"] == source_session.state
    assert sessions[0].payload["events"][0]["id"] == "e1"
    assert memories[0].payload["events"][0]["id"] == "e1"

    # Replacement removes events no longer present in the source memory record.
    replacement = session()
    replacement.events = [event("e2", 200.0)]
    await adapter.upsert(record("memory", replacement))
    memories, _ = await adapter.scan("tenant_a", "memory", None, 10)
    assert [item["id"] for item in memories[0].payload["events"]] == ["e2"]

    with pytest.raises(ValueError, match="scope mismatch"):
        await adapter.scan("other", "session", None, 10)
    with pytest.raises(ValueError, match="unsupported migration kind"):
        await adapter.scan("tenant_a", "artifact", None, 10)
    with pytest.raises(ValueError, match="outside tenant"):
        await adapter.upsert(StorageRecord(tenant_id="other", kind="session", record_id="x", payload={}))
    await adapter.close()


class FakeSessionService:

    def __init__(self, *args, **kwargs):
        self.sessions = {}
        self.closed = False

    async def list_sessions(self, app_name, user_id=None):
        items = [
            item for item in self.sessions.values()
            if item.app_name == app_name and (user_id is None or item.user_id == user_id)
        ]
        return SimpleNamespace(sessions=items)

    async def get_session(self, app_name, user_id, session_id):
        return self.sessions.get((app_name, user_id, session_id))

    async def create_session(self, app_name, user_id, session_id, state=None):
        item = Session(
            app_name=app_name,
            user_id=user_id,
            id=session_id,
            state=state or {},
            save_key=f"{app_name}/{user_id}",
        )
        self.sessions[(app_name, user_id, session_id)] = item
        return item

    async def update_session(self, item):
        self.sessions[(item.app_name, item.user_id, item.id)] = item

    async def close(self):
        self.closed = True


class FakeMemoryService:

    def __init__(self, *args, **kwargs):
        self.stored = []
        self.closed = False

    async def store_session(self, item):
        self.stored.append(item)

    async def close(self):
        self.closed = True


class FakeAsyncRedis:

    def __init__(self):
        self.strings = {}
        self.lists = {}
        self.closed = False

    async def scan_iter(self, match):
        prefix = match.removesuffix("*")
        for key in sorted([*self.strings, *self.lists]):
            if key.startswith(prefix):
                yield key

    async def get(self, key):
        return self.strings.get(key)

    async def lrange(self, key, start, end):
        return self.lists.get(key, [])

    async def aclose(self):
        self.closed = True


async def test_redis_adapter_scans_and_upserts(monkeypatch):
    fake_redis = FakeAsyncRedis()
    monkeypatch.setattr(migration_module, "RedisSessionService", FakeSessionService)
    monkeypatch.setattr(migration_module, "RedisMemoryService", FakeMemoryService)
    monkeypatch.setattr(migration_module.async_redis, "from_url", lambda *args, **kwargs: fake_redis)
    tenant = Tenant(
        tenant_id="tenant_a",
        name="A",
        model=ModelEndpoint(model_name="m"),
        storage_config=StorageBackendConfig(redis_url="redis://fake", mysql_url="mysql+pymysql://unused"),
    )
    adapter = TenantBackendMigrationAdapter(tenant, "redis")
    source = session()
    adapter._session_service.sessions[(source.app_name, source.user_id, source.id)] = source
    fake_redis.strings["session:tenant_a:default:u1:s1"] = source.model_dump_json()
    fake_redis.lists["memory:tenant_a:default/u1:s1"] = [event().model_dump_json()]

    sessions, _ = await adapter.scan("tenant_a", "session", None, 10)
    memories, _ = await adapter.scan("tenant_a", "memory", None, 10)
    assert sessions[0].record_id.endswith("/s1")
    assert memories[0].payload["events"][0]["id"] == "e1"

    await adapter.upsert(record("session", session(session_id="s2")))
    await adapter.upsert(record("memory", session(session_id="s2")))
    assert len(adapter._session_service.sessions) == 2
    assert adapter._memory_service.stored[0].id == "s2"
    await adapter.close()
    assert fake_redis.closed is True


def test_adapter_rejects_bad_backend_and_missing_urls():
    tenant = Tenant(tenant_id="tenant_a", name="A", model=ModelEndpoint(model_name="m"))
    with pytest.raises(ValueError, match="unsupported migration backend"):
        TenantBackendMigrationAdapter(tenant, "object-store")
    with pytest.raises(ValueError, match="Redis URL"):
        TenantBackendMigrationAdapter(tenant, "redis")
    with pytest.raises(ValueError, match="MySQL URL"):
        TenantBackendMigrationAdapter(tenant, "mysql")
