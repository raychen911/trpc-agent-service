# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Persistence, encryption, optimistic locking and cache integration tests."""

from __future__ import annotations

import json
import threading

import pytest

from trpc_service.tenant import ConfigOutboxPublisher
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import MySqlTenantRepository
from trpc_service.tenant import RedisTenantConfigCache
from trpc_service.tenant import StorageBackendConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigCodec
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import WeComChannelConfig
from trpc_service.tenant import build_tenant_config_manager
from trpc_service.tenant._persistence import mysql_sync_url


def make_tenant(tenant_id: str = "tenant_a", name: str = "A") -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        name=name,
        model=ModelEndpoint(model_name="deepseek-chat"),
        channel_configs={
            "wecom": WeComChannelConfig(
                token="callback-secret",
                aes_key="aes-secret",
                corp_id="corp",
                agent_id="1",
            ),
        },
        storage_config=StorageBackendConfig(
            redis_url="redis://:redis-password@localhost/0",
            mysql_url="mysql+pymysql://agent:mysql-password@localhost/app",
        ),
    )


def sqlite_url(tmp_path) -> str:
    return f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}"


def test_codec_encrypts_every_secret_and_round_trips():
    tenant = make_tenant()
    public, encrypted = TenantConfigCodec("a-long-key").encode(tenant)
    serialized = json.dumps(public)
    assert "callback-secret" not in serialized
    assert "redis-password" not in serialized
    assert "mysql-password" not in serialized
    assert encrypted and "callback-secret" not in encrypted

    restored = TenantConfigCodec("a-long-key").decode(public, encrypted)
    assert restored.channel_configs["wecom"].token.get_secret_value() == "callback-secret"
    assert "redis-password" in restored.storage_config.redis_url.get_secret_value()


def test_codec_requires_key_for_secret_config():
    codec = TenantConfigCodec(None)
    with pytest.raises(ValueError, match="ENCRYPTION_KEY"):
        codec.encode(make_tenant())
    with pytest.raises(ValueError, match="decrypt"):
        codec.decode(make_tenant().model_dump(mode="json"), "encrypted")


def test_codec_reports_stable_error_for_wrong_encryption_key():
    public, encrypted = TenantConfigCodec("original-key").encode(make_tenant())

    with pytest.raises(ValueError, match="does not match the key used"):
        TenantConfigCodec("wrong-key").decode(public, encrypted)


def test_mysql_url_normalization():
    assert mysql_sync_url("mysql+aiomysql://u:p@db/x") == "mysql+pymysql://u:p@db/x"
    assert mysql_sync_url("mysql+asyncmy://u:p@db/x") == "mysql+pymysql://u:p@db/x"
    assert mysql_sync_url("sqlite:///:memory:") == "sqlite:///:memory:"


def test_repository_crud_history_outbox_and_conflict(tmp_path):
    repo = MySqlTenantRepository(sqlite_url(tmp_path), "key")
    tenant = make_tenant()
    assert repo.create(tenant, "admin", "create") == 1
    with pytest.raises(ValueError, match="already registered"):
        repo.create(tenant, "admin", "duplicate")

    loaded, version = repo.get("tenant_a")
    assert loaded.name == "A" and version == 1
    assert repo.get("missing") is None
    assert [item[0].tenant_id for item in repo.list()] == ["tenant_a"]

    loaded.name = "B"
    assert repo.update(loaded, 1, "admin", "rename") == 2
    with pytest.raises(ValueError, match="version conflict"):
        repo.update(loaded, 1, "admin", "stale update")
    history = repo.history("tenant_a")
    assert [item.version for item in history] == [1, 2]
    assert history[1].created_by == "admin"

    pending = repo.pending_outbox(limit=10)
    assert [event["event_type"] for event in pending] == ["tenant.created", "tenant.updated"]
    repo.mark_outbox_published(pending[0]["event_id"])
    assert len(repo.pending_outbox()) == 1

    with pytest.raises(ValueError, match="not found or version conflict"):
        repo.delete("tenant_a", 1)
    repo.delete("tenant_a", 2)
    assert repo.get("tenant_a") is None
    assert repo.pending_outbox()[-1]["event_type"] == "tenant.deleted"
    assert repo.create(make_tenant(name="recreated"), "admin", "recreate") == 3
    assert [item.version for item in repo.history("tenant_a")] == [1, 2, 3]
    repo.close()


class FakePipeline:

    def __init__(self, client):
        self.client = client
        self.operations = []

    def set(self, key, value, ex=None):
        self.operations.append((key, value, ex))
        return self

    def execute(self):
        for key, value, _ in self.operations:
            self.client.values[key] = str(value)
        return [True] * len(self.operations)


class FakeRedis:

    def __init__(self):
        self.values = {}
        self.messages = []
        self.closed = False
        self.inbound = []

    def get(self, key):
        return self.values.get(key)

    def pipeline(self, transaction=True):
        assert transaction is True
        return FakePipeline(self)

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)

    def publish(self, channel, payload):
        self.messages.append((channel, payload))

    def pubsub(self, ignore_subscribe_messages=True):
        assert ignore_subscribe_messages is True
        client = self

        class PubSub:

            def subscribe(self, channel):
                self.channel = channel

            def get_message(self, timeout=1.0):
                return client.inbound.pop(0) if client.inbound else None

            def close(self):
                self.closed = True

        return PubSub()

    def close(self):
        self.closed = True


def test_redis_cache_and_outbox_publisher(tmp_path, monkeypatch):
    client = FakeRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *args, **kwargs: client)
    repo = MySqlTenantRepository(sqlite_url(tmp_path), "key")
    tenant = make_tenant()
    repo.create(tenant, "admin", "create")
    cache = RedisTenantConfigCache("redis://unused", repo.codec, ttl_seconds=30)

    assert cache.get("tenant_a") is None
    cache.set(tenant, 1)
    restored, version = cache.get("tenant_a")
    assert restored.name == "A" and version == 1
    assert "callback-secret" not in client.values[cache._key("tenant_a")]

    publisher = ConfigOutboxPublisher(repo, cache)
    assert publisher.publish_pending() == 1
    assert json.loads(client.messages[0][1])["event_type"] == "tenant.created"
    assert publisher.publish_pending() == 0
    cache.invalidate("tenant_a")
    assert cache.get("tenant_a") is None
    cache.close()
    assert client.closed
    repo.close()


def test_redis_listener_dispatches_and_is_idempotent(monkeypatch):
    client = FakeRedis()
    client.inbound.append({
        "type": "message",
        "data": json.dumps({
            "tenant_id": "tenant_a",
            "version": 7,
            "event_type": "tenant.updated",
        }),
    })
    monkeypatch.setattr("redis.Redis.from_url", lambda *args, **kwargs: client)
    cache = RedisTenantConfigCache("redis://unused", TenantConfigCodec("key"))
    received = []
    ready = threading.Event()

    def callback(*event):
        received.append(event)
        ready.set()

    cache.start_listener(callback)
    cache.start_listener(callback)
    assert ready.wait(timeout=2)
    assert received == [("tenant_a", 7, "tenant.updated")]
    cache.close()


def test_persistent_manager_survives_restart_and_rolls_back(tmp_path):
    url = sqlite_url(tmp_path)
    manager = TenantConfigManager(MySqlTenantRepository(url, "key"), listen_for_changes=False)
    manager.register(make_tenant(name="v1"), by="owner")
    updated = manager.get("tenant_a")
    updated.name = "v2"
    manager.update(updated, reason="rename")
    manager.close()

    restarted = TenantConfigManager(MySqlTenantRepository(url, "key"), listen_for_changes=False)
    assert restarted.get("tenant_a").name == "v2"
    assert [version.version for version in restarted.history("tenant_a")] == [1, 2]
    restored = restarted.rollback("tenant_a", 1, by="owner")
    assert restored.name == "v1"
    assert restarted.history("tenant_a")[-1].rolled_back_to == 1
    restarted.close()


def test_two_managers_reject_stale_concurrent_update(tmp_path):
    url = sqlite_url(tmp_path)
    first = TenantConfigManager(MySqlTenantRepository(url, "key"), listen_for_changes=False)
    first.register(make_tenant())
    second = TenantConfigManager(MySqlTenantRepository(url, "key"), listen_for_changes=False)
    one = first.get("tenant_a")
    two = second.get("tenant_a")
    one.name = "first"
    two.name = "second"
    first.update(one)
    with pytest.raises(ValueError, match="version conflict"):
        second.update(two)
    first.close()
    second.close()


def test_factory_falls_back_to_in_memory_and_manager_remote_refresh(tmp_path):
    memory = build_tenant_config_manager()
    memory.register(Tenant(tenant_id="memory", name="Memory", model=ModelEndpoint(model_name="m")))
    assert memory.get("memory").name == "Memory"
    memory.reload()
    memory.close()

    url = sqlite_url(tmp_path)
    repo = MySqlTenantRepository(url, "key")
    manager = TenantConfigManager(repo, listen_for_changes=False)
    manager.register(make_tenant())
    external = repo.get("tenant_a")[0]
    external.name = "remote"
    repo.update(external, 1, "other-node", "remote update")
    events = []
    manager.subscribe(lambda tenant_id, tenant: events.append((tenant_id, tenant.name if tenant else None)))
    manager._on_remote_change("tenant_a", 2, "tenant.updated")
    assert manager.get("tenant_a").name == "remote"
    assert events == [("tenant_a", "remote")]
    manager._on_remote_change("tenant_a", 2, "tenant.updated")
    manager._on_remote_change("missing", 1, "tenant.updated")
    repo.delete("tenant_a", 2)
    manager._on_remote_change("tenant_a", 3, "tenant.deleted")
    assert manager.get("tenant_a") is None
    manager.close()


def test_factory_builds_durable_manager_with_redis(tmp_path, monkeypatch):
    client = FakeRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *args, **kwargs: client)
    manager = build_tenant_config_manager(
        mysql_url=sqlite_url(tmp_path),
        redis_url="redis://unused",
        encryption_key="key",
        listen_for_changes=False,
    )
    manager.register(make_tenant())
    # Force an L1 miss: the next read is restored from the encrypted Redis L2.
    manager._tenants.clear()
    assert manager.get("tenant_a").name == "A"
    updated = manager.get("tenant_a")
    updated.name = "updated"
    manager.update(updated)
    manager.rollback("tenant_a", 1)
    manager.delete("tenant_a")
    assert manager.get("tenant_a") is None
    manager.close()
