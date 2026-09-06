from __future__ import annotations

from copy import deepcopy

import pytest

from trpc_service.tenant.models import ChannelBinding, default_demo_config
from trpc_service.tenant.repository import (
    InMemoryTenantRepository,
    PostgresTenantRepository,
    SQLiteTenantRepository,
    TenantNotFound,
    TenantRepositoryConflict,
    TenantRepositoryError,
    _is_postgres_connection_error,
    _retry_postgres_once,
    _sync_demo_defaults,
    persistent_demo_repository,
)


def tenant(tenant_id: str = "tenant-a"):
    config = default_demo_config()
    config.tenant_id = tenant_id
    config.channel_bindings = [
        ChannelBinding(tenant_id, f"web:{tenant_id}", "web", f"account-{tenant_id}", "app_support")
    ]
    return config


@pytest.mark.parametrize("repository_factory", [InMemoryTenantRepository, SQLiteTenantRepository])
def test_versioned_repository_lifecycle_and_isolation(repository_factory, tmp_path):
    repository = (
        repository_factory()
        if repository_factory is InMemoryTenantRepository
        else repository_factory(tmp_path / "tenants.sqlite3")
    )
    first = repository.create(tenant())
    assert first.config_version == 1
    with pytest.raises(TenantRepositoryError):
        repository.create(tenant())
    saved = repository.save_version(repository.get("tenant-a"), expected_version=1)
    assert saved.config_version == 2
    assert repository.get("tenant-a", version=1).config_version == 1
    with pytest.raises(TenantRepositoryConflict):
        repository.save_version(tenant(), expected_version=0)
    assert repository.publish("tenant-a", 2, expected_version=1).config_version == 2
    with pytest.raises(TenantRepositoryConflict):
        repository.publish("tenant-a", 1, expected_version=1)
    with pytest.raises(TenantNotFound):
        repository.get("tenant-a", version=99)
    with pytest.raises(TenantNotFound):
        repository.publish("tenant-a", 99)
    with pytest.raises(TenantNotFound):
        repository.save_version(tenant("missing"))
    changed = deepcopy(repository.get("tenant-a"))
    changed.apps[0].prompt = "new prompt"
    active = repository.save_and_publish(changed, expected_version=2)
    assert active.config_version == 3
    assert repository.get("tenant-a").apps[0].prompt == "new prompt"
    assert repository.rollback("tenant-a", 1, expected_version=3).config_version == 1
    assert repository.find_binding("WEB", "account-tenant-a").tenant_id == "tenant-a"
    with pytest.raises(TenantNotFound):
        repository.find_binding("web", "missing")
    assert [item.tenant_id for item in repository.all_active()] == ["tenant-a"]
    close = getattr(repository, "close", None)
    if close:
        close()


def test_sqlite_repository_reopens_and_detects_ambiguous_active_binding(tmp_path):
    path = tmp_path / "reopen.sqlite3"
    repository = SQLiteTenantRepository(path)
    repository.create(tenant("tenant-a"))
    second = tenant("tenant-b")
    second.channel_bindings[0].account_id = "account-tenant-a"
    repository.create(second)
    with pytest.raises(TenantRepositoryError, match="ambiguous"):
        repository.find_binding("web", "account-tenant-a")
    repository.close()

    reopened = SQLiteTenantRepository(path)
    assert {config.tenant_id for config in reopened.all_active()} == {"tenant-a", "tenant-b"}
    with pytest.raises(TenantNotFound):
        reopened._check_expected_version("missing", 1)
    with pytest.raises(TenantRepositoryConflict):
        reopened._check_expected_version("tenant-a", 99)
    reopened.close()


def test_repository_abstract_default_save_and_publish_contract():
    class Lightweight(InMemoryTenantRepository):
        def save_and_publish(self, config, expected_version=None):
            return super(InMemoryTenantRepository, self).save_and_publish(config, expected_version)

    repository = Lightweight()
    repository.create(tenant())
    config = repository.get("tenant-a")
    # Exercise the base implementation used by lightweight third-party repositories.
    base = super(InMemoryTenantRepository, repository).save_and_publish(config, expected_version=1)
    assert base.config_version == 2


def test_persistent_demo_repository_seeds_and_syncs_defaults(tmp_path, monkeypatch):
    path = tmp_path / "demo.sqlite3"
    repository = persistent_demo_repository(path)
    assert repository.get("tenant_demo").tenant_id == "tenant_demo"
    initial_version = repository.get("tenant_demo").config_version

    # Startup is idempotent once the shipped defaults are already present.
    repository.close()
    reopened = persistent_demo_repository(path)
    assert reopened.get("tenant_demo").config_version == initial_version

    current = reopened.get("tenant_demo")
    current.apps[0].model_config.provider = ""
    current.apps[0].model_config.model = ""
    current.channel_bindings = []
    reopened.save_and_publish(current, expected_version=initial_version)
    changed_version = reopened.get("tenant_demo").config_version
    _sync_demo_defaults(reopened, reopened.get("tenant_demo"))
    synced = reopened.get("tenant_demo")
    assert synced.config_version == changed_version + 1
    assert synced.apps[0].model_config.provider
    assert synced.channel_bindings

    # Legacy bindings are removed unless explicitly retained, and storage sync
    # is opt-in so a runtime endpoint is never changed accidentally at startup.
    legacy = reopened.get("tenant_demo")
    legacy.channel_bindings.append(
        ChannelBinding("tenant_demo", "legacy", "wecom", "legacy-account", "app_support")
    )
    legacy.storage_profile.redis_url = "redis://runtime.local"
    reopened.save_and_publish(legacy)
    monkeypatch.setenv("SYNC_DEMO_DEFAULT_STORAGE", "0")
    _sync_demo_defaults(reopened, reopened.get("tenant_demo"))
    assert all(binding.channel != "wecom" for binding in reopened.get("tenant_demo").channel_bindings)
    assert reopened.get("tenant_demo").storage_profile.redis_url == "redis://runtime.local"
    reopened.close()


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split()).lower()
        self.connection.statements.append((normalized, params))
        if "select 1 from tenant_config" in normalized:
            self.result = (1,) if params[0] in self.connection.configs else None
        elif "select active_version from tenant_active" in normalized:
            self.result = (self.connection.active[params[0]],) if params[0] in self.connection.active else None
        elif "coalesce(max(version),0)+1" in normalized:
            tenant_id = params[0]
            self.result = (max((version for tid, version in self.connection.configs if tid == tenant_id), default=0) + 1,)
        elif normalized.startswith("select config_json from tenant_config"):
            tenant_id, version = params
            value = self.connection.payloads.get((tenant_id, int(version)))
            self.result = (value,) if value is not None else None
        elif normalized.startswith("select c.config_json"):
            self.result = [
                (payload,)
                for (tenant_id, version), payload in sorted(self.connection.payloads.items())
                if self.connection.active.get(tenant_id) == version
            ]
        elif normalized.startswith("insert into tenant_config"):
            tenant_id, version, payload, created_at, updated_at = params
            self.connection.configs.add((tenant_id, int(version)))
            self.connection.payloads[(tenant_id, int(version))] = payload
            self.result = None
        elif normalized.startswith("insert into tenant_active"):
            self.connection.active[params[0]] = int(params[1])
            self.result = None
        else:
            self.result = None
        return self

    def fetchone(self):
        return self.result[0] if isinstance(self.result, list) and self.result else self.result

    def fetchall(self):
        return self.result if isinstance(self.result, list) else ([] if self.result is None else [self.result])


class _FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.connection.transactions += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    closed = False

    def __init__(self):
        self.autocommit = False
        self.configs = set()
        self.payloads = {}
        self.active = {}
        self.statements = []
        self.transactions = 0

    def cursor(self):
        return _FakeCursor(self)

    def transaction(self):
        return _FakeTransaction(self)

    def close(self):
        self.closed = True


class _FakePsycopg:
    def __init__(self):
        self.connections = []

    def connect(self, dsn):
        connection = _FakeConnection()
        self.connections.append(connection)
        return connection


def _postgres_repository():
    repository = PostgresTenantRepository.__new__(PostgresTenantRepository)
    repository._psycopg = _FakePsycopg()
    repository._dsn = "postgresql://test"
    from threading import RLock
    repository._lock = RLock()
    repository._connection = None
    repository._connect()
    return repository


def test_postgres_repository_contract_and_retry_paths():
    repository = _postgres_repository()
    first = repository.create(tenant())
    assert first.config_version == 1
    assert repository.get("tenant-a").config_version == 1
    saved = repository.save_version(repository.get("tenant-a"), expected_version=1)
    assert saved.config_version == 2
    assert repository.publish("tenant-a", 2, expected_version=1).config_version == 2
    active = repository.save_and_publish(repository.get("tenant-a"), expected_version=2)
    assert active.config_version == 3
    assert repository.all_active()[0].tenant_id == "tenant-a"
    assert repository.find_binding("web", "account-tenant-a").tenant_id == "tenant-a"
    with pytest.raises(TenantRepositoryConflict):
        repository.save_version(repository.get("tenant-a"), expected_version=99)
    with pytest.raises(TenantNotFound):
        repository.get("tenant-a", version=99)
    with pytest.raises(TenantNotFound):
        repository.publish("tenant-a", 99)
    with pytest.raises(TenantNotFound):
        repository.find_binding("web", "missing")
    repository.close()

    class OperationalError(Exception):
        __module__ = "psycopg.errors"

    assert _is_postgres_connection_error(OperationalError("connection is closed"))
    assert not _is_postgres_connection_error(ValueError("connection is closed"))
    class DatabaseError(Exception):
        __module__ = "psycopg.errors"

    assert not _is_postgres_connection_error(DatabaseError("constraint violation"))


def test_postgres_retry_reconnects_only_connection_failures():
    class OperationalError(Exception):
        __module__ = "psycopg.errors"

    class Service:
        def __init__(self):
            self._lock = __import__("threading").RLock()
            self._connection = _FakeConnection()
            self.connects = 0
            self.calls = 0

        def _connect(self):
            self.connects += 1
            self._connection = _FakeConnection()

        @_retry_postgres_once
        def run(self):
            self.calls += 1
            if self.calls == 1:
                raise OperationalError("terminating connection due to administrator command")
            return "ok"

    service = Service()
    assert service.run() == "ok"
    assert service.connects == 1
    assert service._connection.closed is False

    class BrokenService(Service):
        @_retry_postgres_once
        def run(self):
            raise RuntimeError("bad input")

    with pytest.raises(RuntimeError, match="bad input"):
        BrokenService().run()
