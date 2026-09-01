"""Tenant configuration repositories."""

from __future__ import annotations

from abc import ABC, abstractmethod
import json
import sqlite3
import os
from copy import deepcopy
from functools import wraps
from pathlib import Path
from threading import RLock

from trpc_service.tenant.models import ChannelBinding, TenantConfig, default_demo_config, utc_now
from trpc_service.storage.durable import _json_object
from trpc_service.storage.locking import postgres_advisory_lock
from trpc_service.storage.postgres_rls import postgres_schema_auto_create, validate_runtime_role


class TenantRepositoryError(RuntimeError):
    pass


class TenantNotFound(TenantRepositoryError):
    pass


class TenantRepositoryConflict(TenantRepositoryError):
    """Raised when an optimistic-concurrency version is no longer current."""


def _is_postgres_connection_error(exc: Exception) -> bool:
    module = exc.__class__.__module__
    name = exc.__class__.__name__
    text = str(exc).lower()
    return module.startswith("psycopg") and (
        name in {"OperationalError", "InterfaceError", "AdminShutdown", "ConnectionTimeout"}
        or "connection is closed" in text
        or "terminating connection" in text
    )


def _retry_postgres_once(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            if not _is_postgres_connection_error(exc):
                raise
            with self._lock:
                if self._connection is not None and not self._connection.closed:
                    self._connection.close()
                self._connect()
            return method(self, *args, **kwargs)

    return wrapper


class TenantRepository(ABC):
    """Abstract contract for tenant configuration persistence.

    Runtime code should inject one of the concrete implementations below, such
    as InMemoryTenantRepository, SQLiteTenantRepository, or
    PostgresTenantRepository.
    """

    @abstractmethod
    def create(self, config: TenantConfig) -> TenantConfig:
        raise NotImplementedError

    @abstractmethod
    def get(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        raise NotImplementedError

    @abstractmethod
    def save_version(self, config: TenantConfig, expected_version: int | None = None) -> TenantConfig:
        raise NotImplementedError

    @abstractmethod
    def publish(
        self,
        tenant_id: str,
        version: int,
        expected_version: int | None = None,
    ) -> TenantConfig:
        raise NotImplementedError

    def rollback(
        self,
        tenant_id: str,
        version: int,
        expected_version: int | None = None,
    ) -> TenantConfig:
        return self.publish(tenant_id, version, expected_version=expected_version)

    def save_and_publish(
        self,
        config: TenantConfig,
        expected_version: int | None = None,
    ) -> TenantConfig:
        """Persist and activate one immutable version.

        Concrete repositories override this method to make the version check,
        insert, and active-pointer update one transaction. The default keeps
        compatibility for lightweight third-party repositories.
        """

        saved = self.save_version(config, expected_version=expected_version)
        return self.publish(saved.tenant_id, saved.config_version, expected_version=expected_version)

    @abstractmethod
    def find_binding(self, channel: str, account_id: str) -> ChannelBinding:
        raise NotImplementedError


class InMemoryTenantRepository(TenantRepository):
    """Versioned in-memory tenant repository.

    It is intended for local development, tests, and the default demo. Versions
    are immutable snapshots so publish and rollback can switch active config
    without mutating historical tenant configuration.
    """

    def __init__(self) -> None:
        self._versions: dict[str, dict[int, TenantConfig]] = {}
        self._active_versions: dict[str, int] = {}
        self._lock = RLock()

    def create(self, config: TenantConfig) -> TenantConfig:
        with self._lock:
            if config.tenant_id in self._versions:
                raise TenantRepositoryError(f"tenant already exists: {config.tenant_id}")
            config = deepcopy(config)
            config.config_version = max(1, config.config_version)
            config.created_at = utc_now()
            config.updated_at = config.created_at
            self._versions[config.tenant_id] = {config.config_version: config}
            self._active_versions[config.tenant_id] = config.config_version
            return deepcopy(config)

    def get(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        with self._lock:
            if tenant_id not in self._versions:
                raise TenantNotFound(tenant_id)
            version = version or self._active_versions[tenant_id]
            try:
                return deepcopy(self._versions[tenant_id][version])
            except KeyError as exc:
                raise TenantNotFound(f"{tenant_id}@v{version}") from exc

    def save_version(self, config: TenantConfig, expected_version: int | None = None) -> TenantConfig:
        with self._lock:
            if config.tenant_id not in self._versions:
                raise TenantNotFound(config.tenant_id)
            self._check_expected_version(config.tenant_id, expected_version)
            versions = self._versions[config.tenant_id]
            next_version = max(versions) + 1
            snapshot = deepcopy(config)
            snapshot.config_version = next_version
            snapshot.updated_at = utc_now()
            versions[next_version] = snapshot
            return deepcopy(snapshot)

    def publish(
        self,
        tenant_id: str,
        version: int,
        expected_version: int | None = None,
    ) -> TenantConfig:
        with self._lock:
            if tenant_id not in self._versions or version not in self._versions[tenant_id]:
                raise TenantNotFound(f"{tenant_id}@v{version}")
            self._check_expected_version(tenant_id, expected_version)
            self._active_versions[tenant_id] = version
            return deepcopy(self._versions[tenant_id][version])

    def save_and_publish(
        self,
        config: TenantConfig,
        expected_version: int | None = None,
    ) -> TenantConfig:
        with self._lock:
            if config.tenant_id not in self._versions:
                raise TenantNotFound(config.tenant_id)
            self._check_expected_version(config.tenant_id, expected_version)
            versions = self._versions[config.tenant_id]
            snapshot = deepcopy(config)
            snapshot.config_version = max(versions) + 1
            snapshot.updated_at = utc_now()
            versions[snapshot.config_version] = snapshot
            self._active_versions[config.tenant_id] = snapshot.config_version
            return deepcopy(snapshot)

    def _check_expected_version(self, tenant_id: str, expected_version: int | None) -> None:
        if expected_version is None:
            return
        current_version = self._active_versions.get(tenant_id)
        if current_version is None:
            raise TenantNotFound(tenant_id)
        if current_version != int(expected_version):
            raise TenantRepositoryConflict(
                f"tenant config changed: expected v{expected_version}, current v{current_version}"
            )

    def find_binding(self, channel: str, account_id: str) -> ChannelBinding:
        channel = channel.lower()
        with self._lock:
            matches: list[ChannelBinding] = []
            for tenant_id, active_version in self._active_versions.items():
                config = self._versions[tenant_id][active_version]
                try:
                    matches.append(deepcopy(config.channel_binding(channel, account_id)))
                except KeyError:
                    continue
            if len(matches) > 1:
                raise TenantRepositoryError(f"ambiguous channel binding: {channel}/{account_id}")
            if matches:
                return matches[0]
        raise TenantNotFound(f"channel binding not found: {channel}/{account_id}")

    def all_active(self) -> list[TenantConfig]:
        with self._lock:
            return [
                deepcopy(self._versions[tenant_id][version]) for tenant_id, version in self._active_versions.items()
            ]


def demo_repository() -> InMemoryTenantRepository:
    repository = InMemoryTenantRepository()
    repository.create(default_demo_config())
    return repository


def persistent_demo_repository(path: str | Path = "data/tenant_config.sqlite3") -> "SQLiteTenantRepository":
    """Open the durable repository and seed the demo tenant once."""
    dsn = os.getenv("TENANT_DB_DSN", "").strip()
    repository = PostgresTenantRepository(dsn) if dsn else SQLiteTenantRepository(path)
    try:
        current = repository.get("tenant_demo")
    except TenantNotFound:
        # Multiple Gateway/Worker processes can initialize against the same
        # database at once. One process may win the insert while the others
        # observe a unique-key error; re-read the row so startup remains
        # idempotent without hiding real connection/schema failures.
        try:
            repository.create(default_demo_config())
        except Exception as exc:
            try:
                current = repository.get("tenant_demo")
            except TenantNotFound:
                raise exc
            else:
                _sync_demo_defaults(repository, current)
    else:
        _sync_demo_defaults(repository, current)
    return repository


def _sync_demo_defaults(repository: TenantRepository, current: TenantConfig) -> None:
    """Keep the built-in demo tenant aligned with the latest shipped defaults."""
    default = default_demo_config()
    changed = False
    default_apps = {app.agent_app_id: app for app in default.apps}
    for app in current.apps:
        default_app = default_apps.get(app.agent_app_id)
        if default_app is None:
            continue
        if not app.model_config.provider and default_app.model_config.provider:
            app.model_config.provider = default_app.model_config.provider
            changed = True
        if not app.model_config.model and default_app.model_config.model:
            app.model_config.model = default_app.model_config.model
            changed = True
        for field_name in ("base_url", "api_key_env", "api_key_ref", "wire_api"):
            current_value = getattr(app.model_config, field_name)
            default_value = getattr(default_app.model_config, field_name)
            if default_value and current_value != default_value:
                setattr(app.model_config, field_name, default_value)
                changed = True
        for field_name in ("timeout_ms", "max_output_tokens", "cost_per_1k_tokens"):
            current_value = getattr(app.model_config, field_name)
            default_value = getattr(default_app.model_config, field_name)
            if current_value != default_value:
                setattr(app.model_config, field_name, default_value)
                changed = True

    current_bindings = {(binding.channel, binding.account_id) for binding in current.channel_bindings}
    missing = [
        binding for binding in default.channel_bindings if (binding.channel, binding.account_id) not in current_bindings
    ]
    sync_storage = os.getenv("SYNC_DEMO_DEFAULT_STORAGE", "0") == "1"
    storage_changed = sync_storage and (current.storage_profile.to_dict() != default.storage_profile.to_dict())
    if not missing and not changed and not storage_changed:
        return
    merged = deepcopy(current)
    if missing:
        merged.channel_bindings.extend(deepcopy(binding) for binding in missing)
    if storage_changed:
        merged.storage_profile = deepcopy(default.storage_profile)
    merged.updated_at = utc_now()
    saved = repository.save_version(merged)
    repository.publish(saved.tenant_id, saved.config_version)


class SQLiteTenantRepository(TenantRepository):
    """Versioned tenant repository persisted in SQLite."""

    def __init__(self, path: str | Path = "data/tenant_config.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tenant_config (
              tenant_id TEXT NOT NULL,
              version INTEGER NOT NULL,
              config_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, version)
            );

            CREATE TABLE IF NOT EXISTS tenant_active (
              tenant_id TEXT PRIMARY KEY,
              active_version INTEGER NOT NULL
            );
            """
        )
        self._conn.commit()

    def create(self, config: TenantConfig) -> TenantConfig:
        with self._lock, self._conn:
            if self._tenant_exists(config.tenant_id):
                raise TenantRepositoryError(f"tenant already exists: {config.tenant_id}")
            snapshot = deepcopy(config)
            snapshot.config_version = max(1, snapshot.config_version)
            snapshot.created_at = utc_now()
            snapshot.updated_at = snapshot.created_at
            self._insert_version(snapshot)
            self._conn.execute(
                "INSERT INTO tenant_active (tenant_id, active_version) VALUES (?, ?)",
                (snapshot.tenant_id, snapshot.config_version),
            )
            return deepcopy(snapshot)

    def get(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        with self._lock:
            if version is None:
                active = self._conn.execute(
                    "SELECT active_version FROM tenant_active WHERE tenant_id = ?",
                    (tenant_id,),
                ).fetchone()
                if active is None:
                    raise TenantNotFound(tenant_id)
                version = int(active["active_version"])
            row = self._conn.execute(
                "SELECT config_json FROM tenant_config WHERE tenant_id = ? AND version = ?",
                (tenant_id, version),
            ).fetchone()
            if row is None:
                raise TenantNotFound(f"{tenant_id}@v{version}")
            return TenantConfig.from_dict(json.loads(row["config_json"]))

    def save_version(self, config: TenantConfig, expected_version: int | None = None) -> TenantConfig:
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            if not self._tenant_exists(config.tenant_id):
                raise TenantNotFound(config.tenant_id)
            self._check_expected_version(config.tenant_id, expected_version)
            row = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS max_version FROM tenant_config WHERE tenant_id = ?",
                (config.tenant_id,),
            ).fetchone()
            snapshot = deepcopy(config)
            snapshot.config_version = int(row["max_version"]) + 1
            snapshot.updated_at = utc_now()
            self._insert_version(snapshot)
            return deepcopy(snapshot)

    def publish(
        self,
        tenant_id: str,
        version: int,
        expected_version: int | None = None,
    ) -> TenantConfig:
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            self._check_expected_version(tenant_id, expected_version)
            config = self.get(tenant_id, version=version)
            self._conn.execute(
                """
                INSERT INTO tenant_active (tenant_id, active_version) VALUES (?, ?)
                ON CONFLICT(tenant_id) DO UPDATE SET active_version = excluded.active_version
                """,
                (tenant_id, version),
            )
            return config

    def save_and_publish(
        self,
        config: TenantConfig,
        expected_version: int | None = None,
    ) -> TenantConfig:
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            if not self._tenant_exists(config.tenant_id):
                raise TenantNotFound(config.tenant_id)
            self._check_expected_version(config.tenant_id, expected_version)
            row = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS max_version FROM tenant_config WHERE tenant_id = ?",
                (config.tenant_id,),
            ).fetchone()
            snapshot = deepcopy(config)
            snapshot.config_version = int(row["max_version"]) + 1
            snapshot.updated_at = utc_now()
            self._insert_version(snapshot)
            self._conn.execute(
                """
                INSERT INTO tenant_active (tenant_id, active_version) VALUES (?, ?)
                ON CONFLICT(tenant_id) DO UPDATE SET active_version = excluded.active_version
                """,
                (snapshot.tenant_id, snapshot.config_version),
            )
            return deepcopy(snapshot)

    def find_binding(self, channel: str, account_id: str) -> ChannelBinding:
        channel = channel.lower()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT c.config_json
                FROM tenant_active a
                JOIN tenant_config c
                  ON c.tenant_id = a.tenant_id AND c.version = a.active_version
                """
            ).fetchall()
            matches: list[ChannelBinding] = []
            for row in rows:
                config = TenantConfig.from_dict(json.loads(row["config_json"]))
                try:
                    matches.append(deepcopy(config.channel_binding(channel, account_id)))
                except KeyError:
                    continue
            if len(matches) > 1:
                raise TenantRepositoryError(f"ambiguous channel binding: {channel}/{account_id}")
            if matches:
                return matches[0]
        raise TenantNotFound(f"channel binding not found: {channel}/{account_id}")

    def all_active(self) -> list[TenantConfig]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT c.config_json
                FROM tenant_active a
                JOIN tenant_config c
                  ON c.tenant_id = a.tenant_id AND c.version = a.active_version
                ORDER BY c.tenant_id
                """
            ).fetchall()
            return [TenantConfig.from_dict(json.loads(row["config_json"])) for row in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _tenant_exists(self, tenant_id: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM tenant_config WHERE tenant_id = ? LIMIT 1", (tenant_id,)).fetchone()
        return row is not None

    def _check_expected_version(self, tenant_id: str, expected_version: int | None) -> None:
        if expected_version is None:
            return
        row = self._conn.execute(
            "SELECT active_version FROM tenant_active WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if row is None:
            raise TenantNotFound(tenant_id)
        current_version = int(row["active_version"])
        if current_version != int(expected_version):
            raise TenantRepositoryConflict(
                f"tenant config changed: expected v{expected_version}, current v{current_version}"
            )

    def _insert_version(self, config: TenantConfig) -> None:
        self._conn.execute(
            """
            INSERT INTO tenant_config (tenant_id, version, config_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                config.tenant_id,
                config.config_version,
                json.dumps(config.to_dict(), ensure_ascii=False, sort_keys=True),
                config.created_at.isoformat(),
                config.updated_at.isoformat(),
            ),
        )


class PostgresTenantRepository(TenantRepository):
    """Durable versioned repository backed by the Compose PostgreSQL service."""

    def __init__(self, dsn: str | None = None) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL tenant repository requires psycopg[binary]") from exc
        self._psycopg = psycopg
        self._dsn = dsn or os.getenv("POSTGRES_DSN", "")
        self._lock = RLock()
        self._connection = None
        self._connect()
        if postgres_schema_auto_create():
            with postgres_advisory_lock(self._conn, "trpc-agent-tenant-schema-v1"):
                with self._conn.cursor() as cur:
                    cur.execute(
                        (
                            "CREATE TABLE IF NOT EXISTS tenant_config ("
                            "tenant_id TEXT NOT NULL, version INTEGER NOT NULL, "
                            "config_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, "
                            "updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (tenant_id, version))"
                        )
                    )
                    cur.execute(
                        (
                            "CREATE TABLE IF NOT EXISTS tenant_active ("
                            "tenant_id TEXT PRIMARY KEY, active_version INTEGER NOT NULL)"
                        )
                    )
        else:
            validate_runtime_role(self, expected_role_env="POSTGRES_RLS_ADMIN_ROLE")

    @property
    def _conn(self):
        with self._lock:
            if self._connection is None or self._connection.closed:
                self._connect()
            return self._connection

    def _connect(self) -> None:
        connection = self._psycopg.connect(self._dsn)
        connection.autocommit = True
        self._connection = connection

    def create(self, config: TenantConfig) -> TenantConfig:
        snapshot = deepcopy(config)
        snapshot.config_version = max(1, snapshot.config_version)
        snapshot.created_at = utc_now()
        snapshot.updated_at = snapshot.created_at
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM tenant_config WHERE tenant_id=%s LIMIT 1", (snapshot.tenant_id,))
            if cur.fetchone():
                raise TenantRepositoryError(f"tenant already exists: {snapshot.tenant_id}")
            self._insert(cur, snapshot)
            cur.execute("INSERT INTO tenant_active VALUES (%s,%s)", (snapshot.tenant_id, snapshot.config_version))
        return deepcopy(snapshot)

    def get(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        with self._conn.cursor() as cur:
            if version is None:
                cur.execute("SELECT active_version FROM tenant_active WHERE tenant_id=%s", (tenant_id,))
                active = cur.fetchone()
                if not active:
                    raise TenantNotFound(tenant_id)
                version = int(active[0])
            cur.execute("SELECT config_json FROM tenant_config WHERE tenant_id=%s AND version=%s", (tenant_id, version))
            row = cur.fetchone()
        if not row:
            raise TenantNotFound(f"{tenant_id}@v{version}")
        return TenantConfig.from_dict(_json_object(row[0]))

    def save_version(self, config: TenantConfig, expected_version: int | None = None) -> TenantConfig:
        snapshot = deepcopy(config)
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"trpc-agent-config-version:{config.tenant_id}",),
            )
            self._check_expected_version(cur, config.tenant_id, expected_version)
            cur.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM tenant_config WHERE tenant_id=%s",
                (config.tenant_id,),
            )
            snapshot.config_version = int(cur.fetchone()[0])
            snapshot.updated_at = utc_now()
            self._insert(cur, snapshot)
        return deepcopy(snapshot)

    def publish(
        self,
        tenant_id: str,
        version: int,
        expected_version: int | None = None,
    ) -> TenantConfig:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"trpc-agent-config-version:{tenant_id}",),
            )
            self._check_expected_version(cur, tenant_id, expected_version)
            cur.execute(
                "SELECT config_json FROM tenant_config WHERE tenant_id=%s AND version=%s",
                (tenant_id, version),
            )
            row = cur.fetchone()
            if not row:
                raise TenantNotFound(f"{tenant_id}@v{version}")
            config = TenantConfig.from_dict(_json_object(row[0]))
            cur.execute(
                (
                    "INSERT INTO tenant_active VALUES (%s,%s) "
                    "ON CONFLICT (tenant_id) DO UPDATE SET active_version=EXCLUDED.active_version"
                ),
                (tenant_id, version),
            )
        return config

    def save_and_publish(
        self,
        config: TenantConfig,
        expected_version: int | None = None,
    ) -> TenantConfig:
        snapshot = deepcopy(config)
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"trpc-agent-config-version:{config.tenant_id}",),
            )
            self._check_expected_version(cur, config.tenant_id, expected_version)
            cur.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM tenant_config WHERE tenant_id=%s",
                (config.tenant_id,),
            )
            snapshot.config_version = int(cur.fetchone()[0])
            snapshot.updated_at = utc_now()
            self._insert(cur, snapshot)
            cur.execute(
                (
                    "INSERT INTO tenant_active VALUES (%s,%s) "
                    "ON CONFLICT (tenant_id) DO UPDATE SET active_version=EXCLUDED.active_version"
                ),
                (snapshot.tenant_id, snapshot.config_version),
            )
        return deepcopy(snapshot)

    def find_binding(self, channel: str, account_id: str) -> ChannelBinding:
        matches: list[ChannelBinding] = []
        for config in self.all_active():
            try:
                matches.append(deepcopy(config.channel_binding(channel, account_id)))
            except KeyError:
                continue
        if len(matches) > 1:
            raise TenantRepositoryError(f"ambiguous channel binding: {channel}/{account_id}")
        if matches:
            return matches[0]
        raise TenantNotFound(f"channel binding not found: {channel}/{account_id}")

    def all_active(self) -> list[TenantConfig]:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT c.config_json FROM tenant_active a "
                    "JOIN tenant_config c ON c.tenant_id=a.tenant_id "
                    "AND c.version=a.active_version ORDER BY c.tenant_id"
                )
            )
            return [TenantConfig.from_dict(_json_object(row[0])) for row in cur.fetchall()]

    @staticmethod
    def _insert(cur, config: TenantConfig) -> None:
        cur.execute(
            "INSERT INTO tenant_config VALUES (%s,%s,%s,%s,%s)",
            (
                config.tenant_id,
                config.config_version,
                json.dumps(config.to_dict(), ensure_ascii=False),
                config.created_at,
                config.updated_at,
            ),
        )

    @staticmethod
    def _check_expected_version(cur, tenant_id: str, expected_version: int | None) -> None:
        if expected_version is None:
            return
        cur.execute(
            "SELECT active_version FROM tenant_active WHERE tenant_id=%s FOR UPDATE",
            (tenant_id,),
        )
        row = cur.fetchone()
        if not row:
            raise TenantNotFound(tenant_id)
        current_version = int(row[0])
        if current_version != int(expected_version):
            raise TenantRepositoryConflict(
                f"tenant config changed: expected v{expected_version}, current v{current_version}"
            )

    def close(self) -> None:
        if self._connection is not None and not self._connection.closed:
            self._connection.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


for _method_name in ("create", "get", "save_version", "publish", "save_and_publish", "find_binding", "all_active"):
    setattr(
        PostgresTenantRepository,
        _method_name,
        _retry_postgres_once(getattr(PostgresTenantRepository, _method_name)),
    )
del _method_name
