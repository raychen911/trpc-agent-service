from __future__ import annotations

import sys
import time

import asyncpg
import pytest
import redis.asyncio as redis_async

from trpc_service import _cli as cli
from trpc_service import probe
from trpc_service.probe import _resolve_reference, _url_password


@pytest.fixture(autouse=True)
def _isolated_runtime_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TRPC_SERVICE_RUNTIME_STATE_DIR", str(tmp_path))
    monkeypatch.delenv("TRPC_SERVICE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRPC_SERVICE_TENANT_SECRET_ROOT", raising=False)


def test_lightweight_probe_resolves_secret_references(monkeypatch, tmp_path) -> None:
    secret = tmp_path / "secret"
    secret.write_text(" file-value\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_TENANT_SECRET_ROOT", str(tmp_path))
    monkeypatch.setenv("FILE_REF", secret.as_uri())
    monkeypatch.setenv("ENV_VALUE", "env-value")
    monkeypatch.setenv("ENV_REF", "env://ENV_VALUE")

    assert _resolve_reference("FILE_REF") == " file-value"
    assert _resolve_reference("ENV_REF") == "env-value"
    assert _url_password("postgresql://runtime@db:5432/service", "p@ss") == (
        "postgresql://runtime:p%40ss@db:5432/service"
    )


def test_lightweight_probe_reference_and_url_fallbacks(monkeypatch) -> None:
    monkeypatch.delenv("MISSING_REF", raising=False)
    monkeypatch.setenv("LITERAL_REF", "literal://value")
    monkeypatch.setenv("RAW_REF", "raw-value")
    monkeypatch.setenv("MISSING_ENV_REF", "env://MISSING_VALUE")

    assert _resolve_reference("MISSING_REF") is None
    assert _resolve_reference("LITERAL_REF") == "value"
    assert _resolve_reference("RAW_REF") is None
    assert _resolve_reference("MISSING_ENV_REF") is None
    assert _url_password("redis://cache/0", None) == "redis://cache/0"
    assert _url_password("redis://cache/0", "secret") == "redis://:secret@cache/0"


def test_lightweight_probe_production_secret_policy(monkeypatch, tmp_path) -> None:
    secret_root = tmp_path / "secrets"
    secret_root.mkdir()
    inside = secret_root / "database_password"
    inside.write_text("production-file\n", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.write_text("outside-file\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "production")
    monkeypatch.setenv("TRPC_SERVICE_TENANT_SECRET_ROOT", str(secret_root))

    monkeypatch.setenv("INSIDE_REF", inside.as_uri())
    monkeypatch.setenv("OUTSIDE_REF", outside.as_uri())
    monkeypatch.setenv("PRODUCTION_ENV_VALUE", "production-env")
    monkeypatch.setenv("ENV_REF", "env://PRODUCTION_ENV_VALUE")
    monkeypatch.setenv("LITERAL_REF", "literal://production-literal")

    assert _resolve_reference("INSIDE_REF") == "production-file"
    assert _resolve_reference("OUTSIDE_REF") is None
    assert _resolve_reference("ENV_REF") == "production-env"
    assert _resolve_reference("LITERAL_REF") is None


def test_lightweight_probe_production_file_reference_requires_absolute_root(
    monkeypatch, tmp_path
) -> None:
    secret = tmp_path / "secret"
    secret.write_text("value\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "production")
    monkeypatch.setenv("FILE_REF", secret.as_uri())

    assert _resolve_reference("FILE_REF") is None

    monkeypatch.setenv("TRPC_SERVICE_TENANT_SECRET_ROOT", "relative/secrets")
    assert _resolve_reference("FILE_REF") is None


def test_lightweight_probe_rejects_unknown_roles(monkeypatch) -> None:
    assert not probe.check_liveness("unknown-role")
    monkeypatch.setattr(sys, "argv", ["probe", "--role", "unknown-role"])
    assert probe.main() == 1


class _DatabaseConnection:
    def __init__(self, value: int = 1, *, error: Exception | None = None) -> None:
        self.value = value
        self.error = error
        self.closed = False

    async def fetchval(self, _query: str, *_args: object) -> int:
        if self.error is not None:
            raise self.error
        return self.value

    async def fetchrow(self, _query: str) -> dict[str, object]:
        return {
            "current_user": "trpc_worker",
            "session_user": "trpc_worker",
            "is_superuser": False,
            "bypasses_rls": True,
            "rolcanlogin": True,
            "schema_usage": True,
            "owned_rls_table_count": 0,
        }

    async def close(self) -> None:
        self.closed = True


class _RedisConnection:
    def __init__(self, result: bool) -> None:
        self.result = result
        self.closed = False

    async def ping(self) -> bool:
        return self.result

    async def aclose(self) -> None:
        self.closed = True


class _S3Connection:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.closed = False

    def head_bucket(self, *, Bucket: str) -> None:
        assert Bucket == "artifacts"
        if self.fail:
            raise OSError("object store unavailable")

    def close(self) -> None:
        self.closed = True


class _IdentityConnection(_DatabaseConnection):
    def __init__(self, *, schema_usage: bool = True, table_privileges: bool = True) -> None:
        super().__init__()
        self.schema_usage = schema_usage
        self.table_privileges = table_privileges

    async def fetchrow(self, _query: str) -> dict[str, object]:
        return {
            "current_user": "trpc_worker",
            "session_user": "trpc_worker",
            "is_superuser": False,
            "bypasses_rls": True,
            "rolcanlogin": True,
            "schema_usage": self.schema_usage,
            "owned_rls_table_count": 0,
        }

    async def fetchval(self, query: str, *_args: object) -> int | bool:
        if "bool_and" in query:
            return self.table_privileges
        if "has_function_privilege" in query:
            return True
        return 1


def _probe_environment(monkeypatch, *, redis_url: str | None = "redis://cache/0") -> None:
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "development")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql+asyncpg://runtime@db/service")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_PASSWORD_REF", "literal://database-secret")
    monkeypatch.setenv(
        "TRPC_SERVICE_WORKER_DATABASE_DSN",
        "postgresql+asyncpg://worker@db/service",
    )
    monkeypatch.setenv(
        "TRPC_SERVICE_WORKER_DATABASE_DSN_REF",
        "env://TRPC_SERVICE_WORKER_DATABASE_DSN",
    )
    monkeypatch.setenv("TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF", "literal://worker-secret")
    if redis_url is None:
        monkeypatch.delenv("TRPC_SERVICE_REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("TRPC_SERVICE_REDIS_URL", redis_url)
    monkeypatch.setenv("TRPC_SERVICE_REDIS_PASSWORD_REF", "literal://redis-secret")


def test_lightweight_probe_uses_exact_evidence_table_privileges() -> None:
    assert probe._WORKER_TABLE_PRIVILEGES["wecom_connection_state"] == ("SELECT,INSERT,UPDATE")
    assert probe._WORKER_TABLE_PRIVILEGES["im_acceptance_evidence_events"] == ("SELECT,INSERT")
    assert probe._WORKER_TABLE_PRIVILEGES == cli._WORKER_TABLE_PRIVILEGES


@pytest.mark.asyncio
async def test_lightweight_probe_rejects_missing_dependencies(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_SERVICE_DATABASE_DSN", raising=False)
    monkeypatch.delenv("TRPC_SERVICE_REDIS_URL", raising=False)
    assert not await probe.check("channel-dispatcher")

    _probe_environment(monkeypatch, redis_url=None)
    assert not await probe.check("worker")


@pytest.mark.asyncio
async def test_lightweight_probe_database_paths(monkeypatch) -> None:
    _probe_environment(monkeypatch, redis_url=None)
    connection = _DatabaseConnection(value=0)

    async def connect(*_args, **_kwargs):
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect)
    assert not await probe.check("channel-dispatcher")
    assert connection.closed

    monkeypatch.setenv("TRPC_SERVICE_WORKER_DATABASE_DSN_REF", "env://MISSING_WORKER_DATABASE_DSN")
    assert not await probe.check("gateway")
    monkeypatch.delenv("TRPC_SERVICE_WORKER_DATABASE_DSN_REF")
    monkeypatch.setenv(
        "TRPC_SERVICE_WORKER_DATABASE_DSN_REF",
        "env://TRPC_SERVICE_WORKER_DATABASE_DSN",
    )

    connection = _DatabaseConnection()

    async def connect_success(*_args, **_kwargs):
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect_success)
    assert await probe.check("channel-dispatcher")
    assert connection.closed

    async def fail_connect(*_args, **_kwargs):
        raise OSError("database unavailable")

    monkeypatch.setattr(asyncpg, "connect", fail_connect)
    assert not await probe.check("channel-dispatcher")


@pytest.mark.asyncio
@pytest.mark.parametrize("ping_result", [False, True])
async def test_lightweight_probe_redis_paths(monkeypatch, ping_result: bool) -> None:
    _probe_environment(monkeypatch)
    database = _DatabaseConnection()
    redis = _RedisConnection(ping_result)

    async def connect(*_args, **_kwargs):
        return database

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(redis_async, "from_url", lambda *_args, **_kwargs: redis)

    assert await probe.check("worker") is ping_result
    assert database.closed
    assert redis.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("s3_failure", [False, True])
async def test_artifact_gc_probe_requires_database_and_object_store(
    monkeypatch, s3_failure: bool
) -> None:
    _probe_environment(monkeypatch, redis_url=None)
    monkeypatch.setenv("TRPC_SERVICE_S3_ENDPOINT", "https://s3.example.test")
    monkeypatch.setenv("TRPC_SERVICE_S3_ACCESS_KEY", "access")
    monkeypatch.setenv("TRPC_SERVICE_S3_SECRET_KEY_REF", "literal://secret")
    monkeypatch.setenv("TRPC_SERVICE_S3_BUCKET", "artifacts")
    database = _IdentityConnection()
    s3 = _S3Connection(fail=s3_failure)

    async def connect(*_args, **_kwargs):
        return database

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: s3)

    assert await probe.check("artifact-gc") is not s3_failure
    assert database.closed
    assert s3.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema_usage", "table_privileges", "expected"),
    [(True, True, True), (False, True, False), (True, False, False)],
)
async def test_lightweight_worker_probe_preserves_role_privilege_checks(
    monkeypatch,
    schema_usage: bool,
    table_privileges: bool,
    expected: bool,
) -> None:
    _probe_environment(monkeypatch)
    database = _IdentityConnection(
        schema_usage=schema_usage,
        table_privileges=table_privileges,
    )
    redis = _RedisConnection(True)

    async def connect(*_args, **_kwargs):
        return database

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(redis_async, "from_url", lambda *_args, **_kwargs: redis)

    assert await probe.check("worker") is expected
    assert database.closed
    assert redis.closed is expected


def test_lightweight_probe_main_exit_codes(monkeypatch) -> None:
    async def healthy(_role: str) -> bool:
        return True

    monkeypatch.setattr(probe, "check", healthy)
    monkeypatch.setattr(sys, "argv", ["probe", "--role", "worker"])
    assert probe.main() == 0

    async def unhealthy(_role: str) -> bool:
        return False

    monkeypatch.setattr(probe, "check", unhealthy)
    monkeypatch.setattr(sys, "argv", ["probe", "--role", "channel-dispatcher"])
    assert probe.main() == 1


def test_lightweight_probe_liveness_and_main(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TRPC_SERVICE_RUNTIME_STATE_DIR", str(tmp_path))
    heartbeat = tmp_path / "worker.heartbeat"
    heartbeat.write_text(f"{time.time():.6f}\n", encoding="ascii")

    assert probe.check_liveness("worker")
    monkeypatch.setattr(sys, "argv", ["probe", "--role", "worker", "--liveness"])
    assert probe.main() == 0

    monkeypatch.setenv("TRPC_SERVICE_LIVENESS_MAX_AGE_SECONDS", "invalid")
    assert not probe.check_liveness("worker")
    assert probe.main() == 1


@pytest.mark.asyncio
async def test_lightweight_probe_rejects_draining_role(monkeypatch, tmp_path) -> None:
    _probe_environment(monkeypatch, redis_url=None)
    monkeypatch.setenv("TRPC_SERVICE_RUNTIME_STATE_DIR", str(tmp_path))
    (tmp_path / "channel-dispatcher.draining").touch()

    async def unexpected_connect(*_args, **_kwargs):
        raise AssertionError("draining probe must not connect to the database")

    monkeypatch.setattr(asyncpg, "connect", unexpected_connect)
    assert not await probe.check("channel-dispatcher")


@pytest.mark.asyncio
async def test_lightweight_probe_rejects_connection_without_identity_api(monkeypatch) -> None:
    _probe_environment(monkeypatch, redis_url=None)

    class ConnectionWithoutIdentity:
        async def fetchval(self, *_args) -> int:
            return 1

        async def close(self) -> None:
            return None

    async def connect(*_args, **_kwargs):
        return ConnectionWithoutIdentity()

    monkeypatch.setattr(asyncpg, "connect", connect)
    assert not await probe.check("channel-dispatcher")
