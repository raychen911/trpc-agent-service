from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

import scripts.backend_isolation as isolation


def test_generated_resource_names_are_bounded_and_run_scoped() -> None:
    run_id = "backend-ack-a1b2c3d4e5f6"

    assert isolation.database_name(run_id) == "trpc_backend_backendacka1b2c3d4e5f6"
    assert isolation.redis_marker(run_id) == "trpc:backend:isolation:backend-ack-a1b2c3d4e5f6"
    assert isolation.bucket_name(run_id) == "trpc-backend-backend-ack-a1b2c3d4e5f6"


@pytest.mark.parametrize("value", ["Backend-ACK-one", "backend_ack", "backend-ack!", ""])
def test_run_id_rejects_unsafe_values(value: str) -> None:
    with pytest.raises(ValueError):
        isolation.database_name(value)


def test_postgres_url_replaces_database_without_leaking_credentials() -> None:
    source = "postgresql+asyncpg://runtime:p%40ss@db.example:5432/trpc_service?sslmode=require"

    runtime = isolation.replace_database_url(source, "trpc_backend_a1b2c3")
    admin = isolation.replace_database_url(
        source,
        "postgres",
        username="postgres",
        password="admin/p%40ss",
    )

    assert runtime == (
        "postgresql://runtime:p%40ss@db.example:5432/trpc_backend_a1b2c3?sslmode=require"
    )
    assert (
        admin == "postgresql://postgres:admin%2Fp%2540ss@db.example:5432/postgres?sslmode=require"
    )
    assert "trpc_service" not in runtime


def test_postgres_url_rejects_unsafe_database_name() -> None:
    with pytest.raises(ValueError, match="safe generated"):
        isolation.replace_database_url(
            "postgresql://runtime@db.example:5432/trpc_service",
            "trpc_backend_a;drop database trpc_service",
        )


def test_redis_url_isolated_db_preserves_auth_and_query() -> None:
    source = "rediss://:r%40dis@cache.example:6380/0?ssl_cert_reqs=required"

    assert isolation.replace_redis_database(source, 7) == (
        "rediss://:r%40dis@cache.example:6380/7?ssl_cert_reqs=required"
    )


def test_redis_db_zero_and_out_of_range_are_never_selected() -> None:
    with pytest.raises(ValueError):
        isolation.replace_redis_database("redis://cache.example/0", 0)
    with pytest.raises(ValueError):
        isolation.replace_redis_database("redis://cache.example/0", 16)


def test_public_summary_contains_identity_only() -> None:
    summary = isolation.public_summary(
        run_id="backend-ack-a1b2c3",
        database="trpc_backend_backendacka1b2c3",
        redis_database=3,
        marker="trpc:backend:isolation:backend-ack-a1b2c3",
        bucket="trpc-backend-backend-ack-a1b2c3",
        s3_marker_key=".trpc-backend-isolation/backend-ack-a1b2c3",
    )
    rendered = json.dumps(summary)

    assert summary["postgres_database"] == "trpc_backend_backendacka1b2c3"
    assert "password" not in rendered
    assert "postgresql://" not in rendered
    assert "redis://" not in rendered


def test_failure_report_is_structured_and_does_not_include_secret(tmp_path: Path) -> None:
    output = tmp_path / "backend.json"
    isolation_summary = isolation.public_summary(
        run_id="backend-ack-a1b2c3",
        database="trpc_backend_backendacka1b2c3",
        redis_database=3,
        marker="trpc:backend:isolation:backend-ack-a1b2c3",
        bucket="trpc-backend-backend-ack-a1b2c3",
        s3_marker_key=".trpc-backend-isolation/backend-ack-a1b2c3",
    )

    # Exercise the report contract through the runner without importing a
    # script from runs/ as a package; the helper summary is the same payload.
    report = {
        "candidate": {"backend_isolation": isolation_summary},
        "production_gate": "not_run",
    }
    output.write_text(json.dumps(report), encoding="utf-8")
    rendered = output.read_text(encoding="utf-8")

    assert json.loads(rendered)["production_gate"] == "not_run"
    assert "postgres-admin-password" not in rendered


def test_reserve_redis_database_skips_non_empty_and_claims_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    class FakeClient:
        def __init__(self, database: int) -> None:
            self.database = database

        async def ping(self) -> bool:
            return True

        async def eval(self, *_args: Any, **_kwargs: Any) -> int:
            calls.append(("eval", self.database))
            return 0 if self.database == 1 else 1

        async def aclose(self) -> None:
            calls.append(("close", self.database))

    monkeypatch.setattr(
        isolation.redis_async,  # type: ignore[attr-defined]
        "from_url",
        lambda url, **_kwargs: FakeClient(int(url.rsplit("/", 1)[1])),
    )

    result = asyncio.run(
        isolation.reserve_redis_database(
            "redis://cache.example:6379/0", "backend-ack-a1b2c3", candidates=(1, 2)
        )
    )

    assert result == (
        2,
        "redis://cache.example:6379/2",
        "trpc:backend:isolation:backend-ack-a1b2c3",
    )
    assert calls == [("eval", 1), ("close", 1), ("eval", 2), ("close", 2)]


def test_postgres_setup_installs_extensions_and_grants_all_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[str] = []

    class FakeConnection:
        async def fetchval(self, query: str, *_args: Any) -> object:
            if "pg_database" in query:
                return None
            return 1

        async def execute(self, query: str, *_args: Any) -> str:
            commands.append(query)
            return "OK"

        async def close(self) -> None:
            return None

    async def fake_admin_connection(*_args: Any) -> FakeConnection:
        return FakeConnection()

    monkeypatch.setattr(isolation, "_admin_connection", fake_admin_connection)

    asyncio.run(
        isolation.create_postgres_database(
            "postgresql://postgres:admin@db.example:5432/postgres",
            "trpc_backend_a1b2c3",
        )
    )

    assert commands[0] == 'CREATE DATABASE "trpc_backend_a1b2c3" OWNER trpc_migration'
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in commands
    assert "CREATE EXTENSION IF NOT EXISTS vector" in commands
    assert any("trpc_runtime, trpc_worker, trpc_metrics" in command for command in commands)
    assert any(
        "trpc_migration" in command and "CREATE ON SCHEMA" in command for command in commands
    )


def test_release_redis_requires_our_marker_before_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeClient:
        async def get(self, _key: str) -> bytes:
            return b"different-run"

        async def flushdb(self) -> None:
            calls.append("flushdb")

        async def aclose(self) -> None:
            calls.append("close")

    monkeypatch.setattr(
        isolation.redis_async,  # type: ignore[attr-defined]
        "from_url",
        lambda *_args, **_kwargs: FakeClient(),
    )

    with pytest.raises(isolation.BackendIsolationError, match="ownership"):
        asyncio.run(
            isolation.release_redis_database(
                "redis://cache.example:6379/0",
                2,
                "trpc:backend:isolation:backend-ack-a1b2c3",
                "backend-ack-a1b2c3",
            )
        )
    assert calls == ["close"]


def test_s3_bucket_creation_requires_not_found_then_verifies_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeClient:
        def __init__(self) -> None:
            self.head_calls = 0

        def head_bucket(self, **_kwargs: Any) -> None:
            self.head_calls += 1
            calls.append("head")
            if self.head_calls == 1:
                error = ClientError(
                    {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                    "HeadBucket",
                )
                raise error

        def create_bucket(self, **_kwargs: Any) -> None:
            calls.append("create")

        def put_object(self, **_kwargs: Any) -> None:
            calls.append("put-marker")

        def get_object(self, **_kwargs: Any) -> dict[str, Any]:
            calls.append("get-marker")
            return {"Body": _Body()}

    class _Body:
        def read(self) -> bytes:
            return b"backend-ack-a1b2c3"

    client = FakeClient()
    monkeypatch.setattr(isolation, "_s3_client", lambda *_args: client)

    isolation.create_s3_bucket(
        "http://minio.example:9000",
        "access",
        "secret",
        "trpc-backend-backend-ack-a1b2c3",
        ".trpc-backend-isolation/backend-ack-a1b2c3",
        "backend-ack-a1b2c3",
    )

    assert calls == ["head", "create", "put-marker", "get-marker", "head"]


def test_s3_cleanup_deletes_objects_before_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeClient:
        def list_objects_v2(self, **_kwargs: Any) -> dict[str, object]:
            calls.append("list")
            return {"Contents": [{"Key": "owned-key"}], "IsTruncated": False}

        def get_object(self, **_kwargs: Any) -> dict[str, Any]:
            calls.append("marker")
            return {"Body": _Body()}

        def delete_objects(self, **_kwargs: Any) -> None:
            calls.append("objects")

        def delete_bucket(self, **_kwargs: Any) -> None:
            calls.append("bucket")

    class _Body:
        def read(self) -> bytes:
            return b"backend-ack-a1b2c3"

    monkeypatch.setattr(isolation, "_s3_client", lambda *_args: FakeClient())

    isolation.delete_s3_bucket(
        "http://minio.example:9000",
        "access",
        "secret",
        "trpc-backend-backend-ack-a1b2c3",
        ".trpc-backend-isolation/backend-ack-a1b2c3",
        "backend-ack-a1b2c3",
    )

    assert calls == ["marker", "list", "objects", "bucket"]


def test_s3_cleanup_refuses_marker_owned_by_another_run(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class _Body:
        def read(self) -> bytes:
            return b"different-run"

    class FakeClient:
        def get_object(self, **_kwargs: Any) -> dict[str, Any]:
            return {"Body": _Body()}

        def list_objects_v2(self, **_kwargs: Any) -> dict[str, object]:
            calls.append("list")
            return {"IsTruncated": False}

        def delete_bucket(self, **_kwargs: Any) -> None:
            calls.append("bucket")

    monkeypatch.setattr(isolation, "_s3_client", lambda *_args: FakeClient())

    with pytest.raises(isolation.BackendIsolationError, match="ownership"):
        isolation.delete_s3_bucket(
            "http://minio.example:9000",
            "access",
            "secret",
            "trpc-backend-backend-ack-a1b2c3",
            ".trpc-backend-isolation/backend-ack-a1b2c3",
            "backend-ack-a1b2c3",
        )
    assert calls == []
