from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest

from scripts import performance_fixture as fixture
from trpc_service.tenant.models import Channel


class FakeRepository:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def create_tenant(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("create_tenant", kwargs))
        return {"tenant_id": kwargs["tenant_id"], "control_version": 1}

    async def create_config_revision(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("create_config_revision", kwargs))
        return {"version": 1, "tenant_control_version": 2}

    async def activate_config(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("activate_config", kwargs))
        return {"tenant_control_version": 3}

    async def put_binding(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("put_binding", kwargs))
        return {"enabled": True, "tenant_control_version": 4}


class FailingAfterTenantRepository(FakeRepository):
    async def create_config_revision(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("create_config_revision", kwargs))
        raise RuntimeError("synthetic failure with secret-looking text")


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeConnection:
    def __init__(self, *, ownership_result: int = 1) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.ownership_result = ownership_result

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def execute(self, query: str, *args: Any) -> str:
        self.calls.append((query, args))
        return "DELETE 1" if query.startswith("DELETE") else "SELECT 1"

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.calls.append((query, args))
        if "cleanup_performance_fixture" in query:
            return json.dumps({table: 1 for table in fixture.CLEANUP_TABLES})
        return self.ownership_result


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakePool:
    def __init__(self, *, ownership_result: int = 1) -> None:
        self.connection = FakeConnection(ownership_result=ownership_result)
        self.closed = False

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)

    async def close(self) -> None:
        self.closed = True


def _env(
    monkeypatch: pytest.MonkeyPatch,
    dsn: str = "postgresql://trpc:pw@127.0.0.1:5432/test",
) -> None:
    monkeypatch.setenv(fixture.OPT_IN_ENV, "1")
    monkeypatch.setenv(fixture.CONFIRM_ENV, fixture.CONFIRM_VALUE)
    monkeypatch.setenv(fixture.DATABASE_ENV, dsn)


def test_default_create_is_not_run_and_does_not_connect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    called = False

    async def forbidden_pool(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("default fixture command must not connect")

    monkeypatch.setattr(asyncpg, "create_pool", forbidden_pool)
    output = tmp_path / "fixture.json"
    assert fixture.main(["create", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert not called


def test_wrong_confirmation_does_not_connect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _env(monkeypatch)
    monkeypatch.setenv(fixture.CONFIRM_ENV, "I_UNDERSTAND_SOMETHING_ELSE")
    called = False

    async def forbidden_pool(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("invalid confirmation must not connect")

    monkeypatch.setattr(asyncpg, "create_pool", forbidden_pool)
    output = tmp_path / "fixture.json"
    assert fixture.main(["create", "--execute", "--output", str(output)]) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["gate"] == "not_run"
    assert not called


def test_remote_requires_flag_and_exact_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, "postgresql://trpc:pw@database.example:5432/test")
    args = fixture._parser().parse_args(["create", "--execute"])
    reasons = fixture._opt_in_reasons(args, dict(__import__("os").environ))
    assert "--allow-remote is required for a non-loopback DSN" in reasons
    assert f"{fixture.REMOTE_CONFIRM_ENV} exact confirmation is required" in reasons


def test_create_builds_offline_feishu_fixture_without_real_secret_values(tmp_path: Path) -> None:
    pool = FakePool()
    repositories: list[FakeRepository] = []

    def repository_factory(value: Any) -> FakeRepository:
        repository = FakeRepository(value)
        repositories.append(repository)
        return repository

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    suffix = "a" * 32
    report_path = tmp_path / "fixture.json"
    report = asyncio.run(
        fixture._create_fixture(
            pool_factory=cast(fixture.PoolFactory, pool_factory),
            dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
            report_path=report_path,
            suffix=suffix,
            repository_factory=repository_factory,
        )
    )
    assert report["gate"] == "pass"
    assert report["tenant_id"] == f"perf-{suffix}"
    assert report["binding_id"] == f"perf-binding-{suffix}"
    assert report["config_version"] == 1
    tenant_call = repositories[0].calls[0][1]
    assert tenant_call["idempotency_key"] == fixture._tenant_idempotency_key(
        f"perf-fixture-{suffix}", report["manifest_checksum"]
    )
    binding_call = repositories[0].calls[-1][1]["binding"]
    assert binding_call.channel is Channel.FEISHU
    assert all(
        ref.uri.startswith("env://TRPC_PERF_FIXTURE_UNUSED_")
        for ref in binding_call.secret_refs.values()
    )
    rendered = json.dumps(report, ensure_ascii=False)
    assert "postgresql://" not in rendered
    assert "TRPC_PERF_FIXTURE_UNUSED" not in rendered
    assert pool.closed


def test_partial_create_returns_exact_cleanup_report_after_tenant_commit(tmp_path: Path) -> None:
    pool = FakePool()
    repositories: list[FailingAfterTenantRepository] = []

    def repository_factory(value: Any) -> FailingAfterTenantRepository:
        repository = FailingAfterTenantRepository(value)
        repositories.append(repository)
        return repository

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    suffix = "e" * 32
    report_path = tmp_path / "partial.json"
    with pytest.raises(fixture.FixtureCreateError) as raised:
        asyncio.run(
            fixture._create_fixture(
                pool_factory=cast(fixture.PoolFactory, pool_factory),
                dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
                report_path=report_path,
                suffix=suffix,
                repository_factory=repository_factory,
            )
        )
    report = raised.value.partial_report
    assert report is not None
    assert report["gate"] == "partial"
    assert report["cleanup_ready"] is True
    assert report["error_type"] == "RuntimeError"
    assert "secret-looking" not in json.dumps(report)
    validated = fixture._validate_report(
        report_path,
        report,
        tenant_id=f"perf-{suffix}",
        run_id=f"perf-fixture-{suffix}",
    )
    assert validated["manifest_checksum"] == report["manifest_checksum"]
    assert repositories[0].calls[0][0] == "create_tenant"
    assert pool.closed


def test_cleanup_uses_bounded_database_function_and_sets_rls(tmp_path: Path) -> None:
    suffix = "b" * 32
    report_path = tmp_path / "fixture.json"
    report = fixture._fixture_report(fixture._ids(suffix), report_path)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    pool = FakePool()

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    result = asyncio.run(
        fixture._cleanup_fixture(
            pool_factory=cast(fixture.PoolFactory, pool_factory),
            dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
            report_path=report_path,
            tenant_id=f"perf-{suffix}",
            run_id=f"perf-fixture-{suffix}",
        )
    )
    queries = [query for query, _args in pool.connection.calls]
    assert queries[0].startswith("SELECT set_config('app.tenant_id'")
    ownership_queries = [query for query in queries if "FROM tenants AS tenant" in query]
    assert len(ownership_queries) == 1
    assert "audit_logs" in ownership_queries[0]
    assert "admin_idempotency" not in ownership_queries[0]
    assert "audit.user_id=$3" in ownership_queries[0]
    assert "audit.trace_id=$6" in ownership_queries[0]
    cleanup_queries = [query for query in queries if "cleanup_performance_fixture" in query]
    assert cleanup_queries == ["SELECT public.cleanup_performance_fixture($1, $2, $3)"]
    assert not any(query.startswith("DELETE FROM ") for query in queries)
    assert result["deleted_rows"]["tenants"] == 1
    assert result["deleted_rows"]["migration_write_barriers"] == 1
    assert pool.closed


def test_cleanup_accepts_persistent_audit_after_idempotency_expiry(tmp_path: Path) -> None:
    """An expired 24-hour idempotency row must not block owned cleanup."""
    suffix = "9" * 32
    report_path = tmp_path / "fixture.json"
    report = fixture._fixture_report(fixture._ids(suffix), report_path)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    # This fake DB has no admin_idempotency proof at all (as if it expired),
    # but it does return the durable audit_logs ownership proof.
    pool = FakePool(ownership_result=1)

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    result = asyncio.run(
        fixture._cleanup_fixture(
            pool_factory=cast(fixture.PoolFactory, pool_factory),
            dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
            report_path=report_path,
            tenant_id=f"perf-{suffix}",
            run_id=f"perf-fixture-{suffix}",
        )
    )
    ownership_queries = [
        query for query, _args in pool.connection.calls if "FROM tenants AS tenant" in query
    ]
    assert result["gate"] == "pass"
    assert "audit_logs" in ownership_queries[0]
    assert not any("admin_idempotency" in query for query in ownership_queries)


def test_cleanup_accepts_pre_mailbox_v2_cleanup_manifest(tmp_path: Path) -> None:
    suffix = "8" * 32
    report_path = tmp_path / "fixture.json"
    report = fixture._fixture_report(fixture._ids(suffix), report_path)
    report["cleanup_tables"] = list(fixture._LEGACY_CLEANUP_TABLES_V1)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    pool = FakePool(ownership_result=1)

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    result = asyncio.run(
        fixture._cleanup_fixture(
            pool_factory=cast(fixture.PoolFactory, pool_factory),
            dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
            report_path=report_path,
            tenant_id=f"perf-{suffix}",
            run_id=f"perf-fixture-{suffix}",
        )
    )

    assert result["gate"] == "pass"
    assert "session_mailbox_items" in result["deleted_rows"]
    assert "session_mailboxes" in result["deleted_rows"]


def test_cleanup_allowlist_is_child_before_parent_for_all_foreign_keys() -> None:
    position = {table: index for index, table in enumerate(fixture.CLEANUP_TABLES)}
    assert "sessions" in position
    # Keep each child-to-parent relationship explicit and readable.
    edges = (
        ("session_mailbox_items", "session_mailboxes"),
        ("session_mailbox_items", "inbound_messages"),
        ("delivery_attempts", "outbound_messages"),
        ("outbound_messages", "channel_bindings"),
        ("channel_identities", "channel_bindings"),
        ("im_acceptance_evidence_events", "channel_bindings"),
        ("wecom_connection_state", "channel_bindings"),
        ("inbound_messages", "channel_bindings"),
        ("inbound_messages", "config_revisions"),
        ("turn_intents", "session_turns"),
        ("session_events", "sessions"),
        ("session_events", "session_turns"),
        ("session_summaries", "sessions"),
        ("session_turns", "sessions"),
        ("session_turns", "inbound_messages"),
        ("tool_executions", "session_turns"),
        ("knowledge_embeddings", "knowledge_items"),
        ("knowledge_items", "storage_profiles"),
        ("migration_leases", "migration_scope_manifests"),
        ("migration_write_barriers", "migration_scope_manifests"),
        ("config_revisions", "agent_apps"),
        ("channel_bindings", "agent_apps"),
        ("session_mailboxes", "tenants"),
        ("sessions", "agent_apps"),
    )
    assert all(position[child] < position[parent] for child, parent in edges)


def test_cleanup_refuses_to_delete_without_exact_ownership_proof(tmp_path: Path) -> None:
    suffix = "f" * 32
    report_path = tmp_path / "fixture.json"
    report = fixture._fixture_report(fixture._ids(suffix), report_path)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    pool = FakePool(ownership_result=0)

    async def pool_factory(_dsn: str, **_kwargs: Any) -> Any:
        return pool

    with pytest.raises(fixture.FixtureValidationError):
        asyncio.run(
            fixture._cleanup_fixture(
                pool_factory=cast(fixture.PoolFactory, pool_factory),
                dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
                report_path=report_path,
                tenant_id=f"perf-{suffix}",
                run_id=f"perf-fixture-{suffix}",
            )
        )
    assert not any("cleanup_performance_fixture" in query for query, _args in pool.connection.calls)
    assert pool.closed


def test_cleanup_rejects_tampered_report_before_connecting(tmp_path: Path) -> None:
    suffix = "c" * 32
    report_path = tmp_path / "fixture.json"
    report = fixture._fixture_report(fixture._ids(suffix), report_path)
    report["tenant_id"] = "perf-" + "d" * 32
    report_path.write_text(json.dumps(report), encoding="utf-8")
    called = False

    async def forbidden_pool(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("tampered report must be rejected before connecting")

    with pytest.raises(fixture.FixtureValidationError):
        asyncio.run(
            fixture._cleanup_fixture(
                pool_factory=forbidden_pool,
                dsn="postgresql://trpc:pw@127.0.0.1:5432/test",
                report_path=report_path,
                tenant_id=f"perf-{suffix}",
                run_id=f"perf-fixture-{suffix}",
            )
        )
    assert not called
