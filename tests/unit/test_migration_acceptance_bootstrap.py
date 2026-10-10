from __future__ import annotations

import pytest

from scripts import migration_acceptance_bootstrap as bootstrap
from trpc_service.storage.migration import _TARGET_EMPTY_TABLES


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "TRPC_MIGRATION_SOURCE_REDIS_URL": "redis://source.example:6379/0",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN": "postgresql://trpc_runtime@target.example:5432/db",
        "TRPC_MIGRATION_TENANT_ID": "migration-acceptance-tenant-1",
        "TRPC_MIGRATION_ID": "migration-acceptance-run-1",
        "TRPC_MIGRATION_EXPECTED_RECORDS": "200",
        "TRPC_MIGRATION_APP_ID": "migration-acceptance-app-1",
        "TRPC_MIGRATION_APP_REVISION": "1",
        "TRPC_MIGRATION_CONFIG_VERSION": "1",
        "TRPC_MIGRATION_BINDING_ID": "migration-acceptance-binding-1",
        "TRPC_MIGRATION_BINDING_REVISION": "1",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_bootstrap_derives_distinct_phase_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    base, phase = bootstrap._scope_from_env()
    assert base.tenant_id != phase.tenant_id
    assert base.migration_id != phase.migration_id
    assert base.app_id != phase.app_id
    assert phase.binding_id is None


def test_bootstrap_rejects_reused_scope_identifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.setenv("TRPC_MIGRATION_PHASE_APP_ID", "migration-acceptance-app-1")
    with pytest.raises(ValueError, match="unique"):
        bootstrap._scope_from_env()


@pytest.mark.asyncio
async def test_bootstrap_is_opt_in_before_opening_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MIGRATION", raising=False)
    monkeypatch.delenv("TRPC_MIGRATION_FULL_ACCEPTANCE", raising=False)
    monkeypatch.delenv("TRPC_MIGRATION_BOOTSTRAP", raising=False)
    result = await bootstrap.bootstrap()
    assert result == {"status": "not_run", "reason": "both live migration opt-ins are required"}


@pytest.mark.asyncio
async def test_database_scope_empty_reuses_one_connection_for_target_preflight() -> None:
    class Connection:
        def __init__(self) -> None:
            self.fetchval_calls = 0

        def transaction(self) -> Connection:
            return self

        async def __aenter__(self) -> Connection:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def execute(self, query: str, *args: object) -> None:
            del query, args

        async def fetchval(self, query: str, *args: object) -> bool:
            del query, args
            self.fetchval_calls += 1
            return False

        async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
            del query, args
            return [{"table_name": table, "row_count": 0} for table in _TARGET_EMPTY_TABLES]

    class Pool:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection
            self.acquire_calls = 0

        def acquire(self) -> Connection:
            self.acquire_calls += 1
            return self.connection

    connection = Connection()
    pool = Pool(connection)
    scope = bootstrap.BootstrapScope(
        tenant_id="migration-acceptance-empty",
        migration_id="migration-acceptance-empty-run",
        app_id="migration-acceptance-empty-app",
        binding_id="migration-acceptance-empty-binding",
    )

    await bootstrap._assert_database_scope_empty(pool, scope)  # type: ignore[arg-type]

    assert pool.acquire_calls == 1
    assert connection.fetchval_calls == 2
