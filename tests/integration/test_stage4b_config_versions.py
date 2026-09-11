"""Integration tests for Stage 4B tenant_config_versions table and migration.

Proves on real PostgreSQL that the history table exists with all constraints,
that upgrading from Stage 4A backfills existing rows, and that
downgrade/upgrade round-trips preserve data.

Tests share one module-scoped container, so every test creates tenants with
unique IDs and only asserts on its own rows.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.config.tenant import AgentAppConfig
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant import TenantConfigDraft
from trpc_service.config.tenant_repository import TenantAlreadyExistsError
from trpc_service.config.tenant_repository import TenantConfigTargetVersionNotFoundError
from trpc_service.config.tenant_repository import TenantConfigVersionConflictError
from trpc_service.config.tenant_repository import TenantNotFoundError
from trpc_service.config.tenant_repository import TenantRepositoryDataError
from tests.tenant_helpers import make_audit_policy, make_backend_profile, make_governance
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository

from .pg_helpers import requires_docker, run_alembic

_STAGE4A_REVISION = "0001_create_tenant_configs"


def _unique_tenant() -> str:
    return f"tenant{uuid.uuid4().hex[:8]}"


def _head_insert_sql(tenant_id: str, version: int, enabled: bool, app_id: str) -> str:
    return ("INSERT INTO tenant_configs"
            " (tenant_id, enabled, version, app_id, instruction, model_profile, allowed_tools)"
            f" VALUES ('{tenant_id}', {'true' if enabled else 'false'}, {version}, '{app_id}',"
            f" 'Instruction for {tenant_id}', 'default', '[\"get_current_time\"]'::jsonb)")


_DEFAULT_GOV_SQL = ('{"allowed_channels": ["web", "web_console", "wecom", "feishu"],'
                    ' "allowed_user_ids": [], "tool_decisions": {},'
                    ' "content_policy": {"enabled": false, "input_action": "block",'
                    ' "output_action": "block"},'
                    ' "limits": null}')

# R1A (0008): backend_profile is NOT NULL without a server default on head.
_DEFAULT_PROFILE_SQL = ('{"state_backend": "redis", "artifact_backend": "s3",'
                        ' "knowledge_backend": "sql", "audit_backend": "sql"}')
_DEFAULT_AUDIT_POLICY_SQL = '{"retention_days": 365, "delivery_events": "all"}'


def _head_insert_sql_gov(tenant_id: str, version: int, enabled: bool, app_id: str) -> str:
    """Head-revision (>=0008) insert: governance AND backend_profile are NOT
    NULL without a server default, so every probe row must state both."""
    return ("INSERT INTO tenant_configs"
            " (tenant_id, enabled, version, app_id, instruction, model_profile,"
            " allowed_tools, governance, backend_profile, audit_policy)"
            f" VALUES ('{tenant_id}', {'true' if enabled else 'false'}, {version}, '{app_id}',"
            f" 'Instruction for {tenant_id}', 'default', '[\"get_current_time\"]'::jsonb,"
            f" '{_DEFAULT_GOV_SQL}'::jsonb, '{_DEFAULT_PROFILE_SQL}'::jsonb, '{_DEFAULT_AUDIT_POLICY_SQL}'::jsonb)")


def _version_insert_sql(tenant_id: str, version: int, values: str) -> str:
    return ("INSERT INTO tenant_config_versions"
            " (tenant_id, version, enabled, app_id, instruction, model_profile,"
            " allowed_tools, governance, backend_profile, audit_policy)"
            f" VALUES ('{tenant_id}', {version}, {values}, '{_DEFAULT_GOV_SQL}'::jsonb,"
            f" '{_DEFAULT_PROFILE_SQL}'::jsonb, '{_DEFAULT_AUDIT_POLICY_SQL}'::jsonb)")


def _default_version_values(app_id: str) -> str:
    return f"true, '{app_id}', 'i', 'default', '[]'::jsonb"


async def _fetch_all(db_url: str, sql: str) -> list[tuple]:
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(sa.text(sql))
            return list(result.fetchall())
    finally:
        await engine.dispose()


async def _execute(db_url: str, sql: str) -> None:
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(sa.text(sql))
    finally:
        await engine.dispose()


async def _try_execute(db_url: str, sql: str) -> bool:
    """Return True if the statement succeeded, False if it was rejected."""
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            try:
                await conn.execute(sa.text(sql))
                return True
            except sa.exc.DBAPIError:
                return False
    finally:
        await engine.dispose()


@requires_docker
class TestTenantConfigVersionsMigration:

    def test_backfill_creates_history_from_existing_rows(self, postgres_url):
        tenant_a, tenant_b = _unique_tenant(), _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION, check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql(tenant_a, 3, True, "app_x")))
        asyncio.run(_execute(postgres_url, _head_insert_sql(tenant_b, 1, False, "app_y")))

        result = run_alembic(postgres_url, "upgrade", "head")
        assert result.returncode == 0, f"upgrade failed: {result.stderr}"

        head_rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT tenant_id, version, enabled, app_id FROM tenant_configs"
                f" WHERE tenant_id IN ('{tenant_a}', '{tenant_b}') ORDER BY tenant_id",
            ))
        version_rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT tenant_id, version, enabled, app_id FROM tenant_config_versions"
                f" WHERE tenant_id IN ('{tenant_a}', '{tenant_b}') ORDER BY tenant_id",
            ))
        assert len(head_rows) == 2
        assert version_rows == head_rows

    def test_backfill_is_idempotent_after_roundtrip(self, postgres_url):
        tenant = _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION, check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql(tenant, 3, True, "app_x")))
        run_alembic(postgres_url, "upgrade", "head", check=True)

        run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION, check=True)
        run_alembic(postgres_url, "upgrade", "head", check=True)

        head_rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT tenant_id, version, enabled, app_id FROM tenant_configs"
                f" WHERE tenant_id='{tenant}'",
            ))
        version_rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT tenant_id, version, enabled, app_id FROM tenant_config_versions"
                f" WHERE tenant_id='{tenant}'",
            ))
        assert head_rows == [(tenant, 3, True, "app_x")]
        assert version_rows == head_rows

    def test_current_head_not_modified_by_backfill(self, postgres_url):
        tenant = _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION, check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql(tenant, 7, True, "app_x")))

        run_alembic(postgres_url, "upgrade", "head", check=True)
        rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT version, app_id FROM tenant_configs WHERE tenant_id='{tenant}'",
            ))
        assert rows == [(7, "app_x")]


@requires_docker
class TestTenantConfigVersionsConstraints:

    @pytest.fixture(autouse=True)
    def _migrated(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        self.tenant = _unique_tenant()
        asyncio.run(_execute(postgres_url, _head_insert_sql_gov(self.tenant, 1, True, "app_a")))
        self.url = postgres_url

    def test_composite_primary_key_rejects_duplicate(self):
        asyncio.run(_execute(
            self.url,
            _version_insert_sql(self.tenant, 1, _default_version_values("app_a")),
        ))
        ok = asyncio.run(_try_execute(
            self.url,
            _version_insert_sql(self.tenant, 1, _default_version_values("app_a")),
        ))
        assert ok is False, "duplicate (tenant_id, version) must be rejected"

    def test_version_must_be_positive(self):
        ok = asyncio.run(_try_execute(
            self.url,
            _version_insert_sql(self.tenant, 0, _default_version_values("app_a")),
        ))
        assert ok is False, "version 0 must be rejected"

    def test_tools_must_be_array(self):
        ok = asyncio.run(
            _try_execute(
                self.url,
                _version_insert_sql(self.tenant, 2, "true, 'app_a', 'i', 'default', '{}'::jsonb"),
            ))
        assert ok is False, "non-array allowed_tools must be rejected"

    @pytest.mark.parametrize("column", ["app_id", "instruction", "model_profile"])
    def test_text_columns_must_not_be_blank(self, column):
        values = {"app_id": "'app_a'", "instruction": "'i'", "model_profile": "'default'"}
        values[column] = "'  '"
        ok = asyncio.run(
            _try_execute(
                self.url,
                _version_insert_sql(
                    self.tenant,
                    2,
                    f"true, {values['app_id']}, {values['instruction']},"
                    f" {values['model_profile']}, '[]'::jsonb",
                ),
            ))
        assert ok is False, f"blank {column} must be rejected by constraint"

    def test_recorded_at_is_database_generated(self):
        asyncio.run(_execute(
            self.url,
            _version_insert_sql(self.tenant, 2, _default_version_values("app_a")),
        ))
        rows = asyncio.run(
            _fetch_all(
                self.url,
                f"SELECT recorded_at IS NOT NULL FROM tenant_config_versions"
                f" WHERE tenant_id='{self.tenant}' AND version=2",
            ))
        assert rows == [(True, )]

    def test_fk_restricts_tenant_delete(self):
        asyncio.run(_execute(
            self.url,
            _version_insert_sql(self.tenant, 2, _default_version_values("app_a")),
        ))
        ok = asyncio.run(_try_execute(self.url, f"DELETE FROM tenant_configs WHERE tenant_id='{self.tenant}'"))
        assert ok is False, "deleting a tenant with history must be restricted"


@requires_docker
class TestDowngradeUpgradeRoundTrip:

    def test_downgrade_removes_history_but_keeps_head(self, postgres_url):
        tenant = _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql_gov(tenant, 4, True, "app_rt")))

        result = run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION)
        assert result.returncode == 0, f"downgrade failed: {result.stderr}"

        rows = asyncio.run(_fetch_all(
            postgres_url,
            f"SELECT version FROM tenant_configs WHERE tenant_id='{tenant}'",
        ))
        assert rows == [(4, )], "current head data must survive downgrade"

        exists = asyncio.run(
            _fetch_all(
                postgres_url,
                "SELECT EXISTS (SELECT FROM information_schema.tables"
                " WHERE table_name='tenant_config_versions')",
            ))
        assert exists == [(False, )]

    def test_upgrade_again_re_backfills(self, postgres_url):
        tenant = _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql_gov(tenant, 4, True, "app_rt")))
        run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION, check=True)
        run_alembic(postgres_url, "upgrade", "head", check=True)

        rows = asyncio.run(
            _fetch_all(
                postgres_url,
                f"SELECT version FROM tenant_config_versions WHERE tenant_id='{tenant}'",
            ))
        assert rows == [(4, )]


@requires_docker
class TestMigrationOutputSafety:

    def test_upgrade_output_does_not_leak_credentials(self, postgres_url):
        result = run_alembic(postgres_url, "upgrade", "head")
        assert "testpass" not in result.stdout
        assert "testpass" not in result.stderr

    def test_backfill_error_does_not_leak_config_content(self, postgres_url):
        marker_app = f"marker{uuid.uuid4().hex[:8]}"
        tenant = _unique_tenant()
        run_alembic(postgres_url, "upgrade", "head", check=True)
        asyncio.run(_execute(postgres_url, _head_insert_sql_gov(tenant, 1, True, marker_app)))
        result = run_alembic(postgres_url, "downgrade", _STAGE4A_REVISION)
        result2 = run_alembic(postgres_url, "upgrade", "head")
        combined = f"{result.stdout} {result.stderr} {result2.stdout} {result2.stderr}"
        assert marker_app not in combined


class _RepoSession:
    """Engine+repository lifetime bound to a single event loop."""

    def __init__(self, url: str):
        self._url = url
        self.engine = None
        self.repo = None

    async def __aenter__(self) -> SqlTenantConfigRepository:
        self.engine = create_async_engine(self._url)
        self.repo = SqlTenantConfigRepository(self.engine)
        return self.repo

    async def __aexit__(self, *exc_info) -> None:
        if self.repo is not None:
            await self.repo.close()


@requires_docker
class TestSqlCommandRepository:
    """Write-path semantics against real PostgreSQL (Stage 4B Task 2).

    Every test runs its whole scenario inside one asyncio.run() so the
    engine, its pooled connections, and dispose share a single event loop.
    """

    @pytest.fixture(autouse=True)
    def _migrated(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        self.url = postgres_url

    @staticmethod
    def _config(tenant_id: str, version: int = 1, instruction: str = "instr") -> TenantConfig:
        return TenantConfig(
            tenant_id=tenant_id,
            enabled=True,
            version=version,
            app=AgentAppConfig(
                app_id="app_a",
                instruction=instruction,
                model_profile="default",
                allowed_tools=["get_current_time"],
            ),
            governance=make_governance(),
            backend_profile=make_backend_profile(),
            audit_policy=make_audit_policy(),
        )

    @staticmethod
    def _draft(enabled: bool = True, app_id: str = "app_a", instruction: str = "new instr") -> TenantConfigDraft:
        return TenantConfigDraft(
            enabled=enabled,
            app=AgentAppConfig(
                app_id=app_id,
                instruction=instruction,
                model_profile="default",
                allowed_tools=["get_current_time"],
            ),
            governance=make_governance(),
            backend_profile=make_backend_profile(),
            audit_policy=make_audit_policy(),
        )

    # --- create ---

    def test_create_writes_head_and_version1_snapshot(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                created = await repo.create(self._config(tenant))
                assert created.version == 1
                head = await _fetch_all(
                    self.url,
                    f"SELECT version, enabled, app_id, instruction FROM tenant_configs"
                    f" WHERE tenant_id='{tenant}'",
                )
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions"
                    f" WHERE tenant_id='{tenant}' ORDER BY version",
                )
                return head, history

        head, history = asyncio.run(_main())
        assert head == [(1, True, "app_a", "instr")]
        assert history == [(1, )]

    def test_create_forces_version_one(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                created = await repo.create(self._config(tenant, version=9))
                assert created.version == 1

        asyncio.run(_main())

    def test_create_duplicate_tenant_conflicts(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                with pytest.raises(TenantAlreadyExistsError):
                    await repo.create(self._config(tenant, version=2))
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions WHERE tenant_id='{tenant}'",
                )
                return history

        history = asyncio.run(_main())
        assert history == [(1, )], "failed duplicate create must not append history"

    # --- update ---

    def test_update_bumps_version_and_appends_snapshot(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                updated = await repo.update(tenant, 1, self._draft())
                assert updated.version == 2
                assert updated.app.instruction == "new instr"
                head = await _fetch_all(
                    self.url,
                    f"SELECT version, enabled, app_id, instruction FROM tenant_configs"
                    f" WHERE tenant_id='{tenant}'",
                )
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions"
                    f" WHERE tenant_id='{tenant}' ORDER BY version",
                )
                return head, history

        head, history = asyncio.run(_main())
        assert head == [(2, True, "app_a", "new instr")]
        assert history == [(1, ), (2, )]

    def test_update_wrong_expected_version_conflicts(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                with pytest.raises(TenantConfigVersionConflictError):
                    await repo.update(tenant, 5, self._draft())
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions WHERE tenant_id='{tenant}'",
                )
                return history

        history = asyncio.run(_main())
        assert history == [(1, )]

    def test_update_unknown_tenant_not_found(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                with pytest.raises(TenantNotFoundError):
                    await repo.update(tenant, 1, self._draft())

        asyncio.run(_main())

    # --- rollback ---

    def test_rollback_creates_new_version_with_target_content(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant, instruction="v1 text"))
                await repo.update(tenant, 1, self._draft(instruction="v2 text"))
                rolled = await repo.rollback(tenant, 2, 1)
                assert rolled.version == 3
                assert rolled.app.instruction == "v1 text"
                head = await _fetch_all(
                    self.url,
                    f"SELECT version, enabled, app_id, instruction FROM tenant_configs"
                    f" WHERE tenant_id='{tenant}'",
                )
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions"
                    f" WHERE tenant_id='{tenant}' ORDER BY version",
                )
                return head, history

        head, history = asyncio.run(_main())
        assert head == [(3, True, "app_a", "v1 text")]
        assert history == [(1, ), (2, ), (3, )]

    def test_rollback_target_missing(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                with pytest.raises(TenantConfigTargetVersionNotFoundError):
                    await repo.rollback(tenant, 1, 7)
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions WHERE tenant_id='{tenant}'",
                )
                return history

        history = asyncio.run(_main())
        assert history == [(1, )]

    def test_rollback_to_same_content_still_bumps(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant, instruction="same"))
                await repo.update(tenant, 1, self._draft(instruction="changed"))
                rolled = await repo.rollback(tenant, 2, 2)
                assert rolled.version == 3
                assert rolled.app.instruction == "changed"

        asyncio.run(_main())

    def test_rollback_wrong_expected_version_conflicts(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                with pytest.raises(TenantConfigVersionConflictError):
                    await repo.rollback(tenant, 9, 1)

        asyncio.run(_main())

    # --- list_versions ---

    def test_list_versions_descending_with_exclusive_paging(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant, instruction="v1"))
                await repo.update(tenant, 1, self._draft(instruction="v2"))
                await repo.update(tenant, 2, self._draft(instruction="v3"))

                all_versions = await repo.list_versions(tenant)
                assert [c.version for c in all_versions] == [3, 2, 1]
                assert all_versions[2].app.instruction == "v1"

                before_v3 = await repo.list_versions(tenant, before_version=3)
                assert [c.version for c in before_v3] == [2, 1]

                limited = await repo.list_versions(tenant, limit=1)
                assert [c.version for c in limited] == [3]

        asyncio.run(_main())

    def test_list_versions_unknown_tenant_returns_empty(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                assert await repo.list_versions(tenant) == ()

        asyncio.run(_main())

    # --- concurrency ---

    def test_concurrent_same_expected_version_exactly_one_wins(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                outcomes = await asyncio.gather(
                    repo.update(tenant, 1, self._draft(instruction="from A")),
                    repo.update(tenant, 1, self._draft(instruction="from B")),
                    return_exceptions=True,
                )
                head = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_configs WHERE tenant_id='{tenant}'",
                )
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions"
                    f" WHERE tenant_id='{tenant}' ORDER BY version",
                )
                return outcomes, head, history

        outcomes, head, history = asyncio.run(_main())
        succeeded = [o for o in outcomes if not isinstance(o, BaseException)]
        conflicted = [o for o in outcomes if isinstance(o, TenantConfigVersionConflictError)]
        assert len(succeeded) == 1, f"expected exactly one success, got {outcomes!r}"
        assert len(conflicted) == 1, f"expected exactly one conflict, got {outcomes!r}"
        assert head == [(2, )]
        assert history == [(1, ), (2, )], "no version gaps or duplicates after race"

    # --- fault injection: snapshot insert failure rolls back head update ---

    def test_snapshot_failure_rolls_back_head_update(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(self._config(tenant))
                await _execute(
                    self.url,
                    "CREATE OR REPLACE FUNCTION trpc_abort_version_insert() RETURNS trigger AS $$"
                    "BEGIN RAISE EXCEPTION 'injected snapshot failure'; END;"
                    "$$ LANGUAGE plpgsql",
                )
                await _execute(
                    self.url,
                    f"CREATE TRIGGER trpc_fail_versions_{tenant}"
                    f" BEFORE INSERT ON tenant_config_versions"
                    f" FOR EACH ROW WHEN (NEW.tenant_id = '{tenant}')"
                    f" EXECUTE FUNCTION trpc_abort_version_insert()",
                )
                try:
                    with pytest.raises(TenantRepositoryDataError):
                        await repo.update(tenant, 1, self._draft())
                finally:
                    await _execute(self.url, f"DROP TRIGGER IF EXISTS trpc_fail_versions_{tenant}"
                                   f" ON tenant_config_versions")
                    await _execute(self.url, "DROP FUNCTION IF EXISTS trpc_abort_version_insert()")
                head = await _fetch_all(
                    self.url,
                    f"SELECT version, enabled, app_id, instruction FROM tenant_configs"
                    f" WHERE tenant_id='{tenant}'",
                )
                history = await _fetch_all(
                    self.url,
                    f"SELECT version FROM tenant_config_versions WHERE tenant_id='{tenant}'",
                )
                return head, history

        head, history = asyncio.run(_main())
        assert head == [(1, True, "app_a", "instr")], \
            "head must roll back when snapshot insert fails"
        assert history == [(1, )]


@requires_docker
class TestSqlReadAfterWrites:
    """Read path still serves the current head after write operations."""

    @pytest.fixture(autouse=True)
    def _migrated(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        self.url = postgres_url

    def test_get_returns_updated_head(self):
        tenant = _unique_tenant()

        async def _main():
            async with _RepoSession(self.url) as repo:
                await repo.create(
                    TenantConfig(
                        tenant_id=tenant,
                        enabled=True,
                        version=1,
                        app=AgentAppConfig(
                            app_id="app_a",
                            instruction="before",
                            model_profile="default",
                            allowed_tools=[],
                        ),
                        governance=make_governance(),
                        backend_profile=make_backend_profile(),
                        audit_policy=make_audit_policy(),
                    ))
                await repo.update(
                    tenant, 1,
                    TenantConfigDraft(
                        enabled=False,
                        app=AgentAppConfig(
                            app_id="app_b",
                            instruction="after",
                            model_profile="default",
                            allowed_tools=["get_current_time"],
                        ),
                        governance=make_governance(),
                        backend_profile=make_backend_profile(),
                        audit_policy=make_audit_policy(),
                    ))
                fetched = await repo.get(tenant)
                assert fetched is not None
                assert fetched.version == 2
                assert fetched.enabled is False
                assert fetched.app.app_id == "app_b"

        asyncio.run(_main())


@requires_docker
class TestImportWritesHistory:
    """P1-1: import_tenant_configs must write both head and v1 history snapshot."""

    @pytest.fixture(autouse=True)
    def _migrated(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        self.url = postgres_url

    def test_import_creates_v1_history(self, tmp_path):
        """Importing a new tenant must create both tenant_configs and tenant_config_versions rows."""
        tenant = _unique_tenant()
        json_file = tmp_path / "tenants.json"
        json_file.write_text(f"""{{
            "schema_version": 1,
            "tenants": [{{
                "tenant_id": "{tenant}",
                "enabled": true,
                "version": 1,
                "app": {{
                    "app_id": "app_import",
                    "instruction": "import instruction",
                    "model_profile": "default",
                    "allowed_tools": []
                }},
                "governance": {{
                    "allowed_channels": ["web", "web_console", "wecom", "feishu"],
                    "allowed_user_ids": [],
                    "tool_decisions": {{}},
                    "content_policy": {{
                        "enabled": false, "input_action": "block", "output_action": "block"
                    }},
                    "limits": null
                }},
                "backend_profile": {{
                    "state_backend": "redis",
                    "artifact_backend": "s3",
                    "knowledge_backend": "sql",
                    "audit_backend": "sql"
                }},
                "audit_policy": {{"retention_days": 365, "delivery_events": "all"}}
            }}]
        }}""")

        async def _main():
            from pathlib import Path
            from trpc_service.storage.tenant_import import import_tenant_configs

            inserted, skipped = await import_tenant_configs(Path(json_file), environ={
                "TRPC_DATABASE_URL": self.url,
            })
            assert inserted == 1
            assert skipped == 0

            head = await _fetch_all(
                self.url,
                f"SELECT tenant_id, version, app_id FROM tenant_configs WHERE tenant_id='{tenant}'",
            )
            history = await _fetch_all(
                self.url,
                f"SELECT version, app_id, instruction FROM tenant_config_versions"
                f" WHERE tenant_id='{tenant}' ORDER BY version",
            )
            return head, history

        head, history = asyncio.run(_main())
        assert head == [(tenant, 1, "app_import")], "head must be inserted"
        assert history == [(1, "app_import", "import instruction")], \
            "v1 history snapshot must be created in same transaction"
