"""Integration tests for Alembic migrations against real PostgreSQL.

Uses shared fixtures from conftest.py and helpers from pg_helpers.py.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from .pg_helpers import requires_docker, run_alembic

_DEFAULT_GOV_SQL = ('{"allowed_channels": ["web", "web_console", "wecom", "feishu"],'
                    ' "allowed_user_ids": [], "tool_decisions": {},'
                    ' "content_policy": {"enabled": false, "input_action": "block",'
                    ' "output_action": "block"},'
                    ' "limits": null}')

# R1A (0008): backend_profile has no server default on head — the two
# constraint probes below must supply it explicitly so they keep testing the
# intended CHECK (version/tools), not a missing-column NOT NULL.
_DEFAULT_PROFILE_SQL = ('{"state_backend": "redis", "artifact_backend": "s3",'
                        ' "knowledge_backend": "sql", "audit_backend": "sql"}')
_DEFAULT_AUDIT_POLICY_SQL = '{"retention_days": 365, "delivery_events": "all"}'


@requires_docker
class TestAlembicMigrations:

    def test_upgrade_head_creates_table(self, postgres_url):
        result = run_alembic(postgres_url, "upgrade", "head")
        assert result.returncode == 0, f"alembic upgrade failed: {result.stderr}"

        asyncio.run(self._verify_table_exists(postgres_url))

    def test_upgrade_head_is_idempotent(self, postgres_url):
        result1 = run_alembic(postgres_url, "upgrade", "head")
        assert result1.returncode == 0, f"first upgrade failed: {result1.stderr}"

        result2 = run_alembic(postgres_url, "upgrade", "head")
        assert result2.returncode == 0, f"second upgrade failed: {result2.stderr}"
        assert "already up to date" in result2.stdout.lower() or "running" not in result2.stdout.lower()

    def test_table_has_expected_columns(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head")
        asyncio.run(self._verify_columns(postgres_url))

    def test_version_check_constraint(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head")
        asyncio.run(self._verify_version_constraint(postgres_url))

    def test_tools_array_constraint(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head")
        asyncio.run(self._verify_tools_array_constraint(postgres_url))

    def test_migration_output_does_not_leak_credentials(self, postgres_url):
        result = run_alembic(postgres_url, "upgrade", "head")
        assert "testpass" not in result.stdout
        assert "testpass" not in result.stderr

    def test_downgrade_removes_table(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head")
        result = run_alembic(postgres_url, "downgrade", "base")
        assert result.returncode == 0, f"downgrade failed: {result.stderr}"
        asyncio.run(self._verify_table_not_exists(postgres_url))

    @staticmethod
    async def _verify_table_exists(db_url: str) -> None:
        engine = create_async_engine(db_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sa.text("SELECT EXISTS ("
                            "  SELECT FROM information_schema.tables"
                            "  WHERE table_name = 'tenant_configs'"
                            ")"))
                assert result.scalar() is True
        finally:
            await engine.dispose()

    @staticmethod
    async def _verify_columns(db_url: str) -> None:
        engine = create_async_engine(db_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sa.text("SELECT column_name, data_type"
                            " FROM information_schema.columns"
                            " WHERE table_name = 'tenant_configs'"
                            " ORDER BY ordinal_position"))
                rows = result.fetchall()
                col_names = [r[0] for r in rows]
                expected = [
                    "tenant_id",
                    "enabled",
                    "version",
                    "app_id",
                    "instruction",
                    "model_profile",
                    "allowed_tools",
                    "governance",
                    "backend_profile",
                    "audit_policy",
                    "created_at",
                    "updated_at",
                ]
                for col in expected:
                    assert col in col_names, f"missing column: {col}"
        finally:
            await engine.dispose()

    @staticmethod
    async def _verify_version_constraint(db_url: str) -> None:
        engine = create_async_engine(db_url)
        try:
            async with engine.connect() as conn:
                with pytest.raises(Exception):
                    await conn.execute(
                        sa.text("INSERT INTO tenant_configs"
                                " (tenant_id, enabled, version, app_id, instruction, model_profile,"
                                " allowed_tools, governance, backend_profile)"
                                " VALUES ('test', true, 0, 'app', 'inst', 'profile', '[]'::jsonb,"
                                f" '{_DEFAULT_GOV_SQL}'::jsonb,"
                                f" '{_DEFAULT_PROFILE_SQL}'::jsonb,"
                                f" '{_DEFAULT_AUDIT_POLICY_SQL}'::jsonb)"))
        finally:
            await engine.dispose()

    @staticmethod
    async def _verify_tools_array_constraint(db_url: str) -> None:
        engine = create_async_engine(db_url)
        try:
            async with engine.connect() as conn:
                with pytest.raises(Exception):
                    await conn.execute(
                        sa.text("INSERT INTO tenant_configs"
                                " (tenant_id, enabled, version, app_id, instruction, model_profile,"
                                " allowed_tools, governance, backend_profile)"
                                " VALUES ('test', true, 1, 'app', 'inst', 'profile', '{}'::jsonb,"
                                f" '{_DEFAULT_GOV_SQL}'::jsonb,"
                                f" '{_DEFAULT_PROFILE_SQL}'::jsonb,"
                                f" '{_DEFAULT_AUDIT_POLICY_SQL}'::jsonb)"))
        finally:
            await engine.dispose()

    @staticmethod
    async def _verify_table_not_exists(db_url: str) -> None:
        engine = create_async_engine(db_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sa.text("SELECT EXISTS ("
                            "  SELECT FROM information_schema.tables"
                            "  WHERE table_name = 'tenant_configs'"
                            ")"))
                assert result.scalar() is False
        finally:
            await engine.dispose()
