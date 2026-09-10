from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from alembic.script import ScriptDirectory

from trpc_service.database.runner import (
    EXPECTED_SCHEMA,
    DatabaseMigrationError,
    alembic_config,
    downgrade_database,
)
from trpc_service.storage.postgres_rls import RLS_TABLES


class DatabaseMigrationTests(unittest.TestCase):
    def test_alembic_has_one_expected_head(self):
        script = ScriptDirectory.from_config(alembic_config())

        self.assertEqual(script.get_heads(), ["20260903_01"])

    def test_baseline_covers_every_tenant_scoped_table(self):
        revision = importlib.import_module(
            "trpc_service.database.versions.20260903_01_initial_schema"
        )
        ddl = "\n".join(revision.UPGRADE_STATEMENTS)

        self.assertEqual(set(EXPECTED_SCHEMA), set(RLS_TABLES))
        self.assertEqual(set(revision.DROP_ORDER), set(EXPECTED_SCHEMA))
        for table in EXPECTED_SCHEMA:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", ddl)

    def test_downgrade_requires_explicit_destructive_confirmation(self):
        with patch("trpc_service.database.runner.migration_connection") as connection:
            with self.assertRaises(DatabaseMigrationError):
                downgrade_database("postgresql://unused")

        connection.assert_not_called()

    def test_legacy_schema_entry_uses_versioned_upgrade(self):
        from trpc_service.migrate import ensure_postgres_schema

        with patch("trpc_service.migrate.upgrade_database") as upgrade:
            ensure_postgres_schema("postgresql://schema-owner/database")

        upgrade.assert_called_once_with("postgresql://schema-owner/database")


if __name__ == "__main__":
    unittest.main()
