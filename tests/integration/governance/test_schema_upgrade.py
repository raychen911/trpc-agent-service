from __future__ import annotations

from pathlib import Path


def test_governance_migration_is_present_and_versioned() -> None:
    migration = Path("trpc_service/storage/postgres/migrations/005_governance.sql")
    assert migration.exists(), "governance migration is not implemented"
    source = migration.read_text(encoding="utf-8")
    for table in ("governance_policy_versions", "governance_policy_active", "principal_grants", "budget_accounts", "budget_reservations"):
        assert table in source


def test_database_schema_version_includes_governance_migration() -> None:
    from trpc_service.storage.postgres import database

    assert database.SUPPORTED_SCHEMA_VERSION >= 5
