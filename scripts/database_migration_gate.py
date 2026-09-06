"""Destructive migration acceptance gate for a disposable PostgreSQL database."""

from __future__ import annotations

import argparse
import json
import os
from uuid import uuid4

from trpc_service.database import (
    DatabaseMigrationError,
    database_status,
    downgrade_database,
    upgrade_database,
)


def _set_or_clear(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def create_legacy_schema(dsn: str, sentinel: str) -> None:
    """Reproduce the pre-Alembic runtime-created schema and one durable row."""

    from trpc_service.storage.postgres_knowledge import PostgresKnowledgeStore
    from trpc_service.storage.postgres_store import PostgresStorage
    from trpc_service.tenant.repository import PostgresTenantRepository

    previous_auto_create = os.environ.get("POSTGRES_AUTO_CREATE_SCHEMA")
    previous_rls = os.environ.get("POSTGRES_RLS_ENABLED")
    os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = "1"
    os.environ["POSTGRES_RLS_ENABLED"] = "0"
    resources = []
    try:
        resources = [
            PostgresStorage(dsn),
            PostgresKnowledgeStore(dsn),
            PostgresTenantRepository(dsn),
        ]
        import psycopg

        with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO idempotency (
                  tenant_id, key, status, attempt, created_at, updated_at
                ) VALUES (%s, %s, 'completed', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                ("migration-gate", sentinel),
            )
    finally:
        for resource in reversed(resources):
            resource.close()
        _set_or_clear("POSTGRES_AUTO_CREATE_SCHEMA", previous_auto_create)
        _set_or_clear("POSTGRES_RLS_ENABLED", previous_rls)


def sentinel_exists(dsn: str, sentinel: str) -> bool:
    import psycopg

    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM idempotency WHERE tenant_id=%s AND key=%s",
            ("migration-gate", sentinel),
        )
        return cursor.fetchone() is not None


def run_gate(dsn: str) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    upgraded = upgrade_database(dsn)
    checks.append({"name": "empty database upgrade", "ok": upgraded["status"] == "pass"})

    protected = False
    try:
        downgrade_database(dsn)
    except DatabaseMigrationError:
        protected = True
    checks.append({"name": "downgrade requires confirmation", "ok": protected})

    downgrade_database(dsn, "base", allow_destructive=True)
    sentinel = f"legacy-{uuid4().hex}"
    create_legacy_schema(dsn, sentinel)
    legacy = database_status(dsn)
    checks.append(
        {
            "name": "legacy schema is not falsely current",
            "ok": legacy["status"] == "fail"
            and legacy["current_revision"] is None
            and not legacy["missing_tables"],
        }
    )

    adopted = upgrade_database(dsn)
    checks.append({"name": "legacy schema adoption", "ok": adopted["status"] == "pass"})
    checks.append({"name": "legacy data preserved", "ok": sentinel_exists(dsn, sentinel)})

    passed = all(bool(check["ok"]) for check in checks)
    return {
        "status": "pass" if passed else "fail",
        "checks": checks,
        "database": adopted,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Alembic migrations on a disposable PostgreSQL database")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_MIGRATION_TEST_DSN", ""))
    parser.add_argument("--allow-destructive", action="store_true")
    args = parser.parse_args()

    if not args.dsn.strip():
        print(json.dumps({"status": "not_run", "reason": "DATABASE_MIGRATION_TEST_DSN is not set"}))
        return 0
    if not args.allow_destructive:
        print(
            json.dumps(
                {
                    "status": "not_run",
                    "reason": "pass --allow-destructive only for a disposable test database",
                }
            )
        )
        return 0
    try:
        result = run_gate(args.dsn)
    except Exception as exc:
        result = {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
