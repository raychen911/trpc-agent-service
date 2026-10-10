import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
INIT_SCRIPT = ROOT / "deploy" / "postgres" / "init.sh"
BOOTSTRAP_SQL = ROOT / "deploy" / "postgres" / "bootstrap.sql"


def test_postgres_init_imports_both_passwords_without_secret_argv() -> None:
    script = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "</run/secrets/runtime_database_password" in script
    assert "</run/secrets/migration_database_password" in script
    assert 'export TRPC_INIT_RUNTIME_PASSWORD="$runtime_password"' in script
    assert 'export TRPC_INIT_MIGRATION_PASSWORD="$migration_password"' in script
    assert r"\getenv runtime_password TRPC_INIT_RUNTIME_PASSWORD" in script
    assert r"\getenv migration_password TRPC_INIT_MIGRATION_PASSWORD" in script
    assert "trap cleanup_passwords EXIT" in script
    assert "unset runtime_password migration_password" in script
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in script
    assert "CREATE EXTENSION IF NOT EXISTS vector" in script
    assert not re.search(r"--(?:set|variable)=[^\n]*password=", script)


def test_postgres_init_separates_runtime_and_global_worker_roles() -> None:
    script = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "CREATE ROLE trpc_runtime LOGIN NOSUPERUSER" in script
    assert "NOINHERIT NOBYPASSRLS" in script
    assert "CREATE ROLE trpc_worker LOGIN NOSUPERUSER" in script
    assert "NOINHERIT BYPASSRLS" in script
    assert "GRANT CONNECT ON DATABASE %I TO trpc_worker" in script
    assert "/run/secrets/worker_database_password" in script


def test_postgres_init_provisions_a_non_bypass_metrics_role_from_secret() -> None:
    script = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "/run/secrets/metrics_database_password" in script
    assert 'export TRPC_INIT_METRICS_PASSWORD="$metrics_password"' in script
    assert r"\getenv metrics_password TRPC_INIT_METRICS_PASSWORD" in script
    assert "CREATE ROLE trpc_metrics LOGIN NOSUPERUSER" in script
    assert "NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS" in script
    assert "GRANT CONNECT ON DATABASE %I TO trpc_metrics" in script
    assert "ALTER ROLE trpc_metrics PASSWORD %L" in script


def test_postgres_bootstrap_declares_metrics_and_complete_role_attributes() -> None:
    script = BOOTSTRAP_SQL.read_text(encoding="utf-8")

    for role in ("trpc_migration", "trpc_runtime", "trpc_metrics"):
        assert f"CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE" in script
    assert "CREATE ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE" in script
    assert script.count("NOINHERIT NOREPLICATION NOBYPASSRLS") >= 6
    assert script.count("NOINHERIT NOREPLICATION BYPASSRLS") >= 2
    assert "ALTER ROLE trpc_metrics PASSWORD %L" in script
    assert "GRANT CONNECT ON DATABASE %I TO trpc_metrics" in script
