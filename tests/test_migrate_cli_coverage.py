"""CLI dispatch coverage for migration and release operations."""

from __future__ import annotations

import json
import sys
from unittest.mock import patch

import pytest

from trpc_service import migrate
from trpc_service.database import DatabaseMigrationError
from trpc_service.migration_state import MigrationPhase, new_migration


def invoke(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["trpc-agent-migrate", *args])
    migrate.main()


def test_db_commands_dispatch_success_failure_and_check_gate(monkeypatch):
    with patch.object(migrate, "upgrade_database", return_value={"status": "pass"}) as upgrade:
        invoke(monkeypatch, "db", "upgrade", "--sql-dsn", "dsn", "--revision", "v1")
        upgrade.assert_called_once_with("dsn", "v1")
    with patch.object(migrate, "database_status", return_value={"status": "pass"}) as status:
        invoke(monkeypatch, "db", "current", "--sql-dsn", "dsn")
        status.assert_called_once_with("dsn")
    with patch.object(migrate, "database_status", return_value={"status": "fail"}), pytest.raises(SystemExit) as exc:
        invoke(monkeypatch, "db", "check", "--sql-dsn", "dsn")
    assert exc.value.code == 1
    with patch.object(migrate, "downgrade_database", return_value={"status": "pass"}) as downgrade:
        invoke(monkeypatch, "db", "downgrade", "--sql-dsn", "dsn", "--revision", "-2", "--allow-destructive")
        downgrade.assert_called_once_with("dsn", "-2", allow_destructive=True)
    with patch.object(migrate, "upgrade_database", side_effect=DatabaseMigrationError("bad db")), pytest.raises(SystemExit):
        invoke(monkeypatch, "db", "upgrade", "--sql-dsn", "dsn")


def test_migration_run_and_state_commands_cover_success_and_parser_errors(tmp_path, monkeypatch):
    result = {"phase": "cleanup", "tenant_id": "tenant"}
    with patch.object(migrate, "execute_migration", return_value=result) as execute:
        invoke(
            monkeypatch,
            "migration-run", "--tenant", "tenant", "--source-backend", "memory", "--target-backend", "sql",
            "--source-url", "source", "--target-url", "target", "--source-sql-dsn", "source-dsn",
            "--target-sql-dsn", "target-dsn", "--state-file", str(tmp_path / "state"),
            "--snapshot", str(tmp_path / "snapshot"), "--data-dir", str(tmp_path / "data"),
            "--inject-failure-phase", "backfill",
        )
        execute.assert_called_once()
        assert execute.call_args.kwargs["inject_failure_phase"] == "backfill"
    with patch.object(migrate, "execute_migration", side_effect=RuntimeError("migration failed")), pytest.raises(SystemExit):
        invoke(
            monkeypatch, "migration-run", "--tenant", "tenant", "--source-backend", "memory",
            "--target-backend", "sql", "--state-file", str(tmp_path / "state"), "--snapshot", str(tmp_path / "snapshot"),
        )

    output = tmp_path / "new-state.json"
    invoke(
        monkeypatch, "migration-state", "--mode", "new", "--output", str(output),
        "--migration-id", "migration-1", "--tenant", "tenant", "--source-backend", "memory", "--target-backend", "sql",
    )
    assert json.loads(output.read_text(encoding="utf-8"))["migration_id"] == "migration-1"
    with pytest.raises(SystemExit):
        invoke(monkeypatch, "migration-state", "--mode", "new", "--output", str(output))

    state = new_migration("migration-2", "tenant", "memory", "sql")
    state.transition(MigrationPhase.BACKFILL, actor="test")
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps(state.to_dict()), encoding="utf-8")
    for phase in (MigrationPhase.FAILED.value, MigrationPhase.ROLLED_BACK.value, MigrationPhase.SHADOW_READ.value):
        target = tmp_path / f"{phase}.json"
        invoke(
            monkeypatch, "migration-state", "--mode", "transition", "--input", str(state_file), "--phase", phase,
            "--output", str(target), "--actor", "operator", "--reason", "test reason",
        )
        assert target.exists()
    with pytest.raises(SystemExit):
        invoke(monkeypatch, "migration-state", "--mode", "transition", "--output", str(output))


def test_remaining_cli_commands_use_their_adapters_and_close_storage(tmp_path, monkeypatch):
    snapshot = {"tenant_id": "tenant", "sessions": [], "checksums": {}}
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(snapshot), encoding="utf-8")
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps({"tenant_id": "tenant", "source_backend": "memory", "target_backend": "sql"}), encoding="utf-8")

    invoke(monkeypatch, "cutover-plan", "--input", str(plan_file))
    with patch.object(migrate, "apply_rls_migration_dsn") as apply_rls:
        invoke(monkeypatch, "rls", "--sql-dsn", "dsn", "--app-role", "app", "--admin-role", "admin")
        apply_rls.assert_called_once_with("dsn", app_role="app", admin_role="admin")
    with patch.object(migrate, "ensure_postgres_schema") as schema:
        invoke(monkeypatch, "schema", "--backend", "postgres", "--sql-dsn", "dsn")
        schema.assert_called_once_with("dsn")

    class Storage:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    storage = Storage()
    with patch.object(migrate, "create_storage", return_value=storage), patch.object(
        migrate, "export_tenant", return_value=snapshot
    ) as export:
        output = tmp_path / "export.json"
        invoke(monkeypatch, "export", "--tenant", "tenant", "--output", str(output), "--backend", "sql", "--sql-dsn", "dsn")
        export.assert_called_once_with(storage, "tenant")
        assert storage.closed and output.exists()

    storage = Storage()
    with patch.object(migrate, "create_storage", return_value=storage), patch.object(
        migrate, "verify_tenant", return_value={"ok": True}
    ) as verify:
        invoke(monkeypatch, "verify", "--input", str(input_file), "--backend", "sql")
        verify.assert_called_once_with(storage, snapshot)
        assert storage.closed

    storage = Storage()
    with patch.object(migrate, "create_storage", return_value=storage), patch.object(migrate, "import_tenant") as import_tenant:
        invoke(monkeypatch, "import", "--input", str(input_file), "--backend", "redis", "--redis-url", "redis://fake")
        import_tenant.assert_called_once_with(storage, snapshot)
        assert storage.closed

    storage = Storage()
    with patch.object(migrate, "create_storage", return_value=storage), patch.object(
        migrate, "export_tenant", side_effect=RuntimeError("export failed")
    ), pytest.raises(RuntimeError):
        invoke(monkeypatch, "export", "--tenant", "tenant", "--output", str(tmp_path / "bad"), "--backend", "sql")
    assert storage.closed
