"""Assembly tests for migration and standalone Outbox roles."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import subprocess
import sys

import pytest

from trpc_service.config import ServiceSettings
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager

run_outbox = importlib.import_module("trpc_service.agent.run_outbox")
migration_cli = importlib.import_module("trpc_service.migrations.run")


def test_build_outbox_relay_has_only_delivery_dependencies(monkeypatch):
    manager = TenantConfigManager()
    captured = {}

    class Store:

        def __init__(self, url):
            captured["store_url"] = url

    class Relay:

        def __init__(self, **kwargs):
            captured["relay"] = kwargs

    monkeypatch.setenv("TRPC_SERVICE_NODE_ID", "node-a")
    monkeypatch.setattr(run_outbox, "SqlMessageStore", Store)
    monkeypatch.setattr(run_outbox, "OutboxRelay", Relay)
    settings = ServiceSettings(
        role="outbox",
        mysql_url="sqlite:///outbox.db",
        redis_url="redis://unused",
        outbox_max_attempts=4,
    )

    run_outbox.build_outbox_relay(manager=manager, settings=settings)

    assert captured["store_url"] == "sqlite:///outbox.db"
    assert captured["relay"]["owner"] == "outbox-node-a"
    assert captured["relay"]["max_attempts"] == 4
    assert manager not in captured["relay"]["owned_resources"]


def test_build_outbox_relay_bootstraps_manager_and_requires_mysql(monkeypatch):
    with pytest.raises(ValueError, match="MYSQL_URL"):
        run_outbox.build_outbox_relay(manager=TenantConfigManager(), settings=ServiceSettings())

    manager = TenantConfigManager()
    tenant = Tenant(tenant_id="a", name="A", model=ModelEndpoint(model_name="m"))

    class Store:

        def __init__(self, url):
            self.url = url

    class Relay:

        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(run_outbox, "build_tenant_config_manager", lambda **kwargs: manager)
    monkeypatch.setattr(run_outbox, "load_tenants", lambda path: [tenant])
    monkeypatch.setattr(run_outbox, "SqlMessageStore", Store)
    monkeypatch.setattr(run_outbox, "OutboxRelay", Relay)
    relay = run_outbox.build_outbox_relay(
        settings=ServiceSettings(mysql_url="sqlite:///db", tenants_config="tenants.yaml"))
    assert manager.get("a") is not None
    assert manager in relay.kwargs["owned_resources"]


async def test_outbox_main_runs_and_shuts_down(monkeypatch):
    calls = []

    class Relay:

        async def run(self, interval):
            calls.append(("run", interval))

        async def close(self):
            calls.append(("close", 0))

    monkeypatch.setenv("TRPC_SERVICE_MYSQL_URL", "sqlite:///db")
    monkeypatch.setenv("TRPC_SERVICE_OUTBOX_POLL_INTERVAL_SECONDS", "2")
    monkeypatch.setattr(run_outbox, "build_outbox_relay", lambda settings: Relay())
    monkeypatch.setattr(run_outbox, "shutdown_telemetry", lambda: calls.append(("shutdown", 0)))
    await run_outbox.run()
    assert calls == [("run", 2.0), ("close", 0), ("shutdown", 0)]


def test_outbox_console_entrypoint(monkeypatch):
    calls = []

    monkeypatch.setattr(run_outbox.asyncio, "run", lambda coroutine: calls.append(coroutine))
    marker = object()
    monkeypatch.setattr(run_outbox, "run", lambda: marker)

    run_outbox.main()

    assert calls == [marker]


def test_migration_cli_check_and_missing_database(monkeypatch, tmp_path, capsys):
    schema = tmp_path / "schema.sql"
    schema.write_text("CREATE TABLE demo (id INTEGER)", encoding="utf-8")
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0002_more.sql").write_text("CREATE TABLE more (id INTEGER)", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_MYSQL_URL", f"sqlite:///{tmp_path / 'db.sqlite'}")

    migration_cli.main([
        "--check",
        "--schema-file",
        str(schema),
        "--migrations-dir",
        str(migrations),
    ])
    assert "0001 baseline" in capsys.readouterr().out

    monkeypatch.delenv("TRPC_SERVICE_MYSQL_URL")
    with pytest.raises(ValueError, match="MYSQL_URL"):
        migration_cli.main(["--check", "--schema-file", str(schema)])


def test_migration_module_entrypoint_executes_check(tmp_path):
    schema = tmp_path / "schema.sql"
    schema.write_text("CREATE TABLE demo (id INTEGER)", encoding="utf-8")
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    environment = os.environ.copy()
    environment["TRPC_SERVICE_MYSQL_URL"] = f"sqlite:///{tmp_path / 'unused.db'}"
    root = Path(__file__).resolve().parents[2]

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "trpc_service.migrations.run",
            "--check",
            "--schema-file",
            str(schema),
            "--migrations-dir",
            str(migrations),
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "0001 baseline"
