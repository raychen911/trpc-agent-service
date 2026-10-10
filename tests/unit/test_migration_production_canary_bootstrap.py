from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import migration_production_canary_bootstrap as canary


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "TRPC_RUN_REAL_MIGRATION": "1",
        "TRPC_MIGRATION_PROVISION": "1",
        "TRPC_MIGRATION_PROVISION_CONFIRMATION": canary._CONFIRMATION,
        "TRPC_MIGRATION_SOURCE_REDIS_URL": "redis://source.example:6379/0",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN": ("postgresql://trpc_runtime@target.example:5432/db"),
        "TRPC_MIGRATION_TENANT_ID": "production-canary-tenant-1",
        "TRPC_MIGRATION_ID": "production-canary-migration-1",
        "TRPC_MIGRATION_APP_ID": "production-canary-app-1",
        "TRPC_MIGRATION_APP_REVISION": "1",
        "TRPC_MIGRATION_CONFIG_VERSION": "1",
        "TRPC_MIGRATION_BINDING_ID": "production-canary-binding-1",
        "TRPC_MIGRATION_BINDING_REVISION": "1",
        "TRPC_MIGRATION_OPERATOR_ID": "operator-1",
        "TRPC_MIGRATION_CHANGE_TICKET": "change-1",
        "TRPC_RELEASE_ID": "release-candidate-1",
        "TRPC_RELEASE_NONCE": "n" * 32,
        "TRPC_MIGRATION_IMAGE_DIGEST": "sha256:" + "a" * 64,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_canary_scope_requires_non_test_production_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    scope = canary._scope_from_env()
    assert scope.target_config_version == 2
    assert scope.source_profile_id.endswith("-redis")
    assert scope.target_profile_id.endswith("-postgres")

    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "migration-acceptance-tenant-1")
    with pytest.raises(ValueError, match=r"acceptance|prefix"):
        canary._scope_from_env()

    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "production-canary-test-tenant-1")
    with pytest.raises(ValueError, match="test"):
        canary._scope_from_env()


def test_canary_rejects_privileged_database_roles() -> None:
    with pytest.raises(ValueError, match="runtime role"):
        canary._runtime_role("postgresql://trpc_worker@target.example/db")
    with pytest.raises(ValueError, match="runtime role"):
        canary._runtime_role("postgresql://trpc_migration@target.example/db")
    with pytest.raises(ValueError, match="explicit runtime role"):
        canary._runtime_role("postgresql://target.example/db")


def test_canary_role_contract_rejects_owner_and_superuser() -> None:
    base = {
        "session_user": "trpc_runtime",
        "current_user": "trpc_runtime",
        "rolname": "trpc_runtime",
        "rolcanlogin": True,
        "rolsuper": False,
        "rolbypassrls": False,
        "rolcreaterole": False,
        "rolcreatedb": False,
        "rolreplication": False,
        "owns_public_objects": False,
    }
    assert canary._role_contract_error(base, "trpc_runtime") is None
    assert (
        "superuser"
        in canary._role_contract_error({**base, "rolsuper": True}, "trpc_runtime").lower()
    )
    assert "own" in canary._role_contract_error(
        {**base, "owns_public_objects": True}, "trpc_runtime"
    )
    assert "explicit runtime role" in canary._role_contract_error(
        {**base, "current_user": "other"}, "trpc_runtime"
    )


@pytest.mark.asyncio
async def test_canary_is_fail_closed_before_opening_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MIGRATION", raising=False)
    monkeypatch.delenv("TRPC_MIGRATION_PROVISION", raising=False)

    def fail_connection(*args, **kwargs):
        raise AssertionError("connections must not be opened")

    monkeypatch.setattr(canary.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(canary.asyncpg, "create_pool", fail_connection)
    result = await canary.provision()
    assert result["status"] == "not_run"
    assert result["production_gate"] == "not_run"
    assert result["credentials_emitted"] is False


def test_canary_report_contains_no_credentials(tmp_path: Path) -> None:
    scope = canary.CanaryScope(
        tenant_id="production-canary-tenant-1",
        migration_id="production-canary-migration-1",
        app_id="production-canary-app-1",
        app_revision=1,
        config_version=1,
        binding_id="production-canary-binding-1",
        binding_revision=1,
    )
    report = canary._base_report(status="pass", scope=scope)
    path = tmp_path / "canary.json"
    canary._atomic_write_json(path, report)
    rendered = json.loads(path.read_text(encoding="utf-8"))
    assert rendered["credentials_emitted"] is False
    assert "password" not in json.dumps(rendered).lower()
