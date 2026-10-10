from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from trpc_service._cli import _dependencies_ready, _role_uses_redis, app
from trpc_service.config import Role


def test_help_and_version() -> None:
    runner = CliRunner()
    assert runner.invoke(app, ["--help"]).exit_code == 0
    version = runner.invoke(app, ["--version"])
    assert version.exit_code == 0
    assert version.stdout.strip() == "0.1.0"


def test_doctor_is_offline_and_machine_readable(tmp_path) -> None:
    output = tmp_path / "doctor.json"
    result = CliRunner().invoke(app, ["doctor", "--output", str(output)])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["gate"] == "pass"
    assert payload["candidate"] == "trpc-agent-py==1.1.19"
    assert payload["case_deltas"]["sdk_locked_1_1_19"] is True


@pytest.mark.asyncio
async def test_dependency_readiness_checks_database_and_optional_redis() -> None:
    class Repository:
        def __init__(self, ready: bool) -> None:
            self.value = ready

        async def ready(self) -> bool:
            return self.value

    class Redis:
        def __init__(self, ready: bool) -> None:
            self.value = ready

        async def ping(self) -> bool:
            return self.value

    assert await _dependencies_ready(Repository(True))
    assert await _dependencies_ready(Repository(True), Redis(True))
    assert not await _dependencies_ready(Repository(False), Redis(True))
    assert not await _dependencies_ready(Repository(True), Redis(False))


def test_runtime_role_dependency_matrix() -> None:
    assert _role_uses_redis(Role.GATEWAY)
    assert _role_uses_redis(Role.WORKER)
    assert _role_uses_redis(Role.OUTBOX_DISPATCHER)
    assert _role_uses_redis(Role.POST_TURN_PROJECTOR)
    assert not _role_uses_redis(Role.ADMIN)
    assert not _role_uses_redis(Role.CHANNEL_DISPATCHER)
    # The connector owns the encrypted Redis emergency buffer used when
    # PostgreSQL is temporarily unavailable after callback authentication.
    assert _role_uses_redis(Role.WECOM_CONNECTOR)
    assert not _role_uses_redis(Role.SESSION_RECOVERY)
    assert not _role_uses_redis(Role.ARTIFACT_GC)
