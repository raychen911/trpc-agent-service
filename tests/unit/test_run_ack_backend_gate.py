from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType

import pytest


def _load_runner() -> ModuleType:
    path = Path("runs/multitenant/run-ack-backend-gate.py")
    spec = importlib.util.spec_from_file_location("run_ack_backend_gate", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_scope_is_separate_from_backend_resource_id() -> None:
    runner = _load_runner()

    scope = runner._migration_scope("backend-ack-0128ab415eda")

    assert scope == "migration-acceptance-0128ab415eda"
    assert scope.startswith("migration-acceptance-")
    assert not scope.startswith("backend-ack-")


def test_backend_subprocess_has_bounded_timeout_and_keeps_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "child stdout", "child stderr")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    result = runner._run_subprocess(["child"], {})

    assert result.stdout == "child stdout"
    assert result.stderr == "child stderr"
    assert observed["timeout"] == runner.SUBPROCESS_TIMEOUT_SECONDS


def test_subprocess_timeout_fails_closed_and_cleans_isolation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    output = tmp_path / "backend.json"
    args = type(
        "Args",
        (),
        {
            "secret_manifest": tmp_path / "secrets.yaml",
            "image_digest": "sha256:" + "a" * 64,
            "output": output,
            "diagnostic_target": None,
        },
    )()
    isolation = type(
        "Isolation",
        (),
        {
            "environment": {},
            "summary": {"run_id": "backend-ack-test"},
            "cleanup_calls": 0,
        },
    )()

    async def fake_provision(**_kwargs: object) -> object:
        return isolation

    async def fake_cleanup() -> list[str]:
        isolation.cleanup_calls += 1
        return []

    isolation.cleanup = fake_cleanup
    monkeypatch.setattr(runner, "provision_backend_isolation", fake_provision)

    def fake_secret_data(_path: Path, _name: str) -> dict[str, str]:
        return {}

    monkeypatch.setattr(runner, "_secret_data", fake_secret_data)
    monkeypatch.setattr(runner, "_decode", lambda _data, key: f"value-{key}")

    def fake_run(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(["child"], runner.SUBPROCESS_TIMEOUT_SECONDS, "partial")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    result = asyncio.run(runner._run(args))

    assert result == 1
    assert isolation.cleanup_calls == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "fail"
    assert report["production_gate"] == "not_run"
    assert "timeout" in report["rejection_reasons"][0]
