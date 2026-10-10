from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import scripts.deployment_gate as deployment_gate


def _passing_check(
    _command: list[str], *, environment: dict[str, str] | None = None
) -> dict[str, Any]:
    return {"status": "pass", "exit_code": 0, "error": ""}


def test_default_deployment_gate_separates_static_pass_from_runtime_not_run(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "deployment.json"
    monkeypatch.setattr(deployment_gate, "_run", _passing_check)
    monkeypatch.setattr(
        sys,
        "argv",
        ["deployment_gate.py", "--output", str(output)],
    )

    assert deployment_gate.main() == 0

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["baseline"]["compose_static"] == "static_pass"
    assert report["baseline"]["kustomize_static"] == "static_pass"
    assert report["baseline"]["kubernetes_runtime"] == "not_run"
    assert report["static_gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["gate"] == "not_run"
    assert any("Kubernetes" in reason for reason in report["production_rejection_reasons"])


def test_require_production_rejects_static_only_deployment_gate(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "deployment.json"
    monkeypatch.setattr(deployment_gate, "_run", _passing_check)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "deployment_gate.py",
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert deployment_gate.main() == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["static_gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert any("Kubernetes" in reason for reason in report["rejection_reasons"])


def test_static_deployment_failure_remains_a_failure(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "deployment.json"

    def fake_run(
        command: list[str], *, environment: dict[str, str] | None = None
    ) -> dict[str, Any]:
        if command[:2] == ["kubectl", "kustomize"] and any(
            "overlays/production" in item for item in command
        ):
            return {"status": "fail", "exit_code": 1, "error": "invalid manifest"}
        return _passing_check(command, environment=environment)

    monkeypatch.setattr(deployment_gate, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["deployment_gate.py", "--output", str(output)],
    )

    assert deployment_gate.main() == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["static_gate"] == "fail"
    assert report["gate"] == "fail"
    assert report["production_gate"] == "not_run"
