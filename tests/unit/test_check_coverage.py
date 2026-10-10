from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts import check_coverage


def _run_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    branch_minimum: float | None = None,
) -> tuple[int, dict[str, object]]:
    report = tmp_path / "coverage.json"
    output = tmp_path / "coverage-gate.json"
    report.write_text(
        json.dumps(
            {
                "totals": {
                    "percent_statements_covered": 94.6,
                    "percent_branches_covered": 88.9,
                }
            }
        ),
        encoding="utf-8",
    )
    arguments = ["check_coverage.py", str(report), "--output", str(output)]
    if branch_minimum is not None:
        arguments.extend(("--branch-minimum", str(branch_minimum)))
    monkeypatch.setattr(sys, "argv", arguments)
    status = check_coverage.main()
    return status, json.loads(output.read_text(encoding="utf-8"))


def test_default_minimum_applies_to_line_and_branch_coverage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status, result = _run_gate(monkeypatch, tmp_path)

    assert status == 1
    assert result["baseline"] == {"line_percent": 90.0, "branch_percent": 90.0}
    assert result["rejection_reasons"] == ["branch_coverage"]


def test_branch_minimum_is_reported_and_enforced_independently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status, result = _run_gate(monkeypatch, tmp_path, branch_minimum=88.0)

    assert status == 0
    assert result["baseline"] == {"line_percent": 90.0, "branch_percent": 88.0}
    assert result["gate"] == "pass"
    assert result["rejection_reasons"] == []
