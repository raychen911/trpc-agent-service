from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.report_io import atomic_write_json


def test_atomic_write_json_replaces_complete_strict_document(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    output.write_text('{"old":true}', encoding="utf-8")

    rendered = atomic_write_json(output, {"status": "pass", "count": 2})

    assert json.loads(output.read_text(encoding="utf-8")) == {"status": "pass", "count": 2}
    assert rendered.endswith("\n")
    assert not list(tmp_path.glob(".report.json.*.tmp"))


def test_atomic_write_json_rejects_non_finite_numbers(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        atomic_write_json(tmp_path / "report.json", {"latency": float("nan")})


def test_atomic_write_json_rejects_symlink_target(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(ValueError, match="symlink"):
        atomic_write_json(link, {"status": "pass"})
