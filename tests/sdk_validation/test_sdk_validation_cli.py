"""Subprocess contract tests for the SDK validation CLI."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from typing import Any

import pytest

import trpc_service.agent.sdk_validation as sdk_validation
from trpc_service.agent.sdk_validation import main


EXPECTED_STAGES = [
    "version",
    "initialization",
    "single_turn",
    "event_finalization",
    "session_continuity",
    "session_isolation",
    "offline_safety",
]


@dataclass(frozen=True)
class CliResult:
    returncode: int
    stdout: str
    stderr: str


def run_cli(capsys: pytest.CaptureFixture[str], *arguments: str) -> CliResult:
    try:
        returncode = main(list(arguments))
    except SystemExit as exit_signal:
        returncode = int(exit_signal.code)
    captured = capsys.readouterr()
    return CliResult(returncode, captured.out, captured.err)


def normalized(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("run_id", None)
    return result


def test_help_and_unknown_argument_contract(capsys: pytest.CaptureFixture[str]) -> None:
    help_result = run_cli(capsys, "--help")
    assert help_result.returncode == 0
    assert "usage:" in help_result.stdout.lower()

    unknown_result = run_cli(capsys, "--unknown")
    assert unknown_result.returncode == 2
    assert unknown_result.stdout == ""
    assert "unrecognized arguments" in unknown_result.stderr.lower()


def test_human_output_reports_all_stages_and_pass_result(capsys: pytest.CaptureFixture[str]) -> None:
    result = run_cli(capsys)

    assert result.returncode == 0
    assert "SDK target: 1.1.19" in result.stdout
    assert "SDK actual: 1.1.19" in result.stdout
    output_lines = result.stdout.splitlines()
    for stage in EXPECTED_STAGES:
        assert f"{stage}: PASS" in output_lines
    assert "RESULT: PASS" in output_lines


def test_json_output_matches_schema_and_four_turn_contract(capsys: pytest.CaptureFixture[str]) -> None:
    result = run_cli(capsys, "--json")

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "1"
    assert payload["status"] == "passed"
    assert payload["sdk"]["expected_version"] == "1.1.19"
    assert payload["sdk"]["distribution_version"] == "1.1.19"
    assert payload["sdk"]["module_version"] == "1.1.19"
    assert [item["stage"] for item in payload["stages"]] == EXPECTED_STAGES
    assert all(item["status"] == "passed" for item in payload["stages"])
    assert payload["event_count"] >= 4
    assert payload["final_response_count"] == 4
    assert payload["credential_required"] is False
    assert payload["external_model_calls"] == 0


def test_json_runs_are_equal_after_dynamic_fields_are_removed(capsys: pytest.CaptureFixture[str]) -> None:
    first = run_cli(capsys, "--json")
    second = run_cli(capsys, "--json")

    assert first.returncode == second.returncode == 0
    assert normalized(json.loads(first.stdout)) == normalized(json.loads(second.stdout))


def test_cli_output_never_echoes_environment_credentials(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = secrets.token_urlsafe(32)
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    result = run_cli(capsys, "--json")

    assert result.returncode == 0
    assert secret not in result.stdout
    assert secret not in result.stderr


def test_validation_failure_returns_one_with_report_on_stdout(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    real_run_validation = sdk_validation.run_validation

    async def failing_validation():
        return await real_run_validation(expected_version="0.0.0-invalid")

    monkeypatch.setattr(sdk_validation, "run_validation", failing_validation)
    result = run_cli(capsys, "--json")

    assert result.returncode == 1
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert payload["stages"][0]["error_type"] == "version_mismatch"
