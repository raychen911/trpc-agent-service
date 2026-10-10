from __future__ import annotations

import json
import sys
from pathlib import Path
from subprocess import CompletedProcess

import scripts.contract_gate as contract_gate
from scripts.contract_gate import (
    CASES,
    DEFAULT_OUTPUTS,
    _backend_production_status,
    _image_digest,
    _junit_counts,
    _junit_summary,
    _write_history_report,
    _write_report,
    backend_identities,
)


def test_junit_counts_distinguish_executed_and_skipped_tests(tmp_path) -> None:
    report = tmp_path / "junit.xml"
    report.write_text(
        '<testsuites><testsuite tests="4" failures="1" errors="0" skipped="1" /></testsuites>',
        encoding="utf-8",
    )

    assert _junit_counts(report) == {
        "tests": 4,
        "passed": 2,
        "failures": 1,
        "errors": 0,
        "skipped": 1,
    }


def test_junit_summary_keeps_only_failed_case_identity(tmp_path) -> None:
    report = tmp_path / "junit.xml"
    report.write_text(
        """<testsuites><testsuite tests="3" failures="1" errors="1" skipped="0">
        <testcase classname="tests.test_contract" name="test_ok" />
        <testcase classname="tests.test_contract" name="test_failed">
          <failure message="database-password">secret traceback</failure>
          <system-out>secret stdout</system-out>
        </testcase>
        <testcase classname="tests.test_contract" name="test_error">
          <error message="api-token">secret error</error>
          <system-err>secret stderr</system-err>
        </testcase>
        </testsuite></testsuites>""",
        encoding="utf-8",
    )

    status, counts, failed_cases = _junit_summary(report)

    assert status == "available"
    assert counts == {
        "tests": 3,
        "passed": 1,
        "failures": 1,
        "errors": 1,
        "skipped": 0,
    }
    assert failed_cases == [
        {
            "outcome": "failure",
            "classname": "tests.test_contract",
            "testcase": "test_failed",
        },
        {
            "outcome": "error",
            "classname": "tests.test_contract",
            "testcase": "test_error",
        },
    ]
    serialized = json.dumps(failed_cases)
    for secret in (
        "database-password",
        "secret traceback",
        "secret stdout",
        "api-token",
        "secret error",
        "secret stderr",
    ):
        assert secret not in serialized


def test_history_report_is_exclusive_and_cannot_be_overwritten(tmp_path) -> None:
    history = tmp_path / "history" / "backend-run.json"
    _write_history_report(history, {"run_id": "first"})

    try:
        _write_history_report(history, {"run_id": "replacement"})
    except FileExistsError:
        pass
    else:
        raise AssertionError("diagnostic history was overwritten")

    assert json.loads(history.read_text(encoding="utf-8")) == {"run_id": "first"}


def test_backend_production_status_never_promotes_all_skipped_suite() -> None:
    status, reasons = _backend_production_status(
        environment_ready=True,
        passed=True,
        counts={"tests": 4, "passed": 0, "failures": 0, "errors": 0, "skipped": 4},
    )

    assert status == "not_run"
    assert reasons == ["live backend integration suite executed no passing test cases"]


def test_backend_production_status_requires_complete_success() -> None:
    assert _backend_production_status(
        environment_ready=True,
        passed=True,
        counts={"tests": 4, "passed": 4, "failures": 0, "errors": 0, "skipped": 0},
    ) == ("pass", [])
    assert _backend_production_status(
        environment_ready=True,
        passed=False,
        counts={"tests": 4, "passed": 3, "failures": 1, "errors": 0, "skipped": 0},
    ) == ("fail", ["live backend integration suite failed"])


def test_backend_production_status_requires_runtime_evidence() -> None:
    assert _backend_production_status(
        environment_ready=True,
        passed=True,
        counts={"tests": 1, "passed": 1, "failures": 0, "errors": 0, "skipped": 0},
        runtime_evidence_ready=False,
    ) == ("not_run", ["live backend runtime evidence is unavailable"])


def test_backend_identity_report_contains_only_irreversible_hashes() -> None:
    values = backend_identities(
        postgres_dsn="postgresql+asyncpg://user:postgres-secret@db.example:5432/app",
        redis_url="redis://:redis-secret@cache.example:6379/2",
        s3_endpoint="https://s3.example:9443",
        s3_bucket="bucket-with-secret-name",
    )

    assert values is not None
    serialized = json.dumps(values, sort_keys=True)
    for secret in ("postgres-secret", "redis-secret", "bucket-with-secret-name"):
        assert secret not in serialized
    assert set(values) == {"postgres", "redis", "s3"}
    assert all(
        len(identity[key]) == 64
        for identity in values.values()
        for key in ("endpoint_sha256", "resource_sha256")
    )


def test_image_digest_rejects_non_immutable_placeholders() -> None:
    assert _image_digest("sha256:" + "0" * 64) is None
    assert _image_digest("sha256:" + "f" * 64) is None
    assert _image_digest("sha256:" + "A" * 64) == "sha256:" + "a" * 64


def test_default_output_keeps_offline_names_and_uses_release_backend_name() -> None:
    assert DEFAULT_OUTPUTS["fault"].name == "fault-offline.json"
    assert DEFAULT_OUTPUTS["migration"].name == "migration-offline.json"
    assert DEFAULT_OUTPUTS["backend"].name == "backend-compose.json"


def test_migration_contract_runs_full_synthetic_and_release_paths() -> None:
    assert CASES["migration"] == (
        "tests/unit/test_migration.py",
        "tests/unit/test_migration_acceptance.py",
        "tests/unit/test_migration_full_acceptance.py",
        "tests/unit/test_production_migration_control.py",
        "tests/unit/test_migration_live.py",
        "tests/unit/test_migration_release_paths.py",
    )


def test_report_writer_rejects_symlink_target(tmp_path) -> None:
    target = tmp_path / "target.json"
    target.write_text("original", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        return

    try:
        _write_report(link, "replacement")
    except ValueError as error:
        assert "symlink" in str(error)
    else:
        raise AssertionError("report writer followed a symlink")
    assert target.read_text(encoding="utf-8") == "original"


def test_backend_report_binds_junit_runtime_and_safe_identities(tmp_path, monkeypatch) -> None:
    seen_command: list[str] = []

    def fake_run(command, **_kwargs):
        seen_command.extend(command)
        junit = Path(
            next(item.split("=", 1)[1] for item in command if item.startswith("--junitxml="))
        )
        junit.write_text(
            '<testsuites><testsuite tests="2" failures="0" errors="0" skipped="0" /></testsuites>',
            encoding="utf-8",
        )
        return CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(contract_gate.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["contract_gate.py", "backend", "--output", str(tmp_path / "backend-compose.json")],
    )
    monkeypatch.setenv(
        "TRPC_TEST_POSTGRES_DSN",
        "postgresql+asyncpg://user:postgres-canary@db.example:5432/app",
    )
    monkeypatch.setenv(
        "TRPC_TEST_POSTGRES_WORKER_DSN",
        "postgresql+asyncpg://trpc_worker:worker-canary@db.example:5432/app",
    )
    monkeypatch.setenv("TRPC_TEST_REDIS_URL", "redis://:redis-canary@cache.example:6379/2")
    monkeypatch.setenv("TRPC_TEST_S3_ENDPOINT", "https://s3.example:9443")
    monkeypatch.setenv("TRPC_TEST_S3_ACCESS_KEY", "access-canary")
    monkeypatch.setenv("TRPC_TEST_S3_SECRET_KEY", "secret-canary")
    monkeypatch.setenv("TRPC_TEST_S3_BUCKET", "bucket-canary")
    monkeypatch.setenv("TRPC_TEST_IMAGE_DIGEST", "sha256:" + "A" * 64)

    assert contract_gate.main() == 0
    assert "--allow-real-tests" in seen_command

    result = json.loads((tmp_path / "backend-compose.json").read_text(encoding="utf-8"))
    assert result["schema_version"] == 1
    assert result["production_gate"] == "pass"
    assert result["candidate"]["runtime_attestation"]["status"] == "pass"
    assert result["candidate"]["lineage"]["image_digest"] == "sha256:" + "a" * 64
    assert result["candidate"]["junit_status"] == "available"
    assert result["candidate"]["failed_cases"] == []
    history = tmp_path / result["candidate"]["diagnostic_history"]["path"]
    assert history.is_file()
    assert json.loads(history.read_text(encoding="utf-8"))["run_id"] == result["run_id"]
    serialized = json.dumps(result)
    for canary in (
        "postgres-canary",
        "worker-canary",
        "redis-canary",
        "access-canary",
        "secret-canary",
        "bucket-canary",
    ):
        assert canary not in serialized
