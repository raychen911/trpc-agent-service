#!/usr/bin/env python3
"""Run selected deterministic fault or migration contracts and write JSON first."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlsplit

# Keep the documented ``python scripts/contract_gate.py`` invocation working
# as well as ``python -m scripts.contract_gate``.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import build_evidence, canonical_sha256, runtime_fingerprint
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.contract_gate"
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
REPORT_SCHEMA_VERSION = 1
BACKEND_SELECTOR = "tests/integration"
DEFAULT_OUTPUTS = {
    "fault": Path("runs/multitenant/fault-offline.json"),
    "migration": Path("runs/multitenant/migration-offline.json"),
    "backend": Path("runs/multitenant/backend-compose.json"),
}
CASES = {
    "fault": (
        "tests/unit/test_worker_consistency.py",
        "tests/unit/test_queue_dispatchers_and_projection.py",
        "tests/unit/test_agent_extended.py",
    ),
    "migration": (
        "tests/unit/test_migration.py",
        "tests/unit/test_migration_acceptance.py",
        "tests/unit/test_migration_full_acceptance.py",
        "tests/unit/test_production_migration_control.py",
        "tests/unit/test_migration_live.py",
        "tests/unit/test_migration_release_paths.py",
    ),
    "backend": (BACKEND_SELECTOR,),
}

BACKEND_ENV = (
    "TRPC_TEST_POSTGRES_DSN",
    "TRPC_TEST_POSTGRES_WORKER_DSN",
    "TRPC_TEST_REDIS_URL",
    "TRPC_TEST_S3_ENDPOINT",
    "TRPC_TEST_S3_ACCESS_KEY",
    "TRPC_TEST_S3_SECRET_KEY",
    "TRPC_TEST_S3_BUCKET",
    # Production backend evidence must identify the image that exercised the
    # external services.  A source/runtime fingerprint without image lineage
    # cannot be promoted by release_gate.
    "TRPC_TEST_IMAGE_DIGEST",
)


def _image_digest(value: str) -> str | None:
    """Return a normalized immutable image digest, or ``None`` if invalid."""

    digest = value.strip().lower()
    if IMAGE_DIGEST_RE.fullmatch(digest) is None:
        return None
    if digest in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}:
        return None
    return digest


def _endpoint_identity(value: str, *, kind: str) -> tuple[str, str] | None:
    """Hash endpoint and resource identities without retaining URL contents."""

    try:
        parsed = urlsplit(value.strip())
        hostname = parsed.hostname
        port = parsed.port
    except (AttributeError, ValueError):
        return None
    scheme = parsed.scheme.lower()
    if not scheme or not hostname:
        return None
    if port is None:
        port = {
            "postgres": 5432,
            "postgresql": 5432,
            "postgresql+asyncpg": 5432,
            "redis": 6379,
            "rediss": 6379,
            "http": 80,
            "https": 443,
        }.get(scheme)
    if port is None or not 1 <= port <= 65535:
        return None
    endpoint = canonical_sha256(
        {
            "kind": kind,
            "scheme": scheme,
            "hostname": hostname.lower(),
            "port": port,
        }
    )
    resource = canonical_sha256({"kind": kind, "resource": parsed.path.lstrip("/")})
    return endpoint, resource


def backend_identities(
    *, postgres_dsn: str, redis_url: str, s3_endpoint: str, s3_bucket: str
) -> dict[str, dict[str, str]] | None:
    """Return safe hashes for the configured external backend identities."""

    postgres = _endpoint_identity(postgres_dsn, kind="postgres")
    redis = _endpoint_identity(redis_url, kind="redis")
    s3 = _endpoint_identity(s3_endpoint, kind="s3")
    if postgres is None or redis is None or s3 is None or not s3_bucket.strip():
        return None
    return {
        "postgres": {
            "endpoint_sha256": postgres[0],
            "resource_sha256": postgres[1],
        },
        "redis": {
            "endpoint_sha256": redis[0],
            "resource_sha256": redis[1],
        },
        "s3": {
            "endpoint_sha256": s3[0],
            "resource_sha256": canonical_sha256({"kind": "s3", "resource": s3_bucket.strip()}),
        },
    }


def _junit_summary(path: Path) -> tuple[str, dict[str, int], list[dict[str, str]]]:
    """Read bounded, non-payload diagnostics from a local pytest JUnit file."""

    try:
        root = ET.parse(path).getroot()  # noqa: S314 - parses our bounded local pytest output
    except (ET.ParseError, OSError, ValueError):
        return (
            "unavailable",
            {"tests": 0, "passed": 0, "failures": 0, "errors": 0, "skipped": 0},
            [],
        )
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in totals:
            try:
                value = int(suite.attrib.get(key, "0"))
            except ValueError:
                value = 0
            totals[key] += max(value, 0)
    totals["passed"] = max(
        totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"], 0
    )
    failed_cases: list[dict[str, str]] = []
    for testcase in root.iter("testcase"):
        outcome = next(
            (name for name in ("failure", "error") if testcase.find(name) is not None),
            None,
        )
        if outcome is None:
            continue
        failed_cases.append(
            {
                "outcome": outcome,
                "classname": testcase.attrib.get("classname", "")[:512],
                "testcase": testcase.attrib.get("name", "")[:512],
            }
        )
    return "available", totals, failed_cases


def _junit_counts(path: Path) -> dict[str, int]:
    """Compatibility helper returning aggregate pytest counts."""

    return _junit_summary(path)[1]


def _backend_production_status(
    *,
    environment_ready: bool,
    passed: bool,
    counts: dict[str, int],
    runtime_evidence_ready: bool | None = None,
) -> tuple[str, list[str]]:
    if not environment_ready:
        return "not_run", [
            "live backend environment is incomplete or TRPC_TEST_IMAGE_DIGEST is invalid"
        ]
    if runtime_evidence_ready is False:
        return "not_run", ["live backend runtime evidence is unavailable"]
    if counts.get("tests", 0) <= 0 or counts.get("passed", 0) <= 0:
        return "not_run", ["live backend integration suite executed no passing test cases"]
    if counts.get("tests", 0) != sum(
        counts.get(key, 0) for key in ("passed", "failures", "errors", "skipped")
    ):
        return "not_run", ["live backend integration JUnit counts are inconsistent"]
    if counts.get("skipped", 0) != 0:
        return "not_run", ["live backend integration suite contains skipped test cases"]
    if not passed or counts.get("failures", 0) != 0 or counts.get("errors", 0) != 0:
        return "fail", ["live backend integration suite failed"]
    return "pass", []


def _runtime_attestation(
    *,
    status: str,
    run_id: str,
    selectors: tuple[str, ...],
    counts: dict[str, int],
    image_digest: str | None,
    runtime: dict[str, object],
) -> dict[str, object]:
    """Build an allowlisted backend runtime attestation."""

    result: dict[str, object] = {
        "status": status,
        "run_id": run_id,
        "selectors": list(selectors),
        "junit_counts": dict(counts),
    }
    if image_digest is not None:
        result["image_digest"] = image_digest
    runtime_value = runtime.get("value")
    if isinstance(runtime_value, str) and re.fullmatch(r"[0-9a-f]{64}", runtime_value):
        result["runtime_fingerprint_sha256"] = runtime_value
    return result


def _write_report(path: Path, value: object) -> None:
    atomic_write_json(path, value)


def _write_history_report(path: Path, value: object) -> None:
    """Write one immutable diagnostic snapshot without replacing an existing run."""

    absolute = path.absolute()
    for component in (absolute, *absolute.parents):
        if component.exists() and component.is_symlink():
            raise ValueError(f"diagnostic history path must not contain a symlink: {component}")
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode(
        "utf-8"
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=tuple(CASES))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or DEFAULT_OUTPUTS[args.kind]
    image_digest = _image_digest(os.getenv("TRPC_TEST_IMAGE_DIGEST", ""))
    backend_environment_ready = (
        args.kind == "backend"
        and all(os.getenv(name) for name in BACKEND_ENV)
        and image_digest is not None
    )
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="trpc-contract-gate-") as temporary_directory:
        junit_path = Path(temporary_directory) / "pytest-junit.xml"
        completed = subprocess.run(  # noqa: S603 - fixed interpreter and audited selectors
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                f"--junitxml={junit_path}",
                *(("--allow-real-tests",) if backend_environment_ready else ()),
                *CASES[args.kind],
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        junit_status, test_counts, failed_cases = _junit_summary(junit_path)
    duration = time.perf_counter() - started
    passed = completed.returncode == 0
    identities: dict[str, dict[str, str]] | None = None
    if backend_environment_ready:
        identities = backend_identities(
            postgres_dsn=os.environ["TRPC_TEST_POSTGRES_DSN"],
            redis_url=os.environ["TRPC_TEST_REDIS_URL"],
            s3_endpoint=os.environ["TRPC_TEST_S3_ENDPOINT"],
            s3_bucket=os.environ["TRPC_TEST_S3_BUCKET"],
        )
        backend_environment_ready = identities is not None
    runtime: dict[str, object] | None = None
    if backend_environment_ready and identities is not None and image_digest is not None:
        runtime = runtime_fingerprint(
            mode="backend_contract",
            worker_identities=["integration-test-runner"],
            stream=os.environ["TRPC_TEST_POSTGRES_DSN"],
            group=args.kind,
            parameters={
                "selectors": list(CASES[args.kind]),
                "image_digest": image_digest,
                "backend_identities": identities,
            },
        )
    runtime_value = runtime.get("value") if isinstance(runtime, dict) else None
    runtime_evidence_ready = bool(
        isinstance(runtime, dict)
        and runtime.get("status") == "available"
        and isinstance(runtime_value, str)
        and re.fullmatch(r"[0-9a-f]{64}", runtime_value) is not None
    )
    if args.kind == "backend":
        production_gate, production_reasons = _backend_production_status(
            environment_ready=backend_environment_ready,
            passed=passed,
            counts=test_counts,
            runtime_evidence_ready=runtime_evidence_ready,
        )
    elif args.kind == "fault":
        production_gate = "not_run"
        production_reasons = ["requires Toxiproxy/process-kill infrastructure"]
    else:
        production_gate = "not_run"
        production_reasons = ["requires live source and target backends"]
    evidence = build_evidence(root=ROOT, producer=PRODUCER, runtime=runtime)
    candidate: dict[str, object] = {
        "kind": args.kind,
        "selectors": list(CASES[args.kind]),
        "exit_code": completed.returncode,
        "duration_seconds": duration,
        "test_counts": test_counts,
        "junit_status": junit_status,
        "failed_cases": failed_cases,
    }
    if args.kind == "backend":
        candidate["backend_identities"] = identities or {}
        candidate["runtime_attestation"] = _runtime_attestation(
            status=production_gate,
            run_id=evidence["run_id"],
            selectors=CASES[args.kind],
            counts=test_counts,
            image_digest=image_digest if runtime_evidence_ready else None,
            runtime=evidence["runtime_fingerprint"],
        )
    candidate["lineage"] = {
        "status": "pass" if runtime_evidence_ready and image_digest is not None else "not_run",
        "image_digest": image_digest if runtime_evidence_ready else None,
        "run_id": evidence["run_id"] if runtime_evidence_ready else None,
        "runtime_fingerprint_sha256": (
            evidence["runtime_fingerprint"].get("value") if runtime_evidence_ready else None
        ),
    }
    result = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "baseline": {"selected_contracts_must_pass": True},
        "candidate": candidate,
        "case_deltas": {"failed_processes": 0 if passed else 1},
        "gate": "pass" if passed else "fail",
        "rejection_reasons": [] if passed else [f"{args.kind} contract suite failed"],
        "production_gate": production_gate,
        "production_rejection_reasons": production_reasons,
        "run_id": evidence["run_id"],
        "evidence": evidence,
    }
    history_path = output.parent / "history" / f"{output.stem}-{evidence['run_id']}.json"
    candidate["diagnostic_history"] = {
        "status": "written",
        "path": history_path.relative_to(output.parent).as_posix(),
    }
    rendered = json.dumps(result, indent=2)
    try:
        _write_history_report(history_path, result)
        _write_report(output, result)
    except (OSError, TypeError, ValueError) as error:
        print(f"contract gate could not write report: {error}", file=sys.stderr)
        return 2
    print(rendered)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
