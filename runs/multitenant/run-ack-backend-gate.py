from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import yaml

ROOT = Path(__file__).resolve().parents[2]
SUBPROCESS_TIMEOUT_SECONDS = 900
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backend_isolation import (  # noqa: E402
    BackendIsolation,
    BackendIsolationError,
    provision_backend_isolation,
)
from scripts.report_io import atomic_write_json  # noqa: E402


def _secret_data(path: Path, name: str) -> dict[str, str]:
    for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if isinstance(document, dict) and document.get("metadata", {}).get("name") == name:
            data = document.get("data")
            if isinstance(data, dict):
                return {str(key): str(value) for key, value in data.items()}
    raise RuntimeError(f"Secret {name} is unavailable")


def _decode(data: dict[str, str], key: str) -> str:
    value = base64.b64decode(data[key], validate=True).decode("utf-8")
    if not value:
        raise RuntimeError(f"Secret key {key} is empty")
    return value


def _loopback_url(value: str, port: int) -> str:
    parsed = urlsplit(value)
    userinfo = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
    return urlunsplit(
        (parsed.scheme, f"{userinfo}127.0.0.1:{port}", parsed.path, parsed.query, parsed.fragment)
    )


def _failure_report(
    output: Path,
    *,
    stage: str,
    run_id: str,
    isolation: BackendIsolation | None = None,
    cleanup_errors: tuple[str, ...] = (),
    exit_code: int = 1,
) -> None:
    """Write a safe report even when provisioning fails before contract_gate."""

    candidate: dict[str, object] = {
        "status": "fail",
        "exit_code": exit_code,
        "isolation_stage": stage,
    }
    if isolation is not None:
        candidate["backend_isolation"] = isolation.summary
    if cleanup_errors:
        candidate["cleanup_errors"] = list(cleanup_errors)
    report = {
        "schema_version": 1,
        "baseline": {"selected_contracts_must_pass": True},
        "candidate": candidate,
        "case_deltas": {"failed_processes": 1},
        "gate": "fail",
        "rejection_reasons": [f"backend isolation {stage} failed"],
        "production_gate": "not_run",
        "production_rejection_reasons": ["backend contract did not execute"],
        "run_id": run_id,
    }
    atomic_write_json(output, report)


def _run_subprocess(
    command: list[str], environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - interpreter and repository entrypoints are fixed
        command,
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=environment,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )


async def _run_acceptance_subprocess(
    command: list[str], environment: dict[str, str], *, stage: str
) -> subprocess.CompletedProcess[str]:
    try:
        return await asyncio.to_thread(_run_subprocess, command, environment)
    except subprocess.TimeoutExpired as error:
        raise BackendIsolationError(
            f"{stage} timeout after {SUBPROCESS_TIMEOUT_SECONDS}s"
        ) from error


def _migration_scope(run_id: str) -> str:
    """Return a unique scope accepted by migration acceptance safeguards."""

    suffix = run_id.removeprefix("backend-ack-")
    return f"migration-acceptance-{suffix}"


async def _run(args: argparse.Namespace) -> int:
    service = _secret_data(args.secret_manifest, "trpc-service-secrets")
    worker = _secret_data(args.secret_manifest, "trpc-worker-secrets")
    migration = _secret_data(args.secret_manifest, "trpc-migration-secrets")
    support = _secret_data(args.secret_manifest, "runtime-support-secrets")
    runtime_dsn = _loopback_url(_decode(service, "TRPC_SERVICE_DATABASE_DSN"), 35432)
    worker_dsn = _loopback_url(_decode(worker, "TRPC_SERVICE_WORKER_DATABASE_DSN"), 35432)
    migration_dsn = _loopback_url(_decode(migration, "TRPC_SERVICE_DATABASE_DSN"), 35432)
    redis_url = _loopback_url(_decode(service, "TRPC_SERVICE_REDIS_URL"), 36379)
    s3_endpoint = "http://127.0.0.1:39000"
    s3_access_key = _decode(service, "TRPC_SERVICE_S3_ACCESS_KEY")
    s3_secret_key = _decode(service, "TRPC_SERVICE_S3_SECRET_KEY")
    run_id = f"backend-ack-{uuid4().hex[:12]}"
    migration_scope = _migration_scope(run_id)
    isolation: BackendIsolation | None = None
    result_code = 1
    cleanup_errors: list[str] = []
    try:
        isolation = await provision_backend_isolation(
            run_id=run_id,
            runtime_dsn=runtime_dsn,
            worker_dsn=worker_dsn,
            migration_dsn=migration_dsn,
            admin_password=_decode(support, "postgres-admin-password"),
            redis_url=redis_url,
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            root=ROOT,
        )
        environment = os.environ.copy()
        environment.update(isolation.environment)
        environment.update(
            {
                "TRPC_TEST_S3_ACCESS_KEY": s3_access_key,
                "TRPC_TEST_S3_SECRET_KEY": s3_secret_key,
                "TRPC_TEST_IMAGE_DIGEST": args.image_digest,
                "TRPC_MIGRATION_BACKEND_CONTRACT": "1",
                "TRPC_RUN_REAL_MIGRATION": "1",
                "TRPC_MIGRATION_FULL_ACCEPTANCE": "1",
                "TRPC_MIGRATION_BOOTSTRAP": "1",
                "TRPC_MIGRATION_TENANT_ID": f"{migration_scope}-tenant",
                "TRPC_MIGRATION_ID": f"{migration_scope}-migration",
                "TRPC_MIGRATION_APP_ID": f"{migration_scope}-app",
                "TRPC_MIGRATION_APP_REVISION": "1",
                "TRPC_MIGRATION_CONFIG_VERSION": "1",
                "TRPC_MIGRATION_BINDING_ID": f"{migration_scope}-binding",
                "TRPC_MIGRATION_BINDING_REVISION": "1",
                "TRPC_MIGRATION_PHASE_TENANT_ID": f"{migration_scope}-phase-tenant",
                "TRPC_MIGRATION_PHASE_ID": f"{migration_scope}-phase-migration",
                "TRPC_MIGRATION_PHASE_APP_ID": f"{migration_scope}-phase-app",
                "TRPC_MIGRATION_EXPECTED_RECORDS": "200",
                "TRPC_MIGRATION_CONTROL_FACTORY": (
                    "trpc_service.storage.production_migration_control:create"
                ),
                "TRPC_MIGRATION_IMAGE_DIGEST": args.image_digest,
            }
        )
        bootstrap = await _run_acceptance_subprocess(
            [sys.executable, "-m", "scripts.migration_acceptance_bootstrap"],
            environment,
            stage="migration bootstrap",
        )
        try:
            bootstrap_report = json.loads(bootstrap.stdout)
        except json.JSONDecodeError as error:
            raise BackendIsolationError("migration bootstrap output") from error
        if bootstrap.returncode != 0 or bootstrap_report.get("status") != "pass":
            raise BackendIsolationError("migration bootstrap")
        if args.diagnostic_target:
            command = [
                sys.executable,
                "-m",
                "pytest",
                "-vv",
                "-x",
                "--allow-real-tests",
                f"--basetemp=runs/multitenant/pytest-diagnostic-{run_id}",
                args.diagnostic_target,
            ]
        else:
            command = [
                sys.executable,
                "-m",
                "scripts.contract_gate",
                "backend",
                "--output",
                str(args.output),
            ]
        completed = await _run_acceptance_subprocess(
            command,
            environment,
            stage="diagnostic contract" if args.diagnostic_target else "backend contract",
        )
        if args.diagnostic_target and not args.output.exists():
            _failure_report(
                args.output,
                stage="diagnostic contract",
                run_id=run_id,
                isolation=isolation,
                exit_code=completed.returncode,
            )
        result_code = completed.returncode
    except BackendIsolationError as error:
        _failure_report(
            args.output,
            stage=error.stage,
            run_id=run_id,
            isolation=isolation,
            cleanup_errors=error.cleanup_errors,
        )
        print(str(error), file=sys.stderr)
    finally:
        if isolation is not None:
            cleanup_errors = await isolation.cleanup()
            if cleanup_errors:
                print(
                    "backend isolation cleanup failed: " + ", ".join(cleanup_errors),
                    file=sys.stderr,
                )
    if isolation is not None:
        if not args.output.is_file():
            _failure_report(
                args.output,
                stage="contract output",
                run_id=run_id,
                isolation=isolation,
                cleanup_errors=tuple(cleanup_errors),
            )
            return 1
        try:
            report = json.loads(args.output.read_text(encoding="utf-8"))
            candidate = report.setdefault("candidate", {})
            if not isinstance(candidate, dict):
                raise ValueError("backend report candidate is invalid")
            isolation_evidence = dict(isolation.summary)
            isolation_evidence["cleanup_status"] = "fail" if cleanup_errors else "pass"
            candidate["backend_isolation"] = isolation_evidence
            if cleanup_errors:
                candidate["cleanup_errors"] = cleanup_errors
                report["gate"] = "fail"
                report["production_gate"] = "not_run"
                reasons = report.setdefault("rejection_reasons", [])
                if isinstance(reasons, list):
                    reasons.append("backend isolation cleanup failed")
                result_code = 1
            atomic_write_json(args.output, report)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            _failure_report(
                args.output,
                stage="contract report finalization",
                run_id=run_id,
                isolation=isolation,
                cleanup_errors=tuple(cleanup_errors),
            )
            result_code = 1
    return result_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-manifest", type=Path, required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--diagnostic-target")
    args = parser.parse_args()
    try:
        return asyncio.run(_run(args))
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        run_id = "backend-ack-unstarted"
        try:
            _failure_report(args.output, stage="runner setup", run_id=run_id)
        except OSError:
            pass
        print(f"backend ACK runner failed: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
