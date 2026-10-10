#!/usr/bin/env python3
"""Validate deployment manifests and record unavailable runtime gates explicitly."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

from scripts.report_io import atomic_write_json


def _run(command: list[str], *, environment: dict[str, str] | None = None) -> dict[str, object]:
    executable = shutil.which(command[0])
    if executable is None:
        return {"status": "not_run", "reason": f"{command[0]} is not installed"}
    completed = subprocess.run(  # noqa: S603 - command has no shell and is locally selected
        [executable, *command[1:]],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "exit_code": completed.returncode,
        "error": completed.stderr.strip()[-1000:] if completed.returncode else "",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/deployment.json"))
    parser.add_argument(
        "--compose-e2e-report",
        type=Path,
        default=Path("runs/multitenant/compose-e2e.json"),
    )
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args()
    environment = {
        **os.environ,
        "POSTGRES_PASSWORD": "deployment-gate-migration-password",
        "MIGRATION_DATABASE_PASSWORD": "deployment-gate-migration-role-password",
        "RUNTIME_DATABASE_PASSWORD": "deployment-gate-runtime-password",
        "REDIS_PASSWORD": "deployment-gate-redis-password",
        "MINIO_ROOT_PASSWORD": "deployment-gate-minio-password",
        "SESSION_HMAC_KEY": "deployment-gate-session-hmac-key-32",
        "EMERGENCY_QUEUE_KEY": "deployment-gate-emergency-key-32",
        "DEVELOPMENT_TOKEN": "deployment-gate-development-token",
    }
    checks = {
        "compose_config": _run(["docker", "compose", "config", "--quiet"], environment=environment),
        "kustomize_base": _run(["kubectl", "kustomize", "deploy/kustomize/base"]),
        "kustomize_production": _run(
            ["kubectl", "kustomize", "deploy/kustomize/overlays/production"]
        ),
        "docker_daemon": _run(["docker", "info", "--format", "{{.ServerVersion}}"]),
    }
    static_names = ("compose_config", "kustomize_base", "kustomize_production")
    static_passed = all(checks[name]["status"] == "pass" for name in static_names)
    runtime_available = checks["docker_daemon"]["status"] == "pass"
    compose_e2e: dict[str, object] = {
        "status": "not_run",
        "reason": f"report not found: {args.compose_e2e_report}",
    }
    if args.compose_e2e_report.is_file():
        try:
            report = json.loads(args.compose_e2e_report.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            compose_e2e = {"status": "fail", "reason": f"invalid report: {error}"}
        else:
            report_gate = report.get("gate")
            candidate = report.get("candidate")
            scope = candidate.get("scope") if isinstance(candidate, dict) else report.get("scope")
            compose_e2e = {
                # This script is control-plane evidence only.  Do not let it
                # satisfy the production message E2E requirement.
                "status": "pass" if report_gate == "pass" and scope == "control_plane" else "fail",
                "report": str(args.compose_e2e_report),
                "reported_gate": report_gate,
                "scope": scope,
            }
    checks["compose_e2e"] = compose_e2e
    checks["compose_control_plane"] = compose_e2e
    compose_passed = compose_e2e["status"] == "pass"
    production_reasons = ["live Kubernetes rollout/HPA/eviction tests were not executed"]
    if not compose_passed:
        production_reasons.insert(0, "Compose zero-state E2E has no passing report")
    if not runtime_available:
        production_reasons.append("Docker daemon is unavailable")
    # This command validates manifests and (when a report is supplied) the
    # control-plane Compose smoke test.  It never executes the live
    # Kubernetes rollout/HPA/eviction acceptance.  Keep the static result
    # separate from the production result so a static pass cannot be read as
    # a runtime deployment pass by an operator or a report consumer.
    static_gate = "pass" if static_passed else "fail"
    result = {
        "baseline": {
            "compose_static": "static_pass",
            "kustomize_static": "static_pass",
            "compose_control_plane": "static_pass",
            "message_e2e": "not_run",
            "kubernetes_runtime": "not_run",
        },
        "candidate": checks,
        "case_deltas": {
            "static_failures": sum(checks[name]["status"] != "pass" for name in static_names),
            "runtime_checks_not_executed": 1 + int(not compose_passed),
        },
        "gate": "not_run" if static_passed else "fail",
        "static_gate": static_gate,
        "production_gate": "not_run",
        "rejection_reasons": production_reasons
        if static_passed
        else ["static deployment check failed"],
        "production_rejection_reasons": production_reasons,
    }
    rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    if not static_passed or (args.require_production and result["gate"] != "pass"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
