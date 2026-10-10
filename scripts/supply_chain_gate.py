#!/usr/bin/env python3
"""Aggregate complete SBOM, SARIF, dependency, and image lineage evidence."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

from scripts.evidence_lineage import source_fingerprint
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _sarif_paths(value: Path) -> tuple[Path, ...]:
    if value.is_dir():
        return tuple(sorted(value.rglob("*.sarif.json")))
    return (value,)


def _sarif_findings(path: Path) -> tuple[int, int]:
    """Return (finding_count, run_count), rejecting incomplete SARIF."""

    document = _load(path)
    runs = document.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError(f"{path} has no SARIF runs")
    findings = 0
    for run in runs:
        if not isinstance(run, dict) or not isinstance(run.get("results"), list):
            raise ValueError(f"{path} contains a malformed SARIF run")
        if any(not isinstance(result, dict) for result in run["results"]):
            raise ValueError(f"{path} contains a malformed SARIF result")
        findings += len(run["results"])
    return findings, len(runs)


def _image_digest(image: str | None) -> tuple[str | None, str | None]:
    if not image:
        return None, "image reference was not supplied"
    executable = shutil.which("docker")
    if executable is None:
        return None, "docker is not installed; image lineage is unavailable"
    try:
        result = subprocess.run(  # noqa: S603 - executable and fixed arguments
            [executable, "image", "inspect", image, "--format", "{{.Id}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, "docker image inspection was unavailable"
    digest = result.stdout.strip()
    if result.returncode != 0 or not digest.startswith("sha256:"):
        return None, "image digest was not available"
    return digest, None


def _dependency_report(path: Path) -> str | None:
    """Validate the dependency-audit envelope without echoing package data."""

    try:
        value = _load(path)
    except (OSError, ValueError, json.JSONDecodeError, TypeError) as error:
        return f"dependency audit evidence is invalid: {type(error).__name__}"
    # pip-audit JSON currently uses ``dependencies``; accepting a top-level
    # list keeps this gate compatible with older pip-audit releases while still
    # rejecting an empty/malformed success artifact.
    dependencies = value.get("dependencies")
    if dependencies is None:
        dependencies = value.get("results")
    if not isinstance(dependencies, list):
        return "dependency audit evidence has no dependency list"
    if any(not isinstance(item, dict) for item in dependencies):
        return "dependency audit evidence contains a malformed dependency"
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sbom", type=Path, default=Path("runs/multitenant/sbom.spdx.json"))
    parser.add_argument(
        "--vulnerabilities",
        type=Path,
        default=Path("runs/multitenant/image-vulnerabilities.sarif.json"),
        help="SARIF file or directory; every *.sarif.json file/run is scanned",
    )
    parser.add_argument("--image", default=None, help="candidate image reference to inspect")
    parser.add_argument(
        "--dependency-audit-status",
        choices=("pass", "fail", "not_run"),
        default="not_run",
    )
    parser.add_argument(
        "--dependency-audit",
        type=Path,
        default=Path("runs/multitenant/dependency-audit.json"),
    )
    parser.add_argument(
        "--dependency-audit-reason",
        default="standalone dependency audit has no passing report",
    )
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/supply-chain.json"))
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args()

    evidence_error: str | None
    package_count = 0
    finding_count = -1
    sarif_run_count = 0
    report_paths = _sarif_paths(args.vulnerabilities)
    try:
        sbom = _load(args.sbom)
        packages = sbom.get("packages")
        if not isinstance(packages, list):
            raise ValueError("SBOM packages is not a list")
        package_count = len(packages)
        if not report_paths:
            raise ValueError("no SARIF reports were found")
        finding_count = 0
        for report_path in report_paths:
            findings, runs = _sarif_findings(report_path)
            finding_count += findings
            sarif_run_count += runs
    except (OSError, ValueError, json.JSONDecodeError, AttributeError, TypeError) as error:
        package_count = 0
        finding_count = -1
        evidence_error = type(error).__name__
    else:
        evidence_error = None

    image_digest, image_reason = _image_digest(args.image)
    source = source_fingerprint(ROOT)
    lock = source_fingerprint(ROOT, ("pyproject.toml", "uv.lock"))
    dependency_reason = (
        _dependency_report(args.dependency_audit)
        if args.dependency_audit_status == "pass"
        else None
    )
    image_lineage_pass = image_digest is not None
    source_lineage_pass = source.get("status") == "available"
    lock_lineage_pass = lock.get("status") == "available"
    image_passed = (
        evidence_error is None
        and package_count > 0
        and finding_count == 0
        and image_lineage_pass
        and source_lineage_pass
        and lock_lineage_pass
    )
    dependency_status = args.dependency_audit_status
    if dependency_reason:
        gate = "fail"
    elif not image_passed or dependency_status == "fail":
        gate = "fail"
    elif dependency_status == "pass":
        gate = "pass"
    else:
        gate = "not_run"

    reasons: list[str] = []
    if evidence_error:
        reasons.append(f"SBOM or SARIF evidence is invalid: {evidence_error}")
    elif package_count == 0:
        reasons.append("SBOM contains no packages")
    elif finding_count:
        reasons.append(f"image contains {finding_count} critical/high vulnerability findings")
    if image_reason:
        reasons.append(image_reason)
    if not lock_lineage_pass:
        reasons.append("pyproject.toml/uv.lock lineage is unavailable")
    if not source_lineage_pass:
        reasons.append("source checkout lineage is unavailable")
    if dependency_status != "pass":
        reasons.append(args.dependency_audit_reason)
    if dependency_reason:
        reasons.append(dependency_reason)

    result = {
        "baseline": {
            "sbom_required": True,
            "critical_high_vulnerabilities_max": 0,
            "dependency_audit_required": True,
            "image_digest_required": True,
            "all_sarif_runs_scanned": True,
        },
        "candidate": {
            "sbom": str(args.sbom),
            # Preserve the old singular consumer field while the plural field
            # records every SARIF input scanned by this gate.
            "vulnerability_report": str(report_paths[0]) if report_paths else None,
            "sbom_packages": package_count,
            "vulnerability_reports": [str(path) for path in report_paths],
            "sarif_runs": sarif_run_count,
            "critical_high_vulnerabilities": finding_count,
            "dependency_audit": dependency_status,
            "dependency_audit_report": str(args.dependency_audit),
            "image": {"configured": bool(args.image), "digest": image_digest},
            "source_fingerprint": source,
            "lock_source_fingerprint": lock,
        },
        "case_deltas": {
            "critical_high_vulnerabilities": max(0, finding_count),
            "missing_package_inventory": int(package_count == 0),
            "missing_image_lineage": int(not image_lineage_pass),
        },
        "gate": gate,
        "production_gate": gate,
        "image_gate": "pass" if image_passed else "fail",
        "rejection_reasons": reasons,
        "production_rejection_reasons": reasons,
    }
    rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    if gate == "fail" or (args.require_production and gate != "pass"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
