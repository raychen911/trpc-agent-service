"""Validate release supply-chain evidence and bind it to a candidate."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Support both module and direct script execution.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.candidate_lock import verify_lock
from scripts.evidence_lineage import (
    ROOT,
    canonical_json,
    read_json,
    release_binding,
    sha256_bytes,
    write_json,
)
from scripts.release_context import verify_context

HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IMMUTABLE_IMAGE_RE = re.compile(r"@sha256:[0-9a-f]{64}$")
HIGH_LEVELS = {"error", "critical", "high"}
HIGH_SEVERITIES = {"critical", "high"}


def _not_run(reason: str) -> dict[str, Any]:
    return {"status": "not_run", "reason": reason}


def _fail(reason: str, **details: Any) -> dict[str, Any]:
    return {"status": "fail", "reason": reason, **details}


def _pass(**details: Any) -> dict[str, Any]:
    return {"status": "pass", **details}


def _load_object(path: Path) -> dict[str, Any]:
    return read_json(path)


def _load_json_value(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sbom_check(path: Path | None) -> dict[str, Any]:
    if path is None or not path:
        return _not_run("SBOM path is not configured")
    if not path.is_file():
        return _fail("SBOM file does not exist", path=str(path))
    try:
        value = _load_object(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _fail("SBOM is not a JSON object", error=str(exc))
    packages = value.get("packages")
    components = value.get("components")
    if isinstance(packages, list):
        count = len(packages)
        format_name = "spdx"
    elif isinstance(components, list):
        count = len(components)
        format_name = "cyclonedx"
    else:
        return _fail("SBOM has no SPDX packages or CycloneDX components list")
    if count < 1:
        return _fail("SBOM contains no packages")
    return _pass(path=str(path), format=format_name, package_count=count, sha256=sha256_bytes(path.read_bytes()))


def _sarif_result_severity(result: dict[str, Any], rules: dict[str, dict[str, Any]]) -> str:
    level = str(result.get("level", "")).strip().lower()
    if level in HIGH_LEVELS:
        return level
    rule_id = str(result.get("ruleId", ""))
    rule = rules.get(rule_id, {})
    properties = rule.get("properties", {}) if isinstance(rule, dict) else {}
    if isinstance(properties, dict):
        for key in ("security-severity", "severity", "security_severity"):
            value = str(properties.get(key, "")).strip().lower()
            if value in HIGH_SEVERITIES:
                return value
            try:
                if key == "security-severity" and float(value) >= 7.0:
                    return "high"
            except ValueError:
                pass
    return level or "unknown"


def _sarif_check(path: Path | None) -> dict[str, Any]:
    if path is None or not path:
        return _not_run("SARIF path is not configured")
    paths = [path] if path.is_file() else sorted(path.rglob("*.sarif*")) if path.is_dir() else []
    if not paths:
        return _fail("no SARIF files were found", path=str(path))
    total_results = 0
    high_findings: list[dict[str, str]] = []
    file_records: list[dict[str, Any]] = []
    for sarif_path in paths:
        try:
            value = _load_object(sarif_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            return _fail("SARIF file is invalid JSON", path=str(sarif_path), error=str(exc))
        runs = value.get("runs")
        if not isinstance(runs, list) or not runs:
            return _fail("SARIF file has no runs", path=str(sarif_path))
        result_count = 0
        for run in runs:
            if not isinstance(run, dict):
                return _fail("SARIF run is not an object", path=str(sarif_path))
            rules: dict[str, dict[str, Any]] = {}
            tool = run.get("tool")
            driver = tool.get("driver") if isinstance(tool, dict) else None
            rule_list = driver.get("rules") if isinstance(driver, dict) else None
            if isinstance(rule_list, list):
                rules = {
                    str(rule.get("id")): rule
                    for rule in rule_list
                    if isinstance(rule, dict) and rule.get("id") is not None
                }
            results = run.get("results", [])
            if not isinstance(results, list):
                return _fail("SARIF run results is not a list", path=str(sarif_path))
            result_count += len(results)
            for result in results:
                if not isinstance(result, dict):
                    return _fail("SARIF result is not an object", path=str(sarif_path))
                severity = _sarif_result_severity(result, rules)
                if severity in {"error", "high", "critical"}:
                    high_findings.append(
                        {
                            "path": str(sarif_path),
                            "rule_id": str(result.get("ruleId", "unknown")),
                            "severity": severity,
                        }
                    )
        total_results += result_count
        file_records.append(
            {"path": str(sarif_path), "result_count": result_count, "sha256": sha256_bytes(sarif_path.read_bytes())}
        )
    if high_findings:
        return _fail(
            "SARIF contains high or critical findings",
            files=file_records,
            result_count=total_results,
            high_findings=high_findings,
        )
    return _pass(files=file_records, result_count=total_results, high_findings=[])


def _dependency_check(path: Path | None, status: str | None) -> dict[str, Any]:
    normalized_status = (status or "").strip().lower()
    if normalized_status in {"fail", "failure", "1"}:
        return _fail("dependency audit command reported failure")
    if path is None or not path:
        return _not_run("dependency audit path is not configured")
    if not path.is_file():
        return _fail("dependency audit file does not exist", path=str(path))
    try:
        value = _load_json_value(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _fail("dependency audit is not valid JSON", error=str(exc))
    if not isinstance(value, (dict, list)):
        return _fail("dependency audit must be a JSON object or array")
    vulnerabilities: list[Any] = []
    if isinstance(value, list):
        dependencies = value
    else:
        if isinstance(value.get("vulnerabilities"), list):
            vulnerabilities.extend(value["vulnerabilities"])
        dependencies = value.get("dependencies")
    if isinstance(dependencies, list):
        for dependency in dependencies:
            if isinstance(dependency, dict) and isinstance(dependency.get("vulns"), list):
                vulnerabilities.extend(dependency["vulns"])
    if vulnerabilities:
        return _fail(
            "dependency audit reported vulnerabilities",
            vulnerability_count=len(vulnerabilities),
        )
    if normalized_status not in {"pass", "success", "0"}:
        return _fail("dependency audit status is not explicitly passing", status=normalized_status or "missing")
    return _pass(vulnerability_count=0, sha256=sha256_bytes(path.read_bytes()))


def _docker_image_check(
    image: str | None,
    expected_digest: str | None,
    expected_fingerprint: str,
    require_image: bool,
) -> dict[str, Any]:
    if not image:
        return (
            _not_run("candidate image is not configured")
            if not require_image
            else _fail("candidate image is required")
        )
    if not IMMUTABLE_IMAGE_RE.search(image):
        return _fail("candidate image must use an immutable @sha256 reference", image=image)
    if expected_digest and not image.endswith("@" + expected_digest):
        return _fail("candidate image does not match the locked digest", image=image, expected_digest=expected_digest)
    docker = shutil.which("docker")
    if docker is None:
        return (
            _not_run("docker is not installed; image metadata could not be inspected")
            if not require_image
            else _fail("docker is required to verify candidate image metadata")
        )
    try:
        result = subprocess.run(
            [docker, "image", "inspect", image],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return _fail("docker image inspection failed", error=str(exc))
    if result.returncode != 0:
        return _fail("candidate image is not available locally", output=(result.stderr or result.stdout)[-1000:])
    try:
        records = json.loads(result.stdout)
        record = records[0]
        labels = record.get("Config", {}).get("Labels", {}) or {}
        repo_digests = [str(value) for value in record.get("RepoDigests", [])]
    except (ValueError, IndexError, TypeError, AttributeError) as exc:
        return _fail("docker image inspect returned an invalid response", error=str(exc))
    fingerprint = str(labels.get("io.trpc.agent-service.source-fingerprint", ""))
    if fingerprint != expected_fingerprint:
        return _fail(
            "candidate image source fingerprint does not match release context",
            image_fingerprint=fingerprint,
            expected_fingerprint=expected_fingerprint,
        )
    expected_suffix = "@" + image.rsplit("@", 1)[1]
    if repo_digests and not any(value.endswith(expected_suffix) for value in repo_digests):
        return _fail(
            "candidate image repository digest does not match its immutable reference",
            repo_digests=repo_digests,
        )
    return _pass(
        image=image,
        image_id=str(record.get("Id", "")),
        repo_digests=repo_digests,
        source_fingerprint=fingerprint,
    )


def _provenance_check(
    path: Path | None,
    binding: dict[str, Any],
    expected_digest: str,
    require_provenance: bool,
) -> dict[str, Any]:
    if path is None or not path:
        return (
            _not_run("provenance path is not configured")
            if not require_provenance
            else _fail("provenance is required")
        )
    if not path.is_file():
        return _fail("provenance file does not exist", path=str(path))
    try:
        value = _load_object(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _fail("provenance file is not valid JSON", error=str(exc))
    for key in ("source_fingerprint", "release_id"):
        if value.get(key) != binding.get("source_fingerprint" if key == "source_fingerprint" else "release_id"):
            return _fail(f"provenance {key} does not match release binding")
    digest = value.get("image_digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:") or not HEX64_RE.fullmatch(digest[7:]):
        return _fail("provenance image_digest is invalid")
    if digest != expected_digest:
        return _fail("provenance image_digest does not match the locked candidate image")
    return _pass(path=str(path), sha256=sha256_bytes(path.read_bytes()))


def evaluate(
    *,
    context_path: Path,
    lock_path: Path,
    sbom_path: Path | None = None,
    sarif_path: Path | None = None,
    dependency_audit_path: Path | None = None,
    dependency_audit_status: str | None = None,
    image: str | None = None,
    image_role: str = "initial",
    provenance_path: Path | None = None,
    require_production: bool = False,
    require_provenance: bool = False,
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = verify_context(
        context_path,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    lock = verify_lock(
        context_path,
        lock_path,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    images = lock["images"]
    if image_role not in images:
        raise ValueError(f"unknown candidate image role: {image_role}")
    binding = release_binding(context, lock)
    checks = {
        "sbom": _sbom_check(sbom_path),
        "sarif": _sarif_check(sarif_path),
        "dependency_audit": _dependency_check(dependency_audit_path, dependency_audit_status),
        "image": _docker_image_check(
            image,
            str(images[image_role]["digest"]),
            str(context["source_fingerprint"]["value"]),
            require_production,
        ),
        "provenance": _provenance_check(
            provenance_path,
            binding,
            str(images[image_role]["digest"]),
            require_provenance,
        ),
    }
    statuses = {item["status"] for item in checks.values()}
    if "fail" in statuses or require_production and "not_run" in statuses:
        status = "fail"
    elif "not_run" in statuses:
        status = "not_run"
    else:
        status = "pass"
    report: dict[str, Any] = {
        "schema_version": 1,
        "gate": "supply-chain",
        "status": status,
        "generated_at": datetime.now(UTC).isoformat(),
        "release_binding": binding,
        "checks": checks,
        "require_production": require_production,
    }
    report["report_sha256"] = sha256_bytes(canonical_json(report))
    return report


def verify_report(report: dict[str, Any]) -> tuple[bool, str]:
    if report.get("schema_version") != 1 or report.get("gate") != "supply-chain":
        return False, "invalid supply-chain report schema"
    binding = report.get("release_binding")
    if not isinstance(binding, dict):
        return False, "supply-chain report release binding is missing"
    report_hash = report.get("report_sha256")
    unsigned = {key: value for key, value in report.items() if key != "report_sha256"}
    if report_hash != sha256_bytes(canonical_json(unsigned)):
        return False, "supply-chain report hash does not match"
    if report.get("status") not in {"pass", "fail", "not_run"}:
        return False, "supply-chain report status is invalid"
    checks = report.get("checks")
    if not isinstance(checks, dict) or not checks:
        return False, "supply-chain report checks are missing"
    statuses: set[str] = set()
    for name, check in checks.items():
        if not isinstance(name, str) or not isinstance(check, dict):
            return False, "supply-chain report check is invalid"
        status = check.get("status")
        if status not in {"pass", "fail", "not_run"}:
            return False, f"supply-chain report check status is invalid: {name}"
        statuses.add(str(status))
    if "fail" in statuses or report.get("require_production") and "not_run" in statuses:
        expected_status = "fail"
    elif "not_run" in statuses:
        expected_status = "not_run"
    else:
        expected_status = "pass"
    if report.get("status") != expected_status:
        return False, "supply-chain report status does not match its checks"
    return True, "ok"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--sbom", type=Path)
    parser.add_argument("--sarif", type=Path)
    parser.add_argument("--dependency-audit", type=Path)
    parser.add_argument("--dependency-audit-status", default="")
    parser.add_argument("--image", default="")
    parser.add_argument("--image-role", choices=("initial", "upgrade"), default="initial")
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--require-production", action="store_true")
    parser.add_argument("--require-provenance", action="store_true")
    parser.add_argument("--trust-key", type=Path)
    parser.add_argument("--require-signature", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = evaluate(
            context_path=args.context,
            lock_path=args.lock,
            sbom_path=args.sbom,
            sarif_path=args.sarif,
            dependency_audit_path=args.dependency_audit,
            dependency_audit_status=args.dependency_audit_status,
            image=args.image or None,
            image_role=args.image_role,
            provenance_path=args.provenance,
            require_production=args.require_production,
            require_provenance=args.require_provenance,
            trust_key_file=args.trust_key,
            require_signature=args.require_signature or args.require_production,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    if args.output:
        write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] == "pass":
        return 0
    if report["status"] == "not_run" and not args.require_production:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
