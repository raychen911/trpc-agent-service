#!/usr/bin/env python3
"""Aggregate secret-safe deployment prerequisites before a live gate runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.deployment_config import (
    HPA_DRIVER_MAX_BYTES,
    DeploymentConfigError,
    RuntimeGateConfig,
    load_runtime_gate_config,
    secret_manifest_contract,
)
from scripts.report_io import atomic_write_json

PRODUCER = "scripts.deployment_preflight"
EXCLUDED_RANGE_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s*$")


def _check_file(path: Path, name: str) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if path.is_file() else "fail",
        "path_sha256": hashlib.sha256(str(path).encode()).hexdigest(),
        **({} if path.is_file() else {"reason": "required file is missing"}),
    }


def _excluded_windows_ports() -> list[tuple[int, int]]:
    if platform.system() != "Windows":
        return []
    executable = shutil.which("netsh")
    if executable is None:
        return []
    try:
        result = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
            [executable, "interface", "ipv4", "show", "excludedportrange", "protocol=tcp"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    ranges: list[tuple[int, int]] = []
    for line in result.stdout.splitlines():
        matched = EXCLUDED_RANGE_RE.match(line)
        if matched:
            ranges.append((int(matched.group(1)), int(matched.group(2))))
    return ranges


def _port_checks(ports: Mapping[str, int]) -> list[dict[str, Any]]:
    excluded = _excluded_windows_ports()
    checks: list[dict[str, Any]] = []
    for name, port in ports.items():
        in_excluded_range = any(start <= port <= end for start, end in excluded)
        available = False
        if not in_excluded_range:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                    probe.bind(("127.0.0.1", port))
                available = True
            except OSError:
                available = False
        checks.append(
            {
                "name": f"compose_port_{name}",
                "status": "pass" if available else "fail",
                "port": port,
                "excluded_by_host": in_excluded_range,
            }
        )
    return checks


def _secret_check(config: RuntimeGateConfig) -> dict[str, Any]:
    metadata = secret_manifest_contract(config.secret_manifest)
    missing: dict[str, list[str]] = {}
    for name, required_keys in config.required_secret_keys().items():
        entry = metadata.get(name, {})
        absent = sorted(required_keys - entry.get("keys", set()))
        if absent:
            missing[name] = absent
    pull_entry = metadata.get(config.image_pull_secret, {})
    pull_keys = pull_entry.get("keys", set())
    if ".dockerconfigjson" not in pull_keys:
        missing[config.image_pull_secret] = [".dockerconfigjson"]
    namespaced = sorted(name for name, entry in metadata.items() if entry.get("namespace"))
    pull_type_ok = pull_entry.get("type") == "kubernetes.io/dockerconfigjson"
    return {
        "name": "secret_manifest_contract",
        "status": "pass" if not missing and not namespaced and pull_type_ok else "fail",
        "secret_names": sorted(metadata),
        "missing_keys": missing,
        "hardcoded_namespace_names": namespaced,
        "image_pull_secret_type_valid": pull_type_ok,
        "values_recorded": False,
    }


def _hpa_driver_identity_check(config: RuntimeGateConfig) -> dict[str, Any]:
    """Validate the same repository/file boundary used by the live gate."""

    path = config.hpa_driver
    scripts_root = (Path(__file__).resolve().parents[1] / "scripts").resolve()
    reason: str | None = None
    resolved: Path | None = None
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(scripts_root)
    except (OSError, ValueError):
        reason = "HPA driver must resolve beneath the repository scripts directory"
    if reason is None and (
        not resolved or not resolved.is_file() or resolved.suffix.lower() != ".py"
    ):
        reason = "HPA driver must be a regular repository Python script"
    if reason is None and resolved is not None:
        try:
            size = resolved.stat().st_size
        except OSError:
            size = -1
        if not 0 < size <= HPA_DRIVER_MAX_BYTES:
            reason = "HPA driver size is invalid"
    result: dict[str, Any] = {
        "name": "hpa_driver_identity",
        "status": "pass" if reason is None else "fail",
        "repository_bound": reason is None,
    }
    if reason:
        result["reason"] = reason
    return result


def _kubeconfig_separation(config: RuntimeGateConfig) -> dict[str, Any]:
    distinct_paths = config.kubeconfig != config.hpa_kubeconfig
    distinct_bytes = False
    distinct_inodes = False
    if config.kubeconfig.is_file() and config.hpa_kubeconfig.is_file():
        try:
            distinct_bytes = config.kubeconfig.read_bytes() != config.hpa_kubeconfig.read_bytes()
            distinct_inodes = not os.path.samefile(config.kubeconfig, config.hpa_kubeconfig)
        except OSError:
            distinct_bytes = False
            distinct_inodes = False
    return {
        "name": "hpa_kubeconfig_separation",
        "status": "pass" if distinct_paths and distinct_bytes and distinct_inodes else "fail",
        "distinct_paths": distinct_paths,
        "distinct_bytes": distinct_bytes,
        "distinct_inodes": distinct_inodes,
        **(
            {}
            if distinct_paths and distinct_bytes and distinct_inodes
            else {"reason": "HPA driver kubeconfig must differ by path, bytes, and inode"}
        ),
    }


def _image_parts(reference: str) -> tuple[str, str, str]:
    repository, digest = reference.rsplit("@", 1)
    registry, image_path = repository.split("/", 1)
    return registry, image_path, digest


def _image_reference_check(config: RuntimeGateConfig) -> dict[str, Any]:
    """Verify the pull-host rewrite preserves repository paths and digests."""

    try:
        canonical = config.canonical_image_references()
        resolved = config.resolved_image_references()
        details: dict[str, Any] = {}
        for name in ("initial", "upgrade"):
            canonical_registry, canonical_path, canonical_digest = _image_parts(canonical[name])
            resolved_registry, resolved_path, resolved_digest = _image_parts(resolved[name])
            if resolved_path != canonical_path or resolved_digest != canonical_digest:
                return {
                    "name": "image_reference_contract",
                    "status": "fail",
                    "reason": f"{name} pull reference changed repository path or digest",
                    "pull_registry": config.pull_registry,
                }
            if config.pull_registry is not None and resolved_registry != config.pull_registry:
                return {
                    "name": "image_reference_contract",
                    "status": "fail",
                    "reason": f"{name} pull reference does not use configured pull registry",
                    "pull_registry": config.pull_registry,
                }
            if config.pull_registry is None and resolved_registry != canonical_registry:
                return {
                    "name": "image_reference_contract",
                    "status": "fail",
                    "reason": f"{name} pull reference changed registry without configuration",
                }
            details[name] = {
                "canonical_registry": canonical_registry,
                "resolved_registry": resolved_registry,
                "repository_path_sha256": hashlib.sha256(canonical_path.encode()).hexdigest(),
                "digest": canonical_digest,
            }
        if resolved["initial"] == resolved["upgrade"]:
            return {
                "name": "image_reference_contract",
                "status": "fail",
                "reason": "initial and upgrade pull references must differ",
                "pull_registry": config.pull_registry,
            }
        return {
            "name": "image_reference_contract",
            "status": "pass",
            "pull_registry": config.pull_registry,
            "images": details,
            "hpa_job_image_explicit": bool(config.hpa_job_image),
        }
    except (DeploymentConfigError, OSError, ValueError) as error:
        return {
            "name": "image_reference_contract",
            "status": "fail",
            "reason": str(error),
            "pull_registry": config.pull_registry,
        }


def _manifest_digest_present(value: object, expected: str) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in {"digest", "manifestdigest"} and item == expected:
                return True
            if _manifest_digest_present(item, expected):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_manifest_digest_present(item, expected) for item in value)
    return False


def _manifest_reachability_check(config: RuntimeGateConfig) -> dict[str, Any]:
    """Optionally verify pull-host manifests with the local Docker CLI.

    A pull registry is an explicit operator choice, so its two rewritten
    references are checked before the live gate can mutate a namespace.  When
    no alternate pull host is configured, the release binding remains the
    source of truth and this network probe is deliberately not requested.
    """

    if config.pull_registry is None:
        return {
            "name": "image_manifest_reachability",
            "status": "pass",
            "verification": "not_requested_without_pull_registry",
        }
    executable = shutil.which("docker")
    if executable is None:
        return {
            "name": "image_manifest_reachability",
            "status": "fail",
            "reason": "docker CLI is required to verify the configured pull registry",
        }
    verified: list[str] = []
    for name, reference in config.resolved_image_references().items():
        try:
            result = subprocess.run(  # noqa: S603 - fixed Docker subcommand and image reference
                [executable, "manifest", "inspect", "--verbose", reference],
                check=False,
                capture_output=True,
                text=True,
                timeout=45,
            )
        except (OSError, subprocess.TimeoutExpired):
            return {
                "name": "image_manifest_reachability",
                "status": "fail",
                "reason": f"{name} manifest inspection was unavailable",
            }
        if result.returncode != 0:
            return {
                "name": "image_manifest_reachability",
                "status": "fail",
                "reason": f"{name} manifest inspection failed with exit code {result.returncode}",
            }
        try:
            document = json.loads(result.stdout)
            expected_digest = reference.rsplit("@", 1)[1]
        except (json.JSONDecodeError, ValueError):
            return {
                "name": "image_manifest_reachability",
                "status": "fail",
                "reason": f"{name} manifest inspection returned invalid metadata",
            }
        if not _manifest_digest_present(document, expected_digest):
            return {
                "name": "image_manifest_reachability",
                "status": "fail",
                "reason": f"{name} manifest digest did not match the release binding",
            }
        verified.append(name)
    return {
        "name": "image_manifest_reachability",
        "status": "pass",
        "verified_images": verified,
        "credentials_recorded": False,
    }


def build_preflight(
    config_path: Path, *, environment: Mapping[str, str] | None = None
) -> tuple[dict[str, Any], dict[str, str] | None]:
    checks: list[dict[str, Any]] = []
    projected: dict[str, str] | None = None
    try:
        config = load_runtime_gate_config(config_path)
    except (DeploymentConfigError, OSError) as error:
        # A malformed top-level document is the only case where the remaining
        # checks cannot be evaluated.  Keep the failure in the same report
        # shape as all other checks so callers always get machine-readable
        # diagnostics instead of a traceback.
        checks.append(
            {
                "name": "configuration",
                "status": "fail",
                "reason": str(error),
            }
        )
        config = None

    if config is not None:
        checks.extend(
            _check_file(path, name)
            for name, path in (
                ("image_binding", config.image_binding),
                ("admin_kubeconfig", config.kubeconfig),
                ("secret_manifest", config.secret_manifest),
                ("hpa_driver", config.hpa_driver),
                ("hpa_kubeconfig", config.hpa_kubeconfig),
            )
        )

        checks.append(_image_reference_check(config))
        checks.append(_manifest_reachability_check(config))

        # Binding/nonce/source validation is independent from the local
        # Secret, kubeconfig, and host-port checks.  Evaluate it separately so
        # a single missing input never hides the rest of the operator's fixes.
        try:
            projected = config.environment(environment)
            checks.append({"name": "release_binding", "status": "pass"})
        except (DeploymentConfigError, OSError) as error:
            checks.append(
                {
                    "name": "release_binding",
                    "status": "fail",
                    "reason": str(error),
                }
            )
            projected = None

        if config.secret_manifest.is_file():
            try:
                checks.append(_secret_check(config))
            except (DeploymentConfigError, OSError) as error:
                checks.append(
                    {
                        "name": "secret_manifest_contract",
                        "status": "fail",
                        "reason": str(error),
                        "values_recorded": False,
                    }
                )
        else:
            checks.append(
                {
                    "name": "secret_manifest_contract",
                    "status": "fail",
                    "reason": "required Secret manifest is missing",
                    "values_recorded": False,
                }
            )
        checks.append(_hpa_driver_identity_check(config))
        checks.append(_kubeconfig_separation(config))
        checks.extend(_port_checks(config.ports))

    failed = [check["name"] for check in checks if check["status"] != "pass"]
    reasons = []
    for check in checks:
        if check["status"] != "pass":
            detail = check.get("reason")
            reasons.append(
                f"preflight check failed: {check['name']}" + (f": {detail}" if detail else "")
            )
    if failed:
        projected = None
    report = {
        "schema_version": 1,
        "producer": PRODUCER,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "run_id": f"{PRODUCER}-{uuid4().hex}",
        "config_path_sha256": hashlib.sha256(str(config_path.resolve()).encode()).hexdigest(),
        "config_content_sha256": (
            hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path.is_file() else None
        ),
        "checks": checks,
        "gate": "pass" if not reasons else "fail",
        "rejection_reasons": reasons,
        "secrets_recorded": False,
    }
    return report, projected


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/deployment-preflight.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report, _ = build_preflight(args.config, environment=os.environ)
    atomic_write_json(args.output, report)
    print(json.dumps({"gate": report["gate"], "output": str(args.output)}))
    return 0 if report["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
