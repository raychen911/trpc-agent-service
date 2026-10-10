#!/usr/bin/env python3
"""Create and validate a content-addressed production release evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.evidence_lineage import canonical_sha256, source_fingerprint
from scripts.real_runtime_gate import _role_evidence_check
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_NAME = "release-manifest.json"
MANIFEST_SCHEMA_VERSION = 1
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
_RUN_NONCE_RE = re.compile(r"^[A-Za-z0-9._:-]{16,128}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_REPORT_BYTES = 64 * 1024 * 1024
_OPTIONAL_SDK_REPORT_NAMES = ("postgres-worker-gate.json", "postgres_worker_gate.json")
_FUNCTIONAL_DR_LOGICAL_NAME = "functional_disaster_recovery"
_DESTRUCTIVE_DR_LOGICAL_NAME = "disaster_recovery"
_FUNCTIONAL_DR_POLICY_MODE = "explicit_functional_dr"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def _has_symlink_component(path: Path) -> bool:
    current = path.absolute()
    while True:
        if current.is_symlink():
            return True
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _read_report(path: Path) -> dict[str, Any]:
    if _has_symlink_component(path) or not path.is_file():
        raise ValueError(f"release report is missing or is a symlink: {path.name}")
    if path.stat().st_size > _MAX_REPORT_BYTES:
        raise ValueError(f"release report exceeds size limit: {path.name}")
    value = json.loads(
        path.read_text(encoding="utf-8"),
        parse_constant=_reject_constant,
        object_pairs_hook=_unique_object,
    )
    if not isinstance(value, dict):
        raise ValueError(f"release report root is not an object: {path.name}")
    return value


def report_image_digest(report_name: str, report: Mapping[str, Any]) -> str | None:
    candidate = _mapping(report.get("candidate"))
    value: object = None
    if report_name == "backend-compose.json":
        value = _mapping(candidate.get("runtime_attestation")).get("image_digest")
    elif report_name == "real-performance.json":
        workers = _mapping(candidate.get("preflight")).get("worker_processes")
        digests: set[object] = set()
        if isinstance(workers, list):
            digests = {item.get("image_id") for item in workers if isinstance(item, Mapping)}
        value = next(iter(digests)) if len(digests) == 1 else None
    elif report_name == "real-runtime.json":
        value = _mapping(_mapping(candidate.get("preflight")).get("image_attestation")).get(
            "image_id"
        )
    elif report_name in {
        "fault-injection.json",
        "migration-live.json",
        "kubernetes-runtime.json",
        "disaster-recovery.json",
        "disaster-recovery-functional.json",
    }:
        value = _mapping(candidate.get("lineage")).get("image_digest")
    elif report_name == "im-online.json":
        raw = candidate.get("runtime_image_digest")
        value = f"sha256:{raw}" if isinstance(raw, str) and _SHA_RE.fullmatch(raw) else None
    if isinstance(value, str):
        normalized = value.lower()
        if _IMAGE_RE.fullmatch(normalized):
            return normalized
    return None


def _release_binding(evidence: Mapping[str, Any]) -> tuple[str, str] | None:
    binding = _mapping(evidence.get("release_binding"))
    release_id = binding.get("release_id")
    nonce_sha256 = binding.get("nonce_sha256")
    if (
        isinstance(release_id, str)
        and _ID_RE.fullmatch(release_id)
        and isinstance(nonce_sha256, str)
        and _SHA_RE.fullmatch(nonce_sha256)
    ):
        return release_id, nonce_sha256
    return None


def _optional_sdk_attachment(directory: Path) -> dict[str, Any] | None:
    """Record an SDK Worker gate without treating it as service evidence."""

    path = next(
        (directory / name for name in _OPTIONAL_SDK_REPORT_NAMES if (directory / name).exists()),
        None,
    )
    if path is None:
        return None
    filename = path.name
    report = _read_report(path)
    evidence = _mapping(report.get("evidence"))
    lineage_bound = (
        evidence.get("kind") == "current_candidate"
        and isinstance(evidence.get("source_fingerprint"), Mapping)
        and evidence.get("release_binding") is not None
    )
    return {
        "filename": filename,
        "sha256": canonical_sha256(report),
        "producer": "sdk.postgres_worker_gate",
        "lineage_status": "bound" if lineage_bound else "unbound_not_substitute",
        "substitutes_service_runtime": False,
    }


def _expected_functional_dr_policy(
    reports: Mapping[str, object],
    *,
    allow_functional_dr: bool,
    authorized_not_run_gates: tuple[str, ...],
) -> dict[str, Any] | None:
    if not allow_functional_dr:
        if authorized_not_run_gates:
            raise ValueError("functional DR authorization requires --allow-functional-dr")
        if _FUNCTIONAL_DR_LOGICAL_NAME in reports:
            raise ValueError("functional DR evidence requires --allow-functional-dr")
        return None
    if _FUNCTIONAL_DR_LOGICAL_NAME not in reports:
        raise ValueError("functional DR policy requires functional disaster recovery evidence")
    if authorized_not_run_gates not in {(), (_DESTRUCTIVE_DR_LOGICAL_NAME,)}:
        raise ValueError("functional DR policy can authorize only disaster_recovery=not_run")
    destructive_included = _DESTRUCTIVE_DR_LOGICAL_NAME in reports
    destructive_authorized = bool(authorized_not_run_gates)
    if destructive_included == destructive_authorized:
        raise ValueError(
            "functional DR policy must exclude only an authorized not_run destructive report"
        )
    return {
        "mode": _FUNCTIONAL_DR_POLICY_MODE,
        "functional_report": _FUNCTIONAL_DR_LOGICAL_NAME,
        "destructive_report": _DESTRUCTIVE_DR_LOGICAL_NAME,
        "authorized_not_run_gates": list(authorized_not_run_gates),
    }


def _functional_dr_rejection_reason(report: dict[str, Any], *, report_path: Path) -> str | None:
    if report.get("gate") != "pass" or report.get("production_gate") != "not_run":
        return "functional disaster recovery must be gate=pass and production_gate=not_run"
    from scripts.release_gate import _production_evidence_result

    _, reason = _production_evidence_result(
        report,
        report_name=report_path.name,
        report_path=report_path,
    )
    return reason


def _production_report_rejection_reason(report: Mapping[str, Any]) -> str | None:
    if report.get("gate") != "pass":
        return "production report gate must be pass"
    if report.get("production_gate") != "pass":
        return "production report production_gate must be pass"
    return None


def _candidate_lock_identity(directory: Path) -> tuple[tuple[str, str], str, str]:
    lock = _read_report(directory / "candidate-lock.json")
    binding = _read_report(directory / "registry-image-binding.json")
    from scripts.candidate_lock import verify_candidate_lock

    lock_reasons = verify_candidate_lock(lock, binding, root=ROOT)
    if lock_reasons:
        raise ValueError("candidate lock does not match registry binding: " + lock_reasons[0])
    if lock.get("schema_version") != 1 or lock.get("kind") != "release_candidate_lock":
        raise ValueError("candidate lock schema or kind is invalid")
    release = _mapping(lock.get("release_binding"))
    release_id = release.get("release_id")
    nonce_sha256 = release.get("nonce_sha256")
    image_digest = lock.get("image_digest")
    source_value = _mapping(lock.get("source_fingerprint")).get("value")
    if not isinstance(release_id, str) or _ID_RE.fullmatch(release_id) is None:
        raise ValueError("candidate lock release_id is invalid")
    if not isinstance(nonce_sha256, str) or _SHA_RE.fullmatch(nonce_sha256) is None:
        raise ValueError("candidate lock nonce binding is invalid")
    if not isinstance(image_digest, str) or _IMAGE_RE.fullmatch(image_digest) is None:
        raise ValueError("candidate lock image digest is invalid")
    if not isinstance(source_value, str) or _SHA_RE.fullmatch(source_value) is None:
        raise ValueError("candidate lock source fingerprint is invalid")
    return (release_id, nonce_sha256), image_digest, source_value


def build_manifest(
    directory: Path,
    *,
    reports: Mapping[str, str],
    release_id: str,
    release_nonce: str,
    image_digest: str,
    allow_functional_dr: bool = False,
    authorized_not_run_gates: tuple[str, ...] = (),
) -> dict[str, Any]:
    if _ID_RE.fullmatch(release_id) is None:
        raise ValueError("release_id is invalid")
    if _NONCE_RE.fullmatch(release_nonce) is None:
        raise ValueError("release_nonce must contain 32..256 safe characters")
    normalized_image = image_digest.lower()
    if _IMAGE_RE.fullmatch(normalized_image) is None:
        raise ValueError("image_digest must match sha256:<64 lowercase hex>")
    nonce_sha256 = hashlib.sha256(release_nonce.encode("utf-8")).hexdigest()
    source = source_fingerprint(ROOT)
    if source.get("status") != "available":
        raise ValueError("current source fingerprint is unavailable")
    lock_release, lock_image, lock_source = _candidate_lock_identity(directory)
    if lock_release != (release_id, nonce_sha256):
        raise ValueError("release binding does not match candidate lock")
    if lock_image != normalized_image:
        raise ValueError("release image digest does not match candidate lock")
    if lock_source != source.get("value"):
        raise ValueError("release source fingerprint does not match candidate lock")
    policy = _expected_functional_dr_policy(
        reports,
        allow_functional_dr=allow_functional_dr,
        authorized_not_run_gates=authorized_not_run_gates,
    )
    entries: dict[str, Any] = {}
    observed_run_nonces: set[str] = set()
    for logical_name, filename in sorted(reports.items()):
        report_path = directory / filename
        report = _read_report(report_path)
        if logical_name == _FUNCTIONAL_DR_LOGICAL_NAME:
            functional_reason = _functional_dr_rejection_reason(
                report,
                report_path=report_path,
            )
            if functional_reason is not None:
                raise ValueError(
                    f"functional disaster recovery evidence is invalid: {functional_reason}"
                )
        else:
            production_reason = _production_report_rejection_reason(report)
            if production_reason is not None:
                raise ValueError(f"{filename} is not production-valid: {production_reason}")
        if filename == "real-runtime.json":
            role_status, role_reason = _role_evidence_check(
                _mapping(report.get("candidate")).get("database_role_evidence")
            )
            if role_status != "pass":
                raise ValueError(
                    "real-runtime.json database role evidence is not production-valid: "
                    f"{role_reason or role_status}"
                )
        evidence = _mapping(report.get("evidence"))
        if _release_binding(evidence) != (release_id, nonce_sha256):
            raise ValueError(f"{filename} is not bound to this release")
        if _mapping(evidence.get("source_fingerprint")).get("value") != source.get("value"):
            raise ValueError(f"{filename} belongs to a different source candidate")
        observed_image = report_image_digest(filename, report)
        if observed_image != normalized_image:
            raise ValueError(f"{filename} does not attest the release image digest")
        run_nonce = evidence.get("run_nonce")
        if not isinstance(run_nonce, str) or _RUN_NONCE_RE.fullmatch(run_nonce) is None:
            raise ValueError(f"{filename} has no valid one-time run nonce")
        if run_nonce in observed_run_nonces:
            raise ValueError(f"{filename} reuses another report run nonce")
        observed_run_nonces.add(run_nonce)
        entries[logical_name] = {
            "filename": filename,
            "sha256": canonical_sha256(report),
            "producer": evidence.get("producer"),
            "run_id": evidence.get("run_id"),
            "run_nonce": run_nonce,
            "generated_at": evidence.get("generated_at"),
            "image_digest": observed_image,
        }
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": "production_release_evidence",
        "release_id": release_id,
        "nonce_sha256": nonce_sha256,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_fingerprint": source,
        "image_digest": normalized_image,
        "reports": entries,
    }
    if policy is not None:
        manifest["policy"] = policy
    sdk_attachment = _optional_sdk_attachment(directory)
    if sdk_attachment is not None:
        manifest["auxiliary_reports"] = {"sdk_postgres_worker": sdk_attachment}
    return manifest


def validate_manifest(
    directory: Path,
    *,
    reports: Mapping[str, tuple[str, str]],
    current_source: Mapping[str, Any],
    now: datetime | None = None,
    ttl_seconds: int = 24 * 60 * 60,
    allow_functional_dr: bool = False,
    authorized_not_run_gates: tuple[str, ...] = (),
) -> tuple[str, list[str]]:
    path = directory / MANIFEST_NAME
    if _has_symlink_component(path):
        return "fail", ["release evidence manifest path contains a symlink"]
    if not path.exists():
        return "not_run", ["release evidence manifest is missing"]
    try:
        manifest = _read_report(path)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        return "fail", [f"release evidence manifest is invalid: {type(error).__name__}"]
    reasons: list[str] = []
    try:
        lock_release, lock_image, lock_source = _candidate_lock_identity(directory)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        lock_release = None
        lock_image = None
        lock_source = None
        reasons.append(f"candidate lock is invalid: {type(error).__name__}")
    try:
        expected_policy = _expected_functional_dr_policy(
            reports,
            allow_functional_dr=allow_functional_dr,
            authorized_not_run_gates=authorized_not_run_gates,
        )
    except ValueError as error:
        return "fail", [str(error)]
    if expected_policy is None:
        if "policy" in manifest:
            reasons.append("release evidence manifest has an unauthorized functional DR policy")
    elif manifest.get("policy") != expected_policy:
        reasons.append("release evidence manifest functional DR policy is invalid")
    schema_version = manifest.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != MANIFEST_SCHEMA_VERSION
    ):
        reasons.append("release evidence manifest schema is unsupported")
    if manifest.get("kind") != "production_release_evidence":
        reasons.append("release evidence manifest kind is invalid")
    release_id = manifest.get("release_id")
    nonce_sha256 = manifest.get("nonce_sha256")
    if not isinstance(release_id, str) or _ID_RE.fullmatch(release_id) is None:
        reasons.append("release evidence manifest release_id is invalid")
    if not isinstance(nonce_sha256, str) or _SHA_RE.fullmatch(nonce_sha256) is None:
        reasons.append("release evidence manifest nonce binding is invalid")
    if lock_release is not None and lock_release != (release_id, nonce_sha256):
        reasons.append("release evidence manifest binding does not match candidate lock")
    image_digest = manifest.get("image_digest")
    if not isinstance(image_digest, str) or _IMAGE_RE.fullmatch(image_digest) is None:
        reasons.append("release evidence manifest image digest is invalid")
    if lock_image is not None and image_digest != lock_image:
        reasons.append("release evidence manifest image digest does not match candidate lock")
    source = _mapping(manifest.get("source_fingerprint"))
    if (
        source.get("algorithm") != "sha256"
        or source.get("status") != "available"
        or not isinstance(source.get("value"), str)
        or _SHA_RE.fullmatch(str(source.get("value"))) is None
        or current_source.get("algorithm") != "sha256"
        or current_source.get("status") != "available"
        or source.get("value") != current_source.get("value")
    ):
        reasons.append("release evidence manifest belongs to a different source candidate")
    if lock_source is not None and source.get("value") != lock_source:
        reasons.append("release evidence manifest source does not match candidate lock")
    generated_at = manifest.get("generated_at")
    try:
        parsed_raw = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        reasons.append("release evidence manifest generated_at is invalid")
    else:
        if parsed_raw.tzinfo is None or parsed_raw.utcoffset() is None:
            reasons.append("release evidence manifest generated_at has no timezone")
            parsed = None
        else:
            parsed = parsed_raw.astimezone(UTC)
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if parsed is not None and (
            parsed > current or current - parsed >= timedelta(seconds=ttl_seconds)
        ):
            reasons.append("release evidence manifest is stale or from the future")
    entries = _mapping(manifest.get("reports"))
    if set(entries) != set(reports):
        reasons.append("release evidence manifest report set is incomplete")
    if list(entries) != sorted(reports):
        reasons.append("release evidence manifest report order is not canonical")
    observed_run_nonces: set[str] = set()
    for logical_name, (filename, producer) in reports.items():
        entry = _mapping(entries.get(logical_name))
        try:
            report = _read_report(directory / filename)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            reasons.append(f"release report {filename} cannot be validated")
            continue
        if filename == "real-runtime.json":
            role_status, role_reason = _role_evidence_check(
                _mapping(report.get("candidate")).get("database_role_evidence")
            )
            if role_status != "pass":
                reasons.append(
                    "release report real-runtime.json database role evidence is not "
                    f"production-valid: {role_reason or role_status}"
                )
        evidence = _mapping(report.get("evidence"))
        if logical_name == _FUNCTIONAL_DR_LOGICAL_NAME:
            functional_reason = _functional_dr_rejection_reason(
                report,
                report_path=directory / filename,
            )
            if functional_reason is not None:
                reasons.append(
                    "release report disaster-recovery-functional.json is invalid: "
                    f"{functional_reason}"
                )
        else:
            production_reason = _production_report_rejection_reason(report)
            if production_reason is not None:
                reasons.append(f"release report {filename} is invalid: {production_reason}")
        if _release_binding(evidence) != (release_id, nonce_sha256):
            reasons.append(f"release report {filename} has a different release binding")
        report_source = _mapping(evidence.get("source_fingerprint"))
        if (
            report_source.get("algorithm") != "sha256"
            or report_source.get("status") != "available"
            or not _SHA_RE.fullmatch(str(report_source.get("value", "")))
            or report_source.get("value") != current_source.get("value")
        ):
            reasons.append(f"release report {filename} belongs to a different source candidate")
        if entry.get("filename") != filename:
            reasons.append(f"release manifest filename mismatch for {logical_name}")
        if entry.get("sha256") != canonical_sha256(report):
            reasons.append(f"release report {filename} content hash mismatch")
        if entry.get("producer") != producer or evidence.get("producer") != producer:
            reasons.append(f"release report {filename} producer mismatch")
        for key in ("run_id", "run_nonce", "generated_at"):
            if entry.get(key) != evidence.get(key):
                reasons.append(f"release report {filename} {key} mismatch")
        run_nonce = evidence.get("run_nonce")
        if not isinstance(run_nonce, str) or _RUN_NONCE_RE.fullmatch(run_nonce) is None:
            reasons.append(f"release report {filename} run_nonce is invalid")
        elif run_nonce in observed_run_nonces:
            reasons.append(f"release report {filename} reuses a run_nonce")
        else:
            observed_run_nonces.add(run_nonce)
        observed_image = report_image_digest(filename, report)
        if observed_image != image_digest or entry.get("image_digest") != observed_image:
            reasons.append(f"release report {filename} image digest mismatch")
    auxiliary = manifest.get("auxiliary_reports", {})
    if auxiliary is not None and not isinstance(auxiliary, Mapping):
        reasons.append("release evidence manifest auxiliary_reports is invalid")
    elif isinstance(auxiliary, Mapping):
        unknown_auxiliary = set(auxiliary) - {"sdk_postgres_worker"}
        if unknown_auxiliary:
            reasons.append("release evidence manifest contains unknown auxiliary reports")
    if isinstance(auxiliary, Mapping) and "sdk_postgres_worker" in auxiliary:
        attachment = _mapping(auxiliary.get("sdk_postgres_worker"))
        if attachment.get("filename") not in _OPTIONAL_SDK_REPORT_NAMES:
            reasons.append("SDK postgres-worker attachment filename is invalid")
        if attachment.get("substitutes_service_runtime") is not False:
            reasons.append(
                "SDK postgres-worker attachment cannot substitute service runtime evidence"
            )
        try:
            sdk_filename = attachment.get("filename")
            if not isinstance(sdk_filename, str):
                raise ValueError("SDK postgres-worker attachment filename is invalid")
            sdk_report = _read_report(directory / sdk_filename)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            reasons.append("SDK postgres-worker attachment cannot be validated")
        else:
            if attachment.get("sha256") != canonical_sha256(sdk_report):
                reasons.append("SDK postgres-worker attachment content hash mismatch")
    return ("fail", list(dict.fromkeys(reasons))) if reasons else ("pass", [])


def main() -> int:
    from scripts.release_gate import FUNCTIONAL_DR_REPORT, PRODUCTION_EVIDENCE_PRODUCERS, REPORTS

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("runs/multitenant"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--release-id", default=os.getenv("TRPC_RELEASE_ID", ""))
    parser.add_argument("--release-nonce", default=os.getenv("TRPC_RELEASE_NONCE", ""))
    parser.add_argument("--image-digest", required=True)
    parser.add_argument(
        "--allow-functional-dr",
        action="store_true",
        help="bind functional DR and explicitly authorize destructive DR=not_run",
    )
    args = parser.parse_args()
    report_names = {
        name: filename
        for name, (filename, production) in REPORTS.items()
        if production and filename in PRODUCTION_EVIDENCE_PRODUCERS
    }
    authorized_not_run_gates: tuple[str, ...] = ()
    if args.allow_functional_dr:
        report_names[_FUNCTIONAL_DR_LOGICAL_NAME] = FUNCTIONAL_DR_REPORT[0]
        destructive_path = args.directory / REPORTS[_DESTRUCTIVE_DR_LOGICAL_NAME][0]
        if destructive_path.exists():
            destructive_report = _read_report(destructive_path)
            destructive_gate = destructive_report.get("gate")
            destructive_status = destructive_report.get("production_gate", "not_run")
            if destructive_gate == "fail" or destructive_status == "fail":
                raise ValueError("failed destructive disaster recovery cannot be waived")
            if destructive_status not in {"pass", "not_run"}:
                raise ValueError("destructive disaster recovery status is invalid")
        else:
            destructive_status = "not_run"
        if destructive_status == "not_run":
            report_names.pop(_DESTRUCTIVE_DR_LOGICAL_NAME)
            authorized_not_run_gates = (_DESTRUCTIVE_DR_LOGICAL_NAME,)
    manifest = build_manifest(
        args.directory,
        reports=report_names,
        release_id=args.release_id,
        release_nonce=args.release_nonce,
        image_digest=args.image_digest,
        allow_functional_dr=args.allow_functional_dr,
        authorized_not_run_gates=authorized_not_run_gates,
    )
    output = args.output or args.directory / MANIFEST_NAME
    rendered = atomic_write_json(output, manifest)
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
