#!/usr/bin/env python3
"""Validate real PostgreSQL, object-store, and key-recovery drill evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.candidate_lock import verify_candidate_lock
from scripts.evidence_lineage import build_evidence, current_release_binding, new_run_id
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.disaster_recovery_gate"
COMPONENTS = ("postgres_pitr", "artifact_restore", "key_restore")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
IMAGE_REFERENCE_RE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
KUBERNETES_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
KUBERNETES_CONTEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
REDUNDANT_STORAGE_TIERS = frozenset(
    {
        "cross_region_redundant",
        "cross_zone_redundant",
        "managed_ha",
        "object_versioned_remote",
        "remote_redundant",
    }
)
OBJECT_STORE_BACKENDS = frozenset({"minio", "object_store", "s3"})
SENSITIVE_KEY_MARKERS = frozenset(
    {
        "access_key",
        "credential",
        "database_url",
        "dsn",
        "password",
        "secret_key",
        "token",
    }
)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"drill evidence is missing or a symlink: {path.name}")

    def duplicate_key(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    value = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=duplicate_key,
        parse_constant=lambda constant: (_ for _ in ()).throw(
            ValueError(f"non-finite JSON value: {constant}")
        ),
    )
    if not isinstance(value, dict):
        raise ValueError(f"drill evidence root is not an object: {path.name}")
    return value


def _parse_timestamp(value: object, *, now: datetime) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    parsed = parsed.astimezone(UTC)
    if parsed > now or now - parsed >= timedelta(hours=24):
        return None
    return parsed


def _timestamp(value: object, *, now: datetime) -> bool:
    return _parse_timestamp(value, now=now) is not None


def _positive_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _sha256(value: object) -> str | None:
    return value if isinstance(value, str) and SHA_RE.fullmatch(value) else None


def _safe_hash(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sensitive_key(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.lower().replace("-", "_")
    if normalized.endswith("_sha256"):
        return False
    return any(marker in normalized for marker in SENSITIVE_KEY_MARKERS)


def _contains_sensitive_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            _sensitive_key(key) or _contains_sensitive_key(item) for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_contains_sensitive_key(item) for item in value)
    return False


def _execution_value(item: Mapping[str, Any], key: str) -> object:
    execution = _mapping(item.get("execution"))
    return execution.get(key)


def _validate_execution(
    item: Mapping[str, Any],
    *,
    component: str,
    expected_image: object,
    expected_lock_binding: object,
    current: datetime,
) -> tuple[list[str], dict[str, str]]:
    """Validate API-observed Kubernetes Job identity without retaining secrets."""

    prefix = f"{component}:"
    reasons: list[str] = []
    execution = _mapping(item.get("execution"))
    if execution.get("kind") != "kubernetes_job":
        reasons.append(f"{prefix} restore execution is not a Kubernetes Job")
    if execution.get("source") != "kubectl_api":
        reasons.append(f"{prefix} restore execution was not observed through kubectl API")
    if execution.get("status") != "succeeded":
        reasons.append(f"{prefix} Kubernetes restore Job did not succeed")
    for field in ("completion_confirmed", "isolated_namespace", "production_mutation_checked"):
        if execution.get(field) is not True:
            reasons.append(f"{prefix} execution check {field} is not true")
    for field, expected in (("succeeded", 1), ("failed", 0), ("active", 0)):
        value = execution.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            reasons.append(f"{prefix} Kubernetes Job {field} count is not {expected}")

    context = execution.get("context")
    context_hash = execution.get("context_sha256")
    namespace = execution.get("namespace")
    namespace_hash = execution.get("namespace_sha256")
    if not isinstance(context, str) or KUBERNETES_CONTEXT_RE.fullmatch(context) is None:
        reasons.append(f"{prefix} Kubernetes context is missing or invalid")
    else:
        calculated = hashlib.sha256(context.encode("utf-8")).hexdigest()
        if context_hash is not None and context_hash != calculated:
            reasons.append(f"{prefix} Kubernetes context hash does not match")
        context_hash = calculated
    if (
        not isinstance(namespace, str)
        or len(namespace) > 253
        or KUBERNETES_NAME_RE.fullmatch(namespace) is None
    ):
        reasons.append(f"{prefix} Kubernetes namespace is missing or invalid")
    else:
        if namespace in {"default", "kube-system", "trpc-service"}:
            reasons.append(f"{prefix} restore namespace is not isolated from production")
        calculated = hashlib.sha256(namespace.encode("utf-8")).hexdigest()
        if namespace_hash is not None and namespace_hash != calculated:
            reasons.append(f"{prefix} Kubernetes namespace hash does not match")
        namespace_hash = calculated
    for field in ("cluster_uid_sha256", "namespace_uid_sha256", "job_uid_sha256", "pod_uid_sha256"):
        if _sha256(execution.get(field)) is None:
            reasons.append(f"{prefix} Kubernetes {field} is missing or invalid")
    job_name = execution.get("job_name")
    if (
        not isinstance(job_name, str)
        or len(job_name) > 253
        or KUBERNETES_NAME_RE.fullmatch(job_name) is None
    ):
        reasons.append(f"{prefix} Kubernetes Job name is missing or invalid")
    image_reference = execution.get("image_reference")
    if (
        not isinstance(image_reference, str)
        or IMAGE_REFERENCE_RE.fullmatch(image_reference) is None
        or "/" not in image_reference.split("@", 1)[0]
    ):
        reasons.append(f"{prefix} restore Job image is not an immutable registry reference")
    elif image_reference.rsplit("@", 1)[1] != expected_image:
        reasons.append(f"{prefix} restore Job image does not match candidate lock")
    image_id = execution.get("image_id")
    if (
        not isinstance(image_id, str)
        or not isinstance(expected_image, str)
        or IMAGE_RE.fullmatch(expected_image) is None
        or expected_image not in image_id
    ):
        reasons.append(f"{prefix} observed restore Job image ID does not match candidate lock")
    if execution.get("candidate_lock_binding_sha256") != expected_lock_binding:
        reasons.append(f"{prefix} restore Job candidate lock binding is missing or changed")
    started = _parse_timestamp(execution.get("started_at"), now=current)
    completed = _parse_timestamp(execution.get("completed_at"), now=current)
    if started is None or completed is None:
        reasons.append(f"{prefix} Kubernetes Job start/completion timestamps are missing or stale")
    elif completed < started:
        reasons.append(f"{prefix} Kubernetes Job completed before it started")
    return reasons, {
        "context_sha256": context_hash or "",
        "namespace_sha256": namespace_hash or "",
        "cluster_uid_sha256": str(execution.get("cluster_uid_sha256", "")),
        "namespace_uid_sha256": str(execution.get("namespace_uid_sha256", "")),
        "job_uid_sha256": str(execution.get("job_uid_sha256", "")),
    }


def _validate_backup(
    item: Mapping[str, Any], *, component: str, current: datetime
) -> tuple[list[str], tuple[datetime | None, datetime | None]]:
    prefix = f"{component}:"
    reasons: list[str] = []
    backup = _mapping(item.get("backup"))
    backend = backup.get("backend")
    if component == "postgres_pitr":
        if backend not in {"postgres", "postgresql"}:
            reasons.append(f"{prefix} restore is not backed by PostgreSQL")
        if backup.get("pitr_enabled") is not True:
            reasons.append(f"{prefix} PostgreSQL PITR evidence is missing")
    elif component in {"artifact_restore", "key_restore"}:
        if backend not in OBJECT_STORE_BACKENDS and not (
            component == "key_restore" and backend == "kms"
        ):
            reasons.append(f"{prefix} restore is not backed by S3/MinIO-compatible storage")
        if component == "artifact_restore" and backup.get("versioning_enabled") is not True:
            reasons.append(f"{prefix} object-store versioning evidence is missing")
        if component == "key_restore" and backup.get("key_versioned") is not True:
            reasons.append(f"{prefix} key-version evidence is missing")
    tier = backup.get("storage_tier")
    if tier not in REDUNDANT_STORAGE_TIERS or backup.get("disaster_redundant") is not True:
        reasons.append(f"{prefix} backup storage tier is not disaster-redundant")
    if backup.get("replication_verified") is not True:
        reasons.append(f"{prefix} backup replication was not verified")
    for field in ("backup_id_sha256", "restore_id_sha256"):
        if _sha256(backup.get(field)) is None:
            reasons.append(f"{prefix} backup {field} is missing or invalid")
    created = _parse_timestamp(backup.get("created_at"), now=current)
    restore_started = _parse_timestamp(backup.get("restore_started_at"), now=current)
    if created is None or restore_started is None:
        reasons.append(f"{prefix} backup/restore timestamps are missing or stale")
    elif restore_started < created:
        reasons.append(f"{prefix} restore started before the backup was created")
    return reasons, (created, restore_started)


def validate_drill(
    observations: Mapping[str, Mapping[str, Any]],
    *,
    lock: Mapping[str, Any],
    binding: Mapping[str, Any],
    max_rpo_seconds: float,
    max_rto_seconds: float,
    now: datetime | None = None,
) -> list[str]:
    """Validate three independent Kubernetes restore Jobs against one candidate.

    The input is intentionally an observation, not a user-supplied summary:
    each component must carry the API-observed Job identity, immutable image,
    completion counters, backup tier and ordered timestamps.  This keeps an
    offline fixture or a node-local volume from being promoted to a production
    disaster-recovery result.
    """

    reasons = verify_candidate_lock(lock, binding, root=ROOT)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    expected_release = lock.get("release_binding")
    expected_source = _mapping(lock.get("source_fingerprint")).get("value")
    expected_image = lock.get("image_digest")
    expected_lock_binding = lock.get("binding_sha256")
    drill_ids: set[object] = set()
    tenant_hashes: set[object] = set()
    canaries: set[object] = set()
    run_ids: set[object] = set()
    contexts: set[str] = set()
    namespaces: set[str] = set()
    clusters: set[str] = set()
    namespace_uids: set[str] = set()
    job_uids: set[str] = set()
    required_checks = {
        "postgres_pitr": ("point_in_time_recovery", "backup_integrity_verified"),
        "artifact_restore": ("versioned_restore", "checksum_verified"),
        "key_restore": ("key_version_restored", "decrypt_verified"),
    }
    if (
        not math.isfinite(max_rpo_seconds)
        or max_rpo_seconds < 0
        or not math.isfinite(max_rto_seconds)
        or max_rto_seconds < 0
    ):
        reasons.append("disaster recovery RPO/RTO objectives are invalid")
    if set(observations) != set(COMPONENTS):
        reasons.append("disaster recovery component set is incomplete")
    for component in COMPONENTS:
        item = _mapping(observations.get(component))
        prefix = f"{component}:"
        if _contains_sensitive_key(item):
            reasons.append(f"{prefix} observation contains a sensitive field")
        if (
            item.get("schema_version") != 1
            or item.get("kind") != "disaster_recovery_observation"
            or item.get("component") != component
            or item.get("status") != "pass"
        ):
            reasons.append(f"{prefix} observation did not pass its real restore job")
        if item.get("release_binding") != expected_release:
            reasons.append(f"{prefix} release binding does not match candidate lock")
        if _mapping(item.get("source_fingerprint")).get("value") != expected_source:
            reasons.append(f"{prefix} source fingerprint does not match candidate lock")
        if item.get("image_digest") != expected_image:
            reasons.append(f"{prefix} image digest does not match candidate lock")
        if item.get("isolated_restore_target") is not True:
            reasons.append(f"{prefix} restore was not isolated from production")
        if item.get("production_system_mutated") is not False:
            reasons.append(f"{prefix} production mutation safety assertion is missing")
        generated_at = _parse_timestamp(item.get("generated_at"), now=current)
        if generated_at is None:
            reasons.append(f"{prefix} observation is stale or has an invalid timestamp")
        run_id = item.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            reasons.append(f"{prefix} run_id is missing")
        else:
            run_ids.add(run_id)
        drill_ids.add(item.get("drill_id"))
        tenant_hashes.add(item.get("tenant_id_hash"))
        if _sha256(item.get("tenant_id_hash")) is None:
            reasons.append(f"{prefix} tenant_id_hash is missing or invalid")
        expected_canary = item.get("canary_sha256")
        restored_canary = item.get("restored_canary_sha256")
        if (
            not isinstance(expected_canary, str)
            or SHA_RE.fullmatch(expected_canary) is None
            or restored_canary != expected_canary
        ):
            reasons.append(f"{prefix} restored canary checksum does not match")
        else:
            canaries.add(expected_canary)
        rpo = _positive_number(item.get("rpo_seconds"))
        rto = _positive_number(item.get("rto_seconds"))
        if rpo is None or rpo > max_rpo_seconds:
            reasons.append(f"{prefix} RPO exceeds the configured objective")
        if rto is None or rto > max_rto_seconds:
            reasons.append(f"{prefix} RTO exceeds the configured objective")
        for check in required_checks[component]:
            if item.get(check) is not True:
                reasons.append(f"{prefix} required check {check} is not true")
        execution_reasons, identity = _validate_execution(
            item,
            component=component,
            expected_image=expected_image,
            expected_lock_binding=expected_lock_binding,
            current=current,
        )
        reasons.extend(execution_reasons)
        if identity["context_sha256"]:
            contexts.add(identity["context_sha256"])
        if identity["namespace_sha256"]:
            namespaces.add(identity["namespace_sha256"])
        if identity["cluster_uid_sha256"]:
            clusters.add(identity["cluster_uid_sha256"])
        if identity["namespace_uid_sha256"]:
            namespace_uids.add(identity["namespace_uid_sha256"])
        if identity["job_uid_sha256"]:
            job_uids.add(identity["job_uid_sha256"])
        backup_reasons, (backup_created, restore_started) = _validate_backup(
            item, component=component, current=current
        )
        reasons.extend(backup_reasons)
        execution = _mapping(item.get("execution"))
        completed = _parse_timestamp(execution.get("completed_at"), now=current)
        if generated_at is not None and completed is not None and completed > generated_at:
            reasons.append(f"{prefix} observation was generated before Job completion")
        if completed is not None and restore_started is not None:
            actual_rto = (completed - restore_started).total_seconds()
            if actual_rto < 0:
                reasons.append(f"{prefix} RTO timestamps are out of order")
            elif rto is not None and abs(rto - actual_rto) > 1:
                reasons.append(f"{prefix} RTO does not match Job timestamps")
        if backup_created is not None and restore_started is not None:
            actual_rpo = (restore_started - backup_created).total_seconds()
            if actual_rpo < 0:
                reasons.append(f"{prefix} RPO timestamps are out of order")
            elif rpo is not None and abs(rpo - actual_rpo) > 1:
                reasons.append(f"{prefix} RPO does not match backup timestamps")
        validation = _mapping(item.get("validation"))
        if validation.get("source") != "restore_job_output" or validation.get("status") != "pass":
            reasons.append(f"{prefix} restore validation did not come from the Job output")
        if validation.get("production_data_touched") is not False:
            reasons.append(f"{prefix} restore validation did not prove production isolation")
    if len(run_ids) != len(COMPONENTS):
        reasons.append("disaster recovery component run_ids are missing or reused")
    for shared_values, name in (
        (drill_ids, "drill_id"),
        (tenant_hashes, "tenant_id_hash"),
        (canaries, "canary checksum"),
    ):
        value = next(iter(shared_values), None)
        if len(shared_values) != 1 or not isinstance(value, str) or not value:
            reasons.append(f"disaster recovery components do not share one {name}")
    for identity_values, name in (
        (contexts, "Kubernetes context"),
        (namespaces, "Kubernetes namespace"),
        (clusters, "Kubernetes cluster"),
        (namespace_uids, "Kubernetes namespace UID"),
    ):
        if len(identity_values) != 1:
            reasons.append(f"disaster recovery components do not share one {name}")
    if len(job_uids) != len(COMPONENTS):
        reasons.append("disaster recovery restore Job UIDs are missing or reused")
    return list(dict.fromkeys(reasons))


def build_report(
    *,
    enabled: bool,
    evidence_paths: Mapping[str, Path],
    lock_path: Path,
    binding_path: Path,
    max_rpo_seconds: float,
    max_rto_seconds: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    run_id = new_run_id(PRODUCER)
    if not enabled:
        reason = "TRPC_DR_DRILL_ENABLED=true was not supplied; destructive restore jobs are opt-in"
        return {
            "schema_version": 1,
            "baseline": {"required_components": list(COMPONENTS)},
            "candidate": {"mode": "not_run", "components": {}},
            "case_deltas": {"failed_components": list(COMPONENTS)},
            "gate": "not_run",
            "production_gate": "not_run",
            "rejection_reasons": [reason],
            "production_rejection_reasons": [reason],
        }
    try:
        lock = _read(lock_path)
        binding = _read(binding_path)
        observations = {name: _read(path) for name, path in evidence_paths.items()}
        expected_release = current_release_binding(required=True)
        reasons = validate_drill(
            observations,
            lock=lock,
            binding=binding,
            max_rpo_seconds=max_rpo_seconds,
            max_rto_seconds=max_rto_seconds,
            now=now,
        )
        if lock.get("release_binding") != expected_release:
            reasons.append("candidate lock belongs to a different release environment")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        lock = {}
        observations = {}
        reasons = [str(error)]
    image_digest = lock.get("image_digest")

    def evidence_hash(path: Path) -> str | None:
        if path.is_symlink() or not path.is_file():
            return None
        try:
            return hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return None

    def execution_summary(item: Mapping[str, Any]) -> dict[str, Any]:
        execution = _mapping(item.get("execution"))
        context = execution.get("context")
        namespace = execution.get("namespace")
        context_hash = execution.get("context_sha256")
        namespace_hash = execution.get("namespace_sha256")
        if _sha256(context_hash) is None:
            context_hash = _safe_hash(context)
        if _sha256(namespace_hash) is None:
            namespace_hash = _safe_hash(namespace)
        return {
            "source": execution.get("source"),
            "context_sha256": context_hash,
            "namespace_sha256": namespace_hash,
            "cluster_uid_sha256": execution.get("cluster_uid_sha256"),
            "namespace_uid_sha256": execution.get("namespace_uid_sha256"),
            "job_uid_sha256": execution.get("job_uid_sha256"),
            "status": execution.get("status", "not_run"),
        }

    evidence = build_evidence(
        root=ROOT,
        producer=PRODUCER,
        run_id=run_id,
        runtime={
            "image_digest": image_digest,
            "component_evidence_sha256": {
                name: digest
                for name, path in evidence_paths.items()
                if (digest := evidence_hash(path)) is not None
            },
        },
    )
    components = {
        name: {
            "status": _mapping(item).get("status", "not_run"),
            "run_id": _mapping(item).get("run_id"),
            "rpo_seconds": _mapping(item).get("rpo_seconds"),
            "rto_seconds": _mapping(item).get("rto_seconds"),
            "backend": _mapping(_mapping(item).get("backup")).get("backend"),
            "storage_tier": _mapping(_mapping(item).get("backup")).get("storage_tier"),
            "disaster_redundant": _mapping(_mapping(item).get("backup")).get(
                "disaster_redundant", False
            ),
            "execution": execution_summary(_mapping(item)),
        }
        for name, item in observations.items()
    }
    failed_components = [
        name for name in COMPONENTS if any(reason.startswith(f"{name}:") for reason in reasons)
    ]
    if reasons and not failed_components:
        failed_components = list(COMPONENTS)
    runtime_summary: dict[str, Any] = {}
    if observations:
        first = _mapping(observations.get(COMPONENTS[0]))
        runtime_summary = execution_summary(first)
        runtime_summary["platform"] = "kubernetes"
    gate = "fail" if reasons else "pass"
    return {
        "schema_version": 1,
        "baseline": {
            "required_components": list(COMPONENTS),
            "max_rpo_seconds": max_rpo_seconds,
            "max_rto_seconds": max_rto_seconds,
        },
        "candidate": {
            "mode": "isolated_restore_drill",
            "platform": "kubernetes",
            "lineage": {"image_digest": image_digest},
            "candidate_lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest()
            if lock_path.is_file() and not lock_path.is_symlink()
            else None,
            "runtime": runtime_summary,
            "components": components,
        },
        "case_deltas": {"failed_components": failed_components},
        "run_id": run_id,
        "evidence": evidence,
        "gate": gate,
        "production_gate": gate,
        "rejection_reasons": reasons,
        "production_rejection_reasons": reasons,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("runs/drill"))
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/disaster-recovery.json")
    )
    parser.add_argument(
        "--candidate-lock", type=Path, default=Path("runs/multitenant/candidate-lock.json")
    )
    parser.add_argument(
        "--image-binding", type=Path, default=Path("runs/multitenant/registry-image-binding.json")
    )
    parser.add_argument("--max-rpo-seconds", type=float, default=300)
    parser.add_argument("--max-rto-seconds", type=float, default=3_600)
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args(argv)
    enabled = os.getenv("TRPC_DR_DRILL_ENABLED", "").strip().lower() == "true"
    report = build_report(
        enabled=enabled,
        evidence_paths={name: args.directory / f"{name}.json" for name in COMPONENTS},
        lock_path=args.candidate_lock,
        binding_path=args.image_binding,
        max_rpo_seconds=args.max_rpo_seconds,
        max_rto_seconds=args.max_rto_seconds,
    )
    rendered = atomic_write_json(args.output, report)
    print(rendered, end="")
    return 1 if args.require_production and report["production_gate"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
