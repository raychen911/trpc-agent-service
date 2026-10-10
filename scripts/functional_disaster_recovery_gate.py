#!/usr/bin/env python3
"""Validate zero-cost, same-cluster recovery function evidence.

This gate is intentionally separate from the production disaster-recovery
gate.  A passing functional result never upgrades ``production_gate`` beyond
``not_run``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.candidate_lock import verify_candidate_lock
from scripts.disaster_recovery_gate import (
    COMPONENTS,
    _contains_sensitive_key,
    _mapping,
    _parse_timestamp,
    _positive_number,
    _read,
    _sha256,
    _validate_execution,
)
from scripts.evidence_lineage import build_evidence, new_run_id, runtime_fingerprint
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.functional_disaster_recovery_gate"
MODE = "same_cluster_zero_cost_functional"
MEASURED_RTO_TIMESTAMP_TOLERANCE_SECONDS = 0.01
KUBERNETES_TIMESTAMP_TOLERANCE_SECONDS = 1
PRODUCTION_LIMITATIONS = (
    "same-cluster ephemeral storage is not disaster-redundant",
    "PostgreSQL logical snapshot restore is not WAL point-in-time recovery",
    "synthetic MinIO key recovery is not external KMS key recovery",
)


def _exact_image_id(value: object, expected_digest: object) -> bool:
    """Require the runtime image ID to end in the candidate digest exactly.

    Kubelet image IDs are runtime-specific (for example
    ``docker-pullable://registry/name@sha256:...``), so the registry prefix is
    intentionally not constrained here.  The digest suffix must be exact;
    substring matching would allow a forged ``...@<digest>-other`` value.
    """

    if not isinstance(value, str) or not isinstance(expected_digest, str):
        return False
    if not value or any(character.isspace() for character in value):
        return False
    image_name, separator, digest = value.rpartition("@")
    return bool(image_name) and separator == "@" and digest == expected_digest


def _component_checks(component: str) -> tuple[str, ...]:
    if component == "postgres_pitr":
        return ("backup_integrity_verified",)
    if component == "artifact_restore":
        return ("versioned_restore", "checksum_verified")
    return ("key_version_restored", "decrypt_verified")


def validate_functional_drill(
    observations: Mapping[str, Mapping[str, Any]],
    *,
    lock: Mapping[str, Any],
    binding: Mapping[str, Any],
    max_rto_seconds: float,
    now: datetime | None = None,
) -> list[str]:
    """Validate three real Kubernetes Jobs without asserting disaster redundancy."""

    reasons = verify_candidate_lock(lock, binding, root=ROOT)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if not math.isfinite(max_rto_seconds) or max_rto_seconds <= 0:
        reasons.append("functional recovery RTO objective is invalid")
    if set(observations) != set(COMPONENTS):
        reasons.append("functional recovery component set is incomplete")

    expected_release = lock.get("release_binding")
    expected_source = _mapping(lock.get("source_fingerprint")).get("value")
    expected_image = lock.get("image_digest")
    expected_binding = lock.get("binding_sha256")
    drill_ids: set[object] = set()
    tenant_hashes: set[object] = set()
    canaries: set[object] = set()
    run_ids: set[object] = set()
    contexts: set[str] = set()
    namespaces: set[str] = set()
    clusters: set[str] = set()
    namespace_uids: set[str] = set()
    job_uids: set[str] = set()

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
            reasons.append(f"{prefix} functional restore Job did not pass")
        if item.get("mode") != MODE:
            reasons.append(f"{prefix} functional restore mode is missing or invalid")
        if item.get("release_binding") != expected_release:
            reasons.append(f"{prefix} release binding does not match candidate lock")
        if _mapping(item.get("source_fingerprint")).get("value") != expected_source:
            reasons.append(f"{prefix} source fingerprint does not match candidate lock")
        if item.get("image_digest") != expected_image:
            reasons.append(f"{prefix} image digest does not match candidate lock")
        if item.get("isolated_restore_target") is not True:
            reasons.append(f"{prefix} restore target is not isolated")
        if item.get("production_system_mutated") is not False:
            reasons.append(f"{prefix} production mutation safety assertion is missing")
        if _parse_timestamp(item.get("generated_at"), now=current) is None:
            reasons.append(f"{prefix} observation timestamp is invalid or stale")

        run_id = item.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            reasons.append(f"{prefix} run_id is missing")
        else:
            run_ids.add(run_id)
        drill_id = item.get("drill_id")
        tenant_id_hash = item.get("tenant_id_hash")
        if isinstance(drill_id, str) and drill_id:
            drill_ids.add(drill_id)
        else:
            reasons.append(f"{prefix} drill_id is missing or invalid")
        if isinstance(tenant_id_hash, str):
            tenant_hashes.add(tenant_id_hash)
        if _sha256(item.get("tenant_id_hash")) is None:
            reasons.append(f"{prefix} tenant_id_hash is missing or invalid")
        canary = item.get("canary_sha256")
        if _sha256(canary) is None or item.get("restored_canary_sha256") != canary:
            reasons.append(f"{prefix} restored canary checksum does not match")
        else:
            canaries.add(canary)
        rto = _positive_number(item.get("rto_seconds"))
        if rto is None or rto > max_rto_seconds:
            reasons.append(f"{prefix} functional restore exceeds the RTO objective")
        if _positive_number(item.get("rpo_seconds")) is None:
            reasons.append(f"{prefix} recovery-point age is missing or invalid")
        for check in _component_checks(component):
            if item.get(check) is not True:
                reasons.append(f"{prefix} required functional check {check} is not true")

        backup = _mapping(item.get("backup"))
        if backup.get("storage_tier") != "ephemeral_same_cluster":
            reasons.append(f"{prefix} storage tier is not the bounded functional tier")
        if backup.get("disaster_redundant") is not False:
            reasons.append(f"{prefix} functional evidence incorrectly claims redundancy")
        if backup.get("replication_verified") is not False:
            reasons.append(f"{prefix} functional evidence incorrectly claims replication")
        for field in ("backup_id_sha256", "restore_id_sha256"):
            if _sha256(backup.get(field)) is None:
                reasons.append(f"{prefix} backup {field} is missing or invalid")
        if _parse_timestamp(backup.get("created_at"), now=current) is None:
            reasons.append(f"{prefix} backup timestamp is invalid or stale")
        if _parse_timestamp(backup.get("restore_started_at"), now=current) is None:
            reasons.append(f"{prefix} restore timestamp is invalid or stale")
        if _parse_timestamp(backup.get("restore_completed_at"), now=current) is None:
            reasons.append(f"{prefix} restore completion timestamp is invalid or stale")
        backup_created = _parse_timestamp(backup.get("created_at"), now=current)
        restore_started = _parse_timestamp(backup.get("restore_started_at"), now=current)
        if backup_created is not None and restore_started is not None:
            if restore_started < backup_created:
                reasons.append(f"{prefix} restore started before the backup was created")
            elif component == "postgres_pitr" and backup.get("backup_id_sha256") != backup.get(
                "restore_id_sha256"
            ):
                reasons.append(f"{prefix} logical restore checksum identity changed")
        if component == "postgres_pitr":
            if (
                backup.get("backend") not in {"postgres", "postgresql"}
                or backup.get("restore_mode") != "logical_snapshot"
            ):
                reasons.append(f"{prefix} logical PostgreSQL restore evidence is missing")
            if (
                item.get("point_in_time_recovery") is not False
                or backup.get("pitr_enabled") is not False
            ):
                reasons.append(f"{prefix} functional evidence incorrectly claims WAL PITR")
        elif component == "artifact_restore":
            if backup.get("backend") != "minio" or backup.get("versioning_enabled") is not True:
                reasons.append(f"{prefix} MinIO object-version evidence is missing")
        elif (
            backup.get("backend") != "minio"
            or backup.get("restore_mode") != "synthetic_key_version"
            or backup.get("key_versioned") is not True
        ):
            reasons.append(f"{prefix} synthetic key-version evidence is missing")

        validation = _mapping(item.get("validation"))
        if validation.get("source") != "restore_job_output" or validation.get("status") != "pass":
            reasons.append(f"{prefix} validation did not come from Job output")
        if validation.get("production_data_touched") is not False:
            reasons.append(f"{prefix} validation did not prove production isolation")
        if validation.get("synthetic_data_only") is not True:
            reasons.append(f"{prefix} validation did not prove synthetic-only data")

        execution_reasons, identity = _validate_execution(
            item,
            component=component,
            expected_image=expected_image,
            expected_lock_binding=expected_binding,
            current=current,
        )
        reasons.extend(execution_reasons)
        if not _exact_image_id(_mapping(item.get("execution")).get("image_id"), expected_image):
            reasons.append(
                f"{prefix} observed restore Job image ID is not an exact candidate digest"
            )
        for values, key in (
            (contexts, "context_sha256"),
            (namespaces, "namespace_sha256"),
            (clusters, "cluster_uid_sha256"),
            (namespace_uids, "namespace_uid_sha256"),
            (job_uids, "job_uid_sha256"),
        ):
            value = identity[key]
            if value:
                values.add(value)

        execution = _mapping(item.get("execution"))
        completed = _parse_timestamp(execution.get("completed_at"), now=current)
        backup_created = _parse_timestamp(backup.get("created_at"), now=current)
        restore_started = _parse_timestamp(backup.get("restore_started_at"), now=current)
        restore_completed = _parse_timestamp(backup.get("restore_completed_at"), now=current)
        rpo = _positive_number(item.get("rpo_seconds"))
        rto = _positive_number(item.get("rto_seconds"))
        if backup_created is not None and restore_started is not None:
            if restore_started < backup_created:
                reasons.append(f"{prefix} restore started before the backup was created")
            elif rpo is not None:
                actual_rpo = (restore_started - backup_created).total_seconds()
                if abs(rpo - actual_rpo) > 1:
                    reasons.append(f"{prefix} RPO does not match backup timestamps")
        if restore_started is not None and restore_completed is not None:
            if restore_completed < restore_started:
                reasons.append(f"{prefix} restore completed before it started")
            elif rto is not None:
                measured_rto = (restore_completed - restore_started).total_seconds()
                if abs(rto - measured_rto) > MEASURED_RTO_TIMESTAMP_TOLERANCE_SECONDS:
                    reasons.append(f"{prefix} RTO does not match restore timestamps")
        if restore_started is not None and completed is not None:
            if (
                completed + timedelta(seconds=KUBERNETES_TIMESTAMP_TOLERANCE_SECONDS)
                < restore_started
            ):
                reasons.append(f"{prefix} Job completed before restore started")
            elif (completed - restore_started).total_seconds() > (
                max_rto_seconds + KUBERNETES_TIMESTAMP_TOLERANCE_SECONDS
            ):
                reasons.append(f"{prefix} functional Job exceeds the RTO objective")
        if restore_completed is not None and completed is not None:
            if restore_completed > completed + timedelta(
                seconds=KUBERNETES_TIMESTAMP_TOLERANCE_SECONDS
            ):
                reasons.append(f"{prefix} restore completed after Job completion")
        if completed is not None:
            generated_at = _parse_timestamp(item.get("generated_at"), now=current)
            if generated_at is not None and completed > generated_at:
                reasons.append(f"{prefix} observation was generated before Job completion")

    if len(run_ids) != len(COMPONENTS):
        reasons.append("functional recovery run_ids are missing or reused")
    shared_values: tuple[tuple[set[object], str], ...] = (
        (drill_ids, "drill_id"),
        (tenant_hashes, "tenant_id_hash"),
        (canaries, "canary checksum"),
        (set(contexts), "Kubernetes context"),
        (set(namespaces), "Kubernetes namespace"),
        (set(clusters), "Kubernetes cluster"),
        (set(namespace_uids), "Kubernetes namespace UID"),
    )
    for shared_set, label in shared_values:
        shared_value = next(iter(shared_set), None)
        if len(shared_set) != 1 or not isinstance(shared_value, str) or not shared_value:
            reasons.append(f"functional recovery components do not share one {label}")
    if len(job_uids) != len(COMPONENTS):
        reasons.append("functional recovery Job UIDs are missing or reused")
    return list(dict.fromkeys(reasons))


def build_report(
    *,
    enabled: bool,
    evidence_paths: Mapping[str, Path],
    lock_path: Path,
    binding_path: Path,
    max_rto_seconds: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not enabled:
        reason = "TRPC_DR_FUNCTIONAL_ENABLED=true was not supplied; functional Jobs were not run"
        return {
            "schema_version": 1,
            "baseline": {"required_components": list(COMPONENTS)},
            "candidate": {"mode": "not_run", "components": {}},
            "case_deltas": {"failed_components": list(COMPONENTS)},
            "gate": "not_run",
            "production_gate": "not_run",
            "rejection_reasons": [reason],
            "production_rejection_reasons": [reason, *PRODUCTION_LIMITATIONS],
        }
    try:
        lock = _read(lock_path)
        binding = _read(binding_path)
        observations = {component: _read(evidence_paths[component]) for component in COMPONENTS}
        reasons = validate_functional_drill(
            observations,
            lock=lock,
            binding=binding,
            max_rto_seconds=max_rto_seconds,
            now=now,
        )
    except (
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        UnicodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        lock = {}
        observations = {}
        # Keep parser/path details (which may contain credentials or tokens)
        # out of the machine-readable report.  The gate remains fail-closed.
        reasons = ["functional recovery evidence could not be loaded"]

    components = {
        component: {
            "status": _mapping(observations.get(component)).get("status", "not_run"),
            "run_id": _mapping(observations.get(component)).get("run_id"),
            "rpo_seconds": _mapping(observations.get(component)).get("rpo_seconds"),
            "rto_seconds": _mapping(observations.get(component)).get("rto_seconds"),
            "backend": _mapping(_mapping(observations.get(component)).get("backup")).get("backend"),
            "restore_mode": _mapping(_mapping(observations.get(component)).get("backup")).get(
                "restore_mode"
            ),
        }
        for component in COMPONENTS
    }
    failed_components = [
        component
        for component in COMPONENTS
        if any(reason.startswith(f"{component}:") for reason in reasons)
    ]
    if reasons and not failed_components:
        failed_components = list(COMPONENTS)
    image_digest = lock.get("image_digest")
    runtime_attestation: dict[str, Any] | None = None
    runtime = None
    if not reasons:
        executions = {
            component: _mapping(_mapping(observations.get(component)).get("execution"))
            for component in COMPONENTS
        }
        first_execution = executions[COMPONENTS[0]]
        worker_identities = [
            {
                "component": component,
                "image_digest": image_digest,
                "job_uid_sha256": executions[component].get("job_uid_sha256"),
                "pod_uid_sha256": executions[component].get("pod_uid_sha256"),
            }
            for component in COMPONENTS
        ]
        parameters = {
            "candidate_lock_binding_sha256": lock.get("binding_sha256"),
            "components": list(COMPONENTS),
            "image_digest": image_digest,
        }
        runtime_attestation = {
            "cluster_uid_sha256": first_execution.get("cluster_uid_sha256"),
            "namespace_uid_sha256": first_execution.get("namespace_uid_sha256"),
            "jobs": worker_identities,
        }
        runtime = runtime_fingerprint(
            mode=MODE,
            worker_identities=worker_identities,
            stream=first_execution.get("cluster_uid_sha256"),
            group=first_execution.get("namespace_uid_sha256"),
            parameters=parameters,
        )
    evidence = build_evidence(
        root=ROOT,
        producer=PRODUCER,
        run_id=new_run_id(PRODUCER),
        runtime=runtime,
    )
    lock_hash = None
    if lock_path.is_file() and not lock_path.is_symlink():
        try:
            lock_hash = hashlib.sha256(lock_path.read_bytes()).hexdigest()
        except OSError:
            lock_hash = None
    return {
        "schema_version": 1,
        "baseline": {
            "required_components": list(COMPONENTS),
            "max_rto_seconds": max_rto_seconds,
            "production_requirements_excluded": ["remote redundancy", "WAL PITR", "external KMS"],
        },
        "candidate": {
            "mode": MODE,
            "platform": "kubernetes",
            "lineage": {"image_digest": image_digest},
            "candidate_lock_sha256": lock_hash,
            "components": components,
            "runtime_attestation": runtime_attestation,
        },
        "case_deltas": {"failed_components": failed_components},
        "evidence": evidence,
        "gate": "fail" if reasons else "pass",
        "production_gate": "not_run",
        "rejection_reasons": reasons,
        "production_rejection_reasons": list(PRODUCTION_LIMITATIONS),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("runs/drill-functional"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/disaster-recovery-functional.json"),
    )
    parser.add_argument(
        "--candidate-lock", type=Path, default=Path("runs/multitenant/candidate-lock.json")
    )
    parser.add_argument(
        "--image-binding", type=Path, default=Path("runs/multitenant/registry-image-binding.json")
    )
    parser.add_argument("--max-rto-seconds", type=float, default=300)
    parser.add_argument("--require-functional", action="store_true")
    args = parser.parse_args(argv)
    enabled = os.getenv("TRPC_DR_FUNCTIONAL_ENABLED", "").strip().lower() == "true"
    report = build_report(
        enabled=enabled,
        evidence_paths={
            component: args.directory / f"{component}.json" for component in COMPONENTS
        },
        lock_path=args.candidate_lock,
        binding_path=args.image_binding,
        max_rto_seconds=args.max_rto_seconds,
    )
    rendered = atomic_write_json(args.output, report)
    print(rendered, end="")
    return 1 if args.require_functional and report["gate"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
