#!/usr/bin/env python3
"""Collect real Kubernetes disaster-recovery Job evidence.

This command is deliberately a read-only collector.  Restore Jobs are owned
by the operator/backup platform; the collector only reads their Kubernetes
status and one secret-free JSON result line from each Job.  It never treats a
local fixture, a node-local volume, or a hand-written summary as a production
restore.

The collector is inert unless ``TRPC_DR_DRILL_ENABLED=true``.  A live run also
requires an explicit kubeconfig, context, isolated namespace and one Job name
for each component.  The resulting observations are consumed by
``disaster_recovery_gate.py``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.candidate_lock import verify_candidate_lock
from scripts.disaster_recovery_gate import COMPONENTS, build_report
from scripts.evidence_lineage import canonical_sha256
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
CONTEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
IMAGE_RE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
SENSITIVE_KEY_MARKERS = frozenset(
    {"access_key", "credential", "database_url", "dsn", "password", "secret_key", "token"}
)
_COLLECTION_REQUEST_TIMEOUT_SECONDS = 30.0
_COLLECTION_RETRY_INTERVAL_SECONDS = 1.0
_COLLECTION_SETTLE_TIMEOUT_SECONDS = 30.0


class DisasterRecoveryCollectionTimeout(RuntimeError):
    """Raised when final Job evidence cannot be observed before the deadline."""

    def __init__(self) -> None:
        super().__init__("disaster-recovery evidence collection timed out")


class _EvidenceNotReady(RuntimeError):
    """Internal marker for an explicitly transient post-completion observation."""

    def __init__(self) -> None:
        super().__init__("disaster-recovery evidence is not ready")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strict_json(value: str) -> Any:
    def duplicate_key(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    return json.loads(
        value,
        object_pairs_hook=duplicate_key,
        parse_constant=lambda constant: (_ for _ in ()).throw(
            ValueError(f"non-finite JSON value: {constant}")
        ),
    )


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"candidate input is missing or a symlink: {path.name}")
    value = _strict_json(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"candidate input root is not an object: {path.name}")
    return value


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


def _hash(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("expected a non-empty string to hash")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamp is invalid") from error
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _collection_request_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining < 0.1:
        raise DisasterRecoveryCollectionTimeout
    return min(_COLLECTION_REQUEST_TIMEOUT_SECONDS, remaining)


def _kubectl(
    arguments: Sequence[str], *, kubeconfig: Path, context: str, timeout_seconds: float
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("kubectl")
    if executable is None:
        raise RuntimeError("kubectl is not installed")
    command = [executable, "--kubeconfig", str(kubeconfig), "--context", context, *arguments]
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "KUBECONFIG": str(kubeconfig),
    }
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value is not None:
            environment[name] = value
    try:
        result = subprocess.run(  # noqa: S603 - executable and argv are explicit, no shell
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("kubectl request failed") from error
    if result.returncode != 0:
        # Do not expose kubectl stderr: admission plugins and Job logs can
        # contain credentials even when the API request itself failed.
        raise RuntimeError("kubectl request failed")
    return result


def _kubectl_json(
    arguments: Sequence[str], *, kubeconfig: Path, context: str, timeout_seconds: float
) -> dict[str, Any]:
    result = _kubectl(
        arguments,
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    try:
        value = _strict_json(result.stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("kubectl returned invalid JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError("kubectl returned a non-object JSON document")
    return value


def _collection_kubectl(
    arguments: Sequence[str], *, kubeconfig: Path, context: str, deadline: float
) -> subprocess.CompletedProcess[str]:
    try:
        return _kubectl(
            arguments,
            kubeconfig=kubeconfig,
            context=context,
            timeout_seconds=_collection_request_timeout(deadline),
        )
    except DisasterRecoveryCollectionTimeout:
        raise
    except RuntimeError as error:
        raise _EvidenceNotReady from error


def _collection_kubectl_json(
    arguments: Sequence[str], *, kubeconfig: Path, context: str, deadline: float
) -> dict[str, Any]:
    try:
        return _kubectl_json(
            arguments,
            kubeconfig=kubeconfig,
            context=context,
            timeout_seconds=_collection_request_timeout(deadline),
        )
    except DisasterRecoveryCollectionTimeout:
        raise
    except RuntimeError as error:
        raise _EvidenceNotReady from error


def _job_name_args(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--job must use component=name")
        component, name = value.split("=", 1)
        if component not in COMPONENTS or component in result:
            raise ValueError("--job must name each recovery component exactly once")
        if NAMESPACE_RE.fullmatch(name) is None or len(name) > 253:
            raise ValueError("--job name is invalid")
        result[component] = name
    if set(result) != set(COMPONENTS):
        raise ValueError("--job must include postgres_pitr, artifact_restore and key_restore")
    return result


def _pod_for_job(pods: Mapping[str, Any], *, job_name: str) -> Mapping[str, Any]:
    items = pods.get("items")
    if not isinstance(items, list):
        raise RuntimeError("restore Job pod listing is invalid")
    succeeded = [
        item
        for item in items
        if isinstance(item, Mapping) and _mapping(item.get("status")).get("phase") == "Succeeded"
    ]
    if not succeeded:
        raise _EvidenceNotReady
    if len(succeeded) != 1:
        raise RuntimeError(f"restore Job {job_name} does not have exactly one succeeded Pod")
    return succeeded[0]


def _job_output(stdout: str, *, component: str) -> Mapping[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise _EvidenceNotReady
    if len(lines) != 1:
        raise RuntimeError(f"{component} restore Job did not emit one JSON result line")
    try:
        value = _strict_json(lines[0])
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{component} restore Job result is not valid JSON") from error
    if not isinstance(value, Mapping) or _contains_sensitive_key(value):
        raise RuntimeError(f"{component} restore Job result is not secret-safe")
    if value.get("schema_version") != 1 or value.get("component") != component:
        raise RuntimeError(f"{component} restore Job result contract is invalid")
    if value.get("status") != "pass":
        raise RuntimeError(f"{component} restore Job did not pass")
    return value


def _collect_component_once(
    component: str,
    *,
    job_name: str,
    namespace: str,
    context: str,
    kubeconfig: Path,
    deadline: float,
    lock: Mapping[str, Any],
    cluster_uid_sha256: str,
    namespace_uid_sha256: str,
) -> dict[str, Any]:
    """Collect one API-confirmed Job and its allowlisted result contract."""

    job = _collection_kubectl_json(
        ["get", "job", job_name, "--namespace", namespace, "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        deadline=deadline,
    )
    metadata = _mapping(job.get("metadata"))
    status = _mapping(job.get("status"))
    spec = _mapping(job.get("spec"))
    template = _mapping(spec.get("template"))
    pod_spec = _mapping(template.get("spec"))
    containers = pod_spec.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise RuntimeError(f"{component} restore Job container contract is invalid")
    container = _mapping(containers[0])
    image_reference = container.get("image")
    if not isinstance(image_reference, str) or IMAGE_RE.fullmatch(image_reference) is None:
        raise RuntimeError(f"{component} restore Job image is not immutable")
    job_uid = metadata.get("uid")
    if not isinstance(job_uid, str) or not job_uid:
        raise RuntimeError(f"{component} restore Job UID is missing")
    if metadata.get("namespace") not in {None, namespace}:
        raise RuntimeError(
            f"{component} restore Job namespace does not match the isolated namespace"
        )
    conditions = status.get("conditions")
    condition_items = conditions if isinstance(conditions, list) else []
    succeeded = status.get("succeeded", 0)
    failed = status.get("failed", 0)
    active = status.get("active", 0)
    terminal_failure = failed not in (0, None) or any(
        _mapping(condition).get("type") == "Failed" and _mapping(condition).get("status") == "True"
        for condition in condition_items
    )
    if terminal_failure:
        raise RuntimeError(f"{component} restore Job did not complete successfully")
    complete = any(
        _mapping(condition).get("type") == "Complete"
        and _mapping(condition).get("status") == "True"
        for condition in condition_items
    )
    if succeeded != 1 or active != 0 or not complete:
        raise _EvidenceNotReady
    if status.get("startTime") is None or status.get("completionTime") is None:
        raise _EvidenceNotReady
    started_at = _timestamp(status.get("startTime"))
    completed_at = _timestamp(status.get("completionTime"))
    if completed_at < started_at:
        raise RuntimeError(f"{component} restore Job timestamps are out of order")

    pods = _collection_kubectl_json(
        [
            "get",
            "pods",
            "--namespace",
            namespace,
            "--selector",
            f"job-name={job_name}",
            "-o",
            "json",
        ],
        kubeconfig=kubeconfig,
        context=context,
        deadline=deadline,
    )
    pod = _pod_for_job(pods, job_name=job_name)
    pod_metadata = _mapping(pod.get("metadata"))
    pod_uid = pod_metadata.get("uid")
    if not isinstance(pod_uid, str) or not pod_uid:
        raise RuntimeError(f"{component} restore Pod UID is missing")
    container_statuses = _mapping(pod.get("status")).get("containerStatuses")
    if container_statuses is None or container_statuses == []:
        raise _EvidenceNotReady
    if not isinstance(container_statuses, list) or len(container_statuses) != 1:
        raise RuntimeError(f"{component} restore Pod container status is invalid")
    container_status = _mapping(container_statuses[0])
    image_id = container_status.get("imageID")
    terminated = _mapping(_mapping(container_status.get("state")).get("terminated"))
    if not isinstance(image_id, str) or not image_id:
        raise _EvidenceNotReady
    if image_reference.rsplit("@", 1)[1] not in image_id:
        raise RuntimeError(f"{component} restore Pod image or exit status is invalid")
    if not terminated:
        raise _EvidenceNotReady
    exit_code = terminated.get("exitCode")
    if exit_code is None:
        raise _EvidenceNotReady
    if exit_code != 0:
        raise RuntimeError(f"{component} restore Pod image or exit status is invalid")

    log_result = _collection_kubectl(
        [
            "logs",
            f"job/{job_name}",
            "--namespace",
            namespace,
            "--all-containers=true",
        ],
        kubeconfig=kubeconfig,
        context=context,
        deadline=deadline,
    )
    output = _job_output(log_result.stdout, component=component)
    backup = _mapping(output.get("backup"))
    validation = _mapping(output.get("validation"))
    required_hashes = ("tenant_id_hash", "canary_sha256", "restored_canary_sha256")
    if any(
        not isinstance(output.get(key), str) or HASH_RE.fullmatch(output[key]) is None
        for key in required_hashes
    ):
        raise RuntimeError(f"{component} restore Job result checksums are invalid")
    if output.get("restored_canary_sha256") != output.get("canary_sha256"):
        raise RuntimeError(f"{component} restore Job canary checksum does not match")
    if validation.get("source") != "restore_job_output" or validation.get("status") != "pass":
        raise RuntimeError(f"{component} restore Job validation contract is invalid")
    if validation.get("production_data_touched") is not False:
        raise RuntimeError(f"{component} restore Job did not prove production isolation")
    binding_hash = lock.get("binding_sha256")
    if not isinstance(binding_hash, str) or HASH_RE.fullmatch(binding_hash) is None:
        raise RuntimeError("candidate lock binding hash is invalid")
    generated_at = max(datetime.now(UTC), completed_at)
    return {
        "schema_version": 1,
        "kind": "disaster_recovery_observation",
        "component": component,
        "status": "pass",
        "mode": output.get("mode"),
        "generated_at": _utc(generated_at),
        "release_binding": lock.get("release_binding"),
        "source_fingerprint": lock.get("source_fingerprint"),
        "image_digest": lock.get("image_digest"),
        "drill_id": output.get("drill_id"),
        "run_id": output.get("run_id"),
        "tenant_id_hash": output.get("tenant_id_hash"),
        "canary_sha256": output.get("canary_sha256"),
        "restored_canary_sha256": output.get("restored_canary_sha256"),
        "isolated_restore_target": True,
        "production_system_mutated": False,
        "rpo_seconds": output.get("rpo_seconds"),
        "rto_seconds": output.get("rto_seconds"),
        "point_in_time_recovery": output.get("point_in_time_recovery", False),
        "backup_integrity_verified": output.get("backup_integrity_verified", False),
        "versioned_restore": output.get("versioned_restore", False),
        "checksum_verified": output.get("checksum_verified", False),
        "key_version_restored": output.get("key_version_restored", False),
        "decrypt_verified": output.get("decrypt_verified", False),
        "execution": {
            "kind": "kubernetes_job",
            "source": "kubectl_api",
            "status": "succeeded",
            "completion_confirmed": True,
            "isolated_namespace": True,
            "production_mutation_checked": True,
            "succeeded": 1,
            "failed": 0,
            "active": 0,
            "context": context,
            "context_sha256": _hash(context),
            "namespace": namespace,
            "namespace_sha256": _hash(namespace),
            "cluster_uid_sha256": cluster_uid_sha256,
            "namespace_uid_sha256": namespace_uid_sha256,
            "job_uid_sha256": _hash(job_uid),
            "pod_uid_sha256": _hash(pod_uid),
            "job_name": job_name,
            "image_reference": image_reference,
            "image_id": image_id,
            "candidate_lock_binding_sha256": binding_hash,
            "started_at": _utc(started_at),
            "completed_at": _utc(completed_at),
        },
        "backup": dict(backup),
        "validation": dict(validation),
    }


def _collect_component_until_ready(
    component: str,
    *,
    job_name: str,
    namespace: str,
    context: str,
    kubeconfig: Path,
    deadline: float,
    lock: Mapping[str, Any],
    cluster_uid_sha256: str,
    namespace_uid_sha256: str,
) -> dict[str, Any]:
    while True:
        try:
            return _collect_component_once(
                component,
                job_name=job_name,
                namespace=namespace,
                context=context,
                kubeconfig=kubeconfig,
                deadline=deadline,
                lock=lock,
                cluster_uid_sha256=cluster_uid_sha256,
                namespace_uid_sha256=namespace_uid_sha256,
            )
        except _EvidenceNotReady:
            remaining = deadline - time.monotonic()
            if remaining < 0.1:
                raise DisasterRecoveryCollectionTimeout from None
            time.sleep(min(_COLLECTION_RETRY_INTERVAL_SECONDS, remaining))


def collect_component(
    component: str,
    *,
    job_name: str,
    namespace: str,
    context: str,
    kubeconfig: Path,
    timeout_seconds: float,
    lock: Mapping[str, Any],
    cluster_uid_sha256: str,
    namespace_uid_sha256: str,
) -> dict[str, Any]:
    """Collect one component with bounded retries for transient observations."""

    deadline = time.monotonic() + min(max(timeout_seconds, 0.0), _COLLECTION_SETTLE_TIMEOUT_SECONDS)
    return _collect_component_until_ready(
        component,
        job_name=job_name,
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        deadline=deadline,
        lock=lock,
        cluster_uid_sha256=cluster_uid_sha256,
        namespace_uid_sha256=namespace_uid_sha256,
    )


def collect_drill(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    jobs: Mapping[str, str],
    lock: Mapping[str, Any],
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    """Collect all three components from one real Kubernetes namespace."""

    if NAMESPACE_RE.fullmatch(namespace) is None or len(namespace) > 253:
        raise ValueError("disaster-recovery namespace is invalid")
    if namespace in {"default", "kube-system", "trpc-service"}:
        raise ValueError("disaster-recovery namespace must be isolated from production")
    if CONTEXT_RE.fullmatch(context) is None:
        raise ValueError("disaster-recovery Kubernetes context is invalid")
    if kubeconfig.is_symlink() or not kubeconfig.is_file():
        raise ValueError("disaster-recovery kubeconfig is missing or a symlink")
    deadline = time.monotonic() + min(max(timeout_seconds, 0.0), _COLLECTION_SETTLE_TIMEOUT_SECONDS)
    namespace_object = _kubectl_json(
        ["get", "namespace", namespace, "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=_collection_request_timeout(deadline),
    )
    namespace_uid = _mapping(namespace_object.get("metadata")).get("uid")
    if not isinstance(namespace_uid, str) or not namespace_uid:
        raise RuntimeError("isolated namespace UID is missing")
    version = _kubectl_json(
        ["version", "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=_collection_request_timeout(deadline),
    )
    server = _mapping(version.get("serverVersion"))
    server_material = {
        "gitVersion": server.get("gitVersion"),
        "major": server.get("major"),
        "minor": server.get("minor"),
        "namespace_uid": namespace_uid,
    }
    if not server_material["gitVersion"]:
        raise RuntimeError("Kubernetes API server identity is missing")
    cluster_uid_sha256 = canonical_sha256(server_material)
    observations: dict[str, dict[str, Any]] = {}
    pending = list(COMPONENTS)
    while pending:
        for component in tuple(pending):
            try:
                observation = _collect_component_once(
                    component,
                    job_name=jobs[component],
                    namespace=namespace,
                    context=context,
                    kubeconfig=kubeconfig,
                    deadline=deadline,
                    lock=lock,
                    cluster_uid_sha256=cluster_uid_sha256,
                    namespace_uid_sha256=_hash(namespace_uid),
                )
            except _EvidenceNotReady:
                continue
            observations[component] = observation
            pending.remove(component)
        if pending:
            remaining = deadline - time.monotonic()
            if remaining < 0.1:
                raise DisasterRecoveryCollectionTimeout
            time.sleep(min(_COLLECTION_RETRY_INTERVAL_SECONDS, remaining))
    return observations


def _not_run_report(args: argparse.Namespace) -> dict[str, Any]:
    return build_report(
        enabled=False,
        evidence_paths={},
        lock_path=args.candidate_lock,
        binding_path=args.image_binding,
        max_rpo_seconds=args.max_rpo_seconds,
        max_rto_seconds=args.max_rto_seconds,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=os.getenv("TRPC_DR_NAMESPACE", ""))
    parser.add_argument("--context", default=os.getenv("TRPC_DR_KUBE_CONTEXT", ""))
    parser.add_argument(
        "--kubeconfig", type=Path, default=Path(os.getenv("TRPC_DR_KUBECONFIG", ""))
    )
    parser.add_argument("--job", action="append", default=[])
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
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args(argv)
    enabled = os.getenv("TRPC_DR_DRILL_ENABLED", "").strip().lower() == "true"
    if not enabled:
        report = _not_run_report(args)
        rendered = atomic_write_json(args.output, report)
        print(rendered, end="")
        return 1 if args.require_production else 0

    collection_succeeded = False
    try:
        if not args.namespace or not args.context or not str(args.kubeconfig):
            raise ValueError(
                "TRPC_DR_NAMESPACE, TRPC_DR_KUBE_CONTEXT and TRPC_DR_KUBECONFIG are required"
            )
        jobs = _job_name_args(args.job)
        lock = _read_json(args.candidate_lock)
        binding = _read_json(args.image_binding)
        lock_reasons = verify_candidate_lock(lock, binding, root=ROOT)
        if lock_reasons:
            raise ValueError("candidate lock is invalid")
        if not 0 < args.timeout_seconds <= 3600:
            raise ValueError("--timeout-seconds must be within 0..3600")
        observations = collect_drill(
            namespace=args.namespace,
            context=args.context,
            kubeconfig=args.kubeconfig,
            jobs=jobs,
            lock=lock,
            timeout_seconds=args.timeout_seconds,
        )
        args.directory.mkdir(parents=True, exist_ok=True)
        for component, observation in observations.items():
            atomic_write_json(args.directory / f"{component}.json", observation)
        collection_succeeded = True
    except (OSError, UnicodeError, ValueError, RuntimeError, json.JSONDecodeError):
        # Let the validator produce a structured fail-closed report from the
        # missing/incomplete evidence.  No exception text or kubectl stderr is
        # copied into the report because either can contain secret material.
        pass

    evidence_directory = args.directory
    if not collection_succeeded:
        # Never allow a previous run's files in the conventional directory to
        # become evidence for a failed collection.  A fresh, non-existent
        # path makes the validator fail closed without deleting user data.
        evidence_directory = args.directory / f".failed-collection-{uuid.uuid4().hex}"
    evidence_paths = {
        component: evidence_directory / f"{component}.json" for component in COMPONENTS
    }
    report = build_report(
        enabled=True,
        evidence_paths=evidence_paths,
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
