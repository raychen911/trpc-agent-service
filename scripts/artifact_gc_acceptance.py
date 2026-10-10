#!/usr/bin/env python3
"""Run an isolated PostgreSQL/S3 artifact garbage-collection acceptance.

The command is intentionally inert until ``TRPC_RUN_REAL_ARTIFACT_GC=1`` is
set.  The live path creates one unique tenant and staged object, ages only the
matching metadata row past the configured TTL, and then waits for either the
real Kubernetes ``trpc-artifact-gc`` Deployment or an explicitly local,
non-production collector.  It never uses the local collector when a
Kubernetes namespace is supplied, so a local test cannot be reported as a
cluster exercise.

Reports are machine-readable first and contain only hashes for fixture
identifiers.  Database and object-store credentials are never copied to the
report or command output.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import asyncpg
import boto3
from botocore.config import Config

# Keep ``python scripts/artifact_gc_acceptance.py`` equivalent to module
# invocation when the repository root is not already on sys.path.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import build_evidence, runtime_fingerprint, source_fingerprint
from scripts.report_io import atomic_write_json
from trpc_service.storage.artifact_gc import ArtifactGarbageCollector
from trpc_service.storage.artifacts import S3ArtifactStore

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.artifact_gc_acceptance"
DEFAULT_OUTPUT = ROOT / "runs" / "multitenant" / "artifact-gc-acceptance.json"
DEFAULT_TTL_SECONDS = 86_400
DEFAULT_TIMEOUT_SECONDS = 180.0
DEFAULT_POLL_SECONDS = 2.0
OPT_IN_ENV = "TRPC_RUN_REAL_ARTIFACT_GC"
GC_DEPLOYMENT = "trpc-artifact-gc"
GC_SELECTOR = "app.kubernetes.io/name=trpc-artifact-gc"
IMAGE_DIGEST_RE = re.compile(r"sha256:[0-9a-fA-F]{64}")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
CONTEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")


class AcceptanceUnavailable(RuntimeError):
    """A live prerequisite was not available; no production claim is valid."""


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _hash(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _utc() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"candidate lock is missing or a symlink: {path.name}")

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
        raise ValueError(f"candidate lock is not an object: {path.name}")
    return value


def _env_value(name: str, *aliases: str) -> str:
    for candidate in (name, *aliases):
        value = os.getenv(candidate, "").strip()
        if value:
            return value
    return ""


def _live_environment() -> tuple[dict[str, str], list[str]]:
    values = {
        "database_dsn": _env_value(
            "TRPC_REAL_DATABASE_DSN",
            "TRPC_REAL_ARTIFACT_GC_DATABASE_DSN",
            "TRPC_SERVICE_DATABASE_DSN",
        ),
        "s3_endpoint": _env_value(
            "TRPC_REAL_S3_ENDPOINT", "TRPC_REAL_ARTIFACT_GC_S3_ENDPOINT", "TRPC_SERVICE_S3_ENDPOINT"
        ),
        "s3_access_key": _env_value(
            "TRPC_REAL_S3_ACCESS_KEY",
            "TRPC_REAL_ARTIFACT_GC_S3_ACCESS_KEY",
            "TRPC_SERVICE_S3_ACCESS_KEY",
        ),
        "s3_secret_key": _env_value(
            "TRPC_REAL_S3_SECRET_KEY",
            "TRPC_REAL_ARTIFACT_GC_S3_SECRET_KEY",
            "TRPC_SERVICE_S3_SECRET_KEY",
        ),
        "s3_bucket": _env_value(
            "TRPC_REAL_S3_BUCKET", "TRPC_REAL_ARTIFACT_GC_S3_BUCKET", "TRPC_SERVICE_S3_BUCKET"
        ),
    }
    missing = [
        name
        for name, value in (
            ("TRPC_REAL_DATABASE_DSN", values["database_dsn"]),
            ("TRPC_REAL_S3_ENDPOINT", values["s3_endpoint"]),
            ("TRPC_REAL_S3_ACCESS_KEY", values["s3_access_key"]),
            ("TRPC_REAL_S3_SECRET_KEY", values["s3_secret_key"]),
            ("TRPC_REAL_S3_BUCKET", values["s3_bucket"]),
        )
        if not value
    ]
    return values, missing


def _database_dsn(value: str) -> str:
    if value.startswith("postgresql+asyncpg://"):
        return "postgresql://" + value.removeprefix("postgresql+asyncpg://")
    if not value.startswith(("postgresql://", "postgres://")):
        raise ValueError("artifact GC database DSN must use PostgreSQL")
    return value


def _validate_ttl(value: int) -> int:
    if value < 60 or value > 31_536_000:
        raise ValueError("artifact GC ttl must be between 60 and 31536000 seconds")
    return value


def _validate_timeout(value: float) -> float:
    if not isinstance(value, (int, float)) or value < 1 or value > 3_600:
        raise ValueError("artifact GC timeout must be between 1 and 3600 seconds")
    return float(value)


def _validate_poll(value: float) -> float:
    if not isinstance(value, (int, float)) or value <= 0 or value > 60:
        raise ValueError("artifact GC poll interval must be greater than 0 and at most 60 seconds")
    return float(value)


def _validate_namespace(value: str) -> str:
    if not value or len(value) > 253 or NAMESPACE_RE.fullmatch(value) is None:
        raise ValueError("artifact GC Kubernetes namespace is invalid")
    if value in {"default", "kube-system", "trpc-service"}:
        raise ValueError("artifact GC namespace must be isolated from production")
    return value


def _validate_context(value: str) -> str:
    if CONTEXT_RE.fullmatch(value) is None:
        raise ValueError("artifact GC Kubernetes context is invalid")
    return value


def _lock_identity(lock: Mapping[str, Any]) -> tuple[str | None, str | None, list[str]]:
    reasons: list[str] = []
    image = lock.get("image_digest")
    if not isinstance(image, str) or IMAGE_DIGEST_RE.fullmatch(image) is None:
        reasons.append("candidate lock immutable image digest is missing or invalid")
        image = None
    source = _mapping(lock.get("source_fingerprint")).get("value")
    if not isinstance(source, str) or SHA256_RE.fullmatch(source) is None:
        reasons.append("candidate lock source fingerprint is missing or invalid")
        source = None
    images = _mapping(lock.get("images"))
    initial = _mapping(images.get("initial"))
    reference = initial.get("reference")
    if (
        image is None
        or not isinstance(reference, str)
        or "@" not in reference
        or reference.rsplit("@", 1)[-1] != image
    ):
        reasons.append("candidate lock initial image reference is not bound to its digest")
    if initial.get("digest") != image:
        reasons.append("candidate lock initial image digest is inconsistent")
    return image, source, reasons


def _pod_source_fingerprint(pod: Mapping[str, Any]) -> str | None:
    metadata = _mapping(pod.get("metadata"))
    for values in (metadata.get("labels"), metadata.get("annotations")):
        labels = _mapping(values)
        for key in (
            "io.trpc.agent-service.source-fingerprint",
            "org.opencontainers.image.revision",
            "trpc.io/source-fingerprint",
        ):
            value = labels.get(key)
            if isinstance(value, str) and SHA256_RE.fullmatch(value) is not None:
                return value
    return None


def _image_digest(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    match = IMAGE_DIGEST_RE.search(value)
    return match.group(0).lower() if match is not None else None


def validate_gc_pod(
    pod: Mapping[str, Any], *, expected_image: str | None, expected_source: str | None
) -> tuple[dict[str, Any], list[str]]:
    """Validate the API-observed artifact-gc Pod without retaining credentials."""

    reasons: list[str] = []
    metadata = _mapping(pod.get("metadata"))
    labels = _mapping(metadata.get("labels"))
    if labels.get("app.kubernetes.io/name") != GC_DEPLOYMENT:
        reasons.append("artifact GC Pod label does not identify trpc-artifact-gc")
    phase = _mapping(pod.get("status")).get("phase")
    if phase != "Running":
        reasons.append("artifact GC Pod is not running")
    statuses = _mapping(pod.get("status")).get("containerStatuses")
    if not isinstance(statuses, list) or not statuses:
        reasons.append("artifact GC Pod container status is missing")
        statuses = []
    status = next(
        (
            _mapping(item)
            for item in statuses
            if _mapping(item).get("name") in {"artifact-gc", "trpc-artifact-gc"}
        ),
        _mapping(statuses[0]) if statuses else {},
    )
    observed_image = _image_digest(status.get("imageID"))
    if expected_image is None:
        reasons.append("candidate lock immutable image identity is unavailable")
    elif observed_image != expected_image.lower():
        reasons.append("artifact GC Pod image digest does not match candidate lock")
    observed_source = _pod_source_fingerprint(pod)
    if (
        observed_source is not None
        and expected_source is not None
        and observed_source != expected_source
    ):
        reasons.append("artifact GC Pod source fingerprint does not match candidate lock")
    if not metadata.get("uid"):
        reasons.append("artifact GC Pod UID is missing")
    observation = {
        "kind": "kubernetes_pod",
        "source": "kubectl_api",
        "status": "pass" if not reasons else "fail",
        "phase": phase if isinstance(phase, str) else "unknown",
        "pod_name_sha256": _hash(metadata.get("name")) if metadata.get("name") else None,
        "pod_uid_sha256": _hash(metadata.get("uid")) if metadata.get("uid") else None,
        "image_digest": observed_image,
        "source_fingerprint": observed_source or expected_source,
        "source_attestation": "pod_label" if observed_source is not None else "candidate_lock",
    }
    return observation, reasons


def _kubectl(
    arguments: Sequence[str], *, kubeconfig: Path, context: str, timeout_seconds: float
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("kubectl")
    if executable is None:
        raise AcceptanceUnavailable("kubectl is not installed")
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "KUBECONFIG": str(kubeconfig),
    }
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value is not None:
            environment[name] = value
    command = [executable, "--kubeconfig", str(kubeconfig), "--context", context, *arguments]
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
        raise AcceptanceUnavailable("kubectl request failed") from error
    if result.returncode != 0:
        raise AcceptanceUnavailable("kubectl request failed")
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
        value = json.loads(result.stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise AcceptanceUnavailable("kubectl returned invalid JSON") from error
    if not isinstance(value, dict):
        raise AcceptanceUnavailable("kubectl returned a non-object JSON document")
    return value


def _gc_pod(
    *, namespace: str, kubeconfig: Path, context: str, timeout_seconds: float
) -> Mapping[str, Any]:
    payload = _kubectl_json(
        ["get", "pods", "--namespace", namespace, "--selector", GC_SELECTOR, "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    items = payload.get("items")
    if not isinstance(items, list):
        raise AcceptanceUnavailable("artifact GC Pod listing is invalid")
    pods = [item for item in items if isinstance(item, Mapping)]
    if not pods:
        raise AcceptanceUnavailable("artifact GC Pod is not present")
    running = [item for item in pods if _mapping(item.get("status")).get("phase") == "Running"]
    return running[0] if running else pods[0]


async def _s3_exists(client: Any, *, bucket: str, key: str) -> bool:
    def head() -> bool:
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as error:
            response = getattr(error, "response", None)
            metadata = _mapping(response).get("ResponseMetadata")
            code = _mapping(metadata).get("HTTPStatusCode")
            error_code = _mapping(_mapping(response).get("Error")).get("Code")
            if code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    return await asyncio.to_thread(head)


async def _s3_delete(client: Any, *, bucket: str, key: str) -> None:
    def delete() -> None:
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception as error:
            response = getattr(error, "response", None)
            metadata = _mapping(response).get("ResponseMetadata")
            code = _mapping(metadata).get("HTTPStatusCode")
            error_code = _mapping(_mapping(response).get("Error")).get("Code")
            if code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}:
                return
            raise

    await asyncio.to_thread(delete)


async def _cleanup_fixture(
    pool: Any,
    client: Any,
    *,
    bucket: str,
    tenant_id: str | None,
    artifact_id: str | None,
    object_key: str | None,
) -> list[str]:
    """Delete only the unique fixture row/object; return safe cleanup errors."""

    reasons: list[str] = []
    if object_key:
        try:
            await _s3_delete(client, bucket=bucket, key=object_key)
        except Exception as error:
            reasons.append(f"fixture object cleanup failed: {type(error).__name__}")
    if tenant_id:
        try:
            async with pool.acquire() as connection:
                if artifact_id:
                    await connection.execute(
                        "DELETE FROM artifacts WHERE tenant_id=$1 AND artifact_id=$2",
                        tenant_id,
                        artifact_id,
                    )
                await connection.execute("DELETE FROM tenants WHERE tenant_id=$1", tenant_id)
        except Exception as error:
            reasons.append(f"fixture database cleanup failed: {type(error).__name__}")
    return reasons


async def _status(pool: Any, *, tenant_id: str, artifact_id: str) -> str | None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT status FROM artifacts WHERE tenant_id=$1 AND artifact_id=$2",
            tenant_id,
            artifact_id,
        )
    if row is None:
        return None
    value = row.get("status") if hasattr(row, "get") else row["status"]
    return str(value) if value is not None else None


async def _wait_for_deletion(
    pool: Any,
    client: Any,
    *,
    bucket: str,
    tenant_id: str,
    artifact_id: str,
    object_key: str,
    timeout_seconds: float,
    poll_seconds: float,
    namespace: str | None,
    kubeconfig: Path | None,
    context: str | None,
    expected_image: str | None,
    expected_source: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None, list[str]]:
    deadline = time.monotonic() + timeout_seconds
    pod_observation: dict[str, Any] | None = None
    observations = 0
    last_status: str | None = None
    while True:
        observations += 1
        if namespace is not None:
            if kubeconfig is None or context is None:
                raise AcceptanceUnavailable("Kubernetes kubeconfig and context are required")
            pod = _gc_pod(
                namespace=namespace,
                kubeconfig=kubeconfig,
                context=context,
                timeout_seconds=min(30.0, timeout_seconds),
            )
            pod_observation, pod_reasons = validate_gc_pod(
                pod,
                expected_image=expected_image,
                expected_source=expected_source,
            )
            if pod_reasons:
                transient = {
                    "artifact GC Pod is not running",
                    "artifact GC Pod container status is missing",
                }
                if set(pod_reasons).issubset(transient):
                    pod_reasons = []
                else:
                    return (
                        {
                            "status": "fail",
                            "observations": observations,
                            "last_status": last_status,
                            "pod": pod_observation,
                        },
                        pod_observation,
                        pod_reasons,
                    )
        last_status = await _status(pool, tenant_id=tenant_id, artifact_id=artifact_id)
        present = await _s3_exists(client, bucket=bucket, key=object_key)
        if last_status == "deleted" and not present:
            return (
                {
                    "status": "pass",
                    "observations": observations,
                    "last_status": last_status,
                    "s3_present": present,
                    "pod": pod_observation,
                },
                pod_observation,
                [],
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return (
                {
                    "status": "fail",
                    "observations": observations,
                    "last_status": last_status,
                    "s3_present": present,
                    "pod": pod_observation,
                },
                pod_observation,
                ["artifact GC timed out before the metadata row and S3 object were deleted"],
            )
        await asyncio.sleep(min(poll_seconds, remaining))


def _not_run_report(*, args: argparse.Namespace, reason: str) -> dict[str, Any]:
    evidence = build_evidence(root=ROOT, producer=PRODUCER)
    return {
        "schema_version": 1,
        "baseline": {
            "ttl_seconds": args.ttl_seconds,
            "requires_real_postgres_and_s3": True,
            "kubernetes_namespace_required_for_production": True,
            "local_collector_is_not_production_evidence": True,
        },
        "candidate": {"mode": "not_run", "executor": "none"},
        "case_deltas": {
            "fixture": "not_run",
            "database_status_deleted": "not_run",
            "s3_object_absent": "not_run",
            "cleanup": "not_run",
        },
        "gate": "not_run",
        "production_gate": "not_run",
        "rejection_reasons": [reason],
        "production_rejection_reasons": [reason],
        "run_id": evidence["run_id"],
        "evidence": evidence,
    }


def _production_status(
    *,
    gate: str,
    namespace: str | None,
    identity_available: bool,
    identity_rejected: bool,
) -> tuple[str, list[str]]:
    if namespace is None:
        return "not_run", [
            "Kubernetes namespace was not supplied; local GC is not production evidence"
        ]
    if identity_rejected:
        return "fail", ["Kubernetes artifact GC Pod identity did not match candidate lock"]
    if not identity_available:
        return "not_run", ["immutable candidate image/source identity was not available"]
    if gate != "pass":
        return "fail", ["Kubernetes artifact GC acceptance did not pass"]
    return "pass", []


async def run_acceptance(args: argparse.Namespace) -> dict[str, Any]:
    """Run the live acceptance after opt-in and input validation."""

    values, missing = _live_environment()
    if missing:
        return _not_run_report(args=args, reason="missing live prerequisite: " + ", ".join(missing))
    namespace = args.namespace or None
    kubeconfig = args.kubeconfig if namespace else None
    context = args.context if namespace else None
    try:
        if namespace is not None:
            _validate_namespace(namespace)
            if kubeconfig is None or kubeconfig.is_symlink() or not kubeconfig.is_file():
                raise AcceptanceUnavailable("Kubernetes kubeconfig is missing or a symlink")
            if context is None:
                raise AcceptanceUnavailable("Kubernetes context is required with namespace")
            _validate_context(context)
        ttl_seconds = _validate_ttl(args.ttl_seconds)
        timeout_seconds = _validate_timeout(args.timeout_seconds)
        poll_seconds = _validate_poll(args.poll_seconds)
        lock: dict[str, Any] = {}
        lock_reasons: list[str] = []
        expected_image: str | None = None
        expected_source: str | None = None
        candidate_identity_valid = False
        if args.candidate_lock.is_file() and not args.candidate_lock.is_symlink():
            lock = _read_json(args.candidate_lock)
            expected_image, expected_source, lock_reasons = _lock_identity(lock)
            current_source = source_fingerprint(ROOT)
            if expected_source is not None and (
                current_source.get("status") != "available"
                or current_source.get("value") != expected_source
            ):
                lock_reasons.append(
                    "candidate lock source fingerprint does not match this checkout"
                )
            candidate_identity_valid = (
                expected_image is not None and expected_source is not None and not lock_reasons
            )
        elif namespace is not None:
            lock_reasons.append("candidate lock is required for Kubernetes identity evidence")

        pool: Any | None = None
        client: Any | None = None
        tenant_id: str | None = None
        artifact_id: str | None = None
        object_key: str | None = None
        fixture_status = "fail"
        cleanup_reasons: list[str] = []
        observed_status: str | None = None
        observed_s3_present: bool | None = None
        candidate: dict[str, Any] = {
            "mode": "live_artifact_gc",
            "executor": "kubernetes_deployment" if namespace else "local_collector",
            "platform": "kubernetes" if namespace else "local",
            "candidate_lock_sha256": (
                _hash(args.candidate_lock.read_bytes())
                if args.candidate_lock.is_file() and not args.candidate_lock.is_symlink()
                else None
            ),
            "image_digest": expected_image,
            "source_fingerprint": expected_source,
        }
        try:
            pool = await asyncpg.create_pool(
                _database_dsn(values["database_dsn"]),
                min_size=1,
                max_size=2,
                command_timeout=min(timeout_seconds, 60.0),
            )
            client = boto3.client(
                "s3",
                endpoint_url=values["s3_endpoint"],
                aws_access_key_id=values["s3_access_key"],
                aws_secret_access_key=values["s3_secret_key"],
                region_name="us-east-1",
                config=Config(
                    connect_timeout=5,
                    read_timeout=15,
                    max_pool_connections=4,
                    retries={"max_attempts": 2, "mode": "standard"},
                    s3={"addressing_style": "path"},
                ),
            )
            store = S3ArtifactStore(cast(Any, client), bucket=values["s3_bucket"])
            tenant_id = f"artifact-gc-acceptance-{uuid4().hex}"
            artifact_id = f"artifact-gc-{uuid4().hex}"
            body = (f"artifact-gc-acceptance:{uuid4().hex}").encode("ascii")
            checksum = hashlib.sha256(body).hexdigest()
            object_key = await store.stage(tenant_id, artifact_id, body, checksum=checksum)
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,$2)",
                        tenant_id,
                        "artifact GC acceptance fixture",
                    )
                    await connection.execute(
                        """
                        INSERT INTO artifacts
                            (tenant_id,artifact_id,object_key,checksum,size_bytes,status,created_at)
                        VALUES ($1,$2,$3,$4,$5,'staged',clock_timestamp()
                            - ($6::double precision * interval '1 second'))
                        """,
                        tenant_id,
                        artifact_id,
                        object_key,
                        checksum,
                        len(body),
                        float(ttl_seconds + 1),
                    )
            fixture_status = "pass"
            candidate["tenant_id_sha256"] = _hash(tenant_id)
            candidate["artifact_id_sha256"] = _hash(artifact_id)
            candidate["object_key_sha256"] = _hash(object_key)
            if namespace is None:
                collector = ArtifactGarbageCollector(
                    pool,
                    store,
                    ttl_seconds=ttl_seconds,
                    batch_size=1,
                    poll_seconds=1,
                )
                result = await collector.run_once()
                candidate["collector_result"] = {
                    "scanned": result.scanned,
                    "deleted": result.deleted,
                    "failed": result.failed,
                }
                observed_status = await _status(pool, tenant_id=tenant_id, artifact_id=artifact_id)
                observed_s3_present = await _s3_exists(
                    client, bucket=values["s3_bucket"], key=object_key
                )
                candidate["database_status"] = observed_status
                candidate["s3_present"] = observed_s3_present
            else:
                assert client is not None
                assert tenant_id is not None and artifact_id is not None and object_key is not None
                wait_result, pod_observation, pod_reasons = await _wait_for_deletion(
                    pool,
                    client,
                    bucket=values["s3_bucket"],
                    tenant_id=tenant_id,
                    artifact_id=artifact_id,
                    object_key=object_key,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                    namespace=namespace,
                    kubeconfig=kubeconfig,
                    context=context,
                    expected_image=expected_image,
                    expected_source=expected_source,
                )
                candidate["kubernetes"] = wait_result
                candidate["pod"] = pod_observation
                observed_status = wait_result.get("last_status")
                present = wait_result.get("s3_present")
                observed_s3_present = present if isinstance(present, bool) else None
                if pod_reasons:
                    lock_reasons.extend(pod_reasons)
        except AcceptanceUnavailable:
            raise
        except Exception as error:
            candidate["error_type"] = type(error).__name__
            lock_reasons.append(f"live artifact GC execution failed: {type(error).__name__}")
        finally:
            if pool is not None and client is not None and tenant_id is not None:
                cleanup_reasons.extend(
                    await _cleanup_fixture(
                        pool,
                        client,
                        bucket=values["s3_bucket"],
                        tenant_id=tenant_id,
                        artifact_id=artifact_id,
                        object_key=object_key,
                    )
                )
            if pool is not None:
                try:
                    await pool.close()
                except Exception as error:
                    cleanup_reasons.append(f"database pool close failed: {type(error).__name__}")

        if fixture_status != "pass":
            gate = "fail"
            lock_reasons.append("staged fixture was not created")
        elif namespace is None:
            local_result = _mapping(candidate.get("collector_result"))
            gate = (
                "pass"
                if local_result.get("failed") == 0
                and local_result.get("deleted") == 1
                and observed_status == "deleted"
                and observed_s3_present is False
                else "fail"
            )
        else:
            k8s_result = _mapping(candidate.get("kubernetes"))
            gate = "pass" if k8s_result.get("status") == "pass" else "fail"
        if cleanup_reasons:
            gate = "fail"
            lock_reasons.extend(cleanup_reasons)
        identity_rejected = any(
            "image digest does not match candidate lock" in reason
            or "source fingerprint does not match candidate lock" in reason
            or "source fingerprint does not match this checkout" in reason
            for reason in lock_reasons
        )
        identity_available = candidate_identity_valid
        production_gate, production_reasons = _production_status(
            gate=gate,
            namespace=namespace,
            identity_available=identity_available,
            identity_rejected=identity_rejected,
        )
        runtime = None
        if expected_image is not None:
            runtime = runtime_fingerprint(
                mode="artifact_gc_kubernetes" if namespace else "artifact_gc_local",
                worker_identities=[expected_image],
                stream="artifact-gc",
                group=namespace or "local",
                parameters={"ttl_seconds": ttl_seconds, "executor": candidate["executor"]},
            )
        evidence = build_evidence(root=ROOT, producer=PRODUCER, runtime=runtime)
        return {
            "schema_version": 1,
            "baseline": {
                "ttl_seconds": ttl_seconds,
                "timeout_seconds": timeout_seconds,
                "requires_real_postgres_and_s3": True,
                "kubernetes_namespace_required_for_production": True,
                "local_collector_is_not_production_evidence": True,
            },
            "candidate": candidate,
            "case_deltas": {
                "fixture": fixture_status,
                "database_status_deleted": ("pass" if observed_status == "deleted" else "fail"),
                "s3_object_absent": (
                    "pass"
                    if observed_s3_present is False
                    else "fail"
                    if observed_s3_present is True
                    else "not_run"
                ),
                "cleanup": (
                    "fail" if cleanup_reasons else "pass" if fixture_status == "pass" else "not_run"
                ),
            },
            "gate": gate,
            "production_gate": production_gate,
            "rejection_reasons": list(dict.fromkeys(lock_reasons)),
            "production_rejection_reasons": production_reasons,
            "run_id": evidence["run_id"],
            "evidence": evidence,
        }
    except AcceptanceUnavailable as error:
        return _not_run_report(args=args, reason=str(error))
    except (OSError, ValueError, TypeError) as error:
        return _not_run_report(
            args=args, reason=f"live prerequisite rejected: {type(error).__name__}"
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="compatibility flag; live mode still requires opt-in",
    )
    parser.add_argument("--namespace", default=os.getenv("TRPC_ARTIFACT_GC_NAMESPACE", ""))
    parser.add_argument("--context", default=os.getenv("TRPC_ARTIFACT_GC_CONTEXT", ""))
    parser.add_argument(
        "--kubeconfig",
        type=Path,
        default=Path(os.getenv("TRPC_ARTIFACT_GC_KUBECONFIG", "")),
    )
    parser.add_argument(
        "--candidate-lock",
        type=Path,
        default=Path("runs/multitenant/candidate-lock.json"),
    )
    parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=int(os.getenv("TRPC_SERVICE_ARTIFACT_STAGING_TTL_SECONDS", DEFAULT_TTL_SECONDS)),
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--require-production", action="store_true")
    return parser


async def _execute(args: argparse.Namespace) -> dict[str, Any]:
    if os.getenv(OPT_IN_ENV, "").strip() != "1":
        return _not_run_report(
            args=args,
            reason=f"{OPT_IN_ENV}=1 was not supplied; real artifact GC is opt-in",
        )
    return await run_acceptance(args)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = asyncio.run(_execute(args))
    atomic_write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if args.require_production and report.get("production_gate") != "pass":
        return 1
    return 1 if report.get("gate") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
