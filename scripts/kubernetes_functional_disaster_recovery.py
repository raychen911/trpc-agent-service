#!/usr/bin/env python3
"""Run and collect a zero-cost recovery function drill on a real cluster.

The command creates an isolated, short-lived namespace with emptyDir-backed
PostgreSQL and MinIO, runs three candidate-image restore Jobs, collects their
API evidence, and deletes the namespace.  It cannot produce a production DR
pass because no remote durability or external KMS is involved.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

from scripts.candidate_lock import verify_candidate_lock
from scripts.deployment_config import RuntimeGateConfig, load_runtime_gate_config
from scripts.disaster_recovery_gate import COMPONENTS
from scripts.functional_disaster_recovery_gate import build_report
from scripts.kubernetes_disaster_recovery import (
    DisasterRecoveryCollectionTimeout,
    _kubectl,
    _kubectl_json,
    _read_json,
    collect_drill,
)
from scripts.report_io import atomic_write_json

NAMESPACE_PREFIX = "trpc-dr-functional-"
IMMUTABLE_IMAGE_RE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")


def _metadata(name: str, namespace: str | None = None) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "name": name,
        "labels": {"app.kubernetes.io/managed-by": "trpc-dr-functional"},
    }
    if namespace is not None:
        metadata["namespace"] = namespace
    return metadata


def _secret_ref(name: str, key: str) -> dict[str, Any]:
    return {"secretKeyRef": {"name": name, "key": key}}


def _service(name: str, namespace: str, port: int) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(name, namespace),
        "spec": {
            "selector": {"app.kubernetes.io/name": name},
            "ports": [{"name": name, "port": port, "targetPort": name}],
        },
    }


def _require_immutable_image(reference: str, *, label: str) -> str:
    if IMMUTABLE_IMAGE_RE.fullmatch(reference) is None:
        raise ValueError(f"{label} must be an immutable sha256 image reference")
    return reference


def _support_resources(
    *,
    namespace: str,
    pull_secret: str,
    postgres_image: str,
    minio_image: str,
) -> list[dict[str, Any]]:
    _require_immutable_image(postgres_image, label="postgres image")
    _require_immutable_image(minio_image, label="MinIO image")
    postgres = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata("postgres", namespace),
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/name": "postgres"}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": "postgres"}},
                "spec": {
                    "automountServiceAccountToken": False,
                    "imagePullSecrets": [{"name": pull_secret}],
                    "containers": [
                        {
                            "name": "postgres",
                            "image": postgres_image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": [
                                {
                                    "name": "POSTGRES_PASSWORD",
                                    "valueFrom": _secret_ref("dr-functional", "postgres-password"),
                                }
                            ],
                            "ports": [{"name": "postgres", "containerPort": 5432}],
                            "readinessProbe": {
                                "exec": {
                                    "command": ["pg_isready", "-U", "postgres", "-d", "postgres"]
                                },
                                "periodSeconds": 2,
                                "failureThreshold": 60,
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "128Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/var/lib/postgresql/data"}
                            ],
                        }
                    ],
                    "volumes": [{"name": "data", "emptyDir": {}}],
                },
            },
        },
    }
    minio = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata("minio", namespace),
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/name": "minio"}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": "minio"}},
                "spec": {
                    "automountServiceAccountToken": False,
                    "imagePullSecrets": [{"name": pull_secret}],
                    "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
                    "containers": [
                        {
                            "name": "minio",
                            "image": minio_image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": ["server", "/data"],
                            "env": [
                                {
                                    "name": "MINIO_ROOT_USER",
                                    "valueFrom": _secret_ref("dr-functional", "minio-user"),
                                },
                                {
                                    "name": "MINIO_ROOT_PASSWORD",
                                    "valueFrom": _secret_ref("dr-functional", "minio-password"),
                                },
                            ],
                            "ports": [{"name": "minio", "containerPort": 9000}],
                            "readinessProbe": {
                                "httpGet": {"path": "/minio/health/ready", "port": "minio"},
                                "periodSeconds": 2,
                                "failureThreshold": 60,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "128Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                            "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                        }
                    ],
                    "volumes": [{"name": "data", "emptyDir": {}}],
                },
            },
        },
    }
    return [
        postgres,
        _service("postgres", namespace, 5432),
        minio,
        _service("minio", namespace, 9000),
    ]


def _job(
    *,
    component: str,
    namespace: str,
    candidate_image: str,
    pull_secret: str,
    drill_id: str,
) -> dict[str, Any]:
    _require_immutable_image(candidate_image, label="candidate image")
    name = component.replace("_", "-")
    common_env: list[dict[str, Any]] = [
        {"name": "TRPC_DRILL_ID", "value": drill_id},
        {"name": "TRPC_DR_NAMESPACE", "value": namespace},
        {
            "name": "TRPC_DR_TENANT_ID",
            "valueFrom": _secret_ref("dr-functional", "tenant-id"),
        },
        {"name": "TRPC_DR_CANARY", "valueFrom": _secret_ref("dr-functional", "canary")},
        {
            "name": "TRPC_DR_POSTGRES_DSN",
            "valueFrom": _secret_ref("dr-functional", "postgres-dsn"),
        },
        {
            "name": "TRPC_DR_S3_ENDPOINT",
            "value": f"http://minio.{namespace}.svc.cluster.local:9000",
        },
        {"name": "TRPC_DR_S3_BUCKET", "value": "trpc-dr-functional"},
        {
            "name": "TRPC_DR_S3_ACCESS_KEY",
            "valueFrom": _secret_ref("dr-functional", "minio-user"),
        },
        {
            "name": "TRPC_DR_S3_SECRET_KEY",
            "valueFrom": _secret_ref("dr-functional", "minio-password"),
        },
        {
            "name": "TRPC_DR_TEST_WRAPPING_KEY",
            "valueFrom": _secret_ref("dr-functional", "wrapping-key"),
        },
    ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(name, namespace),
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 300,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": name,
                        "app.kubernetes.io/part-of": "trpc-dr-functional",
                    }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "imagePullSecrets": [{"name": pull_secret}],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "restore",
                            "image": candidate_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "python",
                                "-m",
                                "scripts.dr_functional_job",
                                "--component",
                                component,
                            ],
                            "env": common_env,
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "128Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                        }
                    ],
                },
            },
        },
    }


def build_manifests(
    *,
    namespace: str,
    pull_secret: str,
    candidate_image: str,
    postgres_image: str,
    minio_image: str,
    drill_id: str,
    postgres_password: str,
    minio_user: str,
    minio_password: str,
    tenant_id: str,
    canary: str,
    wrapping_key_b64: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return namespace, support, and Job manifests for one isolated drill."""

    _require_immutable_image(candidate_image, label="candidate image")
    _require_immutable_image(postgres_image, label="postgres image")
    _require_immutable_image(minio_image, label="MinIO image")
    postgres_host = f"postgres.{namespace}.svc.cluster.local"
    postgres_dsn = (
        f"postgresql://postgres:{quote(postgres_password, safe='')}@{postgres_host}:5432/postgres"
    )
    namespace_manifest = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": _metadata(namespace),
    }
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": _metadata("dr-functional", namespace),
        "type": "Opaque",
        "stringData": {
            "postgres-password": postgres_password,
            "postgres-dsn": postgres_dsn,
            "minio-user": minio_user,
            "minio-password": minio_password,
            "tenant-id": tenant_id,
            "canary": canary,
            "wrapping-key": wrapping_key_b64,
        },
    }
    support = {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            secret,
            *_support_resources(
                namespace=namespace,
                pull_secret=pull_secret,
                postgres_image=postgres_image,
                minio_image=minio_image,
            ),
        ],
    }
    jobs = {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            _job(
                component=component,
                namespace=namespace,
                candidate_image=candidate_image,
                pull_secret=pull_secret,
                drill_id=drill_id,
            )
            for component in COMPONENTS
        ],
    }
    return namespace_manifest, support, jobs


def _copy_pull_secret(
    *,
    name: str,
    source_namespace: str,
    target_namespace: str,
    kubeconfig: Path,
    context: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    source = _kubectl_json(
        ["get", "secret", name, "--namespace", source_namespace, "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    data = source.get("data")
    if not isinstance(data, Mapping) or not data:
        raise RuntimeError("image pull Secret data is missing")
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": _metadata(name, target_namespace),
        "type": source.get("type", "kubernetes.io/dockerconfigjson"),
        "data": dict(data),
    }


def _delete_namespace(
    *,
    namespace: str,
    namespace_uid: str,
    kubeconfig: Path,
    context: str,
    timeout_seconds: float,
) -> bool:
    if not namespace.startswith(NAMESPACE_PREFIX) or not namespace_uid:
        raise ValueError("refusing to delete a non-functional namespace")
    current = _kubectl_json(
        ["get", "namespace", namespace, "-o", "json"],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    metadata = current.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("uid") != namespace_uid:
        raise RuntimeError("functional namespace identity changed before cleanup")
    _kubectl(
        [
            "delete",
            "namespace",
            namespace,
            f"--timeout={int(timeout_seconds)}s",
            "--wait=true",
        ],
        kubeconfig=kubeconfig,
        context=context,
        timeout_seconds=timeout_seconds + 10,
    )
    return True


def _wait_for_job(
    *,
    job_name: str,
    namespace: str,
    kubeconfig: Path,
    context: str,
    timeout_seconds: float,
) -> None:
    """Wait for one Job while failing immediately on terminal failure."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        job = _kubectl_json(
            ["get", "job", job_name, "--namespace", namespace, "-o", "json"],
            kubeconfig=kubeconfig,
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        status = job.get("status")
        if not isinstance(status, Mapping):
            status = {}
        conditions = status.get("conditions")
        condition_items = conditions if isinstance(conditions, list) else []
        failed = status.get("failed", 0)
        if (isinstance(failed, int) and not isinstance(failed, bool) and failed > 0) or any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Failed"
            and condition.get("status") == "True"
            for condition in condition_items
        ):
            raise RuntimeError(f"functional restore Job {job_name} failed")
        if status.get("succeeded") == 1 and any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Complete"
            and condition.get("status") == "True"
            for condition in condition_items
        ):
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"functional restore Job {job_name} timed out")
        time.sleep(2)


def _validate_runtime(
    config: RuntimeGateConfig,
    *,
    lock: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> str:
    if config.support is None:
        raise ValueError("kubernetes.support is required for the functional recovery drill")
    reasons = verify_candidate_lock(lock, binding, root=Path(__file__).resolve().parents[1])
    if reasons:
        raise ValueError("candidate lock is invalid")
    release_binding = lock.get("release_binding")
    if (
        not isinstance(release_binding, Mapping)
        or release_binding.get("release_id") != config.release_id
    ):
        raise ValueError("candidate lock release_id does not match unified configuration")
    candidate_image = config.resolved_image_references()["initial"]
    if candidate_image.rsplit("@", 1)[1] != lock.get("image_digest"):
        raise ValueError("functional Job image does not match candidate lock")
    return candidate_image


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("deploy/runtime-gate.yaml"))
    parser.add_argument(
        "--candidate-lock", type=Path, default=Path("runs/multitenant/candidate-lock.json")
    )
    parser.add_argument(
        "--image-binding", type=Path, default=Path("runs/multitenant/registry-image-binding.json")
    )
    parser.add_argument("--pull-secret-namespace", default="trpc-service")
    parser.add_argument("--directory", type=Path, default=Path("runs/drill-functional"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/disaster-recovery-functional.json"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=600)
    parser.add_argument("--max-rto-seconds", type=float, default=300)
    parser.add_argument("--require-functional", action="store_true")
    args = parser.parse_args(argv)
    enabled = os.getenv("TRPC_DR_FUNCTIONAL_ENABLED", "").strip().lower() == "true"
    if not enabled:
        report = build_report(
            enabled=False,
            evidence_paths={},
            lock_path=args.candidate_lock,
            binding_path=args.image_binding,
            max_rto_seconds=args.max_rto_seconds,
        )
        print(atomic_write_json(args.output, report), end="")
        return 1 if args.require_functional else 0

    drill_id = f"drf-{uuid.uuid4().hex}"
    namespace = f"{NAMESPACE_PREFIX}{drill_id[-8:]}"
    evidence_directory = args.directory / drill_id
    evidence_paths = {
        component: evidence_directory / f"{component}.json" for component in COMPONENTS
    }
    collection_succeeded = False
    cleanup_completed = False
    namespace_created = False
    jobs_submitted = False
    namespace_uid = ""
    stage = "configuration"
    collection_failure_code: str | None = None
    config: RuntimeGateConfig | None = None
    try:
        if not 0 < args.timeout_seconds <= 1800:
            raise ValueError("--timeout-seconds must be within 0..1800")
        config = load_runtime_gate_config(args.config)
        lock = _read_json(args.candidate_lock)
        binding = _read_json(args.image_binding)
        candidate_image = _validate_runtime(config, lock=lock, binding=binding)
        assert config.support is not None

        postgres_password = secrets.token_urlsafe(32)
        minio_user = f"dr{secrets.token_hex(8)}"
        minio_password = secrets.token_urlsafe(32)
        tenant_id = f"functional-{secrets.token_hex(12)}"
        canary = secrets.token_urlsafe(32)
        wrapping_key_b64 = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
        namespace_manifest, support_manifest, jobs_manifest = build_manifests(
            namespace=namespace,
            pull_secret=config.image_pull_secret,
            candidate_image=candidate_image,
            postgres_image=config.support.postgres_image,
            minio_image=config.support.minio_image,
            drill_id=drill_id,
            postgres_password=postgres_password,
            minio_user=minio_user,
            minio_password=minio_password,
            tenant_id=tenant_id,
            canary=canary,
            wrapping_key_b64=wrapping_key_b64,
        )
        with tempfile.TemporaryDirectory(prefix="trpc-dr-functional-") as temporary:
            temporary_root = Path(temporary)
            namespace_path = temporary_root / "namespace.json"
            support_path = temporary_root / "support.json"
            jobs_path = temporary_root / "jobs.json"
            pull_secret_path = temporary_root / "pull-secret.json"
            _write_json(namespace_path, namespace_manifest)
            stage = "namespace_create"
            _kubectl(
                ["create", "-f", str(namespace_path)],
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )
            namespace_created = True
            namespace_object = _kubectl_json(
                ["get", "namespace", namespace, "-o", "json"],
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )
            metadata = namespace_object.get("metadata")
            if not isinstance(metadata, Mapping) or not isinstance(metadata.get("uid"), str):
                raise RuntimeError("functional namespace UID is missing")
            namespace_uid = metadata["uid"]

            stage = "pull_secret_copy"
            pull_secret = _copy_pull_secret(
                name=config.image_pull_secret,
                source_namespace=args.pull_secret_namespace,
                target_namespace=namespace,
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )
            _write_json(pull_secret_path, pull_secret)
            _kubectl(
                ["apply", "-f", str(pull_secret_path)],
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )

            stage = "support_start"
            _write_json(support_path, support_manifest)
            _kubectl(
                ["apply", "-f", str(support_path)],
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )
            for deployment in ("postgres", "minio"):
                _kubectl(
                    [
                        "rollout",
                        "status",
                        f"deployment/{deployment}",
                        "--namespace",
                        namespace,
                        f"--timeout={int(args.timeout_seconds)}s",
                    ],
                    kubeconfig=config.kubeconfig,
                    context=config.context,
                    timeout_seconds=args.timeout_seconds + 10,
                )

            stage = "restore_jobs"
            _write_json(jobs_path, jobs_manifest)
            _kubectl(
                ["apply", "-f", str(jobs_path)],
                kubeconfig=config.kubeconfig,
                context=config.context,
                timeout_seconds=args.timeout_seconds,
            )
            jobs_submitted = True
            for component in COMPONENTS:
                job_name = component.replace("_", "-")
                _wait_for_job(
                    job_name=job_name,
                    namespace=namespace,
                    kubeconfig=config.kubeconfig,
                    context=config.context,
                    timeout_seconds=args.timeout_seconds,
                )

            stage = "evidence_collection"
            observations = collect_drill(
                namespace=namespace,
                context=config.context,
                kubeconfig=config.kubeconfig,
                jobs={component: component.replace("_", "-") for component in COMPONENTS},
                lock=lock,
                timeout_seconds=args.timeout_seconds,
            )
            evidence_directory.mkdir(parents=True, exist_ok=True)
            for component, observation in observations.items():
                atomic_write_json(evidence_paths[component], observation)
            collection_succeeded = True
    except DisasterRecoveryCollectionTimeout:
        collection_succeeded = False
        collection_failure_code = "evidence_not_ready_timeout"
    except (AssertionError, OSError, UnicodeError, ValueError, RuntimeError, json.JSONDecodeError):
        collection_succeeded = False
        collection_failure_code = (
            "evidence_invalid" if stage == "evidence_collection" else "orchestration_failed"
        )
    finally:
        if config is not None and namespace_created:
            try:
                if not namespace_uid:
                    namespace_object = _kubectl_json(
                        ["get", "namespace", namespace, "-o", "json"],
                        kubeconfig=config.kubeconfig,
                        context=config.context,
                        timeout_seconds=min(args.timeout_seconds, 300),
                    )
                    metadata = namespace_object.get("metadata")
                    if isinstance(metadata, Mapping) and isinstance(metadata.get("uid"), str):
                        namespace_uid = metadata["uid"]
                if not namespace_uid:
                    raise RuntimeError("functional namespace UID is unavailable for cleanup")
                cleanup_completed = _delete_namespace(
                    namespace=namespace,
                    namespace_uid=namespace_uid,
                    kubeconfig=config.kubeconfig,
                    context=config.context,
                    timeout_seconds=min(args.timeout_seconds, 300),
                )
            except (OSError, ValueError, RuntimeError):
                cleanup_completed = False

    if not collection_succeeded:
        evidence_paths = {
            component: evidence_directory / f".failed-{uuid.uuid4().hex}-{component}.json"
            for component in COMPONENTS
        }
    report = build_report(
        enabled=True,
        evidence_paths=evidence_paths,
        lock_path=args.candidate_lock,
        binding_path=args.image_binding,
        max_rto_seconds=args.max_rto_seconds,
    )
    candidate = report.get("candidate")
    if isinstance(candidate, dict):
        candidate["orchestration"] = {
            "failure_stage": None if collection_succeeded else stage,
            "failure_code": None if collection_succeeded else collection_failure_code,
            "namespace_sha256": _hash(namespace),
            "namespace_uid_sha256": _hash(namespace_uid) if namespace_uid else None,
            "namespace_created": namespace_created,
            "jobs_submitted_together": jobs_submitted,
            "cleanup_completed": cleanup_completed,
        }
    if not cleanup_completed:
        report["gate"] = "fail"
        reasons = report.get("rejection_reasons")
        if isinstance(reasons, list):
            reasons.append("functional namespace cleanup was not confirmed")
        failed = report.get("case_deltas")
        if isinstance(failed, dict):
            failed["failed_components"] = list(COMPONENTS)
    rendered = atomic_write_json(args.output, report)
    print(rendered, end="")
    return 1 if report["gate"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
