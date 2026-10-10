#!/usr/bin/env python3
# ruff: noqa: I001
"""Run one auditable performance workload inside the ACK cluster.

The workstation-side command creates exactly one short-lived Kubernetes Job.
The Job is pinned to the dedicated load-driver node and talks to the gateway,
PostgreSQL, and Redis through cluster DNS; no port-forward is involved in the
measurement.  Credentials are copied into a nonce-scoped temporary Secret,
while the bounded command and endpoint settings live in a ConfigMap.

The container-side ``--worker`` mode is intentionally small.  It creates the
synthetic fixture, runs the gate command supplied by the parent, and cleans the
fixture in a ``finally`` block.  It emits one compact, uniquely-prefixed JSON
record so the parent can collect evidence without printing child stdout or
stderr (which might contain an implementation-specific secret or URL).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4


SCHEMA_VERSION = 1
REPORT_MARKER = "TRPC_K8S_PERFORMANCE_REPORT="
DEFAULT_JOB_TIMEOUT_SECONDS = 1_200.0
DEFAULT_TTL_SECONDS = 300
DEFAULT_CLEANUP_TIMEOUT_SECONDS = 60.0
MAX_JOB_TIMEOUT_SECONDS = 1_800.0
MAX_CLEANUP_TIMEOUT_SECONDS = 300.0
MAX_COMMAND_ARGS = 64
MAX_COMMAND_ARG_BYTES = 512
MAX_ENV_VALUE_BYTES = 8_192
MAX_IMAGE_BYTES = 512
MAX_REPORT_BYTES = 8 * 1024 * 1024
MAX_LABEL_VALUE_BYTES = 63
PREFLIGHT_EVIDENCE_FILE_NAME = "preflight-evidence.json"
PREFLIGHT_EVIDENCE_MOUNT_PATH = "/var/run/trpc-performance"
PREFLIGHT_EVIDENCE_PATH = f"{PREFLIGHT_EVIDENCE_MOUNT_PATH}/{PREFLIGHT_EVIDENCE_FILE_NAME}"
WORKER_TOKEN_ENV = "TRPC_REAL_PERFORMANCE_WORKER_TOKEN"  # noqa: S105 - capability name

NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
IMAGE_RE = re.compile(r"^.+@sha256:[0-9a-f]{64}$", re.IGNORECASE)
SOURCE_RE = re.compile(r"^[0-9a-f]{64}$")
CONTEXT_RE = re.compile(r"^[^\x00\r\n\t ]{1,128}$")
QUANTITY_RE = re.compile(
    r"^(?:[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)(?:"
    r"Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E|m)?$"
)

NODE_SELECTOR_KEY = "trpc-role"
NODE_SELECTOR_VALUE = "load-driver"
TOLERATION_EFFECT = "NoSchedule"
DEFAULT_REQUESTS = {"cpu": "2", "memory": "2Gi"}
DEFAULT_LIMITS = {"cpu": "4", "memory": "4Gi"}

SECRET_ENV_NAMES = frozenset(
    {
        "TRPC_SERVICE_DATABASE_DSN",
        "TRPC_SERVICE_REDIS_URL",
        "TRPC_SERVICE_SESSION_HMAC_KEY",
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
        "TRPC_RELEASE_NONCE",
        WORKER_TOKEN_ENV,
    }
)
CONFIG_ENV_NAMES = frozenset(
    {
        "TRPC_PERF_GATEWAY_BASE_URL",
        "TRPC_PERF_FIXTURE_CONFIRM",
        "TRPC_PERF_FIXTURE_REMOTE_CONFIRM",
        "TRPC_REAL_PERFORMANCE_CONFIRM",
        "TRPC_RUN_REAL_MULTINODE",
        "TRPC_RELEASE_ID",
        "TRPC_REAL_IMAGE_DIGEST",
        "TRPC_PERF_K8S_LOAD_JOB",
        "TRPC_K8S_PERF_GATE_COMMAND",
        "TRPC_K8S_PERF_RUN_ID",
        "TRPC_K8S_PERF_NAMESPACE",
        "TRPC_K8S_PERF_CONTEXT",
        "TRPC_K8S_PERF_POSTGRES_SERVICE_DNS",
        "TRPC_K8S_PERF_REDIS_SERVICE_DNS",
        "TRPC_K8S_PERF_POSTGRES_PORT",
        "TRPC_K8S_PERF_REDIS_PORT",
        "TRPC_K8S_PERF_TIMEOUT_SECONDS",
        "TRPC_K8S_PERF_REPORT_PATH",
        "TRPC_K8S_PERF_FIXTURE_PATH",
        "TRPC_K8S_PERF_CLEANUP_PATH",
        "TRPC_K8S_PERF_JOB_MODE",
        "TRPC_K8S_PERF_PREFLIGHT_EVIDENCE_PATH",
    }
)


class PerformanceJobError(RuntimeError):
    """Raised for a safe, reportable orchestration failure."""


class _KubectlTimeoutError(PerformanceJobError):
    """Raised when one bounded kubectl request exceeds its timeout."""


@dataclass(frozen=True)
class PerformanceJobSpec:
    """Validated inputs for one temporary performance Job."""

    namespace: str
    context: str
    kubeconfig: Path
    image: str
    image_pull_secret: str
    source_fingerprint: str
    run_id: str
    command: tuple[str, ...]
    secret_env: Mapping[str, str]
    config_env: Mapping[str, str]
    node_selector: Mapping[str, str]
    toleration: Mapping[str, str]
    requests: Mapping[str, str]
    limits: Mapping[str, str]
    preflight_evidence_json: str
    timeout_seconds: float = DEFAULT_JOB_TIMEOUT_SECONDS
    ttl_seconds: int = DEFAULT_TTL_SECONDS
    cleanup_timeout_seconds: float = DEFAULT_CLEANUP_TIMEOUT_SECONDS


def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _redact(value: Any, secrets: Sequence[str]) -> Any:
    """Return JSON-shaped evidence with credential values removed."""

    usable = tuple(secret for secret in secrets if isinstance(secret, str) and secret)
    if isinstance(value, str):
        redacted = value
        for secret in usable:
            redacted = redacted.replace(secret, "[redacted]")
        return redacted
    if isinstance(value, Mapping):
        return {str(key): _redact(item, usable) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, usable) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, usable) for item in value]
    return value


def _preflight_binding(
    path: Path,
    *,
    run_id: str,
    source_fingerprint: str,
    image_digest: str,
) -> dict[str, str]:
    """Read only the non-secret identity claims from the mounted evidence."""

    if path.is_symlink():
        raise PerformanceJobError("performance preflight evidence must not be a symlink")
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise PerformanceJobError("performance preflight evidence could not be read") from error
    if len(raw) > 2 * 1024 * 1024:
        raise PerformanceJobError("performance preflight evidence exceeds the size limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise PerformanceJobError("performance preflight evidence is invalid JSON") from error
    if not isinstance(value, Mapping):
        raise PerformanceJobError("performance preflight evidence must be an object")
    expected = {
        "run_id": run_id,
        "source_fingerprint": source_fingerprint.lower(),
        "image_digest": image_digest.lower(),
    }
    if any(value.get(key) != item for key, item in expected.items()):
        raise PerformanceJobError("performance preflight evidence binding mismatches Job")
    preflight = value.get("preflight")
    if not isinstance(preflight, Mapping) or preflight.get("status") != "pass":
        raise PerformanceJobError("performance preflight evidence is not a passing preflight")
    return {
        "status": "verified",
        **expected,
        "preflight_sha256": str(value.get("preflight_sha256", "")),
    }


def _require_gate_option(command: Sequence[str], option: str, expected: str | None = None) -> None:
    positions = [index for index, value in enumerate(command) if value == option]
    if len(positions) != 1:
        raise PerformanceJobError(f"performance gate command must contain exactly one {option}")
    if expected is None:
        return
    position = positions[0]
    if position + 1 >= len(command) or command[position + 1] != expected:
        raise PerformanceJobError(f"performance gate command {option} is not bound to this Job")


def _validate_gate_command(
    command: Sequence[str],
    *,
    namespace: str,
    context: str,
    source_fingerprint: str,
    image_digest: str,
    evidence_path: str,
) -> tuple[str, ...]:
    validated = _validate_command(command)
    for option in ("--load-worker", "--kubernetes-load-worker", "--kubernetes"):
        _require_gate_option(validated, option)
    for option, expected in (
        ("--kubernetes-namespace", namespace),
        ("--kubernetes-context", context),
        ("--kubernetes-image-digest", image_digest),
        ("--kubernetes-source-fingerprint", source_fingerprint),
        ("--kubernetes-preflight-evidence", evidence_path),
    ):
        _require_gate_option(validated, option, expected)
    return validated


def _validate_gate_report_identity(
    report: Mapping[str, Any],
    *,
    run_id: str,
    source_fingerprint: str,
    image_digest: str,
) -> None:
    """Require the gate's own final evidence to retain the Job binding."""

    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping) or candidate.get("run_id") != run_id:
        raise PerformanceJobError("performance gate report run ID is not bound to the Job")
    evidence = report.get("evidence")
    if not isinstance(evidence, Mapping):
        raise PerformanceJobError("performance gate report current evidence is missing")
    evidence_source = evidence.get("source_fingerprint")
    if (
        not isinstance(evidence_source, Mapping)
        or evidence_source.get("value") != source_fingerprint
    ):
        raise PerformanceJobError("performance gate report source fingerprint is not current")
    preflight = candidate.get("preflight")
    if not isinstance(preflight, Mapping):
        raise PerformanceJobError("performance gate report preflight evidence is missing")
    preflight_source = preflight.get("source_fingerprint")
    if (
        not isinstance(preflight_source, Mapping)
        or preflight_source.get("value") != source_fingerprint
    ):
        raise PerformanceJobError("performance gate report preflight source is not bound")
    worker_attestation = preflight.get("worker_image_attestation")
    if (
        not isinstance(worker_attestation, Mapping)
        or worker_attestation.get("image_id") != image_digest
    ):
        raise PerformanceJobError("performance gate report worker image is not bound")
    service_attestation = preflight.get("service_image_attestation")
    if not isinstance(service_attestation, Mapping):
        raise PerformanceJobError("performance gate report service image evidence is missing")
    for role in ("worker", "outbox-dispatcher"):
        attestation = service_attestation.get(role)
        if not isinstance(attestation, Mapping) or attestation.get("image_id") != image_digest:
            raise PerformanceJobError(f"performance gate report {role} image is not bound")


def _valid_text(value: object, *, max_bytes: int, field: str) -> str:
    if not isinstance(value, str) or not value or any(char in value for char in "\x00\r\n"):
        raise ValueError(f"{field} is invalid")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field} is too long")
    return value


def _validate_env(
    values: Mapping[str, str], *, field: str, allowed: frozenset[str]
) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in values.items():
        if key not in allowed or ENV_NAME_RE.fullmatch(key) is None:
            raise ValueError(f"{field} contains an unsupported environment name")
        result[key] = _valid_text(value, max_bytes=MAX_ENV_VALUE_BYTES, field=f"{field}.{key}")
    return result


def _validate_command(command: Sequence[str]) -> tuple[str, ...]:
    if isinstance(command, (str, bytes)) or not 1 <= len(command) <= MAX_COMMAND_ARGS:
        raise ValueError("performance Job command must be a bounded argv list")
    result: list[str] = []
    for argument in command:
        result.append(_valid_text(argument, max_bytes=MAX_COMMAND_ARG_BYTES, field="command item"))
    return tuple(result)


def _validate_resources(values: Mapping[str, str], *, field: str) -> dict[str, str]:
    if not values:
        raise ValueError(f"{field} must not be empty")
    result: dict[str, str] = {}
    for key, value in values.items():
        if key not in {"cpu", "memory"}:
            raise ValueError(f"{field} contains an unsupported resource")
        rendered = _valid_text(value, max_bytes=32, field=f"{field}.{key}")
        if QUANTITY_RE.fullmatch(rendered) is None:
            raise ValueError(f"{field}.{key} is not a Kubernetes quantity")
        result[key] = rendered
    return result


def build_spec(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    image: str,
    image_pull_secret: str,
    source_fingerprint: str,
    secret_env: Mapping[str, str],
    config_env: Mapping[str, str],
    command: Sequence[str],
    run_id: str | None = None,
    node_selector: Mapping[str, str] | None = None,
    toleration: Mapping[str, str] | None = None,
    requests: Mapping[str, str] | None = None,
    limits: Mapping[str, str] | None = None,
    preflight_evidence: Mapping[str, Any] | None = None,
    timeout_seconds: float = DEFAULT_JOB_TIMEOUT_SECONDS,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    cleanup_timeout_seconds: float = DEFAULT_CLEANUP_TIMEOUT_SECONDS,
) -> PerformanceJobSpec:
    """Validate operator inputs and return an immutable Job specification."""

    if NAMESPACE_RE.fullmatch(namespace) is None:
        raise ValueError("performance Job namespace is invalid")
    if CONTEXT_RE.fullmatch(context) is None:
        raise ValueError("performance Job context is invalid")
    if not isinstance(kubeconfig, Path) or not str(kubeconfig):
        raise ValueError("performance Job kubeconfig is invalid")
    if IMAGE_RE.fullmatch(image) is None or len(image.encode("utf-8")) > MAX_IMAGE_BYTES:
        raise ValueError("performance Job image must be an immutable sha256 reference")
    if NAME_RE.fullmatch(image_pull_secret) is None:
        raise ValueError("performance Job image pull Secret name is invalid")
    if SOURCE_RE.fullmatch(source_fingerprint.lower()) is None:
        raise ValueError("performance Job source fingerprint is invalid")
    selected_run_id = (run_id or uuid4().hex).lower()
    if NAME_RE.fullmatch(selected_run_id) is None or len(selected_run_id) > 40:
        raise ValueError("performance Job run ID is invalid")
    selected_selector = dict(node_selector or {NODE_SELECTOR_KEY: NODE_SELECTOR_VALUE})
    if len(selected_selector) != 1 or selected_selector != {NODE_SELECTOR_KEY: NODE_SELECTOR_VALUE}:
        raise ValueError("performance Job nodeSelector must target the load-driver node")
    selected_toleration = dict(
        toleration
        or {
            "key": NODE_SELECTOR_KEY,
            "operator": "Equal",
            "value": NODE_SELECTOR_VALUE,
            "effect": TOLERATION_EFFECT,
        }
    )
    if selected_toleration != {
        "key": NODE_SELECTOR_KEY,
        "operator": "Equal",
        "value": NODE_SELECTOR_VALUE,
        "effect": TOLERATION_EFFECT,
    }:
        raise ValueError("performance Job toleration must match the load-driver taint")
    selected_requests = _validate_resources(requests or DEFAULT_REQUESTS, field="requests")
    selected_limits = _validate_resources(limits or DEFAULT_LIMITS, field="limits")
    selected_secret = _validate_env(secret_env, field="secret_env", allowed=SECRET_ENV_NAMES)
    selected_config = _validate_env(config_env, field="config_env", allowed=CONFIG_ENV_NAMES)
    worker_token = selected_secret.get(WORKER_TOKEN_ENV)
    if worker_token is None or not 16 <= len(worker_token) <= 4096:
        raise ValueError("performance Job worker token is missing or invalid")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in worker_token):
        raise ValueError("performance Job worker token is invalid")
    if not isinstance(preflight_evidence, Mapping):
        raise ValueError("performance Job preflight evidence is missing")
    try:
        evidence_json = _safe_json(dict(preflight_evidence))
    except (TypeError, ValueError) as error:
        raise ValueError("performance Job preflight evidence is invalid JSON") from error
    if len(evidence_json.encode("utf-8")) > 2 * 1024 * 1024:
        raise ValueError("performance Job preflight evidence is too large")
    try:
        evidence_value = json.loads(evidence_json)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("performance Job preflight evidence is invalid JSON") from error
    if not isinstance(evidence_value, Mapping):
        raise ValueError("performance Job preflight evidence must be an object")
    image_digest = image.rsplit("@", 1)[1].lower()
    if (
        evidence_value.get("run_id") != selected_run_id
        or evidence_value.get("source_fingerprint") != source_fingerprint.lower()
        or evidence_value.get("image_digest") != image_digest
    ):
        raise ValueError("performance Job preflight evidence binding mismatches Job")
    if worker_token in evidence_json:
        raise ValueError("performance Job preflight evidence must not contain the worker token")
    if not math.isfinite(timeout_seconds) or not 1 <= timeout_seconds <= MAX_JOB_TIMEOUT_SECONDS:
        raise ValueError("performance Job timeout must be in [1, 1800]")
    if (
        not isinstance(ttl_seconds, int)
        or isinstance(ttl_seconds, bool)
        or not 1 <= ttl_seconds <= 86_400
    ):
        raise ValueError("performance Job TTL is invalid")
    if (
        not math.isfinite(cleanup_timeout_seconds)
        or not 1 <= cleanup_timeout_seconds <= MAX_CLEANUP_TIMEOUT_SECONDS
    ):
        raise ValueError("performance Job cleanup timeout must be in [1, 300]")
    return PerformanceJobSpec(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        image=image,
        image_pull_secret=image_pull_secret,
        source_fingerprint=source_fingerprint.lower(),
        run_id=selected_run_id,
        command=_validate_command(command),
        secret_env=selected_secret,
        config_env=selected_config,
        node_selector=selected_selector,
        toleration=selected_toleration,
        requests=selected_requests,
        limits=selected_limits,
        preflight_evidence_json=evidence_json,
        timeout_seconds=timeout_seconds,
        ttl_seconds=ttl_seconds,
        cleanup_timeout_seconds=cleanup_timeout_seconds,
    )


def _resource_name(spec: PerformanceJobSpec, kind: str) -> str:
    return f"trpc-perf-{kind}-{spec.run_id[:24]}"


def _labels(spec: PerformanceJobSpec) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": "trpc-performance-gate",
        "app.kubernetes.io/component": "load-driver",
        "trpc.io/performance-runner": "true",
        "trpc.io/performance-run": spec.run_id,
        "trpc.io/source-fingerprint": spec.source_fingerprint[:MAX_LABEL_VALUE_BYTES],
    }


def secret_manifest(spec: PerformanceJobSpec, name: str | None = None) -> dict[str, Any]:
    """Build a temporary Secret without returning any cleartext values."""

    secret_name = name or _resource_name(spec, "secret")
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": secret_name, "namespace": spec.namespace, "labels": _labels(spec)},
        "type": "Opaque",
        "data": {
            key: base64.b64encode(value.encode("utf-8")).decode("ascii")
            for key, value in spec.secret_env.items()
        },
    }


def config_map_manifest(spec: PerformanceJobSpec, name: str | None = None) -> dict[str, Any]:
    config_name = name or _resource_name(spec, "config")
    data = dict(spec.config_env)
    data[PREFLIGHT_EVIDENCE_FILE_NAME] = spec.preflight_evidence_json
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": config_name, "namespace": spec.namespace, "labels": _labels(spec)},
        "data": data,
    }


def job_manifest(
    spec: PerformanceJobSpec,
    *,
    secret_name: str | None = None,
    config_name: str | None = None,
    job_name: str | None = None,
) -> dict[str, Any]:
    """Build the immutable, single-completion Job manifest."""

    selected_job_name = job_name or _resource_name(spec, "job")
    selected_secret = secret_name or _resource_name(spec, "secret")
    selected_config = config_name or _resource_name(spec, "config")
    labels = _labels(spec)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": selected_job_name,
            "namespace": spec.namespace,
            "labels": labels,
            "annotations": {
                "trpc.io/source-fingerprint": spec.source_fingerprint,
                "trpc.io/image-digest": spec.image.rsplit("@", 1)[1].lower(),
            },
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 0,
            "activeDeadlineSeconds": max(1, math.ceil(spec.timeout_seconds + 60.0)),
            "ttlSecondsAfterFinished": spec.ttl_seconds,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "dnsPolicy": "ClusterFirst",
                    "nodeSelector": dict(spec.node_selector),
                    "tolerations": [dict(spec.toleration)],
                    "imagePullSecrets": [{"name": spec.image_pull_secret}],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "volumes": [
                        {"name": "tmp", "emptyDir": {}},
                        {
                            "name": "preflight-evidence",
                            "configMap": {
                                "name": selected_config,
                                "items": [
                                    {
                                        "key": PREFLIGHT_EVIDENCE_FILE_NAME,
                                        "path": PREFLIGHT_EVIDENCE_FILE_NAME,
                                    }
                                ],
                                "defaultMode": 0o440,
                            },
                        },
                    ],
                    "containers": [
                        {
                            "name": "load-driver",
                            "image": spec.image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": list(spec.command),
                            "envFrom": [
                                {"secretRef": {"name": selected_secret}},
                                {"configMapRef": {"name": selected_config}},
                            ],
                            "env": [
                                {"name": "TRPC_K8S_PERF_JOB_MODE", "value": "worker"},
                                {
                                    "name": "TRPC_K8S_PERF_SOURCE_FINGERPRINT",
                                    "value": spec.source_fingerprint,
                                },
                                {
                                    "name": "TRPC_K8S_PERF_IMAGE_DIGEST",
                                    "value": spec.image.rsplit("@", 1)[1].lower(),
                                },
                            ],
                            "resources": {
                                "requests": dict(spec.requests),
                                "limits": dict(spec.limits),
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [
                                {"name": "tmp", "mountPath": "/tmp"},  # noqa: S108
                                {
                                    "name": "preflight-evidence",
                                    "mountPath": PREFLIGHT_EVIDENCE_PATH,
                                    "subPath": PREFLIGHT_EVIDENCE_FILE_NAME,
                                    "readOnly": True,
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }


def _kubectl_environment(kubeconfig: Path) -> dict[str, str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "KUBECONFIG": str(kubeconfig),
    }
    for name in ("HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value is not None:
            environment[name] = value
    return environment


def _kubectl(
    spec: PerformanceJobSpec,
    arguments: Sequence[str],
    *,
    timeout_seconds: float,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("kubectl")
    if executable is None:
        raise PerformanceJobError("kubectl is not installed")
    command = [
        executable,
        "--kubeconfig",
        str(spec.kubeconfig),
        "--context",
        spec.context,
        *[str(argument) for argument in arguments],
    ]
    try:
        return subprocess.run(  # noqa: S603 - fixed kubectl executable and argv
            command,
            check=False,
            capture_output=True,
            text=True,
            input=input_text,
            timeout=timeout_seconds,
            env=_kubectl_environment(spec.kubeconfig),
        )
    except subprocess.TimeoutExpired as error:
        raise _KubectlTimeoutError("kubectl request timed out") from error
    except OSError as error:
        raise PerformanceJobError("kubectl request could not start") from error


def _kubectl_read(
    spec: PerformanceJobSpec,
    arguments: Sequence[str],
    *,
    timeout_seconds: float,
    deadline: float,
) -> subprocess.CompletedProcess[str]:
    delays = (0.25, 0.5)
    for attempt in range(len(delays) + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _KubectlTimeoutError("kubectl request timed out")
        try:
            return _kubectl(
                spec,
                arguments,
                timeout_seconds=min(timeout_seconds, remaining),
            )
        except _KubectlTimeoutError:
            if attempt >= len(delays):
                raise
            delay = min(delays[attempt], max(0.0, deadline - time.monotonic()))
            if delay <= 0:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable kubectl read retry state")


def _ensure_success(result: subprocess.CompletedProcess[str], operation: str) -> None:
    if result.returncode != 0:
        raise PerformanceJobError(f"Kubernetes {operation} failed")


def _apply(spec: PerformanceJobSpec, document: Mapping[str, Any]) -> None:
    result = _kubectl(
        spec,
        ["apply", "--server-side=false", "--field-manager=trpc-performance-gate", "-f", "-"],
        timeout_seconds=min(spec.cleanup_timeout_seconds, 60.0),
        input_text=_safe_json(document),
    )
    _ensure_success(result, "resource creation")


def _json_result(result: subprocess.CompletedProcess[str], operation: str) -> Mapping[str, Any]:
    _ensure_success(result, operation)
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise PerformanceJobError(f"Kubernetes {operation} returned invalid JSON") from error
    if not isinstance(payload, Mapping):
        raise PerformanceJobError(f"Kubernetes {operation} returned a non-object")
    return payload


def _get_job(
    spec: PerformanceJobSpec,
    job_name: str,
    *,
    deadline: float,
) -> Mapping[str, Any]:
    result = _kubectl_read(
        spec,
        ["get", "job", job_name, "--namespace", spec.namespace, "-o", "json"],
        timeout_seconds=min(spec.cleanup_timeout_seconds, 30.0),
        deadline=deadline,
    )
    return _json_result(result, "Job inspection")


def _wait_for_job(spec: PerformanceJobSpec, job_name: str) -> Mapping[str, Any]:
    deadline = time.monotonic() + spec.timeout_seconds
    while True:
        try:
            payload = _get_job(spec, job_name, deadline=deadline)
        except _KubectlTimeoutError as error:
            if time.monotonic() >= deadline:
                raise PerformanceJobError("performance Job exceeded its active deadline") from error
            raise
        status = payload.get("status")
        status_map = status if isinstance(status, Mapping) else {}
        failed = status_map.get("failed", 0)
        if isinstance(failed, int) and failed > 0:
            return payload
        conditions = status_map.get("conditions")
        if isinstance(conditions, Sequence) and not isinstance(conditions, (str, bytes)):
            for condition in conditions:
                if isinstance(condition, Mapping) and condition.get("type") == "Failed":
                    return payload
        succeeded = status_map.get("succeeded", 0)
        if isinstance(succeeded, int) and succeeded >= 1:
            return payload
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PerformanceJobError("performance Job exceeded its active deadline")
        time.sleep(min(2.0, remaining))


def _logs(spec: PerformanceJobSpec, job_name: str) -> str:
    operation_budget = min(spec.cleanup_timeout_seconds, 60.0)
    result = _kubectl_read(
        spec,
        [
            "logs",
            f"job/{job_name}",
            "--namespace",
            spec.namespace,
            "--all-containers=true",
            "--ignore-errors=true",
        ],
        timeout_seconds=operation_budget / 3.0,
        deadline=time.monotonic() + operation_budget,
    )
    _ensure_success(result, "Job log collection")
    if len(result.stdout.encode("utf-8")) > MAX_REPORT_BYTES:
        raise PerformanceJobError("performance Job logs exceed the evidence limit")
    return result.stdout


def parse_worker_report(logs: str) -> Mapping[str, Any]:
    """Extract exactly one safe JSON envelope from worker logs."""

    records = [
        line[len(REPORT_MARKER) :] for line in logs.splitlines() if line.startswith(REPORT_MARKER)
    ]
    if len(records) != 1:
        raise PerformanceJobError("performance Job did not emit exactly one report")
    try:
        payload = json.loads(records[0])
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise PerformanceJobError("performance Job report is invalid JSON") from error
    if not isinstance(payload, Mapping) or payload.get("schema_version") != SCHEMA_VERSION:
        raise PerformanceJobError("performance Job report schema is unsupported")
    return payload


def _get_named_object(
    spec: PerformanceJobSpec,
    kind: str,
    name: str,
    *,
    deadline: float | None = None,
) -> Mapping[str, Any] | None:
    operation_budget = min(spec.cleanup_timeout_seconds, 20.0)
    read_deadline = deadline if deadline is not None else time.monotonic() + operation_budget
    result = _kubectl_read(
        spec,
        ["get", kind, name, "--namespace", spec.namespace, "-o", "json"],
        timeout_seconds=operation_budget / 3.0,
        deadline=read_deadline,
    )
    if result.returncode != 0:
        text = f"{result.stdout}\n{result.stderr}".lower()
        if "not found" in text or "notfound" in text:
            return None
        raise PerformanceJobError(f"Kubernetes {kind} inspection failed")
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise PerformanceJobError(f"Kubernetes {kind} inspection returned invalid JSON") from error
    return payload if isinstance(payload, Mapping) else None


def _assert_owned_object(payload: Mapping[str, Any], spec: PerformanceJobSpec, name: str) -> None:
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
    if not isinstance(metadata, Mapping) or metadata.get("name") != name:
        raise PerformanceJobError("temporary resource identity changed")
    if not isinstance(labels, Mapping) or labels.get("trpc.io/performance-run") != spec.run_id:
        raise PerformanceJobError("temporary resource ownership proof is unavailable")


def _delete_and_confirm(spec: PerformanceJobSpec, kind: str, name: str) -> dict[str, Any]:
    existing = _get_named_object(spec, kind, name)
    if existing is None:
        return {"status": "pass", "kind": kind, "name": name, "already_absent": True}
    _assert_owned_object(existing, spec, name)
    result = _kubectl(
        spec,
        [
            "delete",
            kind,
            name,
            "--namespace",
            spec.namespace,
            "--ignore-not-found=true",
            "--wait=false",
        ],
        timeout_seconds=min(spec.cleanup_timeout_seconds, 30.0),
    )
    _ensure_success(result, f"{kind} deletion")
    deadline = time.monotonic() + spec.cleanup_timeout_seconds
    while True:
        observed = _get_named_object(spec, kind, name, deadline=deadline)
        if observed is None:
            return {"status": "pass", "kind": kind, "name": name, "already_absent": False}
        _assert_owned_object(observed, spec, name)
        if time.monotonic() >= deadline:
            raise PerformanceJobError(f"temporary {kind} deletion was not observed")
        time.sleep(0.5)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    if path.is_symlink() or any(parent.exists() and parent.is_symlink() for parent in path.parents):
        raise PerformanceJobError("report path must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_safe_json(payload) + "\n", encoding="utf-8")


def run_performance_job(spec: PerformanceJobSpec, output: Path) -> dict[str, Any]:
    """Create, observe, collect, and delete one temporary performance Job."""

    job_name = _resource_name(spec, "job")
    secret_name = _resource_name(spec, "secret")
    config_name = _resource_name(spec, "config")
    resources = (
        ("secret", secret_name),
        ("configmap", config_name),
        ("job", job_name),
    )
    attempted: list[tuple[str, str]] = []
    applied: list[tuple[str, str]] = []
    report: Mapping[str, Any] | None = None
    reasons: list[str] = []
    job_payload: Mapping[str, Any] | None = None
    try:
        for kind, document, name in (
            ("secret", secret_manifest(spec, secret_name), secret_name),
            ("configmap", config_map_manifest(spec, config_name), config_name),
            (
                "job",
                job_manifest(
                    spec,
                    secret_name=secret_name,
                    config_name=config_name,
                    job_name=job_name,
                ),
                job_name,
            ),
        ):
            attempted.append((kind, name))
            _apply(spec, document)
            applied.append((kind, name))
        job_payload = _wait_for_job(spec, job_name)
        report = parse_worker_report(_logs(spec, job_name))
        if report.get("status") != "pass":
            reasons.append("performance Job worker reported failure")
    except PerformanceJobError as error:
        reasons.append(str(error))
    except (OSError, ValueError, TypeError) as error:
        reasons.append(f"performance Job orchestration failed: {type(error).__name__}")
    finally:
        cleanup: list[dict[str, Any]] = []
        for kind, name in reversed(attempted):
            try:
                cleanup.append(_delete_and_confirm(spec, kind, name))
            except PerformanceJobError as error:
                cleanup.append({"status": "fail", "kind": kind, "name": name})
                reasons.append(str(error))
        cleanup.reverse()

    status = "pass" if report is not None and not reasons else "fail"
    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "report": dict(report) if report is not None else None,
        "rejection_reasons": reasons,
        "job": {
            "name": job_name,
            "namespace": spec.namespace,
            "run_id": spec.run_id,
            "image_digest": spec.image.rsplit("@", 1)[1].lower(),
            "source_fingerprint": spec.source_fingerprint,
            "preflight_evidence": {
                "status": "mounted",
                "path": PREFLIGHT_EVIDENCE_PATH,
                "run_id": spec.run_id,
                "source_fingerprint": spec.source_fingerprint,
                "image_digest": spec.image.rsplit("@", 1)[1].lower(),
                "sha256": hashlib.sha256(spec.preflight_evidence_json.encode("utf-8")).hexdigest(),
                "token_sha256": hashlib.sha256(
                    spec.secret_env[WORKER_TOKEN_ENV].encode("utf-8")
                ).hexdigest(),
            },
            "node_selector": dict(spec.node_selector),
            "toleration": dict(spec.toleration),
            "resources": {"requests": dict(spec.requests), "limits": dict(spec.limits)},
            "active_deadline_seconds": math.ceil(spec.timeout_seconds + 60.0),
            "ttl_seconds_after_finished": spec.ttl_seconds,
            "backoff_limit": 0,
            "created_resources": [
                {"kind": kind, "name": name} for kind, name in resources if (kind, name) in applied
            ],
            "cleanup_attempts": [{"kind": kind, "name": name} for kind, name in attempted],
            "uid": (
                job_payload.get("metadata", {}).get("uid")
                if isinstance(job_payload, Mapping)
                and isinstance(job_payload.get("metadata"), Mapping)
                else None
            ),
        },
        "cleanup": cleanup,
    }
    _write_json(output, envelope)
    return envelope


def _strict_command_from_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        raise PerformanceJobError(f"{name} is missing")
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise PerformanceJobError(f"{name} is invalid JSON") from error
    return _validate_command(value)


def _service_url(raw: str, *, hostname: str, port: str, field: str) -> str:
    """Replace only the authority in a Secret-backed service URL."""

    if not hostname or not port or not port.isdigit():
        raise PerformanceJobError(f"{field} service binding is invalid")
    try:
        parsed = urlsplit(raw)
    except ValueError as error:
        raise PerformanceJobError(f"{field} service URL is invalid") from error
    if not parsed.scheme or not parsed.netloc:
        raise PerformanceJobError(f"{field} service URL is invalid")
    authority = parsed.netloc.rsplit("@", 1)
    userinfo = f"{authority[0]}@" if len(authority) == 2 else ""
    safe_hostname = f"[{hostname}]" if ":" in hostname else hostname
    return urlunsplit(
        (
            parsed.scheme,
            f"{userinfo}{safe_hostname}:{port}",
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )


def _child_environment() -> dict[str, str]:
    environment = os.environ.copy()
    service_dsn = environment.get("TRPC_SERVICE_DATABASE_DSN", "")
    redis_url = environment.get("TRPC_SERVICE_REDIS_URL", "")
    worker_token = environment.get(WORKER_TOKEN_ENV, "")
    run_id = environment.get("TRPC_K8S_PERF_RUN_ID", "").strip().lower()
    source_fingerprint = environment.get("TRPC_K8S_PERF_SOURCE_FINGERPRINT", "").strip().lower()
    image_digest = environment.get("TRPC_K8S_PERF_IMAGE_DIGEST", "").strip().lower()
    if not service_dsn or not redis_url or not worker_token:
        raise PerformanceJobError("temporary performance Secret is incomplete")
    if not run_id or not source_fingerprint or not image_digest:
        raise PerformanceJobError("performance Job identity binding is incomplete")
    postgres_service = environment.get("TRPC_K8S_PERF_POSTGRES_SERVICE_DNS", "").strip()
    postgres_port = environment.get("TRPC_K8S_PERF_POSTGRES_PORT", "").strip()
    redis_service = environment.get("TRPC_K8S_PERF_REDIS_SERVICE_DNS", "").strip()
    redis_port = environment.get("TRPC_K8S_PERF_REDIS_PORT", "").strip()
    if postgres_service and postgres_port:
        service_dsn = _service_url(
            service_dsn,
            hostname=postgres_service,
            port=postgres_port,
            field="PostgreSQL",
        )
    if redis_service and redis_port:
        redis_url = _service_url(
            redis_url,
            hostname=redis_service,
            port=redis_port,
            field="Redis",
        )
    environment.update(
        {
            "TRPC_PERF_DATABASE_DSN": service_dsn,
            "TRPC_PERF_FIXTURE_CONFIRM": environment.get(
                "TRPC_PERF_FIXTURE_CONFIRM", "I_UNDERSTAND_PERFORMANCE_FIXTURE"
            ),
            "TRPC_PERF_FIXTURE_REMOTE_CONFIRM": environment.get(
                "TRPC_PERF_FIXTURE_REMOTE_CONFIRM", "I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE"
            ),
            "TRPC_PERF_GATEWAY_BASE_URL": environment.get("TRPC_PERF_GATEWAY_BASE_URL", ""),
            "TRPC_REAL_DATABASE_DSN": service_dsn,
            "TRPC_REAL_REDIS_URL": redis_url,
            "TRPC_REAL_SESSION_HMAC_KEY": environment.get("TRPC_SERVICE_SESSION_HMAC_KEY", ""),
            "TRPC_REAL_IMAGE_DIGEST": environment.get("TRPC_REAL_IMAGE_DIGEST", "") or image_digest,
            "TRPC_REAL_RUN_ID": run_id,
            "TRPC_K8S_PERF_JOB_MODE": "worker",
        }
    )
    if not environment["TRPC_REAL_SESSION_HMAC_KEY"]:
        raise PerformanceJobError("temporary performance Secret is incomplete")
    return environment


def _run_child(command: Sequence[str], environment: Mapping[str, str], timeout: float) -> int:
    try:
        completed = subprocess.run(  # noqa: S603 - command is validated argv, never a shell string
            list(command),
            check=False,
            capture_output=True,
            text=True,
            cwd=Path.cwd(),
            env=dict(environment),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise PerformanceJobError("performance child command timed out") from error
    except OSError as error:
        raise PerformanceJobError("performance child command could not start") from error
    return completed.returncode


def _read_partial_fixture_report(path: Path) -> Mapping[str, Any] | None:
    """Return only a cleanup-ready partial fixture report from the child."""

    if path.is_symlink():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if (
        isinstance(value, Mapping)
        and value.get("gate") == "partial"
        and value.get("cleanup_ready") is True
    ):
        return value
    return None


def _supervision_parent(timeout: float) -> tuple[int, subprocess.Popen[str] | None]:
    """Return a live PID for the gate's child watchdog.

    A Kubernetes container commonly runs this worker as PID 1.  The gate
    intentionally rejects PID 1 as an unsupervised parent, so use a tiny
    sleep sentinel only in that case.  It is terminated in the worker's
    ``finally`` block and never has access to credentials.
    """

    current = os.getpid()
    if current > 1:
        return current, None
    try:
        sleep = shutil.which("sleep")
        if sleep is None:
            raise PerformanceJobError("performance gate supervision is unavailable")
        sentinel = subprocess.Popen(  # noqa: S603 - fixed executable and argv
            [sleep, str(max(60, math.ceil(timeout + 30)))],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except OSError as error:
        raise PerformanceJobError("performance gate supervision could not start") from error
    return sentinel.pid, sentinel


def _worker_report(
    *,
    status: str,
    gate_report: Mapping[str, Any] | None,
    fixture_report: Mapping[str, Any] | None,
    cleanup_report: Mapping[str, Any] | None,
    reasons: Sequence[str],
    evidence_binding: Mapping[str, str] | None = None,
    secrets: Sequence[str] = (),
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "report": dict(gate_report) if gate_report is not None else None,
        "fixture": dict(fixture_report) if fixture_report is not None else {"status": "not_run"},
        "cleanup": dict(cleanup_report) if cleanup_report is not None else {"status": "not_run"},
        "rejection_reasons": list(reasons),
        "evidence_binding": (
            dict(evidence_binding) if evidence_binding is not None else {"status": "not_run"}
        ),
    }
    redacted = _redact(payload, secrets)
    return redacted if isinstance(redacted, dict) else payload


def worker_main() -> int:
    """Execute fixture, gate, and fixture cleanup in the Job container."""

    fixture_path = Path(
        os.getenv("TRPC_K8S_PERF_FIXTURE_PATH", "/tmp/trpc-performance-fixture.json")  # noqa: S108
    )
    gate_path = Path(
        os.getenv("TRPC_K8S_PERF_REPORT_PATH", "/tmp/trpc-performance-report.json")  # noqa: S108
    )
    cleanup_path = Path(
        os.getenv("TRPC_K8S_PERF_CLEANUP_PATH", "/tmp/trpc-performance-cleanup.json")  # noqa: S108
    )
    timeout = float(os.getenv("TRPC_K8S_PERF_TIMEOUT_SECONDS", "1_140"))
    reasons: list[str] = []
    fixture_report: Mapping[str, Any] | None = None
    gate_report: Mapping[str, Any] | None = None
    cleanup_report: Mapping[str, Any] | None = None
    evidence_binding: Mapping[str, str] | None = None
    sentinel: subprocess.Popen[str] | None = None
    environment: dict[str, str] = {}
    secret_values: tuple[str, ...] = ()
    try:
        environment = _child_environment()
        image_digest = environment["TRPC_K8S_PERF_IMAGE_DIGEST"]
        source_fingerprint = environment["TRPC_K8S_PERF_SOURCE_FINGERPRINT"]
        run_id = environment["TRPC_K8S_PERF_RUN_ID"]
        evidence_path = Path(
            environment.get("TRPC_K8S_PERF_PREFLIGHT_EVIDENCE_PATH", PREFLIGHT_EVIDENCE_PATH)
        )
        evidence_binding = _preflight_binding(
            evidence_path,
            run_id=run_id,
            source_fingerprint=source_fingerprint,
            image_digest=image_digest,
        )
        secret_values = tuple(
            value
            for key, value in environment.items()
            if key.startswith("TRPC_") and key in SECRET_ENV_NAMES and value
        )
        fixture_command = [
            sys.executable,
            "scripts/performance_fixture.py",
            "create",
            "--execute",
            "--allow-remote",
            "--output",
            str(fixture_path),
        ]
        if _run_child(fixture_command, environment, timeout) != 0:
            fixture_report = _read_partial_fixture_report(fixture_path)
            reasons.append("performance fixture creation failed")
        else:
            fixture_value = json.loads(fixture_path.read_text(encoding="utf-8"))
            if not isinstance(fixture_value, Mapping) or fixture_value.get("gate") != "pass":
                reasons.append("performance fixture contract failed")
            else:
                fixture_report = fixture_value
                environment.update(
                    {
                        "TRPC_REAL_TENANT_ID": str(fixture_value.get("tenant_id", "")),
                        "TRPC_REAL_BINDING_ID": str(fixture_value.get("binding_id", "")),
                    }
                )
                command = _validate_gate_command(
                    _strict_command_from_env("TRPC_K8S_PERF_GATE_COMMAND"),
                    namespace=environment.get("TRPC_K8S_PERF_NAMESPACE", ""),
                    context=environment.get("TRPC_K8S_PERF_CONTEXT", ""),
                    source_fingerprint=source_fingerprint,
                    image_digest=image_digest,
                    evidence_path=str(evidence_path),
                )
                parent_pid, sentinel = _supervision_parent(timeout)
                environment["TRPC_REAL_PERFORMANCE_PARENT_PID"] = str(parent_pid)
                if _run_child(command, environment, timeout) != 0:
                    reasons.append("performance gate command exited with a non-zero status")
                if gate_path.exists():
                    value = json.loads(gate_path.read_text(encoding="utf-8"))
                    if isinstance(value, Mapping):
                        gate_report = value
                        try:
                            _validate_gate_report_identity(
                                gate_report,
                                run_id=run_id,
                                source_fingerprint=source_fingerprint,
                                image_digest=image_digest,
                            )
                        except PerformanceJobError as error:
                            reasons.append(str(error))
                    else:
                        reasons.append("performance gate report root is not an object")
                else:
                    reasons.append("performance gate command did not produce a report")
    except (OSError, ValueError, TypeError, PerformanceJobError) as error:
        reasons.append(
            str(error) if isinstance(error, PerformanceJobError) else "performance worker failed"
        )
    finally:
        if fixture_report is not None:
            try:
                cleanup_environment = environment
                cleanup_command = [
                    sys.executable,
                    "scripts/performance_fixture.py",
                    "cleanup",
                    "--execute",
                    "--allow-remote",
                    "--report",
                    str(fixture_path),
                    "--tenant-id",
                    str(fixture_report.get("tenant_id", "")),
                    "--run-id",
                    str(fixture_report.get("run_id", "")),
                    "--output",
                    str(cleanup_path),
                ]
                cleanup_returncode = _run_child(cleanup_command, cleanup_environment, timeout)
                if cleanup_path.exists():
                    value = json.loads(cleanup_path.read_text(encoding="utf-8"))
                    if isinstance(value, Mapping):
                        cleanup_report = value
                    else:
                        reasons.append("performance cleanup report root is not an object")
                if cleanup_returncode != 0:
                    reasons.append("performance fixture cleanup failed")
                elif cleanup_report is None:
                    reasons.append("performance fixture cleanup report is missing")
            except (OSError, ValueError, TypeError, PerformanceJobError):
                reasons.append("performance fixture cleanup failed")
        if sentinel is not None:
            try:
                sentinel.kill()
                sentinel.wait(timeout=5)
            except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
                reasons.append("performance gate supervision cleanup failed")
    if gate_report is not None and gate_report.get("production_gate") != "pass":
        reasons.append("performance gate production requirement failed")
    status = (
        "pass"
        if gate_report is not None
        and gate_report.get("gate") == "pass"
        and gate_report.get("production_gate") == "pass"
        and not reasons
        else "fail"
    )
    payload = _worker_report(
        status=status,
        gate_report=gate_report,
        fixture_report=fixture_report,
        cleanup_report=cleanup_report,
        reasons=reasons,
        evidence_binding=evidence_binding,
        secrets=secret_values,
    )
    print(REPORT_MARKER + _safe_json(payload), flush=True)
    return 0 if status == "pass" else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help="run inside the Kubernetes Job")
    parser.add_argument("--namespace", required=False)
    parser.add_argument("--context", required=False)
    parser.add_argument("--kubeconfig", type=Path, required=False)
    parser.add_argument("--image", required=False)
    parser.add_argument("--image-pull-secret", required=False)
    parser.add_argument("--source-fingerprint", required=False)
    parser.add_argument("--output", type=Path, required=False)
    parser.add_argument("--gateway-service", required=False)
    parser.add_argument("--gateway-port", type=int, required=False)
    parser.add_argument("--gate-command", required=False)
    parser.add_argument("--secret-manifest", type=Path, required=False)
    parser.add_argument("--node-selector-key", default=NODE_SELECTOR_KEY)
    parser.add_argument("--node-selector-value", default=NODE_SELECTOR_VALUE)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_JOB_TIMEOUT_SECONDS)
    parser.add_argument("--preflight-evidence", type=Path, required=False)
    parser.add_argument("--worker-token", required=False)
    return parser


def _manifest_secret_values(path: Path) -> dict[str, str]:
    """Read only the allowlisted Secret keys from the operator manifest."""

    try:
        import yaml

        documents = yaml.safe_load_all(path.read_text(encoding="utf-8"))
        found: dict[str, str] = {}
        for document in documents:
            if not isinstance(document, Mapping) or document.get("kind") != "Secret":
                continue
            metadata = document.get("metadata")
            if not isinstance(metadata, Mapping) or metadata.get("name") != "trpc-service-secrets":
                continue
            data = document.get("data")
            if isinstance(data, Mapping):
                for key in SECRET_ENV_NAMES:
                    value = data.get(key)
                    if isinstance(value, str):
                        found[key] = base64.b64decode(value, validate=True).decode("utf-8")
        return found
    except (OSError, ValueError, UnicodeError, TypeError) as error:
        raise PerformanceJobError("performance Secret manifest is unavailable") from error


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.worker:
        return worker_main()
    required = {
        "namespace": args.namespace,
        "context": args.context,
        "kubeconfig": args.kubeconfig,
        "image": args.image,
        "image_pull_secret": args.image_pull_secret,
        "source_fingerprint": args.source_fingerprint,
        "output": args.output,
        "secret_manifest": args.secret_manifest,
        "gate_command": args.gate_command,
        "preflight_evidence": args.preflight_evidence,
        "worker_token": args.worker_token,
    }
    missing = [name for name, value in required.items() if value in (None, "")]
    if missing:
        raise SystemExit("missing required performance Job option: " + ", ".join(missing))
    try:
        secret_env = _manifest_secret_values(args.secret_manifest)
        preflight_evidence = json.loads(args.preflight_evidence.read_text(encoding="utf-8"))
        if not isinstance(preflight_evidence, Mapping):
            raise ValueError("performance Job preflight evidence must be an object")
        secret_env[WORKER_TOKEN_ENV] = args.worker_token
        config_env = {
            "TRPC_PERF_GATEWAY_BASE_URL": (
                f"http://{args.gateway_service}.{args.namespace}.svc.cluster.local:{args.gateway_port}"
            ),
            "TRPC_PERF_FIXTURE_CONFIRM": "I_UNDERSTAND_PERFORMANCE_FIXTURE",
            "TRPC_PERF_FIXTURE_REMOTE_CONFIRM": "I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE",
            "TRPC_REAL_PERFORMANCE_CONFIRM": "I_UNDERSTAND_REAL_LOAD",
            "TRPC_RUN_REAL_MULTINODE": "1",
            "TRPC_REAL_IMAGE_DIGEST": args.image.rsplit("@", 1)[1].lower(),
            "TRPC_PERF_K8S_LOAD_JOB": "1",
            "TRPC_K8S_PERF_GATE_COMMAND": args.gate_command,
            "TRPC_K8S_PERF_RUN_ID": str(preflight_evidence.get("run_id", "")),
            "TRPC_K8S_PERF_NAMESPACE": args.namespace,
            "TRPC_K8S_PERF_CONTEXT": args.context,
            "TRPC_K8S_PERF_TIMEOUT_SECONDS": str(args.timeout_seconds),
            "TRPC_K8S_PERF_REPORT_PATH": "/tmp/trpc-performance-report.json",  # noqa: S108
            "TRPC_K8S_PERF_FIXTURE_PATH": "/tmp/trpc-performance-fixture.json",  # noqa: S108
            "TRPC_K8S_PERF_CLEANUP_PATH": "/tmp/trpc-performance-cleanup.json",  # noqa: S108
            "TRPC_K8S_PERF_PREFLIGHT_EVIDENCE_PATH": PREFLIGHT_EVIDENCE_PATH,
        }
        spec = build_spec(
            namespace=args.namespace,
            context=args.context,
            kubeconfig=args.kubeconfig,
            image=args.image,
            image_pull_secret=args.image_pull_secret,
            source_fingerprint=args.source_fingerprint,
            secret_env=secret_env,
            config_env=config_env,
            command=(sys.executable, "scripts/kubernetes_performance_job.py", "--worker"),
            run_id=str(preflight_evidence.get("run_id", "")),
            preflight_evidence=preflight_evidence,
            timeout_seconds=args.timeout_seconds,
        )
        result = run_performance_job(spec, args.output)
    except (OSError, ValueError, PerformanceJobError) as error:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "fail",
            "report": None,
            "rejection_reasons": [str(error)],
        }
        _write_json(args.output, payload)
        return 1
    print(_safe_json(result))
    return 0 if result.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
