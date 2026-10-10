#!/usr/bin/env python3
"""Run the live Kubernetes gate against an isolated kind-compatible cluster.

The helper creates only temporary, randomly generated test credentials and a
namespaced PostgreSQL/Redis prerequisite manifest.  The underlying runtime
gate owns namespace creation and cleanup; no production credential is read.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# Keep the documented ``python scripts/kind_runtime_gate.py`` form working as
# well as ``python -m scripts.kind_runtime_gate``.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import source_fingerprint
from scripts.kubernetes_runtime_gate import _report, _run_live
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
IMAGE_SOURCE_FINGERPRINT_LABEL = "io.trpc.agent-service.source-fingerprint"
METRICS_SERVER_VERSION = "v0.9.0"
METRICS_SERVER_MANIFEST_URL = (
    "https://github.com/kubernetes-sigs/metrics-server/releases/download/"
    f"{METRICS_SERVER_VERSION}/components.yaml"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KIND_CLUSTER_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_KIND_NODE_RESTART_POLICY = "no"
_KIND_NODE_MIN_NANO_CPUS = 2_000_000_000
_KIND_NODE_MIN_MEMORY_BYTES = 4 * 1024 * 1024 * 1024
_KIND_NODE_MIN_PIDS = 768


def _secret_value(bytes_count: int) -> str:
    return secrets.token_hex(bytes_count)


def _image_source_contract(
    initial: Mapping[str, Any] | None,
    upgrade: Mapping[str, Any] | None,
    *,
    image: str,
    upgrade_image: str,
    current_source_fingerprint: str,
    upgrade_source_fingerprint: str | None = None,
) -> tuple[bool, tuple[str, ...]]:
    """Validate local image provenance without trusting image names or tags."""

    reasons: list[str] = []
    if _SHA256_RE.fullmatch(current_source_fingerprint) is None:
        reasons.append("current checkout source fingerprint is unavailable")
    if not image.strip() or not upgrade_image.strip():
        reasons.append("runtime image references must not be empty")
    elif image == upgrade_image:
        reasons.append("initial and upgrade image references must differ")

    expected_upgrade = upgrade_source_fingerprint or current_source_fingerprint
    if _SHA256_RE.fullmatch(expected_upgrade) is None:
        reasons.append("upgrade image source fingerprint is invalid")

    for role, metadata, expected in (
        ("initial", initial, current_source_fingerprint),
        ("upgrade", upgrade, expected_upgrade),
    ):
        if not isinstance(metadata, Mapping):
            reasons.append(f"{role} image could not be inspected locally")
            continue
        image_id = metadata.get("Id")
        if not isinstance(image_id, str) or not image_id.strip():
            reasons.append(f"{role} image has no immutable image ID")
        config = metadata.get("Config")
        labels = config.get("Labels") if isinstance(config, Mapping) else None
        label = labels.get(IMAGE_SOURCE_FINGERPRINT_LABEL) if isinstance(labels, Mapping) else None
        if not isinstance(label, str) or _SHA256_RE.fullmatch(label) is None:
            reasons.append(f"{role} image source fingerprint label is missing or invalid")
        elif label != expected:
            reasons.append(f"{role} image source fingerprint does not match its expected checkout")

    return not reasons, tuple(dict.fromkeys(reasons))


def _hpa_metrics_contract(
    hpa: Mapping[str, Any] | None, *, metrics_api_available: bool
) -> tuple[bool, tuple[str, ...]]:
    """Require actual metrics-server evidence in addition to HPA readiness."""

    reasons: list[str] = []
    if not metrics_api_available:
        reasons.append("metrics-server API is unavailable")
    if not isinstance(hpa, Mapping):
        reasons.append("worker HPA evidence is unavailable")
        return False, tuple(dict.fromkeys(reasons))
    conditions = hpa.get("conditions")
    if not isinstance(conditions, Mapping) or conditions.get("AbleToScale") != "True":
        reasons.append("worker HPA did not expose AbleToScale=True")
    if not isinstance(conditions, Mapping) or conditions.get("ScalingActive") != "True":
        reasons.append("worker HPA did not expose ScalingActive=True")
    for key in ("current_replicas", "desired_replicas"):
        value = hpa.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 2:
            reasons.append(f"worker HPA {key} is below the configured minimum")
    return not reasons, tuple(dict.fromkeys(reasons))


def _inspect_local_image(image: str) -> tuple[Mapping[str, Any] | None, str | None]:
    """Read local Docker metadata; never include command output in reports."""

    executable = shutil.which("docker")
    if executable is None:
        return None, "docker is not installed"
    try:
        result = subprocess.run(  # noqa: S603 - executable and args are explicit
            [executable, "image", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return None, "docker image inspection timed out"
    if result.returncode != 0:
        return None, "docker image inspection failed"
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None, "docker image inspection returned invalid JSON"
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], Mapping):
        return None, "docker image inspection returned no image metadata"
    return payload[0], None


def _current_source_fingerprint() -> tuple[str | None, str | None]:
    result = source_fingerprint(ROOT)
    value = result.get("value")
    if (
        result.get("status") == "available"
        and isinstance(value, str)
        and _SHA256_RE.fullmatch(value)
    ):
        return value, None
    return None, "current checkout source fingerprint is unavailable"


def _metrics_server_evidence(
    *, context: str | None, timeout_seconds: float
) -> tuple[bool, dict[str, Any], str | None]:
    """Probe the metrics API and require at least one node sample."""

    executable = shutil.which("kubectl")
    evidence: dict[str, Any] = {"status": "not_run", "api": "metrics.k8s.io/v1beta1"}
    if executable is None:
        return False, evidence, "kubectl is not installed"
    command = [executable]
    if context:
        command.extend(["--context", context])
    command.extend(["get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes"])
    try:
        result = subprocess.run(  # noqa: S603 - executable and args are explicit
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=min(timeout_seconds, 30),
        )
    except subprocess.TimeoutExpired:
        return False, evidence, "metrics-server API probe timed out"
    if result.returncode != 0:
        return False, evidence, "metrics-server API probe failed"
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False, evidence, "metrics-server API returned invalid JSON"
    items = payload.get("items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list) or not items:
        return False, evidence, "metrics-server returned no node samples"
    evidence.update({"status": "pass", "node_samples": len(items)})
    return True, evidence, None


def _wait_for_metrics_server(
    *, context: str | None, timeout_seconds: float
) -> tuple[bool, dict[str, Any], str | None]:
    """Allow a freshly rolled metrics-server time to publish its first sample."""

    deadline = time.monotonic() + max(1.0, min(timeout_seconds, 120.0))
    last_evidence: dict[str, Any] = {"status": "not_run", "api": "metrics.k8s.io/v1beta1"}
    last_reason: str | None = "metrics-server API is unavailable"
    while time.monotonic() < deadline:
        ok, evidence, reason = _metrics_server_evidence(
            context=context,
            timeout_seconds=min(timeout_seconds, 30.0),
        )
        last_evidence = evidence
        last_reason = reason
        if ok:
            return True, evidence, None
        time.sleep(2)
    return False, last_evidence, last_reason


def _kind_context_contract(context: str | None) -> bool:
    """Only permit the installer to mutate an explicitly kind-named context."""

    return isinstance(context, str) and context.startswith("kind-") and len(context) > len("kind-")


def _metrics_server_patch_operations(
    deployment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], ...] | None:
    """Build an idempotent kind-only patch for the kubelet certificate mode."""

    spec = deployment.get("spec") if isinstance(deployment, Mapping) else None
    template = spec.get("template") if isinstance(spec, Mapping) else None
    pod_spec = template.get("spec") if isinstance(template, Mapping) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, Mapping) else None
    if not isinstance(containers, list):
        return None
    for index, container in enumerate(containers):
        if not isinstance(container, Mapping) or container.get("name") != "metrics-server":
            continue
        args = container.get("args")
        path = f"/spec/template/spec/containers/{index}/args"
        if isinstance(args, list):
            if "--kubelet-insecure-tls" in args:
                return ()
            return ({"op": "add", "path": f"{path}/-", "value": "--kubelet-insecure-tls"},)
        if args is None:
            return ({"op": "add", "path": path, "value": ["--kubelet-insecure-tls"]},)
        return None
    return None


def _run_kubectl(
    context: str,
    arguments: list[str],
    *,
    timeout_seconds: float,
) -> tuple[int, str]:
    """Run a bounded kubectl command and discard stderr/output on failure."""

    executable = shutil.which("kubectl")
    if executable is None:
        return 127, ""
    command = [executable, "--context", context, *arguments]
    try:
        result = subprocess.run(  # noqa: S603 - executable and args are explicit
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return 124, ""
    return result.returncode, result.stdout


def _run_docker(
    arguments: list[str],
    *,
    timeout_seconds: float,
) -> tuple[int, str]:
    """Run a bounded read-only Docker CLI command for Kind node discovery."""

    executable = shutil.which("docker")
    if executable is None:
        return 127, ""
    try:
        result = subprocess.run(  # noqa: S603 - executable and args are explicit
            [executable, *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return 124, ""
    return result.returncode, result.stdout


def _kind_cluster_name(context: str | None) -> str | None:
    """Return the exact Kind cluster name encoded by a guarded context."""

    if not _kind_context_contract(context):
        return None
    assert context is not None
    cluster_name = context[len("kind-") :]
    if _KIND_CLUSTER_NAME_RE.fullmatch(cluster_name) is None:
        return None
    return cluster_name


def _kind_node_safety_contract(
    *,
    context: str | None,
    timeout_seconds: float,
) -> tuple[bool, dict[str, Any], str | None]:
    """Resolve one Kind context to its node containers and require hard limits.

    The Kubernetes context and Docker label are checked together.  A node is
    accepted only when it is running, has Docker restart policy ``no``, and
    has non-zero CPU, memory, and PID limits.  The exact node-name sets must
    match; a same-prefix or stale container is never silently accepted.
    """

    evidence: dict[str, Any] = {
        "status": "not_run",
        "scope": "kind-only",
        "context": context,
        "required_restart_policy": _KIND_NODE_RESTART_POLICY,
        "required_limits": {
            "cpu_nano_cpus_minimum": _KIND_NODE_MIN_NANO_CPUS,
            "memory_bytes_minimum": _KIND_NODE_MIN_MEMORY_BYTES,
            "memory_swap_must_equal_memory": True,
            "pids_limit_minimum": _KIND_NODE_MIN_PIDS,
        },
    }
    cluster_name = _kind_cluster_name(context)
    if cluster_name is None:
        return False, evidence, "node safety requires a valid kind-* context"
    assert context is not None
    evidence["cluster_name"] = cluster_name

    node_code, node_stdout = _run_kubectl(
        context,
        ["get", "nodes", "-o", "json"],
        timeout_seconds=min(timeout_seconds, 30.0),
    )
    if node_code != 0:
        return False, evidence, "kind context nodes could not be inspected"
    try:
        node_payload = json.loads(node_stdout)
    except json.JSONDecodeError:
        return False, evidence, "kind context returned invalid node JSON"
    node_items = node_payload.get("items") if isinstance(node_payload, Mapping) else None
    if not isinstance(node_items, list) or not node_items:
        return False, evidence, "kind context returned no nodes"
    node_names: list[str] = []
    for item in node_items:
        metadata = item.get("metadata") if isinstance(item, Mapping) else None
        name = metadata.get("name") if isinstance(metadata, Mapping) else None
        if not isinstance(name, str) or not name.strip():
            return False, evidence, "kind context returned a node without a name"
        node_names.append(name)
    if len(set(node_names)) != len(node_names):
        return False, evidence, "kind context returned duplicate node names"
    expected_nodes = set(node_names)
    evidence["kubernetes_nodes"] = sorted(expected_nodes)

    ids_code, ids_stdout = _run_docker(
        [
            "ps",
            "-aq",
            "--filter",
            f"label=io.x-k8s.kind.cluster={cluster_name}",
        ],
        timeout_seconds=min(timeout_seconds, 30.0),
    )
    if ids_code != 0:
        return False, evidence, "Kind node containers could not be listed"
    container_ids = tuple(line.strip() for line in ids_stdout.splitlines() if line.strip())
    if not container_ids:
        return False, evidence, "Kind context has no matching node containers"

    inspect_code, inspect_stdout = _run_docker(
        ["inspect", *container_ids],
        timeout_seconds=min(timeout_seconds, 30.0),
    )
    if inspect_code != 0:
        return False, evidence, "Kind node containers could not be inspected"
    try:
        inspected = json.loads(inspect_stdout)
    except json.JSONDecodeError:
        return False, evidence, "Docker returned invalid Kind node JSON"
    if not isinstance(inspected, list) or not inspected:
        return False, evidence, "Docker returned no Kind node metadata"

    discovered: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []
    for container in inspected:
        if not isinstance(container, Mapping):
            reasons.append("Docker returned an invalid Kind node record")
            continue
        config = container.get("Config")
        labels = config.get("Labels") if isinstance(config, Mapping) else None
        if not isinstance(labels, Mapping):
            reasons.append("Kind node container has no identifying labels")
            continue
        if labels.get("io.x-k8s.kind.cluster") != cluster_name:
            reasons.append("Kind node container cluster label does not match context")
            continue
        if labels.get("io.x-k8s.kind.role") not in {"control-plane", "worker"}:
            reasons.append("Kind node container role is not a node role")
            continue
        raw_name = container.get("Name")
        container_name = raw_name.lstrip("/") if isinstance(raw_name, str) else None
        hostname = config.get("Hostname") if isinstance(config, Mapping) else None
        candidates = tuple(
            value
            for value in (hostname, container_name)
            if isinstance(value, str) and value.strip()
        )
        name = next((value for value in candidates if value in expected_nodes), None)
        if not isinstance(name, str) or name not in expected_nodes:
            reasons.append("Docker Kind node name does not match the context node set")
            continue
        if name in discovered:
            reasons.append(f"Docker returned duplicate metadata for Kind node {name}")
            continue

        state = container.get("State")
        host_config = container.get("HostConfig")
        restart_policy = (
            host_config.get("RestartPolicy") if isinstance(host_config, Mapping) else None
        )
        restart_name = restart_policy.get("Name") if isinstance(restart_policy, Mapping) else None
        nano_cpus = host_config.get("NanoCpus") if isinstance(host_config, Mapping) else None
        memory_bytes = host_config.get("Memory") if isinstance(host_config, Mapping) else None
        memory_swap = host_config.get("MemorySwap") if isinstance(host_config, Mapping) else None
        pids_limit = host_config.get("PidsLimit") if isinstance(host_config, Mapping) else None
        detail = {
            "name": name,
            "role": labels.get("io.x-k8s.kind.role"),
            "running": bool(state.get("Running")) if isinstance(state, Mapping) else False,
            "restart_policy": restart_name,
            "nano_cpus": nano_cpus,
            "memory_bytes": memory_bytes,
            "memory_swap_bytes": memory_swap,
            "pids_limit": pids_limit,
        }
        discovered[name] = detail
        if detail["running"] is not True:
            reasons.append(f"Kind node {name} is not running")
        if restart_name != _KIND_NODE_RESTART_POLICY:
            reasons.append(f"Kind node {name} restart policy is not no")
        for field, minimum, label in (
            (nano_cpus, _KIND_NODE_MIN_NANO_CPUS, "CPU"),
            (memory_bytes, _KIND_NODE_MIN_MEMORY_BYTES, "memory"),
            (pids_limit, _KIND_NODE_MIN_PIDS, "PIDs"),
        ):
            if isinstance(field, bool) or not isinstance(field, int) or field < minimum:
                reasons.append(f"Kind node {name} is below the safe {label} limit")
        if memory_swap != memory_bytes:
            reasons.append(f"Kind node {name} swap limit must equal its memory limit")

    missing_nodes = sorted(expected_nodes - set(discovered))
    extra_nodes = sorted(set(discovered) - expected_nodes)
    if missing_nodes:
        reasons.append(f"Kind context nodes missing Docker matches: {', '.join(missing_nodes)}")
    if extra_nodes:
        reasons.append(f"Docker has nodes outside the context node set: {', '.join(extra_nodes)}")
    role_counts = {
        role: sum(1 for detail in discovered.values() if detail.get("role") == role)
        for role in ("control-plane", "worker")
    }
    evidence["role_counts"] = role_counts
    if role_counts["control-plane"] < 1:
        reasons.append("Kind safety requires at least one control-plane node")
    if role_counts["worker"] < 2:
        reasons.append("Kind eviction safety requires at least two worker nodes")
    evidence["docker_nodes"] = [discovered[name] for name in sorted(discovered)]
    if reasons:
        evidence["status"] = "fail"
        return False, evidence, "; ".join(dict.fromkeys(reasons))
    evidence["status"] = "pass"
    return True, evidence, None


def _install_kind_metrics_server(
    *,
    context: str | None,
    timeout_seconds: float,
    manifest_url: str = METRICS_SERVER_MANIFEST_URL,
) -> tuple[bool, dict[str, Any], str | None]:
    """Install the pinned metrics-server only in a dedicated kind context.

    kind nodes use self-signed kubelet serving certificates, so the ephemeral
    test installation receives ``--kubelet-insecure-tls``.  This function never
    touches the production Kustomize overlay; the context guard is fail-closed.
    """

    evidence: dict[str, Any] = {
        "status": "not_run",
        "scope": "kind-only",
        "version": METRICS_SERVER_VERSION,
    }
    if not _kind_context_contract(context):
        return False, evidence, "metrics-server installation requires a kind-* context"
    assert context is not None
    apply_code, _ = _run_kubectl(
        context,
        ["apply", "--server-side", "--field-manager=trpc-kind-runtime-gate", "-f", manifest_url],
        timeout_seconds=timeout_seconds,
    )
    if apply_code != 0:
        return False, evidence, "pinned metrics-server manifest could not be applied"

    deployment: Mapping[str, Any] | None = None
    deadline = time.monotonic() + max(1.0, min(timeout_seconds, 60.0))
    while time.monotonic() < deadline:
        get_code, get_stdout = _run_kubectl(
            context,
            ["get", "deployment", "metrics-server", "--namespace", "kube-system", "-o", "json"],
            timeout_seconds=min(timeout_seconds, 30.0),
        )
        if get_code == 0:
            try:
                payload = json.loads(get_stdout)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, Mapping):
                deployment = payload
                break
        time.sleep(1)
    patch = _metrics_server_patch_operations(deployment)
    if patch is None:
        return False, evidence, "metrics-server deployment could not be inspected"
    if patch:
        patch_code, _ = _run_kubectl(
            context,
            [
                "patch",
                "deployment",
                "metrics-server",
                "--namespace",
                "kube-system",
                "--type=json",
                "--patch",
                json.dumps(list(patch), separators=(",", ":")),
            ],
            timeout_seconds=timeout_seconds,
        )
        if patch_code != 0:
            return False, evidence, "kind metrics-server kubelet TLS patch failed"

    rollout_code, _ = _run_kubectl(
        context,
        [
            "rollout",
            "status",
            "deployment/metrics-server",
            "--namespace",
            "kube-system",
            f"--timeout={int(timeout_seconds)}s",
        ],
        timeout_seconds=timeout_seconds + 5,
    )
    if rollout_code != 0:
        return False, evidence, "metrics-server rollout did not become ready"
    evidence.update(
        {
            "status": "pass",
            "manifest": manifest_url,
            "kubelet_tls_mode": "insecure-for-ephemeral-kind-only",
        }
    )
    return True, evidence, None


def _write_preflight_report(
    output: Path,
    *,
    context: str | None,
    checks: Mapping[str, Any],
    reasons: list[str],
) -> None:
    _report(
        output,
        gate="not_run",
        candidate={"mode": "kind_preflight", "context": context, "checks": dict(checks)},
        rejection_reasons=reasons,
    )


def _augment_runtime_report(
    output: Path,
    *,
    image_evidence: Mapping[str, Any],
    metrics_install_evidence: Mapping[str, Any],
    metrics_evidence: Mapping[str, Any],
    metrics_api_available: bool,
    run_code: int,
) -> int:
    """Attach preflight evidence and fail closed if HPA metrics are inactive."""

    try:
        report = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 1
    candidate = report.setdefault("candidate", {})
    checks = candidate.setdefault("checks", {})
    checks["image_attestation"] = dict(image_evidence)
    checks["metrics_server_install"] = dict(metrics_install_evidence)
    checks["metrics_server"] = dict(metrics_evidence)
    if report.get("gate") == "pass":
        hpa_ok, hpa_reasons = _hpa_metrics_contract(
            checks.get("worker_scale_and_hpa"),
            metrics_api_available=metrics_api_available,
        )
        if not hpa_ok:
            report["gate"] = "fail"
            report["production_gate"] = "fail"
            report.setdefault("rejection_reasons", []).extend(hpa_reasons)
            report.setdefault("production_rejection_reasons", []).extend(hpa_reasons)
            checks["metrics_server"]["status"] = "fail"
    # kind is an explicitly non-production, ephemeral controller check.  It
    # may prove local rollout mechanics, never the production Kubernetes gate.
    report["production_gate"] = "not_run"
    report.setdefault("production_rejection_reasons", []).append(
        "kind runtime evidence is non-production and cannot satisfy the production cluster gate"
    )
    report["case_deltas"] = {
        **report.get("case_deltas", {}),
        "failed_checks": sum(
            1
            for value in checks.values()
            if isinstance(value, Mapping) and value.get("status") == "fail"
        ),
        "not_run_checks": sum(
            1
            for value in checks.values()
            if isinstance(value, Mapping) and value.get("status") == "not_run"
        ),
    }
    atomic_write_json(output, report)
    return 0 if report.get("gate") == "pass" and run_code == 0 else 1


def _prerequisite_manifest() -> dict[str, Any]:
    migration_password = _secret_value(18)
    runtime_password = _secret_value(18)
    worker_password = _secret_value(18)
    redis_password = _secret_value(18)
    session_hmac = _secret_value(24)
    emergency_key = _secret_value(16)
    s3_secret = _secret_value(18)
    init_sql = (
        """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
    CREATE ROLE trpc_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS
      PASSWORD '__RUNTIME_PASSWORD__';
  ELSE
    ALTER ROLE trpc_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS
      PASSWORD '__RUNTIME_PASSWORD__';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker') THEN
    CREATE ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS
      PASSWORD '__WORKER_PASSWORD__';
  ELSE
    ALTER ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS
      PASSWORD '__WORKER_PASSWORD__';
  END IF;
END $$;
GRANT CONNECT ON DATABASE trpc_service TO trpc_runtime;
GRANT CONNECT ON DATABASE trpc_service TO trpc_worker;
""".replace("__RUNTIME_PASSWORD__", runtime_password)
        .replace("__WORKER_PASSWORD__", worker_password)
        .strip()
    )

    dependency_labels = {"app.kubernetes.io/component": "runtime-gate-dependency"}
    postgres_labels = {**dependency_labels, "app.kubernetes.io/name": "trpc-gate-postgres"}
    redis_labels = {**dependency_labels, "app.kubernetes.io/name": "trpc-gate-redis"}
    items: list[dict[str, Any]] = [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "trpc-service-secrets"},
            "type": "Opaque",
            "stringData": {
                "TRPC_SERVICE_DATABASE_DSN": (
                    f"postgresql://trpc_runtime:{runtime_password}@postgres:5432/trpc_service"
                ),
                "TRPC_SERVICE_REDIS_URL": f"redis://:{redis_password}@redis:6379/0",
                "TRPC_SERVICE_SESSION_HMAC_KEY": session_hmac,
                "TRPC_SERVICE_EMERGENCY_QUEUE_KEY": emergency_key,
                "TRPC_SERVICE_DEVELOPMENT_TOKEN": _secret_value(18),
                "TRPC_SERVICE_S3_ACCESS_KEY": "runtime-gate-access",
                "TRPC_SERVICE_S3_SECRET_KEY_REF": "env://TRPC_SERVICE_S3_SECRET_KEY",
                "TRPC_SERVICE_S3_SECRET_KEY": s3_secret,
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "trpc-migration-secrets"},
            "type": "Opaque",
            "stringData": {
                "TRPC_SERVICE_DATABASE_DSN": (
                    f"postgresql://trpc_migration:{migration_password}@postgres:5432/trpc_service"
                )
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "trpc-worker-secrets"},
            "type": "Opaque",
            "stringData": {
                "TRPC_SERVICE_WORKER_DATABASE_DSN_REF": "env://TRPC_SERVICE_WORKER_DATABASE_DSN",
                "TRPC_SERVICE_WORKER_DATABASE_DSN": (
                    f"postgresql://trpc_worker:{worker_password}@postgres:5432/trpc_service"
                ),
                "TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF": (
                    "env://TRPC_SERVICE_WORKER_DATABASE_PASSWORD"
                ),
                "TRPC_SERVICE_WORKER_DATABASE_PASSWORD": worker_password,
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "trpc-gate-dependency-secrets"},
            "type": "Opaque",
            "stringData": {
                "POSTGRES_PASSWORD": migration_password,
                "REDIS_PASSWORD": redis_password,
            },
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "trpc-gate-postgres-init"},
            "data": {"001-runtime-role.sql": init_sql},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "postgres"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": postgres_labels},
                "template": {
                    "metadata": {"labels": postgres_labels},
                    "spec": {
                        "nodeSelector": {"node-role.kubernetes.io/control-plane": ""},
                        "tolerations": [
                            {
                                "key": "node-role.kubernetes.io/control-plane",
                                "operator": "Exists",
                                "effect": "NoSchedule",
                            }
                        ],
                        "containers": [
                            {
                                "name": "postgres",
                                "image": "pgvector/pgvector:pg16",
                                "env": [
                                    {"name": "POSTGRES_DB", "value": "trpc_service"},
                                    {"name": "POSTGRES_USER", "value": "trpc_migration"},
                                    {
                                        "name": "POSTGRES_PASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "trpc-gate-dependency-secrets",
                                                "key": "POSTGRES_PASSWORD",
                                            }
                                        },
                                    },
                                ],
                                "ports": [{"name": "postgres", "containerPort": 5432}],
                                "resources": {
                                    "requests": {"cpu": "100m", "memory": "256Mi"},
                                    "limits": {"cpu": "500m", "memory": "1Gi"},
                                },
                                "readinessProbe": {
                                    "exec": {
                                        "command": [
                                            "pg_isready",
                                            "-U",
                                            "trpc_migration",
                                            "-d",
                                            "trpc_service",
                                        ]
                                    },
                                    "periodSeconds": 2,
                                    "failureThreshold": 30,
                                },
                                "volumeMounts": [
                                    {
                                        "name": "init",
                                        "mountPath": "/docker-entrypoint-initdb.d",
                                        "readOnly": True,
                                    }
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "init", "configMap": {"name": "trpc-gate-postgres-init"}}
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "postgres"},
            "spec": {
                "selector": postgres_labels,
                "ports": [{"name": "postgres", "port": 5432, "targetPort": "postgres"}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "redis"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": redis_labels},
                "template": {
                    "metadata": {"labels": redis_labels},
                    "spec": {
                        "nodeSelector": {"node-role.kubernetes.io/control-plane": ""},
                        "tolerations": [
                            {
                                "key": "node-role.kubernetes.io/control-plane",
                                "operator": "Exists",
                                "effect": "NoSchedule",
                            }
                        ],
                        "containers": [
                            {
                                "name": "redis",
                                "image": "redis:7.4-alpine",
                                "command": ["/bin/sh", "-c"],
                                "args": [
                                    'exec redis-server --save "" --appendonly no '
                                    '--requirepass "$REDIS_PASSWORD"'
                                ],
                                "env": [
                                    {
                                        "name": "REDIS_PASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "trpc-gate-dependency-secrets",
                                                "key": "REDIS_PASSWORD",
                                            }
                                        },
                                    }
                                ],
                                "ports": [{"name": "redis", "containerPort": 6379}],
                                "resources": {
                                    "requests": {"cpu": "50m", "memory": "64Mi"},
                                    "limits": {"cpu": "250m", "memory": "256Mi"},
                                },
                                "readinessProbe": {
                                    "exec": {
                                        "command": [
                                            "/bin/sh",
                                            "-c",
                                            'redis-cli -a "$REDIS_PASSWORD" ping | grep -q PONG',
                                        ]
                                    },
                                    "periodSeconds": 2,
                                    "failureThreshold": 30,
                                },
                            }
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "redis"},
            "spec": {
                "selector": redis_labels,
                "ports": [{"name": "redis", "port": 6379, "targetPort": "redis"}],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "trpc-gate-allow-dependencies"},
            "spec": {
                "podSelector": {"matchLabels": dependency_labels},
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [{"podSelector": {}}],
                        "ports": [
                            {"port": 5432, "protocol": "TCP"},
                            {"port": 6379, "protocol": "TCP"},
                        ],
                    }
                ],
            },
        },
    ]
    return {"apiVersion": "v1", "kind": "List", "items": items}


@contextmanager
def _temporary_environment(values: Mapping[str, str]) -> Iterator[None]:
    original = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, previous in original.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="kind-trpc-runtime-gate")
    parser.add_argument("--image", default="trpc-agent-service:k8s-gate-a")
    parser.add_argument("--upgrade-image", default="trpc-agent-service:k8s-gate-b")
    parser.add_argument(
        "--source-fingerprint",
        default=None,
        help="expected fingerprint for the initial image; defaults to this checkout",
    )
    parser.add_argument(
        "--upgrade-source-fingerprint",
        default=None,
        help="expected fingerprint for the upgrade image; defaults to this checkout",
    )
    parser.add_argument(
        "--install-metrics-server",
        action="store_true",
        help="install the pinned metrics-server in the kind-* context before probing it",
    )
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/kubernetes-runtime.json")
    )
    args = parser.parse_args()

    current_fingerprint, fingerprint_reason = _current_source_fingerprint()
    source_fingerprint_value = args.source_fingerprint or current_fingerprint
    preflight_checks: dict[str, Any] = {
        "kind_node_safety": {"status": "not_run", "scope": "kind-only"},
        "image_attestation": {"status": "not_run"},
        "metrics_server_install": {"status": "not_run"},
        "metrics_server": {"status": "not_run"},
    }
    if current_fingerprint is None:
        _write_preflight_report(
            args.output,
            context=args.context,
            checks=preflight_checks,
            reasons=[fingerprint_reason or "current checkout source fingerprint is unavailable"],
        )
        return 1
    if args.source_fingerprint is not None and args.source_fingerprint != current_fingerprint:
        _write_preflight_report(
            args.output,
            context=args.context,
            checks=preflight_checks,
            reasons=["supplied source fingerprint does not match the current checkout"],
        )
        return 1

    node_safety_ok, node_safety_evidence, node_safety_reason = _kind_node_safety_contract(
        context=args.context,
        timeout_seconds=args.timeout_seconds,
    )
    preflight_checks["kind_node_safety"] = node_safety_evidence
    if not node_safety_ok:
        _write_preflight_report(
            args.output,
            context=args.context,
            checks=preflight_checks,
            reasons=[node_safety_reason or "Kind node safety contract failed"],
        )
        return 1

    initial_metadata, initial_reason = _inspect_local_image(args.image)
    upgrade_metadata, upgrade_reason = _inspect_local_image(args.upgrade_image)
    image_ok, image_reasons = _image_source_contract(
        initial_metadata,
        upgrade_metadata,
        image=args.image,
        upgrade_image=args.upgrade_image,
        current_source_fingerprint=source_fingerprint_value or current_fingerprint,
        upgrade_source_fingerprint=args.upgrade_source_fingerprint,
    )
    if initial_reason:
        image_reasons = (*image_reasons, f"initial image inspection: {initial_reason}")
        image_ok = False
    if upgrade_reason:
        image_reasons = (*image_reasons, f"upgrade image inspection: {upgrade_reason}")
        image_ok = False
    if not image_ok:
        preflight_checks["image_attestation"] = {
            "status": "fail",
            "source_fingerprint_label": IMAGE_SOURCE_FINGERPRINT_LABEL,
        }
        _write_preflight_report(
            args.output,
            context=args.context,
            checks=preflight_checks,
            reasons=list(dict.fromkeys(image_reasons)),
        )
        return 1

    preflight_checks["image_attestation"] = {
        "status": "pass",
        "source_fingerprint_label": IMAGE_SOURCE_FINGERPRINT_LABEL,
        "current_source_fingerprint": current_fingerprint,
    }
    metrics_install_evidence: dict[str, Any] = {"status": "not_run", "scope": "kind-only"}
    if args.install_metrics_server:
        install_ok, metrics_install_evidence, install_reason = _install_kind_metrics_server(
            context=args.context,
            timeout_seconds=args.timeout_seconds,
        )
        preflight_checks["metrics_server_install"] = metrics_install_evidence
        if not install_ok:
            _write_preflight_report(
                args.output,
                context=args.context,
                checks=preflight_checks,
                reasons=[install_reason or "kind metrics-server installation failed"],
            )
            return 1
    metrics_ok, metrics_evidence, metrics_reason = _wait_for_metrics_server(
        context=args.context,
        timeout_seconds=args.timeout_seconds,
    )
    if not metrics_ok:
        preflight_checks["metrics_server"] = metrics_evidence
        _write_preflight_report(
            args.output,
            context=args.context,
            checks=preflight_checks,
            reasons=[metrics_reason or "metrics-server API is unavailable"],
        )
        return 1

    with tempfile.TemporaryDirectory(prefix="trpc-kind-prerequisites-") as directory:
        manifest = Path(directory) / "runtime-prerequisites.json"
        manifest.write_text(
            json.dumps(_prerequisite_manifest(), separators=(",", ":")), encoding="utf-8"
        )
        environment = {
            "TRPC_K8S_RUNTIME_TESTS_ENABLED": "true",
            "TRPC_K8S_RUNTIME_IMAGE": args.image,
            "TRPC_K8S_RUNTIME_UPGRADE_IMAGE": args.upgrade_image,
            "TRPC_K8S_RUNTIME_SECRET_MANIFEST": str(manifest),
        }
        with _temporary_environment(environment):
            run_code = _run_live(
                output=args.output,
                context=args.context,
                timeout_seconds=args.timeout_seconds,
                require_runtime=True,
                allow_local_images=True,
            )
    return _augment_runtime_report(
        args.output,
        image_evidence=preflight_checks["image_attestation"],
        metrics_install_evidence=metrics_install_evidence,
        metrics_evidence=metrics_evidence,
        metrics_api_available=metrics_ok,
        run_code=run_code,
    )


if __name__ == "__main__":
    raise SystemExit(main())
