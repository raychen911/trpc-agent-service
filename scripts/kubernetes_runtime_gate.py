#!/usr/bin/env python3
"""Run the opt-in Kubernetes production-runtime acceptance gate.

The command deliberately does not infer a live Kubernetes environment from a
static manifest render.  Without ``TRPC_K8S_RUNTIME_TESTS_ENABLED=true`` it
only writes a ``not_run`` report.  A live run uses a generated namespace,
server-side dry-run, an explicitly supplied image and Secret manifest, and
deletes that namespace in a ``finally`` block.

Required environment for a live run:

* ``TRPC_K8S_RUNTIME_TESTS_ENABLED=true``
* ``TRPC_K8S_RUNTIME_IMAGE`` (the image initially deployed)
* ``TRPC_K8S_RUNTIME_UPGRADE_IMAGE`` (a different image for the rollout test)
* ``TRPC_K8S_RUNTIME_SECRET_MANIFEST`` (a manifest containing the runtime and
  migration Secret objects; it is never copied into the report)
* ``TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET`` (optional, non-sensitive name of a
  ``kubernetes.io/dockerconfigjson`` Secret in the supplied Secret manifest;
  the gate never copies registry credentials into its report)
* ``TRPC_K8S_RUNTIME_HPA_DRIVER`` (an absolute, repository-bound Python
   trigger that creates and clears a bounded backlog; HPA observations are
   always read by this gate through the Kubernetes API)
* ``TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256`` and
  ``TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG`` (the pinned driver digest and a
  dedicated, namespace-scoped kubeconfig that is distinct from the gate's
  administrative kubeconfig)
* ``TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT`` (the dedicated service-account
   username; the gate creates a temporary RoleBinding only in its random
   acceptance namespace)
* ``TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT`` (the explicit context used by the
  dedicated driver kubeconfig; it must not be inferred from the gate context)
* ``TRPC_K8S_RUNTIME_HPA_JOB_COMMAND`` (a JSON argument array implemented by
  the immutable driver image; the bounded Job itself is created by the
  repository driver)
* ``TRPC_K8S_RUNTIME_NODE_NAME``, ``TRPC_K8S_RUNTIME_NODE_LABEL`` and the exact
  ``TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM`` value for an explicitly dedicated
  node cordon/drain/uncordon test
* ``TRPC_RELEASE_ID`` and ``TRPC_RELEASE_NONCE`` for the current release
  binding; the nonce is hashed in the report and never written in plaintext

The default exit code is zero for an unrequested ``not_run`` run, which keeps
local offline checks safe.  ``--require-runtime`` makes missing prerequisites
or an unavailable cluster fail the command, as required by a release job.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import uuid4

import yaml

# Keep the documented ``python scripts/kubernetes_runtime_gate.py`` form
# working as well as ``python -m scripts.kubernetes_runtime_gate``.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.deployment_preflight import build_preflight
from scripts.evidence_lineage import build_evidence, runtime_fingerprint
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
OVERLAY_ROOT = ROOT / "deploy" / "kustomize" / "overlays" / "production"
BASE_ROOT = ROOT / "deploy" / "kustomize" / "base"
PRODUCER = "scripts.kubernetes_runtime_gate"
IMAGE_DIGEST_RE = re.compile(r"sha256:[0-9a-fA-F]{64}")
_IMAGE_REFERENCE_RE = re.compile(r"^(?P<name>[^@\s]+)@(?P<digest>sha256:[0-9a-fA-F]{64})$")
# Kubernetes/CRI implementations expose ``imageID`` in more than one
# equivalent form (for example ``docker-pullable://repo@sha256:...`` or
# ``containerd://sha256:...``).  The release evidence deliberately retains
# only the immutable digest, after validating one of these API shapes.
IMAGE_ID_DIGEST_RE = re.compile(r"(?:^|@|://)(sha256:[0-9a-fA-F]{64})$")
S3_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_KUBERNETES_QUANTITY_RE = re.compile(
    r"^(?P<number>[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"
    r"(?P<suffix>m|Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E)?$"
)
_KUBERNETES_QUANTITY_FACTORS = {
    "m": Decimal("0.001"),
    "k": Decimal("1000"),
    "M": Decimal("1000000"),
    "G": Decimal("1000000000"),
    "T": Decimal("1000000000000"),
    "P": Decimal("1000000000000000"),
    "E": Decimal("1000000000000000000"),
    "Ki": Decimal(1024),
    "Mi": Decimal(1024**2),
    "Gi": Decimal(1024**3),
    "Ti": Decimal(1024**4),
    "Pi": Decimal(1024**5),
    "Ei": Decimal(1024**6),
}
RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
RELEASE_NONCE_RE = re.compile(r"[A-Za-z0-9_-]{32,256}")
MAX_TIMEOUT_SECONDS = 3600.0
HPA_DRIVER_PHASES = frozenset({"load", "clear"})
HPA_DRIVER_MAX_BYTES = 1024 * 1024
_ROLLBACK_PROBE_TIMEOUT_SECONDS = 120.0
_ROLLBACK_PROBE_DEPLOYMENT = "trpc-worker"
HPA_DRIVER_SCRIPT = ROOT / "scripts" / "kubernetes_hpa_load_driver.py"
HPA_DRIVER_OWNER_LABEL = "trpc.io/hpa-gate"
HPA_DRIVER_OWNER_VALUE = "bounded-job-driver"
HPA_DRIVER_RUN_LABEL = "trpc.io/hpa-run"
HPA_DRIVER_PHASE_LABEL = "trpc.io/hpa-phase"
HPA_DRIVER_CLUSTER_LABEL = "trpc.io/hpa-cluster"
SCHEMA_HEAD_CHECK_JOB_NAME = "trpc-schema-head-check"
_HPA_DRIVER_ROLE_NAME = "trpc-hpa-load-driver"
_HPA_DRIVER_IDENTITY_RULES = frozenset(
    {
        ("authorization.k8s.io", "selfsubjectaccessreviews", "create"),
        ("authorization.k8s.io", "selfsubjectrulesreviews", "create"),
        ("authentication.k8s.io", "selfsubjectreviews", "create"),
    }
)
_HPA_DRIVER_DISCOVERY_URLS = frozenset(
    {
        "/.well-known/openid-configuration",
        "/.well-known/openid-configuration/",
        "/api",
        "/api/*",
        "/apis",
        "/apis/*",
        "/healthz",
        "/livez",
        "/openid/v1/jwks",
        "/openid/v1/jwks/",
        "/openapi",
        "/openapi/*",
        "/readyz",
        "/version",
        "/version/",
    }
)
RUNTIME_NAMESPACE_OWNER_LABEL = "trpc.io/managed-by"
RUNTIME_NAMESPACE_OWNER_VALUE = "trpc-kubernetes-runtime-gate"
RUNTIME_NAMESPACE_RUN_LABEL = "trpc.io/run-nonce"
RUNTIME_NAMESPACE_EXPIRY_LABEL = "trpc.io/expires-at"
RUNTIME_NAMESPACE_CLUSTER_LABEL = "trpc.io/cluster-fingerprint"
RUNTIME_NAMESPACE_MAX_CLEANUP = 10
RUNTIME_NAMESPACE_TTL_SECONDS = 6 * 60 * 60

PRODUCTION_DEPLOYMENTS: tuple[tuple[str, str], ...] = (
    ("trpc-gateway", "gateway"),
    ("trpc-session-recovery", "session-recovery"),
    ("trpc-artifact-gc", "artifact-gc"),
    ("trpc-backlog-exporter", "backlog-exporter"),
    ("trpc-admin", "admin"),
    ("trpc-worker", "worker"),
    ("trpc-outbox-dispatcher", "outbox-dispatcher"),
    ("trpc-channel-dispatcher", "channel-dispatcher"),
    ("trpc-post-turn-projector", "post-turn-projector"),
    ("trpc-wecom-connector", "wecom-connector"),
)
ACK_RUNTIME_PROVIDER_DISABLED_DEPLOYMENTS = ("trpc-wecom-connector",)
ACK_RUNTIME_DEPLOYMENTS: tuple[tuple[str, str], ...] = tuple(
    deployment
    for deployment in PRODUCTION_DEPLOYMENTS
    if deployment[0] not in ACK_RUNTIME_PROVIDER_DISABLED_DEPLOYMENTS
)
ACK_RUNTIME_SCOPE = "unified_cluster_runtime"
ACK_IM_DEPLOYMENT = "production_cluster"
_PDB_PROTECTED_DEPLOYMENTS = frozenset(
    {
        "trpc-worker",
        "trpc-gateway",
        "trpc-outbox-dispatcher",
        "trpc-channel-dispatcher",
        "trpc-post-turn-projector",
    }
)

_EXPECTED_SCHEDULER_VERSION = "v2"
_EXPECTED_REDIS_STREAM = "trpc:session-ready:v2"
_EXPECTED_REDIS_GROUP = "trpc-session-ready-v2"
_HPA_BACKLOG_METRIC = "trpc_session_ready_backlog"
_EXTERNAL_METRICS_API_VERSION = "v1beta1"
_MIN_PRODUCTION_TURN_CAPACITY = 200
_WORKER_EVICTION_REPLICAS = 4
_NODE_DRAIN_CONFIRMATION = "I_UNDERSTAND_ISOLATED_NODE_DRAIN"
_NODE_LABEL_KEY = "trpc-runtime-gate"
_DNS_1123_LABEL = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"
_DNS_1123_SUBDOMAIN_RE = re.compile(rf"{_DNS_1123_LABEL}(?:\.{_DNS_1123_LABEL})*")

_REQUIRED_RUNTIME_CHECKS = (
    "kube_context",
    "kustomize_render",
    "production_manifest_contract",
    "image_pull_secret_contract",
    "namespace_create",
    "server_side_dry_run",
    "manifest_contract",
    "secret_server_side_dry_run",
    "secret_apply",
    "apply",
    "schema_migration",
    "schema_migration_head",
    "readiness",
    "scheduler_cutover_guard",
    "rolling_upgrade",
    "worker_scale_and_hpa",
    "hpa_driver_rbac_bind",
    "hpa_driver_trust",
    "hpa_load_observation",
    "pdb_eviction",
    "node_eviction",
    "graceful_termination",
    "namespace_cleanup",
)

_REQUIRED_RUNTIME_ACTIONS = (
    "server_side_dry_run",
    "schema_migration",
    "schema_migration_head",
    "readiness",
    "scheduler_cutover_guard",
    "rolling_upgrade",
    "hpa_observed",
    "hpa_load_observed",
    "pod_eviction",
    "node_eviction",
    "graceful_termination",
    "namespace_cleanup",
)


@dataclass(frozen=True)
class CommandResult:
    status: str
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    reason: str = ""
    evidence: dict[str, Any] | None = None


def _enabled() -> bool:
    return os.getenv("TRPC_K8S_RUNTIME_TESTS_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _result_payload(result: CommandResult) -> dict[str, Any]:
    """Return a report-safe result without command output or credentials."""

    payload: dict[str, Any] = {"status": result.status}
    if result.exit_code is not None:
        payload["exit_code"] = result.exit_code
    if result.reason:
        payload["reason"] = result.reason
    if result.stderr:
        payload["stderr_present"] = True
    if result.evidence:
        # Evidence is assembled from validated API objects and contains only
        # identifiers/booleans.  Never copy driver stdout into a report.
        payload["evidence"] = result.evidence
    return payload


def _validate_timeout_seconds(value: object) -> float:
    """Validate a live-gate timeout before any external command can run."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("timeout_seconds must be a finite number")
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0 or timeout > MAX_TIMEOUT_SECONDS:
        raise ValueError("timeout_seconds must be finite and in the range (0, 3600]")
    return timeout


def _kubectl(
    arguments: list[str],
    *,
    context: str | None,
    timeout_seconds: float,
    input_text: str | None = None,
) -> CommandResult:
    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    executable = shutil.which("kubectl")
    if executable is None:
        return CommandResult(status="not_run", reason="kubectl is not installed")
    command = [executable]
    if context:
        command.extend(["--context", context])
    command.extend(arguments)
    try:
        completed = subprocess.run(  # noqa: S603 - executable and args are explicit, no shell
            command,
            check=False,
            capture_output=True,
            text=True,
            input=input_text,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(status="fail", reason="kubectl command timed out")
    except OSError:
        return CommandResult(status="not_run", reason="kubectl command could not start")
    return CommandResult(
        status="pass" if completed.returncode == 0 else "fail",
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _valid_image_pull_secret_name(value: str) -> bool:
    """Return whether *value* is a bounded Kubernetes DNS subdomain name."""

    return len(value) <= 253 and _DNS_1123_SUBDOMAIN_RE.fullmatch(value) is not None


def _image_pull_secret_metadata_contract(
    secret_manifest: str,
    image_pull_secret: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> CommandResult:
    """Inspect only Secret metadata needed by the private-registry contract.

    ``kubectl`` receives the external Secret manifest but the output template
    emits only kind/name/namespace/type.  Secret ``data`` and ``stringData``
    never enter this process or the runtime report.
    """

    name = image_pull_secret.strip()
    if not name:
        return CommandResult(
            status="pass",
            evidence={"configured": False},
        )
    if not _valid_image_pull_secret_name(name):
        return CommandResult(
            status="fail",
            reason="image pull Secret name is not a valid Kubernetes DNS subdomain",
            evidence={"configured": True},
        )
    metadata_template = (
        'go-template={{.kind}}{{"\\t"}}{{.metadata.name}}{{"\\t"}}'
        '{{if .metadata.namespace}}{{.metadata.namespace}}{{end}}{{"\\t"}}'
        '{{.type}}{{"\\n"}}'
    )
    inspection = _kubectl(
        [
            "create",
            "--dry-run=client",
            "--namespace",
            namespace,
            "-f",
            secret_manifest,
            "-o",
            metadata_template,
        ],
        context=context,
        timeout_seconds=min(timeout_seconds, 30),
    )
    if inspection.status != "pass":
        return CommandResult(
            status=inspection.status,
            exit_code=inspection.exit_code,
            reason="Secret manifest metadata inspection failed",
            stderr="present" if inspection.stderr else "",
            evidence={"configured": True, "secret_name": name},
        )
    records: list[tuple[str, str, str, str]] = []
    for line in inspection.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 4:
            return CommandResult(
                status="fail",
                reason="Secret manifest metadata output is invalid",
                evidence={"configured": True, "secret_name": name},
            )
        records.append(cast(tuple[str, str, str, str], tuple(fields)))
    matches = [record for record in records if record[0] == "Secret" and record[1] == name]
    if len(matches) != 1:
        return CommandResult(
            status="fail",
            reason="Secret manifest must contain exactly one configured image pull Secret",
            evidence={"configured": True, "secret_name": name},
        )
    _kind, _name, declared_namespace, resource_type = matches[0]
    if declared_namespace not in {"", namespace}:
        return CommandResult(
            status="fail",
            reason="image pull Secret targets a different namespace",
            evidence={"configured": True, "secret_name": name},
        )
    if resource_type != "kubernetes.io/dockerconfigjson":
        return CommandResult(
            status="fail",
            reason="image pull Secret must use type kubernetes.io/dockerconfigjson",
            evidence={"configured": True, "secret_name": name},
        )
    return CommandResult(
        status="pass",
        evidence={
            "configured": True,
            "secret_name": name,
            "secret_type": resource_type,
            "namespace_bound": declared_namespace == namespace,
        },
    )


def _evict_pod(
    pod_name: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> CommandResult:
    payload = json.dumps(
        {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": pod_name, "namespace": namespace},
            "deleteOptions": {"gracePeriodSeconds": 90},
        },
        separators=(",", ":"),
    )
    return _kubectl(
        [
            "create",
            "--raw",
            f"/api/v1/namespaces/{namespace}/pods/{pod_name}/eviction",
            "-f",
            "-",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
        input_text=payload,
    )


def _json_command(
    arguments: list[str], *, context: str | None, timeout_seconds: float
) -> tuple[CommandResult, dict[str, Any] | None]:
    result = _kubectl(arguments, context=context, timeout_seconds=timeout_seconds)
    if result.status != "pass":
        return result, None
    try:
        decoded = json.loads(result.stdout)
    except json.JSONDecodeError:
        return CommandResult(status="fail", reason="kubectl returned invalid JSON"), None
    if not isinstance(decoded, dict):
        return CommandResult(status="fail", reason="kubectl JSON result was not an object"), None
    return result, decoded


def _image_transform(image: str) -> dict[str, str]:
    image = image.strip()
    if not image:
        raise ValueError("image must not be empty")
    if "@sha256:" in image:
        name, digest = image.split("@", 1)
        if not name or not digest.startswith("sha256:"):
            raise ValueError("image digest must use name@sha256:<digest>")
        return {"newName": name, "digest": digest}
    slash = image.rfind("/")
    colon = image.rfind(":")
    if colon <= slash or not image[colon + 1 :]:
        raise ValueError("image must include an explicit tag or digest")
    return {"newName": image[:colon], "newTag": image[colon + 1 :]}


def _registry_digest_reference(image: str) -> tuple[str, str] | None:
    """Return a registry-qualified immutable image reference.

    Kubernetes accepts an unqualified image name and resolves it through a
    default registry. That is not sufficient for production evidence: the
    exact registry and content digest must be visible in the rendered
    Deployment. A registry host is identified by the same syntax used by
    OCI/Docker references (a dot, a port, or the literal ``localhost`` in
    the first path component).
    """

    match = _IMAGE_REFERENCE_RE.fullmatch(image.strip())
    if match is None:
        return None
    name = match.group("name")
    if name != name.lower() or "://" in name or "/" not in name:
        return None
    first_component, repository_path = name.split("/", 1)
    if not repository_path or ":" in repository_path:
        return None
    host = first_component
    has_port = ":" in first_component
    if has_port:
        host, port = first_component.rsplit(":", 1)
        if not host or not port.isdecimal() or not 1 <= int(port) <= 65535:
            return None
    if not (host == "localhost" or "." in host or has_port):
        return None
    return name, match.group("digest").lower()


def _production_image_contract(image: str, upgrade_image: str) -> tuple[bool, tuple[str, ...]]:
    """Require two distinct registry-qualified immutable production refs."""

    reasons: list[str] = []
    values = (image.strip(), upgrade_image.strip())
    digests: list[str] = []
    for label, value in zip(("initial", "upgrade"), values, strict=True):
        parsed = _registry_digest_reference(value)
        if parsed is None:
            reasons.append(
                f"{label} runtime image must use a registry-qualified "
                "name@sha256:<64-hex-digest> reference"
            )
            continue
        name, digest = parsed
        digests.append(digest)
        if any(token in name.lower() for token in ("example", "replace", "latest")):
            reasons.append(f"{label} runtime image uses a placeholder registry/reference")
    if values[0] == values[1] or (len(digests) == 2 and digests[0] == digests[1]):
        reasons.append("initial and upgrade runtime image digests must differ")
    return not reasons, tuple(dict.fromkeys(reasons))


def _hpa_status_contract(
    hpa: Mapping[str, Any] | None, *, minimum: int = 2
) -> tuple[bool, tuple[str, ...]]:
    """Require active resource/external metrics, not only AbleToScale."""

    reasons: list[str] = []
    if not isinstance(hpa, Mapping):
        return False, ("worker HPA evidence is unavailable",)
    status = hpa.get("status")
    spec = hpa.get("spec")
    if not isinstance(status, Mapping) or not isinstance(spec, Mapping):
        return False, ("worker HPA status/spec is unavailable",)
    conditions = status.get("conditions")
    condition_map = {
        item.get("type"): item.get("status")
        for item in conditions or []
        if isinstance(item, Mapping) and isinstance(item.get("type"), str)
    }
    if condition_map.get("AbleToScale") != "True":
        reasons.append("worker HPA did not expose AbleToScale=True")
    if condition_map.get("ScalingActive") != "True":
        reasons.append("worker HPA did not expose ScalingActive=True")
    metrics = status.get("currentMetrics")
    if not isinstance(metrics, list) or not metrics:
        reasons.append("worker HPA has no current metric samples")
    configured = spec.get("metrics")
    configured_names = (
        {
            item.get("external", {}).get("metric", {}).get("name")
            for item in configured
            if isinstance(item, Mapping)
            and item.get("type") == "External"
            and isinstance(item.get("external"), Mapping)
            and isinstance(item["external"].get("metric"), Mapping)
        }
        if isinstance(configured, list)
        else set()
    )
    if _HPA_BACKLOG_METRIC not in configured_names:
        reasons.append("worker HPA does not configure the exact backlog external metric")
    for key in ("currentReplicas", "desiredReplicas"):
        value = status.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            reasons.append(f"worker HPA {key} is below the configured minimum")
    return not reasons, tuple(dict.fromkeys(reasons))


def _wait_for_hpa_status(
    *, namespace: str, context: str | None, timeout_seconds: float
) -> tuple[CommandResult, dict[str, Any] | None]:
    """Poll the API until the worker HPA exposes a healthy status contract."""

    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] | None = None
    last_reasons: tuple[str, ...] = ()
    poll_count = 0
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        snapshot_timeout = min(10.0, remaining)
        result, payload = _json_command(
            ["get", "hpa", "trpc-worker", "--namespace", namespace, "-o", "json"],
            context=context,
            timeout_seconds=snapshot_timeout,
        )
        poll_count += 1
        if result.status == "pass" and payload is not None:
            last_payload = payload
            healthy, contract_reasons = _hpa_status_contract(payload)
            if healthy:
                return (
                    CommandResult(
                        status="pass",
                        evidence={"poll_count": poll_count},
                    ),
                    payload,
                )
            last_reasons = contract_reasons
        else:
            last_reasons = (result.reason or "worker HPA API read failed",)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(1.0, remaining))

    reason = "worker HPA did not become healthy before timeout"
    if last_reasons:
        reason += ": " + "; ".join(last_reasons)
    return (
        CommandResult(
            status="fail",
            reason=reason,
            evidence={
                "poll_count": poll_count,
                "last_snapshot_available": last_payload is not None,
                "last_reasons": list(last_reasons),
            },
        ),
        last_payload,
    )


def _prepare_worker_eviction_capacity(
    *, namespace: str, context: str | None, timeout_seconds: float, local_kind: bool = False
) -> tuple[CommandResult, dict[str, Any]]:
    """Raise the HPA floor before PDB eviction and verify local PDB capacity."""

    patch_payload = json.dumps(
        {"spec": {"minReplicas": _WORKER_EVICTION_REPLICAS}}, separators=(",", ":")
    )
    patch_result = _kubectl(
        [
            "patch",
            "hpa",
            "trpc-worker",
            "--namespace",
            namespace,
            "--type=merge",
            "-p",
            patch_payload,
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    details: dict[str, Any] = {"patch": _result_payload(patch_result)}
    if patch_result.status != "pass":
        return (
            CommandResult(
                status="fail",
                reason=patch_result.reason or "worker HPA eviction floor patch failed",
            ),
            details,
        )

    confirmation_result, confirmation = _json_command(
        ["get", "hpa", "trpc-worker", "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    details["api_confirmation"] = _result_payload(confirmation_result)
    if confirmation_result.status != "pass" or confirmation is None:
        return (
            CommandResult(
                status="fail",
                reason=confirmation_result.reason
                or "worker HPA eviction floor could not be API confirmed",
            ),
            details,
        )
    spec = confirmation.get("spec")
    spec_map = spec if isinstance(spec, Mapping) else {}
    min_replicas = spec_map.get("minReplicas")
    max_replicas = spec_map.get("maxReplicas")
    details["min_replicas"] = min_replicas
    details["max_replicas"] = max_replicas
    if (
        isinstance(min_replicas, bool)
        or not isinstance(min_replicas, int)
        or min_replicas != _WORKER_EVICTION_REPLICAS
    ):
        return (
            CommandResult(
                status="fail",
                reason="worker HPA eviction floor did not confirm minReplicas=4",
            ),
            details,
        )
    if isinstance(max_replicas, bool) or not isinstance(max_replicas, int) or max_replicas < 4:
        return (
            CommandResult(
                status="fail",
                reason="worker HPA maxReplicas is below the four-replica eviction floor",
            ),
            details,
        )

    ready_result = _kubectl(
        [
            "wait",
            "--for=jsonpath={.status.readyReplicas}=4",
            "deployment/trpc-worker",
            "--namespace",
            namespace,
            f"--timeout={int(timeout_seconds)}s",
        ],
        context=context,
        timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
    )
    details["ready_wait"] = _result_payload(ready_result)
    if ready_result.status != "pass":
        return (
            CommandResult(
                status="fail",
                reason=ready_result.reason or "worker did not reach four ready replicas",
            ),
            details,
        )
    if not local_kind:
        return CommandResult(status="pass"), details

    pdb_capacity: dict[str, Any] = {
        "status": "pass",
        "required_replicas": 2,
        "deployments": {},
    }
    for deployment in sorted(_PDB_PROTECTED_DEPLOYMENTS):
        deployment_result, deployment_payload = _json_command(
            ["get", "deployment", deployment, "--namespace", namespace, "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        entry: dict[str, Any] = {"status": deployment_result.status}
        if deployment_result.status != "pass" or deployment_payload is None:
            entry["reason"] = deployment_result.reason or "Deployment API response unavailable"
            pdb_capacity["status"] = "fail"
            pdb_capacity["deployments"][deployment] = entry
            continue
        spec = deployment_payload.get("spec")
        status = deployment_payload.get("status")
        desired = spec.get("replicas") if isinstance(spec, Mapping) else None
        ready = status.get("readyReplicas", 0) if isinstance(status, Mapping) else 0
        entry.update({"desired_replicas": desired, "ready_replicas": ready})
        if (
            isinstance(desired, bool)
            or not isinstance(desired, int)
            or desired < 2
            or isinstance(ready, bool)
            or not isinstance(ready, int)
            or ready < 2
        ):
            entry["status"] = "fail"
            entry["reason"] = "PDB-protected Deployment has fewer than two ready replicas"
            pdb_capacity["status"] = "fail"
        pdb_capacity["deployments"][deployment] = entry
    details["pdb_capacity"] = pdb_capacity
    if pdb_capacity["status"] != "pass":
        failed = [
            name
            for name, entry in pdb_capacity["deployments"].items()
            if isinstance(entry, Mapping) and entry.get("status") != "pass"
        ]
        return (
            CommandResult(
                status="fail",
                reason=("local-kind PDB eviction capacity is insufficient: " + ", ".join(failed)),
            ),
            details,
        )
    return CommandResult(status="pass"), details


def _finite_nonnegative_number(value: object) -> float | None:
    """Parse a Kubernetes quantity used by the HPA observation contract."""

    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    if isinstance(value, str):
        match = _KUBERNETES_QUANTITY_RE.fullmatch(value)
        if match is None:
            return None
        try:
            parsed_number = Decimal(match.group("number")) * _KUBERNETES_QUANTITY_FACTORS.get(
                match.group("suffix") or "", Decimal(1)
            )
            number = float(parsed_number)
        except (InvalidOperation, OverflowError, ValueError):
            return None
    else:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _hpa_metric_value(hpa: Mapping[str, Any]) -> float | None:
    """Extract the configured backlog metric from an observed HPA object."""

    status = hpa.get("status")
    metrics = status.get("currentMetrics") if isinstance(status, Mapping) else None
    if not isinstance(metrics, list):
        return None
    for metric in metrics:
        if not isinstance(metric, Mapping) or metric.get("type") != "External":
            continue
        external = metric.get("external")
        if not isinstance(external, Mapping):
            continue
        metric_spec = external.get("metric")
        if not isinstance(metric_spec, Mapping):
            continue
        if metric_spec.get("name") != _HPA_BACKLOG_METRIC:
            continue
        current = external.get("current")
        if not isinstance(current, Mapping):
            return None
        # External metrics can expose either value or averageValue.  The
        # service HPA uses AverageValue, so this status value is the
        # controller's per-replica view and is not directly comparable with
        # the namespace total returned by the external metrics API.
        for key in ("value", "averageValue"):
            parsed = _finite_nonnegative_number(current.get(key))
            if parsed is not None:
                return parsed
        return None
    return None


def _external_metric_from_api(
    payload: Mapping[str, Any] | None, *, namespace: str
) -> tuple[float | None, dict[str, Any], tuple[str, ...]]:
    """Parse one namespace-scoped ExternalMetricValueList fail-closed."""

    evidence: dict[str, Any] = {
        "api_observed": False,
        "api_version": _EXTERNAL_METRICS_API_VERSION,
        "metric_name": _HPA_BACKLOG_METRIC,
        "namespace": namespace,
    }
    if not isinstance(payload, Mapping):
        return None, evidence, ("external metrics API response is unavailable",)
    expected_api_version = f"external.metrics.k8s.io/{_EXTERNAL_METRICS_API_VERSION}"
    if payload.get("apiVersion") != expected_api_version:
        return (
            None,
            {**evidence, "api_version": payload.get("apiVersion")},
            ("external metrics API response version is invalid",),
        )
    if payload.get("kind") != "ExternalMetricValueList":
        return (
            None,
            {**evidence, "api_version": expected_api_version},
            ("external metrics API response kind is invalid",),
        )
    items = payload.get("items")
    if not isinstance(items, list) or len(items) != 1:
        return (
            None,
            {**evidence, "item_count": len(items) if isinstance(items, list) else None},
            ("external metrics API must return exactly one backlog value",),
        )
    item = items[0]
    if not isinstance(item, Mapping):
        return None, {**evidence, "item_count": 1}, ("external metrics API value is invalid",)
    if item.get("metricName") != _HPA_BACKLOG_METRIC:
        return (
            None,
            {**evidence, "item_count": 1},
            ("external metrics API returned an unexpected metric name",),
        )
    labels = item.get("metricLabels")
    if not isinstance(labels, Mapping) or labels.get("namespace") != namespace:
        return (
            None,
            {
                **evidence,
                "item_count": 1,
                "label_namespace": labels.get("namespace") if isinstance(labels, Mapping) else None,
            },
            ("external metrics API value is not namespace-bound",),
        )
    value = _finite_nonnegative_number(item.get("value"))
    if value is None:
        return (
            None,
            {**evidence, "item_count": 1, "label_namespace": namespace},
            ("external metrics API value is not finite and nonnegative",),
        )
    return (
        value,
        {
            **evidence,
            "api_observed": True,
            "item_count": 1,
            "label_namespace": namespace,
            "value": value,
        },
        (),
    )


def _observe_external_metric(
    *, namespace: str, context: str | None, timeout_seconds: float
) -> tuple[CommandResult, float | None]:
    """Read the external metrics API directly, never a driver-supplied file."""

    path = (
        f"/apis/external.metrics.k8s.io/{_EXTERNAL_METRICS_API_VERSION}/namespaces/"
        f"{namespace}/{_HPA_BACKLOG_METRIC}"
    )
    result, payload = _json_command(
        ["get", "--raw", path],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    base_evidence = {"api_path": path}
    if result.status != "pass":
        return (
            CommandResult(
                status=result.status,
                exit_code=result.exit_code,
                reason="external metrics API read failed",
                stderr=result.stderr,
                evidence=base_evidence,
            ),
            None,
        )
    value, evidence, reasons = _external_metric_from_api(payload, namespace=namespace)
    evidence = {**base_evidence, **evidence}
    if reasons:
        return (
            CommandResult(
                status="fail",
                reason="; ".join(reasons),
                evidence=evidence,
            ),
            None,
        )
    return CommandResult(status="pass", evidence=evidence), value


def _hpa_observation_from_api(
    hpa: Mapping[str, Any] | None,
    deployment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    """Build one HPA phase only from Kubernetes API objects."""

    reasons: list[str] = []
    if not isinstance(hpa, Mapping):
        reasons.append("HPA API response is unavailable")
        return None, tuple(reasons)
    metadata = hpa.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("name") != "trpc-worker":
        reasons.append("HPA API response is not for trpc-worker")
    status = hpa.get("status")
    if not isinstance(status, Mapping):
        reasons.append("HPA API response has no status")
    metric_value = _hpa_metric_value(hpa)
    if metric_value is None:
        reasons.append("HPA API response has no finite backlog metric value")
    current = _finite_nonnegative_number(
        status.get("currentReplicas") if isinstance(status, Mapping) else None
    )
    desired = _finite_nonnegative_number(
        status.get("desiredReplicas") if isinstance(status, Mapping) else None
    )
    if current is None:
        reasons.append("HPA API response has no finite currentReplicas")
    if desired is None:
        reasons.append("HPA API response has no finite desiredReplicas")
    deployment_status = deployment.get("status") if isinstance(deployment, Mapping) else None
    ready = _finite_nonnegative_number(
        deployment_status.get("readyReplicas") if isinstance(deployment_status, Mapping) else None
    )
    if ready is None:
        reasons.append("worker Deployment API response has no finite readyReplicas")
    if reasons:
        return None, tuple(dict.fromkeys(reasons))
    return {
        "metric_value": metric_value,
        "desired_replicas": desired,
        "current_replicas": current,
        "ready_replicas": ready,
    }, ()


def _observe_hpa_state(
    *, namespace: str, context: str | None, timeout_seconds: float
) -> tuple[CommandResult, dict[str, Any] | None]:
    """Read one HPA phase and its direct external metric from the API server."""

    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    hpa_result, hpa = _json_command(
        ["get", "hpa", "trpc-worker", "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if hpa_result.status != "pass":
        return hpa_result, None
    deployment_result, deployment = _json_command(
        ["get", "deployment", "trpc-worker", "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if deployment_result.status != "pass":
        return deployment_result, None
    observation, reasons = _hpa_observation_from_api(hpa, deployment)
    if observation is None:
        return CommandResult(status="fail", reason="; ".join(reasons)), None
    external_result, external_value = _observe_external_metric(
        namespace=namespace,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if external_result.status != "pass" or external_value is None:
        return (
            CommandResult(
                status=external_result.status,
                reason=external_result.reason or "external metrics API observation failed",
                stderr=external_result.stderr,
                evidence=external_result.evidence,
            ),
            None,
        )
    # The HPA status reports ``averageValue`` for an AverageValue target,
    # while the namespace-scoped external metrics API reports the aggregate
    # backlog.  Preserve the controller's view for diagnostics, but use the
    # direct API aggregate as the load-transition signal.  Requiring equality
    # here would discard every healthy scaled observation once replicas > 1.
    observation["hpa_metric_value"] = observation["metric_value"]
    observation["metric_value"] = external_value
    observation["external_metric"] = external_result.evidence or {}
    return CommandResult(status="pass"), observation


def _hpa_phase_transition_contract(
    before: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    phase: str,
    during: Mapping[str, Any] | None = None,
) -> tuple[bool, tuple[str, ...]]:
    """Check one API snapshot against the preceding live HPA phase."""

    reasons: list[str] = []

    def number(value: object, name: str) -> float | None:
        parsed = _finite_nonnegative_number(value)
        if parsed is None:
            reasons.append(f"HPA {phase} observation {name} is not finite and nonnegative")
        return parsed

    if not isinstance(before, Mapping) or not isinstance(candidate, Mapping):
        return False, (f"HPA {phase} observation shape is invalid",)
    if phase == "during":
        before_metric = number(before.get("metric_value"), "before.metric_value")
        candidate_metric = number(candidate.get("metric_value"), "during.metric_value")
        before_desired = number(before.get("desired_replicas"), "before.desired_replicas")
        candidate_desired = number(candidate.get("desired_replicas"), "during.desired_replicas")
        before_current = number(before.get("current_replicas"), "before.current_replicas")
        candidate_current = number(candidate.get("current_replicas"), "during.current_replicas")
        before_ready = number(before.get("ready_replicas"), "before.ready_replicas")
        candidate_ready = number(candidate.get("ready_replicas"), "during.ready_replicas")

        replicas_increased = any(
            baseline is not None and observed is not None and observed > baseline
            for baseline, observed in (
                (before_desired, candidate_desired),
                (before_current, candidate_current),
                (before_ready, candidate_ready),
            )
        )
        if before_metric is not None and candidate_metric is not None:
            # The external-metrics API and HPA status are independently
            # reconciled.  A cached positive baseline can therefore equal
            # the first post-load metric sample even though the replicas
            # have already scaled.  Replica growth is the independent live
            # evidence in that case; a zero/equal metric remains rejected so
            # manual scale cannot satisfy this gate.
            if candidate_metric < before_metric or (
                candidate_metric == before_metric
                and (candidate_metric <= 0 or not replicas_increased)
            ):
                reasons.append("controlled backlog did not increase the observed HPA metric")
        if not replicas_increased:
            reasons.append("HPA desired replicas did not increase under controlled load")
        if (
            candidate_current is not None
            and candidate_ready is not None
            and before_desired is not None
        ):
            # HPA currentReplicas can lag the Deployment status by one
            # reconciliation cycle.  Use the larger live count for the
            # pre-load bound while still requiring the Deployment readiness
            # check below.
            if max(candidate_current, candidate_ready) < before_desired:
                reasons.append("HPA current replicas did not reach the pre-load desired bound")
        if candidate_ready is not None and candidate_desired is not None:
            if candidate_ready < candidate_desired:
                reasons.append("HPA scaled replicas did not become ready")
    elif phase == "after":
        if not isinstance(during, Mapping):
            return False, ("HPA during observation is unavailable",)
        during_metric = number(during.get("metric_value"), "during.metric_value")
        before_desired = number(before.get("desired_replicas"), "before.desired_replicas")
        before_ready = number(before.get("ready_replicas"), "before.ready_replicas")
        during_current = number(during.get("current_replicas"), "during.current_replicas")
        candidate_metric = number(candidate.get("metric_value"), "after.metric_value")
        candidate_desired = number(candidate.get("desired_replicas"), "after.desired_replicas")
        candidate_current = number(candidate.get("current_replicas"), "after.current_replicas")
        candidate_ready = number(candidate.get("ready_replicas"), "after.ready_replicas")
        if candidate_metric is not None and during_metric is not None:
            if candidate_metric >= during_metric:
                reasons.append("HPA metric did not fall after controlled load removal")
        if candidate_desired is not None and before_desired is not None:
            if candidate_desired > before_desired:
                reasons.append("HPA desired replicas did not return to the pre-load bound")
        if candidate_ready is not None and before_ready is not None:
            if candidate_ready > before_ready:
                reasons.append("HPA ready replicas did not return to the pre-load bound")
        if candidate_current is not None and during_current is not None:
            # currentReplicas is a lagging HPA status field.  Once the HPA
            # desired count has returned to the pre-load bound, a larger
            # current count can be an in-flight stale snapshot. Deployment
            # readyReplicas is the convergence signal for this phase, so keep
            # rejecting current status only while HPA still desires the
            # scaled-up count.
            if candidate_current > during_current and (
                candidate_desired is None
                or before_desired is None
                or candidate_desired > before_desired
            ):
                reasons.append("HPA current replicas increased after controlled load removal")
        if candidate_ready is not None and candidate_desired is not None:
            if candidate_ready < candidate_desired:
                reasons.append("HPA replicas were not ready after load removal")
    else:
        raise ValueError(f"unsupported HPA observation phase: {phase}")
    return not reasons, tuple(dict.fromkeys(reasons))


def _wait_for_hpa_phase(
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
    phase: str,
    before: Mapping[str, Any],
    during: Mapping[str, Any] | None = None,
) -> tuple[CommandResult, dict[str, Any] | None]:
    """Poll API observations until the requested live HPA transition occurs."""

    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    if phase == "during":
        anchor = before
    elif phase == "after":
        if during is None:
            return (
                CommandResult(status="fail", reason="HPA during observation is unavailable"),
                None,
            )
        anchor = during
    else:
        raise ValueError(f"unsupported HPA observation phase: {phase}")

    deadline = time.monotonic() + timeout_seconds
    last_observation: dict[str, Any] | None = None
    last_reasons: tuple[str, ...] = ()
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        snapshot_timeout = max(0.1, min(10.0, remaining / 2))
        result, observation = _observe_hpa_state(
            namespace=namespace,
            context=context,
            timeout_seconds=snapshot_timeout,
        )
        if result.status == "pass" and observation is not None:
            last_observation = observation
            satisfied, transition_reasons = _hpa_phase_transition_contract(
                before,
                observation,
                phase=phase,
                during=anchor if phase == "after" else None,
            )
            if satisfied:
                return CommandResult(status="pass"), observation
            last_reasons = transition_reasons
        else:
            last_reasons = (result.reason or f"HPA {phase} API observation failed",)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            # Polling is bounded by the live deadline; no fixed-duration phase
            # sleep is used, and transient adapter/API gaps remain retryable.
            time.sleep(min(1.0, remaining))

    reason = f"HPA {phase} phase did not reach the required live transition before timeout"
    if last_reasons:
        reason += ": " + "; ".join(last_reasons)
    return CommandResult(status="fail", reason=reason), last_observation


def _hpa_load_observation_contract(
    evidence: Mapping[str, Any] | None,
    *,
    cluster_fingerprint: str | None = None,
    run_nonce: str | None = None,
    namespace: str | None = None,
) -> tuple[bool, tuple[str, ...]]:
    """Require a live, load-triggered HPA scale-up and scale-down observation.

    A manual ``kubectl scale`` is intentionally not accepted.  The bounded
    load driver records HPA observations before load, while the controlled
    backlog is active, and after the backlog is removed.  The report contains
    only those numeric observations and an API-server fingerprint; the load
    payload itself never enters the report.
    """

    reasons: list[str] = []
    if not isinstance(evidence, Mapping):
        return False, ("controlled HPA load observation is missing",)
    if evidence.get("status") != "pass":
        reasons.append("controlled HPA load observation did not pass")
    if evidence.get("observed_live") is not True:
        reasons.append("HPA load observation was not marked as live")
    if evidence.get("source") != "kubectl_api":
        reasons.append("HPA load observation source is not the live kubectl API")
    if evidence.get("hpa_name") != "trpc-worker":
        reasons.append("HPA load observation must target trpc-worker")
    if run_nonce is not None and evidence.get("run_nonce") != run_nonce:
        reasons.append("HPA load observation belongs to a different runtime nonce")
    if namespace is not None and evidence.get("namespace") != namespace:
        reasons.append("HPA load observation belongs to a different namespace")
    for timeout_name in ("scale_up_timeout_seconds", "scale_down_timeout_seconds"):
        timeout_value = evidence.get(timeout_name)
        if (
            isinstance(timeout_value, bool)
            or not isinstance(timeout_value, (int, float))
            or timeout_value <= 0
            or timeout_value > 3600
        ):
            reasons.append(f"HPA load observation {timeout_name} is outside 0..3600 seconds")
    metric_name = evidence.get("metric_name")
    if metric_name != "trpc_session_ready_backlog":
        reasons.append("HPA load observation metric is not trpc_session_ready_backlog")
    trigger = evidence.get("trigger")
    if not isinstance(trigger, Mapping) or trigger.get("kind") != "controlled_backlog":
        reasons.append("HPA load observation lacks a controlled backlog trigger")
    elif not isinstance(trigger.get("source"), str) or not trigger["source"].strip():
        reasons.append("HPA load observation trigger source is missing")
    identity = evidence.get("cluster_identity")
    if not isinstance(identity, Mapping):
        reasons.append("HPA load observation cluster identity is missing")
    else:
        fingerprint = identity.get("fingerprint_sha256")
        if not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
            reasons.append("HPA load observation cluster fingerprint is invalid")
        elif cluster_fingerprint is not None and fingerprint != cluster_fingerprint:
            reasons.append("HPA load observation belongs to a different cluster")

    driver_evidence = evidence.get("driver_evidence")
    if not isinstance(driver_evidence, Mapping):
        reasons.append("HPA load observation lacks bounded Job API evidence")
    else:
        load_evidence = driver_evidence.get("load")
        clear_evidence = driver_evidence.get("clear")
        expected_job_name = _hpa_job_name(str(run_nonce or evidence.get("run_nonce", "")))

        def check_job(value: object, *, phase: str) -> Mapping[str, Any] | None:
            if not isinstance(value, Mapping):
                reasons.append(f"HPA {phase} driver Job evidence is missing")
                return None
            if value.get("api_observed") is not True:
                reasons.append(f"HPA {phase} driver Job was not API observed")
            if value.get("namespace") != evidence.get("namespace"):
                reasons.append(f"HPA {phase} driver Job namespace is not bound")
            if value.get("run_nonce") != evidence.get("run_nonce"):
                reasons.append(f"HPA {phase} driver Job nonce is not bound")
            if value.get("cluster_fingerprint") != (
                identity.get("fingerprint_sha256") if isinstance(identity, Mapping) else None
            ):
                reasons.append(f"HPA {phase} driver Job cluster fingerprint is not bound")
            if value.get("job_name") != expected_job_name:
                reasons.append(f"HPA {phase} driver Job name is not deterministic")
            uid = value.get("job_uid")
            if (
                not isinstance(uid, str)
                or not uid
                or len(uid) > 128
                or any(ch.isspace() for ch in uid)
            ):
                reasons.append(f"HPA {phase} driver Job UID is invalid")
            labels = value.get("job_labels")
            if (
                not isinstance(labels, Mapping)
                or labels.get(HPA_DRIVER_OWNER_LABEL) != HPA_DRIVER_OWNER_VALUE
            ):
                reasons.append(f"HPA {phase} driver Job owner label is missing")
            if isinstance(labels, Mapping) and (
                labels.get(HPA_DRIVER_RUN_LABEL) != evidence.get("run_nonce")
                or labels.get(HPA_DRIVER_PHASE_LABEL) != "load"
            ):
                reasons.append(f"HPA {phase} driver Job labels are not nonce bound")
            return value

        load_checked = check_job(load_evidence, phase="load")
        clear_checked = check_job(clear_evidence, phase="clear")
        if isinstance(load_checked, Mapping) and isinstance(clear_checked, Mapping):
            if load_checked.get("job_uid") != clear_checked.get("job_uid"):
                reasons.append("HPA load and clear evidence refer to different Job UIDs")
        if isinstance(clear_checked, Mapping) and clear_checked.get("job_deleted") is not True:
            reasons.append("HPA clear driver did not API-confirm Job deletion")

    observations: dict[str, Mapping[str, Any]] = {}
    for phase in ("before", "during", "after"):
        value = evidence.get(phase)
        if not isinstance(value, Mapping):
            reasons.append(f"HPA load observation phase {phase} is missing")
        else:
            observations[phase] = value

    expected_namespace = namespace or evidence.get("namespace")
    for phase, observation in observations.items():
        external = observation.get("external_metric")
        if not isinstance(external, Mapping):
            reasons.append(f"HPA {phase} observation lacks direct external metric API evidence")
            continue
        if external.get("api_observed") is not True:
            reasons.append(f"HPA {phase} external metric API was not observed")
        if external.get("metric_name") != _HPA_BACKLOG_METRIC:
            reasons.append(f"HPA {phase} external metric name is not bound")
        if expected_namespace is not None:
            if external.get("namespace") != expected_namespace:
                reasons.append(f"HPA {phase} external metric namespace is not bound")
            if external.get("label_namespace") != expected_namespace:
                reasons.append(f"HPA {phase} external metric label namespace is not bound")
            expected_path = (
                f"/apis/external.metrics.k8s.io/{_EXTERNAL_METRICS_API_VERSION}/namespaces/"
                f"{expected_namespace}/{_HPA_BACKLOG_METRIC}"
            )
            if external.get("api_path") != expected_path:
                reasons.append(f"HPA {phase} external metric API path is not namespace-bound")
        if external.get("item_count") != 1:
            reasons.append(f"HPA {phase} external metric API item count is invalid")
        direct_value = _finite_nonnegative_number(external.get("value"))
        observed_value = _finite_nonnegative_number(observation.get("metric_value"))
        if direct_value is None:
            reasons.append(f"HPA {phase} external metric value is not finite and nonnegative")
        elif observed_value is not None and direct_value != observed_value:
            reasons.append(f"HPA {phase} external metric value does not match observed metric")

    def number(phase: str, key: str) -> float | None:
        value = observations.get(phase, {}).get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            reasons.append(f"HPA load observation {phase}.{key} is not numeric")
            return None
        if not math.isfinite(float(value)) or value < 0:
            reasons.append(f"HPA load observation {phase}.{key} is not finite and nonnegative")
            return None
        return float(value)

    before_metric = number("before", "metric_value")
    during_metric = number("during", "metric_value")
    after_metric = number("after", "metric_value")
    before_desired = number("before", "desired_replicas")
    during_desired = number("during", "desired_replicas")
    after_desired = number("after", "desired_replicas")
    before_current = number("before", "current_replicas")
    during_current = number("during", "current_replicas")
    before_ready = number("before", "ready_replicas")
    during_ready = number("during", "ready_replicas")
    after_current = number("after", "current_replicas")
    after_ready = number("after", "ready_replicas")
    replicas_increased = any(
        baseline is not None and observed is not None and observed > baseline
        for baseline, observed in (
            (before_desired, during_desired),
            (before_current, during_current),
            (before_ready, during_ready),
        )
    )
    if (
        before_metric is not None
        and during_metric is not None
        and (
            during_metric < before_metric
            or (during_metric == before_metric and (during_metric <= 0 or not replicas_increased))
        )
    ):
        reasons.append("controlled backlog did not increase the observed HPA metric")
    if not replicas_increased:
        reasons.append("HPA desired replicas did not increase under controlled load")
    if (
        during_current is not None
        and during_ready is not None
        and before_desired is not None
        and max(during_current, during_ready) < before_desired
    ):
        reasons.append("HPA current replicas did not reach the pre-load desired bound")
    if during_ready is not None and during_desired is not None and during_ready < during_desired:
        reasons.append("HPA scaled replicas did not become ready")
    if after_metric is not None and during_metric is not None and after_metric >= during_metric:
        reasons.append("HPA metric did not fall after controlled load removal")
    if after_desired is not None and before_desired is not None and after_desired > before_desired:
        reasons.append("HPA desired replicas did not return to the pre-load bound")
    if after_ready is not None and before_ready is not None and after_ready > before_ready:
        reasons.append("HPA ready replicas did not return to the pre-load bound")
    if (
        after_current is not None
        and during_current is not None
        and after_current > during_current
        and (after_desired is None or before_desired is None or after_desired > before_desired)
    ):
        reasons.append("HPA current replicas increased after controlled load removal")
    if after_ready is not None and after_desired is not None and after_ready < after_desired:
        reasons.append("HPA replicas were not ready after load removal")
    return not reasons, tuple(dict.fromkeys(reasons))


def _hpa_load_report_payload(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only the fixed, report-safe HPA observation schema."""

    if not isinstance(evidence, Mapping):
        return {}
    result: dict[str, Any] = {
        "status": evidence.get("status"),
        "observed_live": evidence.get("observed_live"),
        "source": evidence.get("source"),
        "hpa_name": evidence.get("hpa_name"),
        "metric_name": evidence.get("metric_name"),
        "run_nonce": evidence.get("run_nonce"),
        "namespace": evidence.get("namespace"),
        "cluster_identity": {
            "fingerprint_sha256": (
                evidence.get("cluster_identity", {}).get("fingerprint_sha256")
                if isinstance(evidence.get("cluster_identity"), Mapping)
                else None
            )
        },
        "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
        "scale_up_timeout_seconds": evidence.get("scale_up_timeout_seconds"),
        "scale_down_timeout_seconds": evidence.get("scale_down_timeout_seconds"),
        "driver_evidence": evidence.get("driver_evidence", {}),
    }
    for phase in ("before", "during", "after"):
        observation = evidence.get(phase)
        result[phase] = {
            key: observation.get(key)
            for key in ("metric_value", "desired_replicas", "current_replicas", "ready_replicas")
            if isinstance(observation, Mapping) and key in observation
        }
        if isinstance(observation, Mapping) and isinstance(
            observation.get("external_metric"), Mapping
        ):
            external = observation["external_metric"]
            result[phase]["external_metric"] = {
                key: external.get(key)
                for key in (
                    "api_observed",
                    "api_version",
                    "api_path",
                    "metric_name",
                    "namespace",
                    "label_namespace",
                    "item_count",
                    "value",
                )
                if key in external
            }
    return result


def _write_overlay(
    directory: Path,
    *,
    namespace: str,
    image: str,
    local_kind: bool = False,
    node_label: tuple[str, str] | None = None,
    run_nonce: str | None = None,
    cluster_fingerprint: str | None = None,
    expires_at: str | None = None,
    image_pull_secret: str | None = None,
) -> Path:
    image_transform = _image_transform(image)

    def resource_path(resource: Path) -> str:
        try:
            return Path(os.path.relpath(resource, directory)).as_posix()
        except ValueError:
            # Pytest temporary directories on Windows can be on a different
            # drive.  The live runner uses a same-drive directory, while the
            # absolute fallback keeps this helper testable and is still
            # constrained by --load-restrictor=LoadRestrictionsNone.
            return resource.resolve().as_posix()

    relative_base = resource_path(BASE_ROOT)
    if local_kind:
        replica_patch = directory / "kind-capacity-patch.yaml"
        replica_documents: list[dict[str, Any]] = []
        for deployment, _container in ACK_RUNTIME_DEPLOYMENTS:
            replica_documents.append(
                {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"name": deployment},
                    "spec": {"replicas": 2 if deployment in _PDB_PROTECTED_DEPLOYMENTS else 1},
                }
            )
        replica_documents.extend(
            [
                {
                    "apiVersion": "autoscaling/v2",
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "trpc-worker"},
                    "spec": {"minReplicas": 2, "maxReplicas": 4},
                },
                {
                    "apiVersion": "autoscaling/v2",
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "trpc-gateway"},
                    "spec": {"minReplicas": 2, "maxReplicas": 2},
                },
            ]
        )
        replica_patch.write_text(
            yaml.safe_dump_all(replica_documents, sort_keys=False), encoding="utf-8"
        )
    else:
        replica_patch = OVERLAY_ROOT / "replicas-patch.yaml"
    relative_replica_patch = resource_path(replica_patch)
    provider_disabled_patch = directory / "provider-disabled-patch.yaml"
    provider_disabled_patch.write_text(
        yaml.safe_dump_all(
            [
                {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"name": deployment},
                    "spec": {"replicas": 0},
                }
                for deployment in ACK_RUNTIME_PROVIDER_DISABLED_DEPLOYMENTS
            ],
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    relative_provider_disabled_patch = resource_path(provider_disabled_patch)
    relative_config_patch = resource_path(OVERLAY_ROOT / "production-config-patch.yaml")
    namespace_labels = {
        RUNTIME_NAMESPACE_OWNER_LABEL: RUNTIME_NAMESPACE_OWNER_VALUE,
    }
    if run_nonce:
        namespace_labels[RUNTIME_NAMESPACE_RUN_LABEL] = run_nonce
    if expires_at:
        namespace_labels[RUNTIME_NAMESPACE_EXPIRY_LABEL] = expires_at
    if cluster_fingerprint:
        namespace_labels[RUNTIME_NAMESPACE_CLUSTER_LABEL] = cluster_fingerprint[:63]
    label_lines = "".join(
        f"    {key}: {json.dumps(value)}\n" for key, value in namespace_labels.items()
    )
    (directory / "namespace.yaml").write_text(
        "apiVersion: v1\nkind: Namespace\nmetadata:\n"
        "  name: " + namespace + "\n  labels:\n" + label_lines,
        encoding="utf-8",
    )
    image_lines = ["  - name: trpc-agent-service"]
    image_lines.append(f"    newName: {image_transform['newName']}")
    if "newTag" in image_transform:
        image_lines.append(f"    newTag: {image_transform['newTag']}")
    else:
        image_lines.append(f"    digest: {image_transform['digest']}")
    lines = [
        "apiVersion: kustomize.config.k8s.io/v1beta1",
        "kind: Kustomization",
        f"namespace: {namespace}",
        "resources:",
        f"  - {relative_base}",
        "  - namespace.yaml",
        "images:",
        *image_lines,
        "patches:",
        f"  - path: {relative_replica_patch}",
        f"  - path: {relative_provider_disabled_patch}",
        f"  - path: {relative_config_patch}",
    ]
    if node_label is not None:
        node_label_patch = directory / "controlled-node-patch.yaml"
        node_label_patch.write_text(
            "- op: add\n"
            "  path: /spec/template/spec/nodeSelector\n"
            "  value:\n"
            f"    {node_label[0]}: {json.dumps(node_label[1])}\n",
            encoding="utf-8",
        )
        lines.extend(
            [
                "  - target:",
                "      kind: Deployment",
                "    path: controlled-node-patch.yaml",
            ]
        )
    if image_pull_secret:
        if not _valid_image_pull_secret_name(image_pull_secret):
            raise ValueError("image pull Secret name is invalid")
        image_pull_secret_patch = directory / "image-pull-secret-patch.yaml"
        image_pull_secret_patch.write_text(
            yaml.safe_dump(
                [
                    {
                        "op": "add",
                        "path": "/spec/template/spec/imagePullSecrets",
                        "value": [{"name": image_pull_secret}],
                    }
                ],
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        for kind in ("Deployment", "Job"):
            lines.extend(
                [
                    "  - target:",
                    f"      kind: {kind}",
                    "    path: image-pull-secret-patch.yaml",
                ]
            )
    lines.append("")
    content = "\n".join(lines)
    path = directory / "kustomization.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def _split_migration_manifests(rendered: str) -> tuple[str, str]:
    """Split rendered resources so schema migration completes before workloads start.

    The migration Job imports schema-dependent application code.  Applying the
    complete Kustomize output in one request lets Kubernetes schedule business
    Deployments before that Job has finished, which can race the first schema
    access.  Keep the migration's ConfigMap and ServiceAccount in the first
    batch; Secrets are applied separately by the caller before this split.
    """

    try:
        documents = [
            document for document in yaml.safe_load_all(rendered) if isinstance(document, Mapping)
        ]
    except yaml.YAMLError as error:
        raise ValueError("rendered manifest is not valid YAML") from error

    migration_resources: list[Mapping[str, Any]] = []
    runtime_resources: list[Mapping[str, Any]] = []
    migration_support = {
        ("ConfigMap", "trpc-service-config"),
        ("ServiceAccount", "trpc-service"),
        ("Job", "trpc-schema-migration"),
    }
    for document in documents:
        metadata = document.get("metadata")
        name = metadata.get("name") if isinstance(metadata, Mapping) else None
        resource = (str(document.get("kind", "")), str(name or ""))
        if resource in migration_support:
            migration_resources.append(document)
        elif document.get("kind") != "Namespace":
            # The namespace is created and labelled by the gate before this
            # point; applying it again is unnecessary and can obscure the
            # dependency boundary in the evidence.
            runtime_resources.append(document)

    migration_job_present = any(
        document.get("kind") == "Job"
        and isinstance(document.get("metadata"), Mapping)
        and document["metadata"].get("name") == "trpc-schema-migration"
        for document in migration_resources
    )
    if not migration_job_present:
        raise ValueError("schema migration Job is missing from rendered manifest")

    return (
        yaml.safe_dump_all(migration_resources, sort_keys=False),
        yaml.safe_dump_all(runtime_resources, sort_keys=False),
    )


def _runtime_object_store_override(
    rendered: str, *, endpoint: str, bucket: str
) -> tuple[str, dict[str, Any]]:
    """Apply one credential-free acceptance endpoint to the rendered ConfigMap."""

    normalized_endpoint = endpoint.strip().rstrip("/")
    normalized_bucket = bucket.strip()
    try:
        parsed = urlsplit(normalized_endpoint)
        port = parsed.port
    except ValueError as error:
        raise ValueError("runtime object-store endpoint is invalid") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError("runtime object-store endpoint must be a credential-free HTTP(S) origin")
    if S3_BUCKET_RE.fullmatch(normalized_bucket) is None:
        raise ValueError("runtime object-store bucket is invalid")
    try:
        documents = [document for document in yaml.safe_load_all(rendered) if document is not None]
    except yaml.YAMLError as error:
        raise ValueError("rendered manifest is not valid YAML") from error
    configs = [
        document
        for document in documents
        if isinstance(document, dict)
        and document.get("kind") == "ConfigMap"
        and isinstance(document.get("metadata"), dict)
        and document["metadata"].get("name") == "trpc-service-config"
    ]
    if len(configs) != 1 or not isinstance(configs[0].get("data"), dict):
        raise ValueError("rendered manifest must contain one trpc-service-config ConfigMap")
    configs[0]["data"]["TRPC_SERVICE_S3_ENDPOINT"] = normalized_endpoint
    configs[0]["data"]["TRPC_SERVICE_S3_BUCKET"] = normalized_bucket
    return (
        yaml.safe_dump_all(documents, sort_keys=False),
        {
            "status": "pass",
            "endpoint_sha256": hashlib.sha256(normalized_endpoint.encode()).hexdigest(),
            "bucket_sha256": hashlib.sha256(normalized_bucket.encode()).hexdigest(),
            "credentials_recorded": False,
        },
    )


def _rendered_manifest_contract(
    rendered: str, *, local_kind: bool = False
) -> tuple[bool, tuple[str, ...]]:
    """Validate safety-critical resources before any live apply.

    Kustomize rendering is not itself a runtime test, but applying a manifest
    that omits a PDB, resource limits, a pre-stop hook, or the pinned v2
    scheduler would make the later runtime observations meaningless.  Keep
    this check pure and report only reasons, never manifest contents.
    """

    try:
        documents = [
            document for document in yaml.safe_load_all(rendered) if isinstance(document, Mapping)
        ]
    except yaml.YAMLError:
        return False, ("rendered manifest is not valid YAML",)

    reasons: list[str] = []
    resources: dict[tuple[str, str], Mapping[str, Any]] = {}
    for document in documents:
        metadata = document.get("metadata")
        if isinstance(metadata, Mapping):
            resources[(str(document.get("kind", "")), str(metadata.get("name", "")))] = document

    config = resources.get(("ConfigMap", "trpc-service-config"))
    config_data = config.get("data") if isinstance(config, Mapping) else None
    if not isinstance(config_data, Mapping):
        reasons.append("scheduler ConfigMap is missing")
    else:
        if config_data.get("TRPC_SERVICE_SCHEDULER_VERSION") != _EXPECTED_SCHEDULER_VERSION:
            reasons.append("scheduler ConfigMap is not pinned to v2")
        if config_data.get("TRPC_SERVICE_REDIS_STREAM") != _EXPECTED_REDIS_STREAM:
            reasons.append("scheduler Redis stream is not the v2 stream")
        if config_data.get("TRPC_SERVICE_REDIS_CONSUMER_GROUP") != _EXPECTED_REDIS_GROUP:
            reasons.append("scheduler Redis consumer group is not the v2 group")

    for kind, name in (
        ("HorizontalPodAutoscaler", "trpc-worker"),
        ("HorizontalPodAutoscaler", "trpc-gateway"),
    ):
        hpa = resources.get((kind, name))
        spec = hpa.get("spec") if isinstance(hpa, Mapping) else None
        if not isinstance(spec, Mapping):
            reasons.append(f"{name} HPA is missing")
            continue
        minimum = spec.get("minReplicas")
        maximum = spec.get("maxReplicas")
        metrics = spec.get("metrics")
        if not isinstance(minimum, int) or not isinstance(maximum, int) or maximum < minimum:
            reasons.append(f"{name} HPA replica bounds are invalid")
        if not isinstance(metrics, list) or not metrics:
            reasons.append(f"{name} HPA has no resource metrics")
        if name == "trpc-worker":
            external_names = {
                metric.get("external", {}).get("metric", {}).get("name")
                for metric in metrics or ()
                if isinstance(metric, Mapping)
                and metric.get("type") == "External"
                and isinstance(metric.get("external"), Mapping)
                and isinstance(metric["external"].get("metric"), Mapping)
            }
            if _HPA_BACKLOG_METRIC not in external_names:
                reasons.append(
                    "trpc-worker HPA does not configure the exact backlog external metric"
                )

    for kind, name in (
        ("PodDisruptionBudget", "trpc-worker"),
        ("PodDisruptionBudget", "trpc-gateway"),
    ):
        pdb = resources.get((kind, name))
        spec = pdb.get("spec") if isinstance(pdb, Mapping) else None
        minimum = spec.get("minAvailable") if isinstance(spec, Mapping) else None
        if isinstance(minimum, bool):
            valid_minimum = False
        elif isinstance(minimum, int):
            valid_minimum = minimum >= 1
        elif isinstance(minimum, str):
            try:
                valid_minimum = int(minimum) >= 1
            except ValueError:
                valid_minimum = False
        else:
            valid_minimum = False
        if not valid_minimum:
            reasons.append(f"{name} PDB does not protect at least one replica")

    if not any(document.get("kind") == "NetworkPolicy" for document in documents):
        reasons.append("no NetworkPolicy is rendered")
    if resources.get(("Job", "trpc-schema-migration")) is None:
        reasons.append("schema migration Job is missing")
    migration = resources.get(("Job", "trpc-schema-migration"))
    migration_spec = migration.get("spec") if isinstance(migration, Mapping) else None
    if not isinstance(migration_spec, Mapping) or not isinstance(
        migration_spec.get("activeDeadlineSeconds"), int
    ):
        reasons.append("schema migration Job has no active deadline")

    for deployment_name, _container_name in PRODUCTION_DEPLOYMENTS:
        deployment = resources.get(("Deployment", deployment_name))
        if not isinstance(deployment, Mapping):
            reasons.append(f"required deployment {deployment_name} is missing")
            continue
        spec = deployment.get("spec")
        template = spec.get("template") if isinstance(spec, Mapping) else None
        pod_spec = template.get("spec") if isinstance(template, Mapping) else None
        strategy = spec.get("strategy") if isinstance(spec, Mapping) else None
        if not isinstance(strategy, Mapping) or strategy.get("type") != "RollingUpdate":
            reasons.append(f"{deployment_name} does not use RollingUpdate")
        grace = (
            pod_spec.get("terminationGracePeriodSeconds") if isinstance(pod_spec, Mapping) else None
        )
        if not isinstance(grace, int) or grace < 30:
            reasons.append(f"{deployment_name} has no safe termination grace period")
        containers = pod_spec.get("containers") if isinstance(pod_spec, Mapping) else None
        if not isinstance(containers, list) or not containers:
            reasons.append(f"{deployment_name} has no container specification")
            continue
        for container in containers:
            if not isinstance(container, Mapping):
                reasons.append(f"{deployment_name} contains an invalid container")
                continue
            container_name = str(container.get("name", "container"))
            resources_block = container.get("resources")
            if not isinstance(resources_block, Mapping):
                reasons.append(f"{deployment_name}/{container_name} has no resource protection")
            else:
                for resource_type in ("requests", "limits"):
                    values = resources_block.get(resource_type)
                    if not isinstance(values, Mapping) or not all(
                        key in values for key in ("cpu", "memory")
                    ):
                        reasons.append(
                            f"{deployment_name}/{container_name} lacks CPU/memory {resource_type}"
                        )
            lifecycle = container.get("lifecycle")
            prestop = lifecycle.get("preStop") if isinstance(lifecycle, Mapping) else None
            if not isinstance(prestop, Mapping):
                reasons.append(f"{deployment_name}/{container_name} has no preStop hook")
            elif deployment_name != "trpc-backlog-exporter":
                command = (
                    prestop.get("exec", {}).get("command", [])
                    if isinstance(prestop.get("exec"), Mapping)
                    else []
                )
                expected_drain = ["trpc-service", "drain", "--role", container_name]
                if command != expected_drain:
                    reasons.append(
                        f"{deployment_name}/{container_name} preStop does not call its exact drain"
                    )
            if deployment_name == "trpc-backlog-exporter":
                if container.get("command") != [
                    "python",
                    "scripts/session_ready_backlog_exporter.py",
                ]:
                    reasons.append(
                        "trpc-backlog-exporter does not run the fixed backlog exporter command"
                    )
                env = container.get("env")
                env_by_name = (
                    {
                        item.get("name"): item
                        for item in env
                        if isinstance(item, Mapping) and isinstance(item.get("name"), str)
                    }
                    if isinstance(env, list)
                    else {}
                )
                dsn_ref = env_by_name.get("TRPC_SERVICE_METRICS_DATABASE_DSN")
                dsn_source = dsn_ref.get("valueFrom") if isinstance(dsn_ref, Mapping) else None
                dsn_secret = (
                    dsn_source.get("secretKeyRef") if isinstance(dsn_source, Mapping) else None
                )
                if (
                    not isinstance(dsn_secret, Mapping)
                    or dsn_secret.get("name") != "trpc-metrics-secrets"
                    or dsn_secret.get("key") != "TRPC_SERVICE_METRICS_DATABASE_DSN"
                ):
                    reasons.append(
                        "trpc-backlog-exporter metrics DSN is not sourced from trpc-metrics-secrets"
                    )
                namespace_ref = env_by_name.get("TRPC_BACKLOG_NAMESPACE")
                namespace_source = (
                    namespace_ref.get("valueFrom") if isinstance(namespace_ref, Mapping) else None
                )
                field_ref = (
                    namespace_source.get("fieldRef")
                    if isinstance(namespace_source, Mapping)
                    else None
                )
                if (
                    not isinstance(field_ref, Mapping)
                    or field_ref.get("fieldPath") != "metadata.namespace"
                ):
                    reasons.append(
                        "trpc-backlog-exporter namespace is not sourced from metadata.namespace"
                    )
                ports = container.get("ports")
                if not isinstance(ports, list) or not any(
                    isinstance(port, Mapping)
                    and port.get("name") == "metrics"
                    and port.get("containerPort") == 9100
                    for port in ports
                ):
                    reasons.append("trpc-backlog-exporter does not expose metrics port 9100")
                for probe_name, expected_path in (
                    ("readinessProbe", "/health/ready"),
                    ("livenessProbe", "/health/live"),
                ):
                    probe = container.get(probe_name)
                    http_get = probe.get("httpGet") if isinstance(probe, Mapping) else None
                    if (
                        not isinstance(http_get, Mapping)
                        or http_get.get("path") != expected_path
                        or http_get.get("port") != "metrics"
                    ):
                        reasons.append(
                            f"trpc-backlog-exporter has no exact {probe_name} HTTP probe"
                        )
            elif deployment_name not in {"trpc-gateway", "trpc-admin"}:
                readiness = container.get("readinessProbe")
                liveness = container.get("livenessProbe")
                for probe_name, probe in (("readiness", readiness), ("liveness", liveness)):
                    command = (
                        probe.get("exec", {}).get("command", [])
                        if isinstance(probe, Mapping) and isinstance(probe.get("exec"), Mapping)
                        else []
                    )
                    expected_probe = [
                        "python",
                        "-m",
                        "trpc_service.probe",
                        "--role",
                        container_name,
                    ]
                    if probe_name == "liveness":
                        expected_probe.append("--liveness")
                    if command != expected_probe:
                        reasons.append(
                            f"{deployment_name}/{container_name} has no exact lightweight "
                            f"{probe_name} probe for its role"
                        )

            template_metadata = (
                template.get("metadata", {}) if isinstance(template, Mapping) else {}
            )
            labels = (
                template_metadata.get("labels", {})
                if isinstance(template_metadata, Mapping)
                else {}
            )
            if (
                isinstance(labels, Mapping)
                and str(labels.get("scheduler-version", "")).lower() == "v1"
            ):
                reasons.append(f"{deployment_name} contains a v1 scheduler label")
            for environment in (
                container.get("env", ()) if isinstance(container.get("env"), list) else ()
            ):
                if not isinstance(environment, Mapping):
                    continue
                if (
                    environment.get("name") == "TRPC_SERVICE_SCHEDULER_VERSION"
                    and environment.get("value") != _EXPECTED_SCHEDULER_VERSION
                ):
                    reasons.append(f"{deployment_name} contains a non-v2 scheduler environment")

    worker_hpa = resources.get(("HorizontalPodAutoscaler", "trpc-worker"))
    worker_spec = worker_hpa.get("spec") if isinstance(worker_hpa, Mapping) else None
    worker_max = worker_spec.get("maxReplicas") if isinstance(worker_spec, Mapping) else None
    config_data = config.get("data") if isinstance(config, Mapping) else None
    worker_concurrency = (
        config_data.get("TRPC_SERVICE_WORKER_CONCURRENCY")
        if isinstance(config_data, Mapping)
        else None
    )
    if isinstance(worker_max, (int, str)) and isinstance(worker_concurrency, (int, str)):
        try:
            capacity = int(worker_max) * int(worker_concurrency)
        except ValueError:
            capacity = 0
    else:
        capacity = 0
    required_capacity = 40 if local_kind else _MIN_PRODUCTION_TURN_CAPACITY
    if local_kind and worker_max != 4:
        reasons.append("local kind worker HPA maxReplicas must be 4")
    if capacity < required_capacity:
        reasons.append(f"worker capacity envelope {capacity} is below {required_capacity} turns")

    return not reasons, tuple(dict.fromkeys(reasons))


def _production_render_contract(
    rendered: str, *, allow_local_images: bool = False
) -> tuple[bool, tuple[str, ...]]:
    """Reject placeholder endpoints/refs from an actual production apply."""

    try:
        documents = [
            document for document in yaml.safe_load_all(rendered) if isinstance(document, Mapping)
        ]
    except yaml.YAMLError:
        return False, ("rendered production manifest is not valid YAML",)
    reasons: list[str] = []
    config = None
    for document in documents:
        metadata = document.get("metadata")
        if (
            document.get("kind") == "ConfigMap"
            and isinstance(metadata, Mapping)
            and metadata.get("name") == "trpc-service-config"
        ):
            config = document
            break
    data = config.get("data") if isinstance(config, Mapping) else None
    if not isinstance(data, Mapping):
        reasons.append("production ConfigMap is missing")
    else:
        for key in ("TRPC_SERVICE_S3_ENDPOINT", "TRPC_SERVICE_OIDC_ISSUER"):
            value = data.get(key)
            if (
                not isinstance(value, str)
                or not value.strip()
                or any(token in value.lower() for token in ("example.", "replace", "placeholder"))
            ):
                reasons.append(f"production ConfigMap contains a placeholder {key}")
    for document in documents:
        if document.get("kind") != "Deployment":
            continue
        spec = document.get("spec")
        template = spec.get("template") if isinstance(spec, Mapping) else None
        pod_spec = template.get("spec") if isinstance(template, Mapping) else None
        containers = pod_spec.get("containers", []) if isinstance(pod_spec, Mapping) else []
        for container in containers if isinstance(containers, list) else ():
            image = container.get("image") if isinstance(container, Mapping) else None
            if not allow_local_images:
                if not isinstance(image, str) or _registry_digest_reference(image) is None:
                    reasons.append(
                        "production Deployment image is not a registry-qualified "
                        "name@sha256:<64-hex-digest> reference"
                    )
    if allow_local_images:
        reasons = [reason for reason in reasons if "placeholder" not in reason]
    return not reasons, tuple(dict.fromkeys(reasons))


def _scheduler_runtime_contract(
    configmap: Mapping[str, Any] | None,
    deployments: Mapping[str, Any] | None,
) -> tuple[bool, tuple[str, ...]]:
    """Reject a live namespace that contains v1/v2 scheduler mixing."""

    reasons: list[str] = []
    config_data = configmap.get("data") if isinstance(configmap, Mapping) else None
    if not isinstance(config_data, Mapping):
        reasons.append("live scheduler ConfigMap is unavailable")
    else:
        expected = {
            "TRPC_SERVICE_SCHEDULER_VERSION": _EXPECTED_SCHEDULER_VERSION,
            "TRPC_SERVICE_REDIS_STREAM": _EXPECTED_REDIS_STREAM,
            "TRPC_SERVICE_REDIS_CONSUMER_GROUP": _EXPECTED_REDIS_GROUP,
        }
        for key, value in expected.items():
            if config_data.get(key) != value:
                reasons.append(f"live scheduler setting {key} is not v2")

    items = deployments.get("items") if isinstance(deployments, Mapping) else None
    if not isinstance(items, list) or not items:
        reasons.append("live scheduler deployment list is unavailable")
    else:
        explicit_versions: set[str] = set()
        for item in items:
            if not isinstance(item, Mapping):
                reasons.append("live scheduler deployment entry is invalid")
                continue
            metadata = item.get("metadata")
            name = str(metadata.get("name", "")) if isinstance(metadata, Mapping) else ""
            labels = metadata.get("labels", {}) if isinstance(metadata, Mapping) else {}
            template = (
                item.get("spec", {}).get("template", {})
                if isinstance(item.get("spec"), Mapping)
                else {}
            )
            template_metadata = (
                template.get("metadata", {}) if isinstance(template, Mapping) else {}
            )
            template_labels = (
                template_metadata.get("labels", {})
                if isinstance(template_metadata, Mapping)
                else {}
            )
            for label_set in (labels, template_labels):
                if isinstance(label_set, Mapping):
                    version = label_set.get("scheduler-version")
                    if version is not None:
                        explicit_versions.add(str(version).lower())
            containers = (
                template.get("spec", {}).get("containers", ())
                if isinstance(template, Mapping)
                else ()
            )
            for container in containers if isinstance(containers, list) else ():
                if not isinstance(container, Mapping):
                    continue
                for environment in (
                    container.get("env", ()) if isinstance(container.get("env"), list) else ()
                ):
                    if (
                        isinstance(environment, Mapping)
                        and environment.get("name") == "TRPC_SERVICE_SCHEDULER_VERSION"
                    ):
                        explicit_versions.add(str(environment.get("value", "")).lower())
            if "v1" in name.lower() or "v1" in explicit_versions:
                reasons.append(
                    f"live namespace contains v1 scheduler deployment {name or '<unknown>'}"
                )
        if "v1" in explicit_versions or len(explicit_versions) > 1:
            reasons.append("live namespace contains mixed scheduler versions")

    return not reasons, tuple(dict.fromkeys(reasons))


def _report(
    output: Path,
    *,
    gate: str,
    candidate: dict[str, Any],
    rejection_reasons: list[str],
) -> dict[str, Any]:
    if output.is_symlink() or any(parent.is_symlink() for parent in output.parents):
        raise ValueError("runtime report output must not be a symlink or contain a symlink parent")
    if output.exists() and not output.is_file():
        raise ValueError("runtime report output must be a regular file")
    checks = candidate.get("checks", {})
    failed = sum(
        1 for value in checks.values() if isinstance(value, dict) and value.get("status") == "fail"
    )
    not_run = sum(
        1
        for value in checks.values()
        if isinstance(value, dict) and value.get("status") == "not_run"
    )
    effective_gate = gate
    effective_rejection_reasons = list(rejection_reasons)
    if gate == "pass":
        runtime_ok, runtime_reasons = _runtime_attestation_contract(candidate)
        if not runtime_ok:
            effective_gate = "fail"
            effective_rejection_reasons.extend(runtime_reasons)
    image_digest = _candidate_image_digest(candidate)
    if image_digest is not None and effective_gate == "pass":
        candidate["lineage"] = {
            "status": "pass",
            "checkout_current": True,
            "producer": PRODUCER,
            "image_digest": image_digest,
        }
    runtime = None
    effective_production_gate = effective_gate
    effective_reasons = list(effective_rejection_reasons)
    if effective_gate == "pass" and image_digest is not None:
        runtime = runtime_fingerprint(
            mode="kubernetes_runtime",
            worker_identities=[image_digest],
            stream=_EXPECTED_REDIS_STREAM,
            group=_EXPECTED_REDIS_GROUP,
            parameters={
                "required_checks": len(_REQUIRED_RUNTIME_CHECKS),
                "image_digest": image_digest,
            },
        )
    elif effective_gate == "pass":
        # A live Kubernetes pass without immutable image identity is not
        # promotable evidence, even when all control-plane checks succeeded.
        effective_production_gate = "not_run"
        effective_reasons.append("Kubernetes image digest lineage is unavailable")
    evidence = build_evidence(root=ROOT, producer=PRODUCER, runtime=runtime)
    result: dict[str, Any] = {
        "schema_version": 1,
        "baseline": {
            "required_checks": [
                *_REQUIRED_RUNTIME_CHECKS,
            ],
            "required_runtime_actions": [
                *_REQUIRED_RUNTIME_ACTIONS,
            ],
            "node_eviction_required_for_production": True,
            "node_eviction_policy": {
                "confirmation": _NODE_DRAIN_CONFIRMATION,
                "label_key": _NODE_LABEL_KEY,
                "scope": "dedicated-node-only",
                "must_uncordon_after_test": True,
            },
            "hpa_load_policy": {
                "metric": "trpc_session_ready_backlog",
                "manual_scale_is_not_evidence": True,
                "required_phases": ["before", "during", "after"],
            },
            "rollback_policy": {
                "deployment": _ROLLBACK_PROBE_DEPLOYMENT,
                "failure_image_is_registry_local_and_unavailable": True,
                "requires_failed_rollout_undo_and_ready_recovery": True,
            },
            "static_manifest_checks_do_not_upgrade_runtime_gate": True,
        },
        "candidate": candidate,
        "case_deltas": {"failed_checks": failed, "not_run_checks": not_run},
        "gate": effective_gate,
        "production_gate": effective_production_gate,
        "rejection_reasons": effective_rejection_reasons,
        "production_rejection_reasons": effective_reasons,
        "run_id": evidence["run_id"],
        "evidence": evidence,
    }
    atomic_write_json(output, result)
    return result


def _candidate_image_digest(candidate: Mapping[str, Any]) -> str | None:
    """Extract one observed immutable image digest without exposing image refs."""

    checks = candidate.get("checks")
    if not isinstance(checks, Mapping):
        return None
    values: list[Any] = []
    initial = checks.get("initial_image_ids")
    if isinstance(initial, Mapping):
        values.extend(
            image_id
            for image_ids in initial.values()
            if isinstance(image_ids, list)
            for image_id in image_ids
        )
    rolling = checks.get("rolling_upgrade")
    image_ids = rolling.get("image_ids") if isinstance(rolling, Mapping) else None
    upgraded = image_ids.get("upgrade") if isinstance(image_ids, Mapping) else None
    if isinstance(upgraded, Mapping):
        values.extend(
            image_id
            for image_list in upgraded.values()
            if isinstance(image_list, list)
            for image_id in image_list
        )
    for value in values:
        if not isinstance(value, str):
            continue
        match = IMAGE_DIGEST_RE.search(value)
        if match is not None:
            return match.group(0).lower()
    return None


def _check_status(checks: Mapping[str, Any], name: str) -> str:
    """Normalize checks that are represented as an observation map."""

    value = checks.get(name)
    if isinstance(value, Mapping):
        status = value.get("status")
        if isinstance(status, str):
            return status
        if name == "initial_image_ids" and value:
            return "pass"
    return "not_run"


def _image_evidence_contract(
    checks: Mapping[str, Any],
    canonical_images: Mapping[str, Any],
    *,
    expected_deployments: set[str],
) -> tuple[bool, tuple[str, ...]]:
    """Bind every check image observation to the attestation's canonical map.

    The attestation is the producer's canonical runtime observation.  The
    duplicated check fields are useful evidence, but must never be allowed to
    carry a stale digest, a partial deployment set, or a forged ``changed``
    flag independently of that observation.
    """

    reasons: list[str] = []
    canonical: dict[str, Mapping[str, Any]] = {}
    for phase in ("initial", "upgrade"):
        value = canonical_images.get(phase)
        if not isinstance(value, Mapping) or set(value) != expected_deployments:
            reasons.append(f"runtime_attestation {phase} image IDs are not canonical")
            continue
        canonical[phase] = value
    if set(canonical) != {"initial", "upgrade"}:
        return False, tuple(dict.fromkeys(reasons))

    rolling = checks.get("rolling_upgrade")
    rolling_image_ids = rolling.get("image_ids") if isinstance(rolling, Mapping) else None
    if not isinstance(rolling_image_ids, Mapping):
        reasons.append("checks.rolling_upgrade.image_ids are missing")
        return False, tuple(dict.fromkeys(reasons))

    comparisons = (
        ("checks.initial_image_ids", checks.get("initial_image_ids"), canonical["initial"]),
        (
            "checks.rolling_upgrade.image_ids.initial",
            rolling_image_ids.get("initial"),
            canonical["initial"],
        ),
        (
            "checks.rolling_upgrade.image_ids.upgrade",
            rolling_image_ids.get("upgrade"),
            canonical["upgrade"],
        ),
    )
    for label, observed, expected in comparisons:
        if not isinstance(observed, Mapping) or set(observed) != expected_deployments:
            reasons.append(f"{label} deployment set does not match canonical image IDs")
        elif observed != expected:
            reasons.append(f"{label} does not match canonical image IDs")

    changed = rolling_image_ids.get("changed")
    expected_changed = {
        deployment: canonical["initial"][deployment] != canonical["upgrade"][deployment]
        for deployment in expected_deployments
    }
    if not isinstance(changed, Mapping) or set(changed) != expected_deployments:
        reasons.append("checks.rolling_upgrade.image_ids.changed deployment set is invalid")
    elif any(not isinstance(value, bool) for value in changed.values()):
        reasons.append("checks.rolling_upgrade.image_ids.changed values are not booleans")
    elif dict(changed) != expected_changed:
        reasons.append(
            "checks.rolling_upgrade.image_ids.changed does not match canonical image IDs"
        )
    return not reasons, tuple(dict.fromkeys(reasons))


def _runtime_attestation_contract(
    candidate: Mapping[str, Any], *, require_node_eviction: bool = True
) -> tuple[bool, tuple[str, ...]]:
    """Validate the evidence shape required before runtime promotion.

    A Kubernetes API call returning zero is not sufficient evidence by itself:
    the report must bind the observations to an isolated namespace, immutable
    pod image IDs and a cluster identity, and list every action that was
    actually observed.  Production promotion additionally requires an
    explicitly approved, dedicated-node cordon/drain and uncordon cycle;
    callers may disable that check only for a non-production diagnostic.
    """

    reasons: list[str] = []
    topology = candidate.get("topology")
    expected_runtime_deployments = [name for name, _container in ACK_RUNTIME_DEPLOYMENTS]
    if not isinstance(topology, Mapping):
        reasons.append("unified-cluster runtime topology is missing")
    else:
        if topology.get("scope") != ACK_RUNTIME_SCOPE:
            reasons.append("runtime topology scope is not unified_cluster_runtime")
        if topology.get("im_deployment") != ACK_IM_DEPLOYMENT:
            reasons.append("runtime topology does not bind IM to the production cluster")
        if topology.get("production_deployments") != [
            name for name, _container in PRODUCTION_DEPLOYMENTS
        ]:
            reasons.append("runtime topology production deployment set is invalid")
        if topology.get("tested_deployments") != expected_runtime_deployments:
            reasons.append("runtime topology tested deployment set is invalid")
        if topology.get("provider_disabled_deployments") != list(
            ACK_RUNTIME_PROVIDER_DISABLED_DEPLOYMENTS
        ):
            reasons.append("runtime topology provider-disabled deployment set is invalid")
    checks = candidate.get("checks")
    if not isinstance(checks, Mapping):
        return False, ("runtime checks are unavailable",)
    for name in _REQUIRED_RUNTIME_CHECKS:
        if _check_status(checks, name) != "pass":
            reasons.append(f"runtime check {name} is not pass")
    attestation = candidate.get("runtime_attestation")
    if not isinstance(attestation, Mapping):
        reasons.append("runtime_attestation is missing")
        return False, tuple(dict.fromkeys(reasons))
    if attestation.get("status") != "pass":
        reasons.append("runtime_attestation.status is not pass")
    if attestation.get("namespace_isolated") is not True:
        reasons.append("runtime_attestation.namespace_isolated is not true")
    candidate_namespace = candidate.get("namespace")
    if not isinstance(candidate_namespace, str) or not candidate_namespace:
        reasons.append("runtime candidate namespace identity is missing")
    elif attestation.get("namespace") != candidate_namespace:
        reasons.append("runtime_attestation namespace does not match this run")
    candidate_run_nonce = candidate.get("run_nonce")
    if (
        not isinstance(candidate_run_nonce, str)
        or re.fullmatch(r"[0-9a-f]{32}", candidate_run_nonce) is None
    ):
        reasons.append("runtime candidate run nonce is invalid")
    elif attestation.get("run_nonce") != candidate_run_nonce:
        reasons.append("runtime_attestation run nonce does not match this run")
    identity = attestation.get("cluster_identity")
    if not isinstance(identity, Mapping):
        reasons.append("runtime_attestation.cluster_identity is missing")
        cluster_fingerprint = None
    else:
        context_fingerprint = identity.get("context_sha256")
        if (
            not isinstance(context_fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", context_fingerprint) is None
        ):
            reasons.append("runtime_attestation cluster context fingerprint is invalid")
        fingerprint = identity.get("fingerprint_sha256")
        if not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
            reasons.append("runtime_attestation cluster identity fingerprint is invalid")
        if identity.get("server_observed") is not True:
            reasons.append("runtime_attestation did not observe an API server identity")
        cluster_fingerprint = identity.get("fingerprint_sha256")
    hpa_check = checks.get("hpa_load_observation")
    hpa_observation = hpa_check.get("observation") if isinstance(hpa_check, Mapping) else None
    hpa_ok, hpa_reasons = _hpa_load_observation_contract(
        hpa_observation,
        cluster_fingerprint=cluster_fingerprint if isinstance(cluster_fingerprint, str) else None,
        run_nonce=candidate_run_nonce if isinstance(candidate_run_nonce, str) else None,
        namespace=candidate_namespace if isinstance(candidate_namespace, str) else None,
    )
    if not hpa_ok:
        reasons.extend(f"runtime_attestation HPA evidence: {reason}" for reason in hpa_reasons)
    node_identity = attestation.get("node_identity")
    if (
        not isinstance(node_identity, Mapping)
        or re.fullmatch(r"[0-9a-f]{64}", str(node_identity.get("fingerprint_sha256", ""))) is None
    ):
        reasons.append("runtime_attestation controlled node identity is invalid")
    actions = attestation.get("actions")
    if not isinstance(actions, Mapping):
        reasons.append("runtime_attestation.actions is missing")
    else:
        for action in _REQUIRED_RUNTIME_ACTIONS:
            if actions.get(action) is not True:
                reasons.append(f"runtime action {action} was not observed")
        if require_node_eviction and actions.get("node_eviction") is not True:
            reasons.append("node eviction was not observed")
    if attestation.get("eviction_scope") != "namespace_pod_eviction+controlled_node":
        reasons.append(
            "runtime_attestation eviction scope is not namespace_pod_eviction+controlled_node"
        )
    node_eviction_status = attestation.get("node_eviction_status")
    if node_eviction_status not in {"not_run", "pass"}:
        reasons.append("runtime_attestation node eviction status is invalid")
    if require_node_eviction and node_eviction_status != "pass":
        reasons.append("runtime_attestation node eviction status is not pass")
    image_ids = attestation.get("image_ids")
    expected_deployments = {name for name, _container in ACK_RUNTIME_DEPLOYMENTS}
    validated_image_ids: dict[str, dict[str, str]] = {}
    if (
        not isinstance(image_ids, Mapping)
        or not image_ids.get("initial")
        or not image_ids.get("upgrade")
    ):
        reasons.append("runtime_attestation immutable initial/upgrade image IDs are missing")
    else:
        for phase in ("initial", "upgrade"):
            observations = image_ids.get(phase)
            if not isinstance(observations, Mapping) or set(observations) != expected_deployments:
                reasons.append(f"runtime_attestation {phase} image IDs are not immutable digests")
                continue
            validated_phase: dict[str, str] = {}
            for deployment in expected_deployments:
                value = observations.get(deployment)
                if (
                    not isinstance(value, list)
                    or len(value) != 1
                    or not isinstance(value[0], str)
                    or IMAGE_DIGEST_RE.fullmatch(value[0]) is None
                ):
                    reasons.append(
                        f"runtime_attestation {phase} image IDs for {deployment} "
                        "must contain one immutable digest"
                    )
                    continue
                validated_phase[deployment] = value[0]
            validated_image_ids[phase] = validated_phase
        initial_ids = validated_image_ids.get("initial", {})
        upgrade_ids = validated_image_ids.get("upgrade", {})
        if set(initial_ids) == expected_deployments and set(upgrade_ids) == expected_deployments:
            for deployment in expected_deployments:
                if initial_ids[deployment] == upgrade_ids[deployment]:
                    reasons.append(
                        f"runtime_attestation rolling image ID did not change for {deployment}"
                    )
        image_ok, image_reasons = _image_evidence_contract(
            checks,
            image_ids,
            expected_deployments=expected_deployments,
        )
        if not image_ok:
            reasons.extend(image_reasons)
    rolling = checks.get("rolling_upgrade")
    rollback = rolling.get("rollback") if isinstance(rolling, Mapping) else None
    if not isinstance(rollback, Mapping) or rollback.get("status") != "pass":
        reasons.append("rolling upgrade failure rollback was not observed")
    else:
        if rollback.get("deployment") != _ROLLBACK_PROBE_DEPLOYMENT:
            reasons.append("rolling upgrade rollback probe deployment is invalid")
        for field in (
            "failure_injected",
            "failure_observed",
            "undo_observed",
            "readiness_recovered",
        ):
            if rollback.get(field) is not True:
                reasons.append(f"rolling upgrade rollback {field} was not observed")
        restored = rollback.get("restored_image_ids")
        expected_upgrade = None
        if isinstance(rolling, Mapping):
            rolling_images = rolling.get("image_ids")
            if isinstance(rolling_images, Mapping):
                upgrade = rolling_images.get("upgrade")
                if isinstance(upgrade, Mapping):
                    expected_upgrade = upgrade.get(_ROLLBACK_PROBE_DEPLOYMENT)
        if not isinstance(restored, list) or restored != expected_upgrade:
            reasons.append(
                "rolling upgrade rollback did not restore the known-good worker image IDs"
            )
    return not reasons, tuple(dict.fromkeys(reasons))


def _cluster_identity(stdout: str, context: str | None) -> dict[str, Any]:
    """Return a report-safe hash of the observed API server identity."""

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        payload = {}
    server = payload.get("serverVersion") if isinstance(payload, Mapping) else None
    if not isinstance(server, Mapping):
        server = {}
    identity = "|".join(str(server.get(key, "")) for key in ("gitVersion", "gitCommit", "platform"))
    observed = bool(identity.strip("|"))
    if not observed:
        identity = "unknown-api-server"
    return {
        "context_sha256": hashlib.sha256(
            (context or "current-context").encode("utf-8")
        ).hexdigest(),
        "fingerprint_sha256": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
        "server_observed": observed,
    }


def _safe_hpa_driver_path(driver_path: str) -> tuple[Path | None, str | None]:
    """Resolve the load trigger to a regular, repository-owned Python file."""

    path = Path(driver_path)
    if not path.is_absolute():
        return None, "HPA driver path must be absolute"
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
        return None, "HPA driver path must not use symlinks"
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to((ROOT / "scripts").resolve())
    except (OSError, ValueError):
        return None, "HPA driver path must resolve beneath the repository scripts directory"
    if not resolved.is_file() or resolved.suffix.lower() != ".py":
        return None, "HPA driver must be a regular repository Python script"
    return resolved, None


def _safe_hpa_driver_kubeconfig(value: str) -> tuple[Path | None, str | None]:
    """Resolve a dedicated kubeconfig and reject path/content aliasing.

    ``KUBECONFIG`` accepts a path list.  Comparing only the first resolved
    path misses a second entry, hard links, and copied gate credentials.  The
    driver must therefore be a regular file with a distinct inode *and* a
    distinct byte digest from every administrative kubeconfig entry.
    """

    path = Path(value)
    if not path.is_absolute():
        return None, "HPA driver kubeconfig path must be absolute"
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
        return None, "HPA driver kubeconfig path must not use symlinks"
    try:
        resolved = path.resolve(strict=True)
        stat = resolved.stat()
    except OSError:
        return None, "HPA driver kubeconfig path is missing"
    if not resolved.is_file() or stat.st_size <= 0 or stat.st_size > HPA_DRIVER_MAX_BYTES:
        return None, "HPA driver kubeconfig must be a bounded regular file"
    main_value = os.environ.get("KUBECONFIG", "").strip()
    raw_paths = [part for part in main_value.split(os.pathsep) if part] if main_value else []
    if not raw_paths:
        raw_paths = [str(Path.home() / ".kube" / "config")]
    try:
        driver_digest = hashlib.sha256(resolved.read_bytes()).hexdigest()
    except OSError:
        return None, "HPA driver kubeconfig could not be read"
    for raw_path in raw_paths:
        main_path = Path(raw_path).expanduser()
        try:
            main_resolved = main_path.resolve(strict=False)
            if main_resolved == resolved or (
                main_resolved.exists() and os.path.samefile(main_resolved, resolved)
            ):
                return None, "HPA driver kubeconfig must be distinct from the gate kubeconfig"
            if (
                main_resolved.is_file()
                and hashlib.sha256(main_resolved.read_bytes()).hexdigest() == driver_digest
            ):
                return None, "HPA driver kubeconfig must not reuse gate credentials"
        except OSError:
            # A missing secondary KUBECONFIG entry is handled by the main
            # kubectl client; it must not make this identity comparison unsafe.
            continue
    return resolved, None


def _validate_hpa_driver_context(value: str) -> str | None:
    context = value.strip()
    if not context:
        return "HPA driver context is required"
    if len(context) > 128 or any(
        character.isspace() or character in "\x00\r\n" for character in context
    ):
        return "HPA driver context is invalid"
    return None


def _hpa_job_name(run_nonce: str) -> str:
    return f"trpc-hpa-load-{run_nonce[:20]}"


def _strict_json_object(value: str, *, description: str) -> dict[str, Any] | None:
    del description

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    try:
        decoded = json.loads(
            value,
            object_pairs_hook=pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON value: {token}")
            ),
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _hpa_driver_identity(
    driver_path: str,
    *,
    expected_sha256: str,
    kubeconfig_path: str,
) -> tuple[dict[str, str] | None, str | None]:
    resolved_driver, driver_error = _safe_hpa_driver_path(driver_path)
    if resolved_driver is None:
        return None, driver_error or "invalid HPA driver path"
    expected = expected_sha256.strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", expected) is None or expected in {"0" * 64, "f" * 64}:
        return None, "HPA driver SHA-256 is invalid"
    try:
        driver_bytes = resolved_driver.read_bytes()
    except OSError:
        return None, "HPA driver could not be read"
    if not driver_bytes or len(driver_bytes) > HPA_DRIVER_MAX_BYTES:
        return None, "HPA driver size is invalid"
    actual = hashlib.sha256(driver_bytes).hexdigest()
    if not hmac.compare_digest(actual, expected):
        return None, "HPA driver SHA-256 does not match the repository script"
    resolved_kubeconfig, kubeconfig_error = _safe_hpa_driver_kubeconfig(kubeconfig_path)
    if resolved_kubeconfig is None:
        return None, kubeconfig_error or "invalid HPA driver kubeconfig"
    try:
        kubeconfig_sha256 = hashlib.sha256(resolved_kubeconfig.read_bytes()).hexdigest()
    except OSError:
        return None, "HPA driver kubeconfig could not be read"
    return {
        "driver_sha256": actual,
        "kubeconfig_sha256": kubeconfig_sha256,
        "driver_path": str(resolved_driver),
        "kubeconfig_path": str(resolved_kubeconfig),
    }, None


def _parse_hpa_driver_subject(value: str) -> tuple[str, str] | None:
    match = re.fullmatch(
        r"system:serviceaccount:"
        r"([a-z0-9](?:[-a-z0-9.]{0,61}[a-z0-9])?):"
        r"([a-z0-9](?:[-a-z0-9.]{0,61}[a-z0-9])?)",
        value,
    )
    return (match.group(1), match.group(2)) if match else None


def _bind_hpa_driver_rbac(
    *,
    subject: str,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> CommandResult:
    parsed = _parse_hpa_driver_subject(subject)
    if parsed is None:
        return CommandResult(status="not_run", reason="HPA driver subject is invalid")
    subject_namespace, subject_name = parsed
    manifest = yaml.safe_dump_all(
        [
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": _HPA_DRIVER_ROLE_NAME, "namespace": namespace},
                "rules": [
                    {
                        "apiGroups": ["batch"],
                        "resources": ["jobs"],
                        "verbs": ["create", "get", "delete"],
                    },
                    {
                        "apiGroups": [""],
                        "resources": ["pods", "pods/log"],
                        "verbs": ["get"],
                    },
                ],
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "trpc-hpa-load-driver", "namespace": namespace},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": _HPA_DRIVER_ROLE_NAME,
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": subject_name,
                        "namespace": subject_namespace,
                    }
                ],
            },
        ],
        sort_keys=True,
    )
    return _kubectl(
        ["apply", "--server-side", "--namespace", namespace, "-f", "-"],
        context=context,
        timeout_seconds=timeout_seconds,
        input_text=manifest,
    )


def _minimal_driver_environment(*, kubeconfig_path: str) -> dict[str, str]:
    """Build the bounded environment shared by the driver and its kubectl calls."""

    environment = {
        "PATH": os.environ.get("PATH", ""),
        "KUBECONFIG": kubeconfig_path,
        "HOME": os.environ.get("HOME", ""),
        "USERPROFILE": os.environ.get("USERPROFILE", ""),
    }
    # Windows kubectl needs these OS runtime variables to initialize its
    # socket/provider stack.  Keep the environment explicit and add only
    # values that are actually present; no arbitrary parent secrets leak in.
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value is not None:
            environment[name] = value
    return environment


def _run_hpa_driver(
    driver_path: str,
    *,
    namespace: str,
    run_nonce: str,
    cluster_fingerprint: str,
    context: str | None,
    timeout_seconds: float,
    phase: str = "load",
    evidence_path: Path | None = None,
    driver_context: str | None = None,
    driver_subject: str | None = None,
) -> CommandResult:
    """Run the bounded Job driver and verify its API-side causal evidence."""

    del evidence_path
    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    if phase not in HPA_DRIVER_PHASES:
        return CommandResult(status="not_run", reason="HPA driver phase is not allowed")
    identity, identity_error = _hpa_driver_identity(
        driver_path,
        expected_sha256=os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256", ""),
        kubeconfig_path=os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG", ""),
    )
    if identity is None:
        return CommandResult(
            status="not_run", reason=identity_error or "invalid HPA driver identity"
        )
    if driver_context is not None:
        context_error = _validate_hpa_driver_context(driver_context)
        if context_error:
            return CommandResult(status="not_run", reason=context_error)
    effective_driver_context = driver_context or context
    command = [sys.executable, identity["driver_path"]]
    environment = _minimal_driver_environment(kubeconfig_path=identity["kubeconfig_path"])
    environment.update(
        {
            "TRPC_K8S_HPA_RUN_NONCE": run_nonce,
            "TRPC_K8S_HPA_NAMESPACE": namespace,
            "TRPC_K8S_HPA_CLUSTER_FINGERPRINT": cluster_fingerprint,
            "TRPC_K8S_HPA_PHASE": phase,
            "TRPC_K8S_HPA_DRIVER_SUBJECT": str(
                driver_subject or os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT", "") or ""
            ),
            "TRPC_K8S_HPA_DRIVER_JOB_IMAGE": os.getenv(
                "TRPC_K8S_RUNTIME_HPA_JOB_IMAGE", os.getenv("TRPC_K8S_RUNTIME_IMAGE", "")
            ),
            "TRPC_K8S_HPA_DRIVER_JOB_COMMAND": os.getenv("TRPC_K8S_RUNTIME_HPA_JOB_COMMAND", ""),
        }
    )
    image_pull_secret = os.getenv("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET", "").strip()
    if image_pull_secret:
        environment["TRPC_K8S_HPA_DRIVER_IMAGE_PULL_SECRET"] = image_pull_secret
    if effective_driver_context:
        environment["TRPC_K8S_HPA_CONTEXT"] = effective_driver_context
    before_job: dict[str, Any] | None = None
    if phase == "clear":
        before_result, before_job = _observe_hpa_driver_job(
            namespace=namespace,
            run_nonce=run_nonce,
            cluster_fingerprint=cluster_fingerprint,
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        if before_result.status != "pass":
            return CommandResult(
                status="not_run",
                reason=before_result.reason
                or "HPA load Job API evidence was unavailable before clear",
            )
    try:
        completed = subprocess.run(  # noqa: S603 - explicit operator-selected driver, no shell
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(status="fail", reason="HPA load driver timed out")
    except OSError:
        return CommandResult(status="fail", reason="HPA load driver could not start")
    if completed.returncode != 0:
        return CommandResult(
            status="fail", exit_code=completed.returncode, reason="HPA driver returned non-zero"
        )
    driver_payload = _strict_json_object(completed.stdout, description="HPA driver output")
    if driver_payload is None:
        return CommandResult(
            status="fail",
            exit_code=completed.returncode,
            reason="HPA driver output is not strict JSON",
        )
    if (
        driver_payload.get("status") != "pass"
        or driver_payload.get("phase") != phase
        or driver_payload.get("namespace") != namespace
        or driver_payload.get("run_nonce") != run_nonce
        or driver_payload.get("cluster_fingerprint") != cluster_fingerprint
    ):
        return CommandResult(
            status="fail",
            exit_code=completed.returncode,
            reason="HPA driver output is not bound to this run",
        )
    if phase == "load":
        api_result, api_evidence = _observe_hpa_driver_job(
            namespace=namespace,
            run_nonce=run_nonce,
            cluster_fingerprint=cluster_fingerprint,
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        if api_result.status != "pass" or api_evidence is None:
            return CommandResult(
                status="fail",
                exit_code=completed.returncode,
                reason=api_result.reason or "HPA load Job API evidence is missing",
            )
        if driver_payload.get("job_uid") != api_evidence.get("job_uid"):
            return CommandResult(
                status="fail",
                exit_code=completed.returncode,
                reason="HPA driver Job UID is not API verified",
            )
        evidence = api_evidence
    else:
        delete_result = _hpa_job_absent(
            namespace=namespace,
            run_nonce=run_nonce,
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        if delete_result.status != "pass":
            return CommandResult(
                status="fail",
                exit_code=completed.returncode,
                reason=delete_result.reason or "HPA load Job deletion was not API verified",
            )
        evidence = dict(before_job or {})
        evidence["job_deleted"] = True
        evidence["api_observed"] = True
        if driver_payload.get("job_uid") not in {None, evidence.get("job_uid")}:
            return CommandResult(
                status="fail",
                exit_code=completed.returncode,
                reason="HPA clear Job UID is not API verified",
            )
    return CommandResult(status="pass", exit_code=completed.returncode, evidence=evidence)


def _driver_kubectl(
    arguments: list[str],
    *,
    kubeconfig_path: str,
    context: str | None,
    timeout_seconds: float,
    input_text: str | None = None,
) -> CommandResult:
    executable = shutil.which("kubectl")
    if executable is None:
        return CommandResult(status="not_run", reason="kubectl is not installed")
    command = [executable, "--kubeconfig", kubeconfig_path]
    if context:
        command.extend(["--context", context])
    command.extend(arguments)
    environment = _minimal_driver_environment(kubeconfig_path=kubeconfig_path)
    try:
        completed = subprocess.run(  # noqa: S603 - fixed kubectl argv, no shell
            command,
            check=False,
            capture_output=True,
            text=True,
            input=input_text,
            timeout=_validate_timeout_seconds(timeout_seconds),
            env=environment,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(status="fail", reason="driver kubectl command timed out")
    except OSError:
        return CommandResult(status="not_run", reason="driver kubectl command could not start")
    return CommandResult(
        status="pass" if completed.returncode == 0 else "fail",
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _driver_json(
    arguments: list[str],
    *,
    kubeconfig_path: str,
    context: str | None,
    timeout_seconds: float,
    input_text: str | None = None,
) -> tuple[CommandResult, dict[str, Any] | None]:
    result = _driver_kubectl(
        arguments,
        kubeconfig_path=kubeconfig_path,
        context=context,
        timeout_seconds=timeout_seconds,
        input_text=input_text,
    )
    if result.status != "pass":
        return result, None
    payload = _strict_json_object(result.stdout, description="driver kubectl JSON")
    if payload is None:
        return CommandResult(status="fail", reason="driver kubectl returned invalid JSON"), None
    return result, payload


def _driver_identity_and_scope(
    *,
    kubeconfig_path: str,
    driver_context: str,
    admin_context: str | None,
    subject: str,
    namespace: str,
    cluster_fingerprint: str,
    timeout_seconds: float,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Use server-side identity, scoped SSRR, and admin-side binding audit."""

    reasons: list[str] = []
    attestation: dict[str, Any] = {
        "driver_context_sha256": hashlib.sha256(driver_context.encode("utf-8")).hexdigest(),
        "identity_verified": False,
        "subject_sha256": hashlib.sha256(subject.encode("utf-8")).hexdigest(),
        "rule_audit": {"complete": False},
    }
    whoami_result, whoami = _driver_json(
        ["auth", "whoami", "-o", "json"],
        kubeconfig_path=kubeconfig_path,
        context=driver_context,
        timeout_seconds=timeout_seconds,
    )
    status = whoami.get("status") if isinstance(whoami, Mapping) else None
    user_info = status.get("userInfo") if isinstance(status, Mapping) else None
    actual_subject = user_info.get("username") if isinstance(user_info, Mapping) else None
    if whoami_result.status != "pass" or actual_subject != subject:
        reasons.append("SelfSubjectReview identity does not match declared HPA driver subject")
    else:
        attestation["identity_verified"] = True

    version_result, version = _driver_json(
        ["version", "--request-timeout=10s", "-o", "json"],
        kubeconfig_path=kubeconfig_path,
        context=driver_context,
        timeout_seconds=timeout_seconds,
    )
    if version_result.status != "pass" or version is None:
        reasons.append("driver Kubernetes API identity could not be observed")
    else:
        server = version.get("serverVersion")
        server_map = server if isinstance(server, Mapping) else {}
        identity = "|".join(
            str(server_map.get(key, "")) for key in ("gitVersion", "gitCommit", "platform")
        )
        if not identity.strip("|"):
            identity = "unknown-api-server"
        observed_fingerprint = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        attestation["cluster_fingerprint_sha256"] = observed_fingerprint
        if observed_fingerprint != cluster_fingerprint:
            reasons.append("driver Kubernetes API fingerprint differs from gate cluster")

    expected_rules = {
        ("batch", "jobs", "create"),
        ("batch", "jobs", "get"),
        ("batch", "jobs", "delete"),
        ("", "pods", "get"),
        ("", "pods/log", "get"),
    }

    def review(
        review_namespace: str,
    ) -> tuple[
        set[tuple[str, str, str]] | None,
        set[tuple[str, str]] | None,
        bool,
        str | None,
    ]:
        if not review_namespace:
            return None, None, False, "SelfSubjectRulesReview namespace is required"
        body = json.dumps(
            {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectRulesReview",
                "spec": {"namespace": review_namespace},
            },
            separators=(",", ":"),
        )
        status_value: Mapping[str, Any] | None = None
        for attempt in range(3):
            result, payload = _driver_json(
                [
                    "create",
                    "--raw",
                    "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
                    "-f",
                    "-",
                ],
                kubeconfig_path=kubeconfig_path,
                context=driver_context,
                timeout_seconds=timeout_seconds,
                input_text=body,
            )
            candidate_status = payload.get("status") if isinstance(payload, Mapping) else None
            if result.status == "pass" and isinstance(candidate_status, Mapping):
                status_value = candidate_status
                break
            if attempt < 2:
                time.sleep(0.25)
        if status_value is None:
            return None, None, False, "SelfSubjectRulesReview could not be observed"
        if status_value.get("incomplete") is True:
            return None, None, False, "SelfSubjectRulesReview is incomplete"
        resource_rules = status_value.get("resourceRules")
        non_resource_rules = status_value.get("nonResourceRules")
        if not isinstance(resource_rules, list) or not isinstance(non_resource_rules, list):
            return None, None, False, "SelfSubjectRulesReview returned an incomplete rule shape"
        expanded: set[tuple[str, str, str]] = set()
        for rule in resource_rules:
            if not isinstance(rule, Mapping):
                return None, None, False, "SelfSubjectRulesReview has an invalid resource rule"
            groups, resources, verbs = (
                rule.get(key) for key in ("apiGroups", "resources", "verbs")
            )
            if not all(isinstance(value, list) and value for value in (groups, resources, verbs)):
                return None, None, False, "SelfSubjectRulesReview has an invalid resource rule"
            group_values = cast(list[Any], groups)
            resource_values = cast(list[Any], resources)
            verb_values = cast(list[Any], verbs)
            if any(item == "*" for item in (*group_values, *resource_values, *verb_values)):
                return None, None, False, "HPA driver rule contains a wildcard"
            if rule.get("resourceNames") not in (None, []):
                return None, None, False, "HPA driver rule is not resource-unbounded as expected"
            expanded.update(
                (str(group), str(resource), str(verb))
                for group in group_values
                for resource in resource_values
                for verb in verb_values
            )
        operational = expanded - _HPA_DRIVER_IDENTITY_RULES
        non_resource_pairs: set[tuple[str, str]] = set()
        for rule in non_resource_rules:
            if not isinstance(rule, Mapping):
                return None, None, False, "SelfSubjectRulesReview has an invalid non-resource rule"
            urls = rule.get("nonResourceURLs")
            verbs = rule.get("verbs")
            if (
                not isinstance(urls, list)
                or not urls
                or not all(isinstance(url, str) and url for url in urls)
            ):
                return None, None, False, "SelfSubjectRulesReview has an invalid non-resource rule"
            if not isinstance(verbs, list) or not verbs or any(verb != "get" for verb in verbs):
                return None, None, False, "HPA driver non-resource rule is not GET-only"
            if any(url not in _HPA_DRIVER_DISCOVERY_URLS for url in urls):
                return None, None, False, "HPA driver non-resource URL is not allowlisted"
            non_resource_pairs.update((url, verb) for url in urls for verb in verbs)
        return operational, non_resource_pairs, True, None

    target_rules, target_non_resource, target_complete, target_error = review(namespace)
    if not target_complete or target_rules != expected_rules:
        reasons.append(
            target_error or "HPA driver target namespace rules are broader or incomplete"
        )
    hashes: dict[str, str] = {}
    if target_rules is not None:
        hashes["target_rules_sha256"] = hashlib.sha256(
            json.dumps(sorted(target_rules), separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    if target_non_resource is not None:
        hashes["target_non_resource_rules_sha256"] = hashlib.sha256(
            json.dumps(sorted(target_non_resource), separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    for name, review_namespace in (("default", "default"), ("kube_system", "kube-system")):
        rules, non_resource, complete, error = review(review_namespace)
        if not complete or rules:
            reasons.append(error or f"HPA driver has permissions outside target namespace ({name})")
        if rules is not None:
            hashes[f"{name}_rules_sha256"] = hashlib.sha256(
                json.dumps(sorted(rules), separators=(",", ":")).encode("utf-8")
            ).hexdigest()
        if non_resource is not None:
            hashes[f"{name}_non_resource_rules_sha256"] = hashlib.sha256(
                json.dumps(sorted(non_resource), separators=(",", ":")).encode("utf-8")
            ).hexdigest()

    binding_audit: dict[str, Any] = {
        "complete": False,
        "matching_rolebinding_count": 0,
        "matching_clusterrolebinding_count": 0,
    }
    parsed_subject = _parse_hpa_driver_subject(subject)
    if not admin_context:
        reasons.append("admin Kubernetes context is required for HPA driver binding audit")
    elif parsed_subject is None:
        reasons.append("HPA driver subject is invalid for binding audit")
    else:
        subject_namespace, subject_name = parsed_subject
        rolebinding_result, rolebindings = _json_command(
            ["get", "rolebindings", "--all-namespaces", "-o", "json"],
            context=admin_context,
            timeout_seconds=timeout_seconds,
        )
        clusterrolebinding_result, clusterrolebindings = _json_command(
            ["get", "clusterrolebindings", "-o", "json"],
            context=admin_context,
            timeout_seconds=timeout_seconds,
        )
        if rolebinding_result.status != "pass" or rolebindings is None:
            reasons.append("admin context could not enumerate HPA driver RoleBindings")
        if clusterrolebinding_result.status != "pass" or clusterrolebindings is None:
            reasons.append("admin context could not enumerate HPA driver ClusterRoleBindings")
        if rolebinding_result.status == "pass" and isinstance(rolebindings, Mapping):
            matching_rolebindings = []
            for item in rolebindings.get("items", []):
                if not isinstance(item, Mapping):
                    continue
                subjects = item.get("subjects")
                if not isinstance(subjects, list) or not any(
                    isinstance(value, Mapping)
                    and value.get("kind") == "ServiceAccount"
                    and value.get("namespace") == subject_namespace
                    and value.get("name") == subject_name
                    for value in subjects
                ):
                    continue
                metadata = item.get("metadata")
                role_ref = item.get("roleRef")
                matching_rolebindings.append(
                    {
                        "namespace": metadata.get("namespace")
                        if isinstance(metadata, Mapping)
                        else None,
                        "role_api_group": role_ref.get("apiGroup")
                        if isinstance(role_ref, Mapping)
                        else None,
                        "role_kind": role_ref.get("kind")
                        if isinstance(role_ref, Mapping)
                        else None,
                        "role_name": role_ref.get("name")
                        if isinstance(role_ref, Mapping)
                        else None,
                    }
                )
            binding_audit["matching_rolebinding_count"] = len(matching_rolebindings)
            if len(matching_rolebindings) != 1:
                reasons.append("HPA driver subject has extra or missing RoleBindings")
            elif matching_rolebindings[0] != {
                "namespace": namespace,
                "role_api_group": "rbac.authorization.k8s.io",
                "role_kind": "Role",
                "role_name": _HPA_DRIVER_ROLE_NAME,
            }:
                reasons.append("HPA driver RoleBinding is outside the declared target Role")
        if clusterrolebinding_result.status == "pass" and isinstance(clusterrolebindings, Mapping):
            matching_clusterrolebindings = []
            for item in clusterrolebindings.get("items", []):
                if not isinstance(item, Mapping):
                    continue
                subjects = item.get("subjects")
                if isinstance(subjects, list) and any(
                    isinstance(value, Mapping)
                    and value.get("kind") == "ServiceAccount"
                    and value.get("namespace") == subject_namespace
                    and value.get("name") == subject_name
                    for value in subjects
                ):
                    matching_clusterrolebindings.append(item)
            binding_audit["matching_clusterrolebinding_count"] = len(matching_clusterrolebindings)
            if matching_clusterrolebindings:
                reasons.append("HPA driver subject has a ClusterRoleBinding")
        if (
            rolebinding_result.status == "pass"
            and clusterrolebinding_result.status == "pass"
            and binding_audit["matching_rolebinding_count"] == 1
            and binding_audit["matching_clusterrolebinding_count"] == 0
            and not any(
                reason
                in {
                    "HPA driver subject has extra or missing RoleBindings",
                    "HPA driver RoleBinding is outside the declared target Role",
                    "HPA driver subject has a ClusterRoleBinding",
                }
                for reason in reasons
            )
        ):
            binding_audit["complete"] = True

    # Keep the release evidence field while proving cluster scope through the
    # admin binding enumeration above; an empty-namespace SSRR is invalid.
    hashes["cluster_rules_sha256"] = hashlib.sha256(
        json.dumps([], separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    attestation["rule_audit"] = {
        "complete": not reasons,
        "scope": "target_namespace_jobs_pods_only",
        "target_namespace": namespace,
        "binding_audit": binding_audit,
        **hashes,
    }
    return not reasons, reasons, attestation


def _observe_hpa_driver_job(
    *,
    namespace: str,
    run_nonce: str,
    cluster_fingerprint: str,
    context: str | None,
    timeout_seconds: float,
) -> tuple[CommandResult, dict[str, Any] | None]:
    result, payload = _json_command(
        ["get", "job", _hpa_job_name(run_nonce), "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "pass" or payload is None:
        return result, None
    metadata = payload.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    labels = metadata_map.get("labels")
    expected_labels = {
        HPA_DRIVER_OWNER_LABEL: HPA_DRIVER_OWNER_VALUE,
        HPA_DRIVER_RUN_LABEL: run_nonce,
        HPA_DRIVER_PHASE_LABEL: "load",
        HPA_DRIVER_CLUSTER_LABEL: cluster_fingerprint[:63],
    }
    uid = metadata_map.get("uid")
    if (
        metadata_map.get("name") != _hpa_job_name(run_nonce)
        or metadata_map.get("namespace") != namespace
        or not isinstance(uid, str)
        or not uid
        or not isinstance(labels, Mapping)
        or any(labels.get(key) != value for key, value in expected_labels.items())
    ):
        return CommandResult(
            status="fail", reason="HPA load Job API object is not nonce-labelled"
        ), None
    return CommandResult(status="pass"), {
        "api_observed": True,
        "job_name": _hpa_job_name(run_nonce),
        "job_uid": uid,
        "job_labels": dict(expected_labels),
        "namespace": namespace,
        "run_nonce": run_nonce,
        "cluster_fingerprint": cluster_fingerprint,
        "phase": "load",
    }


def _hpa_job_absent(
    *, namespace: str, run_nonce: str, context: str | None, timeout_seconds: float
) -> CommandResult:
    result = _kubectl(
        ["get", "job", _hpa_job_name(run_nonce), "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if result.status == "fail" and (
        "notfound" in result.stderr.lower() or "not found" in result.stderr.lower()
    ):
        return CommandResult(status="pass")
    if result.status == "pass":
        return CommandResult(status="fail", reason="HPA load Job still exists after clear")
    return CommandResult(status="fail", reason="HPA load Job deletion could not be verified")


def _hpa_driver_scope_contract(
    *,
    kubeconfig_path: str,
    context: str | None,
    namespace: str,
    timeout_seconds: float,
    driver_context: str | None = None,
    subject: str | None = None,
    cluster_fingerprint: str | None = None,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Verify the real identity and complete SSR rule sets, fail closed."""

    context_value = driver_context or context
    context_error = _validate_hpa_driver_context(context_value or "")
    if context_error:
        return False, [context_error], {"rule_audit": {"complete": False}}
    if not subject or not cluster_fingerprint:
        return (
            False,
            ["HPA driver subject and cluster fingerprint are required"],
            {"rule_audit": {"complete": False}},
        )
    return _driver_identity_and_scope(
        kubeconfig_path=kubeconfig_path,
        driver_context=context_value or "",
        admin_context=context,
        subject=subject,
        namespace=namespace,
        cluster_fingerprint=cluster_fingerprint,
        timeout_seconds=timeout_seconds,
    )


def _parse_controlled_node_label(value: str) -> tuple[str, str] | None:
    key, separator, label_value = value.partition("=")
    if separator != "=" or key != _NODE_LABEL_KEY or not label_value:
        return None
    if any(character.isspace() for character in value):
        return None
    return key, label_value


def _pdb_selector_matches_pod(
    selector: object,
    labels: object,
) -> bool:
    """Match the conservative PDB selector subset used for safe node drains."""

    if not isinstance(selector, Mapping) or not isinstance(labels, Mapping):
        return False
    match_labels = selector.get("matchLabels")
    if not isinstance(match_labels, Mapping) or not match_labels:
        return False
    match_expressions = selector.get("matchExpressions")
    if match_expressions not in (None, []):
        return False
    return all(labels.get(key) == value for key, value in match_labels.items())


def _pdb_protected_system_pod(
    pod: Mapping[str, Any],
    pdb_items: Sequence[object],
) -> bool:
    """Accept one ready kube-system replica only when one fresh PDB permits eviction."""

    metadata_value = pod.get("metadata")
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    namespace = metadata.get("namespace")
    labels = metadata.get("labels")
    owners_value = metadata.get("ownerReferences")
    owners = owners_value if isinstance(owners_value, list) else []
    controlled_replica = any(
        isinstance(owner, Mapping)
        and owner.get("controller") is True
        and owner.get("kind") in {"ReplicaSet", "StatefulSet"}
        for owner in owners
    )
    status_value = pod.get("status")
    status = status_value if isinstance(status_value, Mapping) else {}
    conditions = status.get("conditions") if isinstance(status, Mapping) else []
    ready = any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions or ()
    )
    if namespace != "kube-system" or not controlled_replica or not ready:
        return False

    matching: list[Mapping[str, Any]] = []
    for pdb in pdb_items:
        if not isinstance(pdb, Mapping):
            continue
        pdb_metadata_value = pdb.get("metadata")
        pdb_metadata = pdb_metadata_value if isinstance(pdb_metadata_value, Mapping) else {}
        if pdb_metadata.get("namespace") != namespace:
            continue
        spec_value = pdb.get("spec")
        spec = spec_value if isinstance(spec_value, Mapping) else {}
        if _pdb_selector_matches_pod(spec.get("selector"), labels):
            matching.append(pdb)
    if len(matching) != 1:
        return False

    pdb = matching[0]
    pdb_metadata_value = pdb.get("metadata")
    pdb_metadata = pdb_metadata_value if isinstance(pdb_metadata_value, Mapping) else {}
    pdb_status_value = pdb.get("status")
    pdb_status = pdb_status_value if isinstance(pdb_status_value, Mapping) else {}
    generation = pdb_metadata.get("generation")
    observed_generation = pdb_status.get("observedGeneration")
    fresh = (
        isinstance(generation, int)
        and isinstance(observed_generation, int)
        and generation == observed_generation
    )
    disruptions_allowed = pdb_status.get("disruptionsAllowed")
    current_healthy = pdb_status.get("currentHealthy")
    desired_healthy = pdb_status.get("desiredHealthy")
    return (
        fresh
        and isinstance(disruptions_allowed, int)
        and disruptions_allowed >= 1
        and isinstance(current_healthy, int)
        and isinstance(desired_healthy, int)
        and current_healthy > desired_healthy
    )


def _node_drain_preflight(
    node_name: str,
    *,
    namespace: str,
    label_key: str,
    label_value: str,
    context: str | None,
    timeout_seconds: float,
    require_schedulable: bool = True,
    require_cordoned: bool = False,
    require_gate_workload: bool = False,
    node_read_attempt_limit: int = 1,
) -> tuple[CommandResult, dict[str, Any]]:
    """Prove the named node contains only gate or safely disruptable system pods."""

    node_result = CommandResult(status="not_run", reason="node was not observed")
    node: dict[str, Any] | None = None
    node_read_attempts = 0
    for node_read_attempts in range(1, node_read_attempt_limit + 1):
        node_result, node = _json_command(
            ["get", "node", node_name, "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        if node_result.status == "pass" and node is not None:
            break
        if node_read_attempts < node_read_attempt_limit:
            time.sleep(0.25)
    if node_result.status != "pass" or node is None:
        return node_result, {
            "node": _result_payload(node_result),
            "node_read_attempts": node_read_attempts,
        }
    metadata_value = node.get("metadata")
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    labels = metadata.get("labels") if isinstance(metadata, Mapping) else {}
    spec_value = node.get("spec")
    spec = spec_value if isinstance(spec_value, Mapping) else {}
    status_value = node.get("status")
    status = status_value if isinstance(status_value, Mapping) else {}
    conditions = status.get("conditions") if isinstance(status, Mapping) else []
    ready = any(
        isinstance(item, Mapping) and item.get("type") == "Ready" and item.get("status") == "True"
        for item in conditions or ()
    )
    label_ok = isinstance(labels, Mapping) and labels.get(label_key) == label_value
    schedulable = spec.get("unschedulable") is not True

    pods_result, pods = _json_command(
        [
            "get",
            "pods",
            "--all-namespaces",
            "--field-selector",
            f"spec.nodeName={node_name}",
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    items = pods.get("items") if isinstance(pods, Mapping) else None
    blockers: list[dict[str, str]] = []
    system_candidates: list[Mapping[str, Any]] = []
    gate_namespace_pods = 0
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, Mapping):
                continue
            item_metadata = item.get("metadata")
            item_metadata = item_metadata if isinstance(item_metadata, Mapping) else {}
            item_namespace = str(item_metadata.get("namespace", ""))
            if item_namespace == namespace:
                gate_namespace_pods += 1
                continue
            annotations = item_metadata.get("annotations")
            annotations = annotations if isinstance(annotations, Mapping) else {}
            owners = item_metadata.get("ownerReferences")
            owners = owners if isinstance(owners, list) else []
            daemon_owned = any(
                isinstance(owner, Mapping) and owner.get("kind") == "DaemonSet" for owner in owners
            )
            mirror_pod = "kubernetes.io/config.mirror" in annotations
            if daemon_owned or mirror_pod:
                continue
            if item_namespace == "kube-system":
                system_candidates.append(item)
                continue
            blockers.append(
                {
                    "namespace": item_namespace,
                    "owner_kind": ",".join(
                        str(owner.get("kind"))
                        for owner in owners
                        if isinstance(owner, Mapping) and owner.get("kind")
                    )
                    or "unknown",
                }
            )
    pdb_result = CommandResult(status="not_run", reason="no system pod required PDB review")
    protected_system_pods = 0
    if system_candidates:
        pdb_result, pdbs = _json_command(
            ["get", "pdb", "--all-namespaces", "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        pdb_items = pdbs.get("items") if isinstance(pdbs, Mapping) else None
        for item in system_candidates:
            if (
                pdb_result.status == "pass"
                and isinstance(pdb_items, list)
                and _pdb_protected_system_pod(item, pdb_items)
            ):
                protected_system_pods += 1
                continue
            metadata_value = item.get("metadata")
            item_metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
            owners_value = item_metadata.get("ownerReferences")
            owners = owners_value if isinstance(owners_value, list) else []
            blockers.append(
                {
                    "namespace": "kube-system",
                    "owner_kind": ",".join(
                        str(owner.get("kind"))
                        for owner in owners
                        if isinstance(owner, Mapping) and owner.get("kind")
                    )
                    or "unknown",
                }
            )
    reasons: list[str] = []
    if not label_ok:
        reasons.append("controlled node label does not match")
    if not ready:
        reasons.append("controlled node is not Ready")
    if require_schedulable and not schedulable:
        reasons.append("controlled node is already cordoned")
    if require_cordoned and schedulable:
        reasons.append("controlled node was not observed cordoned")
    if pods_result.status != "pass":
        reasons.append("controlled node pod inventory could not be observed")
    if blockers:
        reasons.append("controlled node has non-daemon workloads outside the gate namespace")
    if require_gate_workload and gate_namespace_pods == 0:
        reasons.append("controlled node has no workload from the isolated gate namespace")
    details = {
        "node": _result_payload(node_result),
        "node_read_attempts": node_read_attempts,
        "node_label_verified": label_ok,
        "node_ready": ready,
        "node_schedulable": schedulable,
        "pod_inventory": _result_payload(pods_result),
        "pdb_inventory": _result_payload(pdb_result),
        "blocking_pod_count": len(blockers),
        "blocking_owner_kinds": sorted({item["owner_kind"] for item in blockers}),
        "pdb_protected_system_pod_count": protected_system_pods,
        "gate_namespace_pod_count": gate_namespace_pods,
    }
    return (
        CommandResult(
            status="pass" if not reasons else "not_run",
            reason="; ".join(reasons),
        ),
        details,
    )


def _controlled_node_drain(
    node_name: str,
    *,
    namespace: str,
    label_key: str,
    label_value: str,
    context: str | None,
    timeout_seconds: float,
) -> tuple[CommandResult, dict[str, Any]]:
    """Cordon/drain one preflighted dedicated node and always uncordon it."""

    preflight, details = _node_drain_preflight(
        node_name,
        namespace=namespace,
        label_key=label_key,
        label_value=label_value,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if preflight.status != "pass":
        return preflight, {"preflight": details, "uncordon": {"status": "not_run"}}
    cordon = _kubectl(["cordon", node_name], context=context, timeout_seconds=timeout_seconds)
    details["cordon"] = _result_payload(cordon)
    if cordon.status != "pass":
        # A failed cordon can still have changed the node.  Make a best effort
        # to restore schedulability and retain the failure as non-promotable
        # evidence rather than leaving the dedicated node cordoned.
        uncordon = _kubectl(
            ["uncordon", node_name], context=context, timeout_seconds=timeout_seconds
        )
        details["uncordon"] = _result_payload(uncordon)
        return cordon, details
    drain = CommandResult(status="not_run", reason="node drain was not attempted")
    post_node_result = CommandResult(status="not_run", reason="post-drain observation not run")
    uncordon = CommandResult(status="not_run", reason="node uncordon was not attempted")
    try:
        # Re-check the node and its workload inventory after cordon.  The
        # initial preflight is not enough: another workload can be scheduled
        # on the node between that check and the cordon call.  Never drain
        # when this second observation is unavailable or finds an out-of-scope
        # workload.
        post_cordon_preflight, post_cordon_details = _node_drain_preflight(
            node_name,
            namespace=namespace,
            label_key=label_key,
            label_value=label_value,
            context=context,
            timeout_seconds=timeout_seconds,
            require_schedulable=False,
            require_cordoned=True,
            require_gate_workload=True,
            node_read_attempt_limit=3,
        )
        details["post_cordon_preflight"] = post_cordon_details
        if post_cordon_preflight.status != "pass":
            return CommandResult(
                status="not_run",
                reason="post-cordon node preflight failed; drain was not attempted",
            ), details
        drain = _kubectl(
            [
                "drain",
                node_name,
                "--ignore-daemonsets",
                # The production pods mount only ephemeral /tmp emptyDir
                # storage.  A node drain otherwise refuses to evict them and
                # makes this runtime check impossible.  The preflight above
                # proves that the node is dedicated and the drain is limited
                # to this isolated namespace, so deleting that disposable
                # state is an explicit part of the controlled test.
                "--delete-emptydir-data=true",
                "--force=false",
                "--grace-period=90",
                f"--timeout={int(timeout_seconds)}s",
            ],
            context=context,
            timeout_seconds=min(timeout_seconds + 10, MAX_TIMEOUT_SECONDS),
        )
        details["drain"] = _result_payload(drain)
        post_node_result, post_node = _json_command(
            ["get", "node", node_name, "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        post_spec = post_node.get("spec") if isinstance(post_node, Mapping) else None
        details["post_drain"] = {
            "node": _result_payload(post_node_result),
            "node_cordoned": isinstance(post_spec, Mapping)
            and post_spec.get("unschedulable") is True,
        }
    except (OSError, ValueError) as error:
        drain = CommandResult(
            status="fail", reason=f"node drain observation failed: {type(error).__name__}"
        )
        post_node_result = CommandResult(status="fail", reason="post-drain observation not run")
        details["drain"] = _result_payload(drain)
        details["post_drain"] = {
            "node": _result_payload(post_node_result),
            "node_cordoned": False,
        }
    finally:
        uncordon = _kubectl(
            ["uncordon", node_name], context=context, timeout_seconds=timeout_seconds
        )
        details["uncordon"] = _result_payload(uncordon)
    success = (
        drain.status == "pass"
        and post_node_result.status == "pass"
        and details["post_drain"]["node_cordoned"] is True
        and uncordon.status == "pass"
    )
    if not success:
        reason = "controlled node drain or uncordon failed"
        return CommandResult(status="fail", reason=reason), details
    return CommandResult(status="pass"), details


def _build_runtime_attestation(
    candidate: Mapping[str, Any], *, context: str | None, cluster_stdout: str
) -> dict[str, Any]:
    checks = candidate.get("checks")
    checks = checks if isinstance(checks, Mapping) else {}
    rolling = checks.get("rolling_upgrade")
    rolling_images = rolling.get("image_ids") if isinstance(rolling, Mapping) else {}
    initial = checks.get("initial_image_ids")
    return {
        "status": "pass",
        "namespace_isolated": bool(candidate.get("namespace")),
        "namespace": candidate.get("namespace"),
        "run_nonce": candidate.get("run_nonce"),
        "cluster_identity": _cluster_identity(cluster_stdout, context),
        "node_identity": candidate.get(
            "controlled_node", {"fingerprint_sha256": hashlib.sha256(b"unknown-node").hexdigest()}
        ),
        "actions": {
            "server_side_dry_run": _check_status(checks, "server_side_dry_run") == "pass",
            "schema_migration": _check_status(checks, "schema_migration") == "pass",
            "schema_migration_head": _check_status(checks, "schema_migration_head") == "pass",
            "readiness": _check_status(checks, "readiness") == "pass",
            "scheduler_cutover_guard": _check_status(checks, "scheduler_cutover_guard") == "pass",
            "rolling_upgrade": _check_status(checks, "rolling_upgrade") == "pass",
            "hpa_observed": _check_status(checks, "worker_scale_and_hpa") == "pass",
            "hpa_load_observed": _check_status(checks, "hpa_load_observation") == "pass",
            "pod_eviction": _check_status(checks, "pdb_eviction") == "pass",
            "node_eviction": _check_status(checks, "node_eviction") == "pass",
            "graceful_termination": _check_status(checks, "graceful_termination") == "pass",
            "namespace_cleanup": _check_status(checks, "namespace_cleanup") == "pass",
        },
        "image_ids": {
            "initial": initial if isinstance(initial, Mapping) else {},
            "upgrade": rolling_images.get("upgrade", {})
            if isinstance(rolling_images, Mapping)
            else {},
        },
        "eviction_scope": (
            "namespace_pod_eviction+controlled_node"
            if _check_status(checks, "node_eviction") == "pass"
            else "namespace_pod_eviction"
        ),
        "node_eviction_status": (
            "pass" if _check_status(checks, "node_eviction") == "pass" else "not_run"
        ),
    }


def _cleanup_expired_gate_namespaces(
    *, context: str | None, cluster_fingerprint: str, timeout_seconds: float
) -> tuple[CommandResult, dict[str, Any]]:
    """Boundedly clean only this tool's expired, fingerprinted namespaces.

    This is the recovery path for a process killed before ``finally``.  A
    namespace without all labels, with a future/invalid expiry, or belonging
    to another cluster is never deleted.
    """

    result, payload = _json_command(
        [
            "get",
            "namespaces",
            "-l",
            f"{RUNTIME_NAMESPACE_OWNER_LABEL}={RUNTIME_NAMESPACE_OWNER_VALUE}",
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    details: dict[str, Any] = {"status": result.status, "deleted": [], "skipped": []}
    if result.status != "pass" or payload is None:
        details["reason"] = result.reason or "expired namespace inventory could not be read"
        return result, details
    items = payload.get("items")
    if not isinstance(items, list):
        return CommandResult(
            status="fail", reason="namespace inventory has invalid JSON shape"
        ), details
    if len(items) > RUNTIME_NAMESPACE_MAX_CLEANUP:
        return CommandResult(
            status="fail", reason="expired namespace cleanup inventory exceeds bound"
        ), details
    now = int(time.time())
    for item in items:
        metadata = item.get("metadata") if isinstance(item, Mapping) else None
        metadata_map = metadata if isinstance(metadata, Mapping) else {}
        name = metadata_map.get("name")
        labels = metadata_map.get("labels")
        labels_map = labels if isinstance(labels, Mapping) else {}
        expiry = labels_map.get(RUNTIME_NAMESPACE_EXPIRY_LABEL)
        namespace_cluster = labels_map.get(RUNTIME_NAMESPACE_CLUSTER_LABEL)
        try:
            expiry_value = int(str(expiry))
        except (TypeError, ValueError):
            details["skipped"].append(str(name or "<unnamed>"))
            continue
        if (
            not isinstance(name, str)
            or not name.startswith("trpc-runtime-gate-")
            or labels_map.get(RUNTIME_NAMESPACE_OWNER_LABEL) != RUNTIME_NAMESPACE_OWNER_VALUE
            or not isinstance(namespace_cluster, str)
            or namespace_cluster != cluster_fingerprint[:63]
            or expiry_value > now
        ):
            details["skipped"].append(str(name or "<unnamed>"))
            continue
        deleted = _kubectl(
            ["delete", "namespace", name, "--ignore-not-found", "--wait=false"],
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        if deleted.status != "pass":
            return CommandResult(
                status="fail", reason=f"expired namespace {name} could not be deleted"
            ), details
        details["deleted"].append(name)
    return CommandResult(status="pass"), details


def _missing_prerequisites(
    *, allow_local_images: bool = False, require_release_binding: bool = True
) -> list[str]:
    required = {
        "TRPC_K8S_RUNTIME_IMAGE": os.getenv("TRPC_K8S_RUNTIME_IMAGE"),
        "TRPC_K8S_RUNTIME_UPGRADE_IMAGE": os.getenv("TRPC_K8S_RUNTIME_UPGRADE_IMAGE"),
        "TRPC_K8S_RUNTIME_SECRET_MANIFEST": os.getenv("TRPC_K8S_RUNTIME_SECRET_MANIFEST"),
        "TRPC_K8S_RUNTIME_HPA_DRIVER": os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER"),
        "TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256": os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256"),
        "TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG": os.getenv(
            "TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG"
        ),
        "TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT": os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT"),
        "TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT": os.getenv("TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT"),
        "TRPC_K8S_RUNTIME_HPA_JOB_COMMAND": os.getenv("TRPC_K8S_RUNTIME_HPA_JOB_COMMAND"),
        "TRPC_K8S_RUNTIME_NODE_NAME": os.getenv("TRPC_K8S_RUNTIME_NODE_NAME"),
        "TRPC_K8S_RUNTIME_NODE_LABEL": os.getenv("TRPC_K8S_RUNTIME_NODE_LABEL"),
        "TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM": os.getenv("TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM"),
    }
    missing = [name for name, value in required.items() if not value]
    secret_manifest = required["TRPC_K8S_RUNTIME_SECRET_MANIFEST"]
    if secret_manifest and not Path(secret_manifest).is_file():
        missing.append("TRPC_K8S_RUNTIME_SECRET_MANIFEST points to a missing file")
    hpa_driver = required["TRPC_K8S_RUNTIME_HPA_DRIVER"]
    if hpa_driver:
        _, driver_error = _hpa_driver_identity(
            hpa_driver,
            expected_sha256=required["TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256"] or "",
            kubeconfig_path=required["TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG"] or "",
        )
        if driver_error:
            missing.append(f"TRPC_K8S_RUNTIME_HPA_DRIVER: {driver_error}")
    subject = required["TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT"] or ""
    if subject and _parse_hpa_driver_subject(subject) is None:
        missing.append("TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT is invalid")
    driver_context = required["TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT"] or ""
    if driver_context and (context_error := _validate_hpa_driver_context(driver_context)):
        missing.append(f"TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT: {context_error}")
    job_command = required["TRPC_K8S_RUNTIME_HPA_JOB_COMMAND"] or ""
    if job_command:
        try:
            command_payload = json.loads(job_command)
        except json.JSONDecodeError:
            command_payload = None
        if (
            not isinstance(command_payload, list)
            or not command_payload
            or len(command_payload) > 64
            or any(
                not isinstance(item, str)
                or not item
                or len(item.encode("utf-8")) > 512
                or any(character in item for character in ("\x00", "\r", "\n"))
                for item in command_payload
            )
        ):
            missing.append("TRPC_K8S_RUNTIME_HPA_JOB_COMMAND must be a bounded JSON argument array")
    if required["TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM"] != _NODE_DRAIN_CONFIRMATION:
        missing.append(f"TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM must equal {_NODE_DRAIN_CONFIRMATION}")
    if require_release_binding:
        release_id = os.getenv("TRPC_RELEASE_ID", "").strip()
        release_nonce = os.getenv("TRPC_RELEASE_NONCE", "").strip()
        if RELEASE_ID_RE.fullmatch(release_id) is None:
            missing.append("TRPC_RELEASE_ID is required for a production runtime acceptance")
        if RELEASE_NONCE_RE.fullmatch(release_nonce) is None:
            missing.append("TRPC_RELEASE_NONCE is required for a production runtime acceptance")
    image = required["TRPC_K8S_RUNTIME_IMAGE"] or ""
    upgrade_image = required["TRPC_K8S_RUNTIME_UPGRADE_IMAGE"] or ""
    image_pull_secret = os.getenv("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET", "").strip()
    if image_pull_secret and not _valid_image_pull_secret_name(image_pull_secret):
        missing.append("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET is invalid")
    if not allow_local_images:
        valid_images, image_reasons = _production_image_contract(image, upgrade_image)
        if not valid_images:
            missing.extend(image_reasons)
    object_store_endpoint = os.getenv("TRPC_K8S_RUNTIME_S3_ENDPOINT", "").strip()
    object_store_bucket = os.getenv("TRPC_K8S_RUNTIME_S3_BUCKET", "").strip()
    if bool(object_store_endpoint) != bool(object_store_bucket):
        missing.append("TRPC_K8S_RUNTIME_S3_ENDPOINT and TRPC_K8S_RUNTIME_S3_BUCKET must be paired")
    elif object_store_endpoint:
        try:
            config_map = (
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n"
                "  name: trpc-service-config\ndata: {}\n"
            )
            _runtime_object_store_override(
                config_map,
                endpoint=object_store_endpoint,
                bucket=object_store_bucket,
            )
        except ValueError as error:
            missing.append(str(error))
    return missing


def _permission_check(
    verb: str,
    resource: str,
    *,
    context: str | None,
    namespace: str | None,
    timeout_seconds: float,
) -> CommandResult:
    args = ["auth", "can-i", verb, resource]
    if namespace:
        args.extend(["--namespace", namespace])
    result = _kubectl(args, context=context, timeout_seconds=timeout_seconds)
    if result.status != "pass":
        return result
    if result.stdout.strip().lower() != "yes":
        return CommandResult(status="not_run", reason=f"permission denied: {verb} {resource}")
    return result


def _first_worker_pod(
    *, namespace: str, context: str | None, timeout_seconds: float
) -> tuple[CommandResult, str | None]:
    result, payload = _json_command(
        [
            "get",
            "pods",
            "--namespace",
            namespace,
            "-l",
            "app.kubernetes.io/name=trpc-worker",
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "pass" or payload is None:
        return result, None
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return CommandResult(status="fail", reason="no worker pod was found"), None
    ready_names: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
            continue
        name = metadata.get("name")
        conditions = status.get("conditions")
        ready = isinstance(conditions, list) and any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        )
        if (
            isinstance(name, str)
            and name
            and metadata.get("deletionTimestamp") is None
            and status.get("phase") == "Running"
            and ready
        ):
            ready_names.append(name)
    if not ready_names:
        return CommandResult(
            status="fail", reason="no ready non-terminating worker pod was found"
        ), None
    return result, min(ready_names)


def _deployment_ready(
    deployment: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> CommandResult:
    result, payload = _json_command(
        ["get", "deployment", deployment, "--namespace", namespace, "-o", "json"],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "pass" or payload is None:
        return result
    spec = payload.get("spec")
    status = payload.get("status")
    desired = spec.get("replicas") if isinstance(spec, dict) else None
    ready = status.get("readyReplicas", 0) if isinstance(status, dict) else 0
    if not isinstance(desired, int) or not isinstance(ready, int) or ready < desired:
        return CommandResult(
            status="fail", reason=f"{deployment} has {ready}/{desired} ready replicas"
        )
    return CommandResult(status="pass")


def _rollout_all(
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> dict[str, CommandResult]:
    checks: dict[str, CommandResult] = {}
    for deployment, _container in ACK_RUNTIME_DEPLOYMENTS:
        checks[deployment] = _rollout_deployment(
            deployment,
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
    return checks


def _rollout_deployment(
    deployment: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> CommandResult:
    return _kubectl(
        [
            "rollout",
            "status",
            f"deployment/{deployment}",
            "--namespace",
            namespace,
            f"--timeout={int(timeout_seconds)}s",
        ],
        context=context,
        timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
    )


def _rolling_upgrade_serial(
    upgrade_image: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> tuple[dict[str, CommandResult], dict[str, CommandResult]]:
    """Upgrade one Deployment at a time so local gates cannot create an eight-role surge."""

    image_updates: dict[str, CommandResult] = {}
    rollouts: dict[str, CommandResult] = {}
    for deployment, container in ACK_RUNTIME_DEPLOYMENTS:
        image_updates[deployment] = _kubectl(
            [
                "set",
                "image",
                f"deployment/{deployment}",
                f"{container}={upgrade_image}",
                "--namespace",
                namespace,
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        if image_updates[deployment].status != "pass":
            break
        rollouts[deployment] = _rollout_deployment(
            deployment,
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        if rollouts[deployment].status != "pass":
            break
    return image_updates, rollouts


def _deployment_image_ids(
    deployment: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> tuple[CommandResult, tuple[str, ...]]:
    """Read immutable runtime image IDs from ready pods without logging them."""

    result, payload = _json_command(
        [
            "get",
            "pods",
            "--namespace",
            namespace,
            "-l",
            f"app.kubernetes.io/name={deployment}",
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "pass" or payload is None:
        return result, ()
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return CommandResult(status="fail", reason=f"no pods found for {deployment}"), ()
    image_ids: list[str] = []
    for item in items:
        metadata = item.get("metadata") if isinstance(item, Mapping) else None
        status = item.get("status") if isinstance(item, Mapping) else None
        if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
            return CommandResult(status="fail", reason=f"{deployment} pod status is invalid"), ()
        if status.get("phase") != "Running" or metadata.get("deletionTimestamp") is not None:
            return (
                CommandResult(
                    status="fail",
                    reason=f"{deployment} has a non-running or terminating pod",
                ),
                (),
            )
        conditions = status.get("conditions")
        if not isinstance(conditions, list) or not any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        ):
            return (
                CommandResult(status="fail", reason=f"{deployment} has a pod that is not Ready"),
                (),
            )
        containers = status.get("containerStatuses") if isinstance(status, Mapping) else None
        if not isinstance(containers, list) or not containers:
            return CommandResult(status="fail", reason=f"{deployment} has no container status"), ()
        for container in containers:
            if not isinstance(container, Mapping):
                return (
                    CommandResult(
                        status="fail", reason=f"{deployment} container status is invalid"
                    ),
                    (),
                )
            image_id = container.get("imageID")
            if not isinstance(image_id, str):
                return (
                    CommandResult(
                        status="fail", reason=f"{deployment} container image ID is invalid"
                    ),
                    (),
                )
            digest = IMAGE_ID_DIGEST_RE.search(image_id)
            if digest is None or digest.end() != len(image_id):
                return (
                    CommandResult(
                        status="fail", reason=f"{deployment} container image ID is invalid"
                    ),
                    (),
                )
            image_ids.append(digest.group(1).lower())
    if not image_ids:
        return CommandResult(status="fail", reason=f"{deployment} has no immutable image IDs"), ()
    return result, tuple(sorted(set(image_ids)))


def _wait_for_deployment_image_ids(
    deployment: str,
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
    previous_image_ids: tuple[str, ...] = (),
) -> tuple[CommandResult, tuple[str, ...], int]:
    """Wait until all ready, non-terminating Pods use one immutable image ID."""

    deadline = time.monotonic() + timeout_seconds
    poll_count = 0
    last_reason = f"{deployment} image IDs were not observed"
    while time.monotonic() < deadline:
        poll_count += 1
        result, image_ids = _deployment_image_ids(
            deployment,
            namespace=namespace,
            context=context,
            timeout_seconds=max(1.0, min(timeout_seconds, deadline - time.monotonic())),
        )
        if result.status == "pass" and len(image_ids) == 1:
            if not previous_image_ids or set(image_ids).isdisjoint(previous_image_ids):
                return result, image_ids, poll_count
            last_reason = f"{deployment} still uses its initial immutable image ID"
        else:
            last_reason = result.reason or f"{deployment} has multiple immutable image IDs"
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(1.0, remaining))
    return CommandResult(status="fail", reason=last_reason), (), poll_count


def _rollback_probe_image(image: str) -> str:
    """Derive an intentionally unavailable image in the same registry.

    The failure probe is used only inside the disposable acceptance namespace.
    Keeping the registry host and changing the repository path avoids testing
    a second registry while the all-zero digest makes accidental reuse of a
    real release image infeasible.
    """

    parsed = _registry_digest_reference(image)
    repository = parsed[0] if parsed is not None else image.split("@", 1)[0].strip()
    slash = repository.rfind("/")
    colon = repository.rfind(":")
    if colon > slash:
        repository = repository[:colon]
    return f"{repository}/__trpc_runtime_gate_failure__@sha256:{'0' * 64}"


def _failure_rollback(
    deployment: str,
    container: str,
    known_good_image: str,
    known_good_image_ids: tuple[str, ...],
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
) -> tuple[CommandResult, dict[str, Any]]:
    """Prove a failed rollout is recoverable in the isolated namespace.

    The gate first points one representative worker Deployment at a
    deliberately unavailable immutable image and requires rollout status to
    fail. It then invokes the controller's own ``rollout undo`` and requires
    both readiness and the previously observed image digest to return. The
    probe is bounded so a broken registry cannot turn the acceptance into an
    unbounded wait.
    """

    probe_image = _rollback_probe_image(known_good_image)
    probe_timeout = min(timeout_seconds, _ROLLBACK_PROBE_TIMEOUT_SECONDS)
    details: dict[str, Any] = {
        "deployment": deployment,
        "failure_injected": False,
        "failure_observed": False,
        "undo_observed": False,
        "readiness_recovered": False,
        "restored_image_ids": [],
    }
    set_failed = _kubectl(
        [
            "set",
            "image",
            f"deployment/{deployment}",
            f"{container}={probe_image}",
            "--namespace",
            namespace,
        ],
        context=context,
        timeout_seconds=probe_timeout,
    )
    details["failure_injection"] = _result_payload(set_failed)
    details["failure_injected"] = set_failed.status == "pass"
    if set_failed.status != "pass":
        return (
            CommandResult(
                status="fail",
                reason=set_failed.reason or "rollback failure image could not be applied",
            ),
            details,
        )

    failed_rollout = _rollout_deployment(
        deployment,
        namespace=namespace,
        context=context,
        timeout_seconds=probe_timeout,
    )
    details["failed_rollout"] = _result_payload(failed_rollout)
    details["failure_observed"] = failed_rollout.status != "pass"
    if not details["failure_observed"]:
        return (
            CommandResult(
                status="fail",
                reason="injected failure rollout unexpectedly became ready",
            ),
            details,
        )

    undo = _kubectl(
        [
            "rollout",
            "undo",
            f"deployment/{deployment}",
            "--namespace",
            namespace,
        ],
        context=context,
        timeout_seconds=probe_timeout,
    )
    details["undo"] = _result_payload(undo)
    details["undo_observed"] = undo.status == "pass"
    if undo.status != "pass":
        return (
            CommandResult(status="fail", reason=undo.reason or "rollout undo failed"),
            details,
        )

    recovered_rollout = _rollout_deployment(
        deployment,
        namespace=namespace,
        context=context,
        timeout_seconds=probe_timeout,
    )
    details["rollback_rollout"] = _result_payload(recovered_rollout)
    if recovered_rollout.status != "pass":
        return (
            CommandResult(
                status="fail",
                reason=recovered_rollout.reason or "rollback rollout did not become ready",
            ),
            details,
        )

    image_result, restored_image_ids, image_poll_count = _wait_for_deployment_image_ids(
        deployment,
        namespace=namespace,
        context=context,
        timeout_seconds=probe_timeout,
    )
    details["rollback_image_poll_count"] = image_poll_count
    details["restored_image_ids"] = list(restored_image_ids)
    details["readiness_recovered"] = image_result.status == "pass" and restored_image_ids == tuple(
        sorted(set(known_good_image_ids))
    )
    details["restored_image_read"] = _result_payload(image_result)
    if not details["readiness_recovered"]:
        return (
            CommandResult(
                status="fail",
                reason="rollback readiness did not restore the known-good immutable image",
            ),
            details,
        )
    return CommandResult(status="pass"), details


def _schema_head_check_manifest(migration_manifest: str, *, namespace: str) -> dict[str, Any]:
    """Derive a one-shot schema-head Job from the rendered migration Job.

    A completed Pod cannot be used with ``kubectl exec``.  Reusing the
    rendered Job's pod template keeps the head check on the migration image,
    database role, Secret references, volumes, and security policy while
    giving ``trpc-service migrate --check`` a fresh process to run in.
    """

    if not namespace:
        raise ValueError("schema head-check namespace is required")
    try:
        documents = [
            document
            for document in yaml.safe_load_all(migration_manifest)
            if isinstance(document, Mapping)
        ]
    except yaml.YAMLError as error:
        raise ValueError("migration manifest is not valid YAML") from error
    migration_job = next(
        (
            document
            for document in documents
            if document.get("kind") == "Job"
            and isinstance(document.get("metadata"), Mapping)
            and document["metadata"].get("name") == "trpc-schema-migration"
        ),
        None,
    )
    if not isinstance(migration_job, Mapping):
        raise ValueError("migration Job is missing from migration manifest")
    migration_spec = migration_job.get("spec")
    template = migration_spec.get("template") if isinstance(migration_spec, Mapping) else None
    pod_spec = template.get("spec") if isinstance(template, Mapping) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, Mapping) else None
    if not isinstance(containers, list) or len(containers) != 1:
        raise ValueError("migration Job must contain exactly one container")
    source_container = containers[0]
    if not isinstance(source_container, Mapping) or not source_container.get("image"):
        raise ValueError("migration Job container image is missing")
    assert isinstance(source_container, Mapping)
    if not isinstance(migration_spec, Mapping) or not isinstance(template, Mapping):
        raise ValueError("migration Job pod template is missing")
    if not isinstance(pod_spec, Mapping):
        raise ValueError("migration Job pod spec is missing")

    head_spec = deepcopy(dict(migration_spec))
    head_template = deepcopy(dict(template))
    head_pod_spec = deepcopy(dict(pod_spec))
    head_container = deepcopy(dict(source_container))
    head_container["command"] = ["trpc-service"]
    head_container["args"] = ["migrate", "--check"]
    head_pod_spec["containers"] = [head_container]
    source_template_metadata = template.get("metadata")
    source_labels = (
        source_template_metadata.get("labels")
        if isinstance(source_template_metadata, Mapping)
        else None
    )
    template_labels = dict(source_labels) if isinstance(source_labels, Mapping) else {}
    template_labels.update(
        {
            "app.kubernetes.io/name": SCHEMA_HEAD_CHECK_JOB_NAME,
            "app.kubernetes.io/component": "migration-head-check",
            "trpc.io/runtime-gate": "schema-head-check",
        }
    )
    head_template_metadata = (
        deepcopy(dict(source_template_metadata))
        if isinstance(source_template_metadata, Mapping)
        else {}
    )
    head_template_metadata["labels"] = template_labels
    head_template["metadata"] = head_template_metadata
    head_template["spec"] = head_pod_spec
    head_spec["template"] = head_template
    head_spec["completions"] = 1
    head_spec["parallelism"] = 1
    head_spec["backoffLimit"] = 0
    # A copied selector would bind the new Job to the migration Job's Pods.
    head_spec.pop("selector", None)

    job_labels = {
        "app.kubernetes.io/name": SCHEMA_HEAD_CHECK_JOB_NAME,
        "app.kubernetes.io/component": "migration-head-check",
        "trpc.io/runtime-gate": "schema-head-check",
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": SCHEMA_HEAD_CHECK_JOB_NAME,
            "namespace": namespace,
            "labels": job_labels,
        },
        "spec": head_spec,
    }


def _migration_head_check(
    *,
    namespace: str,
    context: str | None,
    timeout_seconds: float,
    migration_manifest: str,
) -> CommandResult:
    """Run and API-attest a fresh schema-head-check Job.

    The check deliberately never execs a completed migration Pod.  The fresh
    Job inherits the migration Pod template and runs the CLI check as its own
    process; both its Job UID and its unique successful Pod UID are required
    before the gate can continue.
    """

    try:
        head_manifest = _schema_head_check_manifest(migration_manifest, namespace=namespace)
    except ValueError as error:
        return CommandResult(status="fail", reason=str(error))

    migration_job_result, migration_job_payload = _json_command(
        [
            "get",
            "job",
            "trpc-schema-migration",
            "--namespace",
            namespace,
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if migration_job_result.status != "pass" or migration_job_payload is None:
        return migration_job_result
    migration_metadata = migration_job_payload.get("metadata")
    migration_job_uid = (
        migration_metadata.get("uid") if isinstance(migration_metadata, Mapping) else None
    )
    if not isinstance(migration_job_uid, str) or not migration_job_uid:
        return CommandResult(status="fail", reason="migration Job has no UID")
    evidence: dict[str, Any] = {
        "migration_job_uid": migration_job_uid,
        "head_check_job_name": SCHEMA_HEAD_CHECK_JOB_NAME,
    }

    head_apply = _kubectl(
        ["apply", "--server-side", "-f", "-"],
        context=context,
        timeout_seconds=timeout_seconds,
        input_text=json.dumps(head_manifest, separators=(",", ":")),
    )
    evidence["head_check_apply_status"] = head_apply.status
    if head_apply.status != "pass":
        return CommandResult(
            status="fail",
            exit_code=head_apply.exit_code,
            stderr=head_apply.stderr,
            reason=head_apply.reason or "schema head-check Job could not be applied",
            evidence=evidence,
        )

    head_wait = _kubectl(
        [
            "wait",
            "--for=condition=complete",
            f"job/{SCHEMA_HEAD_CHECK_JOB_NAME}",
            "--namespace",
            namespace,
            f"--timeout={int(timeout_seconds)}s",
        ],
        context=context,
        timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
    )
    evidence["head_check_wait_status"] = head_wait.status
    if head_wait.status != "pass":
        return CommandResult(
            status="fail",
            exit_code=head_wait.exit_code,
            stderr=head_wait.stderr,
            reason=head_wait.reason or "schema head-check Job did not complete",
            evidence=evidence,
        )

    head_job_result, head_job_payload = _json_command(
        [
            "get",
            "job",
            SCHEMA_HEAD_CHECK_JOB_NAME,
            "--namespace",
            namespace,
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if head_job_result.status != "pass" or head_job_payload is None:
        return head_job_result
    head_metadata = head_job_payload.get("metadata")
    head_job_uid = head_metadata.get("uid") if isinstance(head_metadata, Mapping) else None
    head_status = head_job_payload.get("status")
    if not isinstance(head_job_uid, str) or not head_job_uid:
        return CommandResult(
            status="fail", reason="schema head-check Job has no UID", evidence=evidence
        )
    evidence["head_check_job_uid"] = head_job_uid
    if not isinstance(head_status, Mapping) or head_status.get("succeeded") != 1:
        return CommandResult(
            status="fail",
            reason="schema head-check Job did not report one successful completion",
            evidence=evidence,
        )

    pod_result, pod_payload = _json_command(
        [
            "get",
            "pods",
            "--namespace",
            namespace,
            "-l",
            f"job-name={SCHEMA_HEAD_CHECK_JOB_NAME}",
            "-o",
            "json",
        ],
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if pod_result.status != "pass" or pod_payload is None:
        return pod_result
    items = pod_payload.get("items")
    if not isinstance(items, list):
        return CommandResult(
            status="fail", reason="schema head-check Pod list is invalid", evidence=evidence
        )

    owned_pods: list[Mapping[str, Any]] = []
    succeeded_pods: list[Mapping[str, Any]] = []
    failed_pod_names: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        owners = metadata.get("ownerReferences")
        if not isinstance(owners, list) or not any(
            isinstance(owner, Mapping)
            and owner.get("kind") == "Job"
            and owner.get("name") == SCHEMA_HEAD_CHECK_JOB_NAME
            and owner.get("uid") == head_job_uid
            for owner in owners
        ):
            continue
        owned_pods.append(item)
        status = item.get("status")
        phase = status.get("phase") if isinstance(status, Mapping) else None
        pod_name = metadata.get("name")
        if phase == "Succeeded":
            succeeded_pods.append(item)
        elif isinstance(pod_name, str) and pod_name:
            failed_pod_names.append(pod_name)

    evidence.update(
        {
            "owned_pod_count": len(owned_pods),
            "succeeded_pod_count": len(succeeded_pods),
            "failed_pod_count": len(failed_pod_names),
            "failed_pod_names": failed_pod_names,
        }
    )
    if len(succeeded_pods) == 0:
        return CommandResult(
            status="fail",
            reason="schema head-check Job has no successful Pod",
            evidence=evidence,
        )
    if len(succeeded_pods) != 1:
        return CommandResult(
            status="fail",
            reason="schema head-check Job has multiple successful Pods",
            evidence=evidence,
        )

    selected_metadata = succeeded_pods[0].get("metadata")
    selected_status = succeeded_pods[0].get("status")
    pod_name = selected_metadata.get("name") if isinstance(selected_metadata, Mapping) else None
    pod_uid = selected_metadata.get("uid") if isinstance(selected_metadata, Mapping) else None
    container_statuses = (
        selected_status.get("containerStatuses") if isinstance(selected_status, Mapping) else None
    )
    if not isinstance(pod_name, str) or not pod_name:
        return CommandResult(
            status="fail", reason="schema head-check successful Pod has no name", evidence=evidence
        )
    if not isinstance(pod_uid, str) or not pod_uid:
        return CommandResult(
            status="fail", reason="schema head-check successful Pod has no UID", evidence=evidence
        )
    if not isinstance(container_statuses, list) or len(container_statuses) != 1:
        return CommandResult(
            status="fail",
            reason="schema head-check Pod container status is ambiguous",
            evidence=evidence,
        )
    terminated = container_statuses[0].get("state", {}).get("terminated")
    exit_code = terminated.get("exitCode") if isinstance(terminated, Mapping) else None
    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
        return CommandResult(
            status="fail",
            reason="schema head-check Pod has no terminated exit code",
            evidence=evidence,
        )
    evidence.update(
        {
            "head_check_pod_name": pod_name,
            "head_check_pod_uid": pod_uid,
            "container_exit_code": exit_code,
            "container_exit_reason": (
                terminated.get("reason") if isinstance(terminated, Mapping) else None
            ),
        }
    )
    if exit_code != 0:
        return CommandResult(
            status="fail",
            exit_code=exit_code,
            reason="schema head-check Pod exited non-zero",
            evidence=evidence,
        )
    return CommandResult(status="pass", exit_code=0, evidence=evidence)


def _run_live_once(
    *,
    output: Path,
    context: str | None,
    timeout_seconds: float,
    require_runtime: bool,
    allow_local_images: bool = False,
) -> int:
    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    candidate: dict[str, Any] = {
        "mode": "live_kubernetes_control_plane",
        "enabled": True,
        "topology": {
            "scope": ACK_RUNTIME_SCOPE,
            "im_deployment": ACK_IM_DEPLOYMENT,
            "production_deployments": [name for name, _container in PRODUCTION_DEPLOYMENTS],
            "tested_deployments": [name for name, _container in ACK_RUNTIME_DEPLOYMENTS],
            "provider_disabled_deployments": list(ACK_RUNTIME_PROVIDER_DISABLED_DEPLOYMENTS),
        },
        "checks": {},
    }
    reasons: list[str] = []
    checks: dict[str, Any] = candidate["checks"]
    prerequisites = _missing_prerequisites(
        allow_local_images=allow_local_images,
        # kind_runtime_gate is an explicitly non-production diagnostic and
        # intentionally has no release binding; the production path does.
        require_release_binding=not allow_local_images,
    )
    if prerequisites:
        # Preserve an explicit cluster-availability observation even when a
        # local caller is missing image/Secret prerequisites.  This keeps the
        # report useful without attempting any Kubernetes mutation.
        if shutil.which("kubectl") is None:
            checks["kube_context"] = _result_payload(
                CommandResult(status="not_run", reason="kubectl is not installed")
            )
        reasons.extend(prerequisites)
        _report(output, gate="not_run", candidate=candidate, rejection_reasons=reasons)
        return 1 if require_runtime else 0

    run_nonce = uuid4().hex
    namespace = f"trpc-runtime-gate-{run_nonce[:10]}"
    candidate["namespace"] = namespace
    candidate["run_nonce"] = run_nonce
    secret_manifest = os.environ["TRPC_K8S_RUNTIME_SECRET_MANIFEST"]
    image_pull_secret = os.getenv("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET", "").strip()
    image = os.environ["TRPC_K8S_RUNTIME_IMAGE"]
    upgrade_image = os.environ["TRPC_K8S_RUNTIME_UPGRADE_IMAGE"]
    hpa_driver_path = os.environ["TRPC_K8S_RUNTIME_HPA_DRIVER"]
    hpa_driver_subject = os.environ["TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT"]
    hpa_driver_context = os.environ["TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT"]
    hpa_driver_identity, hpa_driver_identity_error = _hpa_driver_identity(
        hpa_driver_path,
        expected_sha256=os.environ["TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256"],
        kubeconfig_path=os.environ["TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG"],
    )
    if hpa_driver_identity is None:
        _report(
            output,
            gate="not_run",
            candidate=candidate,
            rejection_reasons=[hpa_driver_identity_error or "HPA driver identity is invalid"],
        )
        return 1 if require_runtime else 0
    node_name = os.environ["TRPC_K8S_RUNTIME_NODE_NAME"]
    node_label = _parse_controlled_node_label(os.environ["TRPC_K8S_RUNTIME_NODE_LABEL"])
    if re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", node_name) is None:
        _report(
            output,
            gate="not_run",
            candidate=candidate,
            rejection_reasons=["TRPC_K8S_RUNTIME_NODE_NAME is not a valid Kubernetes node name"],
        )
        return 1 if require_runtime else 0
    candidate["controlled_node"] = {
        "fingerprint_sha256": hashlib.sha256(node_name.encode("utf-8")).hexdigest()
    }
    namespace_created = False
    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    gate_status = "not_run"
    cluster_stdout = ""
    try:
        kubectl_check = _kubectl(
            ["version", "--request-timeout=10s", "-o", "json"],
            context=context,
            timeout_seconds=20,
        )
        checks["kube_context"] = _result_payload(kubectl_check)
        cluster_stdout = kubectl_check.stdout
        if kubectl_check.status != "pass":
            reasons.append("current Kubernetes context is unavailable")
            return 1 if require_runtime else 0
        cluster_identity = _cluster_identity(cluster_stdout, context)
        if node_label is None:
            checks["node_eviction"] = {
                "status": "not_run",
                "reason": "controlled node label must use trpc-runtime-gate=<value>",
            }
            reasons.append("controlled node label is invalid")
            return 1 if require_runtime else 0
        node_preflight, node_preflight_details = _node_drain_preflight(
            node_name,
            namespace=namespace,
            label_key=node_label[0],
            label_value=node_label[1],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["node_eviction"] = {
            "status": "not_run" if node_preflight.status == "pass" else node_preflight.status,
            "preflight": node_preflight_details,
        }
        if node_preflight.status != "pass":
            reasons.append(node_preflight.reason or "controlled node preflight failed")
            return 1 if require_runtime else 0

        pull_secret_contract = _image_pull_secret_metadata_contract(
            secret_manifest,
            image_pull_secret,
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["image_pull_secret_contract"] = _result_payload(pull_secret_contract)
        if pull_secret_contract.status != "pass":
            reasons.append(
                pull_secret_contract.reason or "image pull Secret metadata contract failed"
            )
            return 1 if require_runtime else 0

        expired_cleanup, expired_cleanup_details = _cleanup_expired_gate_namespaces(
            context=context,
            cluster_fingerprint=cluster_identity["fingerprint_sha256"],
            timeout_seconds=min(timeout_seconds, 30),
        )
        candidate["expired_namespace_recovery"] = expired_cleanup_details
        if expired_cleanup.status != "pass":
            reasons.append(expired_cleanup.reason or "expired namespace recovery failed")
            return 1 if require_runtime else 0

        for verb, resource in (("create", "namespaces"),):
            permission = _permission_check(
                verb,
                resource,
                context=context,
                namespace=None,
                timeout_seconds=20,
            )
            checks[f"permission_{verb}_{resource}"] = _result_payload(permission)
            if permission.status != "pass":
                reasons.append(permission.reason or f"permission check failed: {verb} {resource}")
        if reasons:
            return 1 if require_runtime else 0

        temporary_directory = tempfile.TemporaryDirectory(prefix="trpc-k8s-gate-", dir=ROOT)
        overlay = _write_overlay(
            Path(temporary_directory.name),
            namespace=namespace,
            image=image,
            local_kind=allow_local_images,
            node_label=node_label,
            run_nonce=run_nonce,
            cluster_fingerprint=cluster_identity["fingerprint_sha256"],
            expires_at=str(int(time.time()) + RUNTIME_NAMESPACE_TTL_SECONDS),
            image_pull_secret=image_pull_secret,
        )
        rendered_manifest = Path(temporary_directory.name) / "runtime-rendered.yaml"
        render = _kubectl(
            [
                "kustomize",
                str(overlay.parent),
                "--load-restrictor=LoadRestrictionsNone",
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["kustomize_render"] = _result_payload(render)
        if render.status != "pass":
            reasons.append("Kustomize render failed")
            return 1 if require_runtime else 0
        rendered = render.stdout
        object_store_endpoint = os.getenv("TRPC_K8S_RUNTIME_S3_ENDPOINT", "").strip()
        object_store_bucket = os.getenv("TRPC_K8S_RUNTIME_S3_BUCKET", "").strip()
        if object_store_endpoint and object_store_bucket:
            try:
                rendered, object_store_evidence = _runtime_object_store_override(
                    rendered,
                    endpoint=object_store_endpoint,
                    bucket=object_store_bucket,
                )
            except ValueError as error:
                checks["runtime_object_store_override"] = {
                    "status": "fail",
                    "reason": str(error),
                }
                reasons.append(str(error))
                return 1 if require_runtime else 0
            checks["runtime_object_store_override"] = object_store_evidence
        else:
            checks["runtime_object_store_override"] = {"status": "not_requested"}
        rendered_manifest.write_text(rendered, encoding="utf-8")

        manifest_ok, manifest_reasons = _rendered_manifest_contract(
            rendered, local_kind=allow_local_images
        )
        checks["manifest_contract"] = {
            "status": "pass" if manifest_ok else "fail",
            "reasons": list(manifest_reasons),
        }
        if not manifest_ok:
            reasons.extend(manifest_reasons)
            return 1 if require_runtime else 0
        production_manifest_ok, production_manifest_reasons = _production_render_contract(
            rendered, allow_local_images=allow_local_images
        )
        checks["production_manifest_contract"] = {
            "status": "pass" if production_manifest_ok else "fail",
            "reasons": list(production_manifest_reasons),
        }
        if not production_manifest_ok:
            reasons.extend(production_manifest_reasons)
            return 1 if require_runtime else 0

        create_namespace = _kubectl(
            [
                "apply",
                "--server-side",
                "-f",
                str(Path(temporary_directory.name) / "namespace.yaml"),
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["namespace_create"] = _result_payload(create_namespace)
        if create_namespace.status != "pass":
            reasons.append("isolated namespace could not be created")
            return 1 if require_runtime else 0
        namespace_created = True
        # Once the disposable namespace exists, every later failure is a real
        # runtime-gate failure rather than an opt-in/preflight not_run result.
        gate_status = "fail"

        dry_run = _kubectl(
            [
                "apply",
                "--server-side",
                "--dry-run=server",
                "-f",
                str(rendered_manifest),
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["server_side_dry_run"] = _result_payload(dry_run)
        if dry_run.status != "pass":
            reasons.append("server-side Kustomize dry-run failed")
            return 1 if require_runtime else 0

        namespaced_permissions = (
            ("create", "secrets"),
            ("create", "deployments"),
            ("get", "deployments"),
            ("patch", "deployments"),
            ("delete", "pods"),
            ("create", "pods/eviction"),
            ("create", "roles.rbac.authorization.k8s.io"),
            ("create", "rolebindings.rbac.authorization.k8s.io"),
        )
        for verb, resource in namespaced_permissions:
            permission = _permission_check(
                verb,
                resource,
                context=context,
                namespace=namespace,
                timeout_seconds=20,
            )
            checks[f"permission_{verb}_{resource}"] = _result_payload(permission)
            if permission.status != "pass":
                reasons.append(permission.reason or f"permission check failed: {verb} {resource}")
        if reasons:
            return 1 if require_runtime else 0

        secret_dry_run = _kubectl(
            [
                "apply",
                "--server-side",
                "--dry-run=server",
                "--namespace",
                namespace,
                "-f",
                secret_manifest,
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["secret_server_side_dry_run"] = _result_payload(secret_dry_run)
        if secret_dry_run.status != "pass":
            reasons.append("Secret manifest server-side dry-run failed")
            return 1 if require_runtime else 0
        secret_apply = _kubectl(
            ["apply", "--server-side", "--namespace", namespace, "-f", secret_manifest],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["secret_apply"] = _result_payload(secret_apply)
        if secret_apply.status != "pass":
            reasons.append("Secret manifest could not be applied")
            return 1 if require_runtime else 0

        migration_manifest, runtime_manifest = _split_migration_manifests(rendered)
        migration_apply = _kubectl(
            [
                "apply",
                "--server-side",
                "-f",
                "-",
            ],
            context=context,
            timeout_seconds=timeout_seconds,
            input_text=migration_manifest,
        )
        checks["migration_apply"] = _result_payload(migration_apply)
        if migration_apply.status != "pass":
            reasons.append("migration prerequisites and Job could not be applied")
            return 1 if require_runtime else 0

        migration = _kubectl(
            [
                "wait",
                "--for=condition=complete",
                "job/trpc-schema-migration",
                "--namespace",
                namespace,
                f"--timeout={int(timeout_seconds)}s",
            ],
            context=context,
            timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
        )
        checks["schema_migration"] = _result_payload(migration)
        if migration.status != "pass":
            reasons.append("schema migration Job did not complete successfully")
            return 1 if require_runtime else 0
        migration_head = _migration_head_check(
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
            migration_manifest=migration_manifest,
        )
        checks["schema_migration_head"] = _result_payload(migration_head)
        if migration_head.status != "pass":
            reasons.append("alembic_version could not be verified at the checkout head")
            return 1 if require_runtime else 0

        # Do not create any schema-dependent Deployment until the migration
        # Job and its head check have both completed successfully.
        apply_result = _kubectl(
            [
                "apply",
                "--server-side",
                "-f",
                "-",
            ],
            context=context,
            timeout_seconds=timeout_seconds,
            input_text=runtime_manifest,
        )
        checks["apply"] = _result_payload(apply_result)
        if apply_result.status != "pass":
            reasons.append("Kustomize runtime resources could not be applied after migration")
            return 1 if require_runtime else 0

        rollout_checks = _rollout_all(
            namespace=namespace, context=context, timeout_seconds=timeout_seconds
        )
        checks["readiness"] = {
            "status": "pass"
            if all(item.status == "pass" for item in rollout_checks.values())
            else "fail",
            "deployments": {name: _result_payload(item) for name, item in rollout_checks.items()},
        }
        for deployment, _container in ACK_RUNTIME_DEPLOYMENTS:
            ready = _deployment_ready(
                deployment,
                namespace=namespace,
                context=context,
                timeout_seconds=timeout_seconds,
            )
            checks["readiness"]["deployments"][f"{deployment}_replicas"] = _result_payload(ready)
        if checks["readiness"]["status"] != "pass" or any(
            value.get("status") != "pass"
            for key, value in checks["readiness"]["deployments"].items()
            if not key.endswith("_replicas")
        ):
            reasons.append("one or more Kubernetes deployments did not become ready")
            return 1 if require_runtime else 0

        scheduler_result, scheduler_config = _json_command(
            ["get", "configmap", "trpc-service-config", "--namespace", namespace, "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        deployment_result, scheduler_deployments = _json_command(
            [
                "get",
                "deployments",
                "--namespace",
                namespace,
                "-l",
                "app.kubernetes.io/part-of=trpc-agent-service",
                "-o",
                "json",
            ],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        scheduler_ok, scheduler_reasons = _scheduler_runtime_contract(
            scheduler_config if scheduler_result.status == "pass" else None,
            scheduler_deployments if deployment_result.status == "pass" else None,
        )
        checks["scheduler_cutover_guard"] = {
            "status": "pass" if scheduler_ok else "fail",
            "configmap": _result_payload(scheduler_result),
            "deployments": _result_payload(deployment_result),
            "reasons": list(scheduler_reasons),
        }
        if not scheduler_ok:
            reasons.extend(scheduler_reasons)
            return 1 if require_runtime else 0

        hpa_result, hpa_payload = _wait_for_hpa_status(
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        hpa_status = hpa_payload.get("status") if hpa_payload else None
        conditions = hpa_status.get("conditions") if isinstance(hpa_status, dict) else None
        condition_map = {
            item.get("type"): item.get("status")
            for item in conditions or []
            if isinstance(item, dict) and isinstance(item.get("type"), str)
        }
        hpa_spec = hpa_payload.get("spec") if isinstance(hpa_payload, dict) else None
        hpa_min = hpa_spec.get("minReplicas") if isinstance(hpa_spec, dict) else None
        hpa_max = hpa_spec.get("maxReplicas") if isinstance(hpa_spec, dict) else None
        current_replicas = (
            hpa_status.get("currentReplicas") if isinstance(hpa_status, dict) else None
        )
        desired_replicas = (
            hpa_status.get("desiredReplicas") if isinstance(hpa_status, dict) else None
        )
        hpa_runtime_ok, hpa_runtime_reasons = _hpa_status_contract(hpa_payload)
        hpa_ok = hpa_result.status == "pass" and hpa_runtime_ok
        checks["worker_scale_and_hpa"] = {
            "status": "pass" if hpa_ok else "fail",
            "hpa": _result_payload(hpa_result),
            "conditions": condition_map,
            "min_replicas": hpa_min,
            "max_replicas": hpa_max,
            "current_replicas": current_replicas,
            "desired_replicas": desired_replicas,
            "reasons": list(hpa_runtime_reasons),
        }
        if not hpa_ok:
            reasons.append(
                "HPA did not expose current backlog/resource metrics, a healthy "
                "AbleToScale/ScalingActive state, and valid ready replica bounds"
            )
            return 1 if require_runtime else 0

        rbac_binding = _bind_hpa_driver_rbac(
            subject=hpa_driver_subject,
            namespace=namespace,
            context=context,
            timeout_seconds=min(timeout_seconds, 30),
        )
        checks["hpa_driver_rbac_bind"] = _result_payload(rbac_binding)
        if rbac_binding.status != "pass":
            reasons.append(rbac_binding.reason or "HPA driver RoleBinding could not be applied")
            return 1 if require_runtime else 0
        scope_ok, scope_reasons, scope_attestation = _hpa_driver_scope_contract(
            kubeconfig_path=hpa_driver_identity["kubeconfig_path"],
            context=context,
            namespace=namespace,
            timeout_seconds=min(timeout_seconds, 30),
            driver_context=hpa_driver_context,
            subject=hpa_driver_subject,
            cluster_fingerprint=cluster_identity["fingerprint_sha256"],
        )
        checks["hpa_driver_trust"] = {
            "status": "pass" if scope_ok else "not_run",
            "driver_sha256": hpa_driver_identity["driver_sha256"],
            "kubeconfig_sha256": hpa_driver_identity["kubeconfig_sha256"],
            "dedicated_kubeconfig": True,
            "scope": "namespace_jobs_only",
            "rbac_verified": scope_ok,
            "subject_sha256": hashlib.sha256(hpa_driver_subject.encode("utf-8")).hexdigest(),
            "driver_context_sha256": scope_attestation.get("driver_context_sha256"),
            "cluster_fingerprint_sha256": scope_attestation.get("cluster_fingerprint_sha256"),
            "identity_verified": scope_attestation.get("identity_verified") is True,
            "rule_audit": scope_attestation.get("rule_audit", {"complete": False}),
            "reasons": list(scope_reasons),
        }
        if not scope_ok:
            reasons.extend(scope_reasons)
            return 1 if require_runtime else 0

        # The load trigger is deliberately not allowed to submit evidence.
        # Observe all three phases through the API server so a driver cannot
        # manufacture replica counts or metric values in hpa-evidence.json.
        before_result, before_observation = _observe_hpa_state(
            namespace=namespace, context=context, timeout_seconds=timeout_seconds
        )
        if before_result.status == "pass":
            load_driver = _run_hpa_driver(
                hpa_driver_path,
                namespace=namespace,
                run_nonce=run_nonce,
                cluster_fingerprint=cluster_identity["fingerprint_sha256"],
                context=context,
                timeout_seconds=timeout_seconds,
                phase="load",
                driver_context=hpa_driver_context,
                driver_subject=hpa_driver_subject,
            )
            during_result, during_observation = _wait_for_hpa_phase(
                namespace=namespace,
                context=context,
                timeout_seconds=timeout_seconds,
                phase="during",
                before=before_observation or {},
            )
            # Always ask the bounded trigger to clear its own backlog before
            # the final read.  This is best-effort cleanup even when load failed.
            clear_driver = _run_hpa_driver(
                hpa_driver_path,
                namespace=namespace,
                run_nonce=run_nonce,
                cluster_fingerprint=cluster_identity["fingerprint_sha256"],
                context=context,
                timeout_seconds=timeout_seconds,
                phase="clear",
                driver_context=hpa_driver_context,
                driver_subject=hpa_driver_subject,
            )
            if during_result.status == "pass" and during_observation is not None:
                after_result, after_observation = _wait_for_hpa_phase(
                    namespace=namespace,
                    context=context,
                    timeout_seconds=timeout_seconds,
                    phase="after",
                    before=before_observation or {},
                    during=during_observation,
                )
            else:
                after_result = CommandResult(
                    status="fail", reason="HPA during observation is unavailable"
                )
                after_observation = None
        else:
            load_driver = CommandResult(
                status="not_run", reason="before HPA API observation failed"
            )
            during_result = CommandResult(
                status="not_run", reason="before HPA API observation failed"
            )
            during_observation = None
            clear_driver = CommandResult(status="not_run", reason="load phase was not started")
            after_result = CommandResult(status="not_run", reason="load phase was not started")
            after_observation = None
        api_observed = all(
            result.status == "pass" for result in (before_result, during_result, after_result)
        )
        observation: dict[str, Any] = {
            "status": "pass" if api_observed else "fail",
            "observed_live": api_observed,
            "source": "kubectl_api",
            "hpa_name": "trpc-worker",
            "metric_name": "trpc_session_ready_backlog",
            "run_nonce": run_nonce,
            "namespace": namespace,
            "cluster_identity": {"fingerprint_sha256": cluster_identity["fingerprint_sha256"]},
            "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
            "scale_up_timeout_seconds": timeout_seconds,
            "scale_down_timeout_seconds": timeout_seconds,
            "before": before_observation or {},
            "during": during_observation or {},
            "after": after_observation or {},
            "driver_evidence": {
                "load": load_driver.evidence or {},
                "clear": clear_driver.evidence or {},
            },
        }
        hpa_load_ok, hpa_load_reasons = _hpa_load_observation_contract(
            observation,
            cluster_fingerprint=cluster_identity["fingerprint_sha256"],
            run_nonce=run_nonce,
            namespace=namespace,
        )
        driver_ok = load_driver.status == "pass" and clear_driver.status == "pass"
        if not driver_ok:
            hpa_load_reasons = (
                *hpa_load_reasons,
                "bounded HPA load trigger did not complete load and clear phases",
            )
        checks["hpa_load_observation"] = {
            "status": "pass" if driver_ok and hpa_load_ok else "fail",
            "driver": {
                "load": _result_payload(load_driver),
                "clear": _result_payload(clear_driver),
            },
            "api_reads": {
                "before": _result_payload(before_result),
                "during": _result_payload(during_result),
                "after": _result_payload(after_result),
            },
            "observed_live": driver_ok and hpa_load_ok,
            "observation": _hpa_load_report_payload(observation),
            "reasons": list(hpa_load_reasons),
        }
        if not driver_ok or not hpa_load_ok:
            reasons.extend(hpa_load_reasons)
            return 1 if require_runtime else 0

        initial_image_ids: dict[str, tuple[str, ...]] = {}
        initial_image_polls: dict[str, int] = {}
        for deployment, _container in ACK_RUNTIME_DEPLOYMENTS:
            image_result, image_ids, poll_count = _wait_for_deployment_image_ids(
                deployment,
                namespace=namespace,
                context=context,
                timeout_seconds=min(timeout_seconds, 60),
            )
            if image_result.status != "pass":
                reasons.append(
                    image_result.reason or f"could not inspect initial image IDs for {deployment}"
                )
            initial_image_ids[deployment] = image_ids
            initial_image_polls[deployment] = poll_count
        checks["initial_image_ids"] = {
            deployment: list(image_ids) for deployment, image_ids in initial_image_ids.items()
        }
        checks["initial_image_id_poll_counts"] = initial_image_polls
        if reasons:
            return 1 if require_runtime else 0

        image_updates, rollouts = _rolling_upgrade_serial(
            upgrade_image,
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        upgrade_complete = len(rollouts) == len(ACK_RUNTIME_DEPLOYMENTS)
        checks["rolling_upgrade"] = {
            "status": "pass"
            if upgrade_complete
            and all(item.status == "pass" for item in image_updates.values())
            and all(item.status == "pass" for item in rollouts.values())
            else "fail",
            "deployments": {name: _result_payload(item) for name, item in rollouts.items()},
            "image_updates": {name: _result_payload(item) for name, item in image_updates.items()},
            "upgrade_image_supplied": True,
            "strategy": "serial_set_and_wait",
        }
        if checks["rolling_upgrade"]["status"] != "pass":
            reasons.append("serial rolling upgrade did not complete every deployment")
            return 1 if require_runtime else 0
        upgraded_image_ids: dict[str, tuple[str, ...]] = {}
        image_changes: dict[str, bool] = {}
        upgraded_image_polls: dict[str, int] = {}
        for deployment, _container in ACK_RUNTIME_DEPLOYMENTS:
            image_result, image_ids, poll_count = _wait_for_deployment_image_ids(
                deployment,
                namespace=namespace,
                context=context,
                timeout_seconds=min(timeout_seconds, 60),
                previous_image_ids=initial_image_ids[deployment],
            )
            upgraded_image_ids[deployment] = image_ids
            upgraded_image_polls[deployment] = poll_count
            image_changes[deployment] = image_result.status == "pass"
        checks["rolling_upgrade"]["image_ids"] = {
            "initial": {name: list(value) for name, value in initial_image_ids.items()},
            "upgrade": {name: list(value) for name, value in upgraded_image_ids.items()},
            "changed": image_changes,
            "poll_counts": upgraded_image_polls,
        }
        if not all(image_changes.values()):
            checks["rolling_upgrade"]["status"] = "fail"
            reasons.append(
                "rolling upgrade did not converge every deployment to one new immutable image ID"
            )
            return 1 if require_runtime else 0

        rollback_result, rollback_details = _failure_rollback(
            _ROLLBACK_PROBE_DEPLOYMENT,
            dict(ACK_RUNTIME_DEPLOYMENTS)[_ROLLBACK_PROBE_DEPLOYMENT],
            upgrade_image,
            upgraded_image_ids[_ROLLBACK_PROBE_DEPLOYMENT],
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["rolling_upgrade"]["rollback"] = {
            "status": rollback_result.status,
            **rollback_details,
        }
        if rollback_result.status != "pass":
            checks["rolling_upgrade"]["status"] = "fail"
            reasons.append(
                rollback_result.reason
                or "failed rollout did not recover through controller rollback"
            )
            return 1 if require_runtime else 0

        eviction_capacity, eviction_capacity_details = _prepare_worker_eviction_capacity(
            namespace=namespace,
            context=context,
            timeout_seconds=timeout_seconds,
            local_kind=allow_local_images,
        )
        checks["worker_scale_and_hpa"]["eviction_capacity_floor"] = eviction_capacity_details
        if eviction_capacity.status != "pass":
            checks["worker_scale_and_hpa"]["status"] = "fail"
            reasons.append(
                eviction_capacity.reason
                or "worker HPA eviction capacity floor did not reach four ready replicas"
            )
            return 1 if require_runtime else 0

        pdb_result, pdb_payload = _json_command(
            ["get", "pdb", "trpc-worker", "--namespace", namespace, "-o", "json"],
            context=context,
            timeout_seconds=timeout_seconds,
        )
        pdb_status = pdb_payload.get("status") if pdb_payload else None
        disruptions = pdb_status.get("disruptionsAllowed") if isinstance(pdb_status, dict) else None
        pod_result, pod_name = _first_worker_pod(
            namespace=namespace, context=context, timeout_seconds=timeout_seconds
        )
        eviction = (
            _evict_pod(
                pod_name,
                namespace=namespace,
                context=context,
                timeout_seconds=timeout_seconds,
            )
            if pod_name
            else CommandResult(status="fail", reason="no worker pod available for eviction")
        )
        evicted_pod_deleted = (
            _kubectl(
                [
                    "wait",
                    "--for=delete",
                    f"pod/{pod_name}",
                    "--namespace",
                    namespace,
                    f"--timeout={int(timeout_seconds)}s",
                ],
                context=context,
                timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
            )
            if pod_name and eviction.status == "pass"
            else CommandResult(status="fail", reason="eviction was not accepted")
        )
        replacement = _kubectl(
            [
                "wait",
                "--for=jsonpath={.status.readyReplicas}=4",
                "deployment/trpc-worker",
                "--namespace",
                namespace,
                f"--timeout={int(timeout_seconds)}s",
            ],
            context=context,
            timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
        )
        pdb_ok = (
            pdb_result.status == "pass"
            and pod_result.status == "pass"
            and eviction.status == "pass"
            and evicted_pod_deleted.status == "pass"
            and replacement.status == "pass"
            and isinstance(disruptions, int)
            and disruptions >= 1
        )
        checks["pdb_eviction"] = {
            "status": "pass" if pdb_ok else "fail",
            "pdb": _result_payload(pdb_result),
            "disruptions_allowed_before_eviction": disruptions,
            "eviction": _result_payload(eviction),
            "evicted_pod_deleted": _result_payload(evicted_pod_deleted),
            "replacement_ready": _result_payload(replacement),
        }
        if not pdb_ok:
            reasons.append("namespace-scoped worker eviction/PDB recovery failed")
            return 1 if require_runtime else 0

        graceful_pod_result, graceful_pod = _first_worker_pod(
            namespace=namespace, context=context, timeout_seconds=timeout_seconds
        )
        started = time.perf_counter()
        graceful_delete = (
            _kubectl(
                [
                    "delete",
                    "pod",
                    graceful_pod,
                    "--namespace",
                    namespace,
                    "--grace-period=90",
                    "--wait=true",
                ],
                context=context,
                timeout_seconds=min(timeout_seconds + 10, MAX_TIMEOUT_SECONDS),
            )
            if graceful_pod
            else CommandResult(status="fail", reason="no worker pod available for termination")
        )
        graceful_replacement = _kubectl(
            [
                "wait",
                "--for=jsonpath={.status.readyReplicas}=4",
                "deployment/trpc-worker",
                "--namespace",
                namespace,
                f"--timeout={int(timeout_seconds)}s",
            ],
            context=context,
            timeout_seconds=min(timeout_seconds + 5, MAX_TIMEOUT_SECONDS),
        )
        graceful_ok = (
            graceful_pod_result.status == "pass"
            and graceful_delete.status == "pass"
            and graceful_replacement.status == "pass"
        )
        checks["graceful_termination"] = {
            "status": "pass" if graceful_ok else "fail",
            "delete": _result_payload(graceful_delete),
            "replacement_ready": _result_payload(graceful_replacement),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "force_delete_used": False,
        }
        if not graceful_ok:
            reasons.append("graceful worker termination did not recover ready capacity")
            return 1 if require_runtime else 0

        node_drain, node_drain_details = _controlled_node_drain(
            node_name,
            namespace=namespace,
            label_key=node_label[0] if node_label is not None else _NODE_LABEL_KEY,
            label_value=node_label[1] if node_label is not None else "",
            context=context,
            timeout_seconds=timeout_seconds,
        )
        checks["node_eviction"] = {
            "status": node_drain.status,
            "preflight": checks["node_eviction"].get("preflight", {}),
            "drain": node_drain_details,
            "uncordon_observed": node_drain_details.get("uncordon", {}).get("status") == "pass",
        }
        if node_drain.status != "pass":
            reasons.append(node_drain.reason or "controlled node drain failed")
            return 1 if require_runtime else 0
        post_drain_rollouts = _rollout_all(
            namespace=namespace, context=context, timeout_seconds=timeout_seconds
        )
        checks["node_eviction"]["post_drain_readiness"] = {
            name: _result_payload(item) for name, item in post_drain_rollouts.items()
        }
        if not all(item.status == "pass" for item in post_drain_rollouts.values()):
            checks["node_eviction"]["status"] = "fail"
            reasons.append("controlled node drain did not preserve deployment readiness")
            return 1 if require_runtime else 0

        checks["namespace_cleanup"] = {"status": "not_run", "reason": "cleanup runs in finally"}
        gate_status = "pass"
        return 0
    except (OSError, ValueError) as error:
        reasons.append(f"runtime gate setup failed: {type(error).__name__}")
        return 1 if require_runtime else 0
    finally:
        if namespace_created:
            cleanup = _kubectl(
                [
                    "delete",
                    "namespace",
                    namespace,
                    "--wait=true",
                    "--ignore-not-found=true",
                    f"--timeout={int(timeout_seconds)}s",
                ],
                context=context,
                timeout_seconds=min(timeout_seconds + 10, MAX_TIMEOUT_SECONDS),
            )
            checks["namespace_cleanup"] = _result_payload(cleanup)
            if cleanup.status != "pass":
                reasons.append("isolated namespace cleanup failed")
        if temporary_directory is not None:
            try:
                temporary_directory.cleanup()
            except OSError:
                reasons.append("temporary runtime-gate workspace cleanup failed")
        if gate_status == "pass" and reasons:
            gate_status = "fail"
        if gate_status == "pass" and checks.get("namespace_cleanup", {}).get("status") != "pass":
            gate_status = "fail"
        if namespace_created:
            candidate["runtime_attestation"] = _build_runtime_attestation(
                candidate, context=context, cluster_stdout=cluster_stdout
            )
        final_gate = gate_status
        _report(output, gate=final_gate, candidate=candidate, rejection_reasons=reasons)


def _run_live(
    *,
    output: Path,
    context: str | None,
    timeout_seconds: float,
    require_runtime: bool,
    allow_local_images: bool = False,
) -> int:
    """Run the live body, then derive the exit code from its final report.

    ``_run_live_once`` performs cleanup and writes the report in its
    ``finally`` block.  Reading that report here prevents an early return from
    masking a cleanup/attestation failure when ``--require-runtime`` is set.
    """

    timeout_seconds = _validate_timeout_seconds(timeout_seconds)
    initial_code = _run_live_once(
        output=output,
        context=context,
        timeout_seconds=timeout_seconds,
        require_runtime=require_runtime,
        allow_local_images=allow_local_images,
    )
    try:
        report = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return 1 if require_runtime else initial_code
    if require_runtime:
        return int(not (report.get("gate") == "pass" and report.get("production_gate") == "pass"))
    return initial_code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        help=(
            "strict unified runtime configuration; when supplied, its release, "
            "image, Kubernetes, HPA, Secret-reference, node, and timeout values "
            "are validated before any cluster mutation"
        ),
    )
    parser.add_argument(
        "--preflight-output",
        type=Path,
        default=Path("runs/multitenant/deployment-preflight.json"),
    )
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/kubernetes-runtime.json")
    )
    parser.add_argument("--context")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--require-runtime",
        action="store_true",
        help="return non-zero when the live gate is not run because prerequisites are missing",
    )
    args = parser.parse_args(argv)
    if args.config is not None:
        if args.context is not None or args.timeout_seconds is not None:
            parser.error("--config cannot be combined with --context or --timeout-seconds")
        preflight, projected = build_preflight(args.config, environment=os.environ)
        atomic_write_json(args.preflight_output, preflight)
        if preflight["gate"] != "pass" or projected is None:
            candidate = {
                "mode": "configuration_preflight",
                "enabled": False,
                "checks": {
                    "configuration_preflight": {
                        "status": "fail",
                        "report": str(args.preflight_output),
                    }
                },
            }
            result = _report(
                args.output,
                gate="not_run",
                candidate=candidate,
                rejection_reasons=list(preflight["rejection_reasons"]),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1
        os.environ.update(projected)
        args.context = projected["TRPC_K8S_RUNTIME_CONTEXT"]
        args.timeout_seconds = float(projected["TRPC_K8S_RUNTIME_TIMEOUT_SECONDS"])
    else:
        if args.context is None:
            args.context = os.getenv("TRPC_K8S_RUNTIME_CONTEXT")
        if args.timeout_seconds is None:
            args.timeout_seconds = float(os.getenv("TRPC_K8S_RUNTIME_TIMEOUT_SECONDS", "600"))
    try:
        timeout_seconds = _validate_timeout_seconds(args.timeout_seconds)
    except ValueError as exc:
        parser.error(str(exc))
    if not _enabled():
        candidate = {
            "mode": "opt_in_required",
            "enabled": False,
            "checks": {},
        }
        result = _report(
            args.output,
            gate="not_run",
            candidate=candidate,
            rejection_reasons=[
                "TRPC_K8S_RUNTIME_TESTS_ENABLED=true was not supplied; "
                "live Kubernetes acceptance is opt-in"
            ],
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1 if args.require_runtime else 0
    result_code = _run_live(
        output=args.output,
        context=args.context,
        timeout_seconds=timeout_seconds,
        require_runtime=args.require_runtime,
    )
    print(args.output.read_text(encoding="utf-8"), end="")
    return result_code


if __name__ == "__main__":
    raise SystemExit(main())
