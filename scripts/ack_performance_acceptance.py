# ruff: noqa: E402

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, TypeVar
from uuid import uuid4

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import real_performance_gate
from scripts.deployment_config import load_runtime_gate_config
from scripts.evidence_lineage import source_fingerprint
from scripts.kubernetes_performance_job import (
    PREFLIGHT_EVIDENCE_PATH,
    build_spec,
    run_performance_job,
)

_Configured = TypeVar("_Configured")

FIXTURE_SECRET_ENV_NAMES: Final[tuple[str, ...]] = (
    "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
    "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
    "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
)
GATEWAY_FIXTURE_SECRET_NAME: Final[str] = "trpc-service-secrets"  # noqa: S105 - name only
GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: Final[str] = "trpc.io/performance-fixture-secret-sha256"  # noqa: S105 - annotation name
GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: Final[str] = "trpc.io/performance-fixture-secret-version"  # noqa: S105 - annotation name
GATEWAY_ENDPOINT_STABILITY_OBSERVATIONS: Final[int] = 2
GATEWAY_ENDPOINT_STABILITY_INTERVAL_SECONDS: Final[float] = 2.0
_KUBERNETES_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class GatewayFixtureSecretBinding:
    """The secret material plus its safe, public deployment binding.

    ``values`` is deliberately excluded from the representation and from
    ``public_evidence``.  It is used only to populate the one-shot Job Secret;
    reports and Kubernetes annotations carry the checksum/resource version.
    """

    values: Mapping[str, str] = field(repr=False)
    secret_name: str
    checksum: str
    resource_version: str

    @property
    def public_evidence(self) -> dict[str, str]:
        return {
            "status": "verified",
            "secret_name": self.secret_name,
            "secret_checksum_sha256": self.checksum,
            "secret_resource_version": self.resource_version,
        }


def _secret_data(path: Path, name: str) -> dict[str, str]:
    for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if isinstance(document, dict) and document.get("metadata", {}).get("name") == name:
            data = document.get("data")
            if isinstance(data, dict):
                return {str(key): str(value) for key, value in data.items()}
    raise RuntimeError(f"Secret {name} is unavailable")


def _decode(data: Mapping[str, str], key: str) -> str:
    try:
        value = base64.b64decode(data[key], validate=True).decode("utf-8")
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        raise RuntimeError(f"Secret key {key} is invalid") from error
    if not value:
        raise RuntimeError(f"Secret key {key} is empty")
    return value


def _fixture_secret_checksum(values: Mapping[str, str]) -> str:
    """Return a deterministic checksum without retaining secret material."""

    try:
        normalized = {
            name: values[name]
            for name in FIXTURE_SECRET_ENV_NAMES
            if isinstance(values[name], str) and values[name]
        }
    except (KeyError, TypeError) as error:
        raise RuntimeError("gateway performance fixture secrets are incomplete") from error
    if len(normalized) != len(FIXTURE_SECRET_ENV_NAMES):
        raise RuntimeError("gateway performance fixture secrets are incomplete")
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _gateway_deployment_secret_name(deployment: Mapping[str, Any]) -> str | None:
    """Find the service Secret that supplies the gateway fixture values."""

    try:
        containers = deployment["spec"]["template"]["spec"]["containers"]
    except (KeyError, TypeError):
        return None
    if not isinstance(containers, list) or not containers:
        return None
    container = containers[0]
    if not isinstance(container, Mapping):
        return None
    env_from = container.get("envFrom", [])
    if not isinstance(env_from, list):
        return None
    candidates: list[str] = []
    for entry in env_from:
        if not isinstance(entry, Mapping):
            continue
        secret_ref = entry.get("secretRef")
        if not isinstance(secret_ref, Mapping):
            continue
        name = secret_ref.get("name")
        if isinstance(name, str) and name:
            candidates.append(name)
    if GATEWAY_FIXTURE_SECRET_NAME in candidates:
        return GATEWAY_FIXTURE_SECRET_NAME
    return candidates[0] if candidates else None


def _gateway_deployment_env_values(deployment: Mapping[str, Any]) -> dict[str, str]:
    """Read literal fixture values for the legacy helper compatibility path."""

    try:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
    except (KeyError, IndexError, TypeError):
        raise RuntimeError("gateway performance fixture secrets are unavailable") from None
    entries = container.get("env", []) if isinstance(container, Mapping) else []
    values = {
        str(entry.get("name")): str(entry.get("value", ""))
        for entry in entries
        if isinstance(entry, Mapping)
    }
    if any(not values.get(name) for name in FIXTURE_SECRET_ENV_NAMES):
        raise RuntimeError("gateway performance fixture secrets are unavailable")
    return {name: values[name] for name in FIXTURE_SECRET_ENV_NAMES}


def _kubectl_json(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    arguments: list[str],
    operation: str,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Run a bounded Kubernetes read without exposing command output."""

    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise RuntimeError("kubectl is not installed")
    command = [
        kubectl,
        "--kubeconfig",
        str(kubeconfig),
        "--context",
        context,
        "--namespace",
        namespace,
        *arguments,
    ]
    try:
        completed = subprocess.run(  # noqa: S603 - executable resolved without a shell
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=ROOT,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(f"Kubernetes {operation} could not complete") from error
    if completed.returncode != 0:
        raise RuntimeError(f"Kubernetes {operation} failed")
    try:
        value = json.loads(completed.stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Kubernetes {operation} returned invalid JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"Kubernetes {operation} returned a non-object")
    return value


def _gateway_fixture_secret_binding(
    *, namespace: str, context: str, kubeconfig: Path, deployment_name: str
) -> GatewayFixtureSecretBinding:
    """Read fixture Secret values and derive a non-secret deployment binding."""

    deployment = _kubectl_json(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        arguments=["get", "deployment", deployment_name, "-o", "json"],
        operation="gateway deployment inspection",
    )
    secret_name = _gateway_deployment_secret_name(deployment)
    if secret_name is None:
        raise RuntimeError("gateway performance fixture Secret reference is unavailable")
    secret = _kubectl_json(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        arguments=["get", "secret", secret_name, "-o", "json"],
        operation="gateway fixture Secret inspection",
    )
    data = secret.get("data")
    if not isinstance(data, Mapping):
        raise RuntimeError("gateway performance fixture secrets are unavailable")
    try:
        values = {name: _decode(data, name) for name in FIXTURE_SECRET_ENV_NAMES}
    except RuntimeError as error:
        raise RuntimeError("gateway performance fixture secrets are unavailable") from error
    metadata = secret.get("metadata")
    resource_version = metadata.get("resourceVersion") if isinstance(metadata, Mapping) else None
    if (
        not isinstance(resource_version, str)
        or _KUBERNETES_VERSION_RE.fullmatch(resource_version) is None
    ):
        raise RuntimeError("gateway fixture Secret resource version is unavailable")
    return GatewayFixtureSecretBinding(
        values=values,
        secret_name=secret_name,
        checksum=_fixture_secret_checksum(values),
        resource_version=resource_version,
    )


def _run(command: list[str], environment: dict[str, str]) -> int:
    completed = subprocess.run(  # noqa: S603 - callers provide fixed argv lists
        command, check=False, env=environment, cwd=ROOT
    )
    return completed.returncode


def _gateway_fixture_secrets(
    *, namespace: str, context: str, kubeconfig: Path, deployment_name: str
) -> dict[str, str]:
    """Return gateway fixture values for the compatibility/test helper."""

    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise RuntimeError("kubectl is not installed")
    completed = subprocess.run(  # noqa: S603 - executable resolved without a shell
        [
            kubectl,
            "--kubeconfig",
            str(kubeconfig),
            "--context",
            context,
            "--namespace",
            namespace,
            "get",
            "deployment",
            deployment_name,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    deployment = json.loads(completed.stdout)
    if not isinstance(deployment, Mapping):
        raise RuntimeError("gateway performance fixture secrets are unavailable")
    required = FIXTURE_SECRET_ENV_NAMES
    secret_name = _gateway_deployment_secret_name(deployment)
    if secret_name is not None:
        secret_result = subprocess.run(  # noqa: S603 - executable resolved without a shell
            [
                kubectl,
                "--kubeconfig",
                str(kubeconfig),
                "--context",
                context,
                "--namespace",
                namespace,
                "get",
                "secret",
                secret_name,
                "-o",
                "json",
            ],
            check=True,
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        secret = json.loads(secret_result.stdout)
        data = secret.get("data") if isinstance(secret, dict) else None
        if not isinstance(data, dict):
            raise RuntimeError("gateway performance fixture secrets are unavailable")
        try:
            return {name: _decode(data, name) for name in required}
        except RuntimeError as error:
            raise RuntimeError("gateway performance fixture secrets are unavailable") from error

    return _gateway_deployment_env_values(deployment)


def _gateway_template_annotations(deployment: Mapping[str, Any]) -> Mapping[str, Any]:
    try:
        template = deployment["spec"]["template"]
        metadata = template["metadata"]
    except (KeyError, TypeError):
        return {}
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    return annotations if isinstance(annotations, Mapping) else {}


def _gateway_rollout_status(deployment: Mapping[str, Any]) -> tuple[bool, str]:
    """Check API-observed readiness for the deployment generation."""

    metadata = deployment.get("metadata")
    status = deployment.get("status")
    spec = deployment.get("spec")
    if (
        not isinstance(metadata, Mapping)
        or not isinstance(status, Mapping)
        or not isinstance(spec, Mapping)
    ):
        return False, "gateway deployment rollout status is incomplete"
    generation = metadata.get("generation")
    observed_generation = status.get("observedGeneration")
    replicas = spec.get("replicas")
    if (
        not isinstance(generation, int)
        or isinstance(generation, bool)
        or not isinstance(observed_generation, int)
        or isinstance(observed_generation, bool)
        or not isinstance(replicas, int)
        or isinstance(replicas, bool)
        or replicas < 1
        or observed_generation < generation
    ):
        return False, "gateway deployment rollout has not converged"
    for field_name in ("updatedReplicas", "readyReplicas", "availableReplicas"):
        value = status.get(field_name)
        if not isinstance(value, int) or isinstance(value, bool) or value != replicas:
            return False, "gateway deployment rollout has not converged"
    return True, ""


def _patch_gateway_fixture_binding(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    deployment_name: str,
    binding: GatewayFixtureSecretBinding,
    timeout_seconds: float,
) -> None:
    """Patch only the pod-template binding; never perform an unscoped restart."""

    patch = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: binding.checksum,
                        GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: binding.resource_version,
                    }
                }
            }
        }
    }
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise RuntimeError("kubectl is not installed")
    command = [
        kubectl,
        "--kubeconfig",
        str(kubeconfig),
        "--context",
        context,
        "--namespace",
        namespace,
        "patch",
        "deployment",
        deployment_name,
        "--type",
        "strategic",
        "--patch",
        json.dumps(patch, ensure_ascii=False, separators=(",", ":")),
    ]
    try:
        completed = subprocess.run(  # noqa: S603 - executable resolved without a shell
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=ROOT,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("gateway fixture Secret binding patch could not complete") from error
    if completed.returncode != 0:
        raise RuntimeError("gateway fixture Secret binding patch failed")


def _wait_for_gateway_fixture_rollout(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    deployment_name: str,
    timeout_seconds: float,
) -> None:
    """Wait for Kubernetes to report the bound gateway rollout complete."""

    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise RuntimeError("kubectl is not installed")
    command = [
        kubectl,
        "--kubeconfig",
        str(kubeconfig),
        "--context",
        context,
        "--namespace",
        namespace,
        "rollout",
        "status",
        "deployment",
        deployment_name,
        "--timeout",
        f"{max(1, int(timeout_seconds))}s",
    ]
    try:
        completed = subprocess.run(  # noqa: S603 - executable resolved without a shell
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 5.0,
            cwd=ROOT,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("gateway deployment rollout status could not complete") from error
    if completed.returncode != 0:
        raise RuntimeError("gateway deployment rollout did not converge")


def _gateway_endpoint_observation(payload: Mapping[str, Any]) -> dict[str, object]:
    """Summarize EndpointSlice readiness without retaining endpoint addresses."""

    items = payload.get("items")
    if not isinstance(items, list):
        raise RuntimeError("gateway EndpointSlice response is invalid")

    ready_endpoints = 0
    unready_endpoints = 0
    terminating_endpoints = 0
    endpoint_keys: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            raise RuntimeError("gateway EndpointSlice response contains an invalid slice")
        endpoints = item.get("endpoints", [])
        if not isinstance(endpoints, list):
            raise RuntimeError("gateway EndpointSlice response contains invalid endpoints")
        for endpoint in endpoints:
            if not isinstance(endpoint, Mapping):
                raise RuntimeError("gateway EndpointSlice response contains an invalid endpoint")
            addresses = endpoint.get("addresses")
            if (
                not isinstance(addresses, list)
                or not addresses
                or any(not isinstance(address, str) or not address for address in addresses)
            ):
                raise RuntimeError("gateway EndpointSlice response contains invalid addresses")
            endpoint_keys.append("\x1f".join(sorted(addresses)))
            conditions = endpoint.get("conditions", {})
            if conditions is None:
                conditions = {}
            if not isinstance(conditions, Mapping):
                raise RuntimeError("gateway EndpointSlice response contains invalid conditions")
            if conditions.get("ready") is True:
                ready_endpoints += 1
            else:
                unready_endpoints += 1
            if conditions.get("terminating") is True:
                terminating_endpoints += 1

    endpoint_set = json.dumps(sorted(endpoint_keys), separators=(",", ":"))
    return {
        "ready_endpoints": ready_endpoints,
        "unready_endpoints": unready_endpoints,
        "terminating_endpoints": terminating_endpoints,
        "total_endpoints": len(endpoint_keys),
        "endpoint_set_sha256": hashlib.sha256(endpoint_set.encode("utf-8")).hexdigest(),
    }


def _gateway_endpoint_timeout_reason(
    *,
    expected_replicas: int,
    timeout_seconds: float,
    stable_observations: int,
    last_observation: Mapping[str, object] | None,
) -> str:
    if last_observation is None:
        details = "no valid EndpointSlice observation"
    else:
        details = (
            f"ready endpoints {last_observation.get('ready_endpoints', 0)}/{expected_replicas}, "
            f"unready={last_observation.get('unready_endpoints', 0)}, "
            f"terminating={last_observation.get('terminating_endpoints', 0)}, "
            f"stable observations {stable_observations}/"
            f"{GATEWAY_ENDPOINT_STABILITY_OBSERVATIONS}"
        )
    return f"gateway EndpointSlice stability check timed out after {timeout_seconds:g}s: {details}"


def _wait_for_gateway_endpoint_stability(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    service_name: str,
    expected_replicas: int,
    timeout_seconds: float,
) -> dict[str, object]:
    """Require ready gateway endpoints and two identical observations."""

    if expected_replicas < 1:
        raise RuntimeError("gateway EndpointSlice stability requires a positive replica target")
    if timeout_seconds <= 0:
        raise RuntimeError("gateway EndpointSlice stability timeout must be positive")

    deadline = time.monotonic() + timeout_seconds
    previous_signature: tuple[int, int, int, int, str] | None = None
    stable_observations = 0
    observations = 0
    last_observation: dict[str, object] | None = None

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                _gateway_endpoint_timeout_reason(
                    expected_replicas=expected_replicas,
                    timeout_seconds=timeout_seconds,
                    stable_observations=stable_observations,
                    last_observation=last_observation,
                )
            )
        try:
            payload = _kubectl_json(
                namespace=namespace,
                context=context,
                kubeconfig=kubeconfig,
                arguments=[
                    "get",
                    "endpointslice",
                    "-l",
                    f"kubernetes.io/service-name={service_name}",
                    "-o",
                    "json",
                ],
                operation="gateway EndpointSlice inspection",
                timeout_seconds=min(30.0, remaining),
            )
            observation = _gateway_endpoint_observation(payload)
        except RuntimeError as error:
            raise RuntimeError(f"gateway EndpointSlice stability check failed: {error}") from error

        observations += 1
        last_observation = observation
        ready_count = observation.get("ready_endpoints")
        unready_count = observation.get("unready_endpoints")
        terminating_count = observation.get("terminating_endpoints")
        total_count = observation.get("total_endpoints")
        endpoint_set_sha256 = observation.get("endpoint_set_sha256")
        if (
            not isinstance(ready_count, int)
            or isinstance(ready_count, bool)
            or not isinstance(unready_count, int)
            or isinstance(unready_count, bool)
            or not isinstance(terminating_count, int)
            or isinstance(terminating_count, bool)
            or not isinstance(total_count, int)
            or isinstance(total_count, bool)
            or not isinstance(endpoint_set_sha256, str)
        ):
            raise RuntimeError("gateway EndpointSlice observation is invalid")
        signature = (
            ready_count,
            unready_count,
            terminating_count,
            total_count,
            endpoint_set_sha256,
        )
        healthy = signature[0] >= expected_replicas and signature[1] == 0 and signature[2] == 0
        if healthy and signature == previous_signature:
            stable_observations += 1
        elif healthy:
            stable_observations = 1
        else:
            stable_observations = 0
        previous_signature = signature

        if stable_observations >= GATEWAY_ENDPOINT_STABILITY_OBSERVATIONS:
            return {
                "status": "pass",
                "service": service_name,
                "expected_replicas": expected_replicas,
                **observation,
                "observations": observations,
                "stable_observations": stable_observations,
                "required_stable_observations": GATEWAY_ENDPOINT_STABILITY_OBSERVATIONS,
                "interval_seconds": GATEWAY_ENDPOINT_STABILITY_INTERVAL_SECONDS,
            }

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                _gateway_endpoint_timeout_reason(
                    expected_replicas=expected_replicas,
                    timeout_seconds=timeout_seconds,
                    stable_observations=stable_observations,
                    last_observation=last_observation,
                )
            )
        time.sleep(min(GATEWAY_ENDPOINT_STABILITY_INTERVAL_SECONDS, remaining))


def _ensure_gateway_fixture_rollout(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    deployment_name: str,
    binding: GatewayFixtureSecretBinding,
    timeout_seconds: float,
) -> dict[str, object]:
    """Bind the gateway template to the current Secret before creating a Job."""

    initial = _kubectl_json(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        arguments=["get", "deployment", deployment_name, "-o", "json"],
        operation="gateway deployment inspection",
    )
    annotations = _gateway_template_annotations(initial)
    changed = (
        annotations.get(GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION) != binding.checksum
        or annotations.get(GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION) != binding.resource_version
    )
    if changed:
        _patch_gateway_fixture_binding(
            namespace=namespace,
            context=context,
            kubeconfig=kubeconfig,
            deployment_name=deployment_name,
            binding=binding,
            timeout_seconds=min(timeout_seconds, 60.0),
        )
    _wait_for_gateway_fixture_rollout(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        deployment_name=deployment_name,
        timeout_seconds=timeout_seconds,
    )
    current = _kubectl_json(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        arguments=["get", "deployment", deployment_name, "-o", "json"],
        operation="gateway rollout inspection",
    )
    current_annotations = _gateway_template_annotations(current)
    if (
        current_annotations.get(GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION) != binding.checksum
        or current_annotations.get(GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION)
        != binding.resource_version
    ):
        raise RuntimeError("gateway deployment template is not bound to fixture Secret")
    ready, reason = _gateway_rollout_status(current)
    if not ready:
        raise RuntimeError(reason)

    latest_secret = _kubectl_json(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        arguments=["get", "secret", binding.secret_name, "-o", "json"],
        operation="gateway fixture Secret reinspection",
    )
    latest_data = latest_secret.get("data")
    if not isinstance(latest_data, Mapping):
        raise RuntimeError("gateway performance fixture secrets are unavailable")
    try:
        latest_values = {name: _decode(latest_data, name) for name in FIXTURE_SECRET_ENV_NAMES}
    except RuntimeError as error:
        raise RuntimeError("gateway performance fixture secrets are unavailable") from error
    latest_metadata = latest_secret.get("metadata")
    latest_version = (
        latest_metadata.get("resourceVersion") if isinstance(latest_metadata, Mapping) else None
    )
    if (
        latest_version != binding.resource_version
        or _fixture_secret_checksum(latest_values) != binding.checksum
    ):
        raise RuntimeError("gateway fixture Secret changed during rollout")
    current_spec = current.get("spec")
    expected_replicas = current_spec.get("replicas") if isinstance(current_spec, Mapping) else None
    if (
        not isinstance(expected_replicas, int)
        or isinstance(expected_replicas, bool)
        or expected_replicas < 1
    ):
        raise RuntimeError("gateway deployment replica target is unavailable")
    endpoint_stability = _wait_for_gateway_endpoint_stability(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        service_name=deployment_name,
        expected_replicas=expected_replicas,
        timeout_seconds=timeout_seconds,
    )
    return {
        "status": "pass",
        "deployment": deployment_name,
        "secret_name": binding.secret_name,
        "secret_checksum_sha256": binding.checksum,
        "secret_resource_version": binding.resource_version,
        "template_updated": changed,
        "rollout_confirmed": True,
        "endpoint_stability": endpoint_stability,
    }


def _runtime_performance(config_path: Path) -> tuple[Any, Any]:
    config = load_runtime_gate_config(config_path)
    performance = config.performance
    if performance is None or not performance.enabled:
        raise RuntimeError("runtime-gate.yaml Kubernetes performance runner is disabled")
    return config, performance


def _candidate_image_reference(config: Any, requested: str | None) -> tuple[str, str, str]:
    reference = config.resolved_image_references()["initial"]
    configured_digest = reference.rsplit("@", 1)[-1].lower()
    requested_digest = requested.rsplit("@", 1)[-1].lower() if requested else configured_digest
    if requested_digest != configured_digest:
        raise RuntimeError("requested image digest does not match runtime-gate.yaml")
    return reference, configured_digest, config.image_pull_secret


def _configured_value(name: str, requested: object, configured: _Configured) -> _Configured:
    if requested not in (None, "") and requested != configured:
        raise RuntimeError(f"{name} must match runtime-gate.yaml")
    return configured


def _configured_path(name: str, requested: Path | None, configured: Path) -> Path:
    if requested is not None and requested.expanduser().resolve() != configured.resolve():
        raise RuntimeError(f"{name} must match runtime-gate.yaml")
    return configured


def _checkout_source(requested: str | None) -> str:
    current = source_fingerprint(ROOT).get("value")
    if not isinstance(current, str) or not current:
        raise RuntimeError("candidate source fingerprint is unavailable")
    if requested and requested.lower() != current.lower():
        raise RuntimeError("requested source fingerprint does not match checkout")
    return current.lower()


def _preflight(
    *,
    namespace: str,
    context: str,
    kubeconfig: Path,
    image_digest: str,
    source: str,
    max_inflight: int,
    db_pool_size: int,
    min_workers: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Run the official gate preflight locally before submitting the Job."""

    gate_args = real_performance_gate._parser().parse_args(
        [
            "--kubernetes",
            "--kubernetes-namespace",
            namespace,
            "--kubernetes-context",
            context,
            "--kubernetes-kubeconfig",
            str(kubeconfig),
            "--kubernetes-image-digest",
            image_digest,
            "--kubernetes-source-fingerprint",
            source,
            "--callbacks",
            "200",
            "--callback-rate",
            "105",
            "--burst-turns",
            "200",
            "--max-inflight",
            str(max_inflight),
            "--db-pool-size",
            str(db_pool_size),
            "--min-workers",
            str(min_workers),
            "--timeout-seconds",
            str(min(timeout_seconds, real_performance_gate.MAX_TIMEOUT_SECONDS)),
        ]
    )
    preflight = real_performance_gate._preflight(gate_args)
    if not isinstance(preflight, Mapping):
        raise RuntimeError("Kubernetes performance preflight returned invalid evidence")
    return dict(preflight)


def _service_host(service_dns: str) -> str:
    return service_dns.split(".", 1)[0]


def _gate_command(
    *,
    namespace: str,
    context: str,
    image_digest: str,
    source: str,
    evidence_path: str,
    max_inflight: int,
    db_pool_size: int,
    min_workers: int,
    timeout_seconds: float,
    output: str,
) -> list[str]:
    return [
        "python",
        "scripts/real_performance_gate.py",
        "--load-worker",
        "--kubernetes-load-worker",
        "--kubernetes",
        "--kubernetes-namespace",
        namespace,
        "--kubernetes-context",
        context,
        "--kubernetes-image-digest",
        image_digest,
        "--kubernetes-source-fingerprint",
        source,
        "--kubernetes-preflight-evidence",
        evidence_path,
        "--execute",
        "--confirm-real-load",
        "--callbacks",
        "200",
        "--callback-rate",
        "105",
        "--burst-turns",
        "200",
        "--max-inflight",
        str(max_inflight),
        "--db-pool-size",
        str(db_pool_size),
        "--min-workers",
        str(min_workers),
        "--timeout-seconds",
        str(timeout_seconds),
        "--output",
        output,
        "--require-production",
    ]


def _build_job_spec(
    *,
    config: Any,
    performance: Any,
    namespace: str,
    context: str,
    kubeconfig: Path,
    image_reference: str,
    image_digest: str,
    image_pull_secret: str,
    source: str,
    run_id: str,
    worker_token: str,
    service: Mapping[str, str],
    fixture_secrets: Mapping[str, str],
    preflight_evidence: Mapping[str, Any],
    job_timeout: float,
) -> Any:
    """Project the unified runtime config and signed preflight into one Job spec."""

    gate_timeout = min(job_timeout, real_performance_gate.MAX_TIMEOUT_SECONDS)
    gate_output = "/tmp/trpc-performance-report.json"  # noqa: S108
    gate_command = _gate_command(
        namespace=namespace,
        context=context,
        image_digest=image_digest,
        source=source,
        evidence_path=PREFLIGHT_EVIDENCE_PATH,
        max_inflight=performance.max_inflight,
        db_pool_size=performance.db_pool_size,
        min_workers=performance.workers,
        timeout_seconds=gate_timeout,
        output=gate_output,
    )
    secret_env = {
        key: _decode(service, key)
        for key in (
            "TRPC_SERVICE_DATABASE_DSN",
            "TRPC_SERVICE_REDIS_URL",
            "TRPC_SERVICE_SESSION_HMAC_KEY",
        )
    }
    secret_env.update(fixture_secrets)
    secret_env["TRPC_REAL_PERFORMANCE_WORKER_TOKEN"] = worker_token
    release_nonce = os.getenv("TRPC_RELEASE_NONCE")
    if release_nonce:
        secret_env["TRPC_RELEASE_NONCE"] = release_nonce
    config_env = {
        "TRPC_PERF_GATEWAY_BASE_URL": performance.gateway_url,
        "TRPC_PERF_FIXTURE_CONFIRM": "I_UNDERSTAND_PERFORMANCE_FIXTURE",
        "TRPC_PERF_FIXTURE_REMOTE_CONFIRM": "I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE",
        "TRPC_REAL_PERFORMANCE_CONFIRM": "I_UNDERSTAND_REAL_LOAD",
        "TRPC_RUN_REAL_MULTINODE": "1",
        "TRPC_RELEASE_ID": config.release_id,
        "TRPC_REAL_IMAGE_DIGEST": image_digest,
        "TRPC_PERF_K8S_LOAD_JOB": "1",
        "TRPC_K8S_PERF_GATE_COMMAND": json.dumps(gate_command, separators=(",", ":")),
        "TRPC_K8S_PERF_RUN_ID": run_id,
        "TRPC_K8S_PERF_NAMESPACE": namespace,
        "TRPC_K8S_PERF_CONTEXT": context,
        "TRPC_K8S_PERF_POSTGRES_SERVICE_DNS": performance.postgres_service,
        "TRPC_K8S_PERF_REDIS_SERVICE_DNS": performance.redis_service,
        "TRPC_K8S_PERF_POSTGRES_PORT": str(performance.postgres_port),
        "TRPC_K8S_PERF_REDIS_PORT": str(performance.redis_port),
        "TRPC_K8S_PERF_TIMEOUT_SECONDS": str(job_timeout),
        "TRPC_K8S_PERF_REPORT_PATH": gate_output,
        "TRPC_K8S_PERF_FIXTURE_PATH": "/tmp/trpc-performance-fixture.json",  # noqa: S108
        "TRPC_K8S_PERF_CLEANUP_PATH": "/tmp/trpc-performance-cleanup.json",  # noqa: S108
        "TRPC_K8S_PERF_PREFLIGHT_EVIDENCE_PATH": PREFLIGHT_EVIDENCE_PATH,
    }
    return build_spec(
        namespace=namespace,
        context=context,
        kubeconfig=kubeconfig,
        image=image_reference,
        image_pull_secret=image_pull_secret,
        source_fingerprint=source,
        run_id=run_id,
        secret_env=secret_env,
        config_env=config_env,
        command=("python", "scripts/kubernetes_performance_job.py", "--worker"),
        node_selector=performance.node_selector,
        toleration={
            "key": performance.taint_key,
            "operator": "Equal",
            "value": performance.taint_value,
            "effect": performance.taint_effect,
        },
        requests={
            "cpu": performance.resources.request_cpu,
            "memory": performance.resources.request_memory,
        },
        limits={
            "cpu": performance.resources.limit_cpu,
            "memory": performance.resources.limit_memory,
        },
        preflight_evidence=preflight_evidence,
        timeout_seconds=job_timeout,
    )


def _write_json(path: Path, payload: object) -> None:
    if path.is_symlink() or any(parent.exists() and parent.is_symlink() for parent in path.parents):
        raise RuntimeError("report path must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _failure_report(
    reason: str,
    *,
    image_digest: str,
    source_fingerprint: str,
    preflight: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    gateway_fixture_binding: Mapping[str, object] | None = None,
) -> dict[str, object]:
    candidate: dict[str, object] = {
        "mode": "real_postgresql_redis_multiprocess",
        "parameters": {},
        "supervision": {
            "status": "fail",
            "load_generator": "kubernetes_job",
            "image_digest": image_digest,
            "source_fingerprint": source_fingerprint,
            "run_id": run_id,
        },
    }
    if preflight is not None:
        candidate["preflight"] = dict(preflight)
    if gateway_fixture_binding is not None:
        supervision = candidate["supervision"]
        if isinstance(supervision, dict):
            supervision["gateway_fixture_secret_binding"] = dict(gateway_fixture_binding)
    return {
        "schema_version": 1,
        "baseline": {},
        "candidate": candidate,
        "case_deltas": {},
        "gate": "fail",
        "production_gate": "fail",
        "rejection_reasons": [reason],
        "production_rejection_reasons": [reason],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=ROOT / "deploy/runtime-gate.yaml")
    parser.add_argument("--secret-manifest", type=Path)
    parser.add_argument("--namespace")
    parser.add_argument("--context")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--image-digest")
    parser.add_argument("--source-fingerprint")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture-output", type=Path, required=True)
    parser.add_argument("--cleanup-output", type=Path, required=True)
    parser.add_argument("--gateway-service")
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--max-inflight", type=int)
    parser.add_argument("--db-pool-size", type=int)
    parser.add_argument("--job-timeout-seconds", type=float)
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args()

    gateway_fixture_binding: GatewayFixtureSecretBinding | None = None
    gateway_rollout: Mapping[str, object] | None = None
    try:
        config, performance = _runtime_performance(args.config)
        namespace = str(
            _configured_value("performance namespace", args.namespace, performance.namespace)
        )
        context = str(_configured_value("Kubernetes context", args.context, config.context))
        kubeconfig = _configured_path("Kubernetes kubeconfig", args.kubeconfig, config.kubeconfig)
        secret_manifest = _configured_path(
            "Secret manifest", args.secret_manifest, config.secret_manifest
        )
        _configured_value("gateway Service DNS", args.gateway_service, performance.gateway_service)
        _configured_value("gateway port", args.gateway_port, performance.gateway_port)
        max_inflight = int(
            _configured_value("max-inflight", args.max_inflight, performance.max_inflight)
        )
        db_pool_size = int(
            _configured_value("db-pool-size", args.db_pool_size, performance.db_pool_size)
        )
        job_timeout = float(
            _configured_value("Job timeout", args.job_timeout_seconds, config.timeout_seconds)
        )
        source = _checkout_source(args.source_fingerprint)
        image_reference, image_digest, image_pull_secret = _candidate_image_reference(
            config, args.image_digest
        )
        service = _secret_data(secret_manifest, "trpc-service-secrets")
        gateway_fixture_binding = _gateway_fixture_secret_binding(
            namespace=namespace,
            context=context,
            kubeconfig=kubeconfig,
            deployment_name=_service_host(performance.gateway_service),
        )
        fixture_secrets = dict(gateway_fixture_binding.values)
    except (OSError, ValueError, RuntimeError) as error:
        report = _failure_report(
            str(error),
            image_digest="unknown",
            source_fingerprint=args.source_fingerprint or "unknown",
            gateway_fixture_binding=(
                gateway_fixture_binding.public_evidence if gateway_fixture_binding else None
            ),
        )
        _write_json(args.output, report)
        return 1

    if args.cleanup_only:
        environment = os.environ.copy()
        environment.update(
            {
                "TRPC_RUN_REAL_MULTINODE": "1",
                "TRPC_PERF_FIXTURE_CONFIRM": "I_UNDERSTAND_PERFORMANCE_FIXTURE",
                "TRPC_PERF_FIXTURE_REMOTE_CONFIRM": "I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE",
                "TRPC_PERF_DATABASE_DSN": _decode(service, "TRPC_SERVICE_DATABASE_DSN"),
                **fixture_secrets,
            }
        )
        fixture = json.loads(args.fixture_output.read_text(encoding="utf-8"))
        return _run(
            [
                sys.executable,
                "scripts/performance_fixture.py",
                "cleanup",
                "--execute",
                "--allow-remote",
                "--report",
                str(args.fixture_output),
                "--tenant-id",
                str(fixture["tenant_id"]),
                "--run-id",
                str(fixture["run_id"]),
                "--output",
                str(args.cleanup_output),
            ],
            environment,
        )

    run_id = uuid4().hex
    worker_token = uuid4().hex
    try:
        if gateway_fixture_binding is None:
            raise RuntimeError("gateway fixture Secret binding is unavailable")
        gateway_rollout = _ensure_gateway_fixture_rollout(
            namespace=namespace,
            context=context,
            kubeconfig=kubeconfig,
            deployment_name=_service_host(performance.gateway_service),
            binding=gateway_fixture_binding,
            timeout_seconds=min(job_timeout, 300.0),
        )
        preflight = _preflight(
            namespace=namespace,
            context=context,
            kubeconfig=kubeconfig,
            image_digest=image_digest,
            source=source,
            max_inflight=max_inflight,
            db_pool_size=db_pool_size,
            min_workers=performance.workers,
            timeout_seconds=job_timeout,
        )
        if preflight.get("status") != "pass":
            reason = str(preflight.get("reason", "Kubernetes performance preflight failed"))
            report = _failure_report(
                reason,
                image_digest=image_digest,
                source_fingerprint=source,
                preflight=preflight,
                run_id=run_id,
                gateway_fixture_binding=(
                    gateway_rollout or gateway_fixture_binding.public_evidence
                ),
            )
            _write_json(args.output, report)
            return 1
        preflight_evidence = real_performance_gate.build_kubernetes_preflight_evidence(
            preflight,
            run_id=run_id,
            run_token=worker_token,
            source_fingerprint=source,
            image_digest=image_digest,
        )
        job_output = args.output.with_name(f"{args.output.stem}.kubernetes-job.json")
        spec = _build_job_spec(
            config=config,
            performance=performance,
            namespace=namespace,
            context=context,
            kubeconfig=kubeconfig,
            image_reference=image_reference,
            image_digest=image_digest,
            image_pull_secret=image_pull_secret,
            source=source,
            run_id=run_id,
            worker_token=worker_token,
            service=service,
            fixture_secrets=fixture_secrets,
            preflight_evidence=preflight_evidence,
            job_timeout=job_timeout,
        )
        job_result = run_performance_job(spec, job_output)
    except (OSError, ValueError, RuntimeError) as error:
        report = _failure_report(
            str(error),
            image_digest=image_digest,
            source_fingerprint=source,
            run_id=run_id,
            gateway_fixture_binding=(
                gateway_rollout
                or (gateway_fixture_binding.public_evidence if gateway_fixture_binding else None)
            ),
        )
        _write_json(args.output, report)
        return 1

    worker_report = job_result.get("report")
    if isinstance(worker_report, Mapping):
        gate_report = worker_report.get("report")
    else:
        gate_report = None
    if isinstance(gate_report, Mapping):
        report = dict(gate_report)
    else:
        report = _failure_report(
            "Kubernetes performance Job did not produce a gate report",
            image_digest=image_digest,
            source_fingerprint=source,
            run_id=run_id,
            gateway_fixture_binding=(
                gateway_rollout
                or (gateway_fixture_binding.public_evidence if gateway_fixture_binding else None)
            ),
        )
    candidate = report.setdefault("candidate", {})
    if not isinstance(candidate, dict):
        candidate = {}
        report["candidate"] = candidate
    candidate["supervision"] = {
        "status": job_result.get("status"),
        "load_generator": "kubernetes_job",
        "image_digest": image_digest,
        "source_fingerprint": source,
        "run_id": run_id,
        "preflight_evidence": (
            worker_report.get("evidence_binding", {"status": "not_run"})
            if isinstance(worker_report, Mapping)
            else {"status": "not_run"}
        ),
        "job": job_result.get("job", {}),
        "gateway_fixture_secret_binding": (
            gateway_rollout
            or (gateway_fixture_binding.public_evidence if gateway_fixture_binding else None)
        ),
    }
    if job_result.get("status") != "pass":
        report["gate"] = "fail"
        report["production_gate"] = "fail"
        reasons = report.setdefault("production_rejection_reasons", [])
        if isinstance(reasons, list):
            reasons.extend(str(item) for item in job_result.get("rejection_reasons", []))
    _write_json(args.output, report)

    fixture = worker_report.get("fixture") if isinstance(worker_report, Mapping) else None
    cleanup = worker_report.get("cleanup") if isinstance(worker_report, Mapping) else None
    if isinstance(fixture, dict):
        _write_json(args.fixture_output, fixture)
    if isinstance(cleanup, dict):
        _write_json(args.cleanup_output, cleanup)
    return 0 if report.get("gate") == "pass" and report.get("production_gate") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
