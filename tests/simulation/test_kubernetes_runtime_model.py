from __future__ import annotations

import math
from pathlib import Path

import yaml

from trpc_service.config.settings import ServiceSettings

BASE = Path(__file__).resolve().parents[2] / "deploy" / "kustomize" / "base"


def _resources(filename: str) -> list[dict]:
    return [
        item for item in yaml.safe_load_all((BASE / filename).read_text(encoding="utf-8")) if item
    ]


def _resource(filename: str, kind: str, name: str) -> dict:
    return next(
        item
        for item in _resources(filename)
        if item["kind"] == kind and item["metadata"]["name"] == name
    )


def _simulate_rolling_update(
    replicas: int, *, max_unavailable: int, max_surge: int
) -> tuple[int, int]:
    old_ready = replicas
    new_ready = 0
    minimum_ready = replicas
    steps = 0
    while new_ready < replicas:
        if old_ready + new_ready < replicas + max_surge:
            new_ready += 1
            steps += 1
        if old_ready and old_ready + new_ready - 1 >= replicas - max_unavailable:
            old_ready -= 1
            steps += 1
        minimum_ready = min(minimum_ready, old_ready + new_ready)
    return minimum_ready, steps


def test_virtual_rollout_hpa_eviction_and_graceful_shutdown() -> None:
    worker = _resource("deployments.yaml", "Deployment", "trpc-worker")
    hpa = _resource("autoscaling.yaml", "HorizontalPodAutoscaler", "trpc-worker")
    pdb = _resource("disruption.yaml", "PodDisruptionBudget", "trpc-worker")

    replicas = worker["spec"]["replicas"]
    rolling = worker["spec"]["strategy"]["rollingUpdate"]
    minimum_ready, rollout_steps = _simulate_rolling_update(
        replicas,
        max_unavailable=rolling["maxUnavailable"],
        max_surge=rolling["maxSurge"],
    )
    assert rollout_steps > 0 and minimum_ready == replicas

    minimum_available = pdb["spec"]["minAvailable"]
    ready = replicas
    assert ready - 1 >= minimum_available
    ready -= 1  # first node eviction is admitted
    assert ready - 1 < minimum_available  # a second concurrent eviction is denied
    ready += 1  # replacement becomes ready
    assert ready == replicas

    cpu_target = next(
        metric["resource"]["target"]["averageUtilization"]
        for metric in hpa["spec"]["metrics"]
        if metric["resource"]["name"] == "cpu"
    )
    raw_desired = math.ceil(replicas * 210 / cpu_target)
    scale_up = hpa["spec"]["behavior"]["scaleUp"]
    allowed_increase = max(
        math.ceil(replicas * policy["value"] / 100)
        if policy["type"] == "Percent"
        else policy["value"]
        for policy in scale_up["policies"]
    )
    scaled = min(hpa["spec"]["maxReplicas"], replicas + allowed_increase, raw_desired)
    assert hpa["spec"]["minReplicas"] <= scaled <= hpa["spec"]["maxReplicas"]
    assert scaled > replicas
    assert hpa["spec"]["behavior"]["scaleDown"]["stabilizationWindowSeconds"] >= 300

    pod_spec = worker["spec"]["template"]["spec"]
    command = pod_spec["containers"][0]["lifecycle"]["preStop"]["exec"]["command"]
    # Shutdown is now delegated to the service lifecycle contract instead of
    # a blind sleep; the role drains before Kubernetes starts termination.
    assert command[:2] == ["trpc-service", "drain"]
    lease_seconds = ServiceSettings.model_fields["lease_seconds"].default
    assert pod_spec["terminationGracePeriodSeconds"] >= lease_seconds
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert pod_spec["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
