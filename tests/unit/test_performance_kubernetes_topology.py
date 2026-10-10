"""Contract tests for the explicit in-cluster performance overlay."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
OVERLAY = ROOT / "deploy" / "kustomize" / "overlays" / "performance"
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _documents(name: str) -> list[dict]:
    path = OVERLAY / name
    return [
        document
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8"))
        if isinstance(document, dict)
    ]


def test_performance_overlay_is_opt_in_and_pins_one_canonical_dockerhub_digest() -> None:
    overlay = yaml.safe_load((OVERLAY / "kustomization.yaml").read_text(encoding="utf-8"))

    assert overlay["namespace"] == "trpc-service"
    assert "../../base" in overlay["resources"]
    assert "performance-network-policy.yaml" in overlay["resources"]
    image = overlay["images"][0]
    assert image["newName"] == "docker.io/zixuan760/trpc-agent-service"
    assert DIGEST_RE.fullmatch(image["digest"])


def test_performance_overlay_locks_four_workers_at_fifty_turns_each() -> None:
    config = _documents("performance-config-patch.yaml")[0]["data"]
    replicas = _documents("performance-replicas-patch.yaml")
    replica_values = {(item["kind"], item["metadata"]["name"]): item["spec"] for item in replicas}

    assert config["TRPC_PERF_K8S_MAX_INFLIGHT"] == "64"
    assert config["TRPC_PERF_K8S_DB_POOL_SIZE"] == "32"
    assert config["TRPC_PERF_K8S_WORKERS"] == "4"
    assert config["TRPC_PERF_K8S_WORKER_CONCURRENCY"] == "50"
    assert config["TRPC_SERVICE_ENVIRONMENT"] == "test"
    assert config["TRPC_SERVICE_CAPTURE_CONTENT"] == "false"
    assert config["TRPC_SERVICE_OTLP_ENDPOINT"] == ""
    assert config["TRPC_SERVICE_PROMETHEUS_ENABLED"] == "false"
    assert config["TRPC_SERVICE_S3_ENDPOINT"] == (
        "http://minio.trpc-runtime-support.svc.cluster.local:9000"
    )
    assert config["TRPC_SERVICE_S3_BUCKET"] == "trpc-artifacts"
    assert config["TRPC_SERVICE_WORKER_CONCURRENCY"] == "50"
    assert config["TRPC_SERVICE_TENANT_SECRET_ENV_NAMES"] == (
        '["TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",'
        '"TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",'
        '"TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY"]'
    )
    assert config["TRPC_PERF_K8S_WORKLOAD_NODE_LABEL"] == "trpc-role=workload"
    assert config["TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MIN_SIZE"] == "5"
    assert config["TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MAX_SIZE"] == "6"
    assert config["TRPC_PERF_K8S_WORKER_DATABASE_POOL_MIN_SIZE"] == "2"
    assert config["TRPC_PERF_K8S_WORKER_DATABASE_POOL_MAX_SIZE"] == "8"
    assert config["TRPC_PERF_K8S_WORKER_OFFLINE_AGENT_DELAY_SECONDS"] == "3.0"
    assert config["TRPC_PERF_K8S_OUTBOX_DATABASE_POOL_MIN_SIZE"] == "2"
    assert config["TRPC_PERF_K8S_OUTBOX_DATABASE_POOL_MAX_SIZE"] == "4"
    assert config["TRPC_PERF_K8S_OUTBOX_REPLICAS"] == "1"
    assert config["TRPC_PERF_K8S_RECOVERY_DATABASE_POOL_MIN_SIZE"] == "1"
    assert config["TRPC_PERF_K8S_RECOVERY_DATABASE_POOL_MAX_SIZE"] == "2"
    assert config["TRPC_PERF_K8S_RECOVERY_REPLICAS"] == "1"
    assert config["TRPC_PERF_K8S_RECOVERY_POLL_SECONDS"] == "1"
    assert config["TRPC_PERF_K8S_ESTIMATED_RUNTIME_CONNECTIONS"] == "97"
    assert replica_values[("Deployment", "trpc-worker")]["replicas"] == 4
    assert replica_values[("Deployment", "trpc-outbox-dispatcher")]["replicas"] == 1
    assert replica_values[("Deployment", "trpc-session-recovery")]["replicas"] == 1
    for name in (
        "trpc-admin",
        "trpc-artifact-gc",
        "trpc-channel-dispatcher",
        "trpc-post-turn-projector",
        "trpc-wecom-connector",
    ):
        assert replica_values[("Deployment", name)]["replicas"] == 0
    assert replica_values[("HorizontalPodAutoscaler", "trpc-worker")]["minReplicas"] == 4
    assert replica_values[("HorizontalPodAutoscaler", "trpc-worker")]["maxReplicas"] == 4


def test_performance_overlay_pins_runtime_roles_to_workload_node() -> None:
    patches = _documents("performance-workload-patch.yaml")
    expected = {
        "trpc-gateway": "gateway",
        "trpc-worker": "worker",
        "trpc-outbox-dispatcher": "outbox-dispatcher",
        "trpc-session-recovery": "session-recovery",
    }
    assert {
        item["metadata"]["name"]: item["spec"]["template"]["spec"] for item in patches
    }.keys() == expected.keys()
    for name, container_name in expected.items():
        spec = next(
            item["spec"]["template"]["spec"] for item in patches if item["metadata"]["name"] == name
        )
        assert spec["nodeSelector"] == {"trpc-role": "workload"}
        assert spec["containers"][0]["name"] == container_name
        assert all(entry["value"] for entry in spec["containers"][0]["env"])


def test_performance_network_policy_allows_only_runner_targets() -> None:
    documents = _documents("performance-network-policy.yaml")
    egress = next(
        item
        for item in documents
        if item["metadata"]["name"] == "trpc-allow-performance-runner-egress"
    )
    ingress = next(
        item
        for item in documents
        if item["metadata"]["name"] == "trpc-allow-performance-runner-ingress"
    )

    assert egress["spec"]["podSelector"]["matchLabels"] == {"trpc.io/performance-runner": "true"}
    ports = {rule["ports"][0]["port"] for rule in egress["spec"]["egress"] if rule.get("ports")}
    assert ports == {53, 5432, 6379, 8080}
    assert ingress["spec"]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "trpc-gateway"
    }
    assert ingress["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"] == {
        "trpc.io/performance-runner": "true"
    }
