from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

from scripts import deployment_preflight
from scripts.deployment_config import (
    DeploymentConfigError,
    PerformanceRunnerConfig,
    RuntimeSupportConfig,
    load_runtime_gate_config,
    secret_manifest_metadata,
)
from scripts.deployment_preflight import build_preflight
from scripts.evidence_lineage import source_fingerprint
from trpc_service.tenant.models import TenantConfig

ROOT = Path(__file__).resolve().parents[2]
NONCE = "n" * 32
PULL_REGISTRY = "mirror.example.com"
SUPPORT_POSTGRES_IMAGE = f"{PULL_REGISTRY}/library/postgres@sha256:" + "3" * 64
SUPPORT_REDIS_IMAGE = f"{PULL_REGISTRY}/library/redis@sha256:" + "4" * 64
SUPPORT_MINIO_IMAGE = f"{PULL_REGISTRY}/minio/minio@sha256:" + "5" * 64
SUPPORT_MINIO_CLIENT_IMAGE = f"{PULL_REGISTRY}/minio/mc@sha256:" + "8" * 64
SUPPORT_PROMETHEUS_IMAGE = f"{PULL_REGISTRY}/prom/prometheus@sha256:" + "6" * 64
SUPPORT_PROMETHEUS_ADAPTER_IMAGE = (
    f"{PULL_REGISTRY}/prometheuscommunity/prometheus-adapter@sha256:" + "7" * 64
)
HPA_JOB_IMAGE = f"{PULL_REGISTRY}/example/runtime@sha256:" + "1" * 64


def _available_port_checks(ports: Mapping[str, int]) -> list[dict[str, object]]:
    return [
        {
            "name": f"compose_port_{name}",
            "status": "pass",
            "port": port,
            "excluded_by_host": False,
        }
        for name, port in ports.items()
    ]


def _reachable_manifest_check(_config: object) -> dict[str, object]:
    return {
        "name": "image_manifest_reachability",
        "status": "pass",
        "verified_images": ["initial", "upgrade"],
        "credentials_recorded": False,
    }


def test_port_checks_report_available_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[tuple[str, int]] = []

    class Probe:
        def __enter__(self) -> Probe:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def bind(self, address: tuple[str, int]) -> None:
            observed.append(address)

    monkeypatch.setattr(deployment_preflight, "_excluded_windows_ports", lambda: [])
    monkeypatch.setattr(deployment_preflight.socket, "socket", lambda *_args: Probe())

    assert deployment_preflight._port_checks({"postgres": 35432}) == [
        {
            "name": "compose_port_postgres",
            "status": "pass",
            "port": 35432,
            "excluded_by_host": False,
        }
    ]
    assert observed == [("127.0.0.1", 35432)]


def test_port_checks_fail_for_busy_and_host_excluded_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bind_attempts = 0

    class BusyProbe:
        def __enter__(self) -> BusyProbe:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def bind(self, _address: tuple[str, int]) -> None:
            nonlocal bind_attempts
            bind_attempts += 1
            raise OSError("busy")

    monkeypatch.setattr(deployment_preflight, "_excluded_windows_ports", lambda: [(46000, 47000)])
    monkeypatch.setattr(deployment_preflight.socket, "socket", lambda *_args: BusyProbe())

    assert deployment_preflight._port_checks({"busy": 35432, "excluded": 46379}) == [
        {
            "name": "compose_port_busy",
            "status": "fail",
            "port": 35432,
            "excluded_by_host": False,
        },
        {
            "name": "compose_port_excluded",
            "status": "fail",
            "port": 46379,
            "excluded_by_host": True,
        },
    ]
    assert bind_attempts == 1


def _add_support_config(config_path: Path, *, body: str | None = None) -> None:
    support = body or (
        "  support:\n"
        "    data_node: ack-data-0\n"
        f"    postgres_image: {SUPPORT_POSTGRES_IMAGE}\n"
        f"    redis_image: {SUPPORT_REDIS_IMAGE}\n"
        f"    minio_image: {SUPPORT_MINIO_IMAGE}\n"
        f"    minio_client_image: {SUPPORT_MINIO_CLIENT_IMAGE}\n"
        f"    prometheus_image: {SUPPORT_PROMETHEUS_IMAGE}\n"
        f"    prometheus_adapter_image: {SUPPORT_PROMETHEUS_ADAPTER_IMAGE}\n"
        "    postgres_host_path: /srv/trpc/support/postgres\n"
        "    redis_host_path: /srv/trpc/support/redis\n"
        "    minio_host_path: /srv/trpc/support/minio\n"
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "  image_pull_secret: ghcr-pull\n",
            "  image_pull_secret: ghcr-pull\n" + support,
        ),
        encoding="utf-8",
    )


def _add_performance_config(config_path: Path, *, enabled: bool = True) -> None:
    performance = (
        "  performance:\n"
        f"    enabled: {'true' if enabled else 'false'}\n"
        "    namespace: trpc-service\n"
        "    service_dns:\n"
        "      gateway: trpc-gateway.trpc-service.svc.cluster.local\n"
        "      postgres: postgres.trpc-runtime-support.svc.cluster.local\n"
        "      redis: redis.trpc-runtime-support.svc.cluster.local\n"
        "    gateway_port: 8080\n"
        "    postgres_port: 5432\n"
        "    redis_port: 6379\n"
        "    runner:\n"
        "      node_label: trpc-role=load-driver\n"
        "      taint:\n"
        "        key: trpc-role\n"
        "        value: load-driver\n"
        "        effect: NoSchedule\n"
        "      resources:\n"
        "        requests:\n"
        '          cpu: "2"\n'
        "          memory: 2Gi\n"
        "        limits:\n"
        '          cpu: "4"\n'
        "          memory: 4Gi\n"
        "      max_inflight: 64\n"
        "      db_pool_size: 32\n"
        "    workload:\n"
        "      node_label: trpc-role=workload\n"
        "      gateway:\n"
        "        replicas: 4\n"
        "        database_pool:\n"
        "          min_size: 5\n"
        "          max_size: 6\n"
        "      worker:\n"
        "        database_pool:\n"
        "          min_size: 2\n"
        "          max_size: 8\n"
        "        offline_agent_delay_seconds: 3.0\n"
        "      outbox:\n"
        "        replicas: 1\n"
        "        database_pool:\n"
        "          min_size: 2\n"
        "          max_size: 4\n"
        "      recovery:\n"
        "        replicas: 1\n"
        "        database_pool:\n"
        "          min_size: 1\n"
        "          max_size: 2\n"
        "        poll_seconds: 1\n"
        "    fixture_secret_env_names:\n"
        "      - TRPC_PERF_FIXTURE_UNUSED_APP_SECRET\n"
        "      - TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN\n"
        "      - TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY\n"
        "    workers: 4\n"
        "    worker_concurrency: 50\n"
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("  node:\n", performance + "  node:\n"),
        encoding="utf-8",
    )


def _set_hpa_backlog_metric(config_path: Path, value: str) -> None:
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "    job_command: [python, -c, 'print(\"load\")']\n",
            "    job_command: [python, -c, 'print(\"load\")']\n"
            f"    backlog_metric_enabled: {value}\n",
            1,
        ),
        encoding="utf-8",
    )


def _write_inputs(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    release_id = "acceptance-1"
    nonce_hash = hashlib.sha256(NONCE.encode()).hexdigest()
    initial = "ghcr.io/example/runtime@sha256:" + "1" * 64
    upgrade = "ghcr.io/example/runtime@sha256:" + "2" * 64
    binding = {
        "release_binding": {"release_id": release_id, "nonce_sha256": nonce_hash},
        "source_fingerprint": source_fingerprint(ROOT),
        "images": {
            "initial": {"reference": initial},
            "upgrade": {"reference": upgrade},
        },
    }
    (tmp_path / "binding.json").write_text(json.dumps(binding), encoding="utf-8")
    (tmp_path / "admin.kubeconfig").write_text("admin", encoding="utf-8")
    (tmp_path / "driver.kubeconfig").write_text("driver", encoding="utf-8")
    (tmp_path / "secrets.yaml").write_text(
        """
apiVersion: v1
kind: Secret
metadata: {name: trpc-service-secrets}
stringData:
  TRPC_SERVICE_DATABASE_DSN: secret
  TRPC_SERVICE_REDIS_URL: secret
  TRPC_SERVICE_SESSION_HMAC_KEY: secret
  TRPC_SERVICE_EMERGENCY_QUEUE_KEY: secret
  TRPC_SERVICE_S3_ACCESS_KEY: secret
  TRPC_SERVICE_S3_SECRET_KEY: secret
---
apiVersion: v1
kind: Secret
metadata: {name: trpc-worker-secrets}
stringData:
  TRPC_SERVICE_WORKER_DATABASE_DSN_REF: env://worker
  TRPC_SERVICE_WORKER_DATABASE_DSN: secret
  TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF: env://password
  TRPC_SERVICE_WORKER_DATABASE_PASSWORD: secret
---
apiVersion: v1
kind: Secret
metadata: {name: trpc-migration-secrets}
stringData: {TRPC_SERVICE_DATABASE_DSN: secret}
---
apiVersion: v1
kind: Secret
metadata: {name: ghcr-pull}
type: kubernetes.io/dockerconfigjson
data: {.dockerconfigjson: c2VjcmV0}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = tmp_path / "runtime-gate.yaml"
    driver_path = (ROOT / "scripts" / "kubernetes_hpa_load_driver.py").as_posix()
    config.write_text(
        f"""
schema_version: 1
release:
  id: {release_id}
  nonce_env: TEST_RELEASE_NONCE
  image_binding: binding.json
kubernetes:
  kubeconfig: admin.kubeconfig
  context: admin-context
  timeout_seconds: 900
  secret_manifest: secrets.yaml
  image_pull_secret: ghcr-pull
  pull_registry: {PULL_REGISTRY}
  object_store:
    endpoint: http://minio.runtime-support.svc.cluster.local:9000
    bucket: trpc-artifacts
  node:
    name: acceptance-node
    label: trpc-runtime-gate=acceptance
    drain_confirmation: I_UNDERSTAND_ISOLATED_NODE_DRAIN
  hpa:
    driver: {driver_path}
    kubeconfig: driver.kubeconfig
    context: driver-context
    subject: system:serviceaccount:runtime-driver:hpa-driver
    job_image: {HPA_JOB_IMAGE}
    job_command: [python, -c, 'print("load")']
compose:
  ports:
    postgres: 35432
    redis: 36379
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config, {"TEST_RELEASE_NONCE": NONCE}


def test_load_config_projects_all_runtime_environment(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert environment["TRPC_RELEASE_ID"] == "acceptance-1"
    assert environment["TRPC_RELEASE_NONCE"] == NONCE
    assert environment["TRPC_K8S_RUNTIME_IMAGE"].endswith("1" * 64)
    assert environment["TRPC_K8S_RUNTIME_UPGRADE_IMAGE"].endswith("2" * 64)
    assert environment["TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET"] == "ghcr-pull"
    assert environment["TRPC_K8S_RUNTIME_HPA_JOB_IMAGE"].endswith("1" * 64)
    assert environment["TRPC_K8S_RUNTIME_HPA_BACKLOG_METRIC_ENABLED"] == "false"
    assert environment["TRPC_K8S_RUNTIME_S3_ENDPOINT"] == (
        "http://minio.runtime-support.svc.cluster.local:9000"
    )
    assert environment["TRPC_K8S_RUNTIME_S3_BUCKET"] == "trpc-artifacts"
    assert (
        environment["TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256"]
        == hashlib.sha256(
            (ROOT / "scripts" / "kubernetes_hpa_load_driver.py").read_bytes()
        ).hexdigest()
    )
    assert environment["POSTGRES_PORT"] == "35432"
    assert environment["REDIS_PORT"] == "36379"
    assert config.support is None
    assert config.requires_real_hpa_backlog is False
    assert "trpc-metrics-secrets" not in config.required_secret_keys()


def test_release_id_defaults_to_the_immutable_candidate_binding(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("  id: acceptance-1\n", "", 1),
        encoding="utf-8",
    )

    config = load_runtime_gate_config(config_path)

    assert config.release_id == "acceptance-1"
    assert config.environment(source)["TRPC_RELEASE_ID"] == "acceptance-1"


def test_explicit_release_id_must_match_the_candidate_binding(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "  id: acceptance-1\n", "  id: acceptance-other\n", 1
        ),
        encoding="utf-8",
    )

    config = load_runtime_gate_config(config_path)
    with pytest.raises(DeploymentConfigError, match="release_id does not match"):
        config.environment(source)


def test_support_config_is_projected_to_explicit_environment_names(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)
    _add_support_config(config_path)

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert config.support == RuntimeSupportConfig(
        data_node="ack-data-0",
        postgres_image=SUPPORT_POSTGRES_IMAGE,
        redis_image=SUPPORT_REDIS_IMAGE,
        minio_image=SUPPORT_MINIO_IMAGE,
        minio_client_image=SUPPORT_MINIO_CLIENT_IMAGE,
        prometheus_image=SUPPORT_PROMETHEUS_IMAGE,
        prometheus_adapter_image=SUPPORT_PROMETHEUS_ADAPTER_IMAGE,
        postgres_host_path="/srv/trpc/support/postgres",
        redis_host_path="/srv/trpc/support/redis",
        minio_host_path="/srv/trpc/support/minio",
    )
    assert environment["TRPC_K8S_SUPPORT_DATA_NODE"] == "ack-data-0"
    assert environment["TRPC_K8S_SUPPORT_POSTGRES_IMAGE"] == SUPPORT_POSTGRES_IMAGE
    assert environment["TRPC_K8S_SUPPORT_REDIS_IMAGE"] == SUPPORT_REDIS_IMAGE
    assert environment["TRPC_K8S_SUPPORT_MINIO_IMAGE"] == SUPPORT_MINIO_IMAGE
    assert environment["TRPC_K8S_SUPPORT_MINIO_CLIENT_IMAGE"] == SUPPORT_MINIO_CLIENT_IMAGE
    assert environment["TRPC_K8S_SUPPORT_PROMETHEUS_IMAGE"] == SUPPORT_PROMETHEUS_IMAGE
    assert (
        environment["TRPC_K8S_SUPPORT_PROMETHEUS_ADAPTER_IMAGE"] == SUPPORT_PROMETHEUS_ADAPTER_IMAGE
    )
    assert environment["TRPC_K8S_SUPPORT_POSTGRES_HOST_PATH"] == "/srv/trpc/support/postgres"
    assert environment["TRPC_K8S_SUPPORT_REDIS_HOST_PATH"] == "/srv/trpc/support/redis"
    assert environment["TRPC_K8S_SUPPORT_MINIO_HOST_PATH"] == "/srv/trpc/support/minio"


def test_performance_config_projects_explicit_cluster_topology(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)
    _add_performance_config(config_path)

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert isinstance(config.performance, PerformanceRunnerConfig)
    assert config.performance.enabled is True
    assert config.performance.node_selector == {"trpc-role": "load-driver"}
    assert config.performance.gateway_url == (
        "http://trpc-gateway.trpc-service.svc.cluster.local:8080"
    )
    assert config.performance.max_inflight == 64
    assert config.performance.db_pool_size == 32
    assert config.performance.workers == 4
    assert config.performance.worker_concurrency == 50
    assert config.performance.workload.node_selector == {"trpc-role": "workload"}
    assert config.performance.workload.gateway.replicas == 4
    assert config.performance.workload.gateway.database_pool_min_size == 5
    assert config.performance.workload.gateway.database_pool_max_size == 6
    assert config.performance.workload.worker.database_pool_min_size == 2
    assert config.performance.workload.worker.database_pool_max_size == 8
    assert config.performance.workload.worker.offline_agent_delay_seconds == 3.0
    assert config.performance.workload.outbox.database_pool_min_size == 2
    assert config.performance.workload.outbox.database_pool_max_size == 4
    assert config.performance.workload.recovery.database_pool_min_size == 1
    assert config.performance.workload.recovery.database_pool_max_size == 2
    assert config.performance.workload.recovery.poll_seconds == 1.0
    assert config.performance.workload.outbox.replicas == 1
    assert config.performance.workload.recovery.replicas == 1
    assert config.performance.workload.estimated_runtime_connections == 97
    assert config.performance.fixture_secret_env_names == (
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
    )
    assert environment["TRPC_PERF_K8S_ENABLED"] == "true"
    assert environment["TRPC_PERF_K8S_GATEWAY_SERVICE"] == (
        "trpc-gateway.trpc-service.svc.cluster.local"
    )
    assert environment["TRPC_PERF_K8S_POSTGRES_SERVICE"] == (
        "postgres.trpc-runtime-support.svc.cluster.local"
    )
    assert environment["TRPC_PERF_K8S_REDIS_SERVICE"] == (
        "redis.trpc-runtime-support.svc.cluster.local"
    )
    assert environment["TRPC_PERF_K8S_MAX_INFLIGHT"] == "64"
    assert environment["TRPC_PERF_K8S_DB_POOL_SIZE"] == "32"
    assert environment["TRPC_PERF_K8S_WORKERS"] == "4"
    assert environment["TRPC_PERF_K8S_WORKER_CONCURRENCY"] == "50"
    assert environment["TRPC_SERVICE_TENANT_SECRET_ENV_NAMES"] == (
        '["TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",'
        '"TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",'
        '"TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY"]'
    )
    assert environment["TRPC_PERF_K8S_WORKLOAD_NODE_LABEL"] == "trpc-role=workload"
    assert environment["TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MIN_SIZE"] == "5"
    assert environment["TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MAX_SIZE"] == "6"
    assert environment["TRPC_PERF_K8S_WORKER_DATABASE_POOL_MIN_SIZE"] == "2"
    assert environment["TRPC_PERF_K8S_WORKER_DATABASE_POOL_MAX_SIZE"] == "8"
    assert environment["TRPC_PERF_K8S_WORKER_OFFLINE_AGENT_DELAY_SECONDS"] == "3.0"
    assert environment["TRPC_PERF_K8S_OUTBOX_DATABASE_POOL_MAX_SIZE"] == "4"
    assert environment["TRPC_PERF_K8S_RECOVERY_DATABASE_POOL_MAX_SIZE"] == "2"
    assert environment["TRPC_PERF_K8S_RECOVERY_POLL_SECONDS"] == "1"
    assert environment["TRPC_PERF_K8S_OUTBOX_REPLICAS"] == "1"
    assert environment["TRPC_PERF_K8S_RECOVERY_REPLICAS"] == "1"
    assert environment["TRPC_PERF_K8S_ESTIMATED_RUNTIME_CONNECTIONS"] == "97"
    assert environment["TRPC_PERF_K8S_IMAGE"].endswith("1" * 64)
    assert environment["TRPC_PERF_K8S_IMAGE_DIGEST"] == "sha256:" + "1" * 64


def test_disabled_performance_config_remains_explicitly_disabled(tmp_path: Path) -> None:
    config_path, source = _write_inputs(tmp_path)
    _add_performance_config(config_path, enabled=False)

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert config.performance is not None
    assert config.performance.enabled is False
    assert environment["TRPC_PERF_K8S_ENABLED"] == "false"


def test_enabled_performance_config_requires_locked_capacity_values(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_performance_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("max_inflight: 64", "max_inflight: 63"),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match="max_inflight must be 64"):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    (
        ("          min_size: 5\n", "          min_size: 0\n", "min_size"),
        ("          max_size: 6\n", "          max_size: 4\n", "max_size must be >="),
        (
            "        offline_agent_delay_seconds: 3.0\n",
            "        offline_agent_delay_seconds: 0\n",
            "offline_agent_delay_seconds",
        ),
        ("        poll_seconds: 1\n", "        poll_seconds: 0\n", "poll_seconds"),
    ),
)
def test_performance_workload_rejects_non_positive_or_inverted_values(
    tmp_path: Path, old: str, new: str, message: str
) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_performance_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(old, new, 1),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match=message):
        load_runtime_gate_config(config_path)


def test_performance_workload_rejects_connection_budget_overrun(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_performance_config(config_path, enabled=False)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "          max_size: 8\n", "          max_size: 100\n", 1
        ),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match="connection total exceeds"):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize(
    ("old", "new"),
    (
        ("node_label: trpc-role=workload", "node_label: trpc-role=other"),
        ("          min_size: 5\n", "          min_size: 4\n"),
        ("          max_size: 6\n", "          max_size: 7\n"),
        (
            "        offline_agent_delay_seconds: 3.0",
            "        offline_agent_delay_seconds: 2.0",
        ),
        (
            "        replicas: 1\n        database_pool:\n          min_size: 2",
            "        replicas: 2\n        database_pool:\n          min_size: 2",
        ),
        ("        poll_seconds: 1\n", "        poll_seconds: 2\n"),
    ),
)
def test_enabled_performance_workload_is_locked_to_gate_topology(
    tmp_path: Path, old: str, new: str
) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_performance_config(config_path)
    original = config_path.read_text(encoding="utf-8")
    config_path.write_text(original.replace(old, new, 1), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="locked performance topology"):
        load_runtime_gate_config(config_path)


def test_support_must_be_a_strict_mapping_with_all_recovery_fields(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(config_path, body="  support: not-a-map\n")
    with pytest.raises(DeploymentConfigError, match=r"kubernetes\.support must be a mapping"):
        load_runtime_gate_config(config_path)

    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(config_path, body="  support: null\n")
    with pytest.raises(DeploymentConfigError, match=r"kubernetes\.support must be a mapping"):
        load_runtime_gate_config(config_path)

    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(
        config_path,
        body=(
            "  support:\n"
            "    data_node: ack-data-0\n"
            f"    postgres_image: {SUPPORT_POSTGRES_IMAGE}\n"
            f"    redis_image: {SUPPORT_REDIS_IMAGE}\n"
            f"    minio_image: {SUPPORT_MINIO_IMAGE}\n"
            f"    minio_client_image: {SUPPORT_MINIO_CLIENT_IMAGE}\n"
            f"    prometheus_image: {SUPPORT_PROMETHEUS_IMAGE}\n"
            f"    prometheus_adapter_image: {SUPPORT_PROMETHEUS_ADAPTER_IMAGE}\n"
            "    postgres_host_path: /srv/trpc/support/postgres\n"
            "    redis_host_path: /srv/trpc/support/redis\n"
            "    minio_host_path: /srv/trpc/support/minio\n"
            "    unsupported: true\n"
        ),
    )
    with pytest.raises(DeploymentConfigError, match="unknown fields"):
        load_runtime_gate_config(config_path)

    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(
        config_path,
        body=(
            "  support:\n"
            "    data_node: ack-data-0\n"
            f"    redis_image: {SUPPORT_REDIS_IMAGE}\n"
            f"    minio_image: {SUPPORT_MINIO_IMAGE}\n"
            f"    minio_client_image: {SUPPORT_MINIO_CLIENT_IMAGE}\n"
            f"    prometheus_image: {SUPPORT_PROMETHEUS_IMAGE}\n"
            f"    prometheus_adapter_image: {SUPPORT_PROMETHEUS_ADAPTER_IMAGE}\n"
            "    postgres_host_path: /srv/trpc/support/postgres\n"
            "    redis_host_path: /srv/trpc/support/redis\n"
            "    minio_host_path: /srv/trpc/support/minio\n"
        ),
    )
    with pytest.raises(DeploymentConfigError, match="postgres_image"):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize(
    "image",
    [
        "registry-1.docker.io/library/postgres:16",
        "registry-1.docker.io/library/postgres@sha256:short",
        "registry-1.docker.io/library/postgres@sha256:" + "A" * 64,
    ],
)
def test_support_postgres_image_must_be_immutable_sha256(tmp_path: Path, image: str) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(
        config_path,
        body=(
            "  support:\n"
            "    data_node: ack-data-0\n"
            f"    postgres_image: {image}\n"
            f"    redis_image: {SUPPORT_REDIS_IMAGE}\n"
            f"    minio_image: {SUPPORT_MINIO_IMAGE}\n"
            f"    minio_client_image: {SUPPORT_MINIO_CLIENT_IMAGE}\n"
            f"    prometheus_image: {SUPPORT_PROMETHEUS_IMAGE}\n"
            f"    prometheus_adapter_image: {SUPPORT_PROMETHEUS_ADAPTER_IMAGE}\n"
            "    postgres_host_path: /srv/trpc/support/postgres\n"
            "    redis_host_path: /srv/trpc/support/redis\n"
            "    minio_host_path: /srv/trpc/support/minio\n"
        ),
    )

    with pytest.raises(DeploymentConfigError, match="postgres_image"):
        load_runtime_gate_config(config_path)


def test_support_redis_image_must_be_immutable_sha256(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(
        config_path,
        body=(
            "  support:\n"
            "    data_node: ack-data-0\n"
            f"    postgres_image: {SUPPORT_POSTGRES_IMAGE}\n"
            "    redis_image: redis:7.4-alpine\n"
            f"    minio_image: {SUPPORT_MINIO_IMAGE}\n"
            f"    minio_client_image: {SUPPORT_MINIO_CLIENT_IMAGE}\n"
            f"    prometheus_image: {SUPPORT_PROMETHEUS_IMAGE}\n"
            f"    prometheus_adapter_image: {SUPPORT_PROMETHEUS_ADAPTER_IMAGE}\n"
            "    postgres_host_path: /srv/trpc/support/postgres\n"
            "    redis_host_path: /srv/trpc/support/redis\n"
            "    minio_host_path: /srv/trpc/support/minio\n"
        ),
    )

    with pytest.raises(DeploymentConfigError, match="redis_image"):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize(
    ("field", "valid", "invalid"),
    (
        ("minio_image", SUPPORT_MINIO_IMAGE, "minio:latest"),
        ("minio_client_image", SUPPORT_MINIO_CLIENT_IMAGE, "minio/mc:latest"),
        ("prometheus_image", SUPPORT_PROMETHEUS_IMAGE, "prometheus:latest"),
        (
            "prometheus_adapter_image",
            SUPPORT_PROMETHEUS_ADAPTER_IMAGE,
            "prometheus-adapter:latest",
        ),
    ),
)
def test_support_provider_images_must_be_immutable_sha256(
    tmp_path: Path, field: str, valid: str, invalid: str
) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"    {field}: {valid}\n", f"    {field}: {invalid}\n", 1
        ),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match=field):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize(
    ("field", "valid"),
    (
        ("postgres_image", SUPPORT_POSTGRES_IMAGE),
        ("redis_image", SUPPORT_REDIS_IMAGE),
        ("minio_image", SUPPORT_MINIO_IMAGE),
        ("minio_client_image", SUPPORT_MINIO_CLIENT_IMAGE),
        ("prometheus_image", SUPPORT_PROMETHEUS_IMAGE),
        ("prometheus_adapter_image", SUPPORT_PROMETHEUS_ADAPTER_IMAGE),
    ),
)
@pytest.mark.parametrize("registry_host", ("docker.io", "registry.example"))
def test_support_images_must_use_the_ack_pull_registry(
    tmp_path: Path, field: str, valid: str, registry_host: str
) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _add_support_config(config_path)
    wrong_registry_image = f"{registry_host}/{valid.split('/', 1)[1]}"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"    {field}: {valid}\n", f"    {field}: {wrong_registry_image}\n", 1
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        DeploymentConfigError,
        match=rf"kubernetes\.support\.{field} registry host must match",
    ):
        load_runtime_gate_config(config_path)


def test_hpa_backlog_metric_enables_the_dedicated_metrics_secret_contract(
    tmp_path: Path,
) -> None:
    config_path, source = _write_inputs(tmp_path)
    _set_hpa_backlog_metric(config_path, "true")

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert config.requires_real_hpa_backlog is True
    assert config.required_secret_keys()["trpc-metrics-secrets"] == {
        "TRPC_SERVICE_METRICS_DATABASE_DSN"
    }
    assert environment["TRPC_K8S_RUNTIME_HPA_BACKLOG_METRIC_ENABLED"] == "true"


def test_hpa_backlog_metric_flag_is_strictly_boolean(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    _set_hpa_backlog_metric(config_path, "not-bool")

    with pytest.raises(
        DeploymentConfigError,
        match=r"kubernetes\.hpa\.backlog_metric_enabled must be a boolean",
    ):
        load_runtime_gate_config(config_path)


@pytest.mark.parametrize("registry_host", ("docker.io", "registry.example"))
def test_hpa_job_image_must_use_the_ack_pull_registry(tmp_path: Path, registry_host: str) -> None:
    config_path, _ = _write_inputs(tmp_path)
    wrong_registry_image = f"{registry_host}/{HPA_JOB_IMAGE.split('/', 1)[1]}"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"    job_image: {HPA_JOB_IMAGE}\n",
            f"    job_image: {wrong_registry_image}\n",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        DeploymentConfigError,
        match=r"kubernetes\.hpa\.job_image registry host must match",
    ):
        load_runtime_gate_config(config_path)


def test_pull_registry_rewrites_candidate_images_and_accepts_pinned_job_image(
    tmp_path: Path,
) -> None:
    config_path, source = _write_inputs(tmp_path)

    config = load_runtime_gate_config(config_path)
    environment = config.environment(source)

    assert environment["TRPC_K8S_RUNTIME_IMAGE"] == (
        f"{PULL_REGISTRY}/example/runtime@sha256:" + "1" * 64
    )
    assert environment["TRPC_K8S_RUNTIME_UPGRADE_IMAGE"] == (
        f"{PULL_REGISTRY}/example/runtime@sha256:" + "2" * 64
    )
    assert environment["TRPC_K8S_RUNTIME_HPA_JOB_IMAGE"] == HPA_JOB_IMAGE


def test_pull_registry_must_be_a_host_without_path(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"  pull_registry: {PULL_REGISTRY}\n",
            "  pull_registry: https://registry.example/pull\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match="pull_registry"):
        load_runtime_gate_config(config_path)


def test_pull_registry_is_required_for_ack_image_validation(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"  pull_registry: {PULL_REGISTRY}\n", "", 1
        ),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match=r"missing fields: pull_registry"):
        load_runtime_gate_config(config_path)


def test_runtime_gate_example_uses_canonical_dockerhub_for_all_ack_images() -> None:
    document = yaml.safe_load(
        (ROOT / "deploy" / "runtime-gate.example.yaml").read_text(encoding="utf-8")
    )
    kubernetes = document["kubernetes"]
    support = kubernetes["support"]

    assert kubernetes["pull_registry"] == "docker.io"
    images = (
        support["postgres_image"],
        support["redis_image"],
        support["minio_image"],
        support["minio_client_image"],
        support["prometheus_image"],
        support["prometheus_adapter_image"],
        kubernetes["hpa"]["job_image"],
    )
    assert all(image.startswith("docker.io/") for image in images)


def test_duplicate_and_unknown_keys_are_rejected(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    original = config_path.read_text(encoding="utf-8")
    config_path.write_text(
        original.replace("schema_version: 1", "schema_version: 1\nschema_version: 1"),
        encoding="utf-8",
    )
    with pytest.raises(DeploymentConfigError, match="duplicate configuration key"):
        load_runtime_gate_config(config_path)

    config_path.write_text(original + "unknown: true\n", encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="unknown fields"):
        load_runtime_gate_config(config_path)


def test_binding_nonce_and_source_must_match(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    config = load_runtime_gate_config(config_path)
    with pytest.raises(DeploymentConfigError, match="nonce"):
        config.environment({"TEST_RELEASE_NONCE": "x" * 32})

    binding_path = tmp_path / "binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["source_fingerprint"]["value"] = "0" * 64
    binding_path.write_text(json.dumps(binding), encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="source fingerprint"):
        config.environment({"TEST_RELEASE_NONCE": NONCE})


def test_secret_metadata_never_returns_values(tmp_path: Path) -> None:
    config_path, _ = _write_inputs(tmp_path)
    config = load_runtime_gate_config(config_path)
    metadata = secret_manifest_metadata(config.secret_manifest)

    assert ".dockerconfigjson" in metadata["ghcr-pull"]
    assert "c2VjcmV0" not in repr(metadata)


def test_preflight_aggregates_files_secrets_and_ports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _write_inputs(tmp_path)
    monkeypatch.setattr(deployment_preflight, "_port_checks", _available_port_checks)
    monkeypatch.setattr(
        deployment_preflight, "_manifest_reachability_check", _reachable_manifest_check
    )

    report, projected = build_preflight(config_path, environment=source)

    assert report["gate"] == "pass"
    assert report["secrets_recorded"] is False
    assert report["config_content_sha256"] == hashlib.sha256(config_path.read_bytes()).hexdigest()
    assert projected is not None
    assert all(check["status"] == "pass" for check in report["checks"])
    assert {"compose_port_postgres", "compose_port_redis"} <= {
        check["name"] for check in report["checks"]
    }
    serialized = json.dumps(report)
    assert "c2VjcmV0" not in serialized
    assert NONCE not in serialized


def test_preflight_requires_metrics_secret_when_real_backlog_is_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _write_inputs(tmp_path)
    _set_hpa_backlog_metric(config_path, "true")
    monkeypatch.setattr(
        deployment_preflight, "_manifest_reachability_check", _reachable_manifest_check
    )

    report, projected = build_preflight(config_path, environment=source)

    check = next(item for item in report["checks"] if item["name"] == "secret_manifest_contract")
    assert report["gate"] == "fail"
    assert projected is None
    assert check["missing_keys"] == {"trpc-metrics-secrets": ["TRPC_SERVICE_METRICS_DATABASE_DSN"]}


def test_preflight_reports_all_missing_files_without_secret_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _write_inputs(tmp_path)
    monkeypatch.setattr(deployment_preflight, "_port_checks", _available_port_checks)
    monkeypatch.setattr(
        deployment_preflight, "_manifest_reachability_check", _reachable_manifest_check
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f"driver: {(ROOT / 'scripts' / 'kubernetes_hpa_load_driver.py').as_posix()}",
            "driver: missing-driver.py",
        ),
        encoding="utf-8",
    )
    for name in (
        "binding.json",
        "admin.kubeconfig",
        "secrets.yaml",
        "driver.kubeconfig",
    ):
        (tmp_path / name).unlink()

    report, projected = build_preflight(config_path, environment=source)

    assert report["gate"] == "fail"
    assert projected is None
    assert {check["name"] for check in report["checks"] if check["status"] == "fail"} == {
        "image_binding",
        "admin_kubeconfig",
        "secret_manifest",
        "hpa_driver",
        "hpa_kubeconfig",
        "image_reference_contract",
        "release_binding",
        "secret_manifest_contract",
        "hpa_driver_identity",
        "hpa_kubeconfig_separation",
    }


def test_preflight_rejects_pull_secret_type_and_hardcoded_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _write_inputs(tmp_path)
    monkeypatch.setattr(
        deployment_preflight, "_manifest_reachability_check", _reachable_manifest_check
    )
    secret_path = tmp_path / "secrets.yaml"
    contents = secret_path.read_text(encoding="utf-8")
    contents = contents.replace(
        "metadata: {name: trpc-service-secrets}",
        "metadata: {name: trpc-service-secrets, namespace: default}",
    ).replace("type: kubernetes.io/dockerconfigjson", "type: Opaque")
    secret_path.write_text(contents, encoding="utf-8")

    report, projected = build_preflight(config_path, environment=source)

    check = next(item for item in report["checks"] if item["name"] == "secret_manifest_contract")
    assert report["gate"] == "fail"
    assert projected is None
    assert check["hardcoded_namespace_names"] == ["trpc-service-secrets"]
    assert check["image_pull_secret_type_valid"] is False


def test_preflight_rejects_kubeconfig_hardlink_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _write_inputs(tmp_path)
    monkeypatch.setattr(
        deployment_preflight, "_manifest_reachability_check", _reachable_manifest_check
    )
    driver_kubeconfig = tmp_path / "driver.kubeconfig"
    driver_kubeconfig.unlink()
    os.link(tmp_path / "admin.kubeconfig", driver_kubeconfig)

    report, projected = build_preflight(config_path, environment=source)

    check = next(item for item in report["checks"] if item["name"] == "hpa_kubeconfig_separation")
    assert report["gate"] == "fail"
    assert projected is None
    assert check["distinct_paths"] is True
    assert check["distinct_bytes"] is False
    assert check["distinct_inodes"] is False


def test_preflight_returns_structured_failure_for_invalid_document(tmp_path: Path) -> None:
    config_path = tmp_path / "runtime-gate.yaml"
    config_path.write_text("schema_version: [", encoding="utf-8")

    report, projected = build_preflight(config_path, environment={})

    assert report["gate"] == "fail"
    assert projected is None
    assert report["checks"] == [
        {
            "name": "configuration",
            "status": "fail",
            "reason": "runtime gate config is not readable YAML",
        }
    ]


def test_production_model_secret_is_scoped_to_worker_and_template_is_runnable() -> None:
    patch = yaml.safe_load(
        (ROOT / "deploy/kustomize/overlays/production/model-secret-mount-patch.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert patch["metadata"]["name"] == "trpc-worker"
    pod = patch["spec"]["template"]["spec"]
    assert pod["containers"] == [
        {
            "name": "worker",
            "volumeMounts": [
                {
                    "name": "model-secrets",
                    "mountPath": "/run/secrets/model_api_key",
                    "subPath": "model_api_key",
                    "readOnly": True,
                }
            ],
        }
    ]
    assert pod["volumes"] == [
        {
            "name": "model-secrets",
            "secret": {
                "secretName": "trpc-model-secrets",
                "defaultMode": 0o440,
            },
        }
    ]

    secrets = list(
        yaml.safe_load_all(
            (ROOT / "deploy/kustomize/base/secrets.example.yaml").read_text(encoding="utf-8")
        )
    )
    model_secret = next(
        item for item in secrets if item.get("metadata", {}).get("name") == "trpc-model-secrets"
    )
    assert model_secret["stringData"] == {"model_api_key": "REPLACE_WITH_MODEL_PROVIDER_API_KEY"}

    kustomization = yaml.safe_load(
        (ROOT / "deploy/kustomize/overlays/production/kustomization.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert "model-secret-mount-patch.yaml" in {item["path"] for item in kustomization["patches"]}

    request = json.loads((ROOT / "deploy/tenant-config.example.json").read_text(encoding="utf-8"))
    config = TenantConfig.model_validate(
        {
            **request["config"],
            "tenant_id": "tenant-example",
            "app_id": request["app_id"],
            "version": 1,
        }
    )
    assert config.model.provider == "openai"
    assert config.model.model == "REPLACE_WITH_APPROVED_MODEL_ID"
    assert config.model.base_url == "https://api.openai.com/v1"
    assert config.model.reasoning_effort == "medium"
    assert config.model.api_key_ref is not None
    assert config.model.api_key_ref.uri == "file:///run/secrets/model_api_key"
