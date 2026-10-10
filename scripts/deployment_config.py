#!/usr/bin/env python3
"""Strict, secret-safe configuration for the production runtime gate.

The file intentionally contains only references to credentials.  Raw release
nonces, kubeconfig contents, registry tokens, DSNs, and Secret values remain in
environment variables or external files.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from scripts.evidence_lineage import source_fingerprint

ROOT = Path(__file__).resolve().parents[1]
IMAGE_REFERENCE_RE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")
REGISTRY_HOST_RE = re.compile(
    r"^(?:localhost|[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?)(?::[1-9][0-9]{0,4})?$"
)
KUBERNETES_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
NODE_LABEL_RE = re.compile(r"^trpc-runtime-gate=[A-Za-z0-9_.-]+$")
PERFORMANCE_LABEL_RE = re.compile(
    r"^[A-Za-z0-9](?:[-A-Za-z0-9./_]*[A-Za-z0-9])?=[A-Za-z0-9](?:[-A-Za-z0-9._]*[A-Za-z0-9])?$"
)
SERVICE_DNS_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?){3,}$"
)
RESOURCE_QUANTITY_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?(?:m|Ki|Mi|Gi|Ti|Pi|Ei)?$")
S3_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
SUBJECT_RE = re.compile(r"^system:serviceaccount:[a-z0-9.-]+:[a-z0-9.-]+$")
HPA_JOB_COMMAND_MAX_ARGS = 64
HPA_JOB_COMMAND_MAX_ITEM_BYTES = 512
HPA_DRIVER_MAX_BYTES = 1024 * 1024
PERFORMANCE_MAX_INFLIGHT = 64
PERFORMANCE_DB_POOL_SIZE = 32
PERFORMANCE_WORKERS = 4
PERFORMANCE_WORKER_CONCURRENCY = 50
PERFORMANCE_GATEWAY_REPLICAS = 4
PERFORMANCE_GATEWAY_POOL_MAX_SIZE = 24
PERFORMANCE_WORKER_POOL_MAX_SIZE = 8
PERFORMANCE_OUTBOX_POOL_MAX_SIZE = 4
PERFORMANCE_RECOVERY_POOL_MAX_SIZE = 2
PERFORMANCE_PROBE_CONNECTION_HEADROOM = 3
PERFORMANCE_MAX_RUNTIME_CONNECTIONS = 97
PERFORMANCE_FIXTURE_SECRET_ENV_NAMES = (
    "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
    "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
    "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
)
PERFORMANCE_SECRET_ENV_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
PORT_ENVIRONMENTS = {
    "gateway": "GATEWAY_PORT",
    "admin": "ADMIN_PORT",
    "postgres": "POSTGRES_PORT",
    "redis": "REDIS_PORT",
    "minio": "MINIO_PORT",
    "minio_console": "MINIO_CONSOLE_PORT",
    "toxiproxy_api": "TOXIPROXY_API_PORT",
    "toxiproxy_postgres": "TOXIPROXY_POSTGRES_PORT",
    "toxiproxy_redis": "TOXIPROXY_REDIS_PORT",
    "toxiproxy_s3": "TOXIPROXY_S3_PORT",
    "prometheus": "PROMETHEUS_PORT",
    "jaeger": "JAEGER_UI_PORT",
}
REQUIRED_SECRET_KEYS = {
    "trpc-service-secrets": {
        "TRPC_SERVICE_DATABASE_DSN",
        "TRPC_SERVICE_REDIS_URL",
        "TRPC_SERVICE_SESSION_HMAC_KEY",
        "TRPC_SERVICE_EMERGENCY_QUEUE_KEY",
        "TRPC_SERVICE_S3_ACCESS_KEY",
        "TRPC_SERVICE_S3_SECRET_KEY",
    },
    "trpc-worker-secrets": {
        "TRPC_SERVICE_WORKER_DATABASE_DSN_REF",
        "TRPC_SERVICE_WORKER_DATABASE_DSN",
        "TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF",
        "TRPC_SERVICE_WORKER_DATABASE_PASSWORD",
    },
    "trpc-migration-secrets": {"TRPC_SERVICE_DATABASE_DSN"},
}
REAL_HPA_BACKLOG_SECRET_KEYS = {
    "trpc-metrics-secrets": {"TRPC_SERVICE_METRICS_DATABASE_DSN"},
}


class DeploymentConfigError(ValueError):
    """Raised when the unified runtime configuration is invalid."""


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise DeploymentConfigError("configuration keys must be strings")
        if key in result:
            raise DeploymentConfigError(f"duplicate configuration key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _mapping(
    value: object,
    *,
    path: str,
    allowed: set[str],
    required: set[str] | frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DeploymentConfigError(f"{path} must be a mapping")
    keys = set(value)
    unknown = sorted(keys - allowed)
    missing = sorted(set(required) - keys)
    if unknown:
        raise DeploymentConfigError(f"{path} has unknown fields: {', '.join(unknown)}")
    if missing:
        raise DeploymentConfigError(f"{path} is missing fields: {', '.join(missing)}")
    return value


@dataclass(frozen=True)
class RuntimeSupportConfig:
    """Validated ACK support-service recovery settings."""

    data_node: str
    postgres_image: str
    redis_image: str
    minio_image: str
    minio_client_image: str
    prometheus_image: str
    prometheus_adapter_image: str
    postgres_host_path: str
    redis_host_path: str
    minio_host_path: str

    @classmethod
    def from_mapping(cls, value: object, *, pull_registry: str) -> RuntimeSupportConfig:
        """Validate the optional ``kubernetes.support`` mapping."""

        support = _mapping(
            value,
            path="kubernetes.support",
            allowed={
                "data_node",
                "postgres_image",
                "redis_image",
                "minio_image",
                "minio_client_image",
                "prometheus_image",
                "prometheus_adapter_image",
                "postgres_host_path",
                "redis_host_path",
                "minio_host_path",
            },
            required={
                "data_node",
                "postgres_image",
                "redis_image",
                "minio_image",
                "minio_client_image",
                "prometheus_image",
                "prometheus_adapter_image",
                "postgres_host_path",
                "redis_host_path",
                "minio_host_path",
            },
        )
        support_images = {
            name: _validate_pull_registry_image(
                _string(support[name], path=f"kubernetes.support.{name}"),
                pull_registry=pull_registry,
                path=f"kubernetes.support.{name}",
            )
            for name in (
                "postgres_image",
                "redis_image",
                "minio_image",
                "minio_client_image",
                "prometheus_image",
                "prometheus_adapter_image",
            )
        }
        return cls(
            data_node=_string(support["data_node"], path="kubernetes.support.data_node"),
            postgres_image=support_images["postgres_image"],
            redis_image=support_images["redis_image"],
            minio_image=support_images["minio_image"],
            minio_client_image=support_images["minio_client_image"],
            prometheus_image=support_images["prometheus_image"],
            prometheus_adapter_image=support_images["prometheus_adapter_image"],
            postgres_host_path=_string(
                support["postgres_host_path"], path="kubernetes.support.postgres_host_path"
            ),
            redis_host_path=_string(
                support["redis_host_path"], path="kubernetes.support.redis_host_path"
            ),
            minio_host_path=_string(
                support["minio_host_path"], path="kubernetes.support.minio_host_path"
            ),
        )

    def environment(self) -> dict[str, str]:
        """Project support settings to their explicit runtime environment names."""

        return {
            "TRPC_K8S_SUPPORT_DATA_NODE": self.data_node,
            "TRPC_K8S_SUPPORT_POSTGRES_IMAGE": self.postgres_image,
            "TRPC_K8S_SUPPORT_REDIS_IMAGE": self.redis_image,
            "TRPC_K8S_SUPPORT_MINIO_IMAGE": self.minio_image,
            "TRPC_K8S_SUPPORT_MINIO_CLIENT_IMAGE": self.minio_client_image,
            "TRPC_K8S_SUPPORT_PROMETHEUS_IMAGE": self.prometheus_image,
            "TRPC_K8S_SUPPORT_PROMETHEUS_ADAPTER_IMAGE": self.prometheus_adapter_image,
            "TRPC_K8S_SUPPORT_POSTGRES_HOST_PATH": self.postgres_host_path,
            "TRPC_K8S_SUPPORT_REDIS_HOST_PATH": self.redis_host_path,
            "TRPC_K8S_SUPPORT_MINIO_HOST_PATH": self.minio_host_path,
        }


@dataclass(frozen=True)
class PerformanceRunnerResources:
    """Resource requests and limits for the opt-in performance Job."""

    request_cpu: str
    request_memory: str
    limit_cpu: str
    limit_memory: str


@dataclass(frozen=True)
class PerformanceDatabasePoolConfig:
    """A bounded PostgreSQL pool for one performance workload role."""

    min_size: int
    max_size: int


@dataclass(frozen=True)
class PerformanceGatewayConfig:
    """Gateway replica and database-pool settings for the performance run."""

    replicas: int
    database_pool: PerformanceDatabasePoolConfig

    @property
    def database_pool_min_size(self) -> int:
        return self.database_pool.min_size

    @property
    def database_pool_max_size(self) -> int:
        return self.database_pool.max_size


@dataclass(frozen=True)
class PerformanceWorkerConfig:
    """Worker database-pool and deterministic offline-agent settings."""

    database_pool: PerformanceDatabasePoolConfig
    offline_agent_delay_seconds: float

    @property
    def database_pool_min_size(self) -> int:
        return self.database_pool.min_size

    @property
    def database_pool_max_size(self) -> int:
        return self.database_pool.max_size


@dataclass(frozen=True)
class PerformanceOutboxConfig:
    """Outbox dispatcher database-pool settings."""

    replicas: int
    database_pool: PerformanceDatabasePoolConfig

    @property
    def database_pool_min_size(self) -> int:
        return self.database_pool.min_size

    @property
    def database_pool_max_size(self) -> int:
        return self.database_pool.max_size


@dataclass(frozen=True)
class PerformanceRecoveryConfig:
    """Session-recovery pool and polling settings."""

    replicas: int
    database_pool: PerformanceDatabasePoolConfig
    poll_seconds: float

    @property
    def database_pool_min_size(self) -> int:
        return self.database_pool.min_size

    @property
    def database_pool_max_size(self) -> int:
        return self.database_pool.max_size


@dataclass(frozen=True)
class PerformanceWorkloadConfig:
    """Explicit placement and role settings for the ACK performance workload."""

    node_label: str
    gateway: PerformanceGatewayConfig
    worker: PerformanceWorkerConfig
    outbox: PerformanceOutboxConfig
    recovery: PerformanceRecoveryConfig
    worker_count: int
    runner_db_pool_size: int

    @property
    def node_selector(self) -> dict[str, str]:
        """Return the workload node selector in Kubernetes map form."""

        key, value = self.node_label.split("=", 1)
        return {key: value}

    @property
    def estimated_runtime_connections(self) -> int:
        """Estimate all fixed role and probe PostgreSQL connections."""

        return (
            self.gateway.replicas * self.gateway.database_pool.max_size
            + self.worker.database_pool.max_size * self.worker_count
            + self.outbox.replicas * self.outbox.database_pool.max_size
            + self.recovery.replicas * self.recovery.database_pool.max_size
            + self.runner_db_pool_size
            + PERFORMANCE_PROBE_CONNECTION_HEADROOM
        )

    def environment(self) -> dict[str, str]:
        """Project workload controls to non-secret ConfigMap values."""

        return {
            "TRPC_PERF_K8S_WORKLOAD_NODE_LABEL": self.node_label,
            "TRPC_PERF_K8S_GATEWAY_REPLICAS": str(self.gateway.replicas),
            "TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MIN_SIZE": str(
                self.gateway.database_pool.min_size
            ),
            "TRPC_PERF_K8S_GATEWAY_DATABASE_POOL_MAX_SIZE": str(
                self.gateway.database_pool.max_size
            ),
            "TRPC_PERF_K8S_WORKER_DATABASE_POOL_MIN_SIZE": str(self.worker.database_pool.min_size),
            "TRPC_PERF_K8S_WORKER_DATABASE_POOL_MAX_SIZE": str(self.worker.database_pool.max_size),
            "TRPC_PERF_K8S_WORKER_OFFLINE_AGENT_DELAY_SECONDS": str(
                self.worker.offline_agent_delay_seconds
            ),
            "TRPC_PERF_K8S_OUTBOX_DATABASE_POOL_MIN_SIZE": str(self.outbox.database_pool.min_size),
            "TRPC_PERF_K8S_OUTBOX_DATABASE_POOL_MAX_SIZE": str(self.outbox.database_pool.max_size),
            "TRPC_PERF_K8S_OUTBOX_REPLICAS": str(self.outbox.replicas),
            "TRPC_PERF_K8S_RECOVERY_DATABASE_POOL_MIN_SIZE": str(
                self.recovery.database_pool.min_size
            ),
            "TRPC_PERF_K8S_RECOVERY_DATABASE_POOL_MAX_SIZE": str(
                self.recovery.database_pool.max_size
            ),
            "TRPC_PERF_K8S_RECOVERY_REPLICAS": str(self.recovery.replicas),
            "TRPC_PERF_K8S_RECOVERY_POLL_SECONDS": (
                str(int(self.recovery.poll_seconds))
                if self.recovery.poll_seconds.is_integer()
                else str(self.recovery.poll_seconds)
            ),
            "TRPC_PERF_K8S_ESTIMATED_RUNTIME_CONNECTIONS": str(self.estimated_runtime_connections),
        }


@dataclass(frozen=True)
class PerformanceRunnerConfig:
    """Explicit in-cluster performance topology settings.

    This block is deliberately separate from the ordinary runtime settings.
    It is consumed only by an explicitly enabled performance acceptance and
    never changes the base or production workload defaults.
    """

    enabled: bool
    namespace: str
    gateway_service: str
    postgres_service: str
    redis_service: str
    gateway_port: int
    postgres_port: int
    redis_port: int
    node_label: str
    taint_key: str
    taint_value: str
    taint_effect: str
    resources: PerformanceRunnerResources
    max_inflight: int
    db_pool_size: int
    workers: int
    worker_concurrency: int
    workload: PerformanceWorkloadConfig
    fixture_secret_env_names: tuple[str, ...]

    @property
    def node_selector(self) -> dict[str, str]:
        """Return the load-driver node selector in Kubernetes map form."""

        key, value = self.node_label.split("=", 1)
        return {key: value}

    @property
    def gateway_url(self) -> str:
        """Return the in-cluster gateway origin used by the runner."""

        return f"http://{self.gateway_service}:{self.gateway_port}"

    def environment(self) -> dict[str, str]:
        """Project only non-secret performance settings to environment names."""

        values = {
            "TRPC_PERF_K8S_ENABLED": str(self.enabled).lower(),
            "TRPC_PERF_K8S_NAMESPACE": self.namespace,
            "TRPC_PERF_K8S_GATEWAY_SERVICE": self.gateway_service,
            "TRPC_PERF_K8S_POSTGRES_SERVICE": self.postgres_service,
            "TRPC_PERF_K8S_REDIS_SERVICE": self.redis_service,
            "TRPC_PERF_K8S_GATEWAY_PORT": str(self.gateway_port),
            "TRPC_PERF_K8S_POSTGRES_PORT": str(self.postgres_port),
            "TRPC_PERF_K8S_REDIS_PORT": str(self.redis_port),
            "TRPC_PERF_K8S_GATEWAY_URL": self.gateway_url,
            "TRPC_PERF_K8S_RUNNER_NODE_LABEL": self.node_label,
            "TRPC_PERF_K8S_RUNNER_TAINT_KEY": self.taint_key,
            "TRPC_PERF_K8S_RUNNER_TAINT_VALUE": self.taint_value,
            "TRPC_PERF_K8S_RUNNER_TAINT_EFFECT": self.taint_effect,
            "TRPC_PERF_K8S_RUNNER_CPU_REQUEST": self.resources.request_cpu,
            "TRPC_PERF_K8S_RUNNER_MEMORY_REQUEST": self.resources.request_memory,
            "TRPC_PERF_K8S_RUNNER_CPU_LIMIT": self.resources.limit_cpu,
            "TRPC_PERF_K8S_RUNNER_MEMORY_LIMIT": self.resources.limit_memory,
            "TRPC_PERF_K8S_MAX_INFLIGHT": str(self.max_inflight),
            "TRPC_PERF_K8S_DB_POOL_SIZE": str(self.db_pool_size),
            "TRPC_PERF_K8S_WORKERS": str(self.workers),
            "TRPC_PERF_K8S_WORKER_CONCURRENCY": str(self.worker_concurrency),
            "TRPC_SERVICE_TENANT_SECRET_ENV_NAMES": json.dumps(
                self.fixture_secret_env_names, separators=(",", ":")
            ),
        }
        values.update(self.workload.environment())
        # Keep the load-driver vocabulary available to the runner while the
        # canonical config field remains named ``runner``.
        values.update(
            {
                "TRPC_PERF_K8S_LOAD_DRIVER_NODE_LABEL": self.node_label,
                "TRPC_PERF_K8S_LOAD_DRIVER_TAINT_KEY": self.taint_key,
                "TRPC_PERF_K8S_LOAD_DRIVER_TAINT_VALUE": self.taint_value,
                "TRPC_PERF_K8S_LOAD_DRIVER_TAINT_EFFECT": self.taint_effect,
                "TRPC_PERF_K8S_LOAD_DRIVER_CPU_REQUEST": self.resources.request_cpu,
                "TRPC_PERF_K8S_LOAD_DRIVER_MEMORY_REQUEST": self.resources.request_memory,
                "TRPC_PERF_K8S_LOAD_DRIVER_CPU_LIMIT": self.resources.limit_cpu,
                "TRPC_PERF_K8S_LOAD_DRIVER_MEMORY_LIMIT": self.resources.limit_memory,
            }
        )
        return values


def _string(value: object, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError(f"{path} must be a non-empty string")
    return value.strip()


def _resolved_path(value: object, *, path: str, base: Path) -> Path:
    raw = _string(value, path=path)
    candidate = Path(os.path.expandvars(raw)).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


def _timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DeploymentConfigError("kubernetes.timeout_seconds must be numeric")
    timeout = float(value)
    if not 0 < timeout <= 3600:
        raise DeploymentConfigError("kubernetes.timeout_seconds must be within 0..3600")
    return timeout


def _port(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1024 <= value <= 65535:
        raise DeploymentConfigError(f"compose.ports.{name} must be an integer within 1024..65535")
    return value


def _performance_port(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise DeploymentConfigError(
            f"kubernetes.performance.{name} must be an integer within 1..65535"
        )
    return value


def _strict_bool(value: object, *, path: str) -> bool:
    if not isinstance(value, bool):
        raise DeploymentConfigError(f"{path} must be a boolean")
    return value


def _performance_quantity(value: object, *, path: str) -> str:
    quantity = _string(value, path=path)
    if RESOURCE_QUANTITY_RE.fullmatch(quantity) is None:
        raise DeploymentConfigError(f"{path} must be a Kubernetes CPU or memory quantity")
    return quantity


def _performance_label(value: object, *, path: str) -> str:
    label = _string(value, path=path)
    if PERFORMANCE_LABEL_RE.fullmatch(label) is None:
        raise DeploymentConfigError(f"{path} must be a key=value node label")
    key, label_value = label.split("=", 1)
    if key.startswith("/") or key.endswith("/") or ("/" in key and key.count("/") > 1):
        raise DeploymentConfigError(f"{path} must contain a valid label key")
    if not label_value:
        raise DeploymentConfigError(f"{path} must contain a non-empty label value")
    return label


def _performance_float(value: object, *, path: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DeploymentConfigError(f"{path} must be numeric")
    number = float(value)
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise DeploymentConfigError(f"{path} must be within {minimum}..{maximum}")
    return number


def _performance_pool(value: object, *, path: str) -> PerformanceDatabasePoolConfig:
    pool = _mapping(
        value,
        path=path,
        allowed={"min_size", "max_size"},
        required={"min_size", "max_size"},
    )
    min_size = _performance_int(pool["min_size"], path=f"{path}.min_size", minimum=1)
    max_size = _performance_int(pool["max_size"], path=f"{path}.max_size", minimum=1)
    if max_size < min_size:
        raise DeploymentConfigError(f"{path}.max_size must be >= {path}.min_size")
    return PerformanceDatabasePoolConfig(min_size=min_size, max_size=max_size)


def _performance_fixture_names(value: object, *, path: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise DeploymentConfigError(f"{path} must be a non-empty list of environment names")
    names = tuple(_string(item, path=f"{path}[{index}]") for index, item in enumerate(value))
    if any(PERFORMANCE_SECRET_ENV_RE.fullmatch(name) is None for name in names):
        raise DeploymentConfigError(f"{path} contains an invalid environment name")
    if len(names) != len(set(names)):
        raise DeploymentConfigError(f"{path} must not contain duplicate environment names")
    if names != PERFORMANCE_FIXTURE_SECRET_ENV_NAMES:
        raise DeploymentConfigError(f"{path} must contain the three synthetic fixture secret names")
    return names


def _service_dns(value: object, *, path: str) -> str:
    hostname = _string(value, path=path).rstrip(".")
    if SERVICE_DNS_RE.fullmatch(hostname) is None:
        raise DeploymentConfigError(f"{path} must be a fully-qualified Kubernetes Service DNS name")
    return hostname


def _performance_runner(value: object) -> PerformanceRunnerConfig:
    performance = _mapping(
        value,
        path="kubernetes.performance",
        allowed={
            "enabled",
            "namespace",
            "service_dns",
            "gateway_port",
            "postgres_port",
            "redis_port",
            "runner",
            "workload",
            "fixture_secret_env_names",
            "workers",
            "worker_concurrency",
        },
        required={
            "enabled",
            "namespace",
            "service_dns",
            "gateway_port",
            "postgres_port",
            "redis_port",
            "runner",
            "workload",
            "fixture_secret_env_names",
            "workers",
            "worker_concurrency",
        },
    )
    enabled = _strict_bool(performance["enabled"], path="kubernetes.performance.enabled")
    namespace = _string(performance["namespace"], path="kubernetes.performance.namespace")
    if KUBERNETES_NAME_RE.fullmatch(namespace) is None:
        raise DeploymentConfigError("kubernetes.performance.namespace is invalid")

    service_dns = _mapping(
        performance["service_dns"],
        path="kubernetes.performance.service_dns",
        allowed={"gateway", "postgres", "redis"},
        required={"gateway", "postgres", "redis"},
    )
    gateway_service = _service_dns(
        service_dns["gateway"], path="kubernetes.performance.service_dns.gateway"
    )
    postgres_service = _service_dns(
        service_dns["postgres"], path="kubernetes.performance.service_dns.postgres"
    )
    redis_service = _service_dns(
        service_dns["redis"], path="kubernetes.performance.service_dns.redis"
    )
    gateway_port = _performance_port(performance["gateway_port"], name="gateway_port")
    postgres_port = _performance_port(performance["postgres_port"], name="postgres_port")
    redis_port = _performance_port(performance["redis_port"], name="redis_port")

    runner = _mapping(
        performance["runner"],
        path="kubernetes.performance.runner",
        allowed={"node_label", "taint", "resources", "max_inflight", "db_pool_size"},
        required={"node_label", "taint", "resources", "max_inflight", "db_pool_size"},
    )
    node_label = _performance_label(
        runner["node_label"], path="kubernetes.performance.runner.node_label"
    )
    taint = _mapping(
        runner["taint"],
        path="kubernetes.performance.runner.taint",
        allowed={"key", "value", "effect"},
        required={"key", "value", "effect"},
    )
    taint_key = _string(taint["key"], path="kubernetes.performance.runner.taint.key")
    taint_value = _string(taint["value"], path="kubernetes.performance.runner.taint.value")
    taint_effect = _string(taint["effect"], path="kubernetes.performance.runner.taint.effect")
    if taint_effect not in {"NoSchedule", "PreferNoSchedule", "NoExecute"}:
        raise DeploymentConfigError("kubernetes.performance.runner.taint.effect is invalid")
    resources = _mapping(
        runner["resources"],
        path="kubernetes.performance.runner.resources",
        allowed={"requests", "limits"},
        required={"requests", "limits"},
    )

    def resource_pair(name: str) -> tuple[str, str]:
        block = _mapping(
            resources[name],
            path=f"kubernetes.performance.runner.resources.{name}",
            allowed={"cpu", "memory"},
            required={"cpu", "memory"},
        )
        return (
            _performance_quantity(
                block["cpu"],
                path=f"kubernetes.performance.runner.resources.{name}.cpu",
            ),
            _performance_quantity(
                block["memory"],
                path=f"kubernetes.performance.runner.resources.{name}.memory",
            ),
        )

    request_cpu, request_memory = resource_pair("requests")
    limit_cpu, limit_memory = resource_pair("limits")
    max_inflight = _performance_int(
        runner["max_inflight"], path="kubernetes.performance.runner.max_inflight", minimum=1
    )
    db_pool_size = _performance_int(
        runner["db_pool_size"], path="kubernetes.performance.runner.db_pool_size", minimum=1
    )
    workers = _performance_int(
        performance["workers"], path="kubernetes.performance.workers", minimum=1
    )
    worker_concurrency = _performance_int(
        performance["worker_concurrency"],
        path="kubernetes.performance.worker_concurrency",
        minimum=1,
    )
    workload = _mapping(
        performance["workload"],
        path="kubernetes.performance.workload",
        allowed={"node_label", "gateway", "worker", "outbox", "recovery"},
        required={"node_label", "gateway", "worker", "outbox", "recovery"},
    )
    workload_node_label = _performance_label(
        workload["node_label"], path="kubernetes.performance.workload.node_label"
    )
    gateway = _mapping(
        workload["gateway"],
        path="kubernetes.performance.workload.gateway",
        allowed={"replicas", "database_pool"},
        required={"replicas", "database_pool"},
    )
    gateway_replicas = _performance_int(
        gateway["replicas"],
        path="kubernetes.performance.workload.gateway.replicas",
        minimum=1,
    )
    worker = _mapping(
        workload["worker"],
        path="kubernetes.performance.workload.worker",
        allowed={"database_pool", "offline_agent_delay_seconds"},
        required={"database_pool", "offline_agent_delay_seconds"},
    )
    outbox = _mapping(
        workload["outbox"],
        path="kubernetes.performance.workload.outbox",
        allowed={"replicas", "database_pool"},
        required={"replicas", "database_pool"},
    )
    recovery = _mapping(
        workload["recovery"],
        path="kubernetes.performance.workload.recovery",
        allowed={"replicas", "database_pool", "poll_seconds"},
        required={"replicas", "database_pool", "poll_seconds"},
    )
    outbox_replicas = _performance_int(
        outbox["replicas"],
        path="kubernetes.performance.workload.outbox.replicas",
        minimum=1,
    )
    recovery_replicas = _performance_int(
        recovery["replicas"],
        path="kubernetes.performance.workload.recovery.replicas",
        minimum=1,
    )
    workload_config = PerformanceWorkloadConfig(
        node_label=workload_node_label,
        gateway=PerformanceGatewayConfig(
            replicas=gateway_replicas,
            database_pool=_performance_pool(
                gateway["database_pool"],
                path="kubernetes.performance.workload.gateway.database_pool",
            ),
        ),
        worker=PerformanceWorkerConfig(
            database_pool=_performance_pool(
                worker["database_pool"],
                path="kubernetes.performance.workload.worker.database_pool",
            ),
            offline_agent_delay_seconds=_performance_float(
                worker["offline_agent_delay_seconds"],
                path="kubernetes.performance.workload.worker.offline_agent_delay_seconds",
                minimum=0.000001,
                maximum=5.0,
            ),
        ),
        outbox=PerformanceOutboxConfig(
            replicas=outbox_replicas,
            database_pool=_performance_pool(
                outbox["database_pool"],
                path="kubernetes.performance.workload.outbox.database_pool",
            ),
        ),
        recovery=PerformanceRecoveryConfig(
            replicas=recovery_replicas,
            database_pool=_performance_pool(
                recovery["database_pool"],
                path="kubernetes.performance.workload.recovery.database_pool",
            ),
            poll_seconds=_performance_float(
                recovery["poll_seconds"],
                path="kubernetes.performance.workload.recovery.poll_seconds",
                minimum=0.1,
                maximum=300.0,
            ),
        ),
        worker_count=workers,
        runner_db_pool_size=db_pool_size,
    )
    fixture_secret_env_names = _performance_fixture_names(
        performance["fixture_secret_env_names"],
        path="kubernetes.performance.fixture_secret_env_names",
    )
    if enabled:
        expected = (
            ("max_inflight", max_inflight, PERFORMANCE_MAX_INFLIGHT),
            ("db_pool_size", db_pool_size, PERFORMANCE_DB_POOL_SIZE),
            ("workers", workers, PERFORMANCE_WORKERS),
            ("worker_concurrency", worker_concurrency, PERFORMANCE_WORKER_CONCURRENCY),
        )
        for name, actual, required in expected:
            if actual != required:
                raise DeploymentConfigError(
                    f"kubernetes.performance.{name} must be {required} when performance is enabled"
                )
        if workload_config.gateway.replicas != PERFORMANCE_GATEWAY_REPLICAS:
            raise DeploymentConfigError(
                "kubernetes.performance.workload.gateway.replicas must be "
                f"{PERFORMANCE_GATEWAY_REPLICAS} when performance is enabled"
            )
        expected_workload = (
            workload_config.node_label == "trpc-role=workload"
            and workload_config.gateway.database_pool == PerformanceDatabasePoolConfig(5, 6)
            and workload_config.worker.database_pool == PerformanceDatabasePoolConfig(2, 8)
            and workload_config.worker.offline_agent_delay_seconds == 3.0
            and workload_config.outbox.replicas == 1
            and workload_config.outbox.database_pool == PerformanceDatabasePoolConfig(2, 4)
            and workload_config.recovery.replicas == 1
            and workload_config.recovery.database_pool == PerformanceDatabasePoolConfig(1, 2)
            and workload_config.recovery.poll_seconds == 1.0
        )
        if not expected_workload:
            raise DeploymentConfigError(
                "kubernetes.performance.workload contains values outside the locked "
                "performance topology"
            )
    if db_pool_size > max_inflight:
        raise DeploymentConfigError(
            "kubernetes.performance.runner.db_pool_size must not exceed max_inflight"
        )
    if workload_config.estimated_runtime_connections > PERFORMANCE_MAX_RUNTIME_CONNECTIONS:
        raise DeploymentConfigError(
            "kubernetes.performance.workload connection total exceeds the "
            f"{PERFORMANCE_MAX_RUNTIME_CONNECTIONS}-connection budget"
        )
    return PerformanceRunnerConfig(
        enabled=enabled,
        namespace=namespace,
        gateway_service=gateway_service,
        postgres_service=postgres_service,
        redis_service=redis_service,
        gateway_port=gateway_port,
        postgres_port=postgres_port,
        redis_port=redis_port,
        node_label=node_label,
        taint_key=taint_key,
        taint_value=taint_value,
        taint_effect=taint_effect,
        resources=PerformanceRunnerResources(
            request_cpu=request_cpu,
            request_memory=request_memory,
            limit_cpu=limit_cpu,
            limit_memory=limit_memory,
        ),
        max_inflight=max_inflight,
        db_pool_size=db_pool_size,
        workers=workers,
        worker_concurrency=worker_concurrency,
        workload=workload_config,
        fixture_secret_env_names=fixture_secret_env_names,
    )


def _performance_int(value: object, *, path: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise DeploymentConfigError(f"{path} must be an integer >= {minimum}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _image_references(binding_path: Path) -> tuple[str, str, Mapping[str, Any]]:
    try:
        binding = json.loads(binding_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DeploymentConfigError("release.image_binding must be readable JSON") from error
    if not isinstance(binding, Mapping):
        raise DeploymentConfigError("release.image_binding must contain a JSON object")
    images = binding.get("images")
    if not isinstance(images, Mapping):
        raise DeploymentConfigError("release.image_binding has no images mapping")
    references: list[str] = []
    for name in ("initial", "upgrade"):
        image = images.get(name)
        reference = image.get("reference") if isinstance(image, Mapping) else None
        if (
            not isinstance(reference, str)
            or IMAGE_REFERENCE_RE.fullmatch(reference) is None
            or "/" not in reference.split("@", 1)[0]
        ):
            raise DeploymentConfigError(
                f"release.image_binding images.{name}.reference must be an "
                "immutable registry reference"
            )
        references.append(reference)
    if references[0] == references[1]:
        raise DeploymentConfigError("initial and upgrade image references must differ")
    return references[0], references[1], binding


def _validate_registry_host(value: str, *, path: str) -> str:
    """Validate a registry host without accepting a repository path or URL."""

    if REGISTRY_HOST_RE.fullmatch(value) is None:
        raise DeploymentConfigError(
            f"{path} must be a registry host without scheme or repository path"
        )
    if value.endswith("."):
        raise DeploymentConfigError(f"{path} must not end with a dot")
    return value


def _validate_pull_registry_image(reference: str, *, pull_registry: str, path: str) -> str:
    """Require an immutable image hosted by the configured ACK pull registry."""

    if IMAGE_REFERENCE_RE.fullmatch(reference) is None:
        raise DeploymentConfigError(f"{path} must be an immutable sha256 reference")
    repository = reference.rsplit("@", 1)[0]
    if "/" not in repository:
        raise DeploymentConfigError(f"{path} must include a registry host and repository")
    registry_host, image_path = repository.split("/", 1)
    if not image_path:
        raise DeploymentConfigError(f"{path} repository path is empty")
    if registry_host != pull_registry:
        raise DeploymentConfigError(f"{path} registry host must match kubernetes.pull_registry")
    return reference


def _object_store_endpoint(value: object) -> str:
    endpoint = _string(value, path="kubernetes.object_store.endpoint").rstrip("/")
    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
    except ValueError as error:
        raise DeploymentConfigError("kubernetes.object_store.endpoint is invalid") from error
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
        raise DeploymentConfigError(
            "kubernetes.object_store.endpoint must be a credential-free HTTP(S) origin"
        )
    return endpoint


def _rewrite_image_registry(reference: str, pull_registry: str | None) -> str:
    """Replace only the registry host and preserve repository path and digest."""

    if IMAGE_REFERENCE_RE.fullmatch(reference) is None:
        raise DeploymentConfigError("image reference must be an immutable sha256 reference")
    repository, digest = reference.rsplit("@", 1)
    if "/" not in repository:
        raise DeploymentConfigError("image reference must include a registry host and repository")
    _, image_path = repository.split("/", 1)
    if not image_path:
        raise DeploymentConfigError("image reference repository path is empty")
    if pull_registry is None:
        return reference
    return f"{pull_registry}/{image_path}@{digest}"


@dataclass(frozen=True)
class RuntimeGateConfig:
    """Validated configuration and its secret-safe environment projection."""

    path: Path
    release_id: str
    nonce_env: str
    image_binding: Path
    kubeconfig: Path
    context: str
    timeout_seconds: float
    secret_manifest: Path
    image_pull_secret: str
    pull_registry: str | None
    node_name: str
    node_label: str
    drain_confirmation: str
    hpa_driver: Path
    hpa_kubeconfig: Path
    hpa_context: str
    hpa_subject: str
    hpa_job_image: str
    hpa_job_command: tuple[str, ...]
    hpa_backlog_metric_enabled: bool
    object_store_endpoint: str | None
    object_store_bucket: str | None
    support: RuntimeSupportConfig | None
    performance: PerformanceRunnerConfig | None
    ports: Mapping[str, int]

    def canonical_image_references(self) -> dict[str, str]:
        """Return the exact immutable references recorded by the release binding."""

        initial, upgrade, _ = _image_references(self.image_binding)
        return {"initial": initial, "upgrade": upgrade}

    def resolved_image_references(self) -> dict[str, str]:
        """Return runtime references after the optional pull-host rewrite."""

        return {
            name: _rewrite_image_registry(reference, self.pull_registry)
            for name, reference in self.canonical_image_references().items()
        }

    def environment(self, source: Mapping[str, str] | None = None) -> dict[str, str]:
        values = os.environ if source is None else source
        nonce = values.get(self.nonce_env, "")
        if RELEASE_NONCE_RE.fullmatch(nonce) is None:
            raise DeploymentConfigError(
                f"release nonce environment {self.nonce_env} is missing or invalid"
            )
        binding = _image_references(self.image_binding)[2]
        resolved_images = self.resolved_image_references()
        release_binding = binding.get("release_binding")
        binding_release = (
            release_binding.get("release_id") if isinstance(release_binding, Mapping) else None
        )
        if binding_release != self.release_id:
            raise DeploymentConfigError("image binding release_id does not match configuration")
        expected_nonce_hash = hashlib.sha256(nonce.encode()).hexdigest()
        binding_nonce_hash = (
            release_binding.get("nonce_sha256") if isinstance(release_binding, Mapping) else None
        )
        if binding_nonce_hash != expected_nonce_hash:
            raise DeploymentConfigError("image binding release nonce does not match configuration")
        binding_source = binding.get("source_fingerprint")
        binding_source_value = (
            binding_source.get("value") if isinstance(binding_source, Mapping) else None
        )
        checkout_source = source_fingerprint(ROOT).get("value")
        if binding_source_value != checkout_source:
            raise DeploymentConfigError("image binding source fingerprint does not match checkout")
        environment = {
            "KUBECONFIG": str(self.kubeconfig),
            "TRPC_RELEASE_ID": self.release_id,
            "TRPC_RELEASE_NONCE": nonce,
            "TRPC_K8S_RUNTIME_TESTS_ENABLED": "true",
            "TRPC_K8S_RUNTIME_CONTEXT": self.context,
            "TRPC_K8S_RUNTIME_TIMEOUT_SECONDS": str(self.timeout_seconds),
            "TRPC_K8S_RUNTIME_IMAGE": resolved_images["initial"],
            "TRPC_K8S_RUNTIME_UPGRADE_IMAGE": resolved_images["upgrade"],
            "TRPC_K8S_RUNTIME_SECRET_MANIFEST": str(self.secret_manifest),
            "TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET": self.image_pull_secret,
            "TRPC_K8S_RUNTIME_HPA_DRIVER": str(self.hpa_driver),
            "TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256": _sha256(self.hpa_driver),
            "TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG": str(self.hpa_kubeconfig),
            "TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT": self.hpa_subject,
            "TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT": self.hpa_context,
            "TRPC_K8S_RUNTIME_HPA_JOB_IMAGE": self.hpa_job_image,
            "TRPC_K8S_RUNTIME_HPA_JOB_COMMAND": json.dumps(
                self.hpa_job_command, separators=(",", ":")
            ),
            "TRPC_K8S_RUNTIME_HPA_BACKLOG_METRIC_ENABLED": str(
                self.hpa_backlog_metric_enabled
            ).lower(),
            "TRPC_K8S_RUNTIME_NODE_NAME": self.node_name,
            "TRPC_K8S_RUNTIME_NODE_LABEL": self.node_label,
            "TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM": self.drain_confirmation,
        }
        environment.update(
            {PORT_ENVIRONMENTS[name]: str(value) for name, value in self.ports.items()}
        )
        if self.object_store_endpoint is not None and self.object_store_bucket is not None:
            environment.update(
                {
                    "TRPC_K8S_RUNTIME_S3_ENDPOINT": self.object_store_endpoint,
                    "TRPC_K8S_RUNTIME_S3_BUCKET": self.object_store_bucket,
                }
            )
        if self.support is not None:
            environment.update(self.support.environment())
        if self.performance is not None:
            environment.update(self.performance.environment())
            initial_image = resolved_images["initial"]
            environment["TRPC_PERF_K8S_IMAGE"] = initial_image
            environment["TRPC_PERF_K8S_IMAGE_DIGEST"] = initial_image.rsplit("@", 1)[1]
        return environment

    @property
    def requires_real_hpa_backlog(self) -> bool:
        """Whether the deployment must provide the real external backlog metric."""

        return self.hpa_backlog_metric_enabled

    def required_secret_keys(self) -> dict[str, set[str]]:
        """Return the secret contract for this deployment mode.

        The metrics Secret is required only by configurations that explicitly
        enable the real HPA backlog path.  Existing runtime-gate configurations
        that leave that capability disabled retain the legacy contract.
        """

        required = {name: set(keys) for name, keys in REQUIRED_SECRET_KEYS.items()}
        if self.requires_real_hpa_backlog:
            required.update(
                {name: set(keys) for name, keys in REAL_HPA_BACKLOG_SECRET_KEYS.items()}
            )
        return required


def load_runtime_gate_config(path: Path) -> RuntimeGateConfig:
    """Load a single strict YAML document without resolving Secret values."""

    resolved = path.expanduser().resolve()
    try:
        raw = yaml.load(resolved.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)  # noqa: S506
    except (OSError, yaml.YAMLError) as error:
        raise DeploymentConfigError("runtime gate config is not readable YAML") from error
    root = _mapping(
        raw,
        path="config",
        allowed={"schema_version", "release", "kubernetes", "compose"},
        required={"schema_version", "release", "kubernetes"},
    )
    if root["schema_version"] != 1:
        raise DeploymentConfigError("schema_version must be 1")
    base = resolved.parent
    release = _mapping(
        root["release"],
        path="release",
        allowed={"id", "nonce_env", "image_binding"},
        required={"nonce_env", "image_binding"},
    )
    image_binding = _resolved_path(
        release["image_binding"], path="release.image_binding", base=base
    )
    if "id" in release:
        release_id = _string(release["id"], path="release.id")
    else:
        binding = _image_references(image_binding)[2]
        release_binding = binding.get("release_binding")
        binding_release_id = (
            release_binding.get("release_id") if isinstance(release_binding, Mapping) else None
        )
        release_id = _string(
            binding_release_id,
            path="release.image_binding.release_binding.release_id",
        )
    if RELEASE_ID_RE.fullmatch(release_id) is None:
        raise DeploymentConfigError("release.id is invalid")
    nonce_env = _string(release["nonce_env"], path="release.nonce_env")
    if re.fullmatch(r"[A-Z][A-Z0-9_]{1,127}", nonce_env) is None:
        raise DeploymentConfigError("release.nonce_env must name an environment variable")
    kubernetes = _mapping(
        root["kubernetes"],
        path="kubernetes",
        allowed={
            "kubeconfig",
            "context",
            "timeout_seconds",
            "secret_manifest",
            "image_pull_secret",
            "pull_registry",
            "object_store",
            "support",
            "performance",
            "node",
            "hpa",
        },
        required={
            "kubeconfig",
            "context",
            "timeout_seconds",
            "secret_manifest",
            "image_pull_secret",
            "pull_registry",
            "node",
            "hpa",
        },
    )
    image_pull_secret = _string(
        kubernetes["image_pull_secret"], path="kubernetes.image_pull_secret"
    )
    if KUBERNETES_NAME_RE.fullmatch(image_pull_secret) is None:
        raise DeploymentConfigError("kubernetes.image_pull_secret is invalid")
    pull_registry = _validate_registry_host(
        _string(kubernetes["pull_registry"], path="kubernetes.pull_registry"),
        path="kubernetes.pull_registry",
    )
    object_store_endpoint: str | None = None
    object_store_bucket: str | None = None
    raw_object_store = kubernetes.get("object_store")
    if raw_object_store is not None:
        object_store = _mapping(
            raw_object_store,
            path="kubernetes.object_store",
            allowed={"endpoint", "bucket"},
            required={"endpoint", "bucket"},
        )
        object_store_endpoint = _object_store_endpoint(object_store["endpoint"])
        object_store_bucket = _string(object_store["bucket"], path="kubernetes.object_store.bucket")
        if S3_BUCKET_RE.fullmatch(object_store_bucket) is None:
            raise DeploymentConfigError("kubernetes.object_store.bucket is invalid")
    support = (
        RuntimeSupportConfig.from_mapping(kubernetes["support"], pull_registry=pull_registry)
        if "support" in kubernetes
        else None
    )
    performance = (
        _performance_runner(kubernetes["performance"]) if "performance" in kubernetes else None
    )
    node = _mapping(
        kubernetes["node"],
        path="kubernetes.node",
        allowed={"name", "label", "drain_confirmation"},
        required={"name", "label", "drain_confirmation"},
    )
    node_name = _string(node["name"], path="kubernetes.node.name")
    node_label = _string(node["label"], path="kubernetes.node.label")
    if (
        KUBERNETES_NAME_RE.fullmatch(node_name) is None
        or NODE_LABEL_RE.fullmatch(node_label) is None
    ):
        raise DeploymentConfigError("kubernetes.node name or label is invalid")
    drain_confirmation = _string(
        node["drain_confirmation"], path="kubernetes.node.drain_confirmation"
    )
    if drain_confirmation != "I_UNDERSTAND_ISOLATED_NODE_DRAIN":
        raise DeploymentConfigError("kubernetes.node.drain_confirmation is invalid")
    hpa = _mapping(
        kubernetes["hpa"],
        path="kubernetes.hpa",
        allowed={
            "driver",
            "kubeconfig",
            "context",
            "subject",
            "job_image",
            "job_command",
            "backlog_metric_enabled",
        },
        required={"driver", "kubeconfig", "context", "subject", "job_image", "job_command"},
    )
    subject = _string(hpa["subject"], path="kubernetes.hpa.subject")
    if SUBJECT_RE.fullmatch(subject) is None:
        raise DeploymentConfigError("kubernetes.hpa.subject is invalid")
    hpa_job_image_value = _string(hpa["job_image"], path="kubernetes.hpa.job_image")
    if hpa_job_image_value == "candidate_initial":
        canonical_initial = _image_references(image_binding)[0]
        hpa_job_image = _rewrite_image_registry(canonical_initial, pull_registry)
    else:
        hpa_job_image = _validate_pull_registry_image(
            hpa_job_image_value,
            pull_registry=pull_registry,
            path="kubernetes.hpa.job_image",
        )
    job_command = hpa["job_command"]
    if (
        not isinstance(job_command, Sequence)
        or isinstance(job_command, (str, bytes))
        or not 1 <= len(job_command) <= HPA_JOB_COMMAND_MAX_ARGS
        or any(
            not isinstance(item, str)
            or not item
            or len(item.encode("utf-8")) > HPA_JOB_COMMAND_MAX_ITEM_BYTES
            or any(character in item for character in ("\x00", "\r", "\n"))
            for item in job_command
        )
    ):
        raise DeploymentConfigError("kubernetes.hpa.job_command must be a bounded argv list")
    hpa_backlog_metric_enabled = _strict_bool(
        hpa.get("backlog_metric_enabled", False),
        path="kubernetes.hpa.backlog_metric_enabled",
    )
    compose = _mapping(
        root.get("compose", {}),
        path="compose",
        allowed={"ports"},
    )
    ports_raw = _mapping(
        compose.get("ports", {}),
        path="compose.ports",
        allowed=set(PORT_ENVIRONMENTS),
    )
    ports = {name: _port(value, name=name) for name, value in ports_raw.items()}
    if len(set(ports.values())) != len(ports):
        raise DeploymentConfigError("compose.ports values must be unique")
    return RuntimeGateConfig(
        path=resolved,
        release_id=release_id,
        nonce_env=nonce_env,
        image_binding=image_binding,
        kubeconfig=_resolved_path(
            kubernetes["kubeconfig"], path="kubernetes.kubeconfig", base=base
        ),
        context=_string(kubernetes["context"], path="kubernetes.context"),
        timeout_seconds=_timeout(kubernetes["timeout_seconds"]),
        secret_manifest=_resolved_path(
            kubernetes["secret_manifest"], path="kubernetes.secret_manifest", base=base
        ),
        image_pull_secret=image_pull_secret,
        pull_registry=pull_registry,
        node_name=node_name,
        node_label=node_label,
        drain_confirmation=drain_confirmation,
        hpa_driver=_resolved_path(hpa["driver"], path="kubernetes.hpa.driver", base=base),
        hpa_kubeconfig=_resolved_path(
            hpa["kubeconfig"], path="kubernetes.hpa.kubeconfig", base=base
        ),
        hpa_context=_string(hpa["context"], path="kubernetes.hpa.context"),
        hpa_subject=subject,
        hpa_job_image=hpa_job_image,
        hpa_job_command=tuple(job_command),
        hpa_backlog_metric_enabled=hpa_backlog_metric_enabled,
        object_store_endpoint=object_store_endpoint,
        object_store_bucket=object_store_bucket,
        support=support,
        performance=performance,
        ports=ports,
    )


def secret_manifest_contract(path: Path) -> dict[str, dict[str, Any]]:
    """Return the Secret metadata contract without exposing any values."""

    try:
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    except (OSError, yaml.YAMLError) as error:
        raise DeploymentConfigError("kubernetes.secret_manifest is not readable YAML") from error
    metadata: dict[str, dict[str, Any]] = {}
    for document in documents:
        if not isinstance(document, Mapping) or document.get("kind") != "Secret":
            raise DeploymentConfigError(
                "kubernetes.secret_manifest may contain only Secret objects"
            )
        resource_metadata = document.get("metadata")
        if not isinstance(resource_metadata, Mapping):
            raise DeploymentConfigError(
                "kubernetes.secret_manifest contains an invalid Secret name"
            )
        name = resource_metadata.get("name")
        if not isinstance(name, str) or KUBERNETES_NAME_RE.fullmatch(name) is None:
            raise DeploymentConfigError(
                "kubernetes.secret_manifest contains an invalid Secret name"
            )
        if name in metadata:
            raise DeploymentConfigError(f"duplicate Secret in manifest: {name}")
        namespace = resource_metadata.get("namespace")
        if namespace is not None and (
            not isinstance(namespace, str) or KUBERNETES_NAME_RE.fullmatch(namespace) is None
        ):
            raise DeploymentConfigError(f"Secret {name} contains an invalid metadata.namespace")
        resource_type = document.get("type", "Opaque")
        if not isinstance(resource_type, str) or not resource_type:
            raise DeploymentConfigError(f"Secret {name} contains an invalid type")
        data = document.get("data", {})
        string_data = document.get("stringData", {})
        if not isinstance(data, Mapping) or not isinstance(string_data, Mapping):
            raise DeploymentConfigError(f"Secret {name} data must be a mapping")
        metadata[name] = {
            "keys": {str(key) for key in data} | {str(key) for key in string_data},
            "namespace": namespace or "",
            "type": resource_type,
        }
    return metadata


def secret_manifest_metadata(path: Path) -> dict[str, set[str]]:
    """Return Secret names and key names without exposing any values."""

    contract = secret_manifest_contract(path)
    return {name: set(entry["keys"]) for name, entry in contract.items()}
