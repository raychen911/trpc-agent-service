from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PRODUCTION = ROOT / "deploy" / "kustomize" / "overlays" / "production"


@pytest.fixture(scope="module")
def resources() -> dict[tuple[str, str], dict[str, Any]]:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required for the Kustomize render contract")
    rendered = subprocess.run(  # noqa: S603 - fixed kubectl binary and arguments
        [kubectl, "kustomize", str(PRODUCTION)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    documents = [item for item in yaml.safe_load_all(rendered) if isinstance(item, dict)]
    return {(item["kind"], item["metadata"]["name"]): item for item in documents}


def test_production_overlay_renders_default_full_stack(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    expected = {
        ("Ingress", "trpc-gateway"),
        ("StatefulSet", "postgres"),
        ("StatefulSet", "redis"),
        ("StatefulSet", "minio"),
        ("StatefulSet", "prometheus"),
        ("Deployment", "otel-collector"),
        ("Deployment", "prometheus-adapter"),
        ("Job", "minio-bootstrap"),
        ("Service", "postgres"),
        ("Service", "redis"),
        ("Service", "minio"),
        ("Service", "prometheus"),
        ("Service", "otel-collector"),
        ("Service", "prometheus-adapter"),
        ("ConfigMap", "trpc-postgres-bootstrap"),
        ("ConfigMap", "trpc-prometheus-config"),
        ("ConfigMap", "trpc-otel-collector-config"),
        ("ConfigMap", "trpc-prometheus-adapter-config"),
        ("APIService", "v1beta1.external.metrics.k8s.io"),
        ("ClusterRole", "trpc-prometheus-adapter-resource-reader"),
        ("ClusterRoleBinding", "trpc-prometheus-adapter-resource-reader"),
    }
    assert expected <= resources.keys()
    assert not any(kind == "Secret" for kind, _name in resources)

    for name in ("postgres", "redis", "minio", "prometheus"):
        stateful_set = resources[("StatefulSet", name)]
        claims = stateful_set["spec"]["volumeClaimTemplates"]
        assert len(claims) == 1
        assert claims[0]["spec"]["accessModes"] == ["ReadWriteOnce"]
        assert (
            stateful_set["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/part-of"]
            == "trpc-agent-service"
        )
    for kind, name in (("Deployment", "otel-collector"), ("Job", "minio-bootstrap")):
        assert (
            resources[(kind, name)]["spec"]["template"]["metadata"]["labels"][
                "app.kubernetes.io/part-of"
            ]
            == "trpc-agent-service"
        )


def test_default_full_stack_uses_local_service_contracts(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    config = resources[("ConfigMap", "trpc-service-config")]["data"]
    assert config["TRPC_SERVICE_S3_ENDPOINT"] == "http://minio:9000"
    assert config["TRPC_SERVICE_S3_BUCKET"] == "trpc-artifacts"
    assert config["TRPC_SERVICE_OTLP_ENDPOINT"] == "http://otel-collector:4317"

    pod_specs = [
        resources[("StatefulSet", name)]["spec"]["template"]["spec"]
        for name in ("postgres", "redis", "minio")
    ]
    pod_specs.append(resources[("Job", "minio-bootstrap")]["spec"]["template"]["spec"])
    env_secret_names = {
        env["valueFrom"]["secretKeyRef"]["name"]
        for pod_spec in pod_specs
        for container in pod_spec.get("initContainers", []) + pod_spec["containers"]
        for env in container.get("env", [])
        if "valueFrom" in env and "secretKeyRef" in env["valueFrom"]
    }
    volume_secret_names = {
        volume["secret"]["secretName"]
        for pod_spec in pod_specs
        for volume in pod_spec.get("volumes", [])
        if "secret" in volume
    }
    assert env_secret_names | volume_secret_names == {"trpc-infrastructure-secrets"}

    expected_keys = {
        "postgres": {
            "postgres_superuser_password",
            "runtime_database_password",
            "migration_database_password",
            "worker_database_password",
            "metrics_database_password",
        },
        "redis": {"redis_password"},
        "minio": {"minio_root_user", "minio_root_password"},
        "minio-bootstrap": {
            "minio_root_user",
            "minio_root_password",
            "minio_application_user",
            "minio_application_password",
        },
    }
    for name, keys in expected_keys.items():
        kind = "Job" if name == "minio-bootstrap" else "StatefulSet"
        pod_spec = resources[(kind, name)]["spec"]["template"]["spec"]
        secret_volume = next(
            volume for volume in pod_spec["volumes"] if volume["name"] == "infrastructure-secrets"
        )
        assert {item["key"] for item in secret_volume["secret"]["items"]} == keys


def test_managed_service_replacement_patch_covers_local_backends() -> None:
    patch_path = PRODUCTION / "managed-services-patch.example.yaml"
    documents = [
        item
        for item in yaml.safe_load_all(patch_path.read_text(encoding="utf-8"))
        if isinstance(item, dict)
    ]
    deleted = {
        (item["kind"], item["metadata"]["name"])
        for item in documents
        if item.get("$patch") == "delete"
    }
    assert {
        ("StatefulSet", "postgres"),
        ("StatefulSet", "redis"),
        ("StatefulSet", "minio"),
        ("StatefulSet", "prometheus"),
        ("Deployment", "otel-collector"),
        ("Job", "minio-bootstrap"),
    } <= deleted


def test_infrastructure_images_are_registry_pinned(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    workloads = [
        resources[("StatefulSet", name)] for name in ("postgres", "redis", "minio", "prometheus")
    ]
    workloads.extend(
        [
            resources[("Deployment", "otel-collector")],
            resources[("Deployment", "prometheus-adapter")],
            resources[("Job", "minio-bootstrap")],
        ]
    )
    for workload in workloads:
        for container in workload["spec"]["template"]["spec"]["containers"]:
            image = container["image"]
            registry, _separator, remainder = image.partition("/")
            assert registry == "docker.io"
            assert "@sha256:" in remainder
            assert len(image.rsplit("@sha256:", 1)[1]) == 64


def test_minio_bootstrap_creates_bucket_scoped_application_identity(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    bootstrap = resources[("Job", "minio-bootstrap")]
    container = bootstrap["spec"]["template"]["spec"]["containers"][0]
    assert container["env"] == [{"name": "MC_CONFIG_DIR", "value": "/" + "tmp/mc"}]
    script = container["args"][0]
    assert "if ! mc admin policy info local trpc-artifacts-rw-v1" in script
    assert "mc admin policy create local trpc-artifacts-rw-v1" in script
    assert "mc admin user add local" in script
    assert "mc admin policy entities local --user" in script
    assert "mc admin policy attach local trpc-artifacts-rw-v1 --user" in script
    assert '"arn:aws:s3:::trpc-artifacts"' in script
    assert '"arn:aws:s3:::trpc-artifacts/*"' in script


def test_application_image_placeholder_uses_docker_hub() -> None:
    kustomization = yaml.safe_load((PRODUCTION / "kustomization.yaml").read_text(encoding="utf-8"))
    assert kustomization["images"] == [
        {
            "name": "trpc-agent-service",
            "newName": "docker.io/replace/trpc-agent-service",
            "digest": "sha256:REPLACE_WITH_64_HEX_DIGEST",
        }
    ]


def test_secret_examples_align_with_local_service_names() -> None:
    secret_path = ROOT / "deploy" / "kustomize" / "base" / "secrets.example.yaml"
    secrets = {
        item["metadata"]["name"]: item
        for item in yaml.safe_load_all(secret_path.read_text(encoding="utf-8"))
        if isinstance(item, dict)
    }
    assert {
        "postgres_superuser_password",
        "runtime_database_password",
        "migration_database_password",
        "worker_database_password",
        "metrics_database_password",
        "redis_password",
        "minio_root_user",
        "minio_root_password",
        "minio_application_user",
        "minio_application_password",
    } == set(secrets["trpc-infrastructure-secrets"]["stringData"])

    assert (
        urlsplit(
            secrets["trpc-service-secrets"]["stringData"]["TRPC_SERVICE_DATABASE_DSN"]
        ).hostname
        == "postgres"
    )
    assert (
        urlsplit(secrets["trpc-service-secrets"]["stringData"]["TRPC_SERVICE_REDIS_URL"]).hostname
        == "redis"
    )
    assert (
        urlsplit(
            secrets["trpc-worker-secrets"]["stringData"]["TRPC_SERVICE_WORKER_DATABASE_DSN"]
        ).hostname
        == "postgres"
    )
    assert (
        urlsplit(
            secrets["trpc-migration-secrets"]["stringData"]["TRPC_SERVICE_DATABASE_DSN"]
        ).hostname
        == "postgres"
    )
    assert (
        urlsplit(
            secrets["trpc-metrics-secrets"]["stringData"]["TRPC_SERVICE_METRICS_DATABASE_DSN"]
        ).hostname
        == "postgres"
    )


def test_managed_service_patch_renders_without_default_backends() -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required for the Kustomize render contract")
    with tempfile.TemporaryDirectory(dir=PRODUCTION.parent) as raw_directory:
        directory = Path(raw_directory)
        (directory / "kustomization.yaml").write_text(
            "\n".join(
                [
                    "apiVersion: kustomize.config.k8s.io/v1beta1",
                    "kind: Kustomization",
                    "resources:",
                    "  - ../production",
                    "patches:",
                    "  - path: ../production/managed-services-patch.example.yaml",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        rendered = subprocess.run(  # noqa: S603 - fixed kubectl binary and arguments
            [
                kubectl,
                "kustomize",
                str(directory),
                "--load-restrictor=LoadRestrictionsNone",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    documents = [item for item in yaml.safe_load_all(rendered) if isinstance(item, dict)]
    managed_resources = {(item["kind"], item["metadata"]["name"]): item for item in documents}
    assert not any(kind == "StatefulSet" for kind, _name in managed_resources)
    assert ("Deployment", "otel-collector") not in managed_resources
    assert ("Deployment", "prometheus-adapter") not in managed_resources
    assert ("Job", "minio-bootstrap") not in managed_resources
    assert ("APIService", "v1beta1.external.metrics.k8s.io") not in managed_resources
    config = managed_resources[("ConfigMap", "trpc-service-config")]["data"]
    assert config["TRPC_SERVICE_S3_ENDPOINT"] == "https://REPLACE_WITH_MANAGED_S3_ENDPOINT"
    assert config["TRPC_SERVICE_OTLP_ENDPOINT"] == "https://REPLACE_WITH_MANAGED_OTLP_ENDPOINT"


def test_external_hpa_metric_has_an_in_cluster_provider(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    worker_hpa = resources[("HorizontalPodAutoscaler", "trpc-worker")]
    metric_names = {
        metric["external"]["metric"]["name"]
        for metric in worker_hpa["spec"]["metrics"]
        if metric["type"] == "External"
    }
    adapter_config = resources[("ConfigMap", "trpc-prometheus-adapter-config")]["data"]
    adapter_rules = yaml.safe_load(adapter_config["config.yaml"])["externalRules"]
    provided_names = {rule["name"]["as"] for rule in adapter_rules}
    assert metric_names == {"trpc_session_ready_backlog"}
    assert metric_names <= provided_names

    api_service = resources[("APIService", "v1beta1.external.metrics.k8s.io")]["spec"]
    assert api_service["service"] == {
        "name": "prometheus-adapter",
        "namespace": "trpc-service",
        "port": 443,
    }


def test_production_egress_is_role_scoped_for_auth_models_and_tools(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    policy = resources[("NetworkPolicy", "trpc-allow-auth-model-tool-https-egress")]
    expression = policy["spec"]["podSelector"]["matchExpressions"][0]
    assert expression == {
        "key": "app.kubernetes.io/name",
        "operator": "In",
        "values": ["trpc-gateway", "trpc-admin", "trpc-worker"],
    }
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            "ports": [{"port": 443, "protocol": "TCP"}],
        }
    ]

    runtime_policy = resources[("NetworkPolicy", "trpc-allow-runtime")]
    private_rule = next(
        rule
        for rule in runtime_policy["spec"]["egress"]
        if {item.get("ipBlock", {}).get("cidr") for item in rule.get("to", [])}
        == {"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
    )
    assert {port["port"] for port in private_rule["ports"]} == {
        443,
        4317,
        4318,
        5432,
        6379,
        9000,
    }


def test_production_migration_waits_for_bundled_postgres(
    resources: dict[tuple[str, str], dict[str, Any]],
) -> None:
    migration = resources[("Job", "trpc-schema-migration")]
    annotations = migration["metadata"]["annotations"]
    assert annotations["argocd.argoproj.io/hook"] == "Sync"
    assert annotations["argocd.argoproj.io/hook-delete-policy"] == (
        "BeforeHookCreation,HookSucceeded"
    )
    assert annotations["argocd.argoproj.io/sync-wave"] == "-1"

    init_containers = migration["spec"]["template"]["spec"]["initContainers"]
    wait = next(item for item in init_containers if item["name"] == "wait-for-postgres")
    command = "\n".join(wait["args"])
    assert "pg_isready -h postgres -p 5432 -d trpc_service" in command
    assert 'while [ "$attempt" -lt 150 ]' in command
    assert "@sha256:" in wait["image"]

    for key in (
        ("ConfigMap", "trpc-service-config"),
        ("ConfigMap", "trpc-postgres-bootstrap"),
        ("ServiceAccount", "trpc-service"),
        ("Service", "postgres"),
        ("Service", "redis"),
        ("Service", "minio"),
        ("StatefulSet", "postgres"),
        ("StatefulSet", "redis"),
        ("StatefulSet", "minio"),
    ):
        assert resources[key]["metadata"]["annotations"]["argocd.argoproj.io/sync-wave"] == "-2"
    bootstrap_annotations = resources[("Job", "minio-bootstrap")]["metadata"]["annotations"]
    assert bootstrap_annotations["argocd.argoproj.io/sync-wave"] == "-1"
    assert bootstrap_annotations["argocd.argoproj.io/hook"] == "Sync"
    assert bootstrap_annotations["argocd.argoproj.io/hook-delete-policy"] == (
        "BeforeHookCreation,HookSucceeded"
    )
