"""Regression checks for the minimal Compose deployment wiring."""

from pathlib import Path
import tomllib

import yaml

COMPOSE_FILE = Path(__file__).resolve().parents[2] / "deploy/docker-compose.minimal.yml"
OBSERVABILITY_COMPOSE_FILE = COMPOSE_FILE.parent / "docker-compose.observability.yml"
COMPOSE_COLLECTOR_FILE = COMPOSE_FILE.parent / "otel-collector.compose.yaml"
PROMETHEUS_CONFIG_FILE = COMPOSE_FILE.parent / "prometheus/prometheus.yml"
PROMETHEUS_ALERTS_FILE = COMPOSE_FILE.parent / "prometheus/alerts.yml"
PYPROJECT_FILE = COMPOSE_FILE.parents[1] / "pyproject.toml"
DOCKERFILE = COMPOSE_FILE.parent / "Dockerfile"
KUBERNETES_DIR = COMPOSE_FILE.parent / "kubernetes"
FAULT_COMPOSE_FILE = COMPOSE_FILE.parent / "fault-stage-runtime.override.yml"
FAULT_SCRIPT_FILE = COMPOSE_FILE.parent / "run-fault-stage.sh"
PRODUCTION_OVERLAY = COMPOSE_FILE.parent / "kustomize/overlays/production"
CI_FILE = COMPOSE_FILE.parents[1] / ".github/workflows/ci.yml"


def test_minimal_compose_passes_named_qq_credentials_to_runtime_roles():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    for variable in (
            "TRPC_SERVICE_QQ_GREETING_APP_ID",
            "TRPC_SERVICE_QQ_GREETING_APP_SECRET",
            "TRPC_SERVICE_QQ_CHAT_APP_ID",
            "TRPC_SERVICE_QQ_CHAT_APP_SECRET",
    ):
        assert compose.count(f"{variable}=${{{variable}:-}}") == 3


def test_tenant_config_binds_two_qq_bots_to_distinct_tenants():
    tenants = (COMPOSE_FILE.parent / "tenants.yaml").read_text(encoding="utf-8")

    assert "tenant_id: local_demo" in tenants
    assert "tenant_id: chat_assistant" in tenants
    assert tenants.count("channel_type: qq") == 2
    assert "app_id: ${TRPC_SERVICE_QQ_GREETING_APP_ID}" in tenants
    assert "app_id: ${TRPC_SERVICE_QQ_CHAT_APP_ID}" in tenants
    assert "secret: env://TRPC_SERVICE_QQ_GREETING_APP_SECRET" in tenants


def test_minimal_compose_uses_queue_and_durable_outbox_roles():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    assert "TRPC_SERVICE_QUEUE_ENABLED=1" in compose
    assert "TRPC_SERVICE_DURABLE_DELIVERY_ENABLED=1" in compose
    assert 'command: ["python", "-m", "trpc_service.agent.run_outbox"]' in compose
    assert 'command: ["python", "-m", "trpc_service.migrations.run"' in compose


def test_fault_stage_routes_all_runtime_roles_through_toxiproxy():
    override = FAULT_COMPOSE_FILE.read_text(encoding="utf-8")
    script = FAULT_SCRIPT_FILE.read_text(encoding="utf-8")

    assert override.count("TRPC_SERVICE_REDIS_URL=redis://toxiproxy:6380/0") == 3
    assert override.count("toxiproxy:3307/trpc_agent") == 3
    assert '"name":"redis"' in override
    assert '"name":"mysql"' in override
    assert "for dependency in redis mysql" in script


def test_production_overlay_includes_isolated_canary():
    kustomization = (PRODUCTION_OVERLAY / "kustomization.yaml").read_text(encoding="utf-8")
    canary = (PRODUCTION_OVERLAY / "canary.yaml").read_text(encoding="utf-8")

    assert "canary.yaml" in kustomization
    assert "name: agent-gateway-canary" in canary
    assert "release-track: canary" in canary
    assert "kind: Ingress" not in canary
    assert "readOnlyRootFilesystem: true" in canary
    assert "name: TRPC_SERVICE_ADMIN_API_KEY" in canary


def test_kustomize_files_are_parseable_and_base_uses_resource_directory():
    root = COMPOSE_FILE.parent / "kustomize"
    files = list(root.rglob("*.yaml")) + [KUBERNETES_DIR / "kustomization.yaml"]

    for path in files:
        assert all(isinstance(document, dict) for document in yaml.safe_load_all(path.read_text(encoding="utf-8")))

    base = (root / "base/kustomization.yaml").read_text(encoding="utf-8")
    production = (root / "overlays/production/kustomization.yaml").read_text(encoding="utf-8")
    assert "- ../../kubernetes" in base
    assert "- production-hardening.yaml" in production
    agent = (KUBERNETES_DIR / "agent.yaml").read_text(encoding="utf-8")
    outbox = (KUBERNETES_DIR / "outbox.yaml").read_text(encoding="utf-8")
    assert agent.count("name: TRPC_SERVICE_ENVIRONMENT") == 2
    assert "name: TRPC_SERVICE_ENVIRONMENT" in outbox


def test_ci_validates_dependencies_fault_topology_overlays_and_migrations():
    workflow = CI_FILE.read_text(encoding="utf-8")

    assert "Install deployment validation dependencies" in workflow
    assert "pip install -e ." in workflow
    assert "-f deploy/fault-stage-runtime.override.yml config --quiet" in workflow
    assert workflow.count("kubectl kustomize deploy/kustomize/overlays/") == 2
    assert "python -m trpc_service.migrations.run --check" in workflow


def test_minimal_compose_shares_local_artifacts_between_gateway_and_worker():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    assert compose.count("artifact-data:/app/data/objects") == 2
    assert "artifact-data:" in compose


def test_minimal_compose_binds_host_ports_to_loopback():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    assert '"127.0.0.1:8080:8080"' in compose
    assert '"127.0.0.1:6379:6379"' not in compose
    assert '"127.0.0.1:3306:3306"' not in compose


def test_package_declares_fastapi_for_service_gateway_runtime():
    pyproject = tomllib.loads(PYPROJECT_FILE.read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]

    assert "fastapi>=0.95.0" in dependencies


def test_service_uses_the_sdk_as_an_external_dependency():
    pyproject_text = PYPROJECT_FILE.read_text(encoding="utf-8")
    pyproject = tomllib.loads(pyproject_text)

    assert "trpc-agent-py[openclaw]>=1.1.17,<1.2" in pyproject["project"]["dependencies"]
    assert "boto3>=1.34" in pyproject["project"]["optional-dependencies"]["object-storage"]
    assert "qdrant-client>=1.15,<2" in pyproject["project"]["optional-dependencies"]["vector-storage"]
    assert pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["trpc_service"]
    assert 'packages = ["trpc_agent_sdk"]' not in pyproject_text


def test_runtime_image_installs_production_storage_adapters():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "'/src[object-storage,vector-storage]'" in dockerfile


def test_production_manifests_wire_vector_and_object_storage_secrets():
    config = (KUBERNETES_DIR / "configmap.yaml").read_text(encoding="utf-8")
    agent = (KUBERNETES_DIR / "agent.yaml").read_text(encoding="utf-8")

    assert config.count("backend: qdrant") == 2
    assert config.count("backend: s3") == 2
    assert "env://TRPC_SERVICE_VECTOR_URL" in config
    assert "${TRPC_SERVICE_OBJECT_STORE_ENDPOINT}" in config
    assert agent.count("name: agent-storage-secrets") == 2


def test_observability_compose_exports_metrics_to_prometheus():
    compose = OBSERVABILITY_COMPOSE_FILE.read_text(encoding="utf-8")
    collector = COMPOSE_COLLECTOR_FILE.read_text(encoding="utf-8")
    prometheus = PROMETHEUS_CONFIG_FILE.read_text(encoding="utf-8")
    alerts = PROMETHEUS_ALERTS_FILE.read_text(encoding="utf-8")

    assert compose.count("OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318") == 2
    assert "PROMETHEUS_URL=http://prometheus:9090" in compose
    assert "127.0.0.1:8889:8889" in compose
    assert "127.0.0.1:9090:9090" in compose
    assert "endpoint: 0.0.0.0:4317" in collector
    assert "endpoint: 0.0.0.0:4318" in collector
    assert "metrics:" in collector
    assert "exporters: [prometheus]" in collector
    assert "translation_strategy: UnderscoreEscapingWithoutSuffixes" in collector
    assert "resource_to_telemetry_conversion:" in collector
    assert 'delete_key(attributes, "gen_ai.user.id")' in collector
    assert "otel-collector:8889" in prometheus
    assert "AgentDeadLetterQueueGrowth" in alerts
    assert "AgentImDeliveryErrors" in alerts
    assert "AgentBudgetRejections" in alerts
    assert "AgentTokenBudgetNearLimit" in alerts


def test_kubernetes_observability_scrapes_every_collector_replica():
    agent = (KUBERNETES_DIR / "agent.yaml").read_text(encoding="utf-8")
    collector = (KUBERNETES_DIR / "otel-collector.yaml").read_text(encoding="utf-8")
    prometheus = (KUBERNETES_DIR / "prometheus.yaml").read_text(encoding="utf-8")

    assert agent.count("name: OTEL_SERVICE_NAME") == 2
    assert "value: trpc-agent-gateway" in agent
    assert "value: trpc-agent-worker" in agent
    assert agent.count("name: OTEL_METRIC_EXPORT_INTERVAL") == 2
    assert "endpoint: 0.0.0.0:8889" in collector
    assert "resource_to_telemetry_conversion:" in collector
    assert "translation_strategy: UnderscoreEscapingWithoutSuffixes" in collector
    assert 'delete_key(attributes, "gen_ai.user.id")' in collector
    assert "clusterIP: None" in collector
    assert "metrics:" in collector
    assert "dns_sd_configs:" in prometheus
    assert "port: 8889" in prometheus
    assert "PROMETHEUS_URL" in agent
    assert "AgentTokenBudgetNearLimit" in prometheus
