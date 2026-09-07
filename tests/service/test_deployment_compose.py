"""Regression checks for the minimal Compose deployment wiring."""

from pathlib import Path
import tomllib

COMPOSE_FILE = Path(__file__).resolve().parents[2] / "deploy/docker-compose.minimal.yml"
PYPROJECT_FILE = COMPOSE_FILE.parents[1] / "pyproject.toml"
DOCKERFILE = COMPOSE_FILE.parent / "Dockerfile"
KUBERNETES_DIR = COMPOSE_FILE.parent / "kubernetes"


def test_minimal_compose_passes_qq_credentials_to_runtime_services():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    assert compose.count("QQBOT_APP_ID=${QQBOT_APP_ID:-}") == 2
    assert compose.count("QQBOT_APP_SECRET=${QQBOT_APP_SECRET:-}") == 2


def test_minimal_compose_allows_disabling_queue_for_small_hosts():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    assert "AGENT_QUEUE_ENABLED=${AGENT_QUEUE_ENABLED:-1}" in compose


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
    assert "${VECTOR_URL}" in config
    assert "${OBJECT_STORE_ENDPOINT}" in config
    assert agent.count("name: agent-storage-secrets") == 2
