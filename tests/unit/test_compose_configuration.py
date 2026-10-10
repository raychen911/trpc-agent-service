import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from scripts.real_runtime_gate import PARTICIPATING_SERVICES

_ACCEPTANCE_PIDS = {
    "migrate": 128,
    "minio-init": 128,
    "toxiproxy": 128,
}

_NON_HTTP_PROBE_ROLES = (
    "worker",
    "outbox-dispatcher",
    "channel-dispatcher",
    "post-turn-projector",
    "wecom-connector",
    "session-recovery",
    "artifact-gc",
)

_HTTP_HEALTHCHECKS = {
    "gateway": [
        "CMD",
        "python",
        "-c",
        "import urllib.request; "
        "urllib.request.urlopen('http://127.0.0.1:8080/health/ready', timeout=3)",
    ],
    "admin": [
        "CMD",
        "python",
        "-c",
        "import urllib.request; "
        "urllib.request.urlopen('http://127.0.0.1:8081/health/ready', timeout=3)",
    ],
}


def _compose_environment(**extra: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "POSTGRES_PASSWORD": "compose-test-postgres",
            "RUNTIME_DATABASE_PASSWORD": "compose-test-runtime",
            "WORKER_DATABASE_PASSWORD": "compose-test-worker",
            "MIGRATION_DATABASE_PASSWORD": "compose-test-migration",
            "REDIS_PASSWORD": "compose-test-redis",
            "MINIO_ROOT_PASSWORD": "compose-test-minio",
            "SESSION_HMAC_KEY": "compose-test-session-hmac-32bytes-0000",
            "EMERGENCY_QUEUE_KEY": "compose-test-emergency-32bytes-0000",
            "DEVELOPMENT_TOKEN": "compose-test-development",
            "MODEL_API_KEY": "compose-test-model-key",
            "TRPC_FAULT_RUN_ID": "compose-fault-test",
            "TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS": "0.5",
            "TRPC_FAULT_RUN_TOKEN": "compose-fault-token",
            "TRPC_PERF_COMPOSE_PROJECT": "trpc-perf-compose-test",
            "TRPC_PERF_IMAGE": "trpc-agent-service:compose-test",
            "TRPC_PERF_GATEWAY_PORT": "18080",
            "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "compose-fixture-app",
            "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "compose-fixture-token",
            "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "compose-fixture-key",
        }
    )
    environment.update(extra)
    return environment


def _compose_command(root: Path, files: tuple[str, ...], project: str) -> list[str]:
    command = ["docker", "compose"]
    for file in files:
        command.extend(("-f", str(root / file)))
    command.extend(("-p", project))
    return command


def _assert_compose_config_and_pids(
    root: Path,
    files: tuple[str, ...],
    project: str,
    expected_pids: dict[str, int],
) -> None:
    command = _compose_command(root, files, project)
    environment = _compose_environment()
    quiet = subprocess.run(  # noqa: S603
        [*command, "config", "--quiet"],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert quiet.returncode == 0, quiet.stderr or quiet.stdout

    rendered = subprocess.run(  # noqa: S603
        [*command, "config", "--format", "json"],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert rendered.returncode == 0, rendered.stderr or rendered.stdout
    services = json.loads(rendered.stdout)["services"]
    assert set(services) == set(expected_pids)
    for name, expected in expected_pids.items():
        service = services[name]
        direct = service.get("pids_limit")
        deploy_limits = service.get("deploy", {}).get("resources", {}).get("limits", {})
        deploy = deploy_limits.get("pids")
        assert direct == expected, name
        assert deploy == expected, name


def test_compose_forwards_worker_queue_and_database_pool_settings() -> None:
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    environment = compose["services"]["gateway"]["environment"]

    assert environment["TRPC_SERVICE_WORKER_CONCURRENCY"] == (
        "${TRPC_SERVICE_WORKER_CONCURRENCY:-1}"
    )
    assert environment["TRPC_SERVICE_SCHEDULER_VERSION"] == (
        "${TRPC_SERVICE_SCHEDULER_VERSION:-v2}"
    )
    assert environment["TRPC_SERVICE_REDIS_STREAM"] == (
        "${TRPC_SERVICE_REDIS_STREAM:-trpc:session-ready:v2}"
    )
    assert environment["TRPC_SERVICE_REDIS_CONSUMER_GROUP"] == (
        "${TRPC_SERVICE_REDIS_CONSUMER_GROUP:-trpc-session-ready-v2}"
    )
    assert environment["TRPC_SERVICE_REDIS_RECLAIM_AFTER_MS"] == (
        "${TRPC_SERVICE_REDIS_RECLAIM_AFTER_MS:-60000}"
    )
    assert environment["TRPC_SERVICE_DATABASE_POOL_MIN_SIZE"] == (
        "${TRPC_SERVICE_DATABASE_POOL_MIN_SIZE:-2}"
    )
    assert environment["TRPC_SERVICE_DATABASE_POOL_MAX_SIZE"] == (
        "${TRPC_SERVICE_DATABASE_POOL_MAX_SIZE:-20}"
    )
    assert environment["TRPC_SERVICE_RUNTIME_STATE_DIR"] == (
        "${TRPC_SERVICE_RUNTIME_STATE_DIR:-/tmp/trpc-agent-service}"
    )
    assert environment["TRPC_SERVICE_TENANT_SECRET_ROOT"] == (
        "${TRPC_SERVICE_TENANT_SECRET_ROOT:-/run/secrets}"
    )
    assert environment["TRPC_SERVICE_TENANT_SECRET_ENV_NAMES"] == (
        "${TRPC_SERVICE_TENANT_SECRET_ENV_NAMES:-[]}"
    )
    assert environment["TRPC_SERVICE_MODEL_ENDPOINT_HOSTS"] == (
        '${TRPC_SERVICE_MODEL_ENDPOINT_HOSTS:-["api.openai.com"]}'
    )
    assert environment["TRPC_SERVICE_FEISHU_ALLOW_STALE_BINDING_CACHE"] == (
        "${TRPC_SERVICE_FEISHU_ALLOW_STALE_BINDING_CACHE:-false}"
    )
    assert environment["TRPC_SERVICE_EMERGENCY_QUEUE_KEY_VERSION"] == (
        "${TRPC_SERVICE_EMERGENCY_QUEUE_KEY_VERSION:-v1}"
    )
    assert environment["TRPC_SERVICE_EMERGENCY_QUEUE_PREVIOUS_KEY_REFS"] == (
        "${TRPC_SERVICE_EMERGENCY_QUEUE_PREVIOUS_KEY_REFS:-{}}"
    )
    assert "TRPC_SERVICE_WORKER_DATABASE_DSN_REF" not in environment
    for role in (
        "worker",
        "outbox-dispatcher",
        "channel-dispatcher",
        "post-turn-projector",
        "wecom-connector",
        "session-recovery",
        "artifact-gc",
    ):
        role_environment = compose["services"][role]["environment"]
        assert role_environment["TRPC_SERVICE_WORKER_DATABASE_DSN_REF"] == (
            "env://TRPC_SERVICE_WORKER_DATABASE_DSN"
        )
        assert "TRPC_WORKER_USER" in role_environment["TRPC_SERVICE_WORKER_DATABASE_DSN"]
        assert role_environment["TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF"] == (
            "file:///run/secrets/worker_database_password"
        )


def test_compose_mounts_model_secret_only_into_agent_worker() -> None:
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    assert compose["secrets"]["model_api_key"] == {"environment": "MODEL_API_KEY"}
    assert "model_api_key" in compose["services"]["worker"]["secrets"]
    for name, service in compose["services"].items():
        if name != "worker":
            assert "model_api_key" not in service.get("secrets", []), name


def test_compose_session_recovery_is_postgres_only_and_conservative() -> None:
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    recovery = compose["services"]["session-recovery"]

    assert recovery["command"] == ["serve", "--role", "session-recovery"]
    assert recovery["depends_on"] == {"migrate": {"condition": "service_completed_successfully"}}
    assert recovery["secrets"] == ["runtime_database_password", "worker_database_password"]
    assert recovery["environment"]["TRPC_SERVICE_RECOVERY_BATCH_SIZE"] == (
        "${TRPC_SERVICE_RECOVERY_BATCH_SIZE:-25}"
    )
    assert recovery["environment"]["TRPC_SERVICE_RECOVERY_POLL_SECONDS"] == (
        "${TRPC_SERVICE_RECOVERY_POLL_SECONDS:-5}"
    )
    assert recovery["healthcheck"]["test"][-1] == "session-recovery"


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is required")
def test_rendered_compose_healthchecks_use_lightweight_probe_without_legacy_cli() -> None:
    """Keep container healthchecks independent of the installed console script."""

    root = Path(__file__).resolve().parents[2]
    command = _compose_command(root, ("docker-compose.yml",), "trpc-compose-probe-test")
    rendered = subprocess.run(  # noqa: S603
        [*command, "config", "--format", "json"],
        cwd=root,
        env=_compose_environment(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert rendered.returncode == 0, rendered.stderr or rendered.stdout
    services = json.loads(rendered.stdout)["services"]

    for role in _NON_HTTP_PROBE_ROLES:
        healthcheck = services[role]["healthcheck"]
        assert healthcheck["test"] == [
            "CMD",
            "python",
            "-m",
            "trpc_service.probe",
            "--role",
            role,
        ]
        assert healthcheck["timeout"] == "10s"
        assert healthcheck["test"] != ["CMD", "trpc-service", "probe", "--role", role]

    for role, expected in _HTTP_HEALTHCHECKS.items():
        healthcheck = services[role]["healthcheck"]
        assert healthcheck["test"] == expected
        assert healthcheck["timeout"] == "5s"


def test_compose_postgres_initialization_receives_worker_password() -> None:
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    assert "worker_database_password" in compose["services"]["postgres"]["secrets"]


def test_compose_postgres_initialization_receives_metrics_password() -> None:
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    assert "metrics_database_password" in compose["services"]["postgres"]["secrets"]
    assert compose["secrets"]["metrics_database_password"] == {
        "environment": "METRICS_DATABASE_PASSWORD"
    }


def test_acceptance_override_is_opt_in_and_bounds_every_compose_service() -> None:
    root = Path(__file__).resolve().parents[2]
    base = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))
    override = yaml.safe_load(
        (root / "deploy" / "acceptance-runtime.override.yml").read_text(encoding="utf-8")
    )
    assert base["services"]["gateway"]["restart"] == "unless-stopped"
    services = override["services"]
    assert set(services) == set(base["services"])
    for name, service in services.items():
        assert service["restart"] == "no", name
        assert service["pids_limit"] > 0, name
        assert service["logging"] == {
            "driver": "json-file",
            "options": {"max-size": "10m", "max-file": "3"},
        }, name
        limits = service["deploy"]["resources"]["limits"]
        assert limits["pids"] == service["pids_limit"], name
        assert float(limits["cpus"]) > 0, name
        assert str(limits["memory"]), name

    # The 100 callback/s acceptance path needs enough CPU headroom for the
    # gateway and PostgreSQL; host-resource preflight remains the performance
    # gate's responsibility rather than this static Compose contract.
    assert float(services["gateway"]["deploy"]["resources"]["limits"]["cpus"]) >= 2.0
    assert float(services["postgres"]["deploy"]["resources"]["limits"]["cpus"]) >= 4.0


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is required")
@pytest.mark.parametrize(
    ("name", "scenario_override"),
    (
        ("normal", "deploy/toxiproxy-runtime.override.yml"),
        ("fault", "deploy/fault-stage-runtime.override.yml"),
        ("performance", "deploy/performance-runtime.override.yml"),
    ),
)
def test_acceptance_compose_combinations_have_no_pid_conflict(
    name: str, scenario_override: str
) -> None:
    root = Path(__file__).resolve().parents[2]
    base_services = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))[
        "services"
    ]
    scenario_services = yaml.safe_load(
        (root / scenario_override).read_text(encoding="utf-8").replace("!override", "")
    ).get("services", {})
    profile_only = {
        service for service, definition in scenario_services.items() if definition.get("profiles")
    }
    expected_pids = {
        service: _ACCEPTANCE_PIDS.get(service, 256)
        for service in base_services
        if service not in profile_only
    }

    _assert_compose_config_and_pids(
        root,
        (
            "docker-compose.yml",
            scenario_override,
            "deploy/acceptance-runtime.override.yml",
        ),
        f"trpc-compose-pids-{name}-test",
        expected_pids,
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is required")
def test_compose_rejects_distinct_service_and_deploy_pid_limits(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    invalid_override = tmp_path / "conflicting-pids.override.yml"
    invalid_override.write_text(
        """
services:
  gateway:
    pids_limit: 256
    deploy:
      resources:
        limits:
          pids: 128
""".lstrip(),
        encoding="utf-8",
    )
    command = _compose_command(
        root, ("docker-compose.yml", str(invalid_override)), "trpc-pids-negative"
    )
    result = subprocess.run(  # noqa: S603
        [*command, "config", "--quiet"],
        cwd=root,
        env=_compose_environment(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode != 0
    error = result.stderr + result.stdout
    assert "pids_limit" in error
    assert "deploy.resources.limits.pids" in error


def test_fault_runtime_routes_and_attests_session_recovery() -> None:
    root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))
    override = yaml.safe_load(
        (root / "deploy" / "toxiproxy-runtime.override.yml").read_text(encoding="utf-8")
    )

    assert "session-recovery" in PARTICIPATING_SERVICES
    recovery_environment = override["services"]["session-recovery"]["environment"]
    assert "@toxiproxy:15432/" in recovery_environment["TRPC_SERVICE_DATABASE_DSN"]
    assert "TRPC_WORKER_USER" in recovery_environment["TRPC_SERVICE_WORKER_DATABASE_DSN"]
    assert recovery_environment["TRPC_SERVICE_REDIS_URL"] == "redis://toxiproxy:16379/0"
    assert compose["services"]["toxiproxy"]["healthcheck"]["test"] == [
        "CMD",
        "/toxiproxy-cli",
        "--host",
        "http://127.0.0.1:8474",
        "list",
    ]
    assert compose["services"]["toxiproxy"]["ports"] == [
        "127.0.0.1:${TOXIPROXY_API_PORT:-8474}:8474",
        "127.0.0.1:${TOXIPROXY_POSTGRES_PORT:-15432}:15432",
        "127.0.0.1:${TOXIPROXY_REDIS_PORT:-16379}:16379",
        "127.0.0.1:${TOXIPROXY_S3_PORT:-19000}:19000",
    ]


def test_kustomize_session_recovery_uses_only_database_secret() -> None:
    deployments_path = (
        Path(__file__).resolve().parents[2] / "deploy" / "kustomize" / "base" / "deployments.yaml"
    )
    documents = list(yaml.safe_load_all(deployments_path.read_text(encoding="utf-8")))
    deployment = next(
        item
        for item in documents
        if item.get("metadata", {}).get("name") == "trpc-session-recovery"
    )
    pod = deployment["spec"]["template"]
    container = pod["spec"]["containers"][0]
    env = {item["name"]: item for item in container["env"]}

    assert deployment["spec"]["replicas"] == 1
    assert container["args"] == ["serve", "--role", "session-recovery"]
    assert pod["spec"].get("initContainers") is None
    assert {
        item["secretRef"]["name"]
        for item in pod["spec"]["containers"][0]["envFrom"]
        if "secretRef" in item
    } == {"trpc-worker-secrets"}
    assert "TRPC_SERVICE_DATABASE_DSN" not in env
    worker_secret = yaml.safe_load_all(
        (
            Path(__file__).resolve().parents[2] / "deploy/kustomize/base/secrets.example.yaml"
        ).read_text(encoding="utf-8")
    )
    assert any(
        item.get("metadata", {}).get("name") == "trpc-worker-secrets" for item in worker_secret
    )
    assert all("REDIS" not in item["name"] for item in container["env"])
    assert container["readinessProbe"]["exec"]["command"][-1] == "session-recovery"
    assert container["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "1000m", "memory": "1Gi"},
    }


def test_kustomize_artifact_gc_receives_database_and_s3_secrets() -> None:
    deployments_path = (
        Path(__file__).resolve().parents[2] / "deploy" / "kustomize" / "base" / "deployments.yaml"
    )
    documents = list(yaml.safe_load_all(deployments_path.read_text(encoding="utf-8")))
    deployment = next(
        item for item in documents if item.get("metadata", {}).get("name") == "trpc-artifact-gc"
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert {item["secretRef"]["name"] for item in container["envFrom"] if "secretRef" in item} == {
        "trpc-service-secrets",
        "trpc-worker-secrets",
    }
    assert container["args"] == ["serve", "--role", "artifact-gc"]


def test_kustomize_scheduler_defaults_are_v2_in_base_and_production() -> None:
    root = Path(__file__).resolve().parents[2]
    base = yaml.safe_load((root / "deploy/kustomize/base/config.yaml").read_text(encoding="utf-8"))
    production = yaml.safe_load(
        (root / "deploy/kustomize/overlays/production/production-config-patch.yaml").read_text(
            encoding="utf-8"
        )
    )
    expected = {
        "TRPC_SERVICE_SCHEDULER_VERSION": "v2",
        "TRPC_SERVICE_REDIS_STREAM": "trpc:session-ready:v2",
        "TRPC_SERVICE_REDIS_CONSUMER_GROUP": "trpc-session-ready-v2",
    }
    assert {key: base["data"][key] for key in expected} == expected
    assert {key: production["data"][key] for key in expected} == expected
