import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = ROOT / "deploy" / "performance-runtime.override.yml"
DOCKERFILE = ROOT / "Dockerfile"


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor(
    "!override",
    lambda loader, node: loader.construct_sequence(node, deep=True),
)


def _compose_override() -> dict[str, object]:
    parsed = yaml.load(
        OVERRIDE.read_text(encoding="utf-8"),
        Loader=_ComposeLoader,  # noqa: S506
    )
    assert isinstance(parsed, dict)
    return parsed


def test_candidate_dockerfile_exposes_only_safe_source_fingerprint_labels() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert 'ARG TRPC_SOURCE_FINGERPRINT=""' in dockerfile
    assert 'org.opencontainers.image.revision="${TRPC_SOURCE_FINGERPRINT}"' in dockerfile
    assert 'io.trpc.agent-service.source-fingerprint="${TRPC_SOURCE_FINGERPRINT}"' in dockerfile


def test_performance_override_is_fail_closed_and_uses_a_dedicated_project() -> None:
    override = _compose_override()

    assert override["name"] == (
        "${TRPC_PERF_COMPOSE_PROJECT:?set to a unique trpc-perf-<run-id> project}"
    )


def test_performance_override_requires_one_candidate_image_for_runtime_roles() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)

    expected = "${TRPC_PERF_IMAGE:?set to the candidate image built with TRPC_SOURCE_FINGERPRINT}"
    for name in (
        "gateway",
        "migrate",
        "outbox-dispatcher",
        "session-recovery",
        "worker",
    ):
        service = services[name]
        assert isinstance(service, dict)
        assert service["image"] == expected


def test_performance_override_enables_only_required_runtime_roles() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)

    expected_overlay_services = {
        "gateway",
        "migrate",
        "minio-init",
        "outbox-dispatcher",
        "session-recovery",
        "worker",
    }
    disabled = {
        name
        for name, service in services.items()
        if isinstance(service, dict) and service.get("profiles") == ["performance-disabled"]
    }
    assert set(services) == expected_overlay_services | disabled
    assert disabled == {
        "admin",
        "channel-dispatcher",
        "jaeger",
        "otel-collector",
        "post-turn-projector",
        "prometheus",
        "toxiproxy",
        "wecom-connector",
    }

    base = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert isinstance(base, dict)
    base_services = base["services"]
    assert isinstance(base_services, dict)
    assert {
        "migrate",
        "minio",
        "minio-init",
        "outbox-dispatcher",
        "postgres",
        "redis",
        "worker",
    } <= set(base_services)


def test_performance_workers_are_bounded_and_do_not_restart() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)
    worker = services["worker"]
    assert isinstance(worker, dict)
    environment = worker["environment"]
    assert isinstance(environment, dict)

    assert worker["restart"] == "no"
    assert "replicas" not in worker["deploy"]
    assert environment == {
        "TRPC_SERVICE_ENVIRONMENT": "test",
        "TRPC_SERVICE_CAPTURE_CONTENT": "false",
        "TRPC_SERVICE_ONLINE_TESTS_ENABLED": "false",
        "TRPC_SERVICE_OTLP_ENDPOINT": "",
        "TRPC_SERVICE_TENANT_SECRET_ROOT": "/run/secrets",
        "TRPC_SERVICE_TENANT_SECRET_ENV_NAMES": (
            '["TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",'
            '"TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",'
            '"TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY"]'
        ),
        "TRPC_SERVICE_WORKER_CONCURRENCY": "50",
        "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": "2",
        "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": "8",
        "TRPC_SERVICE_OFFLINE_AGENT_DELAY_SECONDS": "3.0",
    }
    assert "TRPC_PERF_OFFLINE_AGENT_DELAY_SECONDS" not in environment


def test_performance_recovery_role_is_single_bounded_and_fast_polling() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)
    recovery = services["session-recovery"]
    assert isinstance(recovery, dict)

    assert "replicas" not in recovery["deploy"]
    assert recovery["deploy"]["resources"]["limits"] == {
        "cpus": "0.25",
        "memory": "256M",
    }
    assert recovery["environment"] == {
        "TRPC_SERVICE_ENVIRONMENT": "test",
        "TRPC_SERVICE_OTLP_ENDPOINT": "",
        "TRPC_SERVICE_CAPTURE_CONTENT": "false",
        "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": "1",
        "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": "2",
        "TRPC_SERVICE_RECOVERY_BATCH_SIZE": "25",
        "TRPC_SERVICE_RECOVERY_POLL_SECONDS": "1",
    }


def test_performance_gateway_is_synthetic_http_only_and_fail_closed() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)
    gateway = services["gateway"]
    assert isinstance(gateway, dict)

    assert gateway["image"] == (
        "${TRPC_PERF_IMAGE:?set to the candidate image built with TRPC_SOURCE_FINGERPRINT}"
    )
    assert gateway.get("profiles") != ["performance-disabled"]
    assert gateway["ports"] == [
        "127.0.0.1:${TRPC_PERF_GATEWAY_PORT:?set to an unused local performance gateway port}:8080"
    ]

    environment = gateway["environment"]
    assert isinstance(environment, dict)
    assert environment["TRPC_SERVICE_ENVIRONMENT"] == "test"
    assert environment["TRPC_SERVICE_CAPTURE_CONTENT"] == "false"
    assert environment["TRPC_SERVICE_OTLP_ENDPOINT"] == ""
    assert environment["TRPC_SERVICE_PROMETHEUS_ENABLED"] == "false"
    assert environment["TRPC_SERVICE_TENANT_SECRET_ROOT"] == "/run/secrets"
    assert environment["TRPC_SERVICE_TENANT_SECRET_ENV_NAMES"] == (
        '["TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",'
        '"TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",'
        '"TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY"]'
    )
    assert environment["TRPC_SERVICE_DATABASE_POOL_MIN_SIZE"] == "20"
    assert environment["TRPC_SERVICE_DATABASE_POOL_MAX_SIZE"] == "24"

    for name in (
        "APP_SECRET",
        "VERIFICATION_TOKEN",
        "ENCRYPT_KEY",
    ):
        key = f"TRPC_PERF_FIXTURE_UNUSED_{name}"
        assert environment[key] == (
            "${"
            + key
            + ":?set a random synthetic fixture "
            + {
                "APP_SECRET": "app secret",
                "VERIFICATION_TOKEN": "verification token",
                "ENCRYPT_KEY": "encrypt key",
            }[name]
            + "}"
        )

    source = OVERRIDE.read_text(encoding="utf-8")
    assert "literal://" not in source
    assert "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET: synthetic" not in source
    assert "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN: synthetic" not in source
    assert "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY: synthetic" not in source


def test_performance_outbox_is_single_bounded_node_and_observability_remains_disabled() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)
    outbox = services["outbox-dispatcher"]
    assert isinstance(outbox, dict)
    deploy = outbox["deploy"]
    assert isinstance(deploy, dict)
    assert "replicas" not in deploy
    assert deploy["resources"] == {
        "limits": {"cpus": "0.5", "memory": "512M"},
        "reservations": {"cpus": "0.1", "memory": "128M"},
    }
    for name in (
        "admin",
        "channel-dispatcher",
        "post-turn-projector",
        "wecom-connector",
        "otel-collector",
        "prometheus",
        "jaeger",
        "toxiproxy",
    ):
        assert services[name]["profiles"] == ["performance-disabled"]


def test_performance_override_keeps_outbox_and_storage_dependencies_only() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)
    outbox = services["outbox-dispatcher"]
    assert isinstance(outbox, dict)
    environment = outbox["environment"]
    assert isinstance(environment, dict)
    assert environment["TRPC_SERVICE_ENVIRONMENT"] == "test"
    assert environment["TRPC_SERVICE_CAPTURE_CONTENT"] == "false"
    assert environment["TRPC_SERVICE_OTLP_ENDPOINT"] == ""
    assert environment["TRPC_SERVICE_DATABASE_POOL_MIN_SIZE"] == "2"
    assert environment["TRPC_SERVICE_DATABASE_POOL_MAX_SIZE"] == "4"

    # The override must not introduce a public listener or provider/IM secret.
    for name in ("worker", "outbox-dispatcher"):
        service = services[name]
        assert isinstance(service, dict)
        assert "ports" not in service
        assert "secrets" not in service


def test_performance_runtime_resource_budget_is_explicit_and_bounded() -> None:
    services = _compose_override()["services"]
    assert isinstance(services, dict)

    worker = services["worker"]
    outbox = services["outbox-dispatcher"]
    assert isinstance(worker, dict)
    assert isinstance(outbox, dict)

    assert "replicas" not in worker["deploy"]
    assert worker["deploy"]["resources"]["limits"] == {
        "cpus": "1.0",
        "memory": "1G",
    }
    assert "replicas" not in outbox["deploy"]
    assert outbox["deploy"]["resources"]["limits"] == {
        "cpus": "0.5",
        "memory": "512M",
    }


def _compose_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "TRPC_PERF_COMPOSE_PROJECT": "trpc-perf-config-test",
            "TRPC_PERF_IMAGE": "trpc-agent-service:synthetic-config-test",
            "TRPC_PERF_GATEWAY_PORT": "18080",
            "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "synthetic-app-secret",
            "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "synthetic-verification-token",
            "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "synthetic-encrypt-key",
            "POSTGRES_PASSWORD": "postgres-test-password",
            "RUNTIME_DATABASE_PASSWORD": "runtime-test-password",
            "MIGRATION_DATABASE_PASSWORD": "migration-test-password",
            "REDIS_PASSWORD": "redis-test-password",
            "MINIO_ROOT_PASSWORD": "minio-test-password",
            "SESSION_HMAC_KEY": "session-hmac-test-key",
            "EMERGENCY_QUEUE_KEY": "emergency-queue-test-key",
            "DEVELOPMENT_TOKEN": "development-test-token",
        }
    )
    return environment


@pytest.mark.parametrize(
    "missing",
    (
        "TRPC_PERF_COMPOSE_PROJECT",
        "TRPC_PERF_IMAGE",
        "TRPC_PERF_GATEWAY_PORT",
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
    ),
)
def test_performance_compose_config_fails_when_required_variable_is_missing(
    missing: str,
) -> None:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker is not installed; static YAML checks still run")

    environment = _compose_environment()
    environment.pop(missing)
    result = subprocess.run(  # noqa: S603 - executable is resolved from PATH
        [
            docker,
            "compose",
            "-f",
            "docker-compose.yml",
            "-f",
            "deploy/performance-runtime.override.yml",
            "config",
            "--quiet",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0
