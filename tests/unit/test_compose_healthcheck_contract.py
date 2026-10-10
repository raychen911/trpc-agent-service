from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_NON_HTTP_ROLES = (
    "worker",
    "outbox-dispatcher",
    "channel-dispatcher",
    "post-turn-projector",
    "wecom-connector",
    "session-recovery",
    "artifact-gc",
)
_LIGHTWEIGHT_PROBE = {
    role: ["CMD", "python", "-m", "trpc_service.probe", "--role", role] for role in _NON_HTTP_ROLES
}
_HTTP_READY_PROBES = {
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


def _compose_environment() -> dict[str, str]:
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
    return environment


def _rendered_services(override: str) -> dict[str, object]:
    command = [
        shutil.which("docker") or "docker",
        "compose",
        "-f",
        str(ROOT / "docker-compose.yml"),
        "-f",
        str(ROOT / override),
        "-f",
        str(ROOT / "deploy" / "acceptance-runtime.override.yml"),
        "-p",
        "trpc-compose-healthcheck-contract",
        "config",
        "--format",
        "json",
    ]
    rendered = subprocess.run(  # noqa: S603 - fixed local Compose config command
        command,
        cwd=ROOT,
        env=_compose_environment(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert rendered.returncode == 0, rendered.stderr or rendered.stdout
    document = json.loads(rendered.stdout)
    assert isinstance(document, dict)
    services = document.get("services")
    assert isinstance(services, dict)
    return services


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is required")
@pytest.mark.parametrize(
    ("override", "active_non_http_roles"),
    (
        (
            "deploy/performance-runtime.override.yml",
            {"worker", "outbox-dispatcher", "session-recovery"},
        ),
        ("deploy/toxiproxy-runtime.override.yml", set(_NON_HTTP_ROLES)),
        ("deploy/fault-stage-runtime.override.yml", set(_NON_HTTP_ROLES)),
    ),
)
def test_acceptance_renderings_use_lightweight_non_http_probes(
    override: str, active_non_http_roles: set[str]
) -> None:
    services = _rendered_services(override)

    assert active_non_http_roles <= set(services)
    for role in active_non_http_roles:
        service = services[role]
        assert isinstance(service, dict)
        healthcheck = service["healthcheck"]
        assert healthcheck["test"] == _LIGHTWEIGHT_PROBE[role]
        assert healthcheck["timeout"] == "10s"
        assert healthcheck["interval"] == "10s"
        assert healthcheck["retries"] == 6
        assert healthcheck["start_period"] == "20s"

    # The HTTP roles retain their endpoint healthchecks whenever the scenario
    # keeps them enabled; no module probe is substituted for an HTTP service.
    for role, expected in _HTTP_READY_PROBES.items():
        if role not in services:
            continue
        service = services[role]
        assert isinstance(service, dict)
        assert service["healthcheck"]["test"] == expected
