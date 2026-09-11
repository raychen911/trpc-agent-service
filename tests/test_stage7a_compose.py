"""Stage 7A Docker Compose orchestration contract (unit, RED before files exist).

The compose topology is FIXED and small: 1 Gateway, 2 Workers, 1 Admin,
Redis, PostgreSQL, an OTel Collector and ONE one-shot init service.  These
tests pin the properties the plan demands — no real secrets in the files, a
healthcheck on every long-running service, the Gateway gated on shared
backend health, migrations executed ONLY by the single init service (never
by multiple Workers concurrently), and unconfigured IM long connections OFF
by default.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = PROJECT_ROOT / "compose.yaml"
DOCKERFILE_PATH = PROJECT_ROOT / "Dockerfile"
COLLECTOR_CONFIG_PATH = PROJECT_ROOT / "deploy" / "otel-collector.yaml"
DOCKERIGNORE_PATH = PROJECT_ROOT / ".dockerignore"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"
IM_ENV_EXAMPLE_PATH = PROJECT_ROOT / "deploy" / "im.env.example"
LOCK_PATH = PROJECT_ROOT / "requirements.lock.txt"

SERVICES = {"postgres", "redis", "minio", "otel-collector", "init", "gateway", "worker-a", "worker-b", "admin"}
LONG_RUNNING = SERVICES - {"init"}

# keys whose VALUES may never be committed literals that look like real secrets
_SENSITIVE_KEY = re.compile(r"(TOKEN|PASSWORD|SECRET|API_KEY|CREDENTIAL)", re.IGNORECASE)
_LITERAL_SECRETISH = re.compile(r"^[A-Za-z0-9+/=_-]{24,}$")


@pytest.fixture(scope="module")
def compose() -> dict:
    if not COMPOSE_PATH.exists():
        pytest.skip("compose.yaml missing — Stage 7A not yet implemented")
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


# ------------------------------------------------------------------ topology


def test_compose_files_exist():
    assert COMPOSE_PATH.exists(), "compose.yaml required"
    assert DOCKERFILE_PATH.exists(), "Dockerfile required"
    assert COLLECTOR_CONFIG_PATH.exists(), "OTel collector config required"
    assert LOCK_PATH.exists(), "requirements.lock.txt required"


def test_fixed_topology(compose: dict):
    services = compose["services"]
    assert set(services) == SERVICES
    workers = [name for name in services if name.startswith("worker")]
    assert sorted(workers) == ["worker-a", "worker-b"]


def test_published_ports_are_localhost_only_and_overrideable(compose: dict):
    assert compose["services"]["gateway"]["ports"] == [
        "${TRPC_GATEWAY_PUBLISH_HOST:-127.0.0.1}:${TRPC_GATEWAY_PUBLISH_PORT:-8000}:8000"
    ]
    assert compose["services"]["admin"]["ports"] == [
        "${TRPC_ADMIN_PUBLISH_HOST:-127.0.0.1}:${TRPC_ADMIN_PUBLISH_PORT:-8003}:8003"
    ]


def test_no_latest_tags_and_pinned_images(compose: dict):
    services = compose["services"]
    for name in ("postgres", "redis", "minio", "otel-collector"):
        image = services[name]["image"]
        assert ":" in image, f"{name} must pin an image tag"
        assert not image.endswith(":latest")
    for name in ("gateway", "worker-a", "worker-b", "admin", "init"):
        assert "build" in services[name], f"{name} must build from the project image"


# ----------------------------------------------------------------- healthchecks


def test_all_long_running_services_have_healthchecks(compose: dict):
    services = compose["services"]
    for name in LONG_RUNNING:
        assert "healthcheck" in services[name], f"{name} must declare a healthcheck"
        test = services[name]["healthcheck"]["test"]
        assert test, f"{name} healthcheck must be non-empty"


def test_gateway_and_workers_gate_on_shared_backends(compose: dict):
    """Backend health + the ONE-shot init are preconditions; nothing else."""
    services = compose["services"]
    for name in ("gateway", "worker-a", "worker-b", "admin"):
        deps = services[name]["depends_on"]
        assert deps["postgres"]["condition"] == "service_healthy"
        assert deps["init"]["condition"] == "service_completed_successfully"
    gateway_deps = services["gateway"]["depends_on"]
    assert gateway_deps["redis"]["condition"] == "service_healthy"
    for name in ("init", "worker-a", "worker-b"):
        assert services[name]["depends_on"]["minio"]["condition"] == "service_healthy"


def test_gateway_health_excludes_worker_readiness():
    """Workers must not be hard start-dependencies of the Gateway: an
    unhealthy Worker has to degrade to fixed errors, not block boot."""
    services = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))["services"]
    gateway_deps = set(services["gateway"].get("depends_on", {}))
    assert not {name for name in gateway_deps if name.startswith("worker")}


# ------------------------------------------------------------- single migration


def test_migrations_run_only_in_the_one_shot_init_service(compose: dict):
    services = compose["services"]
    init = services["init"]
    command = " ".join(init.get("command", [])) if isinstance(init.get("command"), list) else init.get("command", "")
    assert "db-migrate" in command, "init service must run db-migrate"
    assert "backend-init" in command, "init service must initialize shared backends"
    # one-shot: must exit, never restart
    assert init.get("restart") in (None, "no"), "init service must be one-shot"
    assert "healthcheck" not in init
    assert init.get("depends_on", {}).get("postgres", {}).get("condition") == "service_healthy"
    for name in ("gateway", "worker-a", "worker-b", "admin"):
        svc = services[name]
        svc_command = svc.get("command", [])
        joined = " ".join(svc_command) if isinstance(svc_command, list) else str(svc_command)
        assert "db-migrate" not in joined and "alembic" not in joined, \
            f"{name} must never run migrations concurrently"
        assert svc["depends_on"]["init"]["condition"] == "service_completed_successfully"


# ------------------------------------------------------------------ no secrets


def _flatten_env_values(svc: dict):
    env = svc.get("environment") or {}
    if isinstance(env, list):  # KEY=VALUE form
        for entry in env:
            key, _, value = entry.partition("=")
            yield key, value
    else:
        for key, value in env.items():
            yield key, "" if value is None else str(value)


def test_compose_contains_no_literal_secrets(compose: dict):
    for name, svc in compose["services"].items():
        for key, value in _flatten_env_values(svc):
            if not _SENSITIVE_KEY.search(key):
                continue
            assert "${" in value, f"{name}.{key}: secrets must be ${'{VAR}'} interpolation, not literals"
            # forbid committed defaults that look like real (non-placeholder) secrets
            default = re.search(r":-([^}]*)\}", value) or re.search(r":\?-?([^}]*)\}", value)
            if default and default.group(1):
                assert not _LITERAL_SECRETISH.match(default.group(1)), \
                    f"{name}.{key} carries a secret-shaped literal default"


def test_compose_model_provider_default_matches_runtime_contract():
    text = COMPOSE_PATH.read_text(encoding="utf-8")
    assert "${TRPC_MODEL_PROVIDER:-openai-compatible}" in text


def test_services_do_not_load_host_dotenv(compose: dict):
    """The repo .env is for host-driven acceptance scripts; no service may
    load it as container env.  Any env_file that IS referenced must be an
    explicitly optional file (IM opt-in), never the root .env."""
    for name, svc in compose["services"].items():
        entries = svc.get("env_file") or []
        if isinstance(entries, (str, dict)):
            entries = [entries]
        for entry in entries:
            path = entry if isinstance(entry, str) else entry.get("path", "")
            assert Path(path).name != ".env", f"{name} must not load the repo .env"
            assert isinstance(entry, dict) and entry.get("required") is False, \
                f"{name}.env_file entries must be optional (required: false)"


def test_dockerfile_and_dockerignore_exclude_local_state():
    assert DOCKERIGNORE_PATH.exists()
    dockerignore = DOCKERIGNORE_PATH.read_text(encoding="utf-8")
    for pattern in (".env", ".venv", ".git"):
        assert pattern in dockerignore
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    assert re.search(r"(?i)^FROM python:3\.12", dockerfile, re.MULTILINE)
    assert "requirements.lock.txt" in dockerfile
    assert re.search(r"pip install .*(-r requirements.lock.txt|--requirement)", dockerfile)
    assert "COPY .env" not in dockerfile
    assert re.search(r"(?m)^ENTRYPOINT", dockerfile)


def test_installed_package_contains_web_console():
    """The image entrypoint imports the installed wheel, not ``/app`` source."""
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    package_data = config["tool"]["setuptools"]["package-data"]
    assert "static/index.html" in package_data["trpc_service.web"]
    assert (PROJECT_ROOT / "trpc_service" / "web" / "static" / "index.html").is_file()


def test_env_example_documents_variables_with_placeholders_only():
    if not ENV_EXAMPLE_PATH.exists():
        pytest.skip(".env.example not provided (optional per plan)")
    text = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    for var in (
            "TRPC_MODEL_API_KEY",
            "TRPC_MODEL_NAME",
            "TRPC_INTERNAL_TOKEN",
            "TRPC_ADMIN_TOKEN",
            "TRPC_COMPOSE_PG_USER",
            "TRPC_COMPOSE_PG_PASSWORD",
    ):
        assert var in text, f".env.example must document {var}"
    for line in text.splitlines():
        if "=" not in line or line.strip().startswith("#"):
            continue
        key, _, value = line.partition("=")
        if _SENSITIVE_KEY.search(key):
            assert not _LITERAL_SECRETISH.match(value.strip()), \
                f".env.example {key} must carry an obviously fake placeholder"


def test_im_example_exposes_only_secret_refs_not_tenant_or_account_authority():
    values = {}
    for line in IM_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value

    assert values == {
        "TRPC_WECOM_BOT_SECRET": "replace-with-your-bot-secret",
        "TRPC_WECOM_WEBHOOK_TOKEN": "replace-with-your-webhook-token",
        "TRPC_WECOM_WEBHOOK_AES_KEY": "replace-with-your-encoding-aes-key",
        "TRPC_FEISHU_APP_SECRET": "replace-with-your-app-secret",
    }


# ------------------------------------------------------ IM connections off by default


def test_compose_never_enables_unconfigured_im_connections(compose: dict):
    """WeCom/Feishu adapters activate only from their own env vars.  The
    compose must not interpolate them from the host/.env (that would silently
    dial real platforms), and the gateway container env must not set them."""
    text = COMPOSE_PATH.read_text(encoding="utf-8")
    assert "TRPC_FEISHU" not in text
    gateway_env_keys = {key for key, _ in _flatten_env_values(compose["services"]["gateway"])}
    assert not {k for k in gateway_env_keys if k.startswith("TRPC_FEISHU")}
    assert {"TRPC_WECOM_WEBHOOK_TOKEN", "TRPC_WECOM_WEBHOOK_AES_KEY"} <= gateway_env_keys
    assert "${TRPC_WECOM_WEBHOOK_TOKEN:-}" in text
    assert "${TRPC_WECOM_WEBHOOK_AES_KEY:-}" in text


# ---------------------------------------------------------- collector config


def test_collector_config_minimal_and_health_ext_enabled():
    config = yaml.safe_load(COLLECTOR_CONFIG_PATH.read_text(encoding="utf-8"))
    assert "otlp" in config["receivers"]
    assert "health_check" in config.get("extensions", {})
    assert "health_check" in config["service"]["extensions"]
    for signal in ("traces", "metrics"):
        pipeline = config["service"]["pipelines"][signal]
        assert "otlp" in pipeline["receivers"]
        assert pipeline["exporters"], f"collector must export {signal} somewhere locally"
        assert pipeline["processors"], f"collector must process {signal}"
