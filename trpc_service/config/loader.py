# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Environment and YAML configuration loading helpers."""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from .models import ServiceRole
from .models import ServiceSettings
from .models import TenantConfig

_ENV_WITH_DEFAULT = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def load_environment_file(path: str | Path = ".env", *, override: bool = False) -> bool:
    """Load a local dotenv file without overwriting exported variables by default."""
    env_path = Path(path)
    if not env_path.is_file():
        return False
    return bool(load_dotenv(dotenv_path=env_path, override=override, encoding="utf-8"))


def load_settings(environ: dict[str, str] | None = None) -> ServiceSettings:
    """Load process settings without a dependency on pydantic-settings."""
    env = environ or os.environ
    roles = {
        ServiceRole(value.strip())
        for value in env.get("TRPC_SERVICE_ROLES", "gateway,worker,delivery,admin").split(",") if value.strip()
    }
    return ServiceSettings(
        environment=env.get("TRPC_SERVICE_ENV", "development"),
        host=env.get("TRPC_SERVICE_HOST", "127.0.0.1"),
        port=int(env.get("TRPC_SERVICE_PORT", "8080")),
        roles=roles,
        config_file=env.get("TRPC_SERVICE_CONFIG", "examples/config/tenants.yaml"),
        log_level=env.get("TRPC_SERVICE_LOG_LEVEL", "INFO"),
        worker_id=env.get("TRPC_SERVICE_WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"),
        admin_token=env.get("TRPC_SERVICE_ADMIN_TOKEN") or None,
        redis_url=env.get("TRPC_SERVICE_REDIS_URL", "redis://127.0.0.1:6379/15"),
        postgres_url=env.get("TRPC_SERVICE_POSTGRES_URL",
                             "postgresql://trpc_agent:trpc_agent@127.0.0.1:5432/trpc_agent"),
        otlp_endpoint=env.get("TRPC_SERVICE_OTLP_ENDPOINT", ""),
    )


def _expand_env(value: Any) -> Any:
    """Expand ${NAME} placeholders recursively while preserving data types."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name, fallback = match.group(1), match.group(2)
            return os.environ.get(name, fallback if fallback is not None else match.group(0))

        return _ENV_WITH_DEFAULT.sub(replace, value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def load_tenant_configs(path: str | Path) -> list[TenantConfig]:
    """Read and validate tenant snapshots from YAML or JSON-compatible YAML."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("tenants", [])
    else:
        items = []
    if not isinstance(items, list):
        raise ValueError("tenant configuration must be a list or contain a 'tenants' list")
    return [TenantConfig.model_validate(_expand_env(item)) for item in items]
