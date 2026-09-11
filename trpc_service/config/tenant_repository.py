"""Tenant configuration repository with JSON snapshot implementation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from pydantic import ValidationError

from trpc_service.config.tenant import TenantConfig, TenantConfigDraft, TenantConfigError

_ROOT_KEYS = frozenset({"schema_version", "tenants"})


class TenantConfigRepository(Protocol):
    """Async protocol for tenant configuration lookup with lifecycle."""

    async def get(self, tenant_id: str) -> TenantConfig | None:
        ...

    async def get_version(self, tenant_id: str, version: int) -> TenantConfig | None:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


class TenantNotFoundError(RuntimeError):
    """The requested tenant does not exist."""


class TenantAlreadyExistsError(RuntimeError):
    """A tenant with the same ID already exists."""


class TenantConfigVersionConflictError(RuntimeError):
    """The expected version does not match the current head version."""


class TenantConfigTargetVersionNotFoundError(RuntimeError):
    """The rollback target version does not exist in history."""


class TenantConfigCommandRepository(Protocol):
    """Async protocol for versioned tenant configuration writes."""

    async def create(self, config: TenantConfig) -> TenantConfig:
        ...

    async def update(
        self,
        tenant_id: str,
        expected_version: int,
        desired: TenantConfigDraft,
    ) -> TenantConfig:
        ...

    async def rollback(
        self,
        tenant_id: str,
        expected_version: int,
        target_version: int,
    ) -> TenantConfig:
        ...

    async def list_versions(
        self,
        tenant_id: str,
        *,
        before_version: int | None = None,
        limit: int = 50,
    ) -> tuple[TenantConfig, ...]:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


class TenantConfigAdminRepository(
        TenantConfigRepository,
        TenantConfigCommandRepository,
        Protocol,
):
    """Combined read/write protocol used only by the Admin application."""


class TenantRepositoryConfigurationError(ValueError):
    """Repository could not be configured (missing/invalid settings)."""


class TenantRepositoryUnavailableError(RuntimeError):
    """Repository backend is not reachable or not ready."""


class TenantRepositoryDataError(RuntimeError):
    """Repository data is corrupt or conflicts with existing data."""


class JsonTenantConfigRepository:
    """Repository that loads tenant configuration from a JSON file snapshot."""

    @classmethod
    def from_path(cls, path: Path) -> JsonTenantConfigRepository:
        """Load and validate tenant configuration from a JSON file."""
        configs = load_tenant_configs(path)
        return cls(MappingProxyType({c.tenant_id: c for c in configs}))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> JsonTenantConfigRepository:
        """Load repository using environment variable or default path."""
        import os

        values = os.environ if environ is None else environ
        path_str = values.get("TRPC_TENANT_CONFIG_PATH", "").strip()

        if path_str:
            path = Path(path_str)
            if not path.is_absolute():
                path = Path.cwd() / path
        else:
            project_root = Path(__file__).resolve().parents[2]
            path = project_root / "data" / "tenants.json"

        return cls.from_path(path)

    def __init__(self, configs: MappingProxyType[str, TenantConfig]) -> None:
        self._configs = configs

    async def get(self, tenant_id: str) -> TenantConfig | None:
        """Look up tenant configuration by ID. Returns None if not found."""
        return self._configs.get(tenant_id)

    async def get_version(self, tenant_id: str, version: int) -> TenantConfig | None:
        config = self._configs.get(tenant_id)
        return config if config is not None and config.version == version else None

    async def check_ready(self) -> None:
        """No-op: JSON snapshot is always available after construction."""

    async def close(self) -> None:
        """No-op: JSON snapshot holds no external resources."""


def load_tenant_configs(path: Path) -> tuple[TenantConfig, ...]:
    """Parse and validate a JSON tenant configuration file.

    Returns a tuple of strictly validated ``TenantConfig`` objects.
    Raises ``TenantConfigError`` on any validation failure.
    """
    if not path.is_file():
        raise TenantConfigError("Tenant configuration file is not readable.")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise TenantConfigError("Tenant configuration file is not readable.") from None

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise TenantConfigError("Tenant configuration is not valid JSON.") from None

    if not isinstance(data, dict):
        raise TenantConfigError("Tenant configuration is not valid JSON.")

    if set(data.keys()) != _ROOT_KEYS:
        raise TenantConfigError("Tenant configuration contains invalid entries.")

    schema_version = data["schema_version"]
    if type(schema_version) is not int or schema_version != 1:
        raise TenantConfigError("Unsupported tenant configuration schema.")

    tenants_list = data["tenants"]
    if not isinstance(tenants_list, list) or len(tenants_list) == 0:
        raise TenantConfigError("Tenant configuration contains invalid entries.")

    configs: list[TenantConfig] = []
    seen: set[str] = set()
    for entry in tenants_list:
        try:
            config = TenantConfig.model_validate(entry)
        except ValidationError:
            raise TenantConfigError("Tenant configuration contains invalid entries.") from None

        if config.tenant_id in seen:
            raise TenantConfigError("Tenant configuration contains duplicate tenant IDs.")
        seen.add(config.tenant_id)
        configs.append(config)

    return tuple(configs)


__all__ = [
    "JsonTenantConfigRepository",
    "TenantAlreadyExistsError",
    "TenantConfigAdminRepository",
    "TenantConfigCommandRepository",
    "TenantConfigRepository",
    "TenantConfigTargetVersionNotFoundError",
    "TenantConfigVersionConflictError",
    "TenantNotFoundError",
    "TenantRepositoryConfigurationError",
    "TenantRepositoryDataError",
    "TenantRepositoryUnavailableError",
    "load_tenant_configs",
]
