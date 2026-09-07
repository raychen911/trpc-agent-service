"""Immutable tenant configuration lifecycle and rollback service."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from tenant_agent.models import ConfigVersion, TenantConfig
from tenant_agent.storage.base import ConfigRepository

ConfigPreflight = Callable[[TenantConfig], Awaitable[None]]


def _canonicalize(value: Any) -> Any:
    """Return a JSON-safe value with deterministic ordering for unordered fields."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _canonicalize(item) for key, item in sorted(value.items())}
    if isinstance(value, (set, frozenset)):
        normalized = [_canonicalize(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                default=str,
            ),
        )
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    return value


def configuration_checksum(config: TenantConfig) -> str:
    canonical = json.dumps(
        _canonicalize(config.model_dump(mode="python")),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class TenantConfigService:
    def __init__(self, repository: ConfigRepository) -> None:
        self.repository = repository

    async def create_version(
        self,
        config: TenantConfig,
        *,
        actor: str,
        activate: bool = False,
    ) -> ConfigVersion:
        versions = await self.repository.list_config_versions(config.tenant_id)
        checksum = configuration_checksum(config)
        matching = next(
            (version for version in versions if version.checksum_sha256 == checksum),
            None,
        )
        if matching is not None:
            if activate and matching.status != "active":
                await self.repository.activate_config(config.tenant_id, matching.revision, datetime.now(UTC))
                refreshed = await self.repository.get_config_version(config.tenant_id, matching.revision)
                assert refreshed is not None
                return refreshed
            return matching
        existing_revisions = {version.revision for version in versions}
        if config.revision in existing_revisions:
            next_revision = max(existing_revisions, default=0) + 1
            config = config.model_copy(update={"revision": next_revision})
        version = ConfigVersion(
            tenant_id=config.tenant_id,
            revision=config.revision,
            config=config,
            status="draft",
            created_by=actor,
            checksum_sha256=configuration_checksum(config),
        )
        await self.repository.save_config_version(version)
        if activate:
            await self.repository.activate_config(config.tenant_id, config.revision, datetime.now(UTC))
            active = await self.repository.get_config_version(config.tenant_id, config.revision)
            assert active is not None
            return active
        return version

    async def activate(self, tenant_id: str, revision: int) -> ConfigVersion:
        await self.repository.activate_config(tenant_id, revision, datetime.now(UTC))
        version = await self.repository.get_config_version(tenant_id, revision)
        if version is None:
            raise KeyError(f"unknown configuration revision {tenant_id}/{revision}")
        return version

    async def rollback(self, tenant_id: str, revision: int) -> ConfigVersion:
        """Rollback is an atomic pointer switch to a validated immutable version."""

        return await self.activate(tenant_id, revision)

    async def resolve_binding(self, channel: str, binding_id: str) -> TenantConfig:
        config = await self.repository.get_tenant_by_binding(channel, binding_id)
        if config is None:
            raise KeyError("unknown or disabled channel binding")
        return config

    async def exact_revision(self, tenant_id: str, revision: int) -> TenantConfig:
        version = await self.repository.get_config_version(tenant_id, revision)
        if version is None:
            raise KeyError(f"unknown tenant configuration {tenant_id}/{revision}")
        return version.config

    async def bootstrap(
        self,
        path: Path | None,
        *,
        preflight: ConfigPreflight | None = None,
    ) -> int:
        if path is None or not await asyncio.to_thread(path.exists):
            return 0
        raw_text = await asyncio.to_thread(path.read_text, encoding="utf-8")
        payload = yaml.safe_load(raw_text) or {}
        raw_tenants: list[dict[str, Any]]
        if isinstance(payload, list):
            raw_tenants = payload
        else:
            raw_tenants = payload.get("tenants", [])
        count = 0
        for raw_tenant in raw_tenants:
            config = TenantConfig.model_validate(raw_tenant)
            versions = await self.repository.list_config_versions(config.tenant_id)
            if any(version.status == "active" for version in versions):
                # Bootstrap is seed-only. Runtime restarts must never reactivate an
                # obsolete file revision over an administrator's active revision.
                continue
            checksum = configuration_checksum(config)
            matching = next(
                (version for version in versions if version.checksum_sha256 == checksum),
                None,
            )
            if matching is not None:
                if preflight is not None:
                    await preflight(matching.config)
                await self.activate(config.tenant_id, matching.revision)
                count += 1
                continue
            if preflight is not None:
                await preflight(config)
            await self.create_version(config, actor="bootstrap", activate=True)
            count += 1
        return count
