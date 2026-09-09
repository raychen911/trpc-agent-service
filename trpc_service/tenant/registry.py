# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Versioned tenant configuration registry."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterable
from typing import Any
from typing import Protocol

from trpc_service.config import AgentAppConfig
from trpc_service.config import ChannelBindingConfig
from trpc_service.config import TenantConfig
from trpc_service.config import TenantStatus


class TenantNotFoundError(KeyError):
    """Raised when a tenant or tenant-owned resource does not exist."""


class TenantUnavailableError(RuntimeError):
    """Raised when a tenant is not active."""


class TenantRegistry(Protocol):
    """Storage-neutral immutable tenant configuration contract."""

    async def publish(self, config: TenantConfig) -> TenantConfig:
        ...

    async def rollback(self, tenant_id: str, version: int) -> TenantConfig:
        ...

    async def get(self, tenant_id: str, version: int | None = None, *, require_active: bool = True) -> TenantConfig:
        ...

    async def get_app(self, tenant_id: str, app_id: str, version: int | None = None) -> AgentAppConfig:
        ...

    async def resolve_binding(self, binding_id: str) -> tuple[TenantConfig, ChannelBindingConfig]:
        ...

    async def list_active(self) -> list[TenantConfig]:
        ...


class InMemoryTenantRegistry:
    """Concurrency-safe version registry used by tests and the initial service."""

    def __init__(self, configs: Iterable[TenantConfig] = ()) -> None:
        self._versions: dict[str, dict[int, TenantConfig]] = {}
        self._active: dict[str, int] = {}
        self._bindings: dict[str, tuple[str, ChannelBindingConfig]] = {}
        self._lock = asyncio.Lock()
        for config in configs:
            self._store(config, activate=True)

    def _store(self, config: TenantConfig, *, activate: bool) -> None:
        snapshot = config.model_copy(deep=True)
        self._versions.setdefault(snapshot.tenant_id, {})[snapshot.version] = snapshot
        if activate:
            self._active[snapshot.tenant_id] = snapshot.version
        for binding in snapshot.channels:
            existing = self._bindings.get(binding.binding_id)
            if existing and existing[0] != snapshot.tenant_id:
                raise ValueError(f"binding_id already belongs to another tenant: {binding.binding_id}")
            self._bindings[binding.binding_id] = (snapshot.tenant_id, binding)

    async def publish(self, config: TenantConfig) -> TenantConfig:
        """Publish and activate an immutable tenant snapshot."""
        async with self._lock:
            versions = self._versions.get(config.tenant_id, {})
            if config.version in versions:
                raise ValueError(f"tenant config version already exists: {config.tenant_id}/{config.version}")
            current = self._active.get(config.tenant_id, 0)
            if config.version <= current:
                raise ValueError("new tenant config version must be greater than the active version")
            self._store(config, activate=True)
            return config.model_copy(deep=True)

    async def rollback(self, tenant_id: str, version: int) -> TenantConfig:
        """Activate an existing validated snapshot."""
        async with self._lock:
            if version not in self._versions.get(tenant_id, {}):
                raise TenantNotFoundError(f"tenant config not found: {tenant_id}/{version}")
            self._active[tenant_id] = version
            return self._versions[tenant_id][version].model_copy(deep=True)

    async def get(self, tenant_id: str, version: int | None = None, *, require_active: bool = True) -> TenantConfig:
        """Get a defensive copy of one tenant configuration."""
        selected = version if version is not None else self._active.get(tenant_id)
        config = self._versions.get(tenant_id, {}).get(selected or -1)
        if config is None:
            raise TenantNotFoundError(f"tenant not found: {tenant_id}")
        if require_active and config.status != TenantStatus.ACTIVE:
            raise TenantUnavailableError(f"tenant is not active: {tenant_id}")
        return config.model_copy(deep=True)

    async def get_app(self, tenant_id: str, app_id: str, version: int | None = None) -> AgentAppConfig:
        """Resolve an enabled Agent App from a tenant snapshot."""
        config = await self.get(tenant_id, version)
        app = config.apps.get(app_id)
        if app is None:
            raise TenantNotFoundError(f"agent app not found: {tenant_id}/{app_id}")
        if not app.enabled:
            raise TenantUnavailableError(f"agent app is disabled: {tenant_id}/{app_id}")
        return app

    async def resolve_binding(self, binding_id: str) -> tuple[TenantConfig, ChannelBindingConfig]:
        """Resolve a published channel binding to its owning tenant."""
        item = self._bindings.get(binding_id)
        if item is None:
            raise TenantNotFoundError(f"channel binding not found: {binding_id}")
        tenant_id, binding = item
        tenant = await self.get(tenant_id)
        active = next((entry for entry in tenant.channels if entry.binding_id == binding_id), None)
        if active is None or not active.enabled:
            raise TenantUnavailableError(f"channel binding is disabled: {binding_id}")
        return tenant, active

    async def list_active(self) -> list[TenantConfig]:
        """List defensive copies of active snapshots."""
        return [await self.get(tenant_id) for tenant_id in sorted(self._active)]


class PostgresTenantRegistry:
    """Transactional control-plane registry backed by the V1 schema."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @staticmethod
    def _json(config: TenantConfig) -> str:
        return config.model_dump_json()

    async def publish(self, config: TenantConfig) -> TenantConfig:
        raw = self._json(config)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                # Serialize initial creation too (a missing row cannot be locked).
                await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", config.tenant_id)
                current = await connection.fetchval(
                    "SELECT active_config_version FROM tenant WHERE tenant_id=$1 FOR UPDATE",
                    config.tenant_id,
                )
                if current is not None and config.version <= current:
                    raise ValueError("new tenant config version must be greater than the active version")
                await connection.execute(
                    """
                    INSERT INTO tenant (tenant_id,name,status,active_config_version)
                    VALUES ($1,$2,$3,NULL)
                    ON CONFLICT (tenant_id) DO UPDATE
                        SET name=EXCLUDED.name,status=EXCLUDED.status,updated_at=now()
                    """,
                    config.tenant_id,
                    config.name,
                    config.status.value,
                )
                await connection.execute(
                    """
                    INSERT INTO tenant_config_version
                        (tenant_id,version,config_json,config_sha256,published_by)
                    VALUES ($1,$2,$3::jsonb,$4,$5)
                    """,
                    config.tenant_id,
                    config.version,
                    raw,
                    digest,
                    "service-admin",
                )
                for app in config.apps.values():
                    await connection.execute(
                        """
                        INSERT INTO agent_app
                            (tenant_id,app_id,config_version,name,agent_name,enabled,
                             model_config,tool_policy,runtime_policy)
                        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb)
                        """,
                        config.tenant_id,
                        app.app_id,
                        config.version,
                        app.name,
                        app.agent_name,
                        app.enabled,
                        app.model.model_dump_json(exclude={"api_key"}),
                        app.tools.model_dump_json(),
                        app.runtime.model_dump_json(),
                    )
                await connection.execute("UPDATE channel_binding SET enabled=FALSE WHERE tenant_id=$1",
                                         config.tenant_id)
                for binding in config.channels:
                    await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,1))", binding.binding_id)
                    owner = await connection.fetchval("SELECT tenant_id FROM channel_binding WHERE binding_id=$1",
                                                      binding.binding_id)
                    if owner is not None and owner != config.tenant_id:
                        raise ValueError(f"binding_id already belongs to another tenant: {binding.binding_id}")
                    await connection.execute(
                        """
                        INSERT INTO channel_binding
                            (binding_id,tenant_id,app_id,channel,external_account_id,
                             secret_ref,webhook_secret_ref,options,enabled,config_version)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
                        ON CONFLICT (binding_id) DO UPDATE SET
                            tenant_id=EXCLUDED.tenant_id,app_id=EXCLUDED.app_id,
                            channel=EXCLUDED.channel,external_account_id=EXCLUDED.external_account_id,
                            secret_ref=EXCLUDED.secret_ref,webhook_secret_ref=EXCLUDED.webhook_secret_ref,
                            options=EXCLUDED.options,enabled=EXCLUDED.enabled,
                            config_version=EXCLUDED.config_version
                        """,
                        binding.binding_id,
                        config.tenant_id,
                        binding.app_id,
                        binding.channel.value,
                        binding.external_account_id,
                        binding.secret_ref,
                        binding.webhook_secret_ref,
                        json.dumps(binding.options),
                        binding.enabled,
                        config.version,
                    )
                await connection.execute(
                    "UPDATE tenant SET active_config_version=$2,updated_at=now() WHERE tenant_id=$1",
                    config.tenant_id,
                    config.version,
                )
        return config.model_copy(deep=True)

    async def rollback(self, tenant_id: str, version: int) -> TenantConfig:
        config = await self.get(tenant_id, version)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                exists = await connection.fetchval(
                    "SELECT 1 FROM tenant_config_version WHERE tenant_id=$1 AND version=$2", tenant_id, version)
                if not exists:
                    raise TenantNotFoundError(f"tenant config not found: {tenant_id}/{version}")
                await connection.execute("UPDATE channel_binding SET enabled=FALSE WHERE tenant_id=$1", tenant_id)
                for binding in config.channels:
                    await connection.execute(
                        """
                        UPDATE channel_binding SET app_id=$3, channel=$4,
                            external_account_id=$5, secret_ref=$6,
                            webhook_secret_ref=$7, options=$8::jsonb,
                            enabled=$9, config_version=$10
                         WHERE tenant_id=$1 AND binding_id=$2
                        """, tenant_id, binding.binding_id, binding.app_id, binding.channel.value,
                        binding.external_account_id, binding.secret_ref, binding.webhook_secret_ref,
                        json.dumps(binding.options), binding.enabled, version)
                await connection.execute(
                    "UPDATE tenant SET active_config_version=$2,updated_at=now() WHERE tenant_id=$1", tenant_id,
                    version)
        return config

    async def get(self, tenant_id: str, version: int | None = None, *, require_active: bool = True) -> TenantConfig:
        row = await self._pool.fetchrow(
            """
            SELECT config.config_json
              FROM tenant
              JOIN tenant_config_version config
                ON config.tenant_id=tenant.tenant_id
               AND config.version=COALESCE($2,tenant.active_config_version)
             WHERE tenant.tenant_id=$1
            """,
            tenant_id,
            version,
        )
        if row is None:
            raise TenantNotFoundError(f"tenant not found: {tenant_id}")
        value = row["config_json"]
        if isinstance(value, str):
            value = json.loads(value)
        config = TenantConfig.model_validate(value)
        if require_active and config.status != TenantStatus.ACTIVE:
            raise TenantUnavailableError(f"tenant is not active: {tenant_id}")
        return config

    async def get_app(self, tenant_id: str, app_id: str, version: int | None = None) -> AgentAppConfig:
        config = await self.get(tenant_id, version)
        app = config.apps.get(app_id)
        if app is None:
            raise TenantNotFoundError(f"agent app not found: {tenant_id}/{app_id}")
        if not app.enabled:
            raise TenantUnavailableError(f"agent app is disabled: {tenant_id}/{app_id}")
        return app

    async def resolve_binding(self, binding_id: str) -> tuple[TenantConfig, ChannelBindingConfig]:
        tenant_id = await self._pool.fetchval(
            """
            SELECT binding.tenant_id FROM channel_binding binding
            JOIN tenant ON tenant.tenant_id=binding.tenant_id
            WHERE binding.binding_id=$1 AND binding.enabled
              AND binding.config_version=tenant.active_config_version
            """,
            binding_id,
        )
        if tenant_id is None:
            raise TenantNotFoundError(f"channel binding not found: {binding_id}")
        tenant = await self.get(tenant_id)
        binding = next(item for item in tenant.channels if item.binding_id == binding_id)
        return tenant, binding

    async def list_active(self) -> list[TenantConfig]:
        tenant_ids = await self._pool.fetch("SELECT tenant_id FROM tenant WHERE status='active' ORDER BY tenant_id")
        return [await self.get(row["tenant_id"]) for row in tenant_ids]
