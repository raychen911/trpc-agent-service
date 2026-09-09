"""Configuration publication coordination and cache invalidation."""

from __future__ import annotations

from typing import Awaitable
from typing import Callable

from trpc_service.config import TenantConfig
from trpc_service.log import AuditEvent
from trpc_service.log import AuditSink

from .registry import TenantRegistry


class TenantConfigurationService:
    """Publish immutable snapshots and invalidate runtimes only after commit."""

    def __init__(self,
                 registry: TenantRegistry,
                 *,
                 invalidate: Callable[[str, int | None], Awaitable[None]] | None = None,
                 validate: Callable[[TenantConfig], Awaitable[None]] | None = None,
                 audit: AuditSink | None = None) -> None:
        self._registry = registry
        self._invalidate = invalidate
        self._validate = validate
        self._audit = audit

    async def publish(self, config: TenantConfig, actor: str = "admin") -> TenantConfig:
        if self._validate:
            await self._validate(config)
        published = await self._registry.publish(config)
        await self._after_change(published, "config_publish", actor)
        return published

    async def rollback(self, tenant_id: str, version: int, actor: str = "admin") -> TenantConfig:
        if self._validate:
            await self._validate(await self._registry.get(tenant_id, version))
        active = await self._registry.rollback(tenant_id, version)
        await self._after_change(active, "config_rollback", actor)
        return active

    async def _after_change(self, config: TenantConfig, action: str, actor: str) -> None:
        if self._invalidate:
            await self._invalidate(config.tenant_id, config.version)
        if self._audit:
            await self._audit.write(
                AuditEvent(
                    tenant_id=config.tenant_id,
                    channel="admin",
                    user_id=actor,
                    session_id="",
                    agent_name="control-plane",
                    action=action,
                    request_id=f"{config.tenant_id}:{config.version}",
                ))
