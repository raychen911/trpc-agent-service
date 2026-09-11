"""Version-exact tenant storage resolution and shared service lifecycle."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from .bundle import TenantScopedService
from .bundle import TenantStorageBundle
from .models import BackendProfile
from .models import BackendProfileRegistry
from .models import ResourceType
from .models import TenantBackendBinding
from .models import validate_tenant_bindings
from .memory import GuardedMemoryService
from .session import GuardedSessionService


ServiceFactory = Callable[[BackendProfile, ResourceType], Any]


class TenantStorageResolver:
    """Build bundles while retaining profile-scoped services until drain."""

    def __init__(self, profiles: BackendProfileRegistry, service_factory: ServiceFactory) -> None:
        self._profiles = profiles
        self._service_factory = service_factory
        self._services: dict[tuple[str, int, ResourceType], Any] = {}
        self._bundle_refs: dict[tuple[str, str, int, int], int] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def resolve(
        self,
        *,
        tenant_id: str,
        agent_app_id: str,
        config_version: int,
        storage_revision: int,
        bindings: list[TenantBackendBinding] | tuple[TenantBackendBinding, ...],
        production: bool = False,
        worker_count: int = 1,
    ) -> TenantStorageBundle:
        if self._closed:
            raise RuntimeError("storage resolver is closed")
        matching = [
            item for item in bindings
            if item.tenant_id == tenant_id and item.agent_app_id == agent_app_id
            and item.config_version == config_version and item.storage_revision == storage_revision
        ]
        if not matching:
            raise LookupError(f"storage revision {storage_revision} not found for tenant application config")
        resolved = validate_tenant_bindings(
            tenant_id,
            matching,
            self._profiles,
            production=production,
            worker_count=worker_count,
        )
        bundle_key = (tenant_id, agent_app_id, config_version, storage_revision)
        services: dict[str, TenantScopedService] = {}
        async with self._lock:
            self._bundle_refs[bundle_key] = self._bundle_refs.get(bundle_key, 0) + 1
            for resource, (binding, profile) in resolved.items():
                # Summary is owned by the Session service in tRPC-Agent.  Reuse
                # the exact same backend object so its watermark cannot diverge
                # from the session event stream.
                factory_resource = ResourceType.SESSION if resource is ResourceType.SUMMARY else resource
                service_key = (profile.profile_id, profile.profile_revision, factory_resource)
                if service_key not in self._services:
                    service = self._service_factory(profile, factory_resource)
                    if inspect.isawaitable(service):
                        service = await service
                    self._services[service_key] = service
                raw_service = self._services[service_key]
                scoped_service = raw_service
                if resource in {ResourceType.SESSION, ResourceType.SUMMARY}:
                    scoped_service = GuardedSessionService(raw_service, tenant_id, agent_app_id)
                elif resource is ResourceType.MEMORY:
                    scoped_service = GuardedMemoryService(raw_service, tenant_id, agent_app_id)
                services[resource.value] = TenantScopedService(
                    tenant_id,
                    binding.namespace,
                    raw_service,
                    scoped_service,
                )

        async def release() -> None:
            async with self._lock:
                current = self._bundle_refs.get(bundle_key, 0)
                if current <= 1:
                    self._bundle_refs.pop(bundle_key, None)
                else:
                    self._bundle_refs[bundle_key] = current - 1

        return TenantStorageBundle(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            config_version=config_version,
            storage_revision=storage_revision,
            services=services,
            _release=release,
        )

    async def close(self) -> None:
        async with self._lock:
            if self._bundle_refs:
                raise RuntimeError("cannot close storage resolver while bundles are in flight")
            self._closed = True
            services = list(self._services.values())
            self._services.clear()
        for service in services:
            close = getattr(service, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result
