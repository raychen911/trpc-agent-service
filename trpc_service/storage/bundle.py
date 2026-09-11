"""Tenant-scoped views over shared backend services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable


@dataclass(frozen=True)
class TenantScopedService:
    tenant_id: str
    namespace: str
    raw_service: Any
    scoped_service: Any | None = None

    def assert_tenant(self, tenant_id: str) -> None:
        if tenant_id != self.tenant_id:
            from .errors import TenantBoundaryError

            raise TenantBoundaryError("service tenant scope does not match the request")

    def __getattr__(self, name: str) -> Any:
        return getattr(self.scoped_service or self.raw_service, name)


@dataclass
class TenantStorageBundle:
    tenant_id: str
    agent_app_id: str
    config_version: int
    storage_revision: int
    services: dict[str, TenantScopedService]
    _release: Callable[[], Awaitable[None]]
    _released: bool = False

    def __getattr__(self, name: str) -> TenantScopedService:
        try:
            return self.services[name]
        except KeyError as error:
            raise AttributeError(name) from error

    async def release(self) -> None:
        if not self._released:
            self._released = True
            await self._release()
