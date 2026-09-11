"""Tenant guard for the public tRPC MemoryService interface."""

from __future__ import annotations

from trpc_agent_sdk.abc import MemoryServiceABC

from .errors import TenantBoundaryError
from trpc_service.telemetry.runtime import measured


class GuardedMemoryService(MemoryServiceABC):
    def __init__(self, service: MemoryServiceABC, tenant_id: str, agent_app_id: str) -> None:
        super().__init__(enabled=service.enabled)
        self._service = service
        self.tenant_id = tenant_id
        self.app_name = f"{tenant_id}:{agent_app_id}"

    def _validate_context(self, agent_context) -> None:
        if agent_context is not None and agent_context.metadata.get("tenant_id") not in {None, self.tenant_id}:
            raise TenantBoundaryError("AgentContext tenant does not match memory scope")

    @measured("memory.store")
    async def store_session(self, session, agent_context=None) -> None:
        if session.app_name != self.app_name:
            raise TenantBoundaryError("memory session is outside the tenant application scope")
        self._validate_context(agent_context)
        if agent_context is not None:
            scope = agent_context.metadata.get("memory_scope_id")
            if scope is not None and session.user_id != scope:
                raise TenantBoundaryError("memory session does not match the conversation scope")
        await self._service.store_session(session, agent_context=agent_context)

    @measured("memory.search")
    async def search_memory(self, key, query, limit=10, agent_context=None):
        expected_prefix = f"{self.app_name}/"
        if not key.startswith(expected_prefix):
            raise TenantBoundaryError("memory key is outside the tenant application scope")
        self._validate_context(agent_context)
        if agent_context is not None:
            scope = agent_context.metadata.get("memory_scope_id")
            if scope is not None and key != f"{self.app_name}/{scope}":
                raise TenantBoundaryError("memory key does not match the conversation scope")
        return await self._service.search_memory(key, query, limit=limit, agent_context=agent_context)

    async def close(self) -> None:
        return None
