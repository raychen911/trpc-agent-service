"""Tenant-and-agent scoped official SDK session services."""

from __future__ import annotations

from trpc_agent_sdk.sessions import InMemorySessionService


class SessionBackendFactory:
    def __init__(self) -> None:
        self._backends: dict[tuple[str, str], InMemorySessionService] = {}
        self.closed = False

    def get_backend(self, tenant_id: str, agent_id: str) -> InMemorySessionService:
        if self.closed:
            raise RuntimeError("session backend factory is closed")
        return self._backends.setdefault((tenant_id, agent_id), InMemorySessionService())

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for backend in self._backends.values():
            await backend.close()
        self._backends.clear()
