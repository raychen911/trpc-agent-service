"""Runtime-neutral agent engine contract."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from tenant_agent.models import AgentEvent, RoutedEnvelope, TenantConfig
from tenant_agent.storage.base import TenantDataPlane


class AgentEngine(Protocol):
    async def stream(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        effective_text: str,
        plane: TenantDataPlane,
    ) -> AsyncIterator[AgentEvent]: ...

    async def close(self) -> None: ...
