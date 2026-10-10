"""Narrow adapters between SessionReady delivery and the Agent worker."""

from __future__ import annotations

from datetime import timedelta

from trpc_service.agent.worker import AgentWorker
from trpc_service.queue.session_ready import SessionReady
from trpc_service.storage.models import SessionClaim
from trpc_service.storage.protocols import RuntimeRepository


class MailboxReadyClaimer:
    """Claim one authoritative PostgreSQL Session for a Redis wake-up."""

    def __init__(
        self,
        repository: RuntimeRepository,
        *,
        owner_id: str,
        lease_for: timedelta,
    ) -> None:
        self._repository = repository
        self._owner_id = owner_id
        self._lease_for = lease_for

    async def claim(self, message: SessionReady) -> SessionClaim:
        return await self._repository.claim_session_ready(
            message.tenant_id,
            message.session_id,
            owner_id=self._owner_id,
            lease_for=self._lease_for,
            expected_generation=message.generation,
            expected_event_id=message.event_id,
        )


class MailboxClaimExecutor:
    """Execute a claim that already owns its PostgreSQL mailbox lease."""

    def __init__(self, worker: AgentWorker) -> None:
        self._worker = worker

    async def execute(self, claim: SessionClaim) -> None:
        await self._worker.process_claimed(claim)


__all__ = ["MailboxClaimExecutor", "MailboxReadyClaimer"]
