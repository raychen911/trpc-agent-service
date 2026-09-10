"""Single-use dangerous-operation confirmations."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import uuid4

from trpc_service.governance.errors import ConfirmationConsumed, ConfirmationExpired, ConfirmationInvalid
from trpc_service.governance.models import ConfirmationClaim, ConfirmationIntent, ConfirmationStatus, PendingConfirmation


def create_pending(*, tenant_id: str, channel: str, binding_id: str, principal_digest: str, session_id: str, tool_name: str, arguments_digest: str, policy_version: int, reservation_id: str, now: datetime, ttl_seconds: int) -> PendingConfirmation:
    opaque = uuid4().hex
    token_digest = sha256(opaque.encode()).hexdigest()
    return PendingConfirmation.new(
        tenant_id=tenant_id, confirmation_id=uuid4().hex, reservation_id=reservation_id,
        channel=channel, binding_id=binding_id, principal_digest=principal_digest,
        session_id=session_id, tool_name=tool_name, arguments_digest=arguments_digest,
        policy_version=policy_version, token_digest=token_digest,
        expires_at=now + timedelta(seconds=ttl_seconds),
    )


def intent_for(pending: PendingConfirmation) -> ConfirmationIntent:
    return ConfirmationIntent(
        tenant_id=pending.tenant_id, channel=pending.channel, binding_id=pending.binding_id,
        principal_digest=pending.principal_digest, session_id=pending.session_id,
        confirmation_id=pending.confirmation_id, token_digest=pending.token_digest,
    )


def consume(pending: PendingConfirmation, *, token: str, now: datetime) -> PendingConfirmation:
    if pending.expires_at is not None and now >= pending.expires_at:
        raise ConfirmationExpired()
    if pending.status != ConfirmationStatus.PENDING:
        raise ConfirmationConsumed()
    if token != pending.token_digest:
        raise ConfirmationInvalid()
    return pending.model_copy(update={"status": ConfirmationStatus.CLAIMED})


class InMemoryConfirmationRepository:
    def __init__(self) -> None:
        self._items: dict[str, PendingConfirmation] = {}
        self._lock = asyncio.Lock()

    async def create_once(self, pending: PendingConfirmation) -> PendingConfirmation:
        async with self._lock:
            return self._items.setdefault(pending.confirmation_id, pending)

    async def claim(self, *, intent: ConfirmationIntent, owner_node_id: str, owner_generation: int, now: datetime) -> ConfirmationClaim | None:
        del owner_node_id, owner_generation
        async with self._lock:
            pending = self._items.get(intent.confirmation_id)
            if pending is None or pending.token_digest != intent.token_digest:
                raise ConfirmationInvalid()
            if pending.status != ConfirmationStatus.PENDING:
                return None
            updated = consume(pending, token=intent.token_digest, now=now)
            self._items[pending.confirmation_id] = updated
            return ConfirmationClaim(confirmation=updated, claim_token=uuid4().hex)

    async def mark_executing(self, *, confirmation_id: str, claim_token: str, owner_generation: int) -> PendingConfirmation:
        del claim_token, owner_generation
        async with self._lock:
            item = self._items[confirmation_id]
            if item.status != ConfirmationStatus.CLAIMED:
                raise ConfirmationConsumed()
            updated = item.model_copy(update={"status": ConfirmationStatus.EXECUTING})
            self._items[confirmation_id] = updated
            return updated

    async def complete(self, *, confirmation_id: str, claim_token: str, owner_generation: int, result_digest: str) -> PendingConfirmation:
        del claim_token, owner_generation, result_digest
        async with self._lock:
            item = self._items[confirmation_id]
            if item.status == ConfirmationStatus.COMPLETED:
                return item
            if item.status != ConfirmationStatus.EXECUTING:
                raise ConfirmationConsumed()
            updated = item.model_copy(update={"status": ConfirmationStatus.COMPLETED})
            self._items[confirmation_id] = updated
            return updated

    async def cancel(self, *, confirmation_id: str, reason: str) -> PendingConfirmation:
        del reason
        async with self._lock:
            item = self._items[confirmation_id]
            if item.status in {ConfirmationStatus.COMPLETED, ConfirmationStatus.EXECUTING}:
                return item
            updated = item.model_copy(update={"status": ConfirmationStatus.CANCELLED})
            self._items[confirmation_id] = updated
            return updated
