"""Publish durable PostgreSQL outbox rows to a derived message queue."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from ..tenant import InboundMessage, inbound_from_dict


@dataclass(frozen=True)
class OutboxRecord:
    tenant_id: str
    outbox_id: str
    event_type: str
    payload: dict[str, Any]
    attempts: int
    created_at: datetime


class OutboxRepository(Protocol):
    async def claim_outbox(self, owner_id: str, limit: int, lease_seconds: int) -> list[OutboxRecord]: ...

    async def mark_outbox_published(self, record: OutboxRecord, owner_id: str) -> None: ...

    async def release_outbox(self, record: OutboxRecord, owner_id: str, error_type: str) -> None: ...


class MessagePublisher(Protocol):
    async def put(self, message: InboundMessage) -> str: ...


class OutboxDispatcher:
    def __init__(
        self,
        repository: OutboxRepository,
        queue: MessagePublisher,
        owner_id: str,
        *,
        handlers: dict[str, Callable[[OutboxRecord], Awaitable[None]]] | None = None,
        reconcile_interval_seconds: float = 30.0,
    ) -> None:
        self.repository = repository
        self.queue = queue
        self.owner_id = owner_id
        self.handlers = handlers or {}
        self.reconcile_interval_seconds = max(1.0, reconcile_interval_seconds)
        self._last_reconcile = 0.0

    async def dispatch_once(self, limit: int = 100) -> int:
        reconcile = getattr(self.repository, "reconcile_inbound", None)
        now = time.monotonic()
        if reconcile is not None and now - self._last_reconcile >= self.reconcile_interval_seconds:
            await reconcile(limit=limit)
            self._last_reconcile = now
        records = await self.repository.claim_outbox(self.owner_id, limit, 30)
        published = 0
        for record in records:
            try:
                if record.event_type == "inbound.accepted":
                    await self.queue.put(inbound_from_dict(record.payload["message"]))
                elif record.event_type in self.handlers:
                    await self.handlers[record.event_type](record)
                else:
                    raise ValueError(f"unsupported outbox event: {record.event_type}")
                await self.repository.mark_outbox_published(record, self.owner_id)
                published += 1
            except Exception as exc:  # noqa: BLE001 - every publish failure must release its durable claim
                await self.repository.release_outbox(record, self.owner_id, type(exc).__name__)
        return published

    async def run(self, stop: asyncio.Event, poll_seconds: float = 0.25) -> None:
        while not stop.is_set():
            try:
                count = await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - dependency outages are retried by the durable loop
                count = 0
            if not count:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                except asyncio.TimeoutError:
                    pass
