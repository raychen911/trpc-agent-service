"""Strict adapter from durable outbox records to SessionReady v2 notices."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from trpc_service.queue.session_ready import SessionReady, SessionReadyQueue
from trpc_service.storage.models import OutboxRecord

SESSION_READY_EVENT_V2 = "session.ready.v2"


class SessionReadyPublisher(Protocol):
    async def ensure_group(self) -> None: ...

    async def publish(self, message: SessionReady) -> str: ...


class SessionReadyOutboxQueue:
    """Expose the generic outbox publisher contract for SessionReady notices.

    PostgreSQL supplies the authoritative tenant, session and generation.  The
    adapter deliberately ignores arbitrary payload fields so provider content
    can never leak into Redis through the v2 scheduler path.
    """

    def __init__(self, queue: SessionReadyQueue | SessionReadyPublisher) -> None:
        self._queue = queue

    async def ensure_group(self) -> None:
        await self._queue.ensure_group()

    async def publish(self, record: OutboxRecord) -> str:
        if record.event_type != SESSION_READY_EVENT_V2:
            raise ValueError("outbox record is not a SessionReady v2 event")
        payload = record.payload
        generation = _required_int(payload, "generation", minimum=1)
        priority = _required_int(payload, "priority", minimum=0)
        trace_id = _required_text(payload, "trace_id")
        created_at = _required_datetime(payload, "created_at")
        return await self._queue.publish(
            SessionReady(
                event_id=record.outbox_id,
                tenant_id=record.tenant_id,
                session_id=record.aggregate_id,
                generation=generation,
                priority=priority,
                trace_id=trace_id,
                created_at=created_at,
            )
        )


def _required_text(payload: dict[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"SessionReady payload requires non-empty {name}")
    return value


def _required_int(payload: dict[str, Any], name: str, *, minimum: int) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"SessionReady payload requires {name} >= {minimum}")
    return value


def _required_datetime(payload: dict[str, Any], name: str) -> datetime:
    value = payload.get(name)
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"SessionReady payload requires {name}")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"SessionReady payload has invalid {name}") from exc


__all__ = ["SESSION_READY_EVENT_V2", "SessionReadyOutboxQueue"]
