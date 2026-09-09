# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Outbound delivery state machine and local reference store."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Protocol

from pydantic import BaseModel
from pydantic import Field

from trpc_service.gateway.models import OutboundMessage
from trpc_service._compat import StrEnum


class OutboxState(StrEnum):
    PENDING = "pending"
    SENDING = "sending"
    DELIVERED = "delivered"
    RETRY = "retry"
    DEAD = "dead"
    UNKNOWN = "unknown"


class OutboxRecord(BaseModel):
    message: OutboundMessage
    state: OutboxState = OutboxState.PENDING
    attempts: int = 0
    next_attempt_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_error: str = ""
    locked_by: str = ""
    locked_at: datetime | None = None
    external_message_id: str = ""


class OutboxStore(Protocol):

    async def exists(self, outbound_id: str) -> bool:
        """Return whether the deterministic outbound ID has been persisted."""

    async def get(self, outbound_id: str) -> OutboxRecord | None:
        """Return a persisted outbound result for replay repair."""

    async def add(self, message: OutboundMessage) -> bool:
        """Insert exactly once by outbound_id."""

    async def claim(self, limit: int = 100, *, binding_ids: list[str] | None = None) -> list[OutboxRecord]:
        """Atomically claim due records."""

    async def delivered(self, outbound_id: str, external_message_id: str = "") -> None:
        """Mark delivery complete."""

    async def unknown(self, outbound_id: str, error: str) -> None:
        """Stop automatic redelivery when the remote outcome is ambiguous."""

    async def failed(self,
                     outbound_id: str,
                     error: str,
                     *,
                     retryable: bool,
                     max_attempts: int = 8,
                     retry_after_seconds: float = 0) -> None:
        """Move a record to retry or dead state."""


class InMemoryOutboxStore:
    """Process-local Outbox used by examples and deterministic tests."""

    def __init__(self) -> None:
        self._records: dict[str, OutboxRecord] = {}
        self._lock = asyncio.Lock()

    async def exists(self, outbound_id: str) -> bool:
        async with self._lock:
            return outbound_id in self._records

    async def add(self, message: OutboundMessage) -> bool:
        async with self._lock:
            if message.outbound_id in self._records:
                return False
            self._records[message.outbound_id] = OutboxRecord(message=message.model_copy(deep=True))
            return True

    async def claim(self, limit: int = 100, *, binding_ids: list[str] | None = None) -> list[OutboxRecord]:
        now = datetime.now(timezone.utc)
        async with self._lock:
            selected = [
                record for record in self._records.values()
                if record.state in {OutboxState.PENDING, OutboxState.RETRY} and record.next_attempt_at <= now and (
                    binding_ids is None or record.message.binding_id in binding_ids)
            ][:limit]
            for record in selected:
                record.state = OutboxState.SENDING
                record.attempts += 1
            return [record.model_copy(deep=True) for record in selected]

    async def delivered(self, outbound_id: str, external_message_id: str = "") -> None:
        async with self._lock:
            record = self._records[outbound_id]
            record.state = OutboxState.DELIVERED
            record.external_message_id = external_message_id

    async def unknown(self, outbound_id, error):
        async with self._lock:
            record = self._records[outbound_id]
            record.state = OutboxState.UNKNOWN
            record.last_error = error

    async def failed(self,
                     outbound_id: str,
                     error: str,
                     *,
                     retryable: bool,
                     max_attempts: int = 8,
                     retry_after_seconds: float = 0) -> None:
        async with self._lock:
            record = self._records[outbound_id]
            record.last_error = error[:512]
            if not retryable or record.attempts >= max_attempts:
                record.state = OutboxState.DEAD
                return
            record.state = OutboxState.RETRY
            delay = retry_after_seconds or min(300, 2**record.attempts)
            record.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

    async def get(self, outbound_id: str) -> OutboxRecord | None:
        record = self._records.get(outbound_id)
        return record.model_copy(deep=True) if record else None


class PostgresOutboxStore:
    """Crash-recoverable Outbox using asyncpg row leases and SKIP LOCKED."""

    def __init__(self, pool: Any, *, worker_id: str | None = None, lease_seconds: int = 60) -> None:
        self._pool = pool
        self._worker_id = worker_id or f"delivery-{uuid.uuid4().hex[:12]}"
        self._lease_seconds = max(5, lease_seconds)

    async def exists(self, outbound_id: str) -> bool:
        return bool(await self._pool.fetchval("SELECT EXISTS(SELECT 1 FROM outbound_message WHERE outbound_id=$1)",
                                              outbound_id))

    async def unknown(self, outbound_id, error):
        await self._pool.execute(
            "UPDATE outbound_message SET state='unknown',last_error_code=$3,locked_by=NULL,locked_at=NULL "
            "WHERE outbound_id=$1 AND state='sending' AND locked_by=$2", outbound_id, self._worker_id, error)

    async def get(self, outbound_id: str) -> OutboxRecord | None:
        row = await self._pool.fetchrow("SELECT * FROM outbound_message WHERE outbound_id=$1", outbound_id)
        return self._from_row(row) if row else None

    async def add(self, message: OutboundMessage) -> bool:
        status = await self._pool.execute(
            """
            INSERT INTO outbound_message
                (outbound_id, tenant_id, binding_id, request_id, payload, state)
            VALUES ($1,$2,$3,$4,$5::jsonb,'pending')
            ON CONFLICT (outbound_id) DO NOTHING
            """,
            message.outbound_id,
            message.tenant_id,
            message.binding_id,
            message.request_id,
            message.model_dump_json(),
        )
        return status.endswith("1")

    async def claim(self, limit: int = 100, *, binding_ids: list[str] | None = None) -> list[OutboxRecord]:
        rows = await self._pool.fetch(
            """
            WITH due AS (
                SELECT outbound_id
                  FROM outbound_message
                 WHERE state IN ('pending','retry')
                   AND next_attempt_at <= now()
                   AND ($3::text[] IS NULL OR binding_id=ANY($3::text[]))
                 ORDER BY next_attempt_at, created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT $1
            )
            UPDATE outbound_message AS message
               SET state='sending', attempts=attempts+1,
                   locked_by=$2, locked_at=now()
              FROM due
             WHERE message.outbound_id=due.outbound_id
         RETURNING message.*
            """,
            limit,
            self._worker_id,
            binding_ids,
        )
        return [self._from_row(row) for row in rows]

    async def delivered(self, outbound_id: str, external_message_id: str = "") -> None:
        await self._pool.execute(
            """
            UPDATE outbound_message
               SET state='delivered', delivered_at=now(), external_message_id=$3,
                   locked_by=NULL, locked_at=NULL, last_error_code=NULL
             WHERE outbound_id=$1 AND state='sending' AND locked_by=$2
            """,
            outbound_id,
            self._worker_id,
            external_message_id or None,
        )

    async def failed(self,
                     outbound_id: str,
                     error: str,
                     *,
                     retryable: bool,
                     max_attempts: int = 8,
                     retry_after_seconds: float = 0) -> None:
        await self._pool.execute(
            """
            UPDATE outbound_message
               SET state=CASE WHEN $4 AND attempts < $5 THEN 'retry' ELSE 'dead' END,
                   next_attempt_at=CASE WHEN $4 AND attempts < $5
                       THEN now() + make_interval(secs => CASE WHEN $6 > 0 THEN $6
                            ELSE LEAST(300, power(2, attempts)) END)
                       ELSE next_attempt_at END,
                   last_error_code=$3, locked_by=NULL, locked_at=NULL
             WHERE outbound_id=$1 AND state='sending' AND locked_by=$2
            """,
            outbound_id,
            self._worker_id,
            error[:128],
            retryable,
            max_attempts,
            retry_after_seconds,
        )

    async def recover_expired(self) -> int:
        status = await self._pool.execute(
            """
            UPDATE outbound_message
               SET state='retry', locked_by=NULL, locked_at=NULL,
                   next_attempt_at=now(), last_error_code='delivery_lease_expired'
             WHERE state='sending'
               AND locked_at < now() - make_interval(secs => $1)
            """,
            self._lease_seconds,
        )
        return int(status.rsplit(" ", 1)[-1])

    async def ping(self) -> bool:
        return bool(await self._pool.fetchval("SELECT 1"))

    @staticmethod
    def _from_row(row: Any) -> OutboxRecord:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return OutboxRecord(
            message=OutboundMessage.model_validate(payload),
            state=row["state"],
            attempts=row["attempts"],
            next_attempt_at=row["next_attempt_at"],
            last_error=row["last_error_code"] or "",
            locked_by=row["locked_by"] or "",
            locked_at=row["locked_at"],
            external_message_id=row["external_message_id"] or "",
        )
