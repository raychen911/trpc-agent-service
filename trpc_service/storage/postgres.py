"""PostgreSQL authority for Inbox/Outbox, sessions, memory and audit."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..queue.outbox import OutboxRecord
from ..tenant import InboundMessage, SessionEvent, SessionSnapshot, inbound_from_dict, inbound_to_dict
from .artifacts import Artifact
from .models import FencingConflict, LeaseBusy, SessionLease


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


class _LazyAcquire:
    def __init__(self, owner: LazyAsyncpgPool) -> None:
        self.owner = owner
        self.context: Any = None

    async def __aenter__(self) -> Any:
        pool = await self.owner.start()
        self.context = pool.acquire()
        return await self.context.__aenter__()

    async def __aexit__(self, *args: object) -> None:
        await self.context.__aexit__(*args)


class LazyAsyncpgPool:
    """Create asyncpg connections only after the ASGI event loop starts."""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10) -> None:
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Any = None
        self._lock = asyncio.Lock()

    async def start(self) -> Any:
        if self._pool is not None:
            return self._pool
        async with self._lock:
            if self._pool is None:
                import asyncpg

                self._pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    command_timeout=30,
                )
        return self._pool

    def acquire(self) -> _LazyAcquire:
        return _LazyAcquire(self)

    async def fetch(self, query: str, *args: Any) -> Any:
        return await (await self.start()).fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return await (await self.start()).fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return await (await self.start()).fetchval(query, *args)

    async def execute(self, query: str, *args: Any) -> Any:
        return await (await self.start()).execute(query, *args)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


class PostgresRepository:
    """One shared pool implementing the production persistence contracts."""

    def __init__(self, pool: Any, *, control_pool: Any | None = None, lease_seconds: int = 60) -> None:
        self.pool = pool
        self.control_pool = control_pool or pool
        self.lease_seconds = lease_seconds

    @classmethod
    async def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 10) -> PostgresRepository:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size, command_timeout=30)
        return cls(pool)

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        *,
        control_dsn: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
    ) -> PostgresRepository:
        return cls(
            LazyAsyncpgPool(dsn, min_size, max_size),
            control_pool=LazyAsyncpgPool(control_dsn, min_size, max_size) if control_dsn else None,
        )

    async def close(self) -> None:
        await self.pool.close()
        if self.control_pool is not self.pool:
            await self.control_pool.close()

    async def ping(self) -> bool:
        return bool(await self.pool.fetchval("SELECT 1"))

    @asynccontextmanager
    async def _scoped(self, tenant_id: str) -> AsyncIterator[Any]:
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id',$1,true)", tenant_id)
            yield connection

    @asynccontextmanager
    async def _control(self) -> AsyncIterator[Any]:
        """Use the separately granted control plane role for cross-tenant work."""
        async with self.control_pool.acquire() as connection, connection.transaction():
            yield connection

    async def migrate(self, path: str | Path | None = None) -> None:
        candidates = (
            Path(path),
        ) if path else (
            Path(__file__).parents[2] / "migrations" / "0001_production.sql",
            Path(sys.prefix) / "share" / "trpc-agent-service" / "0001_production.sql",
        )
        migration = next((candidate for candidate in candidates if candidate.exists()), None)
        if migration is None:
            raise FileNotFoundError("production migration SQL is not installed")
        sql = migration.read_text(encoding="utf-8")
        async with self.pool.acquire() as connection:
            await connection.execute(sql)

    async def accept(self, message: InboundMessage) -> bool:
        """Atomically insert the Inbox row and matching unpublished Outbox row."""
        inbox_key = f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}"
        outbox_id = uuid4().hex
        async with self._scoped(message.tenant_id) as connection:
            inserted = await connection.fetchval(
                """
                INSERT INTO inbound_messages(
                    tenant_id, inbox_key, channel, account_id, external_message_id,
                    session_id, trace_id, payload_json, status
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,'accepted')
                ON CONFLICT (tenant_id, channel, account_id, external_message_id) DO NOTHING
                RETURNING inbox_key
                """,
                message.tenant_id,
                inbox_key,
                message.channel,
                message.account_id,
                message.external_message_id,
                message.session_id,
                message.trace_id,
                _json(inbound_to_dict(message)),
            )
            if not inserted:
                return False
            await connection.execute(
                """
                INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                VALUES($1,$2,'inbound.accepted',$3,$4::jsonb,$5)
                """,
                message.tenant_id,
                outbox_id,
                message.session_id,
                _json({"inbox_key": inbox_key, "message": inbound_to_dict(message)}),
                message.trace_id,
            )
        return True

    async def claim(self, key: str) -> bool:
        """Compatibility claim for callers that do not use transactional accept."""
        tenant_id = key.partition(":")[0]
        async with self._scoped(tenant_id) as connection:
            value = await connection.fetchval(
                """
                INSERT INTO idempotency_keys(tenant_id,idempotency_key,status)
                VALUES($1,$2,'claimed') ON CONFLICT DO NOTHING RETURNING idempotency_key
                """,
                tenant_id,
                key,
            )
        return value is not None

    async def complete(self, key: str, result: Any) -> None:
        tenant_id = key.partition(":")[0]
        status = str(result.get("status", "completed")) if isinstance(result, dict) else "completed"
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO idempotency_keys(tenant_id,idempotency_key,status,result_json)
                VALUES($1,$2,$3,$4::jsonb)
                ON CONFLICT (tenant_id,idempotency_key) DO UPDATE
                SET status=EXCLUDED.status, result_json=EXCLUDED.result_json, updated_at=now()
                """,
                tenant_id,
                key,
                status,
                _json(result),
            )
            await connection.execute(
                "UPDATE inbound_messages SET status=$3,result_json=$4::jsonb,updated_at=now() "
                "WHERE tenant_id=$1 AND inbox_key=$2",
                tenant_id,
                key,
                status,
                _json(result),
            )

    async def release(self, key: str) -> None:
        tenant_id = key.partition(":")[0]
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                "DELETE FROM idempotency_keys WHERE tenant_id=$1 AND idempotency_key=$2 AND status='claimed'",
                tenant_id,
                key,
            )

    async def result(self, key: str) -> Any:
        tenant_id = key.partition(":")[0]
        async with self._scoped(tenant_id) as connection:
            value = await connection.fetchval(
                "SELECT result_json FROM idempotency_keys WHERE tenant_id=$1 AND idempotency_key=$2",
                tenant_id,
                key,
            )
            if value is None:
                value = await connection.fetchval(
                    "SELECT result_json FROM inbound_messages WHERE tenant_id=$1 AND inbox_key=$2", tenant_id, key
                )
        return _object(value) if value is not None else None

    async def contains(self, key: str) -> bool:
        tenant_id = key.partition(":")[0]
        async with self._scoped(tenant_id) as connection:
            value = await connection.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM inbound_messages WHERE tenant_id=$1 AND inbox_key=$2
                    UNION ALL
                    SELECT 1 FROM idempotency_keys WHERE tenant_id=$1 AND idempotency_key=$2
                )
                """,
                tenant_id,
                key,
            )
        return bool(value)

    async def fail(self, key: str, error_type: str) -> None:
        tenant_id = key.partition(":")[0]
        async with self._scoped(tenant_id) as connection:
            current = await connection.fetchrow(
                """
                SELECT status,result_json FROM inbound_messages
                WHERE tenant_id=$1 AND inbox_key=$2 FOR UPDATE
                """,
                tenant_id,
                key,
            )
            if current is not None and current["status"] == "completed":
                return
            previous = _object(current["result_json"]) if current is not None else {}
            status = "delivery_failed" if current is not None and current["status"] == "prepared" else "failed"
            result = {**previous, "status": status, "error_type": error_type}
            await connection.execute(
                """
                INSERT INTO idempotency_keys(tenant_id,idempotency_key,status,result_json)
                VALUES($1,$2,$3,$4::jsonb)
                ON CONFLICT (tenant_id,idempotency_key) DO UPDATE
                SET status=EXCLUDED.status,result_json=EXCLUDED.result_json,updated_at=now()
                WHERE idempotency_keys.status <> 'completed'
                """,
                tenant_id,
                key,
                status,
                _json(result),
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status=$3,result_json=$4::jsonb,updated_at=now()
                WHERE tenant_id=$1 AND inbox_key=$2 AND status <> 'completed'
                """,
                tenant_id,
                key,
                status,
                _json(result),
            )

    async def query_failed_inbound(self, tenant_id: str, limit: int = 100) -> list[dict[str, Any]]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT tenant_id,inbox_key,channel,account_id,external_message_id,status,result_json,
                       received_at,updated_at
                FROM inbound_messages
                WHERE tenant_id=$1 AND status IN ('failed','delivery_failed','queue_failed')
                ORDER BY updated_at DESC LIMIT $2
                """,
                tenant_id,
                max(1, min(limit, 1000)),
            )
        return [dict(row) for row in rows]

    async def replay_inbound(self, tenant_id: str, inbox_key: str) -> bool:
        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT session_id,trace_id,payload_json,status,result_json
                FROM inbound_messages WHERE tenant_id=$1 AND inbox_key=$2
                  AND status IN ('failed','delivery_failed','queue_failed') FOR UPDATE
                """,
                tenant_id,
                inbox_key,
            )
            if row is None:
                return False
            prepared = row["status"] == "delivery_failed" and bool(_object(row["result_json"]).get("text"))
            status = "prepared" if prepared else "accepted"
            result = _object(row["result_json"]) if prepared else None
            if result is not None:
                result = {**result, "status": "prepared", "error_type": None}
            await connection.execute(
                """
                UPDATE inbound_messages SET status=$3,result_json=$4::jsonb,updated_at=now()
                WHERE tenant_id=$1 AND inbox_key=$2
                """,
                tenant_id,
                inbox_key,
                status,
                _json(result) if result is not None else None,
            )
            await connection.execute(
                """
                INSERT INTO idempotency_keys(tenant_id,idempotency_key,status,result_json)
                VALUES($1,$2,$3,$4::jsonb)
                ON CONFLICT (tenant_id,idempotency_key) DO UPDATE
                SET status=EXCLUDED.status,result_json=EXCLUDED.result_json,updated_at=now()
                """,
                tenant_id,
                inbox_key,
                status,
                _json(result) if result is not None else None,
            )
            await connection.execute(
                """
                INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                VALUES($1,$2,'inbound.accepted',$3,$4::jsonb,$5)
                """,
                tenant_id,
                uuid4().hex,
                row["session_id"],
                _json({"inbox_key": inbox_key, "message": _object(row["payload_json"])}),
                row["trace_id"],
            )
        return True

    async def claim_outbox(self, owner_id: str, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        async with self._control() as connection:
            rows = await connection.fetch(
            """
            WITH picked AS (
                SELECT tenant_id,outbox_id FROM outbox_events
            WHERE published_at IS NULL AND dead_lettered_at IS NULL AND available_at <= now()
                  AND (claim_expires_at IS NULL OR claim_expires_at <= now())
                ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $2
            )
            UPDATE outbox_events o SET claimed_by=$1,
                claim_expires_at=now()+make_interval(secs => $3), attempts=o.attempts+1
            FROM picked p WHERE o.tenant_id=p.tenant_id AND o.outbox_id=p.outbox_id
            RETURNING o.*
            """,
            owner_id,
            max(1, min(limit, 1000)),
            lease_seconds,
            )
        return [
            OutboxRecord(
                row["tenant_id"],
                row["outbox_id"],
                row["event_type"],
                _object(row["payload_json"]),
                row["attempts"],
                row["created_at"],
            )
            for row in rows
        ]

    async def mark_outbox_published(self, record: OutboxRecord, owner_id: str) -> None:
        async with self._scoped(record.tenant_id) as connection:
            result = await connection.execute(
                """
                UPDATE outbox_events SET published_at=now(),claimed_by=NULL,claim_expires_at=NULL
                WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3 AND published_at IS NULL
                """,
                record.tenant_id,
                record.outbox_id,
                owner_id,
            )
            if result == "UPDATE 1" and record.event_type == "inbound.accepted":
                message = inbound_from_dict(record.payload["message"])
                inbox_key = str(record.payload.get("inbox_key") or (
                    f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}"
                ))
                await connection.execute(
                    """
                    UPDATE inbound_messages SET status='queued',updated_at=now()
                    WHERE tenant_id=$1 AND inbox_key=$2 AND status IN ('accepted','queued')
                    """,
                    record.tenant_id,
                    inbox_key,
                )
        if result != "UPDATE 1":
            raise FencingConflict("outbox claim was lost")

    async def release_outbox(self, record: OutboxRecord, owner_id: str, error_type: str) -> None:
        if record.attempts >= 8:
            async with self._scoped(record.tenant_id) as connection:
                result = await connection.execute(
                    """
                    UPDATE outbox_events SET dead_lettered_at=now(),last_error=$4,claimed_by=NULL,claim_expires_at=NULL
                    WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                    """,
                    record.tenant_id,
                    record.outbox_id,
                    owner_id,
                    error_type,
                )
                if result != "UPDATE 1":
                    raise FencingConflict("outbox claim was lost")
                if record.event_type == "inbound.accepted":
                    message = inbound_from_dict(record.payload["message"])
                    inbox_key = str(record.payload.get("inbox_key") or (
                        f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}"
                    ))
                    await connection.execute(
                        """
                        UPDATE inbound_messages
                        SET status='queue_failed',result_json=$3::jsonb,updated_at=now()
                        WHERE tenant_id=$1 AND inbox_key=$2 AND status IN ('accepted','queued')
                        """,
                        record.tenant_id,
                        inbox_key,
                        _json({"status": "queue_failed", "error_type": error_type}),
                    )
            return
        async with self._scoped(record.tenant_id) as connection:
            result = await connection.execute(
                """
                UPDATE outbox_events SET available_at=now()+make_interval(secs => LEAST(60, power(2,attempts)::int)),
                    last_error=$4,claimed_by=NULL,claim_expires_at=NULL
                WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                """,
                record.tenant_id,
                record.outbox_id,
                owner_id,
                error_type,
            )
        if result != "UPDATE 1":
            raise FencingConflict("outbox claim was lost")

    async def reconcile_inbound(self, limit: int = 100, stale_seconds: int = 300) -> int:
        """Recreate queue notifications for accepted Inbox rows stranded after Redis loss."""
        async with self._control() as connection:
            rows = await connection.fetch(
                """
                WITH stranded AS (
                    SELECT i.tenant_id,i.inbox_key,i.session_id,i.trace_id,i.payload_json
                    FROM inbound_messages i
                    WHERE i.status IN ('accepted','queued')
                      AND i.updated_at <= now()-make_interval(secs => $2)
                      AND NOT EXISTS (
                        SELECT 1 FROM outbox_events o
                        WHERE o.tenant_id=i.tenant_id AND o.event_type='inbound.accepted'
                          AND o.payload_json->'message'->>'external_message_id'=i.external_message_id
                          AND o.payload_json->'message'->>'channel'=i.channel
                          AND o.payload_json->'message'->>'account_id'=i.account_id
                          AND o.published_at IS NULL AND o.dead_lettered_at IS NULL
                      )
                    ORDER BY i.updated_at FOR UPDATE SKIP LOCKED LIMIT $1
                )
                UPDATE inbound_messages i SET updated_at=now()
                FROM stranded s
                WHERE i.tenant_id=s.tenant_id AND i.inbox_key=s.inbox_key
                RETURNING s.tenant_id,s.session_id,s.trace_id,s.payload_json
                """,
                max(1, min(limit, 1000)),
                max(1, stale_seconds),
            )
            for row in rows:
                await connection.execute(
                    """
                    INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                    VALUES($1,$2,'inbound.accepted',$3,$4::jsonb,$5)
                    """,
                    row["tenant_id"],
                    uuid4().hex,
                    row["session_id"],
                    _json(
                        {
                            "inbox_key": (
                                f"{row['tenant_id']}:"
                                f"{_object(row['payload_json']).get('channel', '')}:"
                                f"{_object(row['payload_json']).get('account_id', '')}:"
                                f"{_object(row['payload_json']).get('external_message_id', '')}"
                            ),
                            "message": _object(row["payload_json"]),
                        }
                    ),
                    row["trace_id"],
                )
        return len(rows)

    async def query_outbox_dead_letters(self, tenant_id: str, limit: int = 100) -> list[dict[str, Any]]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT tenant_id,outbox_id,event_type,aggregate_id,attempts,last_error,
                       created_at,dead_lettered_at
                FROM outbox_events
                WHERE tenant_id=$1 AND dead_lettered_at IS NOT NULL
                ORDER BY dead_lettered_at DESC LIMIT $2
                """,
                tenant_id,
                max(1, min(limit, 1000)),
            )
        return [dict(row) for row in rows]

    async def replay_outbox(self, tenant_id: str, outbox_id: str) -> bool:
        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                UPDATE outbox_events
                SET attempts=0,available_at=now(),claimed_by=NULL,claim_expires_at=NULL,
                    dead_lettered_at=NULL,last_error=NULL
                WHERE tenant_id=$1 AND outbox_id=$2 AND dead_lettered_at IS NOT NULL
                RETURNING event_type,payload_json
                """,
                tenant_id,
                outbox_id,
            )
            if row is not None and row["event_type"] == "inbound.accepted":
                message = inbound_from_dict(_object(row["payload_json"])["message"])
                inbox_key = str(_object(row["payload_json"]).get("inbox_key") or (
                    f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}"
                ))
                await connection.execute(
                    """
                    UPDATE inbound_messages SET status='accepted',result_json=NULL,updated_at=now()
                    WHERE tenant_id=$1 AND inbox_key=$2 AND status='queue_failed'
                    """,
                    tenant_id,
                    inbox_key,
                )
        return row is not None

    async def get_or_create(
        self, tenant_id: str, session_id: str, app_id: str, user_id: str
    ) -> SessionSnapshot:
        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO sessions(tenant_id,session_id,app_id,user_id)
                VALUES($1,$2,$3,$4) ON CONFLICT (tenant_id,session_id) DO UPDATE SET session_id=EXCLUDED.session_id
                RETURNING tenant_id,session_id,app_id,user_id,state_json,version,updated_at
                """,
                tenant_id,
                session_id,
                app_id,
                user_id,
            )
        return self._snapshot(row)

    @asynccontextmanager
    async def lock(self, tenant_id: str, session_id: str) -> AsyncIterator[SessionLease]:
        owner_id = uuid4().hex
        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                UPDATE sessions SET lease_owner=$3,lease_expires_at=now()+make_interval(secs => $4),
                    fencing_epoch=fencing_epoch+1
                WHERE tenant_id=$1 AND session_id=$2
                  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
                RETURNING version,fencing_epoch,lease_expires_at
                """,
                tenant_id,
                session_id,
                owner_id,
                self.lease_seconds,
            )
        if row is None:
            raise LeaseBusy(f"session lease is held: {tenant_id}/{session_id}")
        lease = SessionLease(tenant_id, session_id, owner_id, row["fencing_epoch"], row["version"], row["lease_expires_at"])
        renew_stop = asyncio.Event()
        renew_task = asyncio.create_task(self._renew_lease(lease, renew_stop), name=f"session-lease:{session_id}")
        try:
            yield lease
        finally:
            renew_stop.set()
            await renew_task
            async with self._scoped(tenant_id) as connection:
                await connection.execute(
                    """
                    UPDATE sessions SET lease_owner=NULL,lease_expires_at=NULL
                    WHERE tenant_id=$1 AND session_id=$2 AND lease_owner=$3 AND fencing_epoch=$4
                    """,
                    tenant_id,
                    session_id,
                    owner_id,
                    lease.epoch,
                )

    async def _renew_lease(self, lease: SessionLease, stop: asyncio.Event) -> None:
        interval = max(0.1, self.lease_seconds / 3)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                async with self._scoped(lease.tenant_id) as connection:
                    result = await connection.execute(
                        """
                        UPDATE sessions SET lease_expires_at=now()+make_interval(secs => $5)
                        WHERE tenant_id=$1 AND session_id=$2 AND lease_owner=$3 AND fencing_epoch=$4
                        """,
                        lease.tenant_id,
                        lease.session_id,
                        lease.owner_id,
                        lease.epoch,
                        self.lease_seconds,
                    )
                if result != "UPDATE 1":
                    return

    async def append_turn(
        self,
        snapshot: SessionSnapshot,
        user_text: str,
        assistant_text: str,
        trace_id: str,
        lease: SessionLease | None = None,
        inbox_key: str | None = None,
        config_version: int | None = None,
    ) -> list[SessionEvent]:
        if lease is None:
            raise FencingConflict("PostgreSQL session writes require a lease")
        start = snapshot.version + 1
        events = [
            SessionEvent(snapshot.tenant_id, snapshot.session_id, start, "user_message", {"text": user_text}, trace_id, event_id=uuid4().hex),
            SessionEvent(snapshot.tenant_id, snapshot.session_id, start + 1, "assistant_message", {"text": assistant_text}, trace_id, event_id=uuid4().hex),
        ]
        state = dict(snapshot.state)
        state.update(last_user_text=user_text, last_assistant_text=assistant_text)
        async with self._scoped(snapshot.tenant_id) as connection:
            valid = await connection.fetchval(
                """
                SELECT 1 FROM sessions WHERE tenant_id=$1 AND session_id=$2 AND version=$3
                  AND lease_owner=$4 AND fencing_epoch=$5 AND lease_expires_at > now() FOR UPDATE
                """,
                snapshot.tenant_id,
                snapshot.session_id,
                lease.expected_version,
                lease.owner_id,
                lease.epoch,
            )
            if not valid:
                raise FencingConflict("session version or fencing epoch changed")
            for event in events:
                await connection.execute(
                    """
                    INSERT INTO session_events(tenant_id,session_id,sequence,event_id,event_type,payload_json,trace_id)
                    VALUES($1,$2,$3,$4,$5,$6::jsonb,$7)
                    """,
                    event.tenant_id,
                    event.session_id,
                    event.sequence,
                    event.event_id,
                    event.event_type,
                    _json(event.payload),
                    event.trace_id,
                )
            result = await connection.execute(
                """
                UPDATE sessions SET state_json=$6::jsonb,version=$3,updated_at=now(),lease_owner=NULL,lease_expires_at=NULL
                WHERE tenant_id=$1 AND session_id=$2 AND version=$4 AND lease_owner=$5 AND fencing_epoch=$7
                """,
                snapshot.tenant_id,
                snapshot.session_id,
                start + 1,
                lease.expected_version,
                lease.owner_id,
                _json(state),
                lease.epoch,
            )
            if result != "UPDATE 1":
                raise FencingConflict("session commit lost its lease")
            if inbox_key is not None:
                await connection.execute(
                    """
                    UPDATE inbound_messages SET status='prepared',result_json=$3::jsonb,updated_at=now()
                    WHERE tenant_id=$1 AND inbox_key=$2
                    """,
                    snapshot.tenant_id,
                    inbox_key,
                    _json({"status": "prepared", "text": assistant_text}),
                )
            if config_version is not None:
                await connection.execute(
                    """
                    INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                    VALUES($1,$2,'session.project',$3,$4::jsonb,$5)
                    """,
                    snapshot.tenant_id,
                    uuid4().hex,
                    snapshot.session_id,
                    _json(
                        {
                            "session_id": snapshot.session_id,
                            "source_version": start + 1,
                            "config_version": config_version,
                        }
                    ),
                    trace_id,
                )
        snapshot.version = start + 1
        snapshot.state = state
        snapshot.updated_at = datetime.now(timezone.utc)
        return events

    async def events(self, tenant_id: str, session_id: str) -> list[SessionEvent]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT tenant_id,session_id,sequence,event_type,payload_json,trace_id,created_at,event_id
                FROM session_events WHERE tenant_id=$1 AND session_id=$2 ORDER BY sequence
                """,
                tenant_id,
                session_id,
            )
        return [
            SessionEvent(
                row["tenant_id"], row["session_id"], row["sequence"], row["event_type"],
                _object(row["payload_json"]), row["trace_id"], row["created_at"], row["event_id"]
            )
            for row in rows
        ]

    async def add(self, tenant_id: str, user_id: str, value: str, source_id: str | None = None) -> None:
        memory_id = (
            hashlib.sha256(f"{tenant_id}:{source_id}".encode()).hexdigest() if source_id is not None else uuid4().hex
        )
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO memories(tenant_id,user_id,memory_id,content) VALUES($1,$2,$3,$4)
                ON CONFLICT (tenant_id,memory_id) DO NOTHING
                """,
                tenant_id,
                user_id,
                memory_id,
                value,
            )

    async def list(self, tenant_id: str, user_id: str) -> list[str]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                "SELECT content FROM memories WHERE tenant_id=$1 AND user_id=$2 ORDER BY created_at DESC LIMIT 100",
                tenant_id,
                user_id,
            )
        return [row["content"] for row in reversed(rows)]

    async def write(self, record: dict[str, Any]) -> None:
        safe = dict(record)
        tenant_id = str(safe.pop("tenant_id"))
        audit_id = str(safe.pop("audit_id", "") or uuid4().hex)
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO audit_logs(tenant_id,audit_id,channel,user_id,session_id,agent_name,tool_name,
                    decision,latency_ms,error_type,cost,trace_id,detail_json)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
                ON CONFLICT (tenant_id,audit_id) DO NOTHING
                """,
                tenant_id,
                audit_id,
                safe.pop("channel", None),
                safe.pop("user_id", None),
                safe.pop("session_id", None),
                safe.pop("agent_name", None),
                safe.pop("tool_name", None),
                safe.pop("decision", "unknown"),
                safe.pop("latency_ms", None),
                safe.pop("error_type", None),
                safe.pop("cost", 0),
                safe.pop("trace_id", None),
                _json(safe),
            )

    async def begin_outbound(self, message: Any) -> str:
        import hashlib

        outbound_id = hashlib.sha256(
            f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.in_reply_to}:{message.part}".encode()
        ).hexdigest()
        async with self._scoped(message.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT status FROM outbound_messages
                WHERE tenant_id=$1 AND outbound_id=$2 FOR UPDATE
                """,
                message.tenant_id,
                outbound_id,
            )
            if row is None:
                await connection.execute(
                    """
                    INSERT INTO outbound_messages(tenant_id,outbound_id,channel,account_id,in_reply_to,part,payload_json,status)
                    VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'sending')
                    """,
                    message.tenant_id,
                    outbound_id,
                    message.channel,
                    message.account_id,
                    message.in_reply_to,
                    message.part,
                    _json(asdict(message)),
                )
                return "send"
            status = row["status"]
            if status == "delivered":
                return "delivered"
            if status == "sending":
                await connection.execute(
                    "UPDATE outbound_messages SET status='ambiguous',updated_at=now() WHERE tenant_id=$1 AND outbound_id=$2",
                    message.tenant_id,
                    outbound_id,
                )
                return "ambiguous"
            if status in {"ambiguous", "terminal_failed"}:
                return status
            await connection.execute(
                "UPDATE outbound_messages SET status='sending',updated_at=now() WHERE tenant_id=$1 AND outbound_id=$2",
                message.tenant_id,
                outbound_id,
            )
            return "send"

    async def finish_outbound(self, message: Any, receipt: dict[str, Any]) -> None:
        import hashlib

        outbound_id = hashlib.sha256(
            f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.in_reply_to}:{message.part}".encode()
        ).hexdigest()
        status = "delivered" if receipt.get("ok") else ("failed" if receipt.get("retryable") else "terminal_failed")
        async with self._scoped(message.tenant_id) as connection:
            await connection.execute(
                """
                UPDATE outbound_messages SET status=$3,provider_message_id=$4,error_type=$5,updated_at=now()
                WHERE tenant_id=$1 AND outbound_id=$2 AND status='sending'
                """,
                message.tenant_id,
                outbound_id,
                status,
                receipt.get("provider_message_id"),
                None if receipt.get("ok") else str(receipt.get("code", "unknown")),
            )

    async def query_audit(self, tenant_id: str, limit: int = 100) -> list[dict[str, Any]]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                "SELECT * FROM audit_logs WHERE tenant_id=$1 ORDER BY occurred_at DESC,audit_id DESC LIMIT $2",
                tenant_id,
                max(1, min(limit, 1000)),
            )
        return [dict(row) for row in rows]

    async def purge_expired_audit(self, tenant_id: str, retention_days: int) -> int:
        async with self._scoped(tenant_id) as connection:
            result = await connection.execute(
                """
                DELETE FROM audit_logs
                WHERE tenant_id=$1 AND occurred_at < now()-make_interval(days => $2)
                """,
                tenant_id,
                max(1, min(retention_days, 3650)),
            )
        return int(result.rpartition(" ")[2])

    async def record_artifact(self, artifact: Artifact) -> None:
        async with self._scoped(artifact.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO artifacts(tenant_id,artifact_id,object_key,checksum,size_bytes,content_type,status)
                VALUES($1,$2,$3,$4,$5,$6,'ready')
                ON CONFLICT (tenant_id,artifact_id) DO UPDATE SET object_key=EXCLUDED.object_key,
                    checksum=EXCLUDED.checksum,size_bytes=EXCLUDED.size_bytes,
                    content_type=EXCLUDED.content_type,status='ready'
                """,
                artifact.tenant_id,
                artifact.artifact_id,
                artifact.key,
                artifact.checksum,
                artifact.size,
                artifact.content_type,
            )

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Artifact | None:
        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT tenant_id,artifact_id,object_key,checksum,size_bytes,content_type
                FROM artifacts WHERE tenant_id=$1 AND artifact_id=$2 AND status='ready'
                """,
                tenant_id,
                artifact_id,
            )
        if row is None:
            return None
        return Artifact(
            tenant_id=row["tenant_id"],
            artifact_id=row["artifact_id"],
            key=row["object_key"],
            checksum=row["checksum"],
            size=row["size_bytes"],
            content_type=row["content_type"],
        )

    async def upsert_knowledge_document(
        self,
        tenant_id: str,
        collection: str,
        item_id: str,
        content: str,
        config_version: int,
        trace_id: str,
    ) -> int:
        async with self._scoped(tenant_id) as connection:
            source_version = await connection.fetchval(
                """
                INSERT INTO knowledge_documents(
                    tenant_id,collection,item_id,content,source_version,status
                ) VALUES($1,$2,$3,$4,1,'pending')
                ON CONFLICT (tenant_id,collection,item_id) DO UPDATE
                SET content=EXCLUDED.content,source_version=knowledge_documents.source_version+1,
                    status='pending',updated_at=now()
                RETURNING source_version
                """,
                tenant_id,
                collection,
                item_id,
                content,
            )
            await connection.execute(
                """
                INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                VALUES($1,$2,'knowledge.index',$3,$4::jsonb,$5)
                """,
                tenant_id,
                uuid4().hex,
                f"{collection}:{item_id}",
                _json(
                    {
                        "collection": collection,
                        "item_id": item_id,
                        "content": content,
                        "source_version": int(source_version),
                        "config_version": config_version,
                    }
                ),
                trace_id,
            )
        return int(source_version)

    async def mark_knowledge_indexed(
        self, tenant_id: str, collection: str, item_id: str, source_version: int
    ) -> None:
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                """
                UPDATE knowledge_documents SET status='indexed',updated_at=now()
                WHERE tenant_id=$1 AND collection=$2 AND item_id=$3 AND source_version=$4
                """,
                tenant_id,
                collection,
                item_id,
                source_version,
            )

    async def enqueue_session_projection(
        self,
        tenant_id: str,
        session_id: str,
        source_version: int,
        config_version: int,
        trace_id: str,
    ) -> None:
        outbox_id = hashlib.sha256(
            f"session.project:{tenant_id}:{session_id}:{source_version}".encode()
        ).hexdigest()
        async with self._scoped(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO outbox_events(tenant_id,outbox_id,event_type,aggregate_id,payload_json,trace_id)
                VALUES($1,$2,'session.project',$3,$4::jsonb,$5)
                ON CONFLICT (tenant_id,outbox_id) DO NOTHING
                """,
                tenant_id,
                outbox_id,
                session_id,
                _json(
                    {
                        "session_id": session_id,
                        "source_version": source_version,
                        "config_version": config_version,
                    }
                ),
                trace_id,
            )

    async def list_knowledge_documents(
        self, tenant_id: str, collection: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT tenant_id,collection,item_id,content,source_version,status,created_at,updated_at
                FROM knowledge_documents WHERE tenant_id=$1 AND collection=$2
                ORDER BY item_id LIMIT $3
                """,
                tenant_id,
                collection,
                max(1, min(limit, 1000)),
            )
        return [dict(row) for row in rows]

    async def sweep_expired_leases(self, limit: int = 100) -> int:
        async with self._control() as connection:
            result = await connection.execute(
                """
                WITH expired AS (SELECT tenant_id,session_id FROM sessions WHERE lease_expires_at <= now()
                    ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT $1)
                UPDATE sessions s SET lease_owner=NULL,lease_expires_at=NULL FROM expired e
                WHERE s.tenant_id=e.tenant_id AND s.session_id=e.session_id
                """,
                max(1, min(limit, 1000)),
            )
        return int(result.rpartition(" ")[2])

    async def save_tenant_config(self, config: Any) -> None:
        payload = self._tenant_payload(config)
        async with self._scoped(config.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO tenants(tenant_id,name,status,active_version) VALUES($1,$2,$3,$4)
                ON CONFLICT (tenant_id) DO UPDATE SET name=EXCLUDED.name,status=EXCLUDED.status,
                    active_version=EXCLUDED.active_version
                """,
                config.tenant_id,
                config.name,
                config.status,
                config.version,
            )
            await connection.execute(
                """
                INSERT INTO tenant_revisions(tenant_id,version,config_json) VALUES($1,$2,$3::jsonb)
                ON CONFLICT (tenant_id,version) DO NOTHING
                """,
                config.tenant_id,
                config.version,
                _json(payload),
            )

    async def create_tenant_config(self, config: Any) -> bool:
        payload = self._tenant_payload(config)
        async with self._scoped(config.tenant_id) as connection:
            inserted = await connection.fetchval(
                """
                INSERT INTO tenants(tenant_id,name,status,active_version) VALUES($1,$2,$3,$4)
                ON CONFLICT (tenant_id) DO NOTHING RETURNING tenant_id
                """,
                config.tenant_id,
                config.name,
                config.status,
                config.version,
            )
            if inserted is None:
                return False
            await connection.execute(
                "INSERT INTO tenant_revisions(tenant_id,version,config_json) VALUES($1,$2,$3::jsonb)",
                config.tenant_id,
                config.version,
                _json(payload),
            )
        return True

    async def publish_tenant_config(self, config: Any, expected_version: int) -> bool:
        payload = self._tenant_payload(config)
        async with self._scoped(config.tenant_id) as connection:
            result = await connection.execute(
                """
                UPDATE tenants SET name=$3,status=$4,active_version=$5
                WHERE tenant_id=$1 AND active_version=$2
                """,
                config.tenant_id,
                expected_version,
                config.name,
                config.status,
                config.version,
            )
            if result != "UPDATE 1":
                return False
            await connection.execute(
                "INSERT INTO tenant_revisions(tenant_id,version,config_json) VALUES($1,$2,$3::jsonb)",
                config.tenant_id,
                config.version,
                _json(payload),
            )
        return True

    async def next_tenant_config_version(self, tenant_id: str) -> int:
        async with self._scoped(tenant_id) as connection:
            value = await connection.fetchval(
                "SELECT COALESCE(max(version),0)+1 FROM tenant_revisions WHERE tenant_id=$1",
                tenant_id,
            )
        return int(value)

    async def activate_tenant_revision(self, tenant_id: str, version: int, expected_version: int) -> bool:
        async with self._scoped(tenant_id) as connection:
            exists = await connection.fetchval(
                "SELECT 1 FROM tenant_revisions WHERE tenant_id=$1 AND version=$2",
                tenant_id,
                version,
            )
            if exists is None:
                raise KeyError(f"{tenant_id}@{version}")
            result = await connection.execute(
                "UPDATE tenants SET active_version=$3 WHERE tenant_id=$1 AND active_version=$2",
                tenant_id,
                expected_version,
                version,
            )
        return result == "UPDATE 1"

    async def list_tenant_configs(self) -> list[dict[str, Any]]:
        async with self._control() as connection:
            rows = await connection.fetch(
                """
                SELECT r.config_json FROM tenants t JOIN tenant_revisions r
                  ON r.tenant_id=t.tenant_id AND r.version=t.active_version
                WHERE t.status <> 'deleted' ORDER BY t.tenant_id
                """
            )
        return [_object(row["config_json"]) for row in rows]

    async def get_active_tenant_config(self, tenant_id: str) -> Any:
        from ..web.admin import parse_config

        async with self._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT r.version,r.config_json FROM tenants t JOIN tenant_revisions r
                  ON r.tenant_id=t.tenant_id AND r.version=t.active_version
                WHERE t.tenant_id=$1 AND t.status <> 'deleted'
                """,
                tenant_id,
            )
        if row is None:
            raise KeyError(tenant_id)
        return parse_config(_object(row["config_json"]), version=int(row["version"]))

    async def get_tenant_config(self, tenant_id: str, version: int) -> Any:
        from ..web.admin import parse_config

        async with self._scoped(tenant_id) as connection:
            value = await connection.fetchval(
                "SELECT config_json FROM tenant_revisions WHERE tenant_id=$1 AND version=$2",
                tenant_id,
                version,
            )
        if value is None:
            raise KeyError(f"{tenant_id}@{version}")
        return parse_config(_object(value), version=version)

    async def delete_tenant_config(self, tenant_id: str, expected_version: int) -> bool:
        async with self._control() as connection:
            result = await connection.execute(
                "UPDATE tenants SET status='deleted' WHERE tenant_id=$1 AND active_version=$2",
                tenant_id,
                expected_version,
            )
        return result == "UPDATE 1"

    @staticmethod
    def _tenant_payload(config: Any) -> dict[str, Any]:
        payload = asdict(config)
        for field in ("allowed_tools", "dangerous_tools"):
            payload["policy"][field] = sorted(payload["policy"][field])
        for app in payload["apps"].values():
            app["tools"] = sorted(app["tools"])
            app["knowledge_collections"] = sorted(app["knowledge_collections"])
        for channel in payload["channels"].values():
            channel["allowed_users"] = sorted(channel["allowed_users"])
        return payload

    async def reserve(self, tenant_id: str, budget: int, tokens: int, ttl_seconds: int = 300) -> Any:
        import time

        from ..tool import BudgetExceeded, BudgetLease

        identifier = uuid4().hex
        async with self._scoped(tenant_id) as connection:
            expired = await connection.fetchval(
                """
                WITH released AS (
                    UPDATE budget_reservations SET settled_at=now()
                    WHERE tenant_id=$1 AND usage_day=current_date AND settled_at IS NULL AND expires_at <= now()
                    RETURNING reserved_tokens
                ) SELECT COALESCE(sum(reserved_tokens),0) FROM released
                """,
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO tenant_budget_usage(tenant_id,usage_day) VALUES($1,current_date)
                ON CONFLICT DO NOTHING
                """,
                tenant_id,
            )
            if expired:
                await connection.execute(
                    """
                    UPDATE tenant_budget_usage SET reserved_tokens=GREATEST(0,reserved_tokens-$2)
                    WHERE tenant_id=$1 AND usage_day=current_date
                    """,
                    tenant_id,
                    expired,
                )
            row = await connection.fetchrow(
                "SELECT used_tokens,reserved_tokens FROM tenant_budget_usage "
                "WHERE tenant_id=$1 AND usage_day=current_date FOR UPDATE",
                tenant_id,
            )
            if tokens < 0 or row["used_tokens"] + row["reserved_tokens"] + tokens > budget:
                raise BudgetExceeded("tenant token budget exceeded")
            await connection.execute(
                """
                INSERT INTO budget_reservations(tenant_id,reservation_id,usage_day,reserved_tokens,expires_at)
                VALUES($1,$2,current_date,$3,now()+make_interval(secs => $4))
                """,
                tenant_id,
                identifier,
                tokens,
                ttl_seconds,
            )
            await connection.execute(
                "UPDATE tenant_budget_usage SET reserved_tokens=reserved_tokens+$2 "
                "WHERE tenant_id=$1 AND usage_day=current_date",
                tenant_id,
                tokens,
            )
        return BudgetLease(tenant_id, identifier, tokens, time.time() + ttl_seconds)

    async def settle(self, lease: Any, actual_tokens: int) -> None:
        async with self._scoped(lease.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT usage_day,reserved_tokens FROM budget_reservations
                WHERE tenant_id=$1 AND reservation_id=$2 AND settled_at IS NULL FOR UPDATE
                """,
                lease.tenant_id,
                lease.reservation_id,
            )
            if row is None:
                return
            await connection.execute(
                """
                UPDATE tenant_budget_usage SET reserved_tokens=GREATEST(0,reserved_tokens-$3),
                    used_tokens=used_tokens+$4 WHERE tenant_id=$1 AND usage_day=$2
                """,
                lease.tenant_id,
                row["usage_day"],
                row["reserved_tokens"],
                max(0, actual_tokens),
            )
            await connection.execute(
                "UPDATE budget_reservations SET settled_at=now() WHERE tenant_id=$1 AND reservation_id=$2",
                lease.tenant_id,
                lease.reservation_id,
            )

    @staticmethod
    def _snapshot(row: Any) -> SessionSnapshot:
        return SessionSnapshot(
            row["tenant_id"], row["session_id"], row["app_id"], row["user_id"],
            _object(row["state_json"]), row["version"], row["updated_at"]
        )

    async def export(self) -> dict[str, Any]:
        async with self._control() as connection:
            rows = await connection.fetch(
                "SELECT tenant_id,session_id,app_id,user_id,state_json,version,updated_at FROM sessions"
            )
        sessions = {f"{row['tenant_id']}:{row['session_id']}": asdict(self._snapshot(row)) for row in rows}
        return {"sessions": sessions}
