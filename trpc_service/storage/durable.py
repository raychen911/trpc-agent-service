"""Durable Inbox/Outbox primitives with local and SQL implementations.

The Redis queues remain useful as a transport, but correctness lives here:
deduplication, ownership leases, retries, and the outbox record are persisted
in the structured storage backend.
"""

from __future__ import annotations

import json
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from threading import RLock
from typing import Any, Protocol
from uuid import uuid4

from trpc_service.security.secrets import redact_secret_text
from trpc_service.storage.base import now_utc
from trpc_service.storage.postgres_rls import (
    _tenant_from_first_argument,
    _tenant_from_optional_keyword,
    postgres_schema_auto_create,
    rls_tenant_method,
)


class InboxStatus:
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class OutboxStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    DEAD = "dead"


@dataclass(slots=True)
class InboxRecord:
    message_id: str
    tenant_id: str
    dedupe_key: str
    session_id: str
    payload: dict[str, Any]
    status: str = InboxStatus.PROCESSING
    attempts: int = 1
    owner: str | None = None
    lease_until: datetime | None = None
    result: dict[str, Any] | None = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class OutboxRecord:
    event_id: str
    tenant_id: str
    topic: str
    aggregate_id: str
    payload: dict[str, Any]
    status: str = "pending"
    attempts: int = 0
    available_at: datetime = field(default_factory=now_utc)
    locked_by: str | None = None
    locked_until: datetime | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


class InboxOutboxStore(Protocol):
    def accept_inbox(
        self,
        tenant_id: str,
        dedupe_key: str,
        session_id: str,
        payload: dict[str, Any],
        owner: str,
        lease_seconds: int = 180,
    ) -> tuple[InboxRecord, bool]: ...

    def complete_inbox(self, tenant_id: str, dedupe_key: str, owner: str, result: dict[str, Any]) -> None: ...

    def fail_inbox(self, tenant_id: str, dedupe_key: str, owner: str, error: str) -> None: ...

    def enqueue_outbox(
        self,
        tenant_id: str,
        topic: str,
        aggregate_id: str,
        payload: dict[str, Any],
        event_id: str | None = None,
    ) -> OutboxRecord: ...

    def claim_outbox(
        self,
        owner: str,
        limit: int = 10,
        lease_seconds: int = 180,
        tenant_id: str | None = None,
    ) -> list[OutboxRecord]: ...

    def complete_outbox(self, event_id: str, owner: str, tenant_id: str | None = None) -> None: ...

    def fail_outbox(
        self,
        event_id: str,
        owner: str,
        error: str,
        retry_after_seconds: int = 30,
        tenant_id: str | None = None,
    ) -> None: ...


class DurableOutboxDispatcher:
    """Claim and deliver durable outbox records with crash recovery."""

    def __init__(
        self,
        store: InboxOutboxStore,
        handler,
        owner: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        self.store = store
        self.handler = handler
        self.owner = owner or f"outbox:{uuid4()}"
        self.tenant_id = tenant_id

    def run_once(self, limit: int = 10, lease_seconds: int = 180) -> int:
        records = self.store.claim_outbox(
            self.owner,
            limit=limit,
            lease_seconds=lease_seconds,
            tenant_id=self.tenant_id,
        )
        processed = 0
        for record in records:
            try:
                self.handler(record)
            except Exception as exc:
                self.store.fail_outbox(
                    record.event_id,
                    self.owner,
                    str(exc),
                    tenant_id=record.tenant_id,
                )
            else:
                self.store.complete_outbox(
                    record.event_id,
                    self.owner,
                    tenant_id=record.tenant_id,
                )
                processed += 1
        return processed


def _lease_until(seconds: int) -> datetime:
    return now_utc() + timedelta(seconds=max(1, int(seconds)))


def _max_outbox_attempts() -> int:
    try:
        return max(1, int(os.getenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "10")))
    except (TypeError, ValueError) as exc:
        raise ValueError("DURABLE_OUTBOX_MAX_ATTEMPTS must be an integer") from exc


def _is_postgres_connection_error(exc: Exception) -> bool:
    module = exc.__class__.__module__
    name = exc.__class__.__name__
    text = str(exc).lower()
    return module.startswith("psycopg") and (
        name in {"OperationalError", "InterfaceError", "AdminShutdown", "ConnectionTimeout"}
        or "connection is closed" in text
        or "terminating connection" in text
    )


def _retry_postgres_once(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            if not _is_postgres_connection_error(exc) or self._connection_provider is None:
                raise
            with self._lock:
                self._conn = self._connection_provider()
            return method(self, *args, **kwargs)

    return wrapper


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return dict(value)


class InMemoryInboxOutbox:
    def __init__(self) -> None:
        self._inbox: dict[tuple[str, str], InboxRecord] = {}
        self._outbox: dict[str, OutboxRecord] = {}
        self._lock = RLock()

    def accept_inbox(self, tenant_id, dedupe_key, session_id, payload, owner, lease_seconds=180):
        with self._lock:
            key = (tenant_id, dedupe_key)
            current = self._inbox.get(key)
            now = now_utc()
            if current is not None:
                if current.status == InboxStatus.COMPLETED:
                    return deepcopy(current), False
                if current.status == InboxStatus.PROCESSING and current.lease_until and current.lease_until > now:
                    return deepcopy(current), False
                current.status = InboxStatus.PROCESSING
                current.attempts += 1
                current.owner = owner
                current.lease_until = _lease_until(lease_seconds)
                current.updated_at = now
                return deepcopy(current), True
            record = InboxRecord(
                message_id=str(uuid4()),
                tenant_id=tenant_id,
                dedupe_key=dedupe_key,
                session_id=session_id,
                payload=deepcopy(payload),
                owner=owner,
                lease_until=_lease_until(lease_seconds),
            )
            self._inbox[key] = record
            return deepcopy(record), True

    def complete_inbox(self, tenant_id, dedupe_key, owner, result):
        with self._lock:
            record = self._inbox[(tenant_id, dedupe_key)]
            if record.owner != owner:
                raise RuntimeError("inbox ownership lost")
            record.status = InboxStatus.COMPLETED
            record.owner = None
            record.lease_until = None
            record.result = deepcopy(result)
            record.updated_at = now_utc()

    def fail_inbox(self, tenant_id, dedupe_key, owner, error):
        with self._lock:
            record = self._inbox[(tenant_id, dedupe_key)]
            if record.owner != owner:
                raise RuntimeError("inbox ownership lost")
            record.status = InboxStatus.FAILED
            record.owner = None
            record.lease_until = None
            record.result = {"error": redact_secret_text(str(error))[:1000]}
            record.updated_at = now_utc()

    def enqueue_outbox(self, tenant_id, topic, aggregate_id, payload, event_id=None):
        with self._lock:
            event_id = event_id or str(uuid4())
            current = self._outbox.get(event_id)
            if current:
                return deepcopy(current)
            record = OutboxRecord(
                event_id=event_id,
                tenant_id=tenant_id,
                topic=topic,
                aggregate_id=aggregate_id,
                payload=deepcopy(payload),
            )
            self._outbox[event_id] = record
            return deepcopy(record)

    def claim_outbox(self, owner, limit=10, lease_seconds=180, tenant_id=None):
        with self._lock:
            now = now_utc()
            result = []
            for record in sorted(self._outbox.values(), key=lambda item: item.created_at):
                if len(result) >= limit:
                    break
                if tenant_id is not None and record.tenant_id != tenant_id:
                    continue
                available = record.available_at <= now
                reclaimable = record.status == "processing" and (
                    record.locked_until is None or record.locked_until <= now
                )
                if not (record.status == "pending" and available or reclaimable):
                    continue
                record.status = "processing"
                record.attempts += 1
                record.locked_by = owner
                record.locked_until = _lease_until(lease_seconds)
                record.updated_at = now
                result.append(deepcopy(record))
            return result

    def complete_outbox(self, event_id, owner, tenant_id=None):
        with self._lock:
            record = self._outbox[event_id]
            if record.locked_by != owner or (tenant_id is not None and record.tenant_id != tenant_id):
                raise RuntimeError("outbox ownership lost")
            record.status = "completed"
            record.locked_by = None
            record.locked_until = None
            record.updated_at = now_utc()

    def fail_outbox(self, event_id, owner, error, retry_after_seconds=30, tenant_id=None):
        with self._lock:
            record = self._outbox[event_id]
            if record.locked_by != owner or (tenant_id is not None and record.tenant_id != tenant_id):
                raise RuntimeError("outbox ownership lost")
            record.status = (
                OutboxStatus.DEAD
                if record.attempts >= _max_outbox_attempts()
                else OutboxStatus.PENDING
            )
            record.locked_by = None
            record.locked_until = None
            record.available_at = now_utc() + timedelta(seconds=max(0, retry_after_seconds))
            record.last_error = redact_secret_text(str(error))[:1000]
            record.updated_at = now_utc()


class SQLiteInboxOutbox:
    def __init__(self, connection, lock: RLock) -> None:
        self._conn = connection
        self._lock = lock
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS inbox_message (
                  message_id TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  dedupe_key TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 1,
                  owner TEXT,
                  lease_until TEXT,
                  result_json TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE (tenant_id, dedupe_key)
                );
                CREATE TABLE IF NOT EXISTS outbox_message (
                  event_id TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  topic TEXT NOT NULL,
                  aggregate_id TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  available_at TEXT NOT NULL,
                  locked_by TEXT,
                  locked_until TEXT,
                  last_error TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_ready
                  ON outbox_message(status, available_at, created_at);
                """
            )

    def accept_inbox(self, tenant_id, dedupe_key, session_id, payload, owner, lease_seconds=180):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            now = now_utc()
            record = InboxRecord(
                str(uuid4()), tenant_id, dedupe_key, session_id, deepcopy(payload),
                owner=owner, lease_until=_lease_until(lease_seconds),
            )
            inserted = self._insert_inbox(record, ignore_conflict=True)
            if inserted.rowcount == 1:
                return record, True
            row = self._conn.execute(
                "SELECT * FROM inbox_message WHERE tenant_id=? AND dedupe_key=?",
                (tenant_id, dedupe_key),
            ).fetchone()
            if row is None:
                raise RuntimeError("inbox insert raced but existing record was not found")
            current = self._inbox_from_row(row)
            if current.status == InboxStatus.COMPLETED:
                return current, False
            if current.status == InboxStatus.PROCESSING and current.lease_until and current.lease_until > now:
                return current, False
            self._conn.execute(
                """
                UPDATE inbox_message
                SET status=?, attempts=attempts+1, owner=?, lease_until=?, result_json=NULL, updated_at=?
                WHERE tenant_id=? AND dedupe_key=?
                """,
                (InboxStatus.PROCESSING, owner, _iso(_lease_until(lease_seconds)), _iso(now), tenant_id, dedupe_key),
            )
            return self._inbox_from_row(
                self._conn.execute(
                    "SELECT * FROM inbox_message WHERE tenant_id=? AND dedupe_key=?",
                    (tenant_id, dedupe_key),
                ).fetchone()
            ), True

    def complete_inbox(self, tenant_id, dedupe_key, owner, result):
        self._update_inbox(tenant_id, dedupe_key, owner, InboxStatus.COMPLETED, result)

    def fail_inbox(self, tenant_id, dedupe_key, owner, error):
        self._update_inbox(
            tenant_id,
            dedupe_key,
            owner,
            InboxStatus.FAILED,
            {"error": redact_secret_text(str(error))[:1000]},
        )

    def _update_inbox(self, tenant_id, dedupe_key, owner, status, result):
        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                UPDATE inbox_message
                SET status=?, owner=NULL, lease_until=NULL, result_json=?, updated_at=?
                WHERE tenant_id=? AND dedupe_key=? AND owner=?
                """,
                (
                    status,
                    json.dumps(result, ensure_ascii=False, default=str),
                    _iso(now_utc()),
                    tenant_id,
                    dedupe_key,
                    owner,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("inbox ownership lost")

    def enqueue_outbox(self, tenant_id, topic, aggregate_id, payload, event_id=None):
        with self._lock, self._conn:
            event_id = event_id or str(uuid4())
            row = self._conn.execute("SELECT * FROM outbox_message WHERE event_id=?", (event_id,)).fetchone()
            if row:
                return self._outbox_from_row(row)
            record = OutboxRecord(event_id, tenant_id, topic, aggregate_id, deepcopy(payload))
            self._conn.execute(
                """
                INSERT INTO outbox_message (
                  event_id, tenant_id, topic, aggregate_id, payload_json, status,
                  attempts, available_at, locked_by, locked_until, last_error,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.event_id, record.tenant_id, record.topic, record.aggregate_id,
                    json.dumps(record.payload, ensure_ascii=False, default=str),
                    record.status,
                    record.attempts,
                    _iso(record.available_at), None, None, None, _iso(record.created_at), _iso(record.updated_at),
                ),
            )
            return record

    def claim_outbox(self, owner, limit=10, lease_seconds=180, tenant_id=None):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            now = now_utc()
            tenant_clause = " AND tenant_id = ?" if tenant_id is not None else ""
            rows = self._conn.execute(
                f"""
                SELECT * FROM outbox_message
                WHERE (
                    (status='pending' AND available_at<=?)
                    OR (status='processing' AND (locked_until IS NULL OR locked_until<=?))
                )
                   {tenant_clause}
                ORDER BY created_at LIMIT ?
                """,
                (
                    (_iso(now), _iso(now), tenant_id, int(limit))
                    if tenant_id is not None
                    else (_iso(now), _iso(now), int(limit))
                ),
            ).fetchall()
            result = []
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE outbox_message
                    SET status='processing', attempts=attempts+1, locked_by=?,
                        locked_until=?, updated_at=?
                    WHERE event_id=?
                    """,
                    (owner, _iso(_lease_until(lease_seconds)), _iso(now), row["event_id"]),
                )
                result.append(self._outbox_from_row(
                    self._conn.execute("SELECT * FROM outbox_message WHERE event_id=?", (row["event_id"],)).fetchone()
                ))
            return result

    def complete_outbox(self, event_id, owner, tenant_id=None):
        self._update_outbox(event_id, owner, "completed", None, None, tenant_id=tenant_id)

    def fail_outbox(self, event_id, owner, error, retry_after_seconds=30, tenant_id=None):
        with self._lock, self._conn:
            row = self._conn.execute(
                (
                    "SELECT attempts FROM outbox_message WHERE event_id=? AND locked_by=?"
                    + (" AND tenant_id=?" if tenant_id is not None else "")
                ),
                (event_id, owner, tenant_id) if tenant_id is not None else (event_id, owner),
            ).fetchone()
            if row is None:
                raise RuntimeError("outbox ownership lost")
            status = (
                OutboxStatus.DEAD
                if int(row["attempts"]) >= _max_outbox_attempts()
                else OutboxStatus.PENDING
            )
            cur = self._conn.execute(
                (
                    """
                    UPDATE outbox_message
                    SET status=?, locked_by=NULL, locked_until=NULL, last_error=?,
                        available_at=?, updated_at=?
                    WHERE event_id=? AND locked_by=?
                    """
                    + (" AND tenant_id=?" if tenant_id is not None else "")
                ),
                (
                    (
                        status,
                        redact_secret_text(str(error))[:1000],
                        _iso(now_utc() + timedelta(seconds=max(0, retry_after_seconds))),
                        _iso(now_utc()),
                        event_id,
                        owner,
                        tenant_id,
                    )
                    if tenant_id is not None
                    else (
                        status,
                        redact_secret_text(str(error))[:1000],
                        _iso(now_utc() + timedelta(seconds=max(0, retry_after_seconds))),
                        _iso(now_utc()),
                        event_id,
                        owner,
                    )
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("outbox ownership lost")

    def _update_outbox(self, event_id, owner, status, error, available_at, tenant_id=None):
        with self._lock, self._conn:
            cur = self._conn.execute(
                (
                    """
                UPDATE outbox_message
                SET status=?, locked_by=NULL, locked_until=NULL, last_error=?,
                    available_at=COALESCE(?, available_at), updated_at=?
                WHERE event_id=? AND locked_by=?
                    """
                    + (" AND tenant_id=?" if tenant_id is not None else "")
                ),
                (
                    status,
                    error,
                    available_at,
                    _iso(now_utc()),
                    event_id,
                    owner,
                    tenant_id,
                )
                if tenant_id is not None
                else (status, error, available_at, _iso(now_utc()), event_id, owner),
            )
            if cur.rowcount != 1:
                raise RuntimeError("outbox ownership lost")

    @staticmethod
    def _inbox_from_row(row):
        return InboxRecord(
            message_id=row["message_id"], tenant_id=row["tenant_id"], dedupe_key=row["dedupe_key"],
            session_id=row["session_id"], payload=json.loads(row["payload_json"]),
            status=row["status"], attempts=int(row["attempts"]), owner=row["owner"],
            lease_until=_parse(row["lease_until"]),
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            created_at=_parse(row["created_at"]), updated_at=_parse(row["updated_at"]),
        )

    @staticmethod
    def _outbox_from_row(row):
        return OutboxRecord(
            event_id=row["event_id"], tenant_id=row["tenant_id"], topic=row["topic"],
            aggregate_id=row["aggregate_id"], payload=json.loads(row["payload_json"]),
            status=row["status"], attempts=int(row["attempts"]), available_at=_parse(row["available_at"]),
            locked_by=row["locked_by"], locked_until=_parse(row["locked_until"]),
            last_error=row["last_error"],
            created_at=_parse(row["created_at"]),
            updated_at=_parse(row["updated_at"]),
        )

    def _insert_inbox(self, record, ignore_conflict=False):
        prefix = "INSERT OR IGNORE" if ignore_conflict else "INSERT"
        return self._conn.execute(
            f"""
            {prefix} INTO inbox_message (
              message_id, tenant_id, dedupe_key, session_id, payload_json, status,
              attempts, owner, lease_until, result_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.message_id, record.tenant_id, record.dedupe_key, record.session_id,
                json.dumps(record.payload, ensure_ascii=False, default=str),
                record.status,
                record.attempts,
                record.owner, _iso(record.lease_until), None, _iso(record.created_at), _iso(record.updated_at),
            ),
        )


class PostgresInboxOutbox(SQLiteInboxOutbox):
    """PostgreSQL implementation; SQL is kept separate from the demo backend."""

    def __init__(self, connection, lock: RLock, connection_provider=None) -> None:
        self._connection_provider = connection_provider
        super().__init__(connection, lock)

    def set_connection(self, connection) -> None:
        self._conn = connection

    def _init_schema(self):
        if not postgres_schema_auto_create():
            return
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS inbox_message (
                  message_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                  dedupe_key TEXT NOT NULL, session_id TEXT NOT NULL,
                  payload_json JSONB NOT NULL, status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 1, owner TEXT,
                  lease_until TIMESTAMPTZ, result_json JSONB,
                  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
                  UNIQUE (tenant_id, dedupe_key)
                );
                CREATE TABLE IF NOT EXISTS outbox_message (
                  event_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                  topic TEXT NOT NULL, aggregate_id TEXT NOT NULL,
                  payload_json JSONB NOT NULL, status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0, available_at TIMESTAMPTZ NOT NULL,
                  locked_by TEXT, locked_until TIMESTAMPTZ, last_error TEXT,
                  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_ready
                  ON outbox_message(status, available_at, created_at);
                """
            )

    def accept_inbox(self, tenant_id, dedupe_key, session_id, payload, owner, lease_seconds=180):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            now = now_utc()
            record = InboxRecord(
                str(uuid4()),
                tenant_id,
                dedupe_key,
                session_id,
                deepcopy(payload),
                owner=owner,
                lease_until=_lease_until(lease_seconds),
            )
            cur.execute(
                """
                INSERT INTO inbox_message (
                  message_id, tenant_id, dedupe_key, session_id, payload_json, status,
                  attempts, owner, lease_until, result_json, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, dedupe_key) DO NOTHING
                RETURNING message_id
                """,
                (
                    record.message_id,
                    record.tenant_id,
                    record.dedupe_key,
                    record.session_id,
                    json.dumps(record.payload, default=str),
                    record.status,
                    record.attempts,
                    record.owner,
                    record.lease_until,
                    None,
                    record.created_at,
                    record.updated_at,
                ),
            )
            if cur.fetchone() is not None:
                return record, True
            cur.execute(
                "SELECT * FROM inbox_message WHERE tenant_id=%s AND dedupe_key=%s FOR UPDATE",
                (tenant_id, dedupe_key),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("inbox insert raced but existing record was not found")
            current = self._inbox_from_pg(row)
            if current.status == InboxStatus.COMPLETED or (
                current.status == InboxStatus.PROCESSING and current.lease_until and current.lease_until > now
            ):
                return current, False
            cur.execute(
                (
                    "UPDATE inbox_message SET status=%s, attempts=attempts+1, owner=%s, "
                    "lease_until=%s, result_json=NULL, updated_at=%s "
                    "WHERE tenant_id=%s AND dedupe_key=%s"
                ),
                (InboxStatus.PROCESSING, owner, _lease_until(lease_seconds), now, tenant_id, dedupe_key),
            )
            cur.execute("SELECT * FROM inbox_message WHERE tenant_id=%s AND dedupe_key=%s", (tenant_id, dedupe_key))
            return self._inbox_from_pg(cur.fetchone()), True

    def complete_inbox(self, tenant_id, dedupe_key, owner, result):
        self._update_inbox_pg(tenant_id, dedupe_key, owner, InboxStatus.COMPLETED, result)

    def fail_inbox(self, tenant_id, dedupe_key, owner, error):
        self._update_inbox_pg(
            tenant_id,
            dedupe_key,
            owner,
            InboxStatus.FAILED,
            {"error": redact_secret_text(str(error))[:1000]},
        )

    def _update_inbox_pg(self, tenant_id, dedupe_key, owner, status, result):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                (
                    "UPDATE inbox_message SET status=%s, owner=NULL, lease_until=NULL, "
                    "result_json=%s, updated_at=%s "
                    "WHERE tenant_id=%s AND dedupe_key=%s AND owner=%s"
                ),
                (status, json.dumps(result, default=str), now_utc(), tenant_id, dedupe_key, owner),
            )
            if cur.rowcount != 1:
                raise RuntimeError("inbox ownership lost")

    def enqueue_outbox(self, tenant_id, topic, aggregate_id, payload, event_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            event_id = event_id or str(uuid4())
            record = OutboxRecord(event_id, tenant_id, topic, aggregate_id, deepcopy(payload))
            cur.execute(
                """
                INSERT INTO outbox_message (
                  event_id, tenant_id, topic, aggregate_id, payload_json, status,
                  attempts, available_at, locked_by, locked_until, last_error,
                  created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (
                    record.event_id,
                    record.tenant_id,
                    record.topic,
                    record.aggregate_id,
                    json.dumps(record.payload, default=str),
                    record.status,
                    record.attempts,
                    record.available_at,
                    None,
                    None,
                    None,
                    record.created_at,
                    record.updated_at,
                ),
            )
            if cur.fetchone() is not None:
                return record
            cur.execute("SELECT * FROM outbox_message WHERE event_id=%s", (event_id,))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("outbox insert raced but existing record was not found")
            return self._outbox_from_pg(row)

    def claim_outbox(self, owner, limit=10, lease_seconds=180, tenant_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            now = now_utc()
            if tenant_id is not None:
                cur.execute(
                    (
                        "SELECT * FROM outbox_message "
                        "WHERE tenant_id=%s "
                        "AND ((status='pending' AND available_at<=%s) "
                        "OR (status='processing' AND (locked_until IS NULL OR locked_until<=%s))) "
                        "ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED"
                    ),
                    (tenant_id, now, now, int(limit)),
                )
            else:
                cur.execute(
                    (
                        "SELECT * FROM outbox_message "
                        "WHERE (status='pending' AND available_at<=%s) "
                        "OR (status='processing' AND (locked_until IS NULL OR locked_until<=%s)) "
                        "ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED"
                    ),
                    (now, now, int(limit)),
                )
            rows = cur.fetchall()
            result = []
            for row in rows:
                cur.execute(
                    (
                        "UPDATE outbox_message SET status='processing', attempts=attempts+1, "
                        "locked_by=%s, locked_until=%s, updated_at=%s "
                        "WHERE event_id=%s RETURNING *"
                    ),
                    (owner, _lease_until(lease_seconds), now, row[0]),
                )
                result.append(self._outbox_from_pg(cur.fetchone()))
            return result

    def complete_outbox(self, event_id, owner, tenant_id=None):
        self._update_outbox_pg(event_id, owner, "completed", None, None, tenant_id=tenant_id)

    def fail_outbox(self, event_id, owner, error, retry_after_seconds=30, tenant_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            query = (
                "UPDATE outbox_message SET "
                "status=CASE WHEN attempts >= %s THEN %s ELSE %s END, "
                "locked_by=NULL, locked_until=NULL, last_error=%s, "
                "available_at=%s, updated_at=%s "
                "WHERE event_id=%s AND locked_by=%s"
                + (" AND tenant_id=%s" if tenant_id is not None else "")
            )
            params = (
                _max_outbox_attempts(),
                OutboxStatus.DEAD,
                OutboxStatus.PENDING,
                redact_secret_text(str(error))[:1000],
                now_utc() + timedelta(seconds=max(0, retry_after_seconds)),
                now_utc(),
                event_id,
                owner,
            )
            if tenant_id is not None:
                params += (tenant_id,)
            cur.execute(query, params)
            if cur.rowcount != 1:
                raise RuntimeError("outbox ownership lost")

    def _update_outbox_pg(self, event_id, owner, status, error, available_at, tenant_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            query = (
                "UPDATE outbox_message SET status=%s, locked_by=NULL, locked_until=NULL, "
                "last_error=%s, available_at=COALESCE(%s, available_at), updated_at=%s "
                "WHERE event_id=%s AND locked_by=%s"
                + (" AND tenant_id=%s" if tenant_id is not None else "")
            )
            params = (status, error, available_at, now_utc(), event_id, owner)
            if tenant_id is not None:
                params += (tenant_id,)
            cur.execute(query, params)
            if cur.rowcount != 1:
                raise RuntimeError("outbox ownership lost")

    @staticmethod
    def _inbox_from_pg(row):
        return InboxRecord(
            message_id=row[0], tenant_id=row[1], dedupe_key=row[2], session_id=row[3],
            payload=_json_object(row[4]), status=row[5], attempts=int(row[6]), owner=row[7],
            lease_until=row[8], result=_json_object(row[9]) if row[9] is not None else None,
            created_at=row[10], updated_at=row[11],
        )

    @staticmethod
    def _outbox_from_pg(row):
        return OutboxRecord(
            event_id=row[0], tenant_id=row[1], topic=row[2], aggregate_id=row[3],
            payload=_json_object(row[4]), status=row[5], attempts=int(row[6]), available_at=row[7],
            locked_by=row[8], locked_until=row[9], last_error=row[10],
            created_at=row[11], updated_at=row[12],
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


_POSTGRES_INBOX_OUTBOX_METHODS = {
    "accept_inbox": _tenant_from_first_argument,
    "complete_inbox": _tenant_from_first_argument,
    "fail_inbox": _tenant_from_first_argument,
    "enqueue_outbox": _tenant_from_first_argument,
    "claim_outbox": _tenant_from_optional_keyword(3),
    "complete_outbox": _tenant_from_optional_keyword(2),
    "fail_outbox": _tenant_from_optional_keyword(4),
}
for _method_name, _tenant_getter in _POSTGRES_INBOX_OUTBOX_METHODS.items():
    method = rls_tenant_method(_tenant_getter)(getattr(PostgresInboxOutbox, _method_name))
    setattr(
        PostgresInboxOutbox,
        _method_name,
        _retry_postgres_once(method),
    )
del _method_name, _tenant_getter, method
