"""Ordered, fenced session mailbox.

Inbox/Outbox answers whether a message was accepted and how an outbound
effect is delivered.  The mailbox answers which accepted message is allowed
to mutate a session next.  Its sequence and lease are persisted beside the
tenant's durable state.
"""

from __future__ import annotations

import json
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import RLock
from typing import Any

from trpc_service.security.secrets import redact_secret_text
from trpc_service.storage.base import now_utc
from trpc_service.storage.locking import SessionLeaseLost, postgres_advisory_lock
from trpc_service.storage.postgres_rls import (
    _tenant_from_first_argument,
    _tenant_from_optional_keyword,
    postgres_schema_auto_create,
    rls_tenant_method,
)


class MailboxStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    DEAD = "dead"


@dataclass(slots=True)
class MailboxRecord:
    tenant_id: str
    session_id: str
    sequence: int
    message_id: str
    dedupe_key: str
    payload: dict[str, Any]
    status: str = MailboxStatus.PENDING
    attempts: int = 0
    owner: str | None = None
    fencing_token: int = 0
    lease_until: datetime | None = None
    available_at: datetime = field(default_factory=now_utc)
    last_error: str | None = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


def _ttl(seconds: float) -> datetime:
    return now_utc() + timedelta(seconds=max(1, float(seconds)))


def _max_attempts() -> int:
    try:
        return max(1, int(os.getenv("MAILBOX_MAX_ATTEMPTS", "10")))
    except ValueError as exc:
        raise ValueError("MAILBOX_MAX_ATTEMPTS must be an integer") from exc


def _lease_seconds(value: float | None) -> int:
    if value is not None:
        return max(1, int(value))
    try:
        return max(1, int(os.getenv("MAILBOX_LEASE_SECONDS", "180")))
    except ValueError as exc:
        raise ValueError("MAILBOX_LEASE_SECONDS must be an integer") from exc


class InMemoryMailboxStore:
    """Thread-safe mailbox with the same fencing semantics as SQL backends."""

    backend_name = "memory"

    def __init__(self) -> None:
        self._records: dict[tuple[str, str, int], MailboxRecord] = {}
        self._by_message: dict[tuple[str, str], MailboxRecord] = {}
        self._fences: dict[tuple[str, str], int] = {}
        self._lock = RLock()

    def enqueue(
        self,
        tenant_id: str,
        session_id: str,
        message_id: str,
        dedupe_key: str,
        payload: dict[str, Any],
    ) -> MailboxRecord:
        with self._lock:
            existing = self._by_message.get((tenant_id, dedupe_key))
            if existing is not None:
                return deepcopy(existing)
            sequence = max(
                (record.sequence for record in self._records.values()
                 if record.tenant_id == tenant_id and record.session_id == session_id),
                default=0,
            ) + 1
            record = MailboxRecord(
                tenant_id=tenant_id,
                session_id=session_id,
                sequence=sequence,
                message_id=message_id,
                dedupe_key=dedupe_key,
                payload=deepcopy(payload),
            )
            self._records[(tenant_id, session_id, sequence)] = record
            self._by_message[(tenant_id, dedupe_key)] = record
            return deepcopy(record)

    def get(self, tenant_id: str, dedupe_key: str) -> MailboxRecord | None:
        with self._lock:
            record = self._by_message.get((tenant_id, dedupe_key))
            return deepcopy(record) if record else None

    def list_by_tenant(self, tenant_id: str) -> list[MailboxRecord]:
        with self._lock:
            return [
                deepcopy(record)
                for record in sorted(self._records.values(), key=lambda item: (item.session_id, item.sequence))
                if record.tenant_id == tenant_id
            ]

    def restore(self, record: MailboxRecord) -> MailboxRecord:
        with self._lock:
            current = self._by_message.get((record.tenant_id, record.dedupe_key))
            if current is not None:
                return deepcopy(current)
            restored = deepcopy(record)
            if restored.status == MailboxStatus.PROCESSING:
                restored.status = MailboxStatus.PENDING
                restored.owner = None
                restored.lease_until = None
            self._records[(restored.tenant_id, restored.session_id, restored.sequence)] = restored
            self._by_message[(restored.tenant_id, restored.dedupe_key)] = restored
            self._fences[(restored.tenant_id, restored.session_id)] = max(
                self._fences.get((restored.tenant_id, restored.session_id), 0),
                restored.fencing_token,
            )
            return deepcopy(restored)

    def claim_next(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: int | None = None,
    ) -> MailboxRecord | None:
        with self._lock:
            now = now_utc()
            records = sorted(
                (
                    record for record in self._records.values()
                    if record.tenant_id == tenant_id and record.session_id == session_id
                ),
                key=lambda item: item.sequence,
            )
            for record in records:
                if record.status == MailboxStatus.COMPLETED or record.status == MailboxStatus.DEAD:
                    continue
                if record.status == MailboxStatus.PROCESSING and (
                    record.lease_until is None or record.lease_until > now
                ):
                    return None
                if record.status == MailboxStatus.PENDING and record.available_at > now:
                    return None
                token = self._fences.get((tenant_id, session_id), 0) + 1
                self._fences[(tenant_id, session_id)] = token
                record.status = MailboxStatus.PROCESSING
                record.attempts += 1
                record.owner = owner
                record.fencing_token = token
                record.lease_until = _ttl(_lease_seconds(lease_seconds))
                record.updated_at = now
                return deepcopy(record)
            return None

    def renew(self, record: MailboxRecord, lease_seconds: int | None = None) -> MailboxRecord:
        with self._lock:
            current = self._records.get((record.tenant_id, record.session_id, record.sequence))
            self._assert_owner(current, record)
            current.lease_until = _ttl(_lease_seconds(lease_seconds))
            current.updated_at = now_utc()
            return deepcopy(current)

    def complete(self, record: MailboxRecord) -> None:
        with self._lock:
            current = self._records.get((record.tenant_id, record.session_id, record.sequence))
            self._assert_owner(current, record)
            current.status = MailboxStatus.COMPLETED
            current.owner = None
            current.lease_until = None
            current.updated_at = now_utc()

    def fail(self, record: MailboxRecord, error: str, retry_after_seconds: int = 0) -> None:
        with self._lock:
            current = self._records.get((record.tenant_id, record.session_id, record.sequence))
            self._assert_owner(current, record)
            current.status = (
                MailboxStatus.DEAD if current.attempts >= _max_attempts() else MailboxStatus.PENDING
            )
            current.owner = None
            current.lease_until = None
            current.available_at = now_utc() + timedelta(seconds=max(0, retry_after_seconds))
            current.last_error = redact_secret_text(str(error))[:1000]
            current.updated_at = now_utc()

    def recover_expired(
        self,
        tenant_id: str | None = None,
        session_id: str | None = None,
    ) -> int:
        with self._lock:
            now = now_utc()
            recovered = 0
            for record in self._records.values():
                if tenant_id is not None and record.tenant_id != tenant_id:
                    continue
                if session_id is not None and record.session_id != session_id:
                    continue
                if record.status == MailboxStatus.PROCESSING and (
                    record.lease_until is None or record.lease_until <= now
                ):
                    record.status = MailboxStatus.PENDING
                    record.owner = None
                    record.lease_until = None
                    record.available_at = now
                    record.updated_at = now
                    recovered += 1
            return recovered

    @staticmethod
    def _assert_owner(current: MailboxRecord | None, record: MailboxRecord) -> None:
        if (
            current is None
            or current.status != MailboxStatus.PROCESSING
            or current.owner != record.owner
            or current.fencing_token != record.fencing_token
            or current.lease_until is None
            or current.lease_until <= now_utc()
        ):
            raise SessionLeaseLost(
                f"mailbox fencing token rejected: {record.tenant_id}/{record.session_id}/{record.sequence}"
            )


class SQLiteMailboxStore:
    """SQLite mailbox implementation for local production-like deployments."""

    backend_name = "sql"

    def __init__(self, connection, lock: RLock) -> None:
        self._conn = connection
        self._lock = lock
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS mailbox_message (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  sequence INTEGER NOT NULL,
                  message_id TEXT NOT NULL,
                  dedupe_key TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  owner TEXT,
                  fencing_token INTEGER NOT NULL DEFAULT 0,
                  lease_until TEXT,
                  available_at TEXT NOT NULL,
                  last_error TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, session_id, sequence),
                  UNIQUE (tenant_id, dedupe_key),
                  UNIQUE (tenant_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS mailbox_fence (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  fencing_token INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY (tenant_id, session_id)
                );
                CREATE INDEX IF NOT EXISTS idx_mailbox_ready
                  ON mailbox_message(tenant_id, session_id, status, available_at, sequence);
                """
            )

    def enqueue(self, tenant_id, session_id, message_id, dedupe_key, payload):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=? AND dedupe_key=?",
                (tenant_id, dedupe_key),
            ).fetchone()
            if row is not None:
                return self._from_row(row)
            sequence = self._conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM mailbox_message WHERE tenant_id=? AND session_id=?",
                (tenant_id, session_id),
            ).fetchone()[0]
            now = now_utc()
            self._conn.execute(
                """
                INSERT INTO mailbox_message (
                  tenant_id, session_id, sequence, message_id, dedupe_key, payload_json,
                  status, attempts, owner, fencing_token, lease_until, available_at,
                  last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, 0, NULL, ?, NULL, ?, ?)
                """,
                (
                    tenant_id, session_id, int(sequence), message_id, dedupe_key,
                    json.dumps(payload, ensure_ascii=False, default=str),
                    MailboxStatus.PENDING, _iso(now), _iso(now), _iso(now),
                ),
            )
            return self._from_row(
                self._conn.execute(
                    "SELECT * FROM mailbox_message WHERE tenant_id=? AND dedupe_key=?",
                    (tenant_id, dedupe_key),
                ).fetchone()
            )

    def get(self, tenant_id, dedupe_key):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=? AND dedupe_key=?",
                (tenant_id, dedupe_key),
            ).fetchone()
            return self._from_row(row) if row else None

    def list_by_tenant(self, tenant_id):
        with self._lock:
            return [
                self._from_row(row)
                for row in self._conn.execute(
                    "SELECT * FROM mailbox_message WHERE tenant_id=? ORDER BY session_id, sequence",
                    (tenant_id,),
                ).fetchall()
            ]

    def restore(self, record):
        with self._lock, self._conn:
            restored = deepcopy(record)
            if restored.status == MailboxStatus.PROCESSING:
                restored.status = MailboxStatus.PENDING
                restored.owner = None
                restored.lease_until = None
            self._conn.execute(
                """
                INSERT OR IGNORE INTO mailbox_message (
                  tenant_id, session_id, sequence, message_id, dedupe_key, payload_json,
                  status, attempts, owner, fencing_token, lease_until, available_at,
                  last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    restored.tenant_id, restored.session_id, restored.sequence, restored.message_id,
                    restored.dedupe_key, json.dumps(restored.payload, default=str), restored.status,
                    restored.attempts, restored.owner, restored.fencing_token, _iso(restored.lease_until),
                    _iso(restored.available_at), restored.last_error, _iso(restored.created_at),
                    _iso(restored.updated_at),
                ),
            )
            row = self._conn.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=? AND dedupe_key=?",
                (restored.tenant_id, restored.dedupe_key),
            ).fetchone()
            return self._from_row(row)

    def claim_next(self, tenant_id, session_id, owner, lease_seconds=None):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            now = now_utc()
            row = self._conn.execute(
                """
                SELECT * FROM mailbox_message
                WHERE tenant_id=? AND session_id=?
                  AND status IN ('pending', 'processing')
                  AND (
                    (status='pending' AND available_at<=?)
                    OR (status='processing' AND (lease_until IS NULL OR lease_until<=?))
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM mailbox_message earlier
                    WHERE earlier.tenant_id=mailbox_message.tenant_id
                      AND earlier.session_id=mailbox_message.session_id
                      AND earlier.sequence < mailbox_message.sequence
                      AND earlier.status IN ('pending', 'processing')
                  )
                ORDER BY sequence
                LIMIT 1
                """,
                (tenant_id, session_id, _iso(now), _iso(now)),
            ).fetchone()
            if row is None:
                return None
            token = self._next_fence(tenant_id, session_id)
            lease_until = _ttl(_lease_seconds(lease_seconds))
            self._conn.execute(
                """
                UPDATE mailbox_message
                SET status='processing', attempts=attempts+1, owner=?,
                    fencing_token=?, lease_until=?, updated_at=?
                WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (
                    owner, token, _iso(lease_until), _iso(now),
                    tenant_id, session_id, row["sequence"],
                ),
            )
            return self._from_row(
                self._conn.execute(
                    "SELECT * FROM mailbox_message WHERE tenant_id=? AND session_id=? AND sequence=?",
                    (tenant_id, session_id, row["sequence"]),
                ).fetchone()
            )

    def renew(self, record, lease_seconds=None):
        return self._update_lease(record, _ttl(_lease_seconds(lease_seconds)))

    def complete(self, record):
        self._update_owned(record, "completed", None, None)

    def fail(self, record, error, retry_after_seconds=0):
        current = self._owned_row(record)
        status = MailboxStatus.DEAD if int(current["attempts"]) >= _max_attempts() else MailboxStatus.PENDING
        self._update_owned(
            record,
            status,
            redact_secret_text(str(error))[:1000],
            now_utc() + timedelta(seconds=max(0, retry_after_seconds)),
        )

    def recover_expired(self, tenant_id=None, session_id=None):
        with self._lock, self._conn:
            query = (
                "UPDATE mailbox_message SET status='pending', owner=NULL, lease_until=NULL, "
                "available_at=?, updated_at=? WHERE status='processing' AND (lease_until IS NULL OR lease_until<=?"
            )
            params = [_iso(now_utc()), _iso(now_utc()), _iso(now_utc())]
            if tenant_id is not None:
                query += " AND tenant_id=?"
                params.append(tenant_id)
            if session_id is not None:
                query += " AND session_id=?"
                params.append(session_id)
            query += ")"
            return self._conn.execute(query, params).rowcount

    def _next_fence(self, tenant_id, session_id):
        self._conn.execute(
            """
            INSERT INTO mailbox_fence (tenant_id, session_id, fencing_token)
            VALUES (?, ?, 1)
            ON CONFLICT(tenant_id, session_id) DO UPDATE
            SET fencing_token=mailbox_fence.fencing_token + 1
            """,
            (tenant_id, session_id),
        )
        return int(
            self._conn.execute(
                "SELECT fencing_token FROM mailbox_fence WHERE tenant_id=? AND session_id=?",
                (tenant_id, session_id),
            ).fetchone()[0]
        )

    def _owned_row(self, record):
        row = self._conn.execute(
            """
            SELECT * FROM mailbox_message
            WHERE tenant_id=? AND session_id=? AND sequence=?
              AND status='processing' AND owner=? AND fencing_token=?
              AND lease_until > ?
            """,
            (
                record.tenant_id, record.session_id, record.sequence, record.owner,
                record.fencing_token, _iso(now_utc()),
            ),
        ).fetchone()
        if row is None:
            raise SessionLeaseLost(
                f"mailbox fencing token rejected: {record.tenant_id}/{record.session_id}/{record.sequence}"
            )
        return row

    def _update_owned(self, record, status, error, available_at):
        with self._lock, self._conn:
            self._owned_row(record)
            cur = self._conn.execute(
                """
                UPDATE mailbox_message
                SET status=?, owner=NULL, lease_until=NULL, last_error=?,
                    available_at=COALESCE(?, available_at), updated_at=?
                WHERE tenant_id=? AND session_id=? AND sequence=?
                  AND owner=? AND fencing_token=?
                """,
                (
                    status, error, _iso(available_at), _iso(now_utc()),
                    record.tenant_id, record.session_id, record.sequence,
                    record.owner, record.fencing_token,
                ),
            )
            if cur.rowcount != 1:
                raise SessionLeaseLost("mailbox ownership lost")

    def _update_lease(self, record, expires_at):
        with self._lock, self._conn:
            self._owned_row(record)
            cur = self._conn.execute(
                """
                UPDATE mailbox_message SET lease_until=?, updated_at=?
                WHERE tenant_id=? AND session_id=? AND sequence=?
                  AND owner=? AND fencing_token=?
                """,
                (
                    _iso(expires_at), _iso(now_utc()),
                    record.tenant_id, record.session_id, record.sequence,
                    record.owner, record.fencing_token,
                ),
            )
            if cur.rowcount != 1:
                raise SessionLeaseLost("mailbox ownership lost")
            return self._from_row(
                self._conn.execute(
                    "SELECT * FROM mailbox_message WHERE tenant_id=? AND session_id=? AND sequence=?",
                    (record.tenant_id, record.session_id, record.sequence),
                ).fetchone()
            )

    @staticmethod
    def _from_row(row):
        return MailboxRecord(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            sequence=int(row["sequence"]),
            message_id=row["message_id"],
            dedupe_key=row["dedupe_key"],
            payload=json.loads(row["payload_json"]),
            status=row["status"],
            attempts=int(row["attempts"]),
            owner=row["owner"],
            fencing_token=int(row["fencing_token"]),
            lease_until=_parse(row["lease_until"]),
            available_at=_parse(row["available_at"]),
            last_error=row["last_error"],
            created_at=_parse(row["created_at"]),
            updated_at=_parse(row["updated_at"]),
        )


class PostgresMailboxStore(SQLiteMailboxStore):
    """PostgreSQL mailbox using row locks and SKIP LOCKED semantics."""

    backend_name = "postgres"

    def __init__(self, connection, lock: RLock, connection_provider=None) -> None:
        self._connection_provider = connection_provider
        super().__init__(connection, lock)

    def set_connection(self, connection) -> None:
        self._conn = connection

    def _init_schema(self) -> None:
        if not postgres_schema_auto_create():
            return
        with self._lock, postgres_advisory_lock(self._conn, "trpc-agent-mailbox-schema-v1"):
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                CREATE TABLE IF NOT EXISTS mailbox_message (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  sequence BIGINT NOT NULL,
                  message_id TEXT NOT NULL,
                  dedupe_key TEXT NOT NULL,
                  payload_json JSONB NOT NULL,
                  status TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  owner TEXT,
                  fencing_token BIGINT NOT NULL DEFAULT 0,
                  lease_until TIMESTAMPTZ,
                  available_at TIMESTAMPTZ NOT NULL,
                  last_error TEXT,
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, session_id, sequence),
                  UNIQUE (tenant_id, dedupe_key),
                  UNIQUE (tenant_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS mailbox_fence (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  fencing_token BIGINT NOT NULL DEFAULT 0,
                  PRIMARY KEY (tenant_id, session_id)
                );
                CREATE INDEX IF NOT EXISTS idx_mailbox_ready
                  ON mailbox_message(tenant_id, session_id, status, available_at, sequence);
                    """
                )

    def enqueue(self, tenant_id, session_id, message_id, dedupe_key, payload):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=%s AND dedupe_key=%s FOR UPDATE",
                (tenant_id, dedupe_key),
            )
            current = cur.fetchone()
            if current is not None:
                return self._from_pg(current)
            cur.execute(
                """
                INSERT INTO mailbox_fence (tenant_id, session_id, fencing_token)
                VALUES (%s, %s, 0)
                ON CONFLICT (tenant_id, session_id) DO NOTHING
                """,
                (tenant_id, session_id),
            )
            cur.execute(
                "SELECT fencing_token FROM mailbox_fence WHERE tenant_id=%s AND session_id=%s FOR UPDATE",
                (tenant_id, session_id),
            )
            cur.fetchone()
            cur.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM mailbox_message
                WHERE tenant_id=%s AND session_id=%s
                """,
                (tenant_id, session_id),
            )
            sequence = int(cur.fetchone()[0])
            now = now_utc()
            cur.execute(
                """
                INSERT INTO mailbox_message (
                  tenant_id, session_id, sequence, message_id, dedupe_key, payload_json,
                  status, attempts, fencing_token, available_at, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,0,0,%s,%s,%s)
                """,
                (
                    tenant_id, session_id, sequence, message_id, dedupe_key,
                    json.dumps(payload, default=str), MailboxStatus.PENDING,
                    now, now, now,
                ),
            )
            return MailboxRecord(
                tenant_id, session_id, sequence, message_id, dedupe_key, deepcopy(payload),
                created_at=now, updated_at=now,
            )

    def get(self, tenant_id, dedupe_key):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=%s AND dedupe_key=%s",
                (tenant_id, dedupe_key),
            )
            row = cur.fetchone()
            return self._from_pg(row) if row else None

    def list_by_tenant(self, tenant_id):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=%s ORDER BY session_id, sequence",
                (tenant_id,),
            )
            return [self._from_pg(row) for row in cur.fetchall()]

    def restore(self, record):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            restored = deepcopy(record)
            if restored.status == MailboxStatus.PROCESSING:
                restored.status = MailboxStatus.PENDING
                restored.owner = None
                restored.lease_until = None
            cur.execute(
                """
                INSERT INTO mailbox_message (
                  tenant_id, session_id, sequence, message_id, dedupe_key, payload_json,
                  status, attempts, owner, fencing_token, lease_until, available_at,
                  last_error, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, dedupe_key) DO NOTHING
                """,
                (
                    restored.tenant_id, restored.session_id, restored.sequence, restored.message_id,
                    restored.dedupe_key, json.dumps(restored.payload, default=str), restored.status,
                    restored.attempts, restored.owner, restored.fencing_token, restored.lease_until,
                    restored.available_at, restored.last_error, restored.created_at, restored.updated_at,
                ),
            )
            cur.execute(
                "SELECT * FROM mailbox_message WHERE tenant_id=%s AND dedupe_key=%s",
                (restored.tenant_id, restored.dedupe_key),
            )
            return self._from_pg(cur.fetchone())

    def claim_next(self, tenant_id, session_id, owner, lease_seconds=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            now = now_utc()
            cur.execute(
                """
                SELECT mailbox_message.*
                FROM mailbox_message
                WHERE tenant_id=%s AND session_id=%s
                  AND status IN ('pending', 'processing')
                  AND (
                    (status='pending' AND available_at<=%s)
                    OR (status='processing' AND (lease_until IS NULL OR lease_until<=%s))
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM mailbox_message earlier
                    WHERE earlier.tenant_id=mailbox_message.tenant_id
                      AND earlier.session_id=mailbox_message.session_id
                      AND earlier.sequence < mailbox_message.sequence
                      AND earlier.status IN ('pending', 'processing')
                  )
                ORDER BY sequence
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (tenant_id, session_id, now, now),
            )
            row = cur.fetchone()
            if row is None:
                return None
            token = self._next_pg_fence(cur, tenant_id, session_id)
            lease_until = _ttl(_lease_seconds(lease_seconds))
            cur.execute(
                """
                UPDATE mailbox_message
                SET status='processing', attempts=attempts+1, owner=?,
                    fencing_token=?, lease_until=?, updated_at=?
                WHERE tenant_id=? AND session_id=? AND sequence=?
                RETURNING *
                """.replace("?", "%s"),
                (owner, token, lease_until, now, tenant_id, session_id, row[2]),
            )
            return self._from_pg(cur.fetchone())

    def _next_pg_fence(self, cur, tenant_id, session_id):
        cur.execute(
            """
            INSERT INTO mailbox_fence (tenant_id, session_id, fencing_token)
            VALUES (%s,%s,1)
            ON CONFLICT (tenant_id, session_id) DO UPDATE
            SET fencing_token=mailbox_fence.fencing_token+1
            RETURNING fencing_token
            """,
            (tenant_id, session_id),
        )
        return int(cur.fetchone()[0])

    def _owned_pg(self, cur, record):
        cur.execute(
            """
            SELECT * FROM mailbox_message
            WHERE tenant_id=%s AND session_id=%s AND sequence=%s
              AND status='processing' AND owner=%s AND fencing_token=%s
              AND lease_until > CURRENT_TIMESTAMP
            FOR UPDATE
            """,
            (
                record.tenant_id, record.session_id, record.sequence,
                record.owner, record.fencing_token,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise SessionLeaseLost("mailbox ownership lost")
        return row

    def complete(self, record):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            self._owned_pg(cur, record)
            cur.execute(
                """
                UPDATE mailbox_message
                SET status='completed', owner=NULL, lease_until=NULL, updated_at=%s
                WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                  AND owner=%s AND fencing_token=%s
                """,
                (
                    now_utc(), record.tenant_id, record.session_id, record.sequence,
                    record.owner, record.fencing_token,
                ),
            )

    def fail(self, record, error, retry_after_seconds=0):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            current = self._owned_pg(cur, record)
            status = MailboxStatus.DEAD if int(current[7]) >= _max_attempts() else MailboxStatus.PENDING
            cur.execute(
                """
                UPDATE mailbox_message
                SET status=%s, owner=NULL, lease_until=NULL, available_at=%s,
                    last_error=%s, updated_at=%s
                WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                  AND owner=%s AND fencing_token=%s
                """,
                (
                    status, now_utc() + timedelta(seconds=max(0, retry_after_seconds)),
                    redact_secret_text(str(error))[:1000], now_utc(),
                    record.tenant_id, record.session_id, record.sequence,
                    record.owner, record.fencing_token,
                ),
            )

    def renew(self, record, lease_seconds=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            self._owned_pg(cur, record)
            expires = _ttl(_lease_seconds(lease_seconds))
            cur.execute(
                """
                UPDATE mailbox_message SET lease_until=%s, updated_at=%s
                WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                  AND owner=%s AND fencing_token=%s
                RETURNING *
                """,
                (
                    expires, now_utc(), record.tenant_id, record.session_id, record.sequence,
                    record.owner, record.fencing_token,
                ),
            )
            return self._from_pg(cur.fetchone())

    def recover_expired(self, tenant_id=None, session_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            query = (
                "UPDATE mailbox_message SET status='pending', owner=NULL, lease_until=NULL, "
                "available_at=%s, updated_at=%s WHERE status='processing' "
                "AND (lease_until IS NULL OR lease_until<=CURRENT_TIMESTAMP)"
            )
            params: list[Any] = [now_utc(), now_utc()]
            if tenant_id is not None:
                query += " AND tenant_id=%s"
                params.append(tenant_id)
            if session_id is not None:
                query += " AND session_id=%s"
                params.append(session_id)
            cur.execute(query, params)
            return cur.rowcount

    @staticmethod
    def _from_pg(row):
        return MailboxRecord(
            tenant_id=row[0], session_id=row[1], sequence=int(row[2]), message_id=row[3],
            dedupe_key=row[4], payload=_json_object(row[5]), status=row[6],
            attempts=int(row[7]), owner=row[8], fencing_token=int(row[9]),
            lease_until=row[10], available_at=row[11], last_error=row[12],
            created_at=row[13], updated_at=row[14],
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return dict(value)


_POSTGRES_MAILBOX_METHODS = {
    "enqueue": _tenant_from_first_argument,
    "get": _tenant_from_first_argument,
    "list_by_tenant": _tenant_from_first_argument,
    "claim_next": _tenant_from_first_argument,
    "recover_expired": _tenant_from_optional_keyword(0),
    "complete": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args else getattr(kwargs.get("record"), "tenant_id", None),
    "fail": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args else getattr(kwargs.get("record"), "tenant_id", None),
    "renew": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args else getattr(kwargs.get("record"), "tenant_id", None),
    "restore": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args else getattr(kwargs.get("record"), "tenant_id", None),
}
for _method_name, _tenant_getter in _POSTGRES_MAILBOX_METHODS.items():
    setattr(
        PostgresMailboxStore,
        _method_name,
        rls_tenant_method(_tenant_getter)(getattr(PostgresMailboxStore, _method_name)),
    )
del _method_name, _tenant_getter
