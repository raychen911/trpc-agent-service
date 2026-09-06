"""Durable PostgreSQL migration coordination primitives.

The file-backed migration state machine remains useful for local SQLite
development.  PostgreSQL deployments additionally need a shared lease,
monotonic fencing epoch, durable cursors, and a write barrier so a restarted
coordinator cannot continue with stale authority.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Any
from uuid import uuid4

from trpc_service.storage.base import now_utc
from trpc_service.storage.locking import postgres_advisory_lock


class MigrationControlError(RuntimeError):
    """Base error for durable migration coordination."""


class MigrationLeaseBusy(MigrationControlError):
    """Raised when another live coordinator owns the migration lease."""


class MigrationLeaseLost(MigrationControlError):
    """Raised when a lease or fencing epoch is no longer authoritative."""


class MigrationCheckpointConflict(MigrationControlError):
    """Raised when a checkpoint checksum or identity does not match."""


@dataclass(frozen=True)
class MigrationLease:
    tenant_id: str
    migration_id: str
    owner_id: str
    owner_instance: str
    lease_epoch: int
    expires_at: datetime


@dataclass(frozen=True)
class MigrationCheckpoint:
    tenant_id: str
    migration_id: str
    phase: str
    batch_key: str
    cursor: dict[str, Any]
    source_count: int = 0
    target_count: int = 0
    status: str = "running"
    checksum: str = ""
    updated_at: datetime | None = None


def _required(value: str, label: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} cannot be empty")
    if len(value) > maximum:
        raise ValueError(f"{label} exceeds {maximum} characters")
    return value.strip()


def _lease_seconds(value: float | None) -> int:
    if value is None:
        value = os.getenv("MIGRATION_LEASE_SECONDS", "60")
    try:
        seconds = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("migration lease seconds must be an integer") from exc
    if seconds < 5 or seconds > 3600:
        raise ValueError("migration lease seconds must be between 5 and 3600")
    return seconds


def _checksum(cursor: dict[str, Any]) -> str:
    encoded = json.dumps(
        cursor,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def ensure_migration_control_schema(connection, lock: RLock | None = None) -> None:
    """Create the control-plane tables using the schema-owner connection."""

    guard = lock or RLock()
    with guard, postgres_advisory_lock(connection, "trpc-agent-migration-control-v1"), connection.cursor() as cur:
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS migration_lease (
                  tenant_id TEXT NOT NULL,
                  migration_id TEXT NOT NULL,
                  owner_id TEXT NOT NULL,
                  owner_instance TEXT NOT NULL,
                  lease_epoch BIGINT NOT NULL DEFAULT 1,
                  expires_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, migration_id)
                );
                CREATE TABLE IF NOT EXISTS migration_write_barrier (
                  tenant_id TEXT NOT NULL,
                  migration_id TEXT NOT NULL,
                  owner_instance TEXT NOT NULL,
                  lease_epoch BIGINT NOT NULL,
                  mode TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, migration_id)
                );
                CREATE TABLE IF NOT EXISTS migration_checkpoint (
                  tenant_id TEXT NOT NULL,
                  migration_id TEXT NOT NULL,
                  phase TEXT NOT NULL,
                  batch_key TEXT NOT NULL,
                  cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                  source_count BIGINT NOT NULL DEFAULT 0,
                  target_count BIGINT NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'running',
                  checksum TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, migration_id, phase, batch_key)
                );
                CREATE INDEX IF NOT EXISTS idx_migration_checkpoint_status
                  ON migration_checkpoint(tenant_id, migration_id, status, updated_at);
                """
        )


class PostgresMigrationControl:
    """Synchronous, fenced migration control over an existing psycopg connection."""

    backend_name = "postgres"

    def __init__(self, connection, lock: RLock | None = None, connection_provider=None) -> None:
        self._conn = connection
        self._lock = lock or RLock()
        self._connection_provider = connection_provider

    def set_connection(self, connection) -> None:
        self._conn = connection

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        with self._lock, self._conn.transaction():
            yield self._conn

    def acquire(
        self,
        tenant_id: str,
        migration_id: str,
        owner_id: str,
        owner_instance: str | None = None,
        lease_seconds: float | None = None,
    ) -> MigrationLease:
        tenant_id = _required(tenant_id, "tenant id")
        migration_id = _required(migration_id, "migration id")
        owner_id = _required(owner_id, "migration owner")
        owner_instance = _required(owner_instance or str(uuid4()), "owner instance")
        seconds = _lease_seconds(lease_seconds)
        now = now_utc()
        expires = now + timedelta(seconds=seconds)
        with self._transaction() as connection, connection.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO migration_lease (
                      tenant_id,migration_id,owner_id,owner_instance,lease_epoch,
                      expires_at,updated_at
                    ) VALUES (%s,%s,%s,%s,1,%s,%s)
                    ON CONFLICT (tenant_id,migration_id) DO NOTHING
                    """,
                (tenant_id, migration_id, owner_id, owner_instance, expires, now),
            )
            cur.execute(
                """
                    SELECT owner_id,owner_instance,lease_epoch,expires_at
                      FROM migration_lease
                     WHERE tenant_id=%s AND migration_id=%s
                     FOR UPDATE
                    """,
                (tenant_id, migration_id),
            )
            row = cur.fetchone()
            if row is None:
                raise MigrationControlError("migration lease disappeared during acquisition")
            current_owner, current_instance, epoch, current_expires = row
            live = current_expires > now
            same_owner = current_owner == owner_id and current_instance == owner_instance
            if live and not same_owner:
                raise MigrationLeaseBusy(
                    f"migration lease is held by {current_owner}/{current_instance}"
                )
            if same_owner:
                next_epoch = int(epoch)
            else:
                next_epoch = int(epoch) + 1
            cur.execute(
                """
                    UPDATE migration_lease
                       SET owner_id=%s,owner_instance=%s,lease_epoch=%s,
                           expires_at=%s,updated_at=%s
                     WHERE tenant_id=%s AND migration_id=%s
                    """,
                (
                    owner_id, owner_instance, next_epoch, expires, now,
                    tenant_id, migration_id,
                ),
            )
            self._set_barrier(cur, tenant_id, migration_id, owner_instance, next_epoch, now)
            return MigrationLease(
                tenant_id, migration_id, owner_id, owner_instance,
                next_epoch, expires,
            )

    def renew(self, lease: MigrationLease, lease_seconds: float | None = None) -> MigrationLease:
        seconds = _lease_seconds(lease_seconds)
        now = now_utc()
        expires = now + timedelta(seconds=seconds)
        with self._transaction() as connection, connection.cursor() as cur:
            self._assert_fence_cursor(cur, lease, now)
            cur.execute(
                """
                    UPDATE migration_lease
                       SET expires_at=%s,updated_at=%s
                     WHERE tenant_id=%s AND migration_id=%s
                       AND owner_id=%s AND owner_instance=%s AND lease_epoch=%s
                    """,
                (
                    expires, now, lease.tenant_id, lease.migration_id,
                    lease.owner_id, lease.owner_instance, lease.lease_epoch,
                ),
            )
            self._set_barrier(
                cur, lease.tenant_id, lease.migration_id,
                lease.owner_instance, lease.lease_epoch, now,
            )
        return MigrationLease(
            lease.tenant_id, lease.migration_id, lease.owner_id,
            lease.owner_instance, lease.lease_epoch, expires,
        )

    def release(self, lease: MigrationLease) -> None:
        now = now_utc()
        with self._transaction() as connection, connection.cursor() as cur:
            self._assert_fence_cursor(cur, lease, now)
            cur.execute(
                """
                    UPDATE migration_lease SET expires_at=%s,updated_at=%s
                     WHERE tenant_id=%s AND migration_id=%s
                       AND owner_id=%s AND owner_instance=%s AND lease_epoch=%s
                    """,
                (
                    now, now, lease.tenant_id, lease.migration_id,
                    lease.owner_id, lease.owner_instance, lease.lease_epoch,
                ),
            )
            cur.execute(
                """
                    UPDATE migration_write_barrier SET mode='released',updated_at=%s
                     WHERE tenant_id=%s AND migration_id=%s
                       AND owner_instance=%s AND lease_epoch=%s
                    """,
                (
                    now, lease.tenant_id, lease.migration_id,
                    lease.owner_instance, lease.lease_epoch,
                ),
            )

    def assert_fence(self, lease: MigrationLease) -> None:
        with self._transaction() as connection, connection.cursor() as cur:
            self._assert_fence_cursor(cur, lease, now_utc())

    def assert_write_allowed(
        self,
        tenant_id: str,
        migration_id: str,
        owner_instance: str,
        lease_epoch: int,
    ) -> None:
        """Reject writes from a stale migration coordinator."""

        tenant_id = _required(tenant_id, "tenant id")
        migration_id = _required(migration_id, "migration id")
        with self._transaction() as connection, connection.cursor() as cur:
            cur.execute(
                """
                    SELECT 1
                      FROM migration_lease AS l
                      JOIN migration_write_barrier AS b
                        ON b.tenant_id=l.tenant_id AND b.migration_id=l.migration_id
                       AND b.owner_instance=l.owner_instance AND b.lease_epoch=l.lease_epoch
                       AND b.mode='active'
                     WHERE l.tenant_id=%s AND l.migration_id=%s
                       AND l.owner_instance=%s AND l.lease_epoch=%s
                       AND l.expires_at>%s
                     FOR UPDATE
                    """,
                (tenant_id, migration_id, owner_instance, int(lease_epoch), now_utc()),
            )
            row = cur.fetchone()
            if row is None:
                raise MigrationLeaseLost("migration write barrier rejected stale authority")

    def save_checkpoint(
        self,
        lease: MigrationLease,
        phase: str,
        batch_key: str,
        cursor: dict[str, Any],
        *,
        source_count: int = 0,
        target_count: int = 0,
        status: str = "running",
    ) -> MigrationCheckpoint:
        phase = _required(phase, "migration phase", 64)
        batch_key = _required(batch_key, "migration batch key", 256)
        if not isinstance(cursor, dict):
            raise TypeError("migration checkpoint cursor must be an object")
        if source_count < 0 or target_count < 0:
            raise ValueError("migration checkpoint counts cannot be negative")
        checksum = _checksum(cursor)
        now = now_utc()
        with self._transaction() as connection, connection.cursor() as cur:
            self._assert_fence_cursor(cur, lease, now)
            cur.execute(
                """
                    INSERT INTO migration_checkpoint (
                      tenant_id,migration_id,phase,batch_key,cursor_json,
                      source_count,target_count,status,checksum,updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,migration_id,phase,batch_key) DO UPDATE SET
                      cursor_json=EXCLUDED.cursor_json,
                      source_count=EXCLUDED.source_count,
                      target_count=EXCLUDED.target_count,
                      status=EXCLUDED.status,
                      checksum=EXCLUDED.checksum,
                      updated_at=EXCLUDED.updated_at
                    """,
                (
                    lease.tenant_id, lease.migration_id, phase, batch_key,
                    json.dumps(cursor, ensure_ascii=False, default=str),
                    int(source_count), int(target_count), status, checksum, now,
                ),
            )
        return MigrationCheckpoint(
            lease.tenant_id, lease.migration_id, phase, batch_key,
            dict(cursor), int(source_count), int(target_count), status, checksum, now,
        )

    def load_checkpoint(
        self, tenant_id: str, migration_id: str, phase: str, batch_key: str
    ) -> MigrationCheckpoint | None:
        with self._transaction() as connection, connection.cursor() as cur:
            cur.execute(
                """
                    SELECT cursor_json,source_count,target_count,status,checksum,updated_at
                      FROM migration_checkpoint
                     WHERE tenant_id=%s AND migration_id=%s
                       AND phase=%s AND batch_key=%s
                    """,
                (tenant_id, migration_id, phase, batch_key),
            )
            row = cur.fetchone()
        if row is None:
            return None
        cursor = row[0]
        if isinstance(cursor, str):
            cursor = json.loads(cursor)
        if not isinstance(cursor, dict) or row[4] != _checksum(cursor):
            raise MigrationCheckpointConflict("migration checkpoint checksum mismatch")
        return MigrationCheckpoint(
            tenant_id, migration_id, phase, batch_key, dict(cursor),
            int(row[1]), int(row[2]), row[3], row[4], row[5],
        )

    @staticmethod
    def _set_barrier(cur, tenant_id, migration_id, owner_instance, epoch, updated_at) -> None:
        cur.execute(
            """
            INSERT INTO migration_write_barrier (
              tenant_id,migration_id,owner_instance,lease_epoch,mode,updated_at
            ) VALUES (%s,%s,%s,%s,'active',%s)
            ON CONFLICT (tenant_id,migration_id) DO UPDATE SET
              owner_instance=EXCLUDED.owner_instance,
              lease_epoch=EXCLUDED.lease_epoch,
              mode='active',updated_at=EXCLUDED.updated_at
            """,
            (tenant_id, migration_id, owner_instance, int(epoch), updated_at),
        )

    @staticmethod
    def _assert_fence_cursor(cur, lease: MigrationLease, now: datetime) -> None:
        cur.execute(
            """
            SELECT 1
              FROM migration_lease AS l
              JOIN migration_write_barrier AS b
                ON b.tenant_id=l.tenant_id AND b.migration_id=l.migration_id
               AND b.owner_instance=l.owner_instance AND b.lease_epoch=l.lease_epoch
               AND b.mode='active'
             WHERE l.tenant_id=%s AND l.migration_id=%s
               AND l.owner_id=%s AND l.owner_instance=%s AND l.lease_epoch=%s
               AND l.expires_at>%s
             FOR UPDATE
            """,
            (
                lease.tenant_id, lease.migration_id, lease.owner_id,
                lease.owner_instance, lease.lease_epoch, now,
            ),
        )
        if cur.fetchone() is None:
            raise MigrationLeaseLost("migration lease or write barrier is stale")


def _connection_error(exc: Exception) -> bool:
    module = exc.__class__.__module__
    return module.startswith("psycopg") and exc.__class__.__name__ in {
        "OperationalError", "InterfaceError", "AdminShutdown", "ConnectionTimeout",
    }


__all__ = [
    "MigrationCheckpoint",
    "MigrationCheckpointConflict",
    "MigrationControlError",
    "MigrationLease",
    "MigrationLeaseBusy",
    "MigrationLeaseLost",
    "PostgresMigrationControl",
    "ensure_migration_control_schema",
]
