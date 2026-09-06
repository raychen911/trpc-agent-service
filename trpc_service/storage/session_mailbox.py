"""Session-level ordered mailbox with durable wake-up generations.

The legacy mailbox stores one lease per message.  This module adds the
production-shaped session aggregate used to serialize a whole session:
PostgreSQL/SQLite remain authoritative for ordering and fencing, while the
``session.ready.v2`` outbox record is only a reconstructible wake-up.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from threading import RLock
from typing import Any

from trpc_service.security.secrets import redact_secret_text
from trpc_service.storage.base import now_utc
from trpc_service.storage.durable import OutboxRecord
from trpc_service.storage.locking import SessionLeaseLost


class SessionMailboxStatus:
    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"


class SessionMailboxClaimStatus:
    CLAIMED = "claimed"
    RUNNING = "running"
    STALE = "stale"
    EMPTY = "empty"


@dataclass(slots=True)
class SessionMailbox:
    tenant_id: str
    session_id: str
    status: str = SessionMailboxStatus.IDLE
    accepted_sequence: int = 0
    resolved_sequence: int = 0
    processing_sequence: int | None = None
    processing_message_id: str | None = None
    queue_generation: int = 0
    lease_owner: str | None = None
    lease_epoch: int = 0
    lease_until: datetime | None = None
    retry_count: int = 0
    attempt: int = 0
    priority: int = 0
    retry_at: datetime | None = None
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class SessionMailboxItem:
    tenant_id: str
    session_id: str
    sequence: int
    message_id: str
    trace_id: str
    priority: int = 0
    retry_count: int = 0
    attempt: int = 0
    retry_at: datetime | None = None
    accepted_at: datetime = field(default_factory=now_utc)
    resolved_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SessionMailboxLease:
    tenant_id: str
    session_id: str
    message_id: str
    sequence: int
    owner: str
    epoch: int
    expires_at: datetime
    attempt: int
    retry_count: int
    priority: int

    @property
    def fencing_token(self) -> int:
        return self.epoch


@dataclass(slots=True)
class SessionMailboxClaim:
    status: str
    mailbox: SessionMailbox
    lease: SessionMailboxLease | None = None

    @property
    def claimed(self) -> bool:
        return self.status == SessionMailboxClaimStatus.CLAIMED


def _validate_ids(*values: str) -> None:
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError("session mailbox identifiers must be non-empty strings")


def _validate_priority(priority: int) -> None:
    if isinstance(priority, bool) or not isinstance(priority, int) or priority < 0:
        raise ValueError("session mailbox priority must be non-negative")


def _validate_lease(seconds: float) -> float:
    value = float(seconds)
    if value <= 0:
        raise ValueError("session mailbox lease must be positive")
    return value


def _copy(value):
    return deepcopy(value)


def validate_session_mailbox(mailbox: SessionMailbox) -> None:
    """Reject persisted aggregate states that violate ordering or lease invariants."""

    _validate_ids(mailbox.tenant_id, mailbox.session_id)
    if not 0 <= mailbox.resolved_sequence <= mailbox.accepted_sequence:
        raise ValueError("session mailbox sequence counters are inconsistent")
    if mailbox.queue_generation < 0 or mailbox.lease_epoch < 0:
        raise ValueError("session mailbox generations must be non-negative")
    running = mailbox.status == SessionMailboxStatus.RUNNING
    lease_fields = (
        mailbox.processing_sequence,
        mailbox.processing_message_id,
        mailbox.lease_owner,
        mailbox.lease_until,
    )
    if running:
        if any(value is None for value in lease_fields):
            raise ValueError("running session mailbox requires complete lease ownership")
        if mailbox.processing_sequence != mailbox.resolved_sequence + 1:
            raise ValueError("running session mailbox must process the next sequence")
    elif any(value is not None for value in lease_fields):
        raise ValueError("non-running session mailbox cannot retain lease ownership")
    if mailbox.status == SessionMailboxStatus.IDLE and (
        mailbox.accepted_sequence != mailbox.resolved_sequence
    ):
        raise ValueError("idle session mailbox cannot contain unresolved messages")
    if mailbox.status in (SessionMailboxStatus.QUEUED, SessionMailboxStatus.RETRY_WAIT) and (
        mailbox.accepted_sequence <= mailbox.resolved_sequence
    ):
        raise ValueError("queued session mailbox requires an unresolved message")
    if mailbox.status == SessionMailboxStatus.RETRY_WAIT and mailbox.retry_at is None:
        raise ValueError("retry-wait session mailbox requires retry_at")


class InMemorySessionMailboxStore:
    """Thread-safe reference implementation for local mode and contract tests."""

    backend_name = "memory-v2"

    def __init__(self) -> None:
        self._mailboxes: dict[tuple[str, str], SessionMailbox] = {}
        self._items: dict[tuple[str, str, int], SessionMailboxItem] = {}
        self._outbox: dict[str, OutboxRecord] = {}
        self._lock = RLock()

    @property
    def outbox(self) -> list[OutboxRecord]:
        with self._lock:
            return [_copy(item) for item in self._outbox.values()]

    def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._lock:
            value = self._mailboxes.get((tenant_id, session_id))
            if value is not None:
                validate_session_mailbox(value)
            return _copy(value) if value else None

    def has_unresolved_message(
        self, tenant_id: str, session_id: str, message_id: str
    ) -> bool:
        with self._lock:
            return any(
                item.tenant_id == tenant_id
                and item.session_id == session_id
                and item.message_id == message_id
                and item.resolved_at is None
                for item in self._items.values()
            )

    def export_by_tenant(self, tenant_id: str) -> dict[str, list[dict]]:
        with self._lock:
            return {
                "mailboxes": [
                    asdict(_copy(value))
                    for value in self._mailboxes.values()
                    if value.tenant_id == tenant_id
                ],
                "items": [
                    asdict(_copy(value))
                    for value in self._items.values()
                    if value.tenant_id == tenant_id
                ],
            }

    def restore_export(self, payload: dict[str, list[dict]]) -> None:
        with self._lock:
            for raw in payload.get("mailboxes", []):
                mailbox = SessionMailbox(**_restore_datetimes(raw))
                self._mailboxes[(mailbox.tenant_id, mailbox.session_id)] = mailbox
            for raw in payload.get("items", []):
                item = SessionMailboxItem(**_restore_datetimes(raw))
                self._items[(item.tenant_id, item.session_id, item.sequence)] = item

    def accept(
        self,
        tenant_id: str,
        session_id: str,
        message_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
    ) -> SessionMailbox:
        _validate_ids(tenant_id, session_id, message_id)
        _validate_priority(priority)
        with self._lock:
            key = (tenant_id, session_id)
            mailbox = self._mailboxes.setdefault(
                key, SessionMailbox(tenant_id, session_id)
            )
            duplicate = next(
                (
                    item
                    for item in self._items.values()
                    if item.tenant_id == tenant_id
                    and item.session_id == session_id
                    and item.message_id == message_id
                ),
                None,
            )
            if duplicate is not None:
                return _copy(mailbox)
            sequence = mailbox.accepted_sequence + 1
            self._items[(tenant_id, session_id, sequence)] = SessionMailboxItem(
                tenant_id=tenant_id,
                session_id=session_id,
                sequence=sequence,
                message_id=message_id,
                trace_id=trace_id or message_id,
                priority=priority,
                retry_at=retry_at,
            )
            previous = mailbox.status
            head_waiting = (
                previous == SessionMailboxStatus.RETRY_WAIT
                and mailbox.retry_at is not None
                and mailbox.retry_at > now_utc()
            )
            if previous == SessionMailboxStatus.IDLE:
                status = (
                    SessionMailboxStatus.RETRY_WAIT
                    if retry_at is not None and retry_at > now_utc()
                    else SessionMailboxStatus.QUEUED
                )
            elif previous == SessionMailboxStatus.RETRY_WAIT and not head_waiting:
                status = SessionMailboxStatus.QUEUED
            else:
                status = previous
            generation = mailbox.queue_generation
            if status == SessionMailboxStatus.QUEUED and previous in (
                SessionMailboxStatus.IDLE,
                SessionMailboxStatus.RETRY_WAIT,
            ):
                generation += 1
            updated = _copy(mailbox)
            updated.accepted_sequence = sequence
            updated.status = status
            updated.queue_generation = generation
            updated.priority = max(mailbox.priority, priority)
            updated.retry_at = mailbox.retry_at if head_waiting else (
                retry_at if status == SessionMailboxStatus.RETRY_WAIT else None
            )
            updated.updated_at = now_utc()
            self._mailboxes[key] = updated
            if generation != mailbox.queue_generation:
                self._emit_ready(updated, self._items[(tenant_id, session_id, sequence)])
            return _copy(updated)

    def claim(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: float,
    ) -> SessionMailboxLease | None:
        _validate_ids(tenant_id, session_id, owner)
        seconds = _validate_lease(lease_seconds)
        with self._lock:
            key = (tenant_id, session_id)
            mailbox = self._mailboxes.get(key)
            if mailbox is None:
                return None
            now = now_utc()
            if mailbox.lease_until is not None and mailbox.lease_until > now:
                return None
            sequence = mailbox.processing_sequence or mailbox.resolved_sequence + 1
            if sequence > mailbox.accepted_sequence:
                mailbox.status = SessionMailboxStatus.IDLE
                mailbox.updated_at = now
                return None
            item = self._items.get((tenant_id, session_id, sequence))
            if item is None:
                raise RuntimeError("session mailbox item is missing")
            if item.retry_at is not None and item.retry_at > now:
                mailbox.status = SessionMailboxStatus.RETRY_WAIT
                mailbox.retry_at = item.retry_at
                mailbox.updated_at = now
                return None
            epoch = mailbox.lease_epoch + 1
            item.attempt += 1
            expires = now + timedelta(seconds=seconds)
            mailbox.status = SessionMailboxStatus.RUNNING
            mailbox.processing_sequence = sequence
            mailbox.processing_message_id = item.message_id
            mailbox.lease_owner = owner
            mailbox.lease_epoch = epoch
            mailbox.lease_until = expires
            mailbox.attempt = item.attempt
            mailbox.retry_count = item.retry_count
            mailbox.priority = item.priority
            mailbox.retry_at = item.retry_at
            mailbox.updated_at = now
            return SessionMailboxLease(
                tenant_id,
                session_id,
                item.message_id,
                sequence,
                owner,
                epoch,
                expires,
                item.attempt,
                item.retry_count,
                item.priority,
            )

    def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: float,
        *,
        expected_generation: int | None = None,
    ) -> SessionMailboxClaim:
        with self._lock:
            mailbox = self._mailboxes.get(
                (tenant_id, session_id),
                SessionMailbox(tenant_id, session_id),
            )
            if expected_generation is not None and mailbox.queue_generation != expected_generation:
                return SessionMailboxClaim(
                    SessionMailboxClaimStatus.STALE, _copy(mailbox)
                )
        lease = self.claim(tenant_id, session_id, owner, lease_seconds)
        mailbox = self.get(tenant_id, session_id) or SessionMailbox(tenant_id, session_id)
        if lease is not None:
            return SessionMailboxClaim(
                SessionMailboxClaimStatus.CLAIMED, mailbox, lease
            )
        now = now_utc()
        if mailbox.lease_until is not None and mailbox.lease_until > now:
            status = SessionMailboxClaimStatus.RUNNING
        elif expected_generation is not None and mailbox.queue_generation != expected_generation:
            status = SessionMailboxClaimStatus.STALE
        else:
            status = SessionMailboxClaimStatus.EMPTY
        return SessionMailboxClaim(status, mailbox)

    def renew(self, lease: SessionMailboxLease, lease_seconds: float) -> SessionMailboxLease:
        seconds = _validate_lease(lease_seconds)
        with self._lock:
            mailbox = self._require_owned(lease)
            now = now_utc()
            expires = max(now + timedelta(seconds=seconds), mailbox.lease_until or now)
            mailbox.lease_until = expires
            mailbox.updated_at = now
            return SessionMailboxLease(
                lease.tenant_id,
                lease.session_id,
                lease.message_id,
                lease.sequence,
                lease.owner,
                lease.epoch,
                expires,
                lease.attempt,
                lease.retry_count,
                lease.priority,
            )

    def commit(self, lease: SessionMailboxLease) -> SessionMailbox:
        with self._lock:
            return self._resolve_owned(lease)

    def dead_letter(self, lease: SessionMailboxLease, error: str) -> SessionMailbox:
        with self._lock:
            return self._resolve_owned(lease, error=error)

    def retry(
        self,
        lease: SessionMailboxLease,
        *,
        retry_at: datetime | None = None,
        increment_retry: bool = True,
    ) -> SessionMailbox:
        with self._lock:
            mailbox = self._require_owned(lease)
            item = self._items[(lease.tenant_id, lease.session_id, lease.sequence)]
            if increment_retry:
                item.retry_count += 1
            item.retry_at = retry_at
            mailbox.retry_count = item.retry_count
            mailbox.retry_at = retry_at
            mailbox.processing_sequence = None
            mailbox.processing_message_id = None
            mailbox.lease_owner = None
            mailbox.lease_until = None
            if retry_at is not None and retry_at > now_utc():
                mailbox.status = SessionMailboxStatus.RETRY_WAIT
            else:
                mailbox.status = SessionMailboxStatus.QUEUED
                mailbox.queue_generation += 1
                self._emit_ready(mailbox, item)
            mailbox.updated_at = now_utc()
            return _copy(mailbox)

    def recover(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._lock:
            mailbox = self._mailboxes.get((tenant_id, session_id))
            if mailbox is None or mailbox.lease_until is None or mailbox.lease_until > now_utc():
                return None
            lease = SessionMailboxLease(
                tenant_id,
                session_id,
                mailbox.processing_message_id or "",
                mailbox.processing_sequence or mailbox.resolved_sequence + 1,
                mailbox.lease_owner or "",
                mailbox.lease_epoch,
                mailbox.lease_until,
                mailbox.attempt,
                mailbox.retry_count,
                mailbox.priority,
            )
            item = self._items.get((tenant_id, session_id, lease.sequence))
            mailbox.processing_sequence = None
            mailbox.processing_message_id = None
            mailbox.lease_owner = None
            mailbox.lease_until = None
            if item is None or lease.sequence > mailbox.accepted_sequence:
                mailbox.status = SessionMailboxStatus.IDLE
            elif item.retry_at is not None and item.retry_at > now_utc():
                mailbox.status = SessionMailboxStatus.RETRY_WAIT
                mailbox.retry_at = item.retry_at
            else:
                mailbox.status = SessionMailboxStatus.QUEUED
                mailbox.retry_at = None
                mailbox.queue_generation += 1
                self._emit_ready(mailbox, item)
            mailbox.updated_at = now_utc()
            return _copy(mailbox)

    def reconcile(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._lock:
            mailbox = self._mailboxes.get((tenant_id, session_id))
            if mailbox is None:
                return None
            if mailbox.status == SessionMailboxStatus.RUNNING and (
                mailbox.lease_until is not None and mailbox.lease_until > now_utc()
            ):
                return _copy(mailbox)
        return self.recover(tenant_id, session_id)

    def sweep_expired_leases(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        with self._lock:
            candidates = [
                key
                for key, mailbox in self._mailboxes.items()
                if (tenant_id is None or mailbox.tenant_id == tenant_id)
                and mailbox.lease_until is not None
                and mailbox.lease_until <= now_utc()
            ][:limit]
        return sum(self.recover(*key) is not None for key in candidates)

    def schedule_retries(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        handled = 0
        with self._lock:
            now = now_utc()
            for key, mailbox in tuple(self._mailboxes.items()):
                if handled >= limit:
                    break
                if (
                    (tenant_id is not None and mailbox.tenant_id != tenant_id)
                    or
                    mailbox.status != SessionMailboxStatus.RETRY_WAIT
                    or mailbox.retry_at is None
                    or mailbox.retry_at > now
                ):
                    continue
                item = self._items.get((*key, mailbox.resolved_sequence + 1))
                if item is None:
                    mailbox.status = SessionMailboxStatus.IDLE
                    mailbox.retry_at = None
                else:
                    mailbox.status = SessionMailboxStatus.QUEUED
                    mailbox.retry_at = None
                    mailbox.queue_generation += 1
                    self._emit_ready(mailbox, item)
                mailbox.updated_at = now
                handled += 1
        return handled

    def reconcile_sessions(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        handled = 0
        with self._lock:
            for key, mailbox in tuple(self._mailboxes.items()):
                if handled >= limit:
                    break
                if (
                    (tenant_id is not None and mailbox.tenant_id != tenant_id)
                    or mailbox.status != SessionMailboxStatus.QUEUED
                ):
                    continue
                item = self._items.get((*key, mailbox.resolved_sequence + 1))
                if item is not None:
                    self._emit_ready(mailbox, item)
                    handled += 1
        return handled

    def _require_owned(self, lease: SessionMailboxLease) -> SessionMailbox:
        mailbox = self._mailboxes.get((lease.tenant_id, lease.session_id))
        now = now_utc()
        if (
            mailbox is None
            or mailbox.status != SessionMailboxStatus.RUNNING
            or mailbox.processing_sequence != lease.sequence
            or mailbox.processing_message_id != lease.message_id
            or mailbox.lease_owner != lease.owner
            or mailbox.lease_epoch != lease.epoch
            or mailbox.lease_until is None
            or mailbox.lease_until <= now
        ):
            raise SessionLeaseLost(
                f"session mailbox fencing rejected: {lease.tenant_id}/{lease.session_id}"
            )
        return mailbox

    def _resolve_owned(
        self,
        lease: SessionMailboxLease,
        *,
        error: str | None = None,
    ) -> SessionMailbox:
        mailbox = self._require_owned(lease)
        item = self._items[(lease.tenant_id, lease.session_id, lease.sequence)]
        now = now_utc()
        item.resolved_at = now
        if error is not None:
            item.retry_count += 1
            self._emit_dead_letter(mailbox, item, lease, error, now)
        mailbox.resolved_sequence = lease.sequence
        mailbox.processing_sequence = None
        mailbox.processing_message_id = None
        mailbox.lease_owner = None
        mailbox.lease_until = None
        next_item = self._items.get(
            (lease.tenant_id, lease.session_id, lease.sequence + 1)
        )
        if next_item is None:
            mailbox.status = SessionMailboxStatus.IDLE
            mailbox.retry_at = None
        elif next_item.retry_at is not None and next_item.retry_at > now:
            mailbox.status = SessionMailboxStatus.RETRY_WAIT
            mailbox.retry_at = next_item.retry_at
        else:
            mailbox.status = SessionMailboxStatus.QUEUED
            mailbox.retry_at = None
            mailbox.queue_generation += 1
            self._emit_ready(mailbox, next_item)
        mailbox.updated_at = now
        return _copy(mailbox)

    def _emit_dead_letter(
        self,
        mailbox: SessionMailbox,
        item: SessionMailboxItem,
        lease: SessionMailboxLease,
        error: str,
        dead_at: datetime,
    ) -> None:
        event_id = (
            f"session-dead:{mailbox.tenant_id}:{mailbox.session_id}:{item.sequence}"
        )
        self._outbox.setdefault(
            event_id,
            OutboxRecord(
                event_id=event_id,
                tenant_id=mailbox.tenant_id,
                topic="session.dead_letter.v2",
                aggregate_id=mailbox.session_id,
                payload={
                    "sequence": item.sequence,
                    "message_id": item.message_id,
                    "trace_id": item.trace_id,
                    "attempt": lease.attempt,
                    "retry_count": item.retry_count,
                    "error": redact_secret_text(str(error))[:1000],
                    "dead_at": dead_at.isoformat(),
                },
            ),
        )

    def _emit_ready(self, mailbox: SessionMailbox, item: SessionMailboxItem) -> None:
        event_id = f"session-ready:{mailbox.tenant_id}:{mailbox.session_id}:{mailbox.queue_generation}"
        self._outbox.setdefault(
            event_id,
            OutboxRecord(
                event_id=event_id,
                tenant_id=mailbox.tenant_id,
                topic="session.ready.v2",
                aggregate_id=mailbox.session_id,
                payload={
                    "generation": mailbox.queue_generation,
                    "priority": item.priority,
                    "trace_id": item.trace_id,
                    "created_at": mailbox.updated_at,
                },
            ),
        )


class SQLiteSessionMailboxStore:
    """SQLite implementation with aggregate and item rows in one transaction."""

    backend_name = "sqlite-v2"

    def __init__(self, connection: sqlite3.Connection, lock: RLock) -> None:
        self._conn = connection
        self._lock = lock
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS session_mailbox (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  accepted_sequence INTEGER NOT NULL DEFAULT 0,
                  resolved_sequence INTEGER NOT NULL DEFAULT 0,
                  processing_sequence INTEGER,
                  processing_message_id TEXT,
                  queue_generation INTEGER NOT NULL DEFAULT 0,
                  lease_owner TEXT,
                  lease_epoch INTEGER NOT NULL DEFAULT 0,
                  lease_until TEXT,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  attempt INTEGER NOT NULL DEFAULT 0,
                  priority INTEGER NOT NULL DEFAULT 0,
                  retry_at TEXT,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS session_mailbox_item (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  sequence INTEGER NOT NULL,
                  message_id TEXT NOT NULL,
                  trace_id TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 0,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  attempt INTEGER NOT NULL DEFAULT 0,
                  retry_at TEXT,
                  accepted_at TEXT NOT NULL,
                  resolved_at TEXT,
                  PRIMARY KEY (tenant_id, session_id, sequence),
                  UNIQUE (tenant_id, session_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_session_mailbox_ready
                  ON session_mailbox(status, retry_at, updated_at);
                """
            )

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM session_mailbox WHERE tenant_id=? AND session_id=?",
                (tenant_id, session_id),
            ).fetchone()
        return _mailbox_from_sql(row) if row else None

    def has_unresolved_message(
        self, tenant_id: str, session_id: str, message_id: str
    ) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM session_mailbox_item
                 WHERE tenant_id=? AND session_id=? AND message_id=?
                   AND resolved_at IS NULL
                """,
                (tenant_id, session_id, message_id),
            ).fetchone()
            return row is not None

    def export_by_tenant(self, tenant_id: str) -> dict[str, list[dict]]:
        with self._lock:
            mailbox_rows = self._conn.execute(
                "SELECT * FROM session_mailbox WHERE tenant_id=?",
                (tenant_id,),
            ).fetchall()
            item_rows = self._conn.execute(
                "SELECT * FROM session_mailbox_item WHERE tenant_id=?",
                (tenant_id,),
            ).fetchall()
        return {
            "mailboxes": [dict(row) for row in mailbox_rows],
            "items": [dict(row) for row in item_rows],
        }

    def restore_export(self, payload: dict[str, list[dict]]) -> None:
        with self._transaction():
            for raw in payload.get("mailboxes", []):
                values = _restore_datetimes(raw)
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO session_mailbox (
                      tenant_id,session_id,status,accepted_sequence,
                      resolved_sequence,processing_sequence,processing_message_id,
                      queue_generation,lease_owner,lease_epoch,lease_until,
                      retry_count,attempt,priority,retry_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        values["tenant_id"],
                        values["session_id"],
                        values["status"],
                        values["accepted_sequence"],
                        values["resolved_sequence"],
                        values["processing_sequence"],
                        values["processing_message_id"],
                        values["queue_generation"],
                        values["lease_owner"],
                        values["lease_epoch"],
                        _iso(values["lease_until"]),
                        values["retry_count"],
                        values["attempt"],
                        values["priority"],
                        _iso(values["retry_at"]),
                        _iso(values["updated_at"]),
                    ),
                )
            for raw in payload.get("items", []):
                values = _restore_datetimes(raw)
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO session_mailbox_item (
                      tenant_id,session_id,sequence,message_id,trace_id,priority,
                      retry_count,attempt,retry_at,accepted_at,resolved_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        values["tenant_id"],
                        values["session_id"],
                        values["sequence"],
                        values["message_id"],
                        values["trace_id"],
                        values["priority"],
                        values["retry_count"],
                        values["attempt"],
                        _iso(values["retry_at"]),
                        _iso(values["accepted_at"]),
                        _iso(values["resolved_at"]),
                    ),
                )

    def accept(
        self,
        tenant_id: str,
        session_id: str,
        message_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
    ) -> SessionMailbox:
        _validate_ids(tenant_id, session_id, message_id)
        _validate_priority(priority)
        now = now_utc()
        with self._transaction():
            self._conn.execute(
                """
                INSERT OR IGNORE INTO session_mailbox
                  (tenant_id,session_id,status,updated_at)
                VALUES (?,?,?,?)
                """,
                (tenant_id, session_id, SessionMailboxStatus.IDLE, _iso(now)),
            )
            mailbox = self._mailbox_row(tenant_id, session_id)
            duplicate = self._conn.execute(
                """
                SELECT 1 FROM session_mailbox_item
                 WHERE tenant_id=? AND session_id=? AND message_id=?
                """,
                (tenant_id, session_id, message_id),
            ).fetchone()
            if duplicate:
                return _mailbox_from_sql(mailbox)
            sequence = int(mailbox["accepted_sequence"]) + 1
            self._conn.execute(
                """
                INSERT INTO session_mailbox_item (
                  tenant_id,session_id,sequence,message_id,trace_id,priority,
                  retry_at,accepted_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    tenant_id,
                    session_id,
                    sequence,
                    message_id,
                    trace_id or message_id,
                    priority,
                    _iso(retry_at),
                    _iso(now),
                ),
            )
            previous = str(mailbox["status"])
            head_waiting = (
                previous == SessionMailboxStatus.RETRY_WAIT
                and mailbox["retry_at"] is not None
                and _parse(mailbox["retry_at"]) > now
            )
            if previous == SessionMailboxStatus.IDLE:
                status = (
                    SessionMailboxStatus.RETRY_WAIT
                    if retry_at is not None and retry_at > now
                    else SessionMailboxStatus.QUEUED
                )
            elif previous == SessionMailboxStatus.RETRY_WAIT and not head_waiting:
                status = SessionMailboxStatus.QUEUED
            else:
                status = previous
            emit = status == SessionMailboxStatus.QUEUED and previous in (
                SessionMailboxStatus.IDLE,
                SessionMailboxStatus.RETRY_WAIT,
            )
            generation = int(mailbox["queue_generation"]) + (1 if emit else 0)
            next_retry_at = (
                mailbox["retry_at"]
                if head_waiting
                else (_iso(retry_at) if status == SessionMailboxStatus.RETRY_WAIT else None)
            )
            self._conn.execute(
                """
                UPDATE session_mailbox
                   SET status=?,accepted_sequence=?,queue_generation=?,
                       priority=max(priority,?),retry_at=?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    status,
                    sequence,
                    generation,
                    priority,
                    next_retry_at,
                    _iso(now),
                    tenant_id,
                    session_id,
                ),
            )
            updated = self._mailbox_row(tenant_id, session_id)
            if emit:
                item = self._item_row(tenant_id, session_id, sequence)
                self._emit_ready(updated, item)
            return _mailbox_from_sql(updated)

    def dead_letter(self, lease: SessionMailboxLease, error: str) -> SessionMailbox:
        with self._transaction():
            self._require_owned(lease)
            now = now_utc()
            item = self._item_row(lease.tenant_id, lease.session_id, lease.sequence)
            self._conn.execute(
                """
                UPDATE session_mailbox_item
                   SET retry_count=retry_count+1, resolved_at=?
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (_iso(now), lease.tenant_id, lease.session_id, lease.sequence),
            )
            next_item = self._conn.execute(
                """
                SELECT * FROM session_mailbox_item
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (lease.tenant_id, lease.session_id, lease.sequence + 1),
            ).fetchone()
            next_retry = _parse(next_item["retry_at"]) if next_item else None
            if next_item is None:
                status, retry_at, increment = SessionMailboxStatus.IDLE, None, 0
            elif next_retry is not None and next_retry > now:
                status, retry_at, increment = (
                    SessionMailboxStatus.RETRY_WAIT,
                    next_item["retry_at"],
                    0,
                )
            else:
                status, retry_at, increment = SessionMailboxStatus.QUEUED, None, 1
            self._conn.execute(
                """
                UPDATE session_mailbox
                   SET resolved_sequence=?,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,status=?,retry_at=?,
                       queue_generation=queue_generation+?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    lease.sequence,
                    status,
                    retry_at,
                    increment,
                    _iso(now),
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            updated = self._mailbox_row(lease.tenant_id, lease.session_id)
            self._emit_dead_letter(updated, item, lease, error, now)
            if status == SessionMailboxStatus.QUEUED and next_item is not None:
                self._emit_ready(updated, next_item)
            return _mailbox_from_sql(updated)

    def claim(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: float,
    ) -> SessionMailboxLease | None:
        _validate_ids(tenant_id, session_id, owner)
        seconds = _validate_lease(lease_seconds)
        with self._transaction():
            return self._claim_locked(tenant_id, session_id, owner, seconds)

    def _claim_locked(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: float,
    ) -> SessionMailboxLease | None:
        mailbox = self._mailbox_row(tenant_id, session_id, required=False)
        if mailbox is None:
            return None
        now = now_utc()
        current_lease = _parse(mailbox["lease_until"])
        if current_lease is not None and current_lease > now:
            return None
        sequence = mailbox["processing_sequence"]
        sequence = int(sequence) if sequence is not None else int(mailbox["resolved_sequence"]) + 1
        if sequence > int(mailbox["accepted_sequence"]):
            self._conn.execute(
                """
                UPDATE session_mailbox SET status=?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (SessionMailboxStatus.IDLE, _iso(now), tenant_id, session_id),
            )
            return None
        item = self._item_row(tenant_id, session_id, sequence)
        item_retry = _parse(item["retry_at"])
        if item_retry is not None and item_retry > now:
            self._conn.execute(
                """
                UPDATE session_mailbox SET status=?,retry_at=?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    SessionMailboxStatus.RETRY_WAIT,
                    item["retry_at"],
                    _iso(now),
                    tenant_id,
                    session_id,
                ),
            )
            return None
        attempt = int(item["attempt"]) + 1
        epoch = int(mailbox["lease_epoch"]) + 1
        expires = now + timedelta(seconds=lease_seconds)
        self._conn.execute(
            """
            UPDATE session_mailbox_item SET attempt=?
             WHERE tenant_id=? AND session_id=? AND sequence=?
            """,
            (attempt, tenant_id, session_id, sequence),
        )
        self._conn.execute(
            """
            UPDATE session_mailbox
               SET status=?,processing_sequence=?,processing_message_id=?,
                   lease_owner=?,lease_epoch=?,lease_until=?,attempt=?,
                   retry_count=?,priority=?,retry_at=?,updated_at=?
             WHERE tenant_id=? AND session_id=?
            """,
            (
                SessionMailboxStatus.RUNNING,
                sequence,
                item["message_id"],
                owner,
                epoch,
                _iso(expires),
                attempt,
                int(item["retry_count"]),
                int(item["priority"]),
                item["retry_at"],
                _iso(now),
                tenant_id,
                session_id,
            ),
        )
        return SessionMailboxLease(
            tenant_id,
            session_id,
            item["message_id"],
            sequence,
            owner,
            epoch,
            expires,
            attempt,
            int(item["retry_count"]),
            int(item["priority"]),
        )

    def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: float,
        *,
        expected_generation: int | None = None,
    ) -> SessionMailboxClaim:
        _validate_lease(lease_seconds)
        with self._transaction():
            row = self._mailbox_row(tenant_id, session_id, required=False)
            mailbox = (
                _mailbox_from_sql(row)
                if row is not None
                else SessionMailbox(tenant_id, session_id)
            )
            if expected_generation is not None and mailbox.queue_generation != expected_generation:
                return SessionMailboxClaim(SessionMailboxClaimStatus.STALE, mailbox)
            lease = self._claim_locked(
                tenant_id,
                session_id,
                owner,
                float(lease_seconds),
            )
            row = self._mailbox_row(tenant_id, session_id, required=False)
            mailbox = _mailbox_from_sql(row) if row is not None else mailbox
            if lease is not None:
                return SessionMailboxClaim(SessionMailboxClaimStatus.CLAIMED, mailbox, lease)
            now = now_utc()
            if mailbox.lease_until is not None and mailbox.lease_until > now:
                status = SessionMailboxClaimStatus.RUNNING
            elif expected_generation is not None and mailbox.queue_generation != expected_generation:
                status = SessionMailboxClaimStatus.STALE
            else:
                status = SessionMailboxClaimStatus.EMPTY
            return SessionMailboxClaim(status, mailbox)

    def renew(
        self,
        lease: SessionMailboxLease,
        lease_seconds: float,
    ) -> SessionMailboxLease:
        seconds = _validate_lease(lease_seconds)
        with self._transaction():
            mailbox = self._require_owned(lease)
            now = now_utc()
            expires = max(now + timedelta(seconds=seconds), mailbox.lease_until or now)
            self._conn.execute(
                """
                UPDATE session_mailbox SET lease_until=?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    _iso(expires),
                    _iso(now),
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            return SessionMailboxLease(
                lease.tenant_id,
                lease.session_id,
                lease.message_id,
                lease.sequence,
                lease.owner,
                lease.epoch,
                expires,
                lease.attempt,
                lease.retry_count,
                lease.priority,
            )

    def commit(self, lease: SessionMailboxLease) -> SessionMailbox:
        with self._transaction():
            self._require_owned(lease)
            now = now_utc()
            self._conn.execute(
                """
                UPDATE session_mailbox_item SET resolved_at=?
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (_iso(now), lease.tenant_id, lease.session_id, lease.sequence),
            )
            next_item = self._conn.execute(
                """
                SELECT * FROM session_mailbox_item
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (lease.tenant_id, lease.session_id, lease.sequence + 1),
            ).fetchone()
            next_retry = _parse(next_item["retry_at"]) if next_item else None
            if next_item is None:
                status = SessionMailboxStatus.IDLE
                retry_at = None
                increment = 0
            elif next_retry is not None and next_retry > now:
                status = SessionMailboxStatus.RETRY_WAIT
                retry_at = next_item["retry_at"]
                increment = 0
            else:
                status = SessionMailboxStatus.QUEUED
                retry_at = None
                increment = 1
            self._conn.execute(
                """
                UPDATE session_mailbox
                   SET resolved_sequence=?,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,status=?,retry_at=?,
                       queue_generation=queue_generation+?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    lease.sequence,
                    status,
                    retry_at,
                    increment,
                    _iso(now),
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            updated = self._mailbox_row(lease.tenant_id, lease.session_id)
            if status == SessionMailboxStatus.QUEUED and next_item is not None:
                self._emit_ready(updated, next_item)
            return _mailbox_from_sql(updated)

    def retry(
        self,
        lease: SessionMailboxLease,
        *,
        retry_at: datetime | None = None,
        increment_retry: bool = True,
    ) -> SessionMailbox:
        with self._transaction():
            self._require_owned(lease)
            item = self._item_row(lease.tenant_id, lease.session_id, lease.sequence)
            retries = int(item["retry_count"]) + (1 if increment_retry else 0)
            now = now_utc()
            self._conn.execute(
                """
                UPDATE session_mailbox_item
                   SET retry_count=?,retry_at=?
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (
                    retries,
                    _iso(retry_at),
                    lease.tenant_id,
                    lease.session_id,
                    lease.sequence,
                ),
            )
            due = retry_at is None or retry_at <= now
            status = SessionMailboxStatus.QUEUED if due else SessionMailboxStatus.RETRY_WAIT
            increment = 1 if due else 0
            self._conn.execute(
                """
                UPDATE session_mailbox
                   SET status=?,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,retry_count=?,retry_at=?,
                       queue_generation=queue_generation+?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    status,
                    retries,
                    _iso(retry_at),
                    increment,
                    _iso(now),
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            updated = self._mailbox_row(lease.tenant_id, lease.session_id)
            if due:
                self._emit_ready(updated, self._item_row(
                    lease.tenant_id, lease.session_id, lease.sequence
                ))
            return _mailbox_from_sql(updated)

    def recover(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._transaction():
            row = self._mailbox_row(tenant_id, session_id, required=False)
            if row is None:
                return None
            expires = _parse(row["lease_until"])
            if expires is None or expires > now_utc():
                return None
            sequence = int(row["processing_sequence"] or int(row["resolved_sequence"]) + 1)
            item = self._conn.execute(
                """
                SELECT * FROM session_mailbox_item
                 WHERE tenant_id=? AND session_id=? AND sequence=?
                """,
                (tenant_id, session_id, sequence),
            ).fetchone()
            now = now_utc()
            if item is None or sequence > int(row["accepted_sequence"]):
                status = SessionMailboxStatus.IDLE
                retry_at = None
                increment = 0
            elif _parse(item["retry_at"]) is not None and _parse(item["retry_at"]) > now:
                status = SessionMailboxStatus.RETRY_WAIT
                retry_at = item["retry_at"]
                increment = 0
            else:
                status = SessionMailboxStatus.QUEUED
                retry_at = None
                increment = 1
            self._conn.execute(
                """
                UPDATE session_mailbox
                   SET status=?,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,retry_at=?,
                       queue_generation=queue_generation+?,updated_at=?
                 WHERE tenant_id=? AND session_id=?
                """,
                (
                    status,
                    retry_at,
                    increment,
                    _iso(now),
                    tenant_id,
                    session_id,
                ),
            )
            updated = self._mailbox_row(tenant_id, session_id)
            if status == SessionMailboxStatus.QUEUED and item is not None:
                self._emit_ready(updated, item)
            return _mailbox_from_sql(updated)

    def reconcile(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        mailbox = self.get(tenant_id, session_id)
        if mailbox is None:
            return None
        if (
            mailbox.status == SessionMailboxStatus.RUNNING
            and mailbox.lease_until is not None
            and mailbox.lease_until > now_utc()
        ):
            return mailbox
        return self.recover(tenant_id, session_id)

    def sweep_expired_leases(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        with self._lock:
            query = """
                SELECT tenant_id,session_id FROM session_mailbox
                 WHERE lease_until IS NOT NULL AND lease_until<=?
            """
            params: list[Any] = [_iso(now_utc())]
            if tenant_id is not None:
                query += " AND tenant_id=?"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(query, params).fetchall()
        return sum(self.recover(row["tenant_id"], row["session_id"]) is not None for row in rows)

    def schedule_retries(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        with self._transaction():
            query = """
                SELECT tenant_id,session_id FROM session_mailbox
                 WHERE status=? AND retry_at IS NOT NULL AND retry_at<=?
            """
            params: list[Any] = [SessionMailboxStatus.RETRY_WAIT, _iso(now_utc())]
            if tenant_id is not None:
                query += " AND tenant_id=?"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(query, params).fetchall()
            for row in rows:
                mailbox = self._mailbox_row(row["tenant_id"], row["session_id"])
                item = self._item_row(
                    row["tenant_id"],
                    row["session_id"],
                    int(mailbox["resolved_sequence"]) + 1,
                )
                self._conn.execute(
                    """
                    UPDATE session_mailbox
                       SET status=?,retry_at=NULL,queue_generation=queue_generation+1,
                           updated_at=?
                     WHERE tenant_id=? AND session_id=?
                    """,
                    (
                        SessionMailboxStatus.QUEUED,
                        _iso(now_utc()),
                        row["tenant_id"],
                        row["session_id"],
                    ),
                )
                self._emit_ready(
                    self._mailbox_row(row["tenant_id"], row["session_id"]), item
                )
            return len(rows)

    def reconcile_sessions(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        with self._transaction():
            query = "SELECT * FROM session_mailbox WHERE status=?"
            params: list[Any] = [SessionMailboxStatus.QUEUED]
            if tenant_id is not None:
                query += " AND tenant_id=?"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(query, params).fetchall()
            for mailbox in rows:
                item = self._item_row(
                    mailbox["tenant_id"],
                    mailbox["session_id"],
                    int(mailbox["resolved_sequence"]) + 1,
                )
                self._emit_ready(mailbox, item)
            return len(rows)

    def _mailbox_row(self, tenant_id: str, session_id: str, required: bool = True):
        row = self._conn.execute(
            "SELECT * FROM session_mailbox WHERE tenant_id=? AND session_id=?",
            (tenant_id, session_id),
        ).fetchone()
        if row is None and required:
            raise RuntimeError("session mailbox row is missing")
        return row

    def _item_row(self, tenant_id: str, session_id: str, sequence: int):
        row = self._conn.execute(
            """
            SELECT * FROM session_mailbox_item
             WHERE tenant_id=? AND session_id=? AND sequence=?
            """,
            (tenant_id, session_id, sequence),
        ).fetchone()
        if row is None:
            raise RuntimeError("session mailbox item is missing")
        return row

    def _require_owned(self, lease: SessionMailboxLease) -> SessionMailbox:
        row = self._mailbox_row(lease.tenant_id, lease.session_id, required=False)
        mailbox = _mailbox_from_sql(row) if row else None
        if not _owns_session_mailbox(mailbox, lease):
            raise SessionLeaseLost(
                f"session mailbox fencing rejected: {lease.tenant_id}/{lease.session_id}"
            )
        return mailbox

    def _emit_ready(self, mailbox, item) -> None:
        event_id = (
            f"session-ready:{mailbox['tenant_id']}:"
            f"{mailbox['session_id']}:{mailbox['queue_generation']}"
        )
        now = now_utc()
        payload = json.dumps(
            {
                "generation": int(mailbox["queue_generation"]),
                "priority": int(item["priority"]),
                "trace_id": item["trace_id"],
                "created_at": _iso(now),
            },
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO outbox_message (
              event_id,tenant_id,topic,aggregate_id,payload_json,status,
              attempts,available_at,created_at,updated_at
            ) VALUES (?,?,?,?,?,'pending',0,?,?,?)
            """,
            (
                event_id,
                mailbox["tenant_id"],
                "session.ready.v2",
                mailbox["session_id"],
                payload,
                _iso(now),
                _iso(now),
                _iso(now),
            ),
        )

    def _emit_dead_letter(self, mailbox, item, lease, error, dead_at) -> None:
        event_id = (
            f"session-dead:{mailbox['tenant_id']}:"
            f"{mailbox['session_id']}:{item['sequence']}"
        )
        payload = json.dumps(
            {
                "sequence": int(item["sequence"]),
                "message_id": item["message_id"],
                "trace_id": item["trace_id"],
                "attempt": lease.attempt,
                "retry_count": int(item["retry_count"]) + 1,
                "error": redact_secret_text(str(error))[:1000],
                "dead_at": _iso(dead_at),
            },
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO outbox_message (
              event_id,tenant_id,topic,aggregate_id,payload_json,status,
              attempts,available_at,created_at,updated_at
            ) VALUES (?,?,?,?,?,'pending',0,?,?,?)
            """,
            (
                event_id,
                mailbox["tenant_id"],
                "session.dead_letter.v2",
                mailbox["session_id"],
                payload,
                _iso(dead_at),
                _iso(dead_at),
                _iso(dead_at),
            ),
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _restore_datetimes(value: dict) -> dict:
    result = dict(value)
    for key in (
        "lease_until",
        "retry_at",
        "updated_at",
        "accepted_at",
        "resolved_at",
    ):
        if isinstance(result.get(key), str):
            result[key] = _parse(result[key])
    return result


def _mailbox_from_sql(row: Any) -> SessionMailbox:
    mailbox = SessionMailbox(
        tenant_id=row["tenant_id"],
        session_id=row["session_id"],
        status=row["status"],
        accepted_sequence=int(row["accepted_sequence"]),
        resolved_sequence=int(row["resolved_sequence"]),
        processing_sequence=(
            int(row["processing_sequence"]) if row["processing_sequence"] is not None else None
        ),
        processing_message_id=row["processing_message_id"],
        queue_generation=int(row["queue_generation"]),
        lease_owner=row["lease_owner"],
        lease_epoch=int(row["lease_epoch"]),
        lease_until=_parse(row["lease_until"]),
        retry_count=int(row["retry_count"]),
        attempt=int(row["attempt"]),
        priority=int(row["priority"]),
        retry_at=_parse(row["retry_at"]),
        updated_at=_parse(row["updated_at"]) or now_utc(),
    )
    validate_session_mailbox(mailbox)
    return mailbox


def _owns_session_mailbox(
    mailbox: SessionMailbox | None,
    lease: SessionMailboxLease,
) -> bool:
    return bool(
        mailbox
        and mailbox.status == SessionMailboxStatus.RUNNING
        and mailbox.processing_sequence == lease.sequence
        and mailbox.processing_message_id == lease.message_id
        and mailbox.lease_owner == lease.owner
        and mailbox.lease_epoch == lease.epoch
        and mailbox.lease_until is not None
        and mailbox.lease_until > now_utc()
    )


__all__ = [
    "InMemorySessionMailboxStore",
    "SQLiteSessionMailboxStore",
    "SessionMailbox",
    "SessionMailboxClaim",
    "SessionMailboxClaimStatus",
    "SessionMailboxItem",
    "SessionMailboxLease",
    "SessionMailboxStatus",
    "validate_session_mailbox",
]
