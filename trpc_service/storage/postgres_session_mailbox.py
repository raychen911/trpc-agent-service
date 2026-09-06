"""PostgreSQL session mailbox v2.

This adapter keeps the session aggregate authoritative in PostgreSQL. Redis
or another transport only receives the durable ``session.ready.v2`` outbox
notice after the mailbox transition commits.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import RLock

from trpc_service.security.secrets import redact_secret_text
from trpc_service.storage.base import now_utc
from trpc_service.storage.locking import SessionLeaseLost, postgres_advisory_lock
from trpc_service.storage.postgres_rls import rls_tenant_method
from trpc_service.storage.session_mailbox import (
    SessionMailbox,
    SessionMailboxClaim,
    SessionMailboxClaimStatus,
    SessionMailboxLease,
    SessionMailboxStatus,
    _owns_session_mailbox,
    _restore_datetimes,
    _validate_ids,
    _validate_lease,
    _validate_priority,
    validate_session_mailbox,
)


class PostgresSessionMailboxStore:
    """Server-clock, row-locked implementation of the session mailbox."""

    backend_name = "postgres-v2"

    def __init__(self, connection, lock: RLock) -> None:
        self._conn = connection
        self._lock = lock
        self._init_schema()

    def set_connection(self, connection) -> None:
        self._conn = connection

    def _init_schema(self) -> None:
        from trpc_service.storage.postgres_rls import postgres_schema_auto_create

        if not postgres_schema_auto_create():
            return
        with self._lock, postgres_advisory_lock(self._conn, "trpc-agent-session-mailbox-v2"):
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS session_mailbox (
                      tenant_id TEXT NOT NULL,
                      session_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      accepted_sequence BIGINT NOT NULL DEFAULT 0,
                      resolved_sequence BIGINT NOT NULL DEFAULT 0,
                      processing_sequence BIGINT,
                      processing_message_id TEXT,
                      queue_generation BIGINT NOT NULL DEFAULT 0,
                      lease_owner TEXT,
                      lease_epoch BIGINT NOT NULL DEFAULT 0,
                      lease_until TIMESTAMPTZ,
                      retry_count INTEGER NOT NULL DEFAULT 0,
                      attempt INTEGER NOT NULL DEFAULT 0,
                      priority INTEGER NOT NULL DEFAULT 0,
                      retry_at TIMESTAMPTZ,
                      updated_at TIMESTAMPTZ NOT NULL,
                      PRIMARY KEY (tenant_id, session_id)
                    );
                    CREATE TABLE IF NOT EXISTS session_mailbox_item (
                      tenant_id TEXT NOT NULL,
                      session_id TEXT NOT NULL,
                      sequence BIGINT NOT NULL,
                      message_id TEXT NOT NULL,
                      trace_id TEXT NOT NULL,
                      priority INTEGER NOT NULL DEFAULT 0,
                      retry_count INTEGER NOT NULL DEFAULT 0,
                      attempt INTEGER NOT NULL DEFAULT 0,
                      retry_at TIMESTAMPTZ,
                      accepted_at TIMESTAMPTZ NOT NULL,
                      resolved_at TIMESTAMPTZ,
                      PRIMARY KEY (tenant_id, session_id, sequence),
                      UNIQUE (tenant_id, session_id, message_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_session_mailbox_ready
                      ON session_mailbox(status, retry_at, updated_at);
                    """
                )

    @contextmanager
    def _transaction(self):
        with self._lock, self._conn.transaction():
            yield

    def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._lock:
            row = self._row(tenant_id, session_id)
        return _mailbox_from_pg(row) if row else None

    def has_unresolved_message(
        self, tenant_id: str, session_id: str, message_id: str
    ) -> bool:
        return self._fetchone(
            """
            SELECT 1 FROM session_mailbox_item
             WHERE tenant_id=%s AND session_id=%s AND message_id=%s
               AND resolved_at IS NULL
            """,
            (tenant_id, session_id, message_id),
        ) is not None

    def export_by_tenant(self, tenant_id: str) -> dict[str, list[dict]]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM session_mailbox WHERE tenant_id=%s",
                (tenant_id,),
            )
            mailboxes = [
                _mailbox_export_row(row)
                for row in cur.fetchall()
            ]
            cur.execute(
                "SELECT * FROM session_mailbox_item WHERE tenant_id=%s",
                (tenant_id,),
            )
            items = [_item_export_row(row) for row in cur.fetchall()]
        return {"mailboxes": mailboxes, "items": items}

    def restore_export(self, payload: dict[str, list[dict]]) -> None:
        with self._transaction():
            for raw in payload.get("mailboxes", []):
                values = _restore_datetimes(raw)
                self._execute(
                    """
                    INSERT INTO session_mailbox (
                      tenant_id,session_id,status,accepted_sequence,
                      resolved_sequence,processing_sequence,processing_message_id,
                      queue_generation,lease_owner,lease_epoch,lease_until,
                      retry_count,attempt,priority,retry_at,updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,session_id) DO UPDATE SET
                      status=EXCLUDED.status,
                      accepted_sequence=EXCLUDED.accepted_sequence,
                      resolved_sequence=EXCLUDED.resolved_sequence,
                      processing_sequence=EXCLUDED.processing_sequence,
                      processing_message_id=EXCLUDED.processing_message_id,
                      queue_generation=EXCLUDED.queue_generation,
                      lease_owner=EXCLUDED.lease_owner,
                      lease_epoch=EXCLUDED.lease_epoch,
                      lease_until=EXCLUDED.lease_until,
                      retry_count=EXCLUDED.retry_count,
                      attempt=EXCLUDED.attempt,
                      priority=EXCLUDED.priority,
                      retry_at=EXCLUDED.retry_at,
                      updated_at=EXCLUDED.updated_at
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
                        values["lease_until"],
                        values["retry_count"],
                        values["attempt"],
                        values["priority"],
                        values["retry_at"],
                        values["updated_at"],
                    ),
                )
            for raw in payload.get("items", []):
                values = _restore_datetimes(raw)
                self._execute(
                    """
                    INSERT INTO session_mailbox_item (
                      tenant_id,session_id,sequence,message_id,trace_id,priority,
                      retry_count,attempt,retry_at,accepted_at,resolved_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id,session_id,sequence) DO UPDATE SET
                      message_id=EXCLUDED.message_id,
                      trace_id=EXCLUDED.trace_id,
                      priority=EXCLUDED.priority,
                      retry_count=EXCLUDED.retry_count,
                      attempt=EXCLUDED.attempt,
                      retry_at=EXCLUDED.retry_at,
                      accepted_at=EXCLUDED.accepted_at,
                      resolved_at=EXCLUDED.resolved_at
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
                        values["retry_at"],
                        values["accepted_at"],
                        values["resolved_at"],
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
        with self._transaction():
            now = self._server_now()
            self._execute(
                """
                INSERT INTO session_mailbox (tenant_id,session_id,status,updated_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (tenant_id,session_id) DO NOTHING
                """,
                (tenant_id, session_id, SessionMailboxStatus.IDLE, now),
            )
            mailbox = self._row(tenant_id, session_id, lock=True)
            duplicate = self._fetchone(
                """
                SELECT 1 FROM session_mailbox_item
                 WHERE tenant_id=%s AND session_id=%s AND message_id=%s
                """,
                (tenant_id, session_id, message_id),
            )
            if duplicate:
                return _mailbox_from_pg(mailbox)
            sequence = int(mailbox[3]) + 1
            self._execute(
                """
                INSERT INTO session_mailbox_item (
                  tenant_id,session_id,sequence,message_id,trace_id,priority,
                  retry_at,accepted_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    tenant_id,
                    session_id,
                    sequence,
                    message_id,
                    trace_id or message_id,
                    priority,
                    retry_at,
                    now,
                ),
            )
            previous = str(mailbox[2])
            current_retry_at = mailbox[14]
            head_waiting = (
                previous == SessionMailboxStatus.RETRY_WAIT
                and current_retry_at is not None
                and current_retry_at > now
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
            generation = int(mailbox[7]) + (1 if emit else 0)
            next_retry_at = current_retry_at if head_waiting else (
                retry_at if status == SessionMailboxStatus.RETRY_WAIT else None
            )
            updated = self._fetchone(
                """
                UPDATE session_mailbox
                   SET status=%s,accepted_sequence=%s,queue_generation=%s,
                       priority=greatest(priority,%s),retry_at=%s,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                 RETURNING *
                """,
                (
                    status,
                    sequence,
                    generation,
                    priority,
                    next_retry_at,
                    tenant_id,
                    session_id,
                ),
            )
            if emit:
                self._emit_ready(updated, self._item(tenant_id, session_id, sequence))
            return _mailbox_from_pg(updated)

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

    def _claim_locked(self, tenant_id, session_id, owner, seconds):
        row = self._row(tenant_id, session_id, required=False, lock=True)
        if row is None:
            return None
        now = self._server_now()
        if row[10] is not None and row[10] > now:
            return None
        sequence = int(row[5]) if row[5] is not None else int(row[4]) + 1
        if sequence > int(row[3]):
            self._execute(
                """
                UPDATE session_mailbox SET status=%s,updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                """,
                (SessionMailboxStatus.IDLE, tenant_id, session_id),
            )
            return None
        item = self._item(tenant_id, session_id, sequence)
        if item[8] is not None and item[8] > now:
            self._execute(
                """
                UPDATE session_mailbox SET status=%s,retry_at=?,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                """.replace("?", "%s"),
                (
                    SessionMailboxStatus.RETRY_WAIT,
                    item[8],
                    tenant_id,
                    session_id,
                ),
            )
            return None
        attempt = int(item[7]) + 1
        epoch = int(row[9]) + 1
        expires = now + timedelta(seconds=seconds)
        self._execute(
            """
            UPDATE session_mailbox_item SET attempt=%s
             WHERE tenant_id=%s AND session_id=%s AND sequence=%s
            """,
            (attempt, tenant_id, session_id, sequence),
        )
        updated = self._fetchone(
            """
            UPDATE session_mailbox
               SET status=%s,processing_sequence=%s,processing_message_id=%s,
                   lease_owner=%s,lease_epoch=%s,lease_until=%s,attempt=%s,
                   retry_count=%s,priority=%s,retry_at=%s,
                   updated_at=clock_timestamp()
             WHERE tenant_id=%s AND session_id=%s
             RETURNING lease_until
            """,
            (
                SessionMailboxStatus.RUNNING,
                sequence,
                item[3],
                owner,
                epoch,
                expires,
                attempt,
                int(item[6]),
                int(item[5]),
                item[8],
                tenant_id,
                session_id,
            ),
        )
        return SessionMailboxLease(
            tenant_id,
            session_id,
            item[3],
            sequence,
            owner,
            epoch,
            updated[0],
            attempt,
            int(item[6]),
            int(item[5]),
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
        seconds = _validate_lease(lease_seconds)
        with self._transaction():
            row = self._row(tenant_id, session_id, required=False, lock=True)
            mailbox = (
                _mailbox_from_pg(row)
                if row
                else SessionMailbox(tenant_id, session_id)
            )
            if expected_generation is not None and mailbox.queue_generation != expected_generation:
                return SessionMailboxClaim(SessionMailboxClaimStatus.STALE, mailbox)
            lease = self._claim_locked(tenant_id, session_id, owner, seconds)
            row = self._row(tenant_id, session_id, required=False)
            mailbox = _mailbox_from_pg(row) if row else mailbox
            if lease:
                return SessionMailboxClaim(SessionMailboxClaimStatus.CLAIMED, mailbox, lease)
            now = self._server_now()
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
            expires = self._server_now() + timedelta(seconds=seconds)
            row = self._fetchone(
                """
                UPDATE session_mailbox
                   SET lease_until=greatest(%s,lease_until+interval '1 microsecond'),
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                   AND status=%s AND processing_sequence=%s
                   AND processing_message_id=%s AND lease_owner=%s
                   AND lease_epoch=%s AND lease_until>clock_timestamp()
                 RETURNING lease_until
                """,
                (
                    expires,
                    lease.tenant_id,
                    lease.session_id,
                    SessionMailboxStatus.RUNNING,
                    lease.sequence,
                    lease.message_id,
                    lease.owner,
                    lease.epoch,
                ),
            )
            if row is None:
                raise SessionLeaseLost("session mailbox lease is no longer current")
            return _lease_with_expiry(lease, row[0])

    def commit(self, lease: SessionMailboxLease) -> SessionMailbox:
        with self._transaction():
            self._assert_owned(lease)
            now = self._server_now()
            self._execute(
                """
                UPDATE session_mailbox_item SET resolved_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                """,
                (lease.tenant_id, lease.session_id, lease.sequence),
            )
            item = self._fetchone(
                """
                SELECT * FROM session_mailbox_item
                 WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                """,
                (lease.tenant_id, lease.session_id, lease.sequence + 1),
            )
            if item is None:
                status, retry_at, increment = SessionMailboxStatus.IDLE, None, 0
            elif item[8] is not None and item[8] > now:
                status, retry_at, increment = SessionMailboxStatus.RETRY_WAIT, item[8], 0
            else:
                status, retry_at, increment = SessionMailboxStatus.QUEUED, None, 1
            row = self._fetchone(
                """
                UPDATE session_mailbox
                   SET resolved_sequence=%s,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,status=%s,retry_at=%s,
                       queue_generation=queue_generation+%s,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                 RETURNING *
                """,
                (
                    lease.sequence,
                    status,
                    retry_at,
                    increment,
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            if item is not None and status == SessionMailboxStatus.QUEUED:
                self._emit_ready(row, item)
            return _mailbox_from_pg(row)

    def retry(
        self,
        lease: SessionMailboxLease,
        *,
        retry_at: datetime | None = None,
        increment_retry: bool = True,
    ) -> SessionMailbox:
        with self._transaction():
            self._assert_owned(lease)
            item = self._item(lease.tenant_id, lease.session_id, lease.sequence)
            retries = int(item[6]) + (1 if increment_retry else 0)
            self._execute(
                """
                UPDATE session_mailbox_item SET retry_count=%s,retry_at=%s
                 WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                """,
                (retries, retry_at, lease.tenant_id, lease.session_id, lease.sequence),
            )
            now = self._server_now()
            due = retry_at is None or retry_at <= now
            status = SessionMailboxStatus.QUEUED if due else SessionMailboxStatus.RETRY_WAIT
            row = self._fetchone(
                """
                UPDATE session_mailbox
                   SET status=%s,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,retry_count=%s,retry_at=%s,
                       queue_generation=queue_generation+%s,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                 RETURNING *
                """,
                (
                    status,
                    retries,
                    retry_at,
                    1 if due else 0,
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            if due:
                self._emit_ready(row, item)
            return _mailbox_from_pg(row)

    def dead_letter(self, lease: SessionMailboxLease, error: str) -> SessionMailbox:
        with self._transaction():
            self._assert_owned(lease)
            now = self._server_now()
            item = self._item(lease.tenant_id, lease.session_id, lease.sequence)
            self._execute(
                """
                UPDATE session_mailbox_item
                   SET retry_count=retry_count+1, resolved_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s AND sequence=%s
                """,
                (lease.tenant_id, lease.session_id, lease.sequence),
            )
            next_item = self._item(
                lease.tenant_id,
                lease.session_id,
                lease.sequence + 1,
                required=False,
            )
            if next_item is None:
                status, retry_at, increment = SessionMailboxStatus.IDLE, None, 0
            elif next_item[8] is not None and next_item[8] > now:
                status, retry_at, increment = SessionMailboxStatus.RETRY_WAIT, next_item[8], 0
            else:
                status, retry_at, increment = SessionMailboxStatus.QUEUED, None, 1
            row = self._fetchone(
                """
                UPDATE session_mailbox
                   SET resolved_sequence=%s,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,status=%s,retry_at=%s,
                       queue_generation=queue_generation+%s,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                 RETURNING *
                """,
                (
                    lease.sequence,
                    status,
                    retry_at,
                    increment,
                    lease.tenant_id,
                    lease.session_id,
                ),
            )
            self._emit_dead_letter(row, item, lease, error, now)
            if next_item is not None and status == SessionMailboxStatus.QUEUED:
                self._emit_ready(row, next_item)
            return _mailbox_from_pg(row)

    def recover(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        with self._transaction():
            row = self._row(tenant_id, session_id, required=False, lock=True)
            now = self._server_now()
            if row is None or row[10] is None or row[10] > now:
                return None
            session = self._fetchone(
                """
                SELECT owner,expires_at FROM session_lease
                 WHERE tenant_id=%s AND session_id=%s
                 FOR UPDATE
                """,
                (tenant_id, session_id),
            )
            if session and session[0] and session[1] and session[1] > now:
                return None
            sequence = int(row[5] or int(row[4]) + 1)
            item = self._item(tenant_id, session_id, sequence, required=False)
            if item is None or sequence > int(row[3]):
                status, retry_at, increment = SessionMailboxStatus.IDLE, None, 0
            elif item[8] is not None and item[8] > now:
                status, retry_at, increment = SessionMailboxStatus.RETRY_WAIT, item[8], 0
            else:
                status, retry_at, increment = SessionMailboxStatus.QUEUED, None, 1
            updated = self._fetchone(
                """
                UPDATE session_mailbox
                   SET status=%s,processing_sequence=NULL,
                       processing_message_id=NULL,lease_owner=NULL,
                       lease_until=NULL,retry_at=%s,
                       queue_generation=queue_generation+%s,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=%s AND session_id=%s
                 RETURNING *
                """,
                (status, retry_at, increment, tenant_id, session_id),
            )
            if item is not None and status == SessionMailboxStatus.QUEUED:
                self._emit_ready(updated, item)
            return _mailbox_from_pg(updated)

    def reconcile(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        mailbox = self.get(tenant_id, session_id)
        if mailbox is None:
            return None
        if (
            mailbox.status == SessionMailboxStatus.RUNNING
            and mailbox.lease_until is not None
            and mailbox.lease_until > self._server_now()
        ):
            return mailbox
        return self.recover(tenant_id, session_id)

    def sweep_expired_leases(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        with self._lock, self._conn.cursor() as cur:
            query = """
                SELECT tenant_id,session_id FROM session_mailbox
                 WHERE lease_until IS NOT NULL AND lease_until<=clock_timestamp()
            """
            params: list[object] = []
            if tenant_id is not None:
                query += " AND tenant_id=%s"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT %s"
            params.append(limit)
            cur.execute(query, params)
            rows = cur.fetchall()
        return sum(self.recover(row[0], row[1]) is not None for row in rows)

    def schedule_retries(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        with self._transaction():
            query = """
                SELECT tenant_id,session_id FROM session_mailbox
                 WHERE status=%s AND retry_at IS NOT NULL
                   AND retry_at<=clock_timestamp()
            """
            params: list[object] = [SessionMailboxStatus.RETRY_WAIT]
            if tenant_id is not None:
                query += " AND tenant_id=%s"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT %s"
            params.append(limit)
            rows = self._fetchall(query, params)
            handled = 0
            for tenant_id, session_id in rows:
                mailbox = self._row(tenant_id, session_id, lock=True)
                item = self._item(
                    tenant_id,
                    session_id,
                    int(mailbox[4]) + 1,
                )
                updated = self._fetchone(
                    """
                    UPDATE session_mailbox
                       SET status=%s,retry_at=NULL,
                           queue_generation=queue_generation+1,
                           updated_at=clock_timestamp()
                     WHERE tenant_id=%s AND session_id=%s
                     RETURNING *
                    """,
                    (SessionMailboxStatus.QUEUED, tenant_id, session_id),
                )
                self._emit_ready(updated, item)
                handled += 1
            return handled

    def reconcile_sessions(self, *, limit: int = 100, tenant_id: str | None = None) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        with self._transaction():
            query = """
                SELECT tenant_id,session_id FROM session_mailbox
                 WHERE status=%s
            """
            params: list[object] = [SessionMailboxStatus.QUEUED]
            if tenant_id is not None:
                query += " AND tenant_id=%s"
                params.append(tenant_id)
            query += " ORDER BY updated_at LIMIT %s"
            params.append(limit)
            rows = self._fetchall(query, params)
            handled = 0
            for tenant_id, session_id in rows:
                mailbox = self._row(tenant_id, session_id, lock=True)
                item = self._item(
                    tenant_id,
                    session_id,
                    int(mailbox[4]) + 1,
                )
                self._emit_ready(mailbox, item)
                handled += 1
            return handled

    def _assert_owned(self, lease: SessionMailboxLease) -> None:
        row = self._row(lease.tenant_id, lease.session_id, required=False, lock=True)
        if not _owns_session_mailbox(_mailbox_from_pg(row) if row else None, lease):
            raise SessionLeaseLost("session mailbox lease is no longer current")

    def _row(self, tenant_id, session_id, *, required=True, lock=False):
        suffix = " FOR UPDATE" if lock else ""
        row = self._fetchone(
            f"""
            SELECT tenant_id,session_id,status,accepted_sequence,resolved_sequence,
                   processing_sequence,processing_message_id,queue_generation,
                   lease_owner,lease_epoch,lease_until,retry_count,attempt,
                   priority,retry_at,updated_at
              FROM session_mailbox
             WHERE tenant_id=%s AND session_id=%s{suffix}
            """,
            (tenant_id, session_id),
        )
        if row is None and required:
            raise RuntimeError("session mailbox row is missing")
        return row

    def _item(self, tenant_id, session_id, sequence, required=True):
        row = self._fetchone(
            """
            SELECT tenant_id,session_id,sequence,message_id,trace_id,priority,
                   retry_count,attempt,retry_at,accepted_at,resolved_at
              FROM session_mailbox_item
             WHERE tenant_id=%s AND session_id=%s AND sequence=%s
            """,
            (tenant_id, session_id, sequence),
        )
        if row is None and required:
            raise RuntimeError("session mailbox item is missing")
        return row

    def _server_now(self):
        return self._fetchone("SELECT clock_timestamp()")[0]

    def _fetchone(self, query, params=()):
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def _fetchall(self, query, params=()):
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def _execute(self, query, params=()):
        with self._conn.cursor() as cur:
            cur.execute(query, params)

    def _emit_ready(self, mailbox, item) -> None:
        event_id = f"session-ready:{mailbox[0]}:{mailbox[1]}:{mailbox[7]}"
        payload = json.dumps(
            {
                "generation": int(mailbox[7]),
                "priority": int(item[5]),
                "trace_id": item[4],
                "created_at": now_utc().isoformat(),
            }
        )
        self._execute(
            """
            INSERT INTO outbox_message (
              event_id,tenant_id,topic,aggregate_id,payload_json,status,
              attempts,available_at,created_at,updated_at
            ) VALUES (%s,%s,%s,%s,%s::jsonb,'pending',0,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event_id,
                mailbox[0],
                "session.ready.v2",
                mailbox[1],
                payload,
                now_utc(),
                now_utc(),
                now_utc(),
            ),
        )

    def _emit_dead_letter(self, mailbox, item, lease, error, dead_at) -> None:
        event_id = f"session-dead:{mailbox[0]}:{mailbox[1]}:{item[2]}"
        payload = json.dumps(
            {
                "sequence": int(item[2]),
                "message_id": item[3],
                "trace_id": item[4],
                "attempt": lease.attempt,
                "retry_count": int(item[6]) + 1,
                "error": redact_secret_text(str(error))[:1000],
                "dead_at": dead_at.isoformat(),
            }
        )
        self._execute(
            """
            INSERT INTO outbox_message (
              event_id,tenant_id,topic,aggregate_id,payload_json,status,
              attempts,available_at,created_at,updated_at
            ) VALUES (%s,%s,%s,%s,%s::jsonb,'pending',0,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event_id,
                mailbox[0],
                "session.dead_letter.v2",
                mailbox[1],
                payload,
                dead_at,
                dead_at,
                dead_at,
            ),
        )


def _lease_with_expiry(
    lease: SessionMailboxLease,
    expires_at: datetime,
) -> SessionMailboxLease:
    return SessionMailboxLease(
        lease.tenant_id,
        lease.session_id,
        lease.message_id,
        lease.sequence,
        lease.owner,
        lease.epoch,
        expires_at,
        lease.attempt,
        lease.retry_count,
        lease.priority,
    )


def _mailbox_from_pg(row) -> SessionMailbox:
    mailbox = SessionMailbox(
        tenant_id=str(row[0]),
        session_id=str(row[1]),
        status=str(row[2]),
        accepted_sequence=int(row[3]),
        resolved_sequence=int(row[4]),
        processing_sequence=int(row[5]) if row[5] is not None else None,
        processing_message_id=str(row[6]) if row[6] is not None else None,
        queue_generation=int(row[7]),
        lease_owner=str(row[8]) if row[8] is not None else None,
        lease_epoch=int(row[9]),
        lease_until=row[10],
        retry_count=int(row[11]),
        attempt=int(row[12]),
        priority=int(row[13]),
        retry_at=row[14],
        updated_at=row[15],
    )
    validate_session_mailbox(mailbox)
    return mailbox


def _mailbox_export_row(row) -> dict:
    names = (
        "tenant_id",
        "session_id",
        "status",
        "accepted_sequence",
        "resolved_sequence",
        "processing_sequence",
        "processing_message_id",
        "queue_generation",
        "lease_owner",
        "lease_epoch",
        "lease_until",
        "retry_count",
        "attempt",
        "priority",
        "retry_at",
        "updated_at",
    )
    return dict(zip(names, row, strict=True))


def _item_export_row(row) -> dict:
    names = (
        "tenant_id",
        "session_id",
        "sequence",
        "message_id",
        "trace_id",
        "priority",
        "retry_count",
        "attempt",
        "retry_at",
        "accepted_at",
        "resolved_at",
    )
    return dict(zip(names, row, strict=True))


def _tenant_from_lease(args, kwargs):
    if args:
        return getattr(args[0], "tenant_id", None)
    return getattr(kwargs.get("lease"), "tenant_id", None)


def _tenant_from_first(args, kwargs):
    if args:
        return args[0]
    return kwargs.get("tenant_id")


_TENANT_METHODS = {
    "get": _tenant_from_first,
    "accept": _tenant_from_first,
    "has_unresolved_message": _tenant_from_first,
    "claim": _tenant_from_first,
    "claim_session": _tenant_from_first,
    "recover": _tenant_from_first,
    "reconcile": _tenant_from_first,
    "renew": _tenant_from_lease,
    "commit": _tenant_from_lease,
    "retry": _tenant_from_lease,
    "dead_letter": _tenant_from_lease,
    "sweep_expired_leases": _tenant_from_first,
    "schedule_retries": _tenant_from_first,
    "reconcile_sessions": _tenant_from_first,
}

for _method_name, _tenant_getter in _TENANT_METHODS.items():
    setattr(
        PostgresSessionMailboxStore,
        _method_name,
        rls_tenant_method(_tenant_getter)(getattr(PostgresSessionMailboxStore, _method_name)),
    )


__all__ = ["PostgresSessionMailboxStore"]
