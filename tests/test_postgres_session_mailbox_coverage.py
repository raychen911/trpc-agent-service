"""Protocol-level branch coverage for the PostgreSQL session mailbox.

These tests exercise the adapter with a programmable DB-API connection.  They
validate SQL/transaction behavior without claiming a live PostgreSQL server.
Live database evidence remains the responsibility of the acceptance gate.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import RLock

import pytest

from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.postgres_session_mailbox import (
    PostgresSessionMailboxStore,
)
from trpc_service.storage.session_mailbox import (
    SessionMailbox,
    SessionMailboxClaimStatus,
    SessionMailboxLease,
    SessionMailboxStatus,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=()):
        self.connection.executed.append((" ".join(str(query).split()), params))

    def fetchone(self):
        return self.connection.one.pop(0) if self.connection.one else None

    def fetchall(self):
        return self.connection.many.pop(0) if self.connection.many else []


class Connection:
    def __init__(self):
        self.one = []
        self.many = []
        self.executed = []

    def cursor(self):
        return Cursor(self)

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def mailbox(
    status=SessionMailboxStatus.RUNNING,
    *,
    accepted=1,
    resolved=0,
    processing=None,
    message=None,
    owner=None,
    epoch=1,
    lease_until=None,
    generation=1,
    retry_at=None,
):
    return (
        "tenant-a", "session-a", status, accepted, resolved, processing,
        message, generation, owner, epoch, lease_until, 0, 1, 3, retry_at, NOW,
    )


def item(sequence=1, *, message="message-1", retry_at=None, retry_count=0, attempt=1):
    return (
        "tenant-a", "session-a", sequence, message, "trace-1", 3,
        retry_count, attempt, retry_at, NOW, None,
    )


def lease(*, message="message-1", sequence=1, owner="worker-a", epoch=1):
    return SessionMailboxLease(
        "tenant-a", "session-a", message, sequence, owner, epoch,
        NOW + timedelta(minutes=5), 1, 0, 3,
    )


def store():
    result = PostgresSessionMailboxStore.__new__(PostgresSessionMailboxStore)
    result._conn = Connection()
    result._lock = RLock()
    result._server_now = lambda: NOW
    result._execute = lambda query, params=(): result._conn.executed.append(
        ("execute", " ".join(str(query).split()), params)
    )
    return result


def test_init_schema_and_connection_replacement(monkeypatch):
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "0")
    connection = Connection()
    instance = PostgresSessionMailboxStore(connection, RLock())
    assert instance._conn is connection

    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "1")
    connection.one = [(True,)]
    instance = PostgresSessionMailboxStore(connection, RLock())
    assert any("CREATE TABLE IF NOT EXISTS session_mailbox" in query for query, _ in connection.executed)
    replacement = Connection()
    instance.set_connection(replacement)
    assert instance._conn is replacement


def test_helpers_get_fetch_and_export_restore():
    instance = store()
    instance._conn.one = [mailbox(SessionMailboxStatus.QUEUED, processing=None, message=None, owner=None)]
    current = instance.get("tenant-a", "session-a")
    assert current is not None and current.status == SessionMailboxStatus.QUEUED

    instance._conn.one = [(1,)]
    assert instance.has_unresolved_message("tenant-a", "session-a", "message-1")
    instance._conn.one = [None]
    assert not instance.has_unresolved_message("tenant-a", "session-a", "missing")

    instance._conn.many = [[mailbox()], [item()]]
    exported = instance.export_by_tenant("tenant-a")
    assert exported["mailboxes"][0]["session_id"] == "session-a"
    assert exported["items"][0]["message_id"] == "message-1"

    instance.restore_export(exported)
    assert len(instance._conn.executed) >= 2
    instance._conn.one = [None]
    with pytest.raises(RuntimeError, match="mailbox row is missing"):
        instance._row("tenant-a", "missing")
    instance._conn.one = [None]
    assert instance._row("tenant-a", "missing", required=False) is None
    instance._conn.one = [None]
    with pytest.raises(RuntimeError, match="item is missing"):
        instance._item("tenant-a", "session-a", 9)


def test_accept_duplicate_and_retry_wait_paths():
    instance = store()
    current = mailbox(SessionMailboxStatus.IDLE, accepted=0, resolved=0, processing=None,
                      message=None, owner=None, epoch=0, lease_until=None, generation=0)
    updated = mailbox(SessionMailboxStatus.QUEUED, accepted=1, resolved=0, processing=None,
                      message=None, owner=None, epoch=0, lease_until=None, generation=1)
    instance._row = lambda *args, **kwargs: current
    instance._fetchone = lambda query, params=(): (
        None if "SELECT 1" in query else updated
    )
    instance._item = lambda *args, **kwargs: item()
    accepted = instance.accept("tenant-a", "session-a", "message-1", priority=3)
    assert accepted.status == SessionMailboxStatus.QUEUED

    duplicate = mailbox(SessionMailboxStatus.QUEUED, accepted=1, resolved=0, processing=None,
                         message=None, owner=None, epoch=0, lease_until=None, generation=1)
    instance._row = lambda *args, **kwargs: duplicate
    instance._fetchone = lambda query, params=(): (1,) if "SELECT 1" in query else None
    duplicate_result = instance.accept("tenant-a", "session-a", "message-1")
    assert duplicate_result.status == SessionMailboxStatus.QUEUED
    assert duplicate_result.accepted_sequence == 1

    waiting = mailbox(SessionMailboxStatus.RETRY_WAIT, accepted=1, resolved=0, processing=None,
                      message=None, owner=None, epoch=0, lease_until=None, generation=1,
                      retry_at=NOW + timedelta(minutes=2))
    waiting_updated = mailbox(SessionMailboxStatus.RETRY_WAIT, accepted=2, resolved=0, processing=None,
                              message=None, owner=None, epoch=0, lease_until=None, generation=1,
                              retry_at=waiting[14])
    instance._row = lambda *args, **kwargs: waiting
    instance._fetchone = lambda query, params=(): (
        None if "SELECT 1" in query else waiting_updated
    )
    instance._item = lambda *args, **kwargs: item()
    result = instance.accept("tenant-a", "session-a", "message-2", retry_at=NOW + timedelta(minutes=3))
    assert result.status == SessionMailboxStatus.RETRY_WAIT


def test_claim_claim_session_and_renew_paths():
    instance = store()
    queued = mailbox(processing=None, message=None, owner=None, epoch=0, lease_until=None)
    instance._row = lambda *args, **kwargs: queued
    instance._item = lambda *args, **kwargs: item()
    instance._fetchone = lambda query, params=(): (NOW + timedelta(minutes=1),) if "RETURNING lease_until" in query else None
    claimed = instance.claim("tenant-a", "session-a", "worker-a", 30)
    assert claimed is not None and claimed.message_id == "message-1"

    instance._row = lambda *args, **kwargs: None
    assert instance.claim("tenant-a", "missing", "worker-a", 30) is None

    instance._row = lambda *args, **kwargs: mailbox(SessionMailboxStatus.IDLE, accepted=0, resolved=0,
                                                   processing=None, message=None, owner=None,
                                                   epoch=0, lease_until=None, generation=0)
    stale = instance.claim_session("tenant-a", "session-a", "worker-a", 30, expected_generation=99)
    assert stale.status == SessionMailboxClaimStatus.STALE

    instance._row = lambda *args, **kwargs: mailbox(SessionMailboxStatus.IDLE, accepted=0, resolved=0,
                                                   processing=None, message=None, owner=None,
                                                   epoch=0, lease_until=None, generation=0)
    instance._fetchone = lambda query, params=(): None
    empty = instance.claim_session("tenant-a", "session-a", "worker-a", 30)
    assert empty.status == SessionMailboxClaimStatus.EMPTY

    running = mailbox()
    current_lease = lease()
    instance._row = lambda *args, **kwargs: running
    instance._fetchone = lambda query, params=(): (NOW + timedelta(minutes=10),)
    renewed = instance.renew(current_lease, 30)
    assert renewed.expires_at == NOW + timedelta(minutes=10)
    instance._fetchone = lambda query, params=(): None
    with pytest.raises(SessionLeaseLost):
        instance.renew(current_lease, 30)


def test_commit_retry_and_dead_letter_emit_outbox():
    instance = store()
    current = mailbox(message="message-1", processing=1, owner="worker-a",
                      lease_until=NOW + timedelta(minutes=5))
    current_after = mailbox(SessionMailboxStatus.QUEUED, accepted=2, resolved=1,
                            processing=None, message=None, owner=None, epoch=1,
                            lease_until=None, generation=2)
    next_item = item(2, message="message-2")
    instance._row = lambda *args, **kwargs: current
    instance._item = lambda tenant, session, sequence, required=True: next_item if sequence == 2 else item()
    instance._fetchone = lambda query, params=(): current_after if "UPDATE session_mailbox" in query else None
    committed = instance.commit(lease())
    assert committed.resolved_sequence == 1

    retry_after = NOW + timedelta(minutes=2)
    retry_state = mailbox(SessionMailboxStatus.RETRY_WAIT, accepted=1, resolved=0,
                          processing=None, message=None, owner=None, epoch=1,
                          lease_until=None, generation=1, retry_at=retry_after)
    instance._row = lambda *args, **kwargs: current
    instance._item = lambda *args, **kwargs: item()
    instance._fetchone = lambda query, params=(): retry_state if "UPDATE session_mailbox" in query else None
    result = instance.retry(lease(), retry_at=retry_after)
    assert result.status == SessionMailboxStatus.RETRY_WAIT

    dead_state = mailbox(SessionMailboxStatus.IDLE, accepted=1, resolved=1,
                         processing=None, message=None, owner=None, epoch=1,
                         lease_until=None, generation=2)
    instance._row = lambda *args, **kwargs: current
    instance._item = lambda tenant, session, sequence, required=True: None if sequence == 2 else item()
    instance._fetchone = lambda query, params=(): dead_state if "UPDATE session_mailbox" in query else None
    dead = instance.dead_letter(lease(), "api_key=secret-value")
    assert dead.status == SessionMailboxStatus.IDLE
    assert any("session-dead:" in str(params) for _, _, params in instance._conn.executed if isinstance(_, str))


def test_recover_reconcile_and_schedulers():
    instance = store()
    expired = mailbox(lease_until=NOW - timedelta(minutes=1))
    recovered = mailbox(SessionMailboxStatus.QUEUED, processing=None, message=None, owner=None,
                        epoch=1, lease_until=None, generation=2)
    instance._row = lambda *args, **kwargs: expired
    instance._item = lambda *args, **kwargs: item()
    instance._fetchone = lambda query, params=(): (
        None if "FROM session_lease" in query else recovered
    )
    assert instance.recover("tenant-a", "session-a").status == SessionMailboxStatus.QUEUED

    active = mailbox()
    instance._row = lambda *args, **kwargs: active
    assert instance.recover("tenant-a", "session-a") is None
    instance.get = lambda *args: None

    instance._fetchall = lambda query, params=(): [("tenant-a", "session-a")]
    instance._row = lambda *args, **kwargs: recovered
    instance._item = lambda *args, **kwargs: item()
    instance._fetchone = lambda query, params=(): recovered
    assert instance.schedule_retries(limit=2, tenant_id="tenant-a") == 1
    assert instance.reconcile_sessions(limit=2, tenant_id="tenant-a") == 1
    instance._conn.many = [[("tenant-a", "session-a")]]
    instance.recover = lambda *args: recovered
    assert instance.sweep_expired_leases(limit=2, tenant_id="tenant-a") == 1

    with pytest.raises(ValueError):
        instance.schedule_retries(limit=0)
    with pytest.raises(ValueError):
        instance.reconcile_sessions(limit=1001)
    with pytest.raises(ValueError):
        instance.sweep_expired_leases(limit=0)


def test_reconcile_and_claim_running_statuses():
    instance = store()
    running = mailbox(message="message-1", processing=1, owner="worker-a",
                      lease_until=NOW + timedelta(minutes=5))
    instance.get = lambda *args: SessionMailbox(
        "tenant-a", "session-a", status=SessionMailboxStatus.RUNNING,
        accepted_sequence=1, resolved_sequence=0, processing_sequence=1,
        processing_message_id="message-1", queue_generation=1, lease_owner="worker-a",
        lease_epoch=1, lease_until=NOW + timedelta(minutes=1), attempt=1, priority=3,
    )
    assert instance.reconcile("tenant-a", "session-a").status == SessionMailboxStatus.RUNNING

    instance.get = lambda *args: None
    assert instance.reconcile("tenant-a", "missing") is None

    instance.get = lambda *args: SessionMailbox(
        "tenant-a", "session-a", status=SessionMailboxStatus.RUNNING,
        accepted_sequence=1, resolved_sequence=0, processing_sequence=1,
        processing_message_id="message-1", queue_generation=1, lease_owner="worker-a",
        lease_epoch=1, lease_until=NOW - timedelta(minutes=1), attempt=1, priority=3,
    )
    instance.recover = lambda *args: SessionMailbox("tenant-a", "session-a")
    assert instance.reconcile("tenant-a", "session-a").status == SessionMailboxStatus.IDLE

    instance._row = lambda *args, **kwargs: running
    instance._fetchone = lambda query, params=(): None
    claim = instance.claim_session("tenant-a", "session-a", "worker-b", 30)
    assert claim.status == SessionMailboxClaimStatus.RUNNING


def test_validation_and_server_clock_helpers():
    instance = store()
    instance._conn.one = [(NOW,)]
    assert instance._server_now() == NOW
    instance._conn.one = [None]
    assert instance._fetchone("SELECT 1") is None
    instance._execute("UPDATE x")
    assert instance._conn.executed
    instance._conn.many = [[("a", "b")]]
    assert instance._fetchall("SELECT 1") == [("a", "b")]
