"""Boundary and invariant coverage for both session mailbox backends."""

import sqlite3
from datetime import timedelta
from threading import RLock

import pytest

from trpc_service.storage.base import now_utc
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.session_mailbox import (
    InMemorySessionMailboxStore,
    SessionMailbox,
    SessionMailboxClaimStatus,
    SessionMailboxStatus,
    SQLiteSessionMailboxStore,
    validate_session_mailbox,
)


@pytest.fixture(params=["memory", "sqlite"])
def mailbox(request):
    if request.param == "memory":
        yield InMemorySessionMailboxStore()
        return
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    store = SQLiteSessionMailboxStore(connection, RLock())
    try:
        yield store
    finally:
        connection.close()


def test_mailbox_rejects_invalid_identifiers_priority_and_leases(mailbox):
    for args in (("", "session", "message"), ("tenant", "", "message"), ("tenant", "session", "")):
        with pytest.raises(ValueError, match="identifiers"):
            mailbox.accept(*args)
    with pytest.raises(ValueError, match="priority"):
        mailbox.accept("tenant", "session", "message", priority=-1)
    with pytest.raises(ValueError, match="priority"):
        mailbox.accept("tenant", "session", "message", priority=True)
    with pytest.raises(ValueError, match="identifiers"):
        mailbox.claim("", "session", "worker", 1)
    with pytest.raises(ValueError, match="lease"):
        mailbox.claim("tenant", "session", "worker", 0)
    with pytest.raises(ValueError, match="lease"):
        mailbox.claim("tenant", "session", "worker", -1)
    with pytest.raises(ValueError):
        mailbox.claim("tenant", "session", "worker", "bad")


def test_memory_mailbox_expired_retry_and_missing_item_paths():
    mailbox = InMemorySessionMailboxStore()
    assert mailbox.claim("tenant", "missing", "worker", 1) is None
    mailbox.accept("tenant", "session", "message", retry_at=now_utc() + timedelta(minutes=1))
    assert mailbox.claim("tenant", "session", "worker", 1) is None

    current = mailbox._mailboxes[("tenant", "session")]
    current.retry_at = now_utc() - timedelta(seconds=1)
    mailbox._mailboxes[("tenant", "session")] = current
    assert mailbox.accept("tenant", "session", "message-2").status == SessionMailboxStatus.QUEUED

    mailbox._items.pop(("tenant", "session", 1))
    with pytest.raises(RuntimeError, match="item is missing"):
        mailbox.claim("tenant", "session", "worker", 1)

    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker", 1)
    assert lease is not None
    mailbox._items[("tenant", "session", 1)].retry_at = now_utc() + timedelta(minutes=1)
    waiting = mailbox.retry(lease, retry_at=mailbox._items[("tenant", "session", 1)].retry_at)
    assert waiting.status == SessionMailboxStatus.RETRY_WAIT
    assert mailbox.reconcile("tenant", "missing") is None
    assert mailbox.reconcile("tenant", "session") is None


def test_memory_mailbox_recovery_handles_future_retry_and_missing_item():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker", 1)
    assert lease is not None
    item = mailbox._items[("tenant", "session", 1)]
    item.retry_at = now_utc() + timedelta(minutes=1)
    current = mailbox._mailboxes[("tenant", "session")]
    current.lease_until = now_utc() - timedelta(seconds=1)
    mailbox._mailboxes[("tenant", "session")] = current
    assert mailbox.recover("tenant", "session").status == SessionMailboxStatus.RETRY_WAIT

    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker", 1)
    mailbox._items.pop(("tenant", "session", 1))
    current = mailbox._mailboxes[("tenant", "session")]
    current.lease_until = now_utc() - timedelta(seconds=1)
    mailbox._mailboxes[("tenant", "session")] = current
    assert mailbox.recover("tenant", "session").status == SessionMailboxStatus.IDLE
    assert mailbox.sweep_expired_leases(tenant_id="other") == 0


def test_sqlite_mailbox_missing_rows_and_retry_recovery_edges():
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE outbox_message (
          event_id TEXT PRIMARY KEY, tenant_id TEXT, topic TEXT,
          aggregate_id TEXT, payload_json TEXT, status TEXT,
          attempts INTEGER, available_at TEXT, created_at TEXT, updated_at TEXT
        )
        """
    )
    connection.commit()
    mailbox = SQLiteSessionMailboxStore(connection, RLock())
    try:
        assert mailbox.get("tenant", "missing") is None
        assert mailbox.has_unresolved_message("tenant", "missing", "message") is False
        assert mailbox.export_by_tenant("tenant")["mailboxes"] == []
        with pytest.raises(RuntimeError, match="row is missing"):
            mailbox._mailbox_row("tenant", "missing")

        mailbox.accept("tenant", "session", "message", retry_at=now_utc() + timedelta(minutes=1))
        assert mailbox.claim("tenant", "session", "worker", 1) is None
        assert mailbox.reconcile("tenant", "session") is None
        with pytest.raises(RuntimeError, match="item is missing"):
            mailbox._item_row("tenant", "session", 99)
        assert mailbox.sweep_expired_leases(tenant_id="other") == 0

        mailbox.accept("tenant", "other-session", "message-2")
        connection.execute(
            "DELETE FROM session_mailbox_item WHERE tenant_id=? AND session_id=?",
            ("tenant", "other-session"),
        )
        connection.commit()
        with pytest.raises(RuntimeError, match="item is missing"):
            mailbox.claim("tenant", "other-session", "worker", 1)
    finally:
        connection.close()


def test_sqlite_mailbox_recovery_and_reconcile_missing_item_state():
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE outbox_message (
          event_id TEXT PRIMARY KEY, tenant_id TEXT, topic TEXT,
          aggregate_id TEXT, payload_json TEXT, status TEXT,
          attempts INTEGER, available_at TEXT, created_at TEXT, updated_at TEXT
        )
        """
    )
    connection.commit()
    mailbox = SQLiteSessionMailboxStore(connection, RLock())
    try:
        mailbox.accept("tenant", "session", "message")
        lease = mailbox.claim("tenant", "session", "worker", 1)
        assert lease is not None
        future = now_utc() + timedelta(minutes=1)
        mailbox.retry(lease, retry_at=future)
        assert mailbox.reconcile("tenant", "session") is None

        mailbox.accept("tenant", "broken", "message-2")
        connection.execute(
            "DELETE FROM session_mailbox_item WHERE tenant_id=? AND session_id=?",
            ("tenant", "broken"),
        )
        connection.execute(
            "UPDATE session_mailbox SET status=?, retry_at=?, updated_at=? WHERE tenant_id=? AND session_id=?",
            (SessionMailboxStatus.RETRY_WAIT, (now_utc() - timedelta(seconds=1)).isoformat(), now_utc().isoformat(), "tenant", "broken"),
        )
        connection.commit()
        with pytest.raises(RuntimeError, match="item is missing"):
            mailbox.schedule_retries(tenant_id="tenant")
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("mailbox", "message"),
    [
        (SessionMailbox("", "session"), "identifiers"),
        (SessionMailbox("tenant", "session", accepted_sequence=1), "idle"),
        (SessionMailbox("tenant", "session", queue_generation=-1), "generations"),
        (SessionMailbox("tenant", "session", status=SessionMailboxStatus.QUEUED), "queued"),
        (SessionMailbox("tenant", "session", status=SessionMailboxStatus.RETRY_WAIT, accepted_sequence=1), "retry_at"),
        (SessionMailbox("tenant", "session", status=SessionMailboxStatus.RUNNING, accepted_sequence=2, resolved_sequence=0, processing_sequence=2, processing_message_id="m", lease_owner="w", lease_until=now_utc()), "next sequence"),
    ],
)
def test_session_mailbox_invariant_matrix(mailbox, message):
    with pytest.raises(ValueError, match=message):
        validate_session_mailbox(mailbox)


def test_claim_session_reports_running_and_empty_states():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    first = mailbox.claim_session("tenant", "session", "worker-a", 30)
    assert first.status == SessionMailboxClaimStatus.CLAIMED
    running = mailbox.claim_session("tenant", "session", "worker-b", 30)
    assert running.status == SessionMailboxClaimStatus.RUNNING
    mailbox.commit(first.lease)
    empty = mailbox.claim_session("tenant", "session", "worker-b", 30)
    assert empty.status == SessionMailboxClaimStatus.EMPTY
    with pytest.raises(SessionLeaseLost):
        mailbox.commit(first.lease)
