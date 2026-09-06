from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from threading import RLock

import pytest

from trpc_service.storage.base import now_utc
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.mailbox import (
    InMemoryMailboxStore,
    MailboxRecord,
    MailboxStatus,
    PostgresMailboxStore,
    SQLiteMailboxStore,
    _json_object,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=()):
        normalized = " ".join(str(query).split())
        self.connection.executed.append((normalized, params))
        self.rowcount = self.connection.next_rowcount
        self.connection.next_rowcount = 1

    def fetchone(self):
        return self.connection.one.pop(0) if self.connection.one else None

    def fetchall(self):
        return self.connection.many.pop(0) if self.connection.many else []


class FakeConnection:
    def __init__(self):
        self.one = []
        self.many = []
        self.executed = []
        self.next_rowcount = 1

    def cursor(self):
        return FakeCursor(self)

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def sqlite_store() -> SQLiteMailboxStore:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return SQLiteMailboxStore(connection, RLock())


def pg_row(
    *,
    status=MailboxStatus.PENDING,
    attempts=0,
    owner=None,
    fencing_token=0,
    lease_until=None,
    available_at=None,
    last_error=None,
):
    now = now_utc()
    return (
        "tenant-a", "session-a", 1, "message-1", "dedupe-1", {"text": "hello"},
        status, attempts, owner, fencing_token, lease_until, available_at or now,
        last_error, now, now,
    )


def test_sqlite_mailbox_handles_duplicate_restore_and_skips_blocked_or_terminal_records():
    store = sqlite_store()
    try:
        first = store.enqueue("tenant-a", "session-a", "message-1", "dedupe-1", {"x": 1})
        assert store.restore(first).sequence == first.sequence
        first_lease = store.claim_next("tenant-a", "session-a", "worker", 30)
        assert first_lease is not None
        store.complete(first_lease)

        blocked = store.enqueue("tenant-a", "session-a", "message-2", "dedupe-2", {"x": 2})
        store._conn.execute(
            "UPDATE mailbox_message SET available_at=? WHERE tenant_id=? AND dedupe_key=?",
            ((now_utc() + timedelta(minutes=1)).isoformat(), "tenant-a", "dedupe-2"),
        )
        store._conn.commit()
        assert store.claim_next("tenant-a", "session-a", "worker", 30) is None
        store._conn.execute(
            "UPDATE mailbox_message SET available_at=? WHERE tenant_id=? AND dedupe_key=?",
            (now_utc().isoformat(), "tenant-a", "dedupe-2"),
        )
        store._conn.commit()
        second_lease = store.claim_next("tenant-a", "session-a", "worker", 30)
        assert second_lease is not None
        store.complete(second_lease)
        assert store.claim_next("tenant-a", "session-a", "worker", 30) is None
    finally:
        store._conn.close()


@pytest.mark.parametrize("store_factory", [InMemoryMailboxStore, sqlite_store])
def test_mailbox_expired_lease_takeover_and_stale_mutations_are_fenced(store_factory):
    store = store_factory()
    first = store.enqueue("tenant-a", "session-a", "message-1", "dedupe-1", {"x": 1})
    lease = store.claim_next("tenant-a", "session-a", "worker-a", 30)
    assert lease is not None
    expired = store.get("tenant-a", "dedupe-1")
    assert expired is not None
    expired.lease_until = now_utc() - timedelta(seconds=1)
    if isinstance(store, InMemoryMailboxStore):
        store._records[("tenant-a", "session-a", 1)] = expired
    else:
        store._conn.execute(
            "UPDATE mailbox_message SET lease_until=? WHERE tenant_id=? AND dedupe_key=?",
            (expired.lease_until.isoformat(), "tenant-a", "dedupe-1"),
        )
        store._conn.commit()
    replacement = store.claim_next("tenant-a", "session-a", "worker-b", 30)
    assert replacement is not None and replacement.fencing_token > first.fencing_token
    with pytest.raises(SessionLeaseLost):
        store.complete(lease)
    store.complete(replacement)
    if hasattr(store, "_conn"):
        store._conn.close()


def test_sqlite_mailbox_update_rowcount_loss_and_configuration_edges(monkeypatch):
    store = sqlite_store()
    try:
        store.enqueue("tenant", "session", "message", "dedupe", {})
        lease = store.claim_next("tenant", "session", "worker", 30)
        assert lease is not None
        monkeypatch.setenv("MAILBOX_MAX_ATTEMPTS", "not-an-int")
        with pytest.raises(ValueError, match="MAILBOX_MAX_ATTEMPTS"):
            store.fail(lease, "failure")
        store._conn.execute(
            "UPDATE mailbox_message SET owner='other' WHERE tenant_id='tenant'"
        )
        store._conn.commit()
        with pytest.raises(SessionLeaseLost):
            store.renew(lease, 30)
    finally:
        store._conn.close()


def test_postgres_mailbox_protocol_lifecycle_and_filters(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "1")
    connection.one = [(True,)]
    store = PostgresMailboxStore(connection, RLock())
    assert any("CREATE TABLE IF NOT EXISTS mailbox_message" in query for query, _ in connection.executed)

    connection.one = [None, (0,), (1,)]
    created = store.enqueue("tenant-a", "session-a", "message-1", "dedupe-1", {"text": "hello"})
    assert created.sequence == 1

    connection.one = [pg_row(status=MailboxStatus.PENDING)]
    assert store.enqueue("tenant-a", "session-a", "message-1", "dedupe-1", {}) is not None
    connection.one = [pg_row()]
    assert store.get("tenant-a", "dedupe-1").payload == {"text": "hello"}
    connection.many = [[pg_row(), pg_row(status=MailboxStatus.COMPLETED)]]
    assert len(store.list_by_tenant("tenant-a")) == 2

    restored = MailboxRecord(
        "tenant-a", "session-a", 2, "message-2", "dedupe-2", {"x": 2},
        status=MailboxStatus.PROCESSING, owner="old", fencing_token=4,
        lease_until=now_utc() + timedelta(minutes=1),
    )
    connection.one = [pg_row(status=MailboxStatus.PENDING, fencing_token=4)]
    assert store.restore(restored).status == MailboxStatus.PENDING

    active = pg_row(status=MailboxStatus.PROCESSING, attempts=1, owner="worker", fencing_token=5,
                    lease_until=now_utc() + timedelta(minutes=1))
    claimed = pg_row(status=MailboxStatus.PROCESSING, attempts=2, owner="worker", fencing_token=6,
                     lease_until=now_utc() + timedelta(minutes=1))
    connection.one = [active, (6,), claimed]
    lease = store.claim_next("tenant-a", "session-a", "worker", 30)
    assert lease is not None and lease.attempts == 2

    connection.one = [active]
    store.complete(lease)
    connection.one = [active]
    store.fail(lease, "api_key=secret", retry_after_seconds=0)
    connection.one = [active, claimed]
    renewed = store.renew(lease, 30)
    assert renewed.fencing_token == 6

    connection.next_rowcount = 3
    assert store.recover_expired("tenant-a", "session-a") == 3
    assert any("FOR UPDATE SKIP LOCKED" in query for query, _ in connection.executed)


def test_postgres_mailbox_ownership_and_retry_dead_letter_branches(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "0")
    store = PostgresMailboxStore(connection, RLock())
    record = MailboxRecord(
        "tenant-a", "session-a", 1, "message-1", "dedupe-1", {},
        status=MailboxStatus.PROCESSING, owner="worker", fencing_token=1,
        lease_until=now_utc() + timedelta(minutes=1),
    )
    connection.one = [None]
    with pytest.raises(SessionLeaseLost):
        store.complete(record)
    connection.one = [pg_row(status=MailboxStatus.PROCESSING, attempts=1, owner="worker", fencing_token=1,
                              lease_until=now_utc() + timedelta(minutes=1))]
    monkeypatch.setenv("MAILBOX_MAX_ATTEMPTS", "1")
    store.fail(record, "token=private", retry_after_seconds=-1)
    connection.one = [pg_row(status=MailboxStatus.PROCESSING, owner="worker", fencing_token=1,
                              lease_until=now_utc() + timedelta(minutes=1))]
    monkeypatch.setenv("MAILBOX_LEASE_SECONDS", "bad")
    with pytest.raises(ValueError, match="MAILBOX_LEASE_SECONDS"):
        store.renew(record)


def test_mailbox_json_object_accepts_db_shapes_and_rejects_scalars():
    assert _json_object({"x": 1}) == {"x": 1}
    assert _json_object(b'{"x": 1}') == {"x": 1}
    assert _json_object(json.dumps({"x": 1})) == {"x": 1}
    with pytest.raises(ValueError, match="JSON object"):
        _json_object([1])
