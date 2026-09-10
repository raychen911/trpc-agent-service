from __future__ import annotations

import sqlite3
from datetime import timedelta

import pytest

from trpc_service.storage.base import AuditRecord, IdempotencyStatus, MemoryItem, SessionEvent, Summary, now_utc
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.sql_store import SQLiteStorage


def event(
    tenant: str = "tenant-a",
    session: str = "session-a",
    key: str | None = None,
    event_id: str | None = None,
) -> SessionEvent:
    return SessionEvent(
        tenant_id=tenant,
        session_id=session,
        event_id=event_id or f"event-{key or 'one'}",
        event_type="message",
        payload={"text": "hello"},
        trace_id="trace-1",
        idempotency_key=key,
    )


def test_sqlite_session_memory_summary_and_audit_round_trip(tmp_path):
    path = tmp_path / "state.sqlite3"
    with SQLiteStorage(path) as storage:
        assert storage.load_events("tenant-a", "missing") == []
        assert storage.load_state("tenant-a", "missing").latest_event_seq == 0
        assert storage.append_event(event(key="same")) == 1
        assert storage.append_event(event(event_id="ignored", key="same")) == 1
        assert storage.append_event(event(key=None, session="session-a")) == 2
        assert [item.seq for item in storage.load_events("tenant-a", "session-a", after_seq=1)] == [2]
        assert storage.compare_and_set_state("tenant-a", "session-a", 0, {"answer": "one"})
        assert not storage.compare_and_set_state("tenant-a", "session-a", 0, {"answer": "stale"})
        storage.restore_state("tenant-a", "session-a", {"answer": "restored"}, 4)
        assert storage.load_state("tenant-a", "session-a").state_version == 4

        first = MemoryItem("tenant-a", "m1", "session-a", "private hello", {"kind": "note"})
        storage.memory.put(first)
        storage.memory.put(MemoryItem("tenant-b", "m2", "session-a", "private hello"))
        assert storage.memory.search("tenant-a", "private")[0].memory_id == "m1"
        assert storage.memory.search("tenant-a", "private", scope_keys=())[0:1] == []
        assert storage.memory.search("tenant-a", "private", scope_keys=("other",)) == []
        storage.memory.put(MemoryItem("tenant-a", "m1", "session-b", "updated", version=2))
        assert storage.memory.search("tenant-a", "updated")[0].version == 2

        assert storage.summary.latest("tenant-a", "session-a") is None
        storage.summary.put(Summary("tenant-a", "session-a", "new", 2))
        storage.summary.put(Summary("tenant-a", "session-a", "old", 1))
        storage.summary.put(Summary("tenant-a", "session-a", "newer", 3))
        assert storage.summary.latest("tenant-a", "session-a").content == "newer"

        storage.audit.append(
            AuditRecord(
                "audit-1", "tenant-a", "allowed", "trace-a", metadata={"secret": "api_key=hidden"}
            )
        )
        assert storage.audit.list_by_tenant("tenant-a", limit=1)[0].audit_id == "audit-1"
        assert storage.audit.list_by_tenant("tenant-b") == []
        assert "CREATE TABLE" in storage.export_schema()

    with SQLiteStorage(path) as reopened:
        assert reopened.load_state("tenant-a", "session-a").state == {"answer": "restored"}
        assert reopened.memory.search("tenant-a", "updated")


def test_sqlite_idempotency_processing_failed_lease_and_delivery_claims(tmp_path):
    with SQLiteStorage(tmp_path / "idempotency.sqlite3") as storage:
        assert storage.idempotency.get("tenant-a", "missing") is None
        record = storage.idempotency.start("tenant-a", "key", "trace-1")
        assert record.status == IdempotencyStatus.PROCESSING
        assert storage.idempotency.start("tenant-a", "key", "trace-2").trace_id == "trace-1"
        completed = storage.idempotency.complete("tenant-a", "key", "response", {"text": "ok"})
        assert completed.status == IdempotencyStatus.COMPLETED
        assert storage.idempotency.claim_delivery("tenant-a", "key")
        assert not storage.idempotency.claim_delivery("tenant-a", "key")
        storage.idempotency.release_delivery("tenant-a", "key")
        assert storage.idempotency.claim_delivery("tenant-a", "key")
        storage.idempotency.release_delivery("tenant-a", "key")
        storage.idempotency.fail("tenant-a", "key", "provider")
        restarted = storage.idempotency.start("tenant-a", "key", "trace-3")
        assert restarted.status == IdempotencyStatus.PROCESSING
        assert restarted.attempt == 2

        stale = storage.idempotency.start("tenant-a", "stale", "trace")
        storage._conn.execute(
            "UPDATE idempotency SET updated_at=? WHERE tenant_id=? AND key=?",
            ((now_utc() - timedelta(seconds=10)).isoformat(), "tenant-a", "stale"),
        )
        storage._conn.commit()
        reclaimed = storage.idempotency.start("tenant-a", "stale", "new-trace", lease_seconds=1)
        assert reclaimed.attempt == stale.attempt + 1
        with pytest.raises(KeyError):
            storage.idempotency.complete("tenant-a", "unknown", "ref", {})


def test_sqlite_compensation_retry_dead_letter_and_tenant_filters(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPENSATION_MAX_ATTEMPTS", "1")
    with SQLiteStorage(tmp_path / "compensation.sqlite3") as storage:
        first = storage.compensation.enqueue("tenant-a", "memory.put", {"content": "one"}, task_id="task-a")
        storage.compensation.enqueue("tenant-b", "memory.put", {"content": "two"}, task_id="task-b")
        assert storage.compensation.enqueue("tenant-a", "memory.put", {}, task_id="task-a").payload == first.payload
        assert [task.task_id for task in storage.compensation.claim(10, tenant_id="tenant-a")] == ["task-a"]
        storage.compensation.fail("task-a", "api_key=secret", retry_after_seconds=0, tenant_id="tenant-a")
        dead = storage.compensation.claim(10, tenant_id="tenant-a")
        assert dead == []
        with pytest.raises(ValueError):
            storage.compensation.replay("task-a", tenant_id="tenant-b")
        replayed = storage.compensation.replay("task-a", tenant_id="tenant-a")
        assert replayed.status == "pending" and replayed.attempt == 0
        claimed = storage.compensation.claim(10, tenant_id="tenant-a")[0]
        storage.compensation.complete(claimed.task_id, tenant_id="tenant-b")
        assert storage.compensation.claim(10, tenant_id="tenant-a") == []
        storage.compensation.complete(claimed.task_id, tenant_id="tenant-a")
        assert storage.compensation.claim(10, tenant_id="tenant-a") == []


def test_sqlite_session_lock_and_lease_fencing_paths(tmp_path, monkeypatch):
    path = tmp_path / "locks.sqlite3"
    first = SQLiteStorage(path)
    second = SQLiteStorage(path)
    token = first.acquire_session_lock("tenant-a", "session-a", timeout=0)
    with pytest.raises(TimeoutError):
        second.acquire_session_lock("tenant-a", "session-a", timeout=0)
    first.release_session_lock("tenant-a", "session-a", "wrong")
    first.release_session_lock("tenant-a", "session-a", token)

    lease = first.acquire_session_lease("tenant-a", "session-a", timeout=0)
    first.validate_session_lease(lease)
    renewed = first.renew_session_lease(lease)
    assert renewed.fencing_token == lease.fencing_token
    with pytest.raises(TimeoutError):
        second.acquire_session_lease("tenant-a", "session-a", timeout=0)
    first.release_session_lease("tenant-a", "session-a", lease)
    with pytest.raises(SessionLeaseLost):
        first.validate_session_lease(lease)
    monkeypatch.setenv("SQLITE_SESSION_LOCK_TTL_SECONDS", "1")
    lock_token = first.acquire_session_lock("tenant-a", "other", timeout=0)
    first.release_session_lock("tenant-a", "other", lock_token)
    first.close()
    second.close()


def test_sqlite_legacy_schema_upgrade_adds_columns(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE idempotency (tenant_id TEXT NOT NULL, key TEXT NOT NULL,
          status TEXT NOT NULL, response_ref TEXT, result_json TEXT, trace_id TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (tenant_id, key));
        CREATE TABLE session_lock (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL,
          token TEXT NOT NULL, expires_at TEXT NOT NULL, PRIMARY KEY (tenant_id, session_id));
        """
    )
    connection.commit()
    connection.close()
    storage = SQLiteStorage(path)
    columns = {row["name"] for row in storage._conn.execute("PRAGMA table_info(idempotency)")}
    lock_columns = {row["name"] for row in storage._conn.execute("PRAGMA table_info(session_lock)")}
    assert {"attempt"} <= columns
    assert {"fencing_token"} <= lock_columns
    storage.close()
