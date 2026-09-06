import tempfile
import time
from datetime import timedelta
from pathlib import Path

import pytest

from trpc_service.gateway.router import _SessionMailboxV2Adapter
from trpc_service.storage.base import now_utc
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.mirror import MirroredStorageBundle
from trpc_service.storage.session_mailbox import (
    SessionMailbox,
    SessionMailboxClaimStatus,
    SessionMailboxStatus,
    validate_session_mailbox,
)
from trpc_service.storage.sql_store import SQLiteStorage


def test_session_mailbox_serializes_one_session_and_emits_generation_wakeup():
    storage = InMemoryStorage()
    mailbox = storage.session_mailbox_v2

    first = mailbox.accept("tenant", "session", "message-1", trace_id="trace-1")
    second = mailbox.accept("tenant", "session", "message-2", trace_id="trace-2")

    assert first.status == SessionMailboxStatus.QUEUED
    assert second.accepted_sequence == 2
    assert second.queue_generation == first.queue_generation
    assert len(mailbox.outbox) == 1
    assert mailbox.outbox[0].topic == "session.ready.v2"
    assert mailbox.outbox[0].payload["generation"] == 1

    claim = mailbox.claim("tenant", "session", "worker-a", 30)
    assert claim is not None and claim.sequence == 1
    assert mailbox.claim("tenant", "session", "worker-b", 30) is None

    completed = mailbox.commit(claim)
    assert completed.status == SessionMailboxStatus.QUEUED
    assert completed.resolved_sequence == 1
    assert completed.queue_generation == 2
    assert len(mailbox.outbox) == 2

    next_claim = mailbox.claim("tenant", "session", "worker-b", 30)
    assert next_claim is not None and next_claim.sequence == 2


def test_session_mailbox_generation_rejects_stale_wakeup():
    mailbox = InMemoryStorage().session_mailbox_v2
    accepted = mailbox.accept("tenant", "session", "message-1")

    stale = mailbox.claim_session(
        "tenant",
        "session",
        "worker-a",
        30,
        expected_generation=accepted.queue_generation + 1,
    )
    assert stale.status == SessionMailboxClaimStatus.STALE

    current = mailbox.claim_session(
        "tenant",
        "session",
        "worker-a",
        30,
        expected_generation=accepted.queue_generation,
    )
    assert current.claimed


def test_session_mailbox_expired_takeover_fences_old_worker():
    mailbox = InMemoryStorage().session_mailbox_v2
    mailbox.accept("tenant", "session", "message-1")
    old = mailbox.claim("tenant", "session", "worker-a", 30)
    assert old is not None

    current = mailbox.get("tenant", "session")
    assert current is not None
    current.lease_until = current.lease_until - timedelta(seconds=60)
    mailbox._mailboxes[("tenant", "session")] = current

    replacement = mailbox.claim("tenant", "session", "worker-b", 30)
    assert replacement is not None
    assert replacement.epoch == old.epoch + 1
    with pytest.raises(SessionLeaseLost):
        mailbox.commit(old)

    mailbox.commit(replacement)


def test_sqlite_session_mailbox_persists_generation_and_outbox_atomically():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "state.sqlite3"
        storage = SQLiteStorage(path)
        mailbox = storage.session_mailbox_v2
        accepted = mailbox.accept(
            "tenant", "session", "message-1", trace_id="trace-1"
        )
        assert accepted.queue_generation == 1
        storage.close()

        reopened = SQLiteStorage(path)
        restored = reopened.session_mailbox_v2.get("tenant", "session")
        assert restored is not None
        assert restored.accepted_sequence == 1
        assert restored.queue_generation == 1
        rows = reopened.inbox_outbox.list_outbox_by_tenant("tenant")
        assert len(rows) == 1
        assert rows[0].topic == "session.ready.v2"
        reopened.close()


def test_sqlite_session_mailbox_rejects_stale_epoch_after_takeover():
    with tempfile.TemporaryDirectory() as directory:
        storage = SQLiteStorage(Path(directory) / "state.sqlite3")
        mailbox = storage.session_mailbox_v2
        mailbox.accept("tenant", "session", "message-1")
        old = mailbox.claim("tenant", "session", "worker-a", 30)
        assert old is not None
        storage._conn.execute(
            """
            UPDATE session_mailbox SET lease_until=?
             WHERE tenant_id=? AND session_id=?
            """,
            (
                (old.expires_at - timedelta(seconds=60)).isoformat(),
                "tenant",
                "session",
            ),
        )
        storage._conn.commit()
        replacement = mailbox.claim("tenant", "session", "worker-b", 30)
        assert replacement is not None
        assert replacement.epoch == old.epoch + 1
        with pytest.raises(SessionLeaseLost):
            mailbox.commit(old)
        mailbox.commit(replacement)
        storage.close()


def test_mirrored_session_mailbox_v2_replicates_lease_lifecycle():
    primary = InMemoryStorage()
    secondary = InMemoryStorage()
    storage = MirroredStorageBundle(StorageBundle(primary), StorageBundle(secondary))
    try:
        accepted = storage.session_mailbox_v2.accept(
            "tenant",
            "session",
            "message-1",
            trace_id="trace-1",
        )
        primary_state = primary.session_mailbox_v2.get("tenant", "session")
        secondary_state = secondary.session_mailbox_v2.get("tenant", "session")
        assert primary_state is not None and secondary_state is not None
        assert primary_state.accepted_sequence == secondary_state.accepted_sequence
        assert primary_state.queue_generation == secondary_state.queue_generation
        assert primary_state.status == secondary_state.status
        lease = storage.session_mailbox_v2.claim("tenant", "session", "worker", 30)
        assert lease is not None
        storage.session_mailbox_v2.renew(lease, 30)
        storage.session_mailbox_v2.retry(lease, retry_at=None)
        retried = primary.session_mailbox_v2.get("tenant", "session")
        mirrored = secondary.session_mailbox_v2.get("tenant", "session")
        assert retried is not None and mirrored is not None
        assert retried.queue_generation == mirrored.queue_generation
        assert retried.retry_count == mirrored.retry_count
        assert accepted.accepted_sequence == 1
    finally:
        storage.close()


def test_poison_message_dead_letter_is_atomic_and_unblocks_next_message(monkeypatch):
    mailbox = InMemoryStorage().session_mailbox_v2
    mailbox.accept("tenant", "session", "message-1", trace_id="trace-1")
    mailbox.accept("tenant", "session", "message-2", trace_id="trace-2")
    lease = mailbox.claim("tenant", "session", "worker-a", 30)
    assert lease is not None

    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "1")
    adapter = _SessionMailboxV2Adapter(mailbox)
    state = adapter.fail(lease, "api_key=top-secret")

    assert state.resolved_sequence == 1
    assert state.status == SessionMailboxStatus.QUEUED
    dead = [item for item in mailbox.outbox if item.topic == "session.dead_letter.v2"]
    assert len(dead) == 1
    assert dead[0].payload["message_id"] == "message-1"
    assert "top-secret" not in dead[0].payload["error"]
    next_lease = mailbox.claim("tenant", "session", "worker-b", 30)
    assert next_lease is not None and next_lease.message_id == "message-2"


def test_sqlite_poison_message_fences_stale_owner_and_persists_dead_letter():
    with tempfile.TemporaryDirectory() as directory:
        storage = SQLiteStorage(Path(directory) / "state.sqlite3")
        mailbox = storage.session_mailbox_v2
        mailbox.accept("tenant", "session", "message-1", trace_id="trace-1")
        mailbox.accept("tenant", "session", "message-2", trace_id="trace-2")
        lease = mailbox.claim("tenant", "session", "worker-a", 30)
        assert lease is not None
        state = mailbox.dead_letter(lease, "permanent")
        assert state.resolved_sequence == 1
        with pytest.raises(SessionLeaseLost):
            mailbox.dead_letter(lease, "duplicate")
        rows = storage.inbox_outbox.list_outbox_by_tenant("tenant")
        assert [row.topic for row in rows].count("session.dead_letter.v2") == 1
        assert mailbox.claim("tenant", "session", "worker-b", 30).sequence == 2
        storage.close()


def test_session_mailbox_invariants_reject_impossible_persisted_states():
    validate_session_mailbox(SessionMailbox("tenant", "session"))
    with pytest.raises(ValueError, match="idle"):
        validate_session_mailbox(
            SessionMailbox("tenant", "session", accepted_sequence=1)
        )
    with pytest.raises(ValueError, match="complete lease"):
        validate_session_mailbox(
            SessionMailbox(
                "tenant",
                "session",
                status=SessionMailboxStatus.RUNNING,
                accepted_sequence=1,
                processing_sequence=1,
            )
        )


def test_sqlite_retry_scheduler_is_tenant_scoped():
    with tempfile.TemporaryDirectory() as directory:
        storage = SQLiteStorage(Path(directory) / "scheduler.sqlite3")
        try:
            mailbox = storage.session_mailbox_v2
            for tenant in ("tenant-a", "tenant-b"):
                mailbox.accept(tenant, "session", f"message-{tenant}")
                lease = mailbox.claim(tenant, "session", "worker", 30)
                mailbox.retry(lease, retry_at=now_utc() + timedelta(milliseconds=50))
            time.sleep(0.1)
            assert mailbox.schedule_retries(limit=10, tenant_id="tenant-a") == 1
            assert mailbox.get("tenant-a", "session").status == SessionMailboxStatus.QUEUED
            assert mailbox.get("tenant-b", "session").status == SessionMailboxStatus.RETRY_WAIT
            assert mailbox.claim("tenant-a", "session", "worker-a", 30) is not None
        finally:
            storage.close()


def test_in_memory_mailbox_covers_deferred_retry_recovery_and_reconciliation():
    mailbox = InMemoryStorage().session_mailbox_v2
    future = now_utc() + timedelta(minutes=5)
    waiting = mailbox.accept("tenant-a", "session-a", "message-1", retry_at=future)
    assert waiting.status == SessionMailboxStatus.RETRY_WAIT
    assert mailbox.claim("tenant-a", "session-a", "worker-a", 30) is None
    duplicate = mailbox.accept("tenant-a", "session-a", "message-1", priority=9)
    assert duplicate.accepted_sequence == 1

    current = mailbox.get("tenant-a", "session-a")
    assert current is not None
    current.retry_at = now_utc() - timedelta(seconds=1)
    mailbox._mailboxes[("tenant-a", "session-a")] = current
    mailbox._items[("tenant-a", "session-a", 1)].retry_at = current.retry_at
    assert mailbox.schedule_retries(tenant_id="tenant-a") == 1
    lease = mailbox.claim("tenant-a", "session-a", "worker-a", 30)
    assert lease is not None
    retried = mailbox.retry(lease, retry_at=None, increment_retry=False)
    assert retried.status == SessionMailboxStatus.QUEUED

    expired = mailbox.get("tenant-a", "session-a")
    assert expired is not None
    expired.lease_until = now_utc() - timedelta(seconds=1)
    expired.status = SessionMailboxStatus.RUNNING
    expired.processing_sequence = 1
    expired.processing_message_id = "message-1"
    expired.lease_owner = "worker-a"
    mailbox._mailboxes[("tenant-a", "session-a")] = expired
    recovered = mailbox.recover("tenant-a", "session-a")
    assert recovered is not None and recovered.status == SessionMailboxStatus.QUEUED
    assert mailbox.reconcile_sessions(tenant_id="tenant-a") == 1
    assert mailbox.sweep_expired_leases(limit=1, tenant_id="tenant-b") == 0

    with pytest.raises(ValueError):
        mailbox.schedule_retries(limit=0)
    with pytest.raises(ValueError):
        mailbox.reconcile_sessions(limit=0)


def test_sqlite_mailbox_export_restore_and_claim_state_edges():
    with tempfile.TemporaryDirectory() as directory:
        storage = SQLiteStorage(Path(directory) / "edges.sqlite3")
        try:
            mailbox = storage.session_mailbox_v2
            future = now_utc() + timedelta(minutes=5)
            assert mailbox.accept("tenant-a", "session-a", "message-1", retry_at=future).status == SessionMailboxStatus.RETRY_WAIT
            assert mailbox.accept("tenant-a", "session-a", "message-1").accepted_sequence == 1
            assert mailbox.claim("tenant-a", "session-a", "worker-a", 30) is None

            storage._conn.execute(
                "UPDATE session_mailbox SET retry_at=? WHERE tenant_id=? AND session_id=?",
                ((now_utc() - timedelta(seconds=1)).isoformat(), "tenant-a", "session-a"),
            )
            storage._conn.execute(
                "UPDATE session_mailbox_item SET retry_at=? WHERE tenant_id=? AND session_id=? AND sequence=1",
                ((now_utc() - timedelta(seconds=1)).isoformat(), "tenant-a", "session-a"),
            )
            storage._conn.commit()
            assert mailbox.schedule_retries(tenant_id="tenant-a") == 1
            claim = mailbox.claim_session("tenant-a", "session-a", "worker-a", 30)
            assert claim.claimed and claim.lease is not None
            renewed = mailbox.renew(claim.lease, 30)
            assert renewed.expires_at >= claim.lease.expires_at
            assert mailbox.commit(renewed).status == SessionMailboxStatus.IDLE

            stale = mailbox.claim_session("tenant-a", "session-a", "worker-a", 30, expected_generation=99)
            assert stale.status == SessionMailboxClaimStatus.STALE
            empty = mailbox.claim_session("tenant-a", "session-a", "worker-a", 30)
            assert empty.status == SessionMailboxClaimStatus.EMPTY

            exported = mailbox.export_by_tenant("tenant-a")
            assert exported["mailboxes"] and exported["items"]
            restored = SQLiteStorage(Path(directory) / "restored.sqlite3")
            try:
                restored.session_mailbox_v2.restore_export(exported)
                assert restored.session_mailbox_v2.get("tenant-a", "session-a").status == SessionMailboxStatus.IDLE
            finally:
                restored.close()
        finally:
            storage.close()
