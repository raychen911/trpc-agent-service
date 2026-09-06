from datetime import timedelta

import pytest

from trpc_service.gateway.session_ready_consumer import SessionReadyConsumer
from trpc_service.storage.base import now_utc
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import SessionLease, SessionLeaseHeartbeat

pytestmark = pytest.mark.unit


class OneShotTransport:
    def __init__(self, payload):
        self.payload = payload
        self.seen = False

    def consume_once(self, handler, block_ms=1000):
        del block_ms
        if self.seen:
            return False
        self.seen = True
        handler(self.payload)
        return True


def _prepare_storage():
    storage = InMemoryStorage()
    storage.inbox_outbox.accept_inbox(
        "tenant", "dedupe-1", "session", {
            "channel": "web",
            "account_id": "account",
            "external_message_id": "external-1",
            "external_user_id": "user-1",
            "text": "hello",
            "trace_id": "trace-1",
        }, "gateway-owner", message_id="message-1",
    )
    storage.session_mailbox_v2.accept("tenant", "session", "message-1", trace_id="trace-1")
    return storage


def test_session_ready_consumer_claims_executes_and_commits():
    storage = _prepare_storage()
    transport = OneShotTransport({
        "topic": "session.ready.v2",
        "tenant_id": "tenant",
        "aggregate_id": "session",
        "generation": 1,
    })
    calls = []

    def execute(claim):
        calls.append(claim.lease.message_id)
        return {"text": "done"}

    consumer = SessionReadyConsumer(
        storage.session_mailbox_v2,
        storage.inbox_outbox,
        transport,
        execute,
        owner="worker-1",
        lease_seconds=1,
    )
    assert consumer.consume_once()
    assert calls == ["message-1"]
    mailbox = storage.session_mailbox_v2.get("tenant", "session")
    assert mailbox is not None and mailbox.resolved_sequence == 1
    assert storage.inbox_outbox.list_inbox_by_tenant("tenant")[0].status == "completed"


def test_session_ready_consumer_retries_executor_failure():
    storage = _prepare_storage()
    transport = OneShotTransport({"tenant_id": "tenant", "aggregate_id": "session"})

    def execute(claim):
        del claim
        raise RuntimeError("provider down")

    consumer = SessionReadyConsumer(
        storage.session_mailbox_v2,
        storage.inbox_outbox,
        transport,
        execute,
        owner="worker-1",
        lease_seconds=1,
    )
    with pytest.raises(RuntimeError):
        consumer.consume_once()
    mailbox = storage.session_mailbox_v2.get("tenant", "session")
    assert mailbox is not None and mailbox.status == "retry_wait"
    assert storage.inbox_outbox.list_inbox_by_tenant("tenant")[0].status == "failed"


def test_session_ready_consumer_dead_letters_after_max_attempts(monkeypatch):
    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "1")
    storage = _prepare_storage()
    transport = OneShotTransport({"tenant_id": "tenant", "aggregate_id": "session"})

    def execute(claim):
        del claim
        raise RuntimeError("poison")

    consumer = SessionReadyConsumer(
        storage.session_mailbox_v2,
        storage.inbox_outbox,
        transport,
        execute,
        owner="worker-1",
        lease_seconds=1,
    )
    with pytest.raises(RuntimeError):
        consumer.consume_once()
    mailbox = storage.session_mailbox_v2.get("tenant", "session")
    assert mailbox is not None and mailbox.resolved_sequence == 1
    assert storage.inbox_outbox.list_inbox_by_tenant("tenant")[0].status == "dead"


def test_session_ready_retry_reclaims_failed_inbox_owner():
    storage = _prepare_storage()
    storage.inbox_outbox.fail_inbox("tenant", "dedupe-1", "gateway-owner", "transient")
    transport = OneShotTransport({"tenant_id": "tenant", "aggregate_id": "session"})
    consumer = SessionReadyConsumer(
        storage.session_mailbox_v2,
        storage.inbox_outbox,
        transport,
        lambda claim: {"text": "recovered"},
        owner="worker-1",
        lease_seconds=1,
    )
    assert consumer.consume_once()
    record = storage.inbox_outbox.get_inbox_by_message_id("tenant", "message-1")
    assert record is not None and record.status == "completed"


def test_session_lease_heartbeat_renews_until_turn_finishes(monkeypatch):
    monkeypatch.setenv("SESSION_LEASE_TTL_SECONDS", "1")
    lease = SessionLease("tenant", "session", "owner", 1, now_utc() + timedelta(seconds=1))
    calls = []

    class Store:
        def renew_session_lease(self, current):
            calls.append(current)
            return SessionLease(current.tenant_id, current.session_id, current.owner, current.fencing_token,
                                now_utc() + timedelta(seconds=1))

    with SessionLeaseHeartbeat(Store(), lease, interval=0.02):
        import time
        time.sleep(0.15)
    assert calls
