from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from trpc_service.channels.base import InboundMessage
from trpc_service.gateway.router import AgentGateway, GatewayError
from trpc_service.gateway.session_id import build_idempotency_key, session_id_for_message
from trpc_service.storage.base import now_utc
from trpc_service.storage.compensation import (
    InMemoryCompensationStore,
    replay_compensations,
)
from trpc_service.storage.durable import (
    DurableOutboxDispatcher,
    InboxRecord,
    InboxStatus,
    InMemoryInboxOutbox,
    OutboxStatus,
)
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.retry import retry_delay_seconds
from trpc_service.storage.sql_store import SQLiteStorage
from trpc_service.tenant.models import default_demo_config
from trpc_service.tenant.repository import InMemoryTenantRepository
from trpc_service.tenant.service import TenantService


def test_bounded_retry_delay_is_stable_exponential_and_capped(monkeypatch):
    monkeypatch.setenv("TEST_RETRY_BASE", "2")
    monkeypatch.setenv("TEST_RETRY_CAP", "5")
    first = retry_delay_seconds(
        1,
        identity="record",
        base_env="TEST_RETRY_BASE",
        cap_env="TEST_RETRY_CAP",
    )
    second = retry_delay_seconds(
        2,
        identity="record",
        base_env="TEST_RETRY_BASE",
        cap_env="TEST_RETRY_CAP",
    )
    capped = retry_delay_seconds(
        20,
        identity="record",
        base_env="TEST_RETRY_BASE",
        cap_env="TEST_RETRY_CAP",
    )
    assert first == retry_delay_seconds(
        1,
        identity="record",
        base_env="TEST_RETRY_BASE",
        cap_env="TEST_RETRY_CAP",
    )
    assert first < second <= 5
    assert capped <= 5


def test_outbox_dead_letter_can_only_be_replayed_explicitly(monkeypatch):
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("DURABLE_OUTBOX_RETRY_BASE_SECONDS", "2")
    monkeypatch.setenv("DURABLE_OUTBOX_RETRY_MAX_SECONDS", "2")
    store = InMemoryInboxOutbox()
    store.enqueue_outbox("tenant", "topic", "aggregate", {}, event_id="event-1")
    before = now_utc()

    dispatcher = DurableOutboxDispatcher(
        store,
        lambda record: (_ for _ in ()).throw(RuntimeError("token=top-secret")),
        owner="worker",
        tenant_id="tenant",
    )
    assert dispatcher.run_once() == 0
    dead = store.list_outbox_by_tenant("tenant")[0]
    assert dead.status == "dead"
    assert dead.available_at > before + timedelta(seconds=1)
    assert "top-secret" not in (dead.last_error or "")

    replayed = store.replay_outbox("event-1", "tenant")
    assert replayed.status == "pending"
    assert replayed.attempts == 0
    assert replayed.last_error is None
    with pytest.raises(ValueError, match="only dead"):
        store.replay_outbox("event-1", "tenant")


def test_dead_inbox_is_terminal_for_duplicate_delivery():
    store = InMemoryInboxOutbox()
    first, claimed = store.accept_inbox(
        "tenant", "dedupe", "session", {"text": "one"}, "worker-a"
    )
    assert claimed
    store.dead_inbox("tenant", "dedupe", "worker-a", "token=top-secret")

    duplicate, duplicate_claimed = store.accept_inbox(
        "tenant", "dedupe", "session", {"text": "one"}, "worker-b"
    )
    assert not duplicate_claimed
    assert duplicate.message_id == first.message_id
    assert duplicate.status == "dead"
    assert "top-secret" not in str(duplicate.result)


def test_gateway_poison_message_cannot_reenter_after_terminal_inbox(monkeypatch):
    repository = InMemoryTenantRepository()
    repository.create(default_demo_config())
    storage = InMemoryStorage()
    failing_worker = SimpleNamespace(run=Mock(side_effect=RuntimeError("permanent")))
    gateway = AgentGateway(
        TenantService(repository), storage, workers=[failing_worker]
    )
    message = InboundMessage(
        channel="telegram",
        account_id="corp_account_1",
        external_message_id="poison-message",
        external_user_id="user",
        text="hello",
    )
    monkeypatch.setenv("DURABLE_INBOX_OUTBOX", "1")
    monkeypatch.setenv("TRPC_AGENT_RUNTIME_MODE", "local")
    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "1")

    with pytest.raises(RuntimeError, match="permanent"):
        gateway.dispatch(message)
    record = next(iter(storage.inbox_outbox._inbox.values()))
    assert record.status == "dead"
    with pytest.raises(GatewayError, match="dead-lettered"):
        gateway.dispatch(message)
    assert failing_worker.run.call_count == 1


def test_gateway_repairs_crash_gap_between_mailbox_and_inbox_terminal_state(monkeypatch):
    config = default_demo_config()
    repository = InMemoryTenantRepository()
    repository.create(config)
    storage = InMemoryStorage()
    failing_worker = SimpleNamespace(run=Mock())
    gateway = AgentGateway(
        TenantService(repository), storage, workers=[failing_worker]
    )
    message = InboundMessage(
        channel="telegram",
        account_id="corp_account_1",
        external_message_id="crash-gap-message",
        external_user_id="user",
        text="hello",
    )
    binding = config.channel_binding(message.channel, message.account_id)
    session_id = session_id_for_message(config.tenant_id, binding.agent_app_id, message)
    dedupe_key = build_idempotency_key(
        config.tenant_id,
        message.channel,
        message.account_id,
        message.external_message_id,
    )
    inbox, _ = storage.inbox_outbox.accept_inbox(
        config.tenant_id,
        dedupe_key,
        session_id,
        {},
        "crashed-worker",
    )
    storage.session_mailbox_v2.accept(
        config.tenant_id, session_id, inbox.message_id
    )
    lease = storage.session_mailbox_v2.claim(
        config.tenant_id, session_id, "crashed-worker", 30
    )
    storage.session_mailbox_v2.dead_letter(lease, "permanent")
    storage.inbox_outbox.fail_inbox(
        config.tenant_id, dedupe_key, "crashed-worker", "crash-gap"
    )
    monkeypatch.setenv("DURABLE_INBOX_OUTBOX", "1")
    monkeypatch.setenv("TRPC_AGENT_RUNTIME_MODE", "local")

    with pytest.raises(GatewayError, match="already terminal"):
        gateway.dispatch(message)
    repaired = storage.inbox_outbox._inbox[(config.tenant_id, dedupe_key)]
    assert repaired.status == "dead"
    failing_worker.run.assert_not_called()


def test_gateway_async_handoff_reserves_idempotency_record(monkeypatch):
    repository = InMemoryTenantRepository()
    config = default_demo_config()
    repository.create(config)
    storage = InMemoryStorage()
    gateway = AgentGateway(
        TenantService(repository),
        storage,
        workers=[SimpleNamespace(run=Mock())],
    )
    monkeypatch.setenv("DURABLE_INBOX_OUTBOX", "1")
    monkeypatch.setenv("SESSION_READY_ASYNC", "1")
    monkeypatch.setenv("TRPC_AGENT_RUNTIME_MODE", "local")
    message = InboundMessage(
        channel="telegram",
        account_id="corp_account_1",
        external_message_id="async-idempotency",
        external_user_id="user",
        text="hello",
    )

    session_id, events, _ = gateway.dispatch(message)
    assert session_id
    assert events[0].metadata["queued"] is True
    key = build_idempotency_key(
        config.tenant_id,
        message.channel,
        message.account_id,
        message.external_message_id,
    )
    record = storage.idempotency.get(config.tenant_id, key)
    assert record is not None
    assert record.status.value == "processing"


def test_compensation_failure_uses_backoff_and_dead_task_requires_replay(monkeypatch):
    monkeypatch.setenv("COMPENSATION_RETRY_BASE_SECONDS", "2")
    monkeypatch.setenv("COMPENSATION_RETRY_MAX_SECONDS", "2")
    store = InMemoryCompensationStore(max_attempts=1)
    task = store.enqueue("tenant", "unsupported.operation", {}, task_id="task-1")
    storage = SimpleNamespace(compensation=store)
    before = now_utc()

    assert replay_compensations(storage, tenant_id="tenant") == 0
    dead = store._tasks[task.task_id]
    assert dead.status == "dead"
    assert dead.available_at > before + timedelta(seconds=1)

    replayed = store.replay(task.task_id, tenant_id="tenant")
    assert replayed.status == "pending"
    assert replayed.attempt == 0
    assert replayed.last_error is None
    with pytest.raises(ValueError, match="only dead"):
        store.replay(task.task_id, tenant_id="tenant")


def test_sqlite_outbox_and_compensation_replay_are_tenant_scoped(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("COMPENSATION_MAX_ATTEMPTS", "1")
    storage = SQLiteStorage(tmp_path / "replay.sqlite3")
    try:
        storage.inbox_outbox.enqueue_outbox(
            "tenant", "topic", "aggregate", {}, event_id="event-1"
        )
        storage.inbox_outbox.claim_outbox("owner", tenant_id="tenant")
        storage.inbox_outbox.fail_outbox(
            "event-1", "owner", "permanent", retry_after_seconds=0, tenant_id="tenant"
        )
        with pytest.raises(ValueError, match="missing or is not dead"):
            storage.inbox_outbox.replay_outbox("event-1", "other-tenant")
        assert storage.inbox_outbox.replay_outbox("event-1", "tenant").status == "pending"

        storage.compensation.enqueue(
            "tenant", "audit.append", {}, task_id="task-1"
        )
        storage.compensation.claim(tenant_id="tenant")
        storage.compensation.fail(
            "task-1", "permanent", retry_after_seconds=0, tenant_id="tenant"
        )
        with pytest.raises(ValueError, match="missing or is not dead"):
            storage.compensation.replay("task-1", tenant_id="other-tenant")
        assert storage.compensation.replay("task-1", tenant_id="tenant").status == "pending"
    finally:
        storage.close()


def test_inmemory_durable_store_reclaims_leases_and_preserves_terminal_records(monkeypatch):
    store = InMemoryInboxOutbox()
    first, created = store.accept_inbox("tenant-a", "dedupe", "session", {"x": 1}, "worker-a")
    assert created
    current, claimed = store.accept_inbox("tenant-a", "dedupe", "session", {"x": 2}, "worker-b")
    assert not claimed and current.message_id == first.message_id

    first.lease_until = now_utc() - timedelta(seconds=1)
    store._inbox[("tenant-a", "dedupe")].lease_until = first.lease_until
    reclaimed, claimed = store.accept_inbox("tenant-a", "dedupe", "session", {"x": 3}, "worker-b")
    assert claimed and reclaimed.attempts == 2 and reclaimed.owner == "worker-b"
    with pytest.raises(RuntimeError, match="ownership lost"):
        store.complete_inbox("tenant-a", "dedupe", "worker-a", {})
    store.fail_inbox("tenant-a", "dedupe", "worker-b", "token=secret")
    assert "token=secret" not in str(store.list_inbox_by_tenant("tenant-a")[0].result)
    _, claimed = store.accept_inbox("tenant-a", "dedupe", "session", {}, "worker-c")
    assert claimed
    store.dead_inbox("tenant-a", "dedupe", "worker-c", "permanent")

    duplicate, claimed = store.accept_inbox("tenant-a", "dedupe", "session", {}, "worker-c")
    assert not claimed and duplicate.status == InboxStatus.DEAD
    restored = store.restore_inbox(InboxRecord("other", "tenant-a", "dedupe", "session", {"new": True}))
    assert restored.status == InboxStatus.DEAD

    first_outbox = store.enqueue_outbox("tenant-a", "topic", "session", {"x": 1}, event_id="event-1")
    assert store.restore_outbox(first_outbox).event_id == "event-1"
    claimed_outbox = store.claim_outbox("worker-a", tenant_id="tenant-a")[0]
    with pytest.raises(RuntimeError, match="ownership lost"):
        store.complete_outbox(claimed_outbox.event_id, "worker-b", tenant_id="tenant-a")
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "3")
    store.fail_outbox(claimed_outbox.event_id, "worker-a", "token=secret", retry_after_seconds=0, tenant_id="tenant-a")
    assert store.list_outbox_by_tenant("tenant-a")[0].status == OutboxStatus.PENDING
    reclaimed_outbox = store.claim_outbox("worker-b", tenant_id="tenant-a")[0]
    store.fail_outbox(reclaimed_outbox.event_id, "worker-b", "permanent", retry_after_seconds=0, tenant_id="tenant-a")
    assert store.list_outbox_by_tenant("tenant-a")[0].status == OutboxStatus.PENDING


def test_durable_dispatcher_completes_success_and_records_retry(monkeypatch):
    store = InMemoryInboxOutbox()
    store.enqueue_outbox("tenant-a", "topic", "session", {}, event_id="ok")
    store.enqueue_outbox("tenant-a", "topic", "session", {}, event_id="bad")
    seen = []

    def handler(record):
        seen.append(record.event_id)
        if record.event_id == "bad":
            raise RuntimeError("api_key=secret")

    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "5")
    dispatcher = DurableOutboxDispatcher(store, handler, owner="dispatcher", tenant_id="tenant-a")
    assert dispatcher.run_once(limit=10, lease_seconds=30) == 1
    assert seen == ["ok", "bad"]
    statuses = {record.event_id: record.status for record in store.list_outbox_by_tenant("tenant-a")}
    assert statuses["ok"] == OutboxStatus.COMPLETED
    assert statuses["bad"] == OutboxStatus.PENDING
    assert "api_key=secret" not in (store.list_outbox_by_tenant("tenant-a")[1].last_error or "")
    assert dispatcher.run_once(limit=0) == 0
