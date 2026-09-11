"""Durable inbox, outbox, lease, CAS, and post-turn contracts."""

import asyncio

import pytest

from trpc_service.persistence import Database
from trpc_service.reliability import DuplicatePayloadError
from trpc_service.reliability import InboxDisposition
from trpc_service.reliability import InboxRepository
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.storage import ConflictError


@pytest.fixture
def database(tmp_path):
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'phase2.db'}")
    database.create_schema()
    yield database
    database.close()


def test_schema_contains_all_phase_two_core_tables(database: Database) -> None:
    required = {
        "tenants",
        "tenant_config_versions",
        "backend_profiles",
        "tenant_backend_bindings",
        "agent_apps",
        "channel_bindings",
        "sessions",
        "session_events",
        "session_summaries",
        "memories",
        "artifact_metadata",
        "knowledge_bases",
        "knowledge_documents",
        "knowledge_index_versions",
        "audit_logs",
        "inbound_messages",
        "execution_attempts",
        "outbox_messages",
        "tool_invocations",
        "post_turn_tasks",
        "migration_jobs",
        "migration_checkpoints",
        "session_leases",
    }
    assert required <= database.table_names()


def test_inbox_deduplicates_and_rejects_same_id_with_new_payload(database: Database) -> None:
    inbox = InboxRepository(database)
    first = inbox.accept(
        tenant_id="tenant_acme",
        channel_binding_id="binding_primary",
        external_message_id="msg-1",
        payload={"text": "hello"},
        request_id="req-1",
        trace_id="trace-1",
        partition_key="tenant_acme:ses_1",
        config_version=3,
        storage_revision=4,
    )
    duplicate = inbox.accept(
        tenant_id="tenant_acme",
        channel_binding_id="binding_primary",
        external_message_id="msg-1",
        payload={"text": "hello"},
        request_id="req-2",
        trace_id="trace-2",
        partition_key="tenant_acme:ses_1",
        config_version=3,
        storage_revision=4,
    )
    assert first.disposition is InboxDisposition.ACCEPTED
    assert duplicate.disposition is InboxDisposition.DUPLICATE
    assert duplicate.execution_id == first.execution_id

    with pytest.raises(DuplicatePayloadError):
        inbox.accept(
            tenant_id="tenant_acme",
            channel_binding_id="binding_primary",
            external_message_id="msg-1",
            payload={"text": "changed"},
            request_id="req-3",
            trace_id="trace-3",
            partition_key="tenant_acme:ses_1",
            config_version=3,
            storage_revision=4,
        )


def test_expired_inbox_claim_is_recovered_with_same_execution(database: Database) -> None:
    inbox = InboxRepository(database)
    receipt = inbox.accept(
        tenant_id="tenant_acme",
        channel_binding_id="binding_primary",
        external_message_id="msg-recover",
        payload={"text": "recover"},
        request_id="req-recover",
        trace_id="trace-recover",
        partition_key="tenant_acme:ses_recover",
        config_version=3,
        storage_revision=4,
    )
    first = inbox.claim(worker_id="worker-a", lease_seconds=-1)
    recovered = inbox.claim(worker_id="worker-b", lease_seconds=30)
    assert first.execution_id == receipt.execution_id
    assert recovered.execution_id == receipt.execution_id
    assert recovered.attempt == 2


def test_outbox_and_post_turn_enqueue_are_idempotent(database: Database) -> None:
    outbox = OutboxRepository(database)
    first = outbox.enqueue("tenant_acme", "inbound-1", 0, {"text": "ok"})
    second = outbox.enqueue("tenant_acme", "inbound-1", 0, {"text": "ok"})
    assert first == second
    assert len(outbox.claim(limit=10, worker_id="dispatcher-a", lease_seconds=30)) == 1

    tasks = PostTurnRepository(database)
    task_a = tasks.enqueue("tenant_acme", "ses-1", "summary", 7)
    task_b = tasks.enqueue("tenant_acme", "ses-1", "summary", 7)
    assert task_a == task_b
    claimed = tasks.claim(limit=1, worker_id="post-a", lease_seconds=30)
    assert claimed[0].task_id == task_a


@pytest.mark.asyncio
async def test_fencing_and_revision_prevent_stale_or_overwriting_writes(database: Database) -> None:
    coordinator = SessionCoordinator(database)
    first = await coordinator.acquire("tenant_acme", "support_app", "user-1", "ses-1", "worker-a", 0.01)
    await asyncio.sleep(0.02)
    second = await coordinator.acquire("tenant_acme", "support_app", "user-1", "ses-1", "worker-b", 30)
    assert second.fencing_token > first.fencing_token

    with pytest.raises(ConflictError, match="fencing"):
        await coordinator.commit(first, expected_revision=0, event_id="event-old", event_payload={"text": "old"})

    revision = await coordinator.commit(
        second,
        expected_revision=0,
        event_id="event-new",
        event_payload={"text": "new"},
        state_delta={"answer": 1},
    )
    assert revision == 1
    assert await coordinator.commit(
        second,
        expected_revision=0,
        event_id="event-new",
        event_payload={"text": "new"},
        state_delta={"answer": 1},
    ) == 1
    with pytest.raises(ConflictError, match="revision"):
        await coordinator.commit(
            second,
            expected_revision=0,
            event_id="event-other",
            event_payload={"text": "other"},
        )

    snapshot = coordinator.snapshot("tenant_acme", "support_app", "user-1", "ses-1")
    assert snapshot.revision == 1
    assert snapshot.state == {"answer": 1}
    assert [event.seq_no for event in snapshot.events] == [1]
