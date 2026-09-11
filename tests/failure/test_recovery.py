"""Crash-window recovery for leased durable work."""

import pytest

from trpc_service.persistence import Database
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.storage import ConflictError


@pytest.fixture
def database(tmp_path):
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'recovery.db'}")
    database.create_schema()
    yield database
    database.close()


def test_outbox_dispatch_is_reclaimed_after_worker_crash(database: Database) -> None:
    outbox = OutboxRepository(database)
    message_id = outbox.enqueue("tenant_acme", "inbound-1", 0, {"text": "reply"})
    first = outbox.claim(limit=1, worker_id="worker-a", lease_seconds=-1)[0]
    recovered = outbox.claim(limit=1, worker_id="worker-b", lease_seconds=30)[0]
    assert recovered.outbox_message_id == first.outbox_message_id == message_id
    assert recovered.attempt == 2
    with pytest.raises(RuntimeError, match="owned"):
        outbox.mark_delivered(message_id, "worker-a", "delivery-stale")
    outbox.mark_delivered(message_id, "worker-b", "delivery-ok")
    assert outbox.claim(limit=1, worker_id="worker-c", lease_seconds=30) == []


def test_post_turn_task_is_reclaimed_after_worker_crash(database: Database) -> None:
    tasks = PostTurnRepository(database)
    task_id = tasks.enqueue("tenant_acme", "session-1", "summary", 5)
    first = tasks.claim(limit=1, worker_id="worker-a", lease_seconds=-1)[0]
    recovered = tasks.claim(limit=1, worker_id="worker-b", lease_seconds=30)[0]
    assert recovered.task_id == first.task_id == task_id
    assert recovered.attempt == 2
    tasks.complete(task_id, "worker-b")


@pytest.mark.asyncio
async def test_expired_session_lease_cannot_commit(database: Database) -> None:
    coordinator = SessionCoordinator(database)
    lease = await coordinator.acquire(
        "tenant_acme",
        "support_app",
        "user-1",
        "session-1",
        "worker-a",
        -1,
    )
    with pytest.raises(ConflictError, match="fencing"):
        await coordinator.commit(lease, expected_revision=0, event_id="late", event_payload={"late": True})
