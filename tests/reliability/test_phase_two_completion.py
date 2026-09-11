"""Regression evidence for second-phase completion work."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from tests.storage.test_artifact_knowledge import FakeMinio
from trpc_service.persistence import Database
from trpc_service.persistence.models import InboundMessageRow, OutboxMessageRow, ArtifactMetadataRow
from trpc_service.reliability import InboxRepository, OutboxRepository
from trpc_service.reliability.outbox import OutboxDispatcher, DeliveryRateLimited, PermanentDeliveryError
from trpc_service.storage import S3ArtifactService


@pytest.fixture
def database(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'completion.db'}")
    db.create_schema()
    yield db
    db.close()


def test_artifact_versions_are_unique_across_concurrent_services(database):
    client = FakeMinio()
    artifact_id = ArtifactId(app_name="tenant_acme:support", user_id="usr", session_id="ses", filename="same.txt")

    def upload(index):
        service = S3ArtifactService(tenant_id="tenant_acme", bucket="artifacts", client=client, database=database)
        return asyncio.run(service.save_artifact(artifact_id=artifact_id, artifact=Part.from_text(text=str(index))))

    with ThreadPoolExecutor(max_workers=8) as executor:
        versions = list(executor.map(upload, range(16)))
    assert sorted(versions) == list(range(16))
    assert len({data[0] for data in client.objects.values()}) == 16


@pytest.mark.asyncio
async def test_unregistered_artifact_is_never_overwritten(database):
    from io import BytesIO
    client = FakeMinio()
    service = S3ArtifactService(tenant_id="tenant_acme", bucket="artifacts", client=client, database=database)
    artifact_id = ArtifactId(app_name="tenant_acme:support", user_id="usr", session_id="ses", filename="same.txt")
    key = service._scope(artifact_id) + "/versions/0"
    client.put_object("artifacts", key, BytesIO(b"existing"), 8)
    with pytest.raises(ValueError, match="unregistered"):
        await service.save_artifact(artifact_id=artifact_id, artifact=Part.from_text(text="new"))
    assert client.objects[("artifacts", key)][0] == b"existing"


@pytest.mark.asyncio
async def test_artifact_publish_gap_is_invisible_and_recoverable(database, monkeypatch):
    client = FakeMinio()
    service = S3ArtifactService(tenant_id="tenant_acme", bucket="artifacts", client=client, database=database)
    artifact_id = ArtifactId(app_name="tenant_acme:support", user_id="usr", session_id="ses", filename="same.txt")
    original = service._publish

    def fail(*args):
        raise ConnectionError("confirmation unavailable")

    monkeypatch.setattr(service, "_publish", fail)
    with pytest.raises(ConnectionError):
        await service.save_artifact(artifact_id=artifact_id, artifact=Part.from_text(text="saved"))
    assert await service.load_artifact(artifact_id=artifact_id) is None
    with database.sessions() as session:
        assert session.scalar(select(ArtifactMetadataRow.status)) == "reserved"
    monkeypatch.setattr(service, "_publish", original)
    await service.recover_upload(artifact_id=artifact_id, version=0)
    assert (await service.load_artifact(artifact_id=artifact_id)).data.text == "saved"


def test_later_session_reply_waits_for_earlier_message(database):
    inbox, outbox = InboxRepository(database), OutboxRepository(database)
    ids = []
    for index in range(2):
        receipt = inbox.accept(tenant_id="tenant_acme", channel_binding_id="binding_bot",
                               external_message_id=str(index), payload={"text": str(index)},
                               request_id=str(index), trace_id="trace", partition_key="tenant_acme:ses",
                               config_version=1, storage_revision=1)
        ids.append(receipt.inbound_message_id)
    outbox.enqueue("tenant_acme", ids[1], 0, {"text": "second"})
    assert outbox.claim(limit=10, worker_id="sender", lease_seconds=30) == []
    outbox.enqueue("tenant_acme", ids[0], 0, {"text": "first"})
    with database.sessions.begin() as session:
        for identity in ids:
            session.get(InboundMessageRow, identity).status = "succeeded"
    first, = outbox.claim(limit=10, worker_id="sender", lease_seconds=30)
    assert first.inbound_message_id == ids[0]
    outbox.mark_delivered(first.outbox_message_id, "sender", "confirmed")
    second, = outbox.claim(limit=10, worker_id="sender", lease_seconds=30)
    assert second.inbound_message_id == ids[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("error,status", [(DeliveryRateLimited(120), "retry"),
                                          (PermanentDeliveryError(), "dead_letter")])
async def test_delivery_error_classification(database, error, status):
    outbox = OutboxRepository(database)
    identity = outbox.enqueue("tenant_acme", "message", 0, {"text": "reply"})

    async def send(item):
        raise error

    await OutboxDispatcher(outbox, send, worker_id="sender").run_once()
    with database.sessions() as session:
        row = session.get(OutboxMessageRow, identity)
        assert row.status == status
        if status == "retry":
            assert (row.next_retry_at - row.created_at).total_seconds() >= 120


def test_finalization_rolls_back_all_platform_completion_writes(database):
    from types import SimpleNamespace
    from sqlalchemy import event
    from trpc_service.agent import AgentChannelEvent
    from trpc_service.persistence.models import AuditLogRow, PostTurnTaskRow, SessionEventRow
    from trpc_service.reliability.completion import ExecutionCompletion

    inbox = InboxRepository(database)
    receipt = inbox.accept(tenant_id="tenant_acme", channel_binding_id="binding_bot",
                           external_message_id="atomic", payload={}, request_id="req", trace_id="trace",
                           partition_key="tenant_acme:ses", config_version=1, storage_revision=1)
    work = inbox.claim(worker_id="worker", lease_seconds=60)
    route = SimpleNamespace(tenant_id="tenant_acme", session_id="ses", internal_user_id="scp_storage",
                            actor_id="usr_actor", config_version=1)
    with database.sessions.begin() as session:
        session.add(SessionEventRow(tenant_id="tenant_acme", app_id="support", user_id="usr",
                                    session_id="ses", event_id="event", seq_no=1,
                                    execution_id=receipt.execution_id, event_type="channel_event", content_json={}))
    outputs = [AgentChannelEvent(type="text", event_id="event", text="reply", final=True)]

    def fail_audit(*args):
        raise ConnectionError("audit unavailable")

    completion = ExecutionCompletion(database)
    event.listen(AuditLogRow, "before_insert", fail_audit)
    try:
        with pytest.raises(ConnectionError):
            completion.finish(work, "worker", outputs, route, "succeeded", None)
    finally:
        event.remove(AuditLogRow, "before_insert", fail_audit)
    with database.sessions() as session:
        assert session.get(InboundMessageRow, receipt.inbound_message_id).status == "processing"
        assert session.scalar(select(OutboxMessageRow)) is None
        assert session.scalar(select(PostTurnTaskRow)) is None
    completion.finish(work, "worker", outputs, route, "succeeded", None)
    with database.sessions() as session:
        assert session.get(InboundMessageRow, receipt.inbound_message_id).status == "succeeded"
        audit = session.scalar(select(AuditLogRow).where(AuditLogRow.decision == "execution_succeeded"))
        assert audit.user_id == "usr_actor" and audit.trace_id == "trace" and audit.request_id == "req"
        assert len(list(session.scalars(select(PostTurnTaskRow)))) == 2
        assert session.scalar(select(AuditLogRow)).decision == "execution_succeeded"
