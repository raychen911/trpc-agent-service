"""Receipt, transactional Outbox, checkpoint, and recovery tests."""

from __future__ import annotations

import asyncio
import importlib
from datetime import timedelta

import pytest
from sqlalchemy import func
from sqlalchemy import select

from trpc_service.agent import StreamWorker
from trpc_service.agent import TaskMessage
from trpc_service.channels import InboundMessage
from trpc_service.channels import SendResult
from trpc_service.channels import ChannelDeliveryTransport
from trpc_service.messaging import ClaimStatus
from trpc_service.messaging import InMemoryMessageStore
from trpc_service.messaging import OutboxRelay
from trpc_service.messaging import SqlMessageStore
from trpc_service.messaging._repository import delivery_attempt_table
from trpc_service.messaging._repository import delivery_outbox_table
from trpc_service.messaging._models import utcnow
from trpc_service.messaging import OutboxMessage
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager

relay_module = importlib.import_module("trpc_service.messaging._relay")


def _task(message_id: str = "message-1", text: str = "hello", revision: int = 2) -> TaskMessage:
    inbound = InboundMessage(
        channel="qq",
        chat_id="chat",
        sender_id="user",
        message_id=message_id,
        text=text,
    )
    return TaskMessage.from_inbound("tenant-a", "qq", inbound, config_revision=revision)


async def test_in_memory_receipt_lease_fencing_completion_and_part_checkpoints():
    store = InMemoryMessageStore()
    task = _task(text="你" * 2000)
    inbound = task.to_inbound()

    first = await store.claim_inbound(task, owner="worker-a", lease_seconds=30)
    busy = await store.claim_inbound(task, owner="worker-b", lease_seconds=30)
    assert first.status == ClaimStatus.ACQUIRED
    assert first.fencing_token == 1
    assert busy.status == ClaimStatus.BUSY
    assert await store.renew_inbound(
        task,
        owner="worker-a",
        fencing_token=first.fencing_token,
        lease_seconds=60,
    ) is True
    assert await store.renew_inbound(
        task,
        owner="worker-b",
        fencing_token=first.fencing_token,
        lease_seconds=60,
    ) is False

    message = await store.complete_with_outbox(
        task,
        inbound,
        "你" * 2000,
        owner="worker-a",
        fencing_token=first.fencing_token,
    )
    assert message is not None
    assert len(message.parts) == 2
    assert (await store.claim_inbound(task, owner="worker-b", lease_seconds=30)).status == ClaimStatus.COMPLETED

    leased = (await store.claim_outbox(owner="relay-a", limit=10, lease_seconds=30))[0]
    await store.checkpoint_part(leased.event_id, owner="relay-a", part_index=0, provider_message_id="part-0")
    resumed = (await store.claim_outbox(owner="relay-b", limit=10, lease_seconds=30))[0]
    assert resumed.next_part == 1
    await store.checkpoint_part(resumed.event_id, owner="relay-b", part_index=1)
    assert store.get_outbox(resumed.event_id).status == "delivered"


async def test_in_memory_receipt_abandon_recovery_ownership_and_empty_reply():
    store = InMemoryMessageStore()
    task = _task()
    claim = await store.claim_inbound(task, owner="worker", lease_seconds=0)
    await store.abandon_inbound(task, owner="worker", fencing_token=claim.fencing_token, error="token=secret")
    recovered = await store.claim_inbound(task, owner="other", lease_seconds=1)
    assert recovered.fencing_token == claim.fencing_token + 1
    assert await store.complete_with_outbox(
        task,
        task.to_inbound(),
        "",
        owner="other",
        fencing_token=recovered.fencing_token,
    ) is None
    try:
        await store.abandon_inbound(task, owner="other", fencing_token=recovered.fencing_token, error="late")
    except RuntimeError as exc:
        assert "ownership" in str(exc)
    else:
        raise AssertionError("a completed receipt cannot be abandoned")


class RecordingTransport:

    def __init__(self, fail: bool = False, raises: bool = False):
        self.fail = fail
        self.raises = raises
        self.parts = []

    async def send_part(self, message, part, part_index):
        self.parts.append((message.event_id, part_index, part))
        if self.raises:
            raise TimeoutError("provider timeout")
        if self.fail:
            return SendResult(ok=False, error="provider rejected")
        return SendResult(ok=True, message_id=f"provider-{part_index}")


async def test_outbox_relay_success_dead_letter_and_manual_replay():
    store = InMemoryMessageStore()
    task = _task(text="x" * 2000)
    claim = await store.claim_inbound(task, owner="worker", lease_seconds=10)
    message = await store.complete_with_outbox(
        task,
        task.to_inbound(),
        "x" * 2000,
        owner="worker",
        fencing_token=claim.fencing_token,
    )
    transport = RecordingTransport()
    relay = OutboxRelay(store=store, transport=transport, owner="relay")
    assert await relay.run_once() == 1
    assert await relay.run_once() == 1
    assert [item[1] for item in transport.parts] == [0, 1]
    assert store.get_outbox(message.event_id).status == "delivered"

    failed_task = _task("failed")
    failed_claim = await store.claim_inbound(failed_task, owner="worker", lease_seconds=10)
    failed = await store.complete_with_outbox(
        failed_task,
        failed_task.to_inbound(),
        "reply",
        owner="worker",
        fencing_token=failed_claim.fencing_token,
    )
    failing_relay = OutboxRelay(
        store=store,
        transport=RecordingTransport(raises=True),
        owner="failed-relay",
        max_attempts=1,
    )
    assert await failing_relay.run_once() == 1
    assert store.get_outbox(failed.event_id).status == "dead_letter"
    assert await store.replay_dead_letters(failed.event_id) == 1
    assert await store.replay_dead_letters("missing") == 0


async def test_outbox_relay_recovers_from_transient_poll_failure(monkeypatch):

    class FlakyStore:

        def __init__(self):
            self.calls = 0

        async def claim_outbox(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("database unavailable")
            return []

    sleeps = []

    async def stop_after_retry(interval):
        sleeps.append(interval)
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    store = FlakyStore()
    relay = OutboxRelay(store=store, transport=RecordingTransport(), owner="relay")
    monkeypatch.setattr(relay_module.asyncio, "sleep", stop_after_retry)

    with pytest.raises(asyncio.CancelledError):
        await relay.run(0.25)

    assert store.calls == 2
    assert sleeps == [0.25, 0.25]


async def test_outbox_relay_closes_owned_resources_once():
    calls = []

    class ClosingStore:

        async def close(self):
            calls.append("store")

    class ClosingTransport:

        async def close(self):
            calls.append("transport")

    class Owned:

        def close(self):
            calls.append("owned")

    relay = OutboxRelay(
        store=ClosingStore(),
        transport=ClosingTransport(),
        owner="relay",
        owned_resources=[Owned()],
    )
    await relay.close()
    await relay.close()
    assert calls == ["store", "transport", "owned"]


async def test_sql_store_commits_receipt_outbox_attempt_and_replay(tmp_path):
    store = SqlMessageStore(f"sqlite:///{tmp_path / 'messages.db'}")
    task = _task(text="durable")
    claim = await store.claim_inbound(task, owner="worker-a", lease_seconds=30)
    assert (await store.claim_inbound(task, owner="worker-b", lease_seconds=30)).status == ClaimStatus.BUSY
    message = await store.complete_with_outbox(
        task,
        task.to_inbound(),
        "durable reply",
        owner="worker-a",
        fencing_token=claim.fencing_token,
    )
    assert message.config_revision == 2
    leased = (await store.claim_outbox(owner="relay", limit=1, lease_seconds=30))[0]
    await store.fail_delivery(
        leased.event_id,
        owner="relay",
        part_index=0,
        error="Bearer should-not-leak",
        max_attempts=1,
    )
    assert await store.replay_dead_letters(leased.event_id) == 1
    leased = (await store.claim_outbox(owner="relay-2", limit=1, lease_seconds=30))[0]
    await store.checkpoint_part(
        leased.event_id,
        owner="relay-2",
        part_index=0,
        provider_message_id="provider-id",
    )
    with store._engine.connect() as connection:
        outbox = connection.execute(select(delivery_outbox_table)).mappings().one()
        attempts = connection.execute(select(func.count()).select_from(delivery_attempt_table)).scalar_one()
    assert outbox["status"] == "delivered"
    assert attempts == 2
    assert "should-not-leak" not in (outbox["last_error"] or "")
    assert await store.replay_dead_letters() == 0
    await store.close()


async def test_sql_store_recovers_expired_outbox_lease_and_rejects_wrong_checkpoint(tmp_path):
    store = SqlMessageStore(f"sqlite:///{tmp_path / 'recovery.db'}")
    task = _task()
    claim = await store.claim_inbound(task, owner="worker", lease_seconds=10)
    assert await store.renew_inbound(
        task,
        owner="worker",
        fencing_token=claim.fencing_token,
        lease_seconds=30,
    ) is True
    assert await store.renew_inbound(
        task,
        owner="wrong-worker",
        fencing_token=claim.fencing_token,
        lease_seconds=30,
    ) is False
    message = await store.complete_with_outbox(
        task,
        task.to_inbound(),
        "reply",
        owner="worker",
        fencing_token=claim.fencing_token,
    )
    await store.claim_outbox(owner="lost-relay", limit=1, lease_seconds=30)
    with store._engine.begin() as connection:
        connection.execute(delivery_outbox_table.update().where(
            delivery_outbox_table.c.event_id == message.event_id).values(available_at=utcnow() - timedelta(seconds=1)))
    recovered = (await store.claim_outbox(owner="new-relay", limit=1, lease_seconds=30))[0]
    try:
        await store.checkpoint_part(recovered.event_id, owner="wrong", part_index=0)
    except RuntimeError as exc:
        assert "ownership" in str(exc)
    else:
        raise AssertionError("wrong relay must not checkpoint")
    await store.close()


class FakeQueue:

    consumer_name = "worker-durable"

    def __init__(self):
        self.acked = []

    async def ack(self, message_id):
        self.acked.append(message_id)

    async def delivery_count(self, message_id):
        return 1


class FakeTurnWorker:

    def __init__(self):
        self.calls = []

    async def handle(self, tenant_id, channel, inbound):
        self.calls.append((tenant_id, channel, dict(inbound.metadata)))
        return "durable reply"


async def test_stream_worker_durable_mode_acks_after_outbox_and_deduplicates_turn():
    queue = FakeQueue()
    turn_worker = FakeTurnWorker()
    store = InMemoryMessageStore()
    stream_worker = StreamWorker(
        queue=queue,
        worker=turn_worker,
        registry=object(),
        message_store=store,
    )
    task = _task()

    assert await stream_worker._process("redis-1", task.model_dump_json()) is True
    assert await stream_worker._process("redis-2", task.model_dump_json()) is True
    assert queue.acked == ["redis-1", "redis-2"]
    assert len(turn_worker.calls) == 1
    assert turn_worker.calls[0][2]["config_revision"] == 2
    assert turn_worker.calls[0][2]["turn_id"] == task.turn_id


async def test_stream_worker_stops_turn_when_receipt_renewal_loses_fencing():

    class OwnedQueue(FakeQueue):

        async def touch(self, _message_id):
            return True

    class LostReceiptStore:

        async def renew_inbound(self, *_args, **_kwargs):
            return False

    stream_worker = StreamWorker(
        queue=OwnedQueue(),
        worker=FakeTurnWorker(),
        registry=object(),
        message_store=LostReceiptStore(),
        heartbeat_interval_ms=1,
    )
    owner = asyncio.create_task(asyncio.sleep(10))
    ownership_lost = asyncio.Event()

    await stream_worker._heartbeat(
        "redis-1",
        owner,
        ownership_lost,
        task=_task(),
        receipt_owner="worker",
        receipt_token=1,
        receipt_lease_seconds=30,
    )

    assert ownership_lost.is_set()
    with pytest.raises(asyncio.CancelledError):
        await owner


async def test_channel_delivery_transport_uses_recorded_revision_and_part_metadata():
    manager = TenantConfigManager()
    first = Tenant(
        tenant_id="tenant-a",
        name="A",
        model=ModelEndpoint(model_name="v1"),
        channel_configs={"qq": QQChannelConfig(app_id="old-app", secret="secret")},
    )
    manager.register(first)
    current = first.model_copy(deep=True)
    current.model.model_name = "v2"
    current.channel_configs["qq"].app_id = "new-app"
    manager.update(current)

    class Adapter:
        inbound = None

        async def reply_text(self, inbound, text):
            self.inbound = inbound
            assert text == "part"
            return SendResult(ok=True, message_id="provider")

    adapter = Adapter()

    class Registry:

        def get(self, tenant, channel):
            assert channel == "qq"
            assert tenant.model.model_name == "v1"
            assert tenant.channel_configs["qq"].app_id == "old-app"
            return adapter

    message = OutboxMessage(
        event_id="event",
        tenant_id="tenant-a",
        channel="qq",
        message_id="message",
        turn_id="turn",
        config_revision=1,
        inbound=_task().inbound,
        parts=["part"],
    )
    result = await ChannelDeliveryTransport(manager=manager, registry=Registry()).send_part(message, "part", 0)
    assert result.message_id == "provider"
    assert adapter.inbound.metadata["outbox_event_id"] == "event"
    assert adapter.inbound.metadata["outbox_part_index"] == 0
