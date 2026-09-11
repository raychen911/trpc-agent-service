"""Service recovery contracts, independent of the model provider."""

import asyncio
from datetime import datetime, timezone
from datetime import timedelta

import pytest
from sqlalchemy import select

from tests.reliability.test_pipeline import FakeRunner
from trpc_service.agent import RunnerRegistry
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.persistence import Database
from trpc_service.persistence.models import InboundMessageRow, PostTurnTaskRow, OutboxMessageRow, SessionLeaseRow
from trpc_service.reliability import (
    DurableExecutionPipeline, InboxRepository, OutboxRepository, PostTurnRepository, SessionCoordinator,
)
from trpc_service.tenant import ResolvedRoute


@pytest.fixture
def service(tmp_path):
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'worker.db'}")
    db.create_schema()
    inbox, outbox, post = InboxRepository(db), OutboxRepository(db), PostTurnRepository(db)
    coordinator = SessionCoordinator(db)
    pipeline = DurableExecutionPipeline(inbox, outbox, post, coordinator)
    route = ResolvedRoute(tenant_id="tenant_acme", config_version=3, storage_revision=4,
                          agent_app_id="support_app", channel_binding_id="binding_primary",
                          actor_id="actor_1", internal_user_id="usr_1", session_id="ses_1",
                          partition_key="tenant_acme:ses_1")
    message = NormalizedInboundMessage(
        channel="wecom", webhook_public_id="callback_acme", external_message_id="message-1",
        external_user_id="user-1", conversation_type="direct", text="hello",
        received_at=datetime.now(timezone.utc), request_id="request-1")
    registry, runner = RunnerRegistry(), FakeRunner()
    registry.register("tenant_acme", "support_app", 3, runner)
    yield db, inbox, outbox, post, pipeline, route, message, registry, runner
    db.close()


def test_retransmission_keeps_original_configuration(service):
    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    first = pipeline.ingest(message, route, trace_id="first")
    duplicate = pipeline.ingest(
        message.model_copy(update={"request_id": "new-request", "received_at": datetime.now(timezone.utc)}),
        route.model_copy(update={"config_version": 9, "storage_revision": 12}), trace_id="new-trace")
    assert first.execution_id == duplicate.execution_id
    work = inbox.claim(worker_id="worker", lease_seconds=30)
    assert (work.config_version, work.storage_revision, work.trace_id) == (3, 4, "first")


@pytest.mark.asyncio
async def test_final_output_recovery_repairs_missing_derived_tasks(service):
    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    receipt = pipeline.ingest(message, route, trace_id="trace")
    work = inbox.claim(worker_id="worker", lease_seconds=30)
    await pipeline.execute(work, registry, worker_id="worker")
    with db.sessions.begin() as session:
        for task in session.scalars(select(PostTurnTaskRow)):
            session.delete(task)
        row = session.get(InboundMessageRow, receipt.inbound_message_id)
        row.status, row.lease_owner = "retry", None
    recovered = inbox.claim(worker_id="recovered", lease_seconds=30)
    await pipeline.execute(recovered, registry, worker_id="recovered")
    assert runner.calls == 1
    assert len(post.claim(limit=10, worker_id="post", lease_seconds=30)) == 2


@pytest.mark.asyncio
async def test_worker_consumes_and_stops_claiming_on_drain(service):
    from trpc_service.worker import AgentWorker, WorkerSettings

    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    pipeline.ingest(message, route, trace_id="trace")
    stop = asyncio.Event()
    worker = AgentWorker(inbox, pipeline, registry, worker_id="worker",
                         settings=WorkerSettings(poll_seconds=0.01))
    task = asyncio.create_task(worker.run(stop))
    for _ in range(100):
        with db.sessions() as session:
            if session.scalar(select(InboundMessageRow.status)) == "succeeded":
                break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("worker did not consume the inbox")
    stop.set()
    await asyncio.wait_for(task, 2)
    pipeline.ingest(message.model_copy(update={"external_message_id": "after-stop"}), route, trace_id="trace")
    assert runner.calls == 1
    assert inbox.claim(worker_id="next", lease_seconds=30) is not None


def test_session_order_and_expired_owner_cannot_complete(service):
    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    pipeline.ingest(message, route, trace_id="first")
    pipeline.ingest(message.model_copy(update={"external_message_id": "second"}), route, trace_id="second")
    work = inbox.claim(worker_id="first", lease_seconds=30)
    assert inbox.claim(worker_id="competing", lease_seconds=30) is None
    with db.sessions.begin() as session:
        session.get(InboundMessageRow, work.inbound_message_id).lease_expires_at = (
            datetime.now(timezone.utc) - timedelta(seconds=1))
    with pytest.raises(RuntimeError, match="expired"):
        inbox.mark_succeeded(work.inbound_message_id, "first")
    recovered = inbox.claim(worker_id="replacement", lease_seconds=30)
    assert recovered.execution_id == work.execution_id
    with pytest.raises(RuntimeError):
        inbox.renew(work, "first", 30)


@pytest.mark.asyncio
async def test_long_run_renews_both_leases(service):
    from trpc_service.worker import AgentWorker, WorkerSettings

    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    original = runner.run_async
    allow_finish, renewed = asyncio.Event(), asyncio.Event()
    original_renew = pipeline._coordinator.renew
    renewals = 0

    async def observe_renewal(*args):
        nonlocal renewals
        await original_renew(*args)
        renewals += 1
        if renewals == 3:
            renewed.set()

    async def slow(**kwargs):
        await allow_finish.wait()
        async for event in original(**kwargs):
            yield event

    runner.run_async = slow
    pipeline._coordinator.renew = observe_renewal
    receipt = pipeline.ingest(message, route, trace_id="trace")
    worker = AgentWorker(inbox, pipeline, registry,
                         settings=WorkerSettings(lease_seconds=2, run_timeout_seconds=10))
    task = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(renewed.wait(), 8)
    with db.sessions() as session:
        inbound = session.get(InboundMessageRow, receipt.inbound_message_id)
        lease = session.scalar(select(SessionLeaseRow))
        now = datetime.now(timezone.utc)
        assert inbound.lease_expires_at.replace(tzinfo=timezone.utc) > now
        assert lease.expires_at.replace(tzinfo=timezone.utc) > now
    allow_finish.set()
    await task
    assert runner.calls == 1


@pytest.mark.asyncio
async def test_outbox_ack_loss_reuses_key_and_preserves_part_order(service):
    from trpc_service.reliability.outbox import OutboxDispatcher

    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    first = outbox.enqueue("tenant_acme", "inbound", 0, {"text": "one"})
    second = outbox.enqueue("tenant_acme", "inbound", 1, {"text": "two"})
    received, attempts = {}, []

    async def sender(item):
        attempts.append(item.outbox_message_id)
        received.setdefault(item.outbox_message_id, item.payload)
        if len(attempts) == 1:
            raise TimeoutError("receiver committed; acknowledgement lost")
        return item.outbox_message_id

    dispatcher = OutboxDispatcher(outbox, sender, worker_id="sender")
    await dispatcher.run_once()
    assert not await dispatcher.run_once()  # Part two cannot overtake retrying part one.
    with db.sessions.begin() as session:
        session.get(OutboxMessageRow, first).next_retry_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await dispatcher.run_once()
    await dispatcher.run_once()
    assert attempts == [first, first, second]
    assert len(received) == 2 and runner.calls == 0


@pytest.mark.asyncio
async def test_outbox_exhausted_delivery_is_visible_dead_letter(service):
    from trpc_service.reliability.outbox import OutboxDispatcher

    db, inbox, outbox, post, pipeline, route, message, registry, runner = service
    message_id = outbox.enqueue("tenant_acme", "inbound", 0, {"text": "reply"})

    async def unavailable(item):
        raise TimeoutError()

    await OutboxDispatcher(outbox, unavailable, worker_id="sender", max_attempts=1).run_once()
    with db.sessions() as session:
        assert session.get(OutboxMessageRow, message_id).status == "dead_letter"


@pytest.mark.asyncio
async def test_hung_operation_obeys_total_retry_deadline():
    from trpc_service.reliability import retry_async, RetryPolicy

    async def never_returns():
        await asyncio.Event().wait()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(retry_async(never_returns, RetryPolicy(max_elapsed_seconds=0.02)), 0.2)


@pytest.mark.asyncio
async def test_session_renewal_failure_does_not_cancel_consumer(service):
    from trpc_service.worker import AgentWorker, WorkerSettings
    from trpc_service.storage import ConflictError

    db, inbox, outbox, post, pipeline, route, message, registry, runner = service

    async def slow(**kwargs):
        await asyncio.Event().wait()
        yield  # pragma: no cover

    async def lost(*args):
        raise ConflictError("lease revoked")

    runner.run_async = slow
    pipeline._coordinator.renew = lost
    receipt = pipeline.ingest(message, route, trace_id="trace")
    worker = AgentWorker(inbox, pipeline, registry, settings=WorkerSettings(lease_seconds=0.3))
    assert await asyncio.wait_for(worker.run_once(), 2)
    with db.sessions() as session:
        assert session.get(InboundMessageRow, receipt.inbound_message_id).status == "retry"
