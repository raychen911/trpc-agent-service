"""Crash, uncertain send and multipart retry scenarios using the durable IM inbox."""

from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import timedelta

import pytest
from sqlalchemy import select

from tests.reliability.test_im_ingress import system as im_system, make_event, grant
from trpc_service.channels.delivery import IMDeliveryWorker
from trpc_service.channels.events import DeliveryResult
from trpc_service.persistence.models import IMDeliveryAttemptRow, OutboxMessageRow, utcnow
from trpc_service.persistence.models import IMMembershipRow
from trpc_service.reliability import OutboxRepository

system = im_system


def queue_reply(system, text="reply"):
    db, binding, router, cipher, _, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    receipt = ingress.record(event, binding, lease, route=route)
    outbox = OutboxRepository(db)
    outbox_id = outbox.enqueue(binding.tenant_id, receipt.inbound_id, 0, {"final": True, "text": text})
    assert outbox.claim(limit=1, worker_id="simulation", lease_seconds=30) == []
    adapter = SimpleNamespace(send_text=AsyncMock(
        return_value=DeliveryResult(outcome="accepted", receipt_kind="message_id", external_message_id="telegram-7")))
    return IMDeliveryWorker(db, lease, adapter, cipher), outbox_id, adapter


@pytest.mark.asyncio
async def test_uncertain_network_is_not_retried(system):
    worker, outbox_id, adapter = queue_reply(system)
    adapter.send_text.side_effect = TimeoutError("lost response after remote accepted")
    assert await worker.run_once()
    assert not await worker.run_once()
    assert adapter.send_text.await_count == 1
    with worker.database.sessions() as session:
        assert session.get(OutboxMessageRow, outbox_id).status == "delivery_unknown"


@pytest.mark.asyncio
async def test_crash_after_network_intent_never_repeats_send(system):
    worker, outbox_id, adapter = queue_reply(system)
    work = worker.prepare()
    assert work["attempt_id"]
    # Simulate a killed process before a success/failure record reaches SQL.
    replacement = IMDeliveryWorker(worker.database, worker.lease, adapter, worker.cipher)
    assert await replacement.run_once()
    assert not await replacement.run_once()
    adapter.send_text.assert_not_awaited()
    with worker.database.sessions() as session:
        assert session.get(OutboxMessageRow, outbox_id).status == "delivery_unknown"
        assert session.get(IMDeliveryAttemptRow, work["attempt_id"]).outcome == "unknown"


@pytest.mark.asyncio
async def test_multipart_retries_only_explicitly_rejected_chunk(system):
    worker, outbox_id, adapter = queue_reply(system, "a" * 5000)
    adapter.send_text.side_effect = [
        DeliveryResult(outcome="accepted", receipt_kind="message_id", external_message_id="first"),
        DeliveryResult(outcome="rejected", error_type="rate_limited", retryable=True, retry_after=10),
        DeliveryResult(outcome="accepted", receipt_kind="message_id", external_message_id="second"),
    ]
    assert await worker.run_once()
    assert await worker.run_once()
    assert not await worker.run_once()
    with worker.database.sessions.begin() as session:
        session.get(OutboxMessageRow, outbox_id).next_retry_at = utcnow() - timedelta(seconds=1)
    assert await worker.run_once()
    assert not await worker.run_once()
    assert [len(call.args[1]) for call in adapter.send_text.await_args_list] == [4096, 904, 904]
    with worker.database.sessions() as session:
        assert session.get(OutboxMessageRow, outbox_id).status == "delivered"
        assert len(list(session.scalars(select(IMDeliveryAttemptRow)))) == 3


@pytest.mark.asyncio
async def test_revocation_before_delivery_prevents_network_write(system):
    worker, outbox_id, adapter = queue_reply(system)
    with worker.database.sessions.begin() as session:
        member = session.scalar(select(IMMembershipRow))
        member.active = False
        member.authz_epoch += 1
    assert await worker.run_once()
    adapter.send_text.assert_not_awaited()
    with worker.database.sessions() as session:
        assert session.get(OutboxMessageRow, outbox_id).error_type == "authorization_revoked"
