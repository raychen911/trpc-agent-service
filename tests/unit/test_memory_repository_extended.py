from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import binding, envelope, repository
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import BindingRoute, OutboxRecord, TurnCommit
from trpc_service.storage.protocols import DeliveryInProgress, FencingConflict
from trpc_service.tenant.models import Channel


@pytest.mark.asyncio
async def test_memory_repository_reads_routing_and_duplicate_acceptance() -> None:
    repo = repository()
    assert await repo.resolve_binding("missing") is None
    assert len(await repo.list_bindings(Channel.FEISHU)) == 1
    repo.add_route(
        BindingRoute(
            binding=binding(binding_id="disabled").model_copy(update={"enabled": False}),
            active_config_version=1,
        )
    )
    repo.add_route(
        BindingRoute(
            binding=binding(binding_id="inactive"),
            tenant_active=False,
            active_config_version=1,
        )
    )
    assert len(await repo.list_bindings(Channel.FEISHU)) == 1
    with pytest.raises(LookupError, match="configuration"):
        await repo.get_config("tenant-a", "support", 99)

    runtime = TenantRuntime(repo, routing_key=b"m" * 32)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())
    duplicate = await runtime.accept("binding-unpredictable-a", envelope())
    assert duplicate.duplicate and duplicate.inbound_id == accepted.inbound_id
    persisted = await repo.get_acceptance("tenant-a", accepted.inbound_id)
    assert persisted is not None and not persisted.duplicate
    assert await repo.get_acceptance("other", accepted.inbound_id) is None
    assert await repo.get_acceptance("tenant-a", "missing") is None
    assert await repo.get_session_snapshot("tenant-a", "missing") is None


@pytest.mark.asyncio
async def test_memory_lease_order_retry_renew_fail_and_empty_commit() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"m" * 32)
    first = await runtime.accept("binding-unpredictable-a", envelope("first"))
    second = await runtime.accept("binding-unpredictable-a", envelope("second"))
    duration = timedelta(seconds=30)
    assert await repo.acquire(acceptance=second, worker_id="second", lease_for=duration) is None
    lease = await repo.acquire(acceptance=first, worker_id="owner", lease_for=duration)
    assert lease is not None
    assert await repo.acquire(acceptance=first, worker_id="owner", lease_for=duration) is None
    assert await repo.acquire(acceptance=first, worker_id="other", lease_for=duration) is None
    renewed = await repo.renew(lease, lease_for=duration)
    assert renewed.expires_at > lease.expires_at

    stale = lease.model_copy(update={"fencing_token": lease.fencing_token + 1})
    with pytest.raises(FencingConflict, match="lease"):
        await repo.renew(stale, lease_for=duration)
    await repo.fail(stale, error_type="ignored")
    assert repo._leases
    await repo.fail(renewed, error_type="retry")
    assert not repo._leases

    retry = await repo.acquire(acceptance=first, worker_id="owner", lease_for=duration)
    assert retry is not None
    repo._leases[(retry.tenant_id, retry.session_id)] = retry.model_copy(
        update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    retried = await repo.acquire(acceptance=first, worker_id="owner", lease_for=duration)
    assert retried is not None and retried.attempt == 2
    result = await repo.commit(
        TurnCommit(context=first.context, lease=retried, state={"done": True}, events=())
    )
    assert result.first_sequence is None and result.outbound_id is None
    assert await repo.acquire(acceptance=first, worker_id="owner", lease_for=duration) is None
    redelivery = await repo.get_acceptance("tenant-a", first.inbound_id)
    assert redelivery is not None and redelivery.duplicate
    worker = AgentWorker(repo, worker_id="owner", agent_loader=lambda _config: None)
    assert (await worker.process(redelivery)).status == ProcessStatus.DUPLICATE
    assert repo.snapshot("tenant-a", first.context.session_id).state == {"done": True}

    second_lease = await repo.acquire(acceptance=second, worker_id="second", lease_for=duration)
    assert second_lease is not None
    repo._leases[(second_lease.tenant_id, second_lease.session_id)] = second_lease.model_copy(
        update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    with pytest.raises(FencingConflict, match="stale"):
        await repo.commit(
            TurnCommit(context=second.context, lease=second_lease, state={}, events=())
        )


@pytest.mark.asyncio
async def test_memory_outbox_claim_fencing_release_and_delivery() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"o" * 32)
    await runtime.accept("binding-unpredictable-a", envelope("one"))
    await runtime.accept("binding-unpredictable-a", envelope("two", user_id="other"))
    claimed = await repo.claim_outbox(
        event_type="inbound.accepted",
        owner_id="owner",
        limit=1,
        lease_for=timedelta(seconds=30),
    )
    assert len(claimed) == 1 and claimed[0].attempts == 1
    other = await repo.claim_outbox(
        event_type="inbound.accepted",
        owner_id="other",
        limit=10,
        lease_for=timedelta(seconds=30),
    )
    assert len(other) == 1
    assert not await repo.claim_outbox(
        event_type="unknown",
        owner_id="owner",
        limit=10,
        lease_for=timedelta(seconds=30),
    )
    with pytest.raises(FencingConflict, match="claim"):
        await repo.mark_outbox_published(
            claimed[0].tenant_id, claimed[0].outbox_id, owner_id="wrong"
        )
    with pytest.raises(FencingConflict, match="claim"):
        await repo.release_outbox(
            claimed[0].tenant_id,
            claimed[0].outbox_id,
            owner_id="wrong",
            delay=timedelta(),
            error_type="ignored",
        )
    await repo.release_outbox(
        claimed[0].tenant_id,
        claimed[0].outbox_id,
        owner_id="owner",
        delay=timedelta(),
        error_type="retry",
    )
    repo._outbox_claims[claimed[0].outbox_id] = (
        "owner",
        datetime.now(UTC) + timedelta(seconds=1),
    )
    await repo.mark_outbox_published(claimed[0].tenant_id, claimed[0].outbox_id, owner_id="owner")
    receipt = DeliveryReceipt(outbound_id="out", status=DeliveryStatus.DELIVERED)
    await repo.record_delivery("tenant-a", receipt)
    assert repo.delivery_receipts == [receipt]


@pytest.mark.asyncio
async def test_memory_delivery_attempt_is_tenant_scoped_and_exposes_unresolved_attempt() -> None:
    repo = repository()
    now = datetime.now(UTC) + timedelta(minutes=1)
    first = OutboxRecord(
        outbox_id="outbox-a",
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id="same-outbound-id",
        payload={},
    )
    second = first.model_copy(update={"outbox_id": "outbox-b", "tenant_id": "tenant-b"})
    repo._outbox[first.outbox_id] = first
    repo._outbox[second.outbox_id] = second
    repo._outbox_claims[first.outbox_id] = ("worker-a", now)
    repo._outbox_claims[second.outbox_id] = ("worker-b", now)

    attempt_a = await repo.begin_delivery(first, owner_id="worker-a")
    attempt_b = await repo.begin_delivery(second, owner_id="worker-b")
    assert attempt_a.attempt_number == attempt_b.attempt_number == 1
    with pytest.raises(DeliveryInProgress) as caught:
        await repo.begin_delivery(first, owner_id="worker-a")
    assert caught.value.attempt_number == 1

    with pytest.raises(ValueError, match="does not match"):
        await repo.finish_delivery(
            first,
            owner_id="worker-a",
            attempt_number=attempt_a.attempt_number,
            receipt=DeliveryReceipt(
                outbound_id="different-outbound-id", status=DeliveryStatus.DELIVERED
            ),
        )


@pytest.mark.asyncio
async def test_memory_delivery_takeover_allows_new_owner_to_close_unresolved_attempt() -> None:
    repo = repository()
    record = OutboxRecord(
        outbox_id="outbox-takeover",
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id="outbound-takeover",
        payload={},
    )
    repo._outbox[record.outbox_id] = record
    lease_expiry = datetime.now(UTC) + timedelta(minutes=1)
    repo._outbox_claims[record.outbox_id] = ("worker-a", lease_expiry)

    attempt = await repo.begin_delivery(record, owner_id="worker-a")
    # Simulate the expired outbox lease being reclaimed by worker-b before it
    # tries to start the provider request.
    repo._outbox_claims[record.outbox_id] = ("worker-b", lease_expiry)
    with pytest.raises(DeliveryInProgress) as caught:
        await repo.begin_delivery(record, owner_id="worker-b")
    assert caught.value.attempt_number == attempt.attempt_number

    # The provider request is still unresolved, so the dispatcher closes the
    # existing attempt as ambiguous instead of sending a duplicate request.
    repo._outbox_claims[record.outbox_id] = ("worker-b", lease_expiry)
    await repo.finish_delivery(
        record,
        owner_id="worker-b",
        attempt_number=attempt.attempt_number,
        receipt=DeliveryReceipt(
            outbound_id=record.aggregate_id,
            status=DeliveryStatus.AMBIGUOUS,
            provider_code="delivery_in_progress",
        ),
    )
    assert repo.dead_letters == [(record, DeliveryStatus.AMBIGUOUS.value)]
    assert record.outbox_id not in repo._outbox

    # The previous owner cannot finish after the claim has moved on.
    with pytest.raises(FencingConflict, match="delivery attempt"):
        await repo.finish_delivery(
            record,
            owner_id="worker-a",
            attempt_number=attempt.attempt_number,
            receipt=DeliveryReceipt(
                outbound_id=record.aggregate_id,
                status=DeliveryStatus.DELIVERED,
            ),
        )
