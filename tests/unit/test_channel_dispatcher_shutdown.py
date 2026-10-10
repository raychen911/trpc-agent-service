from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from trpc_service.channels.dispatcher import ChannelDispatcher
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus, OutboundEnvelope
from trpc_service.storage.models import BindingRoute, DeliveryAttempt, OutboxRecord
from trpc_service.storage.protocols import DeliveryInProgress
from trpc_service.tenant.models import Channel, ChannelBinding


def _record(outbox_id: str) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=outbox_id,
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id="aggregate-a",
        payload={},
        trace_headers={},
    )


class _Repository:
    def __init__(self, records: tuple[OutboxRecord, ...]) -> None:
        self.records = records
        self.released: list[tuple[str, str, dict[str, object]]] = []
        self.dead_letters: list[tuple[str, str]] = []

    async def claim_outbox(self, **kwargs: object) -> tuple[OutboxRecord, ...]:
        return self.records

    async def release_outbox(
        self,
        tenant_id: str,
        outbox_id: str,
        **kwargs: object,
    ) -> None:
        self.released.append((tenant_id, outbox_id, kwargs))

    async def dead_letter_outbox(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        reason: str,
    ) -> None:
        self.dead_letters.append((record.outbox_id, f"{owner_id}:{reason}"))


def _outbound_record(
    *,
    channel: Channel = Channel.FEISHU,
    attempts: int = 0,
) -> tuple[OutboxRecord, BindingRoute]:
    binding = ChannelBinding(
        binding_id="binding-a",
        tenant_id="tenant-a",
        app_id="app-a",
        channel=channel,
        account_id="account-a",
    )
    envelope = OutboundEnvelope(
        outbound_id="outbound-a",
        tenant_id="tenant-a",
        binding_id=binding.binding_id,
        channel=binding.channel,
        target_id="user-a",
        session_id="session-a",
        text="hello",
    )
    record = OutboxRecord(
        outbox_id="outbox-a",
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id=envelope.outbound_id,
        payload=envelope.model_dump(mode="json"),
        attempts=attempts,
    )
    return record, BindingRoute(binding=binding, active_config_version=1)


class _AtomicRepository(_Repository):
    def __init__(self, record: OutboxRecord, route: BindingRoute, begin_result: object) -> None:
        super().__init__((record,))
        self.route = route
        self.begin_result = begin_result
        self.begin_calls = 0
        self.finish_calls: list[dict[str, object]] = []

    async def resolve_binding(self, _binding_id: str) -> BindingRoute:
        return self.route

    async def begin_delivery(self, _record: OutboxRecord, *, owner_id: str) -> DeliveryAttempt:
        self.begin_calls += 1
        if isinstance(self.begin_result, BaseException):
            raise self.begin_result
        return self.begin_result  # type: ignore[return-value]

    async def finish_delivery(self, record: OutboxRecord, **kwargs: object) -> None:
        self.finish_calls.append({"record": record, **kwargs})


class _LegacyRepository(_Repository):
    def __init__(self, records: tuple[OutboxRecord, ...], route: BindingRoute) -> None:
        super().__init__(records)
        self.route = route
        self.published: list[str] = []
        self.receipts: list[DeliveryReceipt] = []

    async def resolve_binding(self, _binding_id: str) -> BindingRoute:
        return self.route

    async def record_delivery(
        self,
        _tenant_id: str,
        receipt: DeliveryReceipt,
        *,
        retrying: bool = False,
    ) -> None:
        assert not retrying
        self.receipts.append(receipt)

    async def mark_outbox_published(
        self,
        _tenant_id: str,
        outbox_id: str,
        *,
        owner_id: str,
    ) -> None:
        assert owner_id == "dispatcher-a"
        self.published.append(outbox_id)


class _Adapter:
    def __init__(
        self,
        receipt: DeliveryReceipt | None = None,
        error: Exception | None = None,
    ) -> None:
        self.receipt = receipt or DeliveryReceipt(
            outbound_id="outbound-a", status=DeliveryStatus.DELIVERED
        )
        self.error = error
        self.calls = 0

    async def send(self, _envelope: OutboundEnvelope, _binding: ChannelBinding) -> DeliveryReceipt:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.receipt


@pytest.mark.asyncio
async def test_dispatcher_releases_unprocessed_claims_when_stopping() -> None:
    repository = _Repository((_record("out-1"), _record("out-2")))
    dispatcher = ChannelDispatcher(
        repository,
        {},
        owner_id="dispatcher-a",
        event_type="outbound.feishu.ready",
    )
    stop_event = asyncio.Event()
    stop_event.set()

    assert await dispatcher.dispatch_once(stop_event) == 0
    assert [item[1] for item in repository.released] == ["out-1", "out-2"]
    assert all(item[2]["owner_id"] == "dispatcher-a" for item in repository.released)
    assert all(item[2]["delay"] == timedelta(0) for item in repository.released)
    assert all(item[2]["error_type"] == "dispatcher_draining" for item in repository.released)


@pytest.mark.asyncio
async def test_dispatcher_uses_atomic_delivery_attempt_and_finish() -> None:
    record, route = _outbound_record()
    attempt = DeliveryAttempt(
        tenant_id="tenant-a",
        outbound_id="outbound-a",
        attempt_number=2,
        owner_id="dispatcher-a",
    )
    repository = _AtomicRepository(record, route, attempt)
    adapter = _Adapter()
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: adapter},
        owner_id="dispatcher-a",
        event_type=record.event_type,
    )

    assert await dispatcher.dispatch_once() == 1
    assert adapter.calls == 1
    assert repository.begin_calls == 1
    assert repository.finish_calls[0]["attempt_number"] == 2
    assert repository.finish_calls[0]["receipt"].status == DeliveryStatus.DELIVERED


@pytest.mark.asyncio
async def test_wecom_standby_releases_outbox_before_attempt_then_delivers_when_ready() -> None:
    record, route = _outbound_record(channel=Channel.WECOM_AI_BOT)
    attempt = DeliveryAttempt(
        tenant_id=record.tenant_id,
        outbound_id=record.aggregate_id,
        attempt_number=1,
        owner_id="dispatcher-a",
    )
    repository = _AtomicRepository(record, route, attempt)
    adapter = _Adapter()
    ready = False

    def binding_ready(_binding: ChannelBinding) -> bool:
        return ready

    dispatcher = ChannelDispatcher(
        repository,
        {Channel.WECOM_AI_BOT: adapter},
        owner_id="dispatcher-a",
        event_type=record.event_type,
        binding_ready=binding_ready,
    )

    assert await dispatcher.dispatch_once() == 0
    assert repository.begin_calls == 0
    assert adapter.calls == 0
    assert repository.finish_calls == []
    assert repository.dead_letters == []
    assert repository.released == [
        (
            record.tenant_id,
            record.outbox_id,
            {
                "owner_id": "dispatcher-a",
                "delay": timedelta(milliseconds=100),
                "error_type": "adapter_standby",
            },
        )
    ]

    ready = True
    assert await dispatcher.dispatch_once() == 1
    assert repository.begin_calls == 1
    assert adapter.calls == 1
    assert repository.finish_calls[0]["receipt"].status == DeliveryStatus.DELIVERED
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_atomic_retry_budget_uses_delivery_attempt_not_outbox_claim_count() -> None:
    record, route = _outbound_record(attempts=99)
    attempt = DeliveryAttempt(
        tenant_id=record.tenant_id,
        outbound_id=record.aggregate_id,
        attempt_number=1,
        owner_id="dispatcher-a",
    )
    repository = _AtomicRepository(record, route, attempt)
    retryable = DeliveryReceipt(
        outbound_id=record.aggregate_id,
        status=DeliveryStatus.FAILED,
        provider_code="rate_limit",
        retryable=True,
    )
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: _Adapter(retryable)},
        owner_id="dispatcher-a",
        event_type=record.event_type,
        max_attempts=2,
        retry_jitter=lambda seconds: seconds,
    )

    assert await dispatcher.dispatch_once() == 0
    assert repository.begin_calls == 1
    assert repository.dead_letters == []
    assert repository.finish_calls[0]["attempt_number"] == 1
    assert repository.finish_calls[0]["receipt"].retryable is True
    assert repository.finish_calls[0]["retry_delay"] == timedelta(seconds=2)


@pytest.mark.asyncio
async def test_dispatcher_resolves_in_progress_attempt_as_ambiguous_without_send() -> None:
    record, route = _outbound_record()
    repository = _AtomicRepository(
        record,
        route,
        DeliveryInProgress("still sending", attempt_number=7),
    )
    adapter = _Adapter()
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: adapter},
        owner_id="dispatcher-a",
        event_type=record.event_type,
    )

    assert await dispatcher.dispatch_once() == 1
    assert adapter.calls == 0
    assert repository.finish_calls[0]["attempt_number"] == 7
    assert repository.finish_calls[0]["receipt"].status == DeliveryStatus.AMBIGUOUS


@pytest.mark.asyncio
async def test_dispatcher_turns_provider_exception_into_atomic_ambiguous_finish() -> None:
    record, route = _outbound_record()
    attempt = DeliveryAttempt(
        tenant_id="tenant-a",
        outbound_id="outbound-a",
        attempt_number=1,
        owner_id="dispatcher-a",
    )
    repository = _AtomicRepository(record, route, attempt)
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: _Adapter(error=ConnectionError("provider unavailable"))},
        owner_id="dispatcher-a",
        event_type=record.event_type,
    )

    assert await dispatcher.dispatch_once() == 1
    assert repository.finish_calls[0]["receipt"].status == DeliveryStatus.AMBIGUOUS
    assert repository.finish_calls[0]["receipt"].provider_code == "transport_unknown"


@pytest.mark.asyncio
async def test_dispatcher_quarantines_poison_payload_and_continues_batch() -> None:
    valid_record, route = _outbound_record()
    poison_record = _record("outbox-poison")
    repository = _LegacyRepository((poison_record, valid_record), route)
    adapter = _Adapter()
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: adapter},
        owner_id="dispatcher-a",
        event_type=valid_record.event_type,
    )

    assert await dispatcher.dispatch_once() == 2
    assert repository.dead_letters == [("outbox-poison", "dispatcher-a:invalid_outbound_payload")]
    assert repository.published == [valid_record.outbox_id]
    assert adapter.calls == 1


@pytest.mark.asyncio
async def test_dispatcher_run_recovers_from_cycle_failure(monkeypatch) -> None:
    repository = _Repository(())
    dispatcher = ChannelDispatcher(
        repository,
        {},
        owner_id="dispatcher-a",
        event_type="outbound.feishu.ready",
    )
    stop_event = asyncio.Event()
    calls = 0

    async def dispatch_once(_stop_event: asyncio.Event | None = None) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("database unavailable")
        stop_event.set()
        return 0

    monkeypatch.setattr(dispatcher, "dispatch_once", dispatch_once)
    await asyncio.wait_for(
        dispatcher.run(poll_seconds=0.001, stop_event=stop_event),
        timeout=1.0,
    )
    assert calls == 2
