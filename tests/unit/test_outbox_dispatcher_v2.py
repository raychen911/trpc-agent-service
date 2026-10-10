from __future__ import annotations

from datetime import timedelta

import pytest

from trpc_service.queue.dispatcher import OutboxDispatcher
from trpc_service.storage.models import OutboxRecord


def make_record(event_type: str, *, outbox_id: str, attempts: int = 0) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=outbox_id,
        tenant_id="tenant-a",
        event_type=event_type,
        aggregate_id="aggregate-a",
        payload={"value": outbox_id},
        trace_headers={"traceparent": "00-abc-def-01"},
        attempts=attempts,
    )


class RecordingPublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.published: list[OutboxRecord] = []
        self.ensure_group_calls = 0

    async def ensure_group(self) -> None:
        self.ensure_group_calls += 1

    async def publish(self, record: OutboxRecord) -> str:
        if self.error is not None:
            raise self.error
        self.published.append(record)
        return "stream-entry-1"


class RecordingRepository:
    def __init__(self, *records: OutboxRecord) -> None:
        self.records_by_type = {record.event_type: record for record in records}
        self.claimed_event_types: list[str] = []
        self.marked: list[tuple[str, str, str]] = []
        self.released: list[tuple[str, str, str, timedelta, str]] = []

    async def claim_outbox(
        self,
        *,
        event_type: str,
        owner_id: str,
        limit: int,
        lease_for: timedelta,
    ) -> tuple[OutboxRecord, ...]:
        del owner_id, limit, lease_for
        self.claimed_event_types.append(event_type)
        record = self.records_by_type.get(event_type)
        return () if record is None else (record,)

    async def mark_outbox_published(self, tenant_id: str, outbox_id: str, *, owner_id: str) -> None:
        self.marked.append((tenant_id, outbox_id, owner_id))

    async def release_outbox(
        self,
        tenant_id: str,
        outbox_id: str,
        *,
        owner_id: str,
        delay: timedelta,
        error_type: str,
    ) -> None:
        self.released.append((tenant_id, outbox_id, owner_id, delay, error_type))


@pytest.mark.asyncio
async def test_v2_dispatch_claims_only_v2_and_publishes_outbox_record() -> None:
    v1 = make_record("inbound.accepted", outbox_id="v1")
    v2 = make_record("session.ready.v2", outbox_id="v2")
    repository = RecordingRepository(v1, v2)
    publisher = RecordingPublisher()

    dispatcher = OutboxDispatcher(
        repository,
        publisher,
        owner_id="dispatcher-a",
        event_type="session.ready.v2",
    )

    assert await dispatcher.dispatch_once() == 1
    assert repository.claimed_event_types == ["session.ready.v2"]
    assert repository.claimed_event_types != ["inbound.accepted"]
    assert publisher.published == [v2]
    assert isinstance(publisher.published[0], OutboxRecord)
    assert repository.marked == [("tenant-a", "v2", "dispatcher-a")]


@pytest.mark.asyncio
async def test_dispatcher_default_event_type_remains_v1() -> None:
    v1 = make_record("inbound.accepted", outbox_id="v1")
    repository = RecordingRepository(v1)
    publisher = RecordingPublisher()

    dispatcher = OutboxDispatcher(repository, publisher, owner_id="dispatcher-a")

    assert await dispatcher.dispatch_once() == 1
    assert repository.claimed_event_types == ["inbound.accepted"]
    assert publisher.published == [v1]


@pytest.mark.asyncio
async def test_publish_failure_releases_claimed_v2_record() -> None:
    v2 = make_record("session.ready.v2", outbox_id="v2", attempts=3)
    repository = RecordingRepository(v2)
    publisher = RecordingPublisher(ConnectionError("redis unavailable"))
    dispatcher = OutboxDispatcher(
        repository,
        publisher,
        owner_id="dispatcher-a",
        event_type="session.ready.v2",
    )

    assert await dispatcher.dispatch_once() == 0
    assert repository.claimed_event_types == ["session.ready.v2"]
    assert repository.marked == []
    assert repository.released == [
        ("tenant-a", "v2", "dispatcher-a", timedelta(seconds=8), "ConnectionError")
    ]
