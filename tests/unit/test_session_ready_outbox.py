from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trpc_service.queue.session_ready import SessionReady
from trpc_service.queue.session_ready_outbox import (
    SESSION_READY_EVENT_V2,
    SessionReadyOutboxQueue,
)
from trpc_service.storage.models import OutboxRecord


class Publisher:
    def __init__(self) -> None:
        self.group_ready = False
        self.messages: list[SessionReady] = []

    async def ensure_group(self) -> None:
        self.group_ready = True

    async def publish(self, message: SessionReady) -> str:
        self.messages.append(message)
        return "1-0"


def record(**payload_updates: object) -> OutboxRecord:
    payload: dict[str, object] = {
        "generation": 7,
        "priority": 0,
        "trace_id": "trace-a",
        "created_at": "2026-08-23T12:00:00Z",
        "ignored_provider_content": "must-not-reach-redis",
    }
    payload.update(payload_updates)
    return OutboxRecord(
        outbox_id="outbox-a",
        tenant_id="tenant-a",
        event_type=SESSION_READY_EVENT_V2,
        aggregate_id="session-a",
        payload=payload,
    )


@pytest.mark.asyncio
async def test_outbox_adapter_emits_only_authoritative_scheduler_fields() -> None:
    publisher = Publisher()
    queue = SessionReadyOutboxQueue(publisher)
    await queue.ensure_group()
    assert await queue.publish(record()) == "1-0"
    assert publisher.group_ready
    assert publisher.messages == [
        SessionReady(
            event_id="outbox-a",
            tenant_id="tenant-a",
            session_id="session-a",
            generation=7,
            priority=0,
            trace_id="trace-a",
            created_at=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"generation": 0}, "generation"),
        ({"generation": True}, "generation"),
        ({"priority": -1}, "priority"),
        ({"trace_id": ""}, "trace_id"),
        ({"created_at": "not-a-date"}, "created_at"),
    ],
)
async def test_outbox_adapter_rejects_invalid_scheduler_payload(
    updates: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        await SessionReadyOutboxQueue(Publisher()).publish(record(**updates))


@pytest.mark.asyncio
async def test_outbox_adapter_rejects_v1_or_unrelated_events() -> None:
    wrong = record().model_copy(update={"event_type": "inbound.accepted"})
    with pytest.raises(ValueError, match="not a SessionReady"):
        await SessionReadyOutboxQueue(Publisher()).publish(wrong)
