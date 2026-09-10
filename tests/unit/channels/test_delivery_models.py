from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import ProviderReplyContext
from trpc_service.storage.models import (
    DeliveryAttempt,
    DeliveryOutcome,
    DeliveryRecord,
    DeliveryStatus,
)


NOW = datetime(2026, 9, 8, 8, 0, tzinfo=timezone.utc)


def _record() -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=UUID(int=1),
        tenant_id="tenant-alpha",
        binding_id="binding-alpha",
        channel=Channel.FEISHU,
        idempotency_key_digest="a" * 64,
        execution_trace_id=UUID(int=2),
        reply_context=ProviderReplyContext(
            channel=Channel.FEISHU,
            conversation_type="direct",
            reply_target_id="chat-sensitive",
            provider_message_id="message-sensitive",
        ),
        result_digest="b" * 64,
        status=DeliveryStatus.PENDING,
        adapter_generation=1,
        created_at=NOW,
        updated_at=NOW,
    )


def test_delivery_record_allows_only_documented_transitions() -> None:
    sending = _record().transition(DeliveryStatus.SENDING, NOW)
    retrying = sending.transition(
        DeliveryStatus.RETRY_WAIT,
        NOW,
        next_attempt_at=NOW + timedelta(seconds=1),
    )
    sending_again = retrying.transition(DeliveryStatus.SENDING, NOW + timedelta(seconds=1))
    delivered = sending_again.transition(DeliveryStatus.DELIVERED, NOW + timedelta(seconds=1))

    assert retrying.next_attempt_at == NOW + timedelta(seconds=1)
    assert delivered.next_attempt_at is None
    with pytest.raises(ValueError, match="terminal"):
        delivered.transition(DeliveryStatus.SENDING, NOW + timedelta(seconds=2))
    with pytest.raises(ValueError, match="invalid"):
        _record().transition(DeliveryStatus.DELIVERED, NOW)


@pytest.mark.parametrize("attempt_no", [0, 5])
def test_delivery_attempt_number_is_limited(attempt_no: int) -> None:
    with pytest.raises(ValidationError):
        DeliveryAttempt(
            attempt_id=UUID(int=3),
            delivery_id=UUID(int=1),
            attempt_no=attempt_no,
            trace_id=UUID(int=4),
            adapter_node_id="adapter-a",
            adapter_generation=1,
            started_at=NOW,
            finished_at=NOW,
            outcome=DeliveryOutcome.SUCCEEDED,
        )


def test_delivery_attempt_validates_outcome_metadata_and_time_order() -> None:
    attempt = DeliveryAttempt(
        attempt_id=UUID(int=3),
        delivery_id=UUID(int=1),
        attempt_no=2,
        trace_id=UUID(int=4),
        adapter_node_id="adapter-a",
        adapter_generation=1,
        started_at=NOW,
        finished_at=NOW + timedelta(milliseconds=10),
        outcome=DeliveryOutcome.TRANSIENT,
        safe_error_code="provider_unavailable",
        retry_delay_seconds=1,
    )
    assert attempt.retry_delay_seconds == 1

    with pytest.raises(ValidationError):
        attempt.model_copy(
            update={"finished_at": NOW - timedelta(seconds=1)},
        ).model_validate(
            attempt.model_copy(update={"finished_at": NOW - timedelta(seconds=1)}).model_dump()
        )
