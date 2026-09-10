from __future__ import annotations

from importlib import import_module
from uuid import UUID

import pytest

from tests.support import FIXED_UTC


def test_idempotency_models_preserve_three_traces_and_terminal_invariants() -> None:
    models = import_module("trpc_service.storage.models")
    key = models.IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="message-001")
    record = models.IdempotencyRecord.pending(
        key=key,
        content_fingerprint="a" * 64,
        owner_token="owner-1",
        trace_id=UUID("11111111-1111-4111-8111-111111111111"),
        now=FIXED_UTC,
    )
    running = record.mark_running("owner-1", record.owner_trace_id, FIXED_UTC)
    assert running.first_claim_trace_id == record.first_claim_trace_id
    assert running.execution_trace_id == record.owner_trace_id
    assert running.state.value == "running"
    with pytest.raises(ValueError, match="transition"):
        running.mark_running("owner-1", running.owner_trace_id, FIXED_UTC)


def test_content_fingerprint_is_stable_and_excludes_delivery_metadata() -> None:
    models = import_module("trpc_service.storage.models")
    from trpc_service.channels.contracts import InboundMessage
    from tests.support import inbound_message_data

    first = InboundMessage(**inbound_message_data())
    redelivery = InboundMessage(**inbound_message_data(trace_id=UUID("22222222-2222-4222-8222-222222222222")))
    assert models.content_fingerprint(first) == models.content_fingerprint(redelivery)
