from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

from trpc_service.storage.postgres.repositories import _safe_database_failure
from trpc_service.storage.postgres.repositories import PostgresDeliveryRepository


class UnsafeDriverError(RuntimeError):
    sqlstate = "23503"
    constraint_name = "delivery_records_binding_id_fkey"


class UnsafeWrapper(RuntimeError):
    def __init__(self, marker: str) -> None:
        super().__init__(marker)
        self.orig = UnsafeDriverError(marker)


def test_database_failure_diagnostic_never_renders_exception_message() -> None:
    marker = "credential-and-message-marker-must-not-appear"

    diagnostic = _safe_database_failure(UnsafeWrapper(marker))

    assert diagnostic == (
        "UnsafeWrapper",
        "UnsafeDriverError",
        "23503",
        "delivery_records_binding_id_fkey",
    )
    assert marker not in repr(diagnostic)


def test_database_failure_diagnostic_rejects_unbounded_metadata() -> None:
    error = UnsafeDriverError("ignored")
    error.sqlstate = "bad value with spaces"
    error.constraint_name = "unsafe;detail"

    diagnostic = _safe_database_failure(error)

    assert diagnostic[2:] == ("unknown", "unknown")


def test_delivery_readback_ignores_legacy_computed_context_digest() -> None:
    now = datetime(2026, 9, 9, tzinfo=timezone.utc)
    row = SimpleNamespace(
        delivery_id=str(UUID(int=1)),
        tenant_id="tenant-alpha",
        binding_id="binding-feishu-real",
        channel="feishu",
        idempotency_key_digest="a" * 64,
        execution_trace_id=str(UUID(int=2)),
        reply_context={
            "channel": "feishu",
            "conversation_type": "direct",
            "reply_target_id": "chat",
            "protocol_request_id": None,
            "provider_message_id": "message",
            "context_digest": "b" * 64,
        },
        result_digest="c" * 64,
        status="pending",
        adapter_generation=1,
        next_attempt_at=None,
        created_at=now,
        updated_at=now,
    )

    record = PostgresDeliveryRepository._delivery_domain(row)

    assert record.reply_context.provider_message_id == "message"
