"""Shared deterministic test helpers for the local message flow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID


FIXED_UTC = datetime(2026, 9, 5, 8, 0, tzinfo=timezone.utc)


@dataclass(slots=True)
class CallCounter:
    count: int = 0

    def __call__(self, *_args: Any, **_kwargs: Any) -> None:
        self.count += 1


def inbound_message_data(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "channel": "local_http",
        "binding_id": "binding-alpha",
        "external_message_id": "message-001",
        "external_user_id": "user-001",
        "conversation_type": "direct",
        "external_conversation_id": "conversation-001",
        "text": "Remember validation token ALPHA.",
        "received_at": FIXED_UTC,
        "trace_id": UUID("11111111-1111-4111-8111-111111111111"),
    }
    payload.update(overrides)
    return payload
