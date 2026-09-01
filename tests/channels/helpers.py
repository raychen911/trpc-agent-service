"""Shared constructors for channel contract tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trpc_service.channels.contracts import (
    CallbackRequest,
    Channel,
    TrustedBindingContext,
)

FIXTURES = Path(__file__).with_name("fixtures")
IDENTITY_KEY = b"channel-identity-test-key-32-bytes-minimum"
TELEGRAM_AUTH_VALUE = "telegram_secret_2026"


def load_json(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def binding(
    channel: Channel,
    *,
    binding_id: str = "binding-001",
    account_id: str = "bot-001",
    tenant_id: str = "tenant-001",
    app_id: str = "app-001",
    enabled: bool = True,
) -> TrustedBindingContext:
    return TrustedBindingContext(
        tenant_id=tenant_id,
        app_id=app_id,
        app_revision=1,
        binding_id=binding_id,
        binding_revision=3,
        channel=channel,
        external_account_id=account_id,
        enabled=enabled,
    )


def callback_request(
    body: bytes,
    *,
    binding_id: str = "binding-001",
    headers: tuple[tuple[str, str], ...] = (),
    query: tuple[tuple[str, str], ...] = (),
) -> CallbackRequest:
    return CallbackRequest(
        path_binding_id=binding_id,
        headers=headers,
        query=query,
        body=body,
        received_at=datetime(2026, 8, 29, tzinfo=UTC),
        request_id="request-001",
        trace_id="0123456789abcdef0123456789abcdef",
    )
