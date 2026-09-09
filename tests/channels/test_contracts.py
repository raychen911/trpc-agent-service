"""Tests for immutable cross-channel contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from trpc_service.channels.contracts import (
    CallbackKind,
    CallbackRequest,
    Channel,
    ConversationKind,
    NormalizedInbound,
    ReplyIntent,
    ReplyKind,
    VerifiedCallback,
)


def _normalized() -> NormalizedInbound:
    return NormalizedInbound(
        tenant_id="tenant",
        app_id="app",
        binding_id="binding",
        binding_revision=1,
        channel=Channel.TELEGRAM,
        delivery_id="42",
        payload_sha256="a" * 64,
        received_at=datetime.now(UTC),
        principal_id="usr",
        conversation_id="conv",
        session_id="sess",
        conversation_kind=ConversationKind.PRIVATE,
        text="hello",
        reply_route_key="route",
        request_id="request",
        trace_id="trace",
    )


def test_callback_request_lookup_is_case_aware_as_required() -> None:
    request = CallbackRequest(
        path_binding_id="binding",
        headers=(("content-type", "application/json"),),
        query=(("nonce", "abc"),),
        received_at=datetime.now(UTC),
        request_id="request",
        trace_id="trace",
    )

    assert request.header("Content-Type") == "application/json"
    assert request.query_value("nonce") == "abc"
    assert request.query_value("Nonce") is None


def test_callback_request_rejects_ambiguous_values_and_naive_time() -> None:
    request = CallbackRequest(
        path_binding_id="binding",
        headers=(("X-Test", "a"), ("x-test", "b")),
        query=(("nonce", "a"), ("nonce", "b")),
        received_at=datetime.now(UTC),
        request_id="request",
        trace_id="trace",
    )

    with pytest.raises(ValueError, match="duplicate header"):
        request.header("X-Test")
    with pytest.raises(ValueError, match="duplicate query"):
        request.query_value("nonce")
    with pytest.raises(ValidationError, match="timezone-aware"):
        CallbackRequest(
            path_binding_id="binding",
            received_at=datetime(2026, 8, 29),
            request_id="request",
            trace_id="trace",
        )


def test_models_are_frozen() -> None:
    inbound = _normalized()

    with pytest.raises(ValidationError, match="frozen"):
        inbound.text = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("digest", ["A" * 64, "a" * 63, "z" * 64])
def test_normalized_inbound_rejects_bad_digest(digest: str) -> None:
    data = _normalized().model_dump()
    data["payload_sha256"] = digest

    with pytest.raises(ValidationError, match="SHA-256"):
        NormalizedInbound.model_validate(data)


def test_normalized_inbound_requires_supported_content() -> None:
    data = _normalized().model_dump()
    data["text"] = None

    with pytest.raises(ValidationError, match="requires text or an attachment"):
        NormalizedInbound.model_validate(data)


def test_verified_callback_enforces_routing_shape() -> None:
    inbound = _normalized()
    user = VerifiedCallback(
        kind=CallbackKind.USER_MESSAGE,
        channel=Channel.TELEGRAM,
        delivery_id="42",
        payload_sha256="a" * 64,
        inbound=inbound,
    )
    assert user.inbound is inbound

    with pytest.raises(ValidationError, match="requires normalized inbound"):
        VerifiedCallback(
            kind=CallbackKind.USER_MESSAGE,
            channel=Channel.TELEGRAM,
            delivery_id="42",
            payload_sha256="a" * 64,
        )
    with pytest.raises(ValidationError, match="must not invoke"):
        VerifiedCallback(
            kind=CallbackKind.CHANNEL_EVENT,
            channel=Channel.TELEGRAM,
            delivery_id="42",
            payload_sha256="a" * 64,
            inbound=inbound,
        )
    with pytest.raises(ValidationError, match="requires control_id"):
        VerifiedCallback(
            kind=CallbackKind.CONTROL_REFRESH,
            channel=Channel.WECOM,
            delivery_id="42",
            payload_sha256="a" * 64,
        )


def _reply(**overrides: object) -> ReplyIntent:
    values: dict[str, object] = {
        "intent_id": "intent",
        "tenant_id": "tenant",
        "binding_id": "binding",
        "session_id": "session",
        "run_id": "run",
        "in_reply_to_delivery_id": "42",
        "kind": ReplyKind.FINAL,
        "text": "answer",
        "final": True,
        "idempotency_key": "reply:42",
    }
    values.update(overrides)
    return ReplyIntent.model_validate(values)


def test_reply_intent_semantics() -> None:
    assert _reply().final is True
    assert _reply(kind=ReplyKind.SNAPSHOT, final=False).revision == 1

    with pytest.raises(ValidationError, match="snapshot reply cannot be final"):
        _reply(kind=ReplyKind.SNAPSHOT, final=True)
    with pytest.raises(ValidationError, match="final/error reply must be final"):
        _reply(kind=ReplyKind.ERROR, final=False)
    with pytest.raises(ValidationError, match="requires text or an attachment"):
        _reply(text=None)
