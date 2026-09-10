from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from tests.support import inbound_message_data
from trpc_service.channels.contracts import (
    DeliveryAction,
    ErrorDetail,
    InboundMessage,
    OutboundReply,
    ReplyStatus,
    VerifiedBindingScope,
)


def test_inbound_message_accepts_strict_valid_payload_and_is_frozen() -> None:
    message = InboundMessage(**inbound_message_data())

    assert message.channel.value == "local_http"
    assert message.conversation_type.value == "direct"
    assert isinstance(message.trace_id, UUID)
    with pytest.raises(ValidationError):
        message.text = "changed"


@pytest.mark.parametrize("text", ["", "   ", "x" * 4001])
def test_inbound_message_rejects_invalid_text_boundaries(text: str) -> None:
    with pytest.raises(ValidationError):
        InboundMessage(**inbound_message_data(text=text))


def test_inbound_message_trims_text_and_accepts_unicode_limit() -> None:
    message = InboundMessage(**inbound_message_data(text="  你好  "))
    limit = InboundMessage(**inbound_message_data(text="界" * 4000))

    assert message.text == "你好"
    assert len(limit.text) == 4000


def test_inbound_message_rejects_unknown_fields_and_invalid_enums() -> None:
    with pytest.raises(ValidationError):
        InboundMessage(**inbound_message_data(extra="unsigned"))
    with pytest.raises(ValidationError):
        InboundMessage(**inbound_message_data(channel="wechat"))
    with pytest.raises(ValidationError):
        InboundMessage(**inbound_message_data(conversation_type="room"))


def test_verified_binding_scope_cannot_be_constructed_from_request_values() -> None:
    with pytest.raises(TypeError):
        VerifiedBindingScope(binding_id="binding-alpha", channel="local_http")


def test_success_reply_requires_safe_complete_delivery_data() -> None:
    reply = OutboundReply(
        status=ReplyStatus.SUCCEEDED,
        trace_id=UUID("11111111-1111-4111-8111-111111111111"),
        tenant_id="tenant-alpha",
        platform_session_id="sess_" + "a" * 64,
        external_message_id="message-001",
        text="stored:ALPHA",
        delivery_action=DeliveryAction.DELIVER,
    )

    assert reply.error is None
    assert reply.text == "stored:ALPHA"


def test_failed_reply_requires_error_and_rejects_stack_or_unknown_data() -> None:
    error = ErrorDetail(
        code="agent_failed",
        message="Agent execution failed.",
        retryable=False,
        execution_started=True,
    )
    reply = OutboundReply(
        status="failed",
        trace_id="11111111-1111-4111-8111-111111111111",
        delivery_action="none",
        error=error,
    )
    assert reply.error == error

    with pytest.raises(ValidationError):
        OutboundReply(
            status="failed",
            trace_id="11111111-1111-4111-8111-111111111111",
            delivery_action="none",
        )
    with pytest.raises(ValidationError):
        ErrorDetail(
            code="agent_failed",
            message="safe",
            retryable=False,
            execution_started=True,
            stack="secret stack",
        )
