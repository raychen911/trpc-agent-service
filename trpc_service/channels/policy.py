"""Pure, shared policies for authenticated IM channel input and output."""

from __future__ import annotations

from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.models import InboundMessage, UnboundChannelMessage

CHANNEL_TEXT_LIMIT = 4000
UNSUPPORTED_MESSAGE_REPLY = "This message type is not supported."
_BINDING_ERROR = "Invalid channel binding."


def bind_message(message: UnboundChannelMessage, binding: ChannelBinding) -> InboundMessage:
    """Attach authority from one enabled, exactly matching channel binding."""
    if not isinstance(message, UnboundChannelMessage) or not isinstance(binding, ChannelBinding):
        raise ValueError(_BINDING_ERROR)
    if not binding.enabled:
        raise ValueError(_BINDING_ERROR)
    if message.channel != binding.channel or message.external_account_id != binding.external_account_id:
        raise ValueError(_BINDING_ERROR)
    if message.kind != "text":
        raise ValueError(UNSUPPORTED_MESSAGE_REPLY)
    # UnboundChannelMessage enforces a non-empty text for kind="text".
    assert message.text is not None
    return InboundMessage(
        tenant_id=binding.tenant_id,
        app_id=binding.app_id,
        binding_id=binding.binding_id,
        channel=message.channel,
        external_user_id=message.external_user_id,
        external_conversation_id=message.external_conversation_id,
        external_message_id=message.external_message_id,
        conversation_kind=message.conversation_kind,
        text=message.text,
    )


def split_text(text: str, limit: int = CHANNEL_TEXT_LIMIT) -> tuple[str, ...]:
    """Split text at Unicode code-point boundaries without loss or empty chunks."""
    if not isinstance(text, str) or isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("Invalid text split input.")
    return tuple(text[offset:offset + limit] for offset in range(0, len(text), limit))


__all__ = [
    "CHANNEL_TEXT_LIMIT",
    "UNSUPPORTED_MESSAGE_REPLY",
    "bind_message",
    "split_text",
]
