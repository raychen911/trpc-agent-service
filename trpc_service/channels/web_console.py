"""Web Console channel adapter for local testing."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, StrictStr

from trpc_service.channels.models import InboundMessage, PublicChannelEvent

WEB_CONSOLE_CHANNEL = "web_console"


class WebConsoleInbound(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: StrictStr
    user_id: StrictStr
    conversation_id: StrictStr
    message_id: StrictStr
    message: StrictStr


class WebConsoleChannelAdapter:
    channel = WEB_CONSOLE_CHANNEL

    def decode(self, payload: object) -> InboundMessage:
        if not isinstance(payload, dict):
            raise ValueError("Invalid payload.")
        inbound = WebConsoleInbound.model_validate(payload)
        return InboundMessage(
            tenant_id=inbound.tenant_id,
            channel=WEB_CONSOLE_CHANNEL,
            external_user_id=inbound.user_id,
            external_conversation_id=inbound.conversation_id,
            external_message_id=inbound.message_id,
            text=inbound.message,
        )

    def encode_sync(self, response: str) -> dict[str, Any]:
        return {"response": response}

    def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
        return {
            "type": event.type,
            "data": event.data,
        }


__all__ = ["WEB_CONSOLE_CHANNEL", "WebConsoleChannelAdapter", "WebConsoleInbound"]
