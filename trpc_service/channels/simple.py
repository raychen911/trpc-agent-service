"""Reusable JSON webhook adapter."""

from __future__ import annotations

from typing import Any

from trpc_service.channels.base import (
    ChannelCapabilities,
    InboundMessage,
    OutboundMessage,
    SendResult,
    normalize_event_type,
    parse_attachments,
    revoke_target_message_id,
    sanitize_event_payload,
    verify_optional_hmac,
)
from trpc_service.tenant.models import ChannelBinding


class SimpleJsonChannelAdapter:
    channel_name = "simple"
    capabilities = ChannelCapabilities()
    message_id_fields = ("message_id", "MsgId", "update_id")
    user_id_fields = ("user_id", "FromUserName", "from_user_id", "from")
    group_id_fields = ("group_id", "chat_id", "room_id")
    text_fields = ("text", "Content", "message")

    def verify_callback(self, payload: dict[str, Any], binding: ChannelBinding) -> None:
        verify_optional_hmac(payload, binding)

    def parse_event(self, payload: dict[str, Any], binding: ChannelBinding) -> InboundMessage:
        event_type = normalize_event_type(payload)
        raw_event = sanitize_event_payload(payload)
        raw_event["normalized_event_type"] = event_type
        if event_type == "revoke":
            target_message_id = revoke_target_message_id(payload)
            if target_message_id:
                raw_event["target_message_id"] = target_message_id
        external_user_id = self._first(payload, self.user_id_fields, "anonymous")
        return InboundMessage(
            channel=self.channel_name,
            account_id=str(payload.get("account_id", binding.account_id)),
            external_message_id=self._first(payload, self.message_id_fields, "local-message"),
            external_user_id=external_user_id,
            group_id=self._optional_first(payload, self.group_id_fields),
            text=self._optional_first(payload, self.text_fields),
            attachments=parse_attachments(payload.get("attachments")),
            raw_event=raw_event,
            internal_user_id=binding.resolve_user_id(external_user_id),
        )

    def send(self, message: OutboundMessage, binding: ChannelBinding) -> SendResult:
        target = message.group_id or message.external_user_id
        return SendResult(
            ok=True,
            response_ref=f"{self.channel_name}:{binding.account_id}:{message.session_id}",
            metadata={"target": target, "text_length": len(message.text)},
        )

    @staticmethod
    def _first(payload: dict[str, Any], fields: tuple[str, ...], default: str) -> str:
        return str(SimpleJsonChannelAdapter._optional_first(payload, fields) or default)

    @staticmethod
    def _optional_first(payload: dict[str, Any], fields: tuple[str, ...]) -> str | None:
        for field in fields:
            value = payload.get(field)
            if value is not None and value != "":
                return str(value)
        return None
