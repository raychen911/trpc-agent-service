"""Strict versioned channel contract for inbound messages and public events."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from trpc_service.tenant.context import validate_tenant_id

_CHANNEL_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_MAX_TEXT_LENGTH: int = 8000
_MAX_EXTERNAL_ID_LENGTH: int = 200
_FIXED_FIELD_ERROR: str = "Invalid field value."

ConversationKind = Literal["direct", "group"]
ChannelMessageKind = Literal["text", "image", "file", "unsupported"]


def validate_channel(channel: str) -> str:
    if not isinstance(channel, str):
        raise ValueError(_FIXED_FIELD_ERROR)
    stripped = channel.strip()
    if not stripped:
        raise ValueError(_FIXED_FIELD_ERROR)
    if _CHANNEL_PATTERN.fullmatch(stripped) is None:
        raise ValueError(_FIXED_FIELD_ERROR)
    return stripped


def _strip_nonblank(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(_FIXED_FIELD_ERROR)
    stripped = value.strip()
    if not stripped:
        raise ValueError(_FIXED_FIELD_ERROR)
    return stripped


@dataclass(frozen=True, slots=True)
class InboundMessage:
    tenant_id: str
    channel: str
    external_user_id: str
    external_conversation_id: str
    external_message_id: str
    text: str
    app_id: str | None = None
    binding_id: UUID | None = None
    conversation_kind: ConversationKind | None = None

    def __post_init__(self) -> None:
        tid = _strip_nonblank(self.tenant_id, "tenant_id")
        validate_tenant_id(tid)
        object.__setattr__(self, "tenant_id", tid)

        ch = _strip_nonblank(self.channel, "channel")
        validate_channel(ch)
        object.__setattr__(self, "channel", ch)

        euid = _strip_nonblank(self.external_user_id, "external_user_id")
        if len(euid) > _MAX_EXTERNAL_ID_LENGTH:
            raise ValueError(_FIXED_FIELD_ERROR)
        object.__setattr__(self, "external_user_id", euid)

        ecid = _strip_nonblank(self.external_conversation_id, "external_conversation_id")
        if len(ecid) > _MAX_EXTERNAL_ID_LENGTH:
            raise ValueError(_FIXED_FIELD_ERROR)
        object.__setattr__(self, "external_conversation_id", ecid)

        emid = _strip_nonblank(self.external_message_id, "external_message_id")
        if len(emid) > _MAX_EXTERNAL_ID_LENGTH:
            raise ValueError(_FIXED_FIELD_ERROR)
        object.__setattr__(self, "external_message_id", emid)

        txt = _strip_nonblank(self.text, "text")
        if len(txt) > _MAX_TEXT_LENGTH:
            raise ValueError(_FIXED_FIELD_ERROR)
        object.__setattr__(self, "text", txt)

        if self.app_id is not None:
            app_id = _strip_nonblank(self.app_id, "app_id")
            if len(app_id) > _MAX_EXTERNAL_ID_LENGTH:
                raise ValueError(_FIXED_FIELD_ERROR)
            object.__setattr__(self, "app_id", app_id)

        if self.binding_id is not None and not isinstance(self.binding_id, UUID):
            raise ValueError(_FIXED_FIELD_ERROR)
        if self.conversation_kind is not None and self.conversation_kind not in {"direct", "group"}:
            raise ValueError(_FIXED_FIELD_ERROR)


@dataclass(frozen=True, slots=True)
class UnboundChannelMessage:
    """Authenticated IM input before its account is resolved to a tenant.

    SDK facades construct this type.  It deliberately has no tenant or app
    field: only a persisted :class:`ChannelBinding` can add that authority.
    """

    channel: Literal["wecom", "feishu"]
    external_account_id: str
    conversation_kind: ConversationKind
    external_user_id: str
    external_conversation_id: str
    external_message_id: str
    kind: ChannelMessageKind
    text: str | None = None
    occurred_at_ms: int | None = None

    def __post_init__(self) -> None:
        if self.channel not in {"wecom", "feishu"}:
            raise ValueError(_FIXED_FIELD_ERROR)
        for field_name in (
                "external_account_id",
                "external_user_id",
                "external_conversation_id",
                "external_message_id",
        ):
            value = _strip_nonblank(getattr(self, field_name), field_name)
            if len(value) > _MAX_EXTERNAL_ID_LENGTH:
                raise ValueError(_FIXED_FIELD_ERROR)
            object.__setattr__(self, field_name, value)
        if self.conversation_kind not in {"direct", "group"}:
            raise ValueError(_FIXED_FIELD_ERROR)
        if self.kind not in {"text", "image", "file", "unsupported"}:
            raise ValueError(_FIXED_FIELD_ERROR)
        if self.kind == "text":
            text = _strip_nonblank(self.text, "text")
            if len(text) > _MAX_TEXT_LENGTH:
                raise ValueError(_FIXED_FIELD_ERROR)
            object.__setattr__(self, "text", text)
        elif self.text is not None:
            raise ValueError(_FIXED_FIELD_ERROR)
        if self.occurred_at_ms is not None:
            if isinstance(self.occurred_at_ms, bool) or not isinstance(self.occurred_at_ms, int):
                raise ValueError(_FIXED_FIELD_ERROR)
            if self.occurred_at_ms < 0:
                raise ValueError(_FIXED_FIELD_ERROR)


class PublicChannelEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["delta", "tool", "done", "error", "approval"]
    data: str | dict[str, Any] | None = None


__all__ = [
    "InboundMessage",
    "UnboundChannelMessage",
    "ConversationKind",
    "ChannelMessageKind",
    "PublicChannelEvent",
    "validate_channel",
]
