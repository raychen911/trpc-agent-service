"""Fail-closed contracts for durable IM reply delivery.

Only opaque identifiers and normalized outcomes cross this boundary.  Decrypted
routes, bot tokens, chat identifiers, and reply content are intentionally absent
from result objects so accidental ``repr``/structured logging cannot expose them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

from trpc_service.reliability.types import (
    OutboxDeliveryClaim,
    OutboxDeliveryOutcome,
)


class DeliveryConfigurationError(RuntimeError):
    """A tenant-scoped delivery binding is unavailable or invalid."""


class DispatchState(StrEnum):
    """Observable dispatcher result without channel credentials or message data."""

    NO_WORK = "no_work"
    RECORDED = "recorded"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class DeliveryBinding:
    """Minimal tenant-scoped projection of a live ChannelBinding row."""

    tenant_id: str
    binding_id: str
    channel_type: str
    status: str
    secret_refs: dict[str, str]


@dataclass(frozen=True, slots=True)
class DispatchReport:
    """A logging-safe outcome for one ``dispatch_once`` call."""

    state: DispatchState
    outbox_id: str | None = None
    outcome: OutboxDeliveryOutcome | None = None
    error_type: str | None = None
    persisted: bool = False


class TextDeliveryPayload(BaseModel):
    """The only currently supported Outbox payload schema."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1]
    kind: Literal["text"]
    text: str = Field(min_length=1, max_length=16_384)

    @field_validator("text")
    @classmethod
    def text_is_not_only_whitespace(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be blank")
        return value


class TelegramDeliveryRoute(BaseModel):
    """Strict plaintext schema recovered from an authenticated route envelope."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    chat_id: int
    message_thread_id: int | None
    reply_to_message_id: int = Field(gt=0)
    callback_query_id: str | None = Field(default=None, min_length=1, max_length=256)

    @field_validator("chat_id")
    @classmethod
    def chat_id_is_not_zero(cls, value: int) -> int:
        if value == 0:
            raise ValueError("chat_id must not be zero")
        return value

    @field_validator("message_thread_id")
    @classmethod
    def thread_id_is_positive(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("message_thread_id must be positive")
        return value


class OutboxPort(Protocol):
    """Reliability operations required by the dispatcher."""

    async def claim_outbox(
        self,
        tenant_id: str,
        dispatcher_id: str,
    ) -> OutboxDeliveryClaim | None:
        """Claim one ordered outbound part."""

    async def renew_outbox_claim(self, claim: OutboxDeliveryClaim) -> bool:
        """Renew the delivery fence before an external side effect."""

    async def record_delivery(
        self,
        claim: OutboxDeliveryClaim,
        outcome: OutboxDeliveryOutcome,
        *,
        external_message_id: str | None = None,
        error_type: str | None = None,
        next_retry_at: datetime | None = None,
    ) -> bool:
        """Record a result under the claim's delivery token and attempt fence."""


class BindingPort(Protocol):
    """Tenant-scoped ChannelBinding reader."""

    async def load(self, tenant_id: str, binding_id: str) -> DeliveryBinding:
        """Return one active tenant-owned binding or fail closed."""
