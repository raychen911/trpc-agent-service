"""Transport-neutral IM adapter contract and delivery errors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from tenant_agent.models import ChannelBindingConfig, InboundEnvelope, OutboundMessage, TenantConfig
from tenant_agent.security import CompositeSecretResolver

TELEGRAM_TEXT_MAX_CHARS = 4_000  # Safety margin below the Bot API's 4,096-character limit.
WECOM_TEXT_MAX_CHARS = 1_800
WECOM_TEXT_MAX_UTF8_BYTES = 2_048


class ChannelError(RuntimeError):
    public_status = 400


class SignatureError(ChannelError):
    public_status = 401


class UnsupportedMessage(ChannelError):
    public_status = 422


class DeliveryError(ChannelError):
    public_status = 502


class PermanentDeliveryError(DeliveryError):
    """A provider rejection that retries cannot repair without configuration changes."""


class RateLimited(DeliveryError):
    def __init__(self, retry_after_seconds: float):
        super().__init__("channel rate limit")
        self.retry_after_seconds = max(0.1, retry_after_seconds)


@dataclass(frozen=True, slots=True)
class WebhookRequest:
    method: str
    headers: dict[str, str]
    query: dict[str, str]
    body: bytes
    client_host: str | None = None


@dataclass(frozen=True, slots=True)
class WebhookAck:
    status_code: int = 200
    body: bytes = b"ok"
    media_type: str = "text/plain"
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ParsedWebhook:
    messages: tuple[InboundEnvelope, ...]
    acknowledgement: WebhookAck = WebhookAck()


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    external_message_ids: tuple[str, ...]


class ChannelAdapter(Protocol):
    name: str

    async def parse(
        self,
        request: WebhookRequest,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> ParsedWebhook: ...

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> DeliveryResult: ...


def split_text(text: str, *, max_chars: int, max_utf8_bytes: int | None = None) -> tuple[str, ...]:
    if not text:
        return ("",)
    chunks: list[str] = []
    remaining = text
    while remaining:
        candidate = remaining[:max_chars]
        if max_utf8_bytes is not None:
            while candidate and len(candidate.encode()) > max_utf8_bytes:
                candidate = candidate[:-1]
        if not candidate:
            candidate = remaining[0]
        if len(candidate) < len(remaining):
            split_at = max(candidate.rfind("\n"), candidate.rfind(" "))
            if split_at >= len(candidate) // 2:
                candidate = candidate[: split_at + 1]
        chunks.append(candidate)
        remaining = remaining[len(candidate) :]
    return tuple(chunks)
