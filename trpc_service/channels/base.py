# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Contracts shared by IM Channel Adapters."""

from __future__ import annotations

from typing import Any
from typing import Protocol
from typing import runtime_checkable

from pydantic import BaseModel

from trpc_service.gateway.models import NormalizedInboundMessage
from trpc_service.gateway.models import OutboundMessage
from trpc_service.gateway.models import Attachment


class ChannelAuthenticationError(PermissionError):
    """Raised when a webhook or frame cannot be authenticated."""


class UnsupportedMessageError(ValueError):
    """Raised when a channel event has no supported content."""


class ChannelTransportError(RuntimeError):
    """Transport failure classified without exposing a remote error body."""

    def __init__(self,
                 code: str,
                 *,
                 retryable: bool = False,
                 uncertain: bool = False,
                 retry_after_seconds: float = 0) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.uncertain = uncertain
        self.retry_after_seconds = retry_after_seconds


class DeliveryResult(BaseModel):
    delivered: bool
    external_message_id: str = ""
    retryable: bool = False
    error_code: str = ""
    retry_after_seconds: float = 0
    uncertain: bool = False


class ChannelAdapter(Protocol):
    """Normalize inbound payloads and deliver transport-neutral replies."""

    async def normalize(self, binding_id: str, payload: dict[str, Any], headers: dict[str,
                                                                                      str]) -> NormalizedInboundMessage:
        """Authenticate and normalize one inbound event."""

    async def deliver(self, message: OutboundMessage) -> DeliveryResult:
        """Deliver one outbound message or return a retryable result."""

    async def close(self) -> None:
        """Close network resources."""


@runtime_checkable
class InboundAttachmentDownloader(Protocol):
    """Optional capability for materializing a channel-owned attachment."""

    async def download_attachment(self, attachment: Attachment) -> tuple[bytes, str, str]:
        """Return content, MIME type and a safe original filename."""
