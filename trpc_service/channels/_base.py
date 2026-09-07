# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Channel adapter abstraction.

A channel adapter converts platform-specific webhook payloads into normalized
:class:`InboundMessage` objects and converts agent output back into platform
messages. Each supported IM platform provides one implementation.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import AsyncIterable
from typing import Optional

from ._models import InboundMessage
from ._models import OutboundMessage
from ._models import SendResult


class ChannelAdapter(ABC):
    """Base class for IM channel adapters."""

    channel: str = "unknown"
    """Channel identifier, overridden by subclasses."""

    @abstractmethod
    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        """Verify the webhook signature / secret of an inbound request.

        Args:
            payload: Raw request body (dict / bytes / str depending on platform).
            headers: HTTP headers.
            query: HTTP query parameters (used for WeCom's GET URL verification).

        Returns:
            ``True`` when the request is authentic.
        """

    async def verify_request(
        self,
        raw_body: bytes,
        payload: Any,
        headers: dict[str, str],
        query: dict[str, str],
    ) -> bool:
        """Verify a callback while retaining access to the exact request body.

        Most adapters can authenticate the parsed payload and therefore use
        this default implementation. Protocols whose signature covers the raw
        HTTP bytes (for example QQ Bot's Ed25519 callback) override it.
        """
        return await self.verify_signature(payload, headers, query)

    async def challenge_response(self, payload: Any) -> Optional[dict[str, Any]]:
        """Return a platform callback-verification response when applicable."""
        return None

    def callback_response(self, *, duplicate: bool = False) -> dict[str, Any]:
        """Return the successful callback acknowledgement body."""
        return {"status": "duplicate" if duplicate else "ok"}

    @abstractmethod
    async def parse_message(self, payload: Any) -> InboundMessage:
        """Convert a raw webhook payload into a normalized inbound message."""

    @abstractmethod
    async def send_message(self, outbound: OutboundMessage) -> SendResult:
        """Send a single message to the platform."""

    @abstractmethod
    async def send_stream(self, chat_id: str, stream: AsyncIterable[str]) -> SendResult:
        """Send a stream of text chunks as (optionally) a streaming message."""

    @abstractmethod
    async def reply_text(self, inbound: InboundMessage, text: str) -> SendResult:
        """Send a plain-text reply to an inbound message."""
