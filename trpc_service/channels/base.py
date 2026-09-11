"""Protocol implemented by concrete Web, WeCom and Telegram adapters."""

from __future__ import annotations

from typing import Any
from typing import Protocol

from .models import ChannelCapabilities
from .models import NormalizedInboundMessage
from .models import OutboundMessage


class CallbackVerificationError(ValueError):
    """Raised when an IM callback cannot be authenticated."""


class ChannelAdapter(Protocol):
    """Boundary between an external IM SDK and the service data plane."""

    @property
    def capabilities(self) -> ChannelCapabilities:
        ...

    async def verify_callback(self, request: Any, binding: Any) -> None:
        ...

    async def parse_inbound(self, request: Any, binding: Any) -> NormalizedInboundMessage:
        ...

    async def acknowledge(self, message: NormalizedInboundMessage) -> Any:
        ...

    async def send(self, binding: Any, message: OutboundMessage) -> str:
        """Send a message and return the external message identifier."""
        ...


async def verify_and_parse_callback(
    adapter: ChannelAdapter,
    request: Any,
    binding: Any,
) -> NormalizedInboundMessage:
    """Authenticate a raw callback before allowing it to become a normalized message."""

    await adapter.verify_callback(request, binding)
    return await adapter.parse_inbound(request, binding)
