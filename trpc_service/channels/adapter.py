"""ChannelAdapter Protocol and strict AdapterRegistry."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from trpc_service.channels.models import InboundMessage, PublicChannelEvent


@runtime_checkable
class ChannelAdapter(Protocol):
    """Narrow contract for channel adapters: decode inbound, encode outbound."""

    channel: str

    def decode(self, payload: object) -> InboundMessage:
        ...

    def encode_sync(self, response: str) -> dict[str, Any]:
        ...

    def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
        ...


class DuplicateAdapterError(Exception):
    """Raised when registering an adapter for an already-registered channel."""


class UnknownAdapterError(Exception):
    """Raised when looking up an adapter for an unregistered channel."""


class AdapterRegistry:
    """Strict mapping from channel name to ChannelAdapter."""

    def __init__(self) -> None:
        self._adapters: dict[str, ChannelAdapter] = {}

    def register(self, adapter: ChannelAdapter) -> None:
        if adapter.channel in self._adapters:
            raise DuplicateAdapterError(f"Adapter for channel '{adapter.channel}' already registered.")
        self._adapters[adapter.channel] = adapter

    def get(self, channel: str) -> ChannelAdapter:
        if channel not in self._adapters:
            raise UnknownAdapterError(f"No adapter registered for channel '{channel}'.")
        return self._adapters[channel]

    def channels(self) -> list[str]:
        return list(self._adapters.keys())


__all__ = [
    "AdapterRegistry",
    "ChannelAdapter",
    "DuplicateAdapterError",
    "UnknownAdapterError",
]
