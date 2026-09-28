"""Explicit registry for independently deployable channel adapters."""

from trpc_service.channels.contracts import ChannelAdapter
from trpc_service.config.models import normalize_channel_type


class ChannelAdapterNotFound(LookupError):
    """Raised when a binding requests an adapter unavailable on this node."""


class ChannelAdapterAlreadyRegistered(ValueError):
    """Raised when startup registers the same channel type twice."""


class ChannelAdapterRegistry:
    """Map channel names to concrete adapters without importing providers centrally."""

    def __init__(self) -> None:
        self._adapters: dict[str, ChannelAdapter] = {}

    @staticmethod
    def _normalize(channel_type: str) -> str:
        return normalize_channel_type(channel_type)

    def register(self, adapter: ChannelAdapter, *, replace: bool = False) -> None:
        """Register one concrete implementation during application composition."""

        channel_type = self._normalize(adapter.channel_type)
        if channel_type in self._adapters and not replace:
            raise ChannelAdapterAlreadyRegistered(
                f"channel adapter is already registered: {channel_type}")
        self._adapters[channel_type] = adapter

    def resolve(self, channel_type: str) -> ChannelAdapter:
        """Return the adapter selected by a Channel Binding."""

        normalized = self._normalize(channel_type)
        try:
            return self._adapters[normalized]
        except KeyError as error:
            raise ChannelAdapterNotFound(
                f"channel adapter is not available on this node: {normalized}") from error

    @property
    def supported_types(self) -> tuple[str, ...]:
        """Return deterministic channel names for health checks and diagnostics."""

        return tuple(sorted(self._adapters))
