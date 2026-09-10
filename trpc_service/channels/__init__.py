"""Channel-neutral messages and supported IM adapters."""

from trpc_service.channels.models import ChannelMessage
from trpc_service.channels.processing import ChannelProcessor, ChannelProcessResult
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom import WeComAdapter

__all__ = [
    "ChannelMessage",
    "ChannelProcessResult",
    "ChannelProcessor",
    "TelegramAdapter",
    "WeComAdapter",
]
