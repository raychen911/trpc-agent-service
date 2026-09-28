"""Concrete IM adapters live here and are registered explicitly at startup."""

from trpc_service.channels.adapters.wecom import WeComChannelAdapter
from trpc_service.channels.adapters.feishu import FeishuChannelAdapter, FeishuTransportRegistry

__all__ = [
    "FeishuChannelAdapter",
    "FeishuTransportRegistry",
    "WeComChannelAdapter",
]
