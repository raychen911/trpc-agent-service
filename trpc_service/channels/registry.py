"""
Channel Adapter Registry and Factory.
"""

from typing import Dict, Optional
from trpc_service.config.models import ChannelType
from trpc_service.channels.base import BaseChannelAdapter
from trpc_service.channels.wechat_work import WeChatWorkChannelAdapter
from trpc_service.channels.telegram import TelegramChannelAdapter
from trpc_service.channels.wechat_kf import WeChatKFChannelAdapter


class ChannelRegistry:
    """Registry maintaining active channel adapter instances."""

    def __init__(self):
        self._adapters: Dict[ChannelType, BaseChannelAdapter] = {}
        # Register standard built-in adapters
        self.register_adapter(ChannelType.WECOM, WeChatWorkChannelAdapter())
        self.register_adapter(ChannelType.TELEGRAM, TelegramChannelAdapter())
        self.register_adapter(ChannelType.WECHAT_KF, WeChatKFChannelAdapter())

    def register_adapter(self, channel_type: ChannelType, adapter: BaseChannelAdapter):
        self._adapters[channel_type] = adapter

    def get_adapter(self, channel_type: ChannelType) -> Optional[BaseChannelAdapter]:
        return self._adapters.get(channel_type)


channel_registry = ChannelRegistry()
