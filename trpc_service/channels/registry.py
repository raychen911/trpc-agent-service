from trpc_service.channels.base import ChannelAdapter
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.web import WebAdapter
from trpc_service.channels.wechat_customer_service import (
    WeChatCustomerServiceAdapter,
)
from trpc_service.channels.wechat_official_account import (
    WeChatOfficialAccountAdapter,
)
from trpc_service.channels.wecom import WeComAdapter


def default_channel_adapters() -> dict[str, ChannelAdapter]:
    adapters = [
        WebAdapter(),
        WeComAdapter(),
        WeChatCustomerServiceAdapter(),
        WeChatOfficialAccountAdapter(),
        TelegramAdapter(),
    ]
    return {adapter.channel_name: adapter for adapter in adapters}
