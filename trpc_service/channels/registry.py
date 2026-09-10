import os

from trpc_service.channels.base import ChannelAdapter
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.web import WebAdapter
from trpc_service.channels.wecom_ai_bot import WeComAIBotAdapter


def default_channel_adapters() -> dict[str, ChannelAdapter]:
    adapters = [
        WebAdapter(),
        WeComAIBotAdapter(),
        FeishuAdapter(),
        TelegramAdapter(),
    ]
    if os.getenv("ENABLE_LEGACY_WECOM", "0").lower() in {"1", "true", "yes", "on"}:
        from trpc_service.channels.wecom import WeComAdapter

        adapters.append(WeComAdapter())
    return {adapter.channel_name: adapter for adapter in adapters}
