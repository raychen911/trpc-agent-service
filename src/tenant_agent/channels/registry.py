"""Fixed channel registry; tenant input cannot import arbitrary adapter code."""

from __future__ import annotations

from tenant_agent.channels.base import ChannelAdapter
from tenant_agent.channels.telegram import TelegramAdapter
from tenant_agent.channels.web import WebAdapter
from tenant_agent.channels.wecom import WeComAdapter
from tenant_agent.channels.wecom_bot import WeComBotAdapter
from tenant_agent.models import ChannelType


class ChannelRegistry:
    def __init__(
        self, *, delivery_timeout_seconds: float = 10.0, wecom_bot: WeComBotAdapter | None = None
    ) -> None:
        self._adapters: dict[ChannelType, ChannelAdapter] = {
            ChannelType.TELEGRAM: TelegramAdapter(timeout_seconds=delivery_timeout_seconds),
            ChannelType.WECOM: WeComAdapter(timeout_seconds=delivery_timeout_seconds),
            ChannelType.WECOM_BOT: wecom_bot or WeComBotAdapter(),
            ChannelType.WEB: WebAdapter(),
        }

    def get(self, channel: ChannelType) -> ChannelAdapter:
        return self._adapters[channel]
