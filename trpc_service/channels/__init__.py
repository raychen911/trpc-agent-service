"""IM 通道适配层。"""
from trpc_service.channels.base import ChannelAdapter  # noqa: F401
from trpc_service.channels.base import WebhookRequest  # noqa: F401
from trpc_service.channels.base import WebhookResponse  # noqa: F401
from trpc_service.channels.feishu import FeishuAdapter  # noqa: F401
from trpc_service.channels.wecom import WeComAdapter  # noqa: F401
