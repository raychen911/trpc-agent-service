# ===================================================================
# channels - IM Channel Adapter（平台层新增）
# ===================================================================
# 说明: 每类 IM 一个适配器（PRD 3.1），抽象一个 IM 类，适配
#   企业微信 / 飞书 / Web UI（自测）。外部 IM 消息 -> AgentEvent，
#   AgentResponse -> IM 回复（PRD 3.2）。
# 规范: 验签 / 去重 / 消息转换在适配器完成，治理交给 Filter 链。
# ===================================================================

from .base import IMAdapter, ParsedWebhook, PlatformLimits
from .factory import ChannelFactory
from .feishu import FeishuAdapter
from .feishu_sdk import CHANNEL_TYPE as FEISHU_SDK_CHANNEL_TYPE
from .feishu_sdk import FeishuSdkConnector
from .web import WebImAdapter
from .wechat_work import WechatWorkAdapter
from .wecom_bot import CHANNEL_TYPE as WECOM_BOT_CHANNEL_TYPE
from .wecom_bot import WecomBotConnector

__all__ = [
    "ChannelFactory",
    "IMAdapter",
    "ParsedWebhook",
    "PlatformLimits",
    "FeishuAdapter",
    "FeishuSdkConnector",
    "FEISHU_SDK_CHANNEL_TYPE",
    "WebImAdapter",
    "WechatWorkAdapter",
    "WecomBotConnector",
    "WECOM_BOT_CHANNEL_TYPE",
]
