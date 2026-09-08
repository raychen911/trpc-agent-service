# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""IM channel adapters (企业微信 / 微信客服 / 钉钉 / 飞书 / QQ)."""

from ._base import ChannelAdapter
from ._models import CHAT_GROUP
from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._models import SendResult
from ._models import generate_session_id
from ._models import split_text
from ._models import split_text_bytes
from ._dingtalk import DingTalkAdapter
from ._feishu import FeishuAdapter
from ._qq import QQAdapter
from ._wecom import WecomAdapter
from ._wechat_kf import WechatCustomerServiceAdapter
from ._delivery import ChannelDeliveryTransport

__all__ = [
    "CHAT_GROUP",
    "CHAT_PRIVATE",
    "ChannelAdapter",
    "ChannelDeliveryTransport",
    "InboundMessage",
    "OutboundMessage",
    "SendResult",
    "DingTalkAdapter",
    "FeishuAdapter",
    "QQAdapter",
    "WecomAdapter",
    "WechatCustomerServiceAdapter",
    "generate_session_id",
    "split_text",
    "split_text_bytes",
]
