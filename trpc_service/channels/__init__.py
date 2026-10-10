"""IM channel adapters."""

from trpc_service.channels.base import (
    ChannelAdapter,
    ChannelCapabilities,
    StreamingChannelConnector,
    WebhookChannelAdapter,
    WebhookRequest,
)
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    OutboundEnvelope,
    PayloadKind,
    RecallEnvelope,
    VerifiedCallback,
)
from trpc_service.channels.feishu import (
    FeishuAdapter,
    FeishuCallback,
    FeishuMediaError,
    FeishuResource,
    FeishuResourceError,
    FeishuVerificationError,
)

__all__ = [
    "ChannelAdapter",
    "ChannelCapabilities",
    "DeliveryReceipt",
    "DeliveryStatus",
    "FeishuAdapter",
    "FeishuCallback",
    "FeishuMediaError",
    "FeishuResource",
    "FeishuResourceError",
    "FeishuVerificationError",
    "InboundEnvelope",
    "OutboundEnvelope",
    "PayloadKind",
    "RecallEnvelope",
    "StreamingChannelConnector",
    "VerifiedCallback",
    "WebhookChannelAdapter",
    "WebhookRequest",
]
