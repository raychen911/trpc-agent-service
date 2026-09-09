"""First-party, tenant-safe IM channel contracts and adapters."""

from trpc_service.channels.contracts import (
    AttachmentKind,
    AttachmentRef,
    CallbackKind,
    CallbackRequest,
    Channel,
    ChannelAdapter,
    ConversationKind,
    NormalizedInbound,
    ReplyIntent,
    ReplyKind,
    SensitiveReplyRoute,
    SensitiveReplyRouteKind,
    SensitiveRouteChannelAdapter,
    TrustedBindingContext,
    VerifiedCallback,
)
from trpc_service.channels.session import ChannelIdentityDeriver, DerivedChannelIdentity
from trpc_service.channels.telegram import (
    TelegramAdapter,
    TelegramAuthenticationError,
    TelegramCallbackError,
    TelegramProtocolError,
)
from trpc_service.channels.wecom import (
    WeComAdapter,
    WeComCallbackError,
    WeComCrypto,
    WeComCryptoError,
    WeComProtocolError,
    WeComSignatureError,
)

__all__ = [
    "AttachmentKind",
    "AttachmentRef",
    "CallbackKind",
    "CallbackRequest",
    "Channel",
    "ChannelAdapter",
    "ChannelIdentityDeriver",
    "ConversationKind",
    "DerivedChannelIdentity",
    "NormalizedInbound",
    "ReplyIntent",
    "ReplyKind",
    "SensitiveReplyRoute",
    "SensitiveReplyRouteKind",
    "SensitiveRouteChannelAdapter",
    "TelegramAdapter",
    "TelegramAuthenticationError",
    "TelegramCallbackError",
    "TelegramProtocolError",
    "TrustedBindingContext",
    "VerifiedCallback",
    "WeComAdapter",
    "WeComCallbackError",
    "WeComCrypto",
    "WeComCryptoError",
    "WeComProtocolError",
    "WeComSignatureError",
]
