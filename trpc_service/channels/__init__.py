"""Channel-neutral contracts for Web and third-party IM adapters."""

from .base import CallbackVerificationError
from .base import ChannelAdapter
from .base import verify_and_parse_callback
from .models import AttachmentRef
from .models import ChannelCapabilities
from .models import ChannelType
from .models import ConversationType
from .models import MessageType
from .models import NormalizedInboundMessage
from .models import OutboundMessage

__all__ = [
    "AttachmentRef",
    "CallbackVerificationError",
    "ChannelAdapter",
    "ChannelCapabilities",
    "ChannelType",
    "ConversationType",
    "MessageType",
    "NormalizedInboundMessage",
    "OutboundMessage",
    "verify_and_parse_callback",
]
