"""IM Channel Adapter implementations."""

from .base import ChannelAdapter
from .base import ChannelAuthenticationError
from .base import DeliveryResult
from .base import UnsupportedMessageError
from .base import ChannelTransportError
from .base import InboundAttachmentDownloader
from .wecom import WeComChannelAdapter
from .wecom_runtime import FakeWeComClient
from .wecom_runtime import AibotWeComClient
from .wecom_runtime import WeComChannelRuntime
from .wecom_runtime import WeComClient
from .customer_service import CustomerServiceAdapter
from .customer_service import FakeCustomerServiceClient
from .customer_service import HttpCustomerServiceClient
from .telegram import TelegramChannelAdapter

__all__ = [
    "ChannelAdapter",
    "ChannelAuthenticationError",
    "DeliveryResult",
    "UnsupportedMessageError",
    "ChannelTransportError",
    "InboundAttachmentDownloader",
    "WeComChannelAdapter",
    "FakeWeComClient",
    "AibotWeComClient",
    "WeComChannelRuntime",
    "WeComClient",
    "CustomerServiceAdapter",
    "FakeCustomerServiceClient",
    "HttpCustomerServiceClient",
    "TelegramChannelAdapter",
]
