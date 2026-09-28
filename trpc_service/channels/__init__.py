"""External messaging channel domain exports."""

from trpc_service.channels.contracts import (
    ChannelAdapter,
    ChannelBindingConfig,
    ChannelResponse,
    DeliveryReceipt,
    IncomingEnvelope,
    IncomingMessage,
    MessageKind,
    OutgoingMessage,
)
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.registry import (
    ChannelAdapterAlreadyRegistered,
    ChannelAdapterNotFound,
    ChannelAdapterRegistry,
)
from trpc_service.channels.schemas import (
    ChannelBindingCreate,
    ChannelBindingRead,
    ChannelBindingUpdate,
)

__all__ = [
    "ChannelAdapter",
    "ChannelAdapterAlreadyRegistered",
    "ChannelAdapterNotFound",
    "ChannelAdapterRegistry",
    "ChannelBinding",
    "ChannelBindingConfig",
    "ChannelBindingCreate",
    "ChannelBindingRead",
    "ChannelBindingUpdate",
    "ChannelResponse",
    "DeliveryReceipt",
    "IncomingEnvelope",
    "IncomingMessage",
    "MessageKind",
    "OutgoingMessage",
]
