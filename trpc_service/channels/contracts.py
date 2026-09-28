"""Stable contracts shared by messaging-channel implementations."""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class MessageKind(StrEnum):
    """Provider-neutral message kinds understood by the Agent runtime."""

    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    CARD = "card"
    EVENT = "event"


@dataclass(frozen=True, slots=True)
class ChannelBindingConfig:
    """Provider-neutral binding snapshot passed to one channel adapter."""

    binding_id: UUID
    tenant_id: UUID
    agent_app_id: UUID
    channel_type: str
    account_config: Mapping[str, object] = field(default_factory=dict)
    secret_ref_map: Mapping[str, str] = field(default_factory=dict)
    capabilities: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IncomingEnvelope:
    """Raw provider callback before signature checks and normalization."""

    binding_public_id: str
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    query: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    """Provider-neutral message produced by a concrete channel adapter."""

    external_message_id: str
    principal_id: str
    conversation_id: str
    kind: MessageKind
    occurred_at: datetime
    text: str | None = None
    artifact_refs: tuple[str, ...] = ()
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    """Provider-neutral reply consumed by a concrete channel adapter."""

    delivery_id: str
    conversation_id: str
    kind: MessageKind
    text: str | None = None
    artifact_refs: tuple[str, ...] = ()
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    """Channel acknowledgement stored after a delivery attempt is accepted."""

    delivery_id: str
    external_delivery_id: str
    accepted_at: datetime


@dataclass(frozen=True, slots=True)
class ChannelResponse:
    """HTTP response returned quickly to a provider webhook callback."""

    status_code: int = 200
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


class ChannelAdapter(ABC):
    """Abstract base inherited by every IM or Web implementation."""

    @property
    @abstractmethod
    def channel_type(self) -> str:
        """Return the unique provider type used for adapter registration."""

        ...

    @abstractmethod
    async def decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> IncomingMessage:
        """Verify and normalize one provider callback."""

        ...

    @abstractmethod
    async def acknowledge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse:
        """Build the provider-specific acknowledgement for an accepted callback."""

        ...

    async def verify_challenge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse | None:
        """Handle an optional webhook handshake; ordinary callbacks return None."""

        # Only providers with a callback challenge, such as WeCom, override this
        # hook. Keeping it optional avoids forcing unrelated IMs to fake support.
        return None

    @abstractmethod
    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        """Convert and send one provider-neutral reply."""

        ...
