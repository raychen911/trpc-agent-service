"""
Base IM Channel Adapter Interface.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import uuid
import logging
from trpc_service.config.models import (
    ChannelType,
    ChannelBindingConfig,
    InboundMessage,
    OutboundMessage,
)

logger = logging.getLogger(__name__)


class BaseChannelAdapter(ABC):
    """
    Abstract Channel Adapter to normalize heterogeneous IM messaging protocols.
    """

    channel_type: ChannelType

    @abstractmethod
    def verify_signature(
        self,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        token: Optional[str] = None,
        secret: Optional[str] = None,
    ) -> bool:
        """Verify message authenticity and integrity from IM provider."""
        pass

    @abstractmethod
    def parse_inbound_message(
        self,
        tenant_id: str,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        aes_key: Optional[str] = None,
    ) -> InboundMessage:
        """Parse raw incoming HTTP request into standard InboundMessage."""
        pass

    @abstractmethod
    def format_outbound_payload(
        self,
        outbound: OutboundMessage,
    ) -> Dict[str, Any]:
        """Convert standard OutboundMessage into IM-specific payload dictionary."""
        pass

    @abstractmethod
    async def send_message(
        self,
        outbound: OutboundMessage,
        binding: ChannelBindingConfig,
    ) -> bool:
        """Actively push message to the IM platform via HTTP API."""
        pass

    def split_long_message(self, text: str, max_chars: int) -> List[str]:
        """Split long messages into chunks respecting sentence boundaries."""
        if len(text) <= max_chars:
            return [text]

        chunks = []
        curr = ""
        lines = text.split("\n")
        for line in lines:
            if len(curr) + len(line) + 1 <= max_chars:
                curr += ("\n" if curr else "") + line
            else:
                if curr:
                    chunks.append(curr)
                # If single line is larger than max_chars, split by slice
                while len(line) > max_chars:
                    chunks.append(line[:max_chars])
                    line = line[max_chars:]
                curr = line
        if curr:
            chunks.append(curr)
        return chunks

    def generate_trace_id(self) -> str:
        return uuid.uuid4().hex[:16]
