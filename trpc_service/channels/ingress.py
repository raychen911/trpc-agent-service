"""Channel ingress contract owned by the channels layer.

Platform adapters (WeCom / Feishu / webhook) depend ONLY on this contract;
the Gateway provides the implementation (``ChannelIngressService``).  The
dependency therefore points from the implementation (gateway) to the
contract (channels) — adapters never import the gateway package.

``stream`` is the required member.  ``record_external_delivery`` is an
OPTIONAL capability (external IM services only): adapters keep a defensive
lookup so console-style ingresses without delivery audit still work.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from trpc_service.channels.delivery import ChannelExecutionStream
from trpc_service.channels.models import InboundMessage


class ChannelIngress(Protocol):
    """Ingress boundary every channel adapter talks to.

    ``stream`` admits one inbound message and returns the public event
    stream plus internal task identity (used by bound IM services for
    delivery audit; never serialized to the platform).
    """

    def stream(self, inbound: InboundMessage) -> ChannelExecutionStream:
        ...


@runtime_checkable
class ExternalDeliveryRecorder(Protocol):
    """Optional delivery-audit capability of an ingress implementation.

    Appends exactly one IM SDK-terminal delivery fact.  Audit failures are
    isolated from the reply chain: they can never cause a resend or a
    second Worker execution.  Adapters must tolerate an ingress without
    this capability (``getattr`` lookup, not an assumption).
    """

    def record_external_delivery(self, execution: ChannelExecutionStream, error_code) -> object:
        ...


__all__ = [
    "ChannelIngress",
    "ExternalDeliveryRecorder",
]
