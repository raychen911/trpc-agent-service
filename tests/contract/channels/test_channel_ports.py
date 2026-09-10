from __future__ import annotations

import inspect

from trpc_service.channels.base import ChannelAdapterPort, ProviderClientPort
from trpc_service.storage.contracts import (
    AdapterOwnershipRepository,
    DeliveryRepository,
)


def _assert_async_methods(port: type, *names: str) -> None:
    assert getattr(port, "_is_runtime_protocol", False) is True
    for name in names:
        assert inspect.iscoroutinefunction(getattr(port, name)), f"{port.__name__}.{name} must be async"


def test_provider_and_adapter_ports_are_runtime_async_protocols() -> None:
    _assert_async_methods(ProviderClientPort, "authenticate", "connect", "close", "send_text")
    _assert_async_methods(ChannelAdapterPort, "start", "stop", "handle_provider_event", "deliver")
    assert callable(getattr(ProviderClientPort, "connection_state"))
    assert callable(getattr(ChannelAdapterPort, "readiness"))


def test_delivery_and_ownership_ports_are_runtime_async_protocols() -> None:
    _assert_async_methods(
        DeliveryRepository,
        "create_or_get",
        "begin_attempt",
        "finish_attempt",
        "get",
        "list_due",
    )
    _assert_async_methods(AdapterOwnershipRepository, "acquire", "inspect")
