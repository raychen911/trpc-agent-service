from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from trpc_service.channels import (
    ChannelAdapter,
    ChannelAdapterAlreadyRegistered,
    ChannelAdapterNotFound,
    ChannelAdapterRegistry,
    ChannelBindingConfig,
    ChannelResponse,
    DeliveryReceipt,
    IncomingEnvelope,
    IncomingMessage,
    MessageKind,
    OutgoingMessage,
)
from trpc_service.channels.schemas import ChannelBindingCreate


class StubChannelAdapter(ChannelAdapter):
    """Small concrete adapter used to verify the public extension contract."""

    channel_type = "custom_im"

    async def decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> IncomingMessage:
        return IncomingMessage(
            external_message_id="message-1",
            principal_id="user-1",
            conversation_id="conversation-1",
            kind=MessageKind.TEXT,
            text=envelope.body.decode(),
            occurred_at=datetime.now(timezone.utc),
        )

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            delivery_id=message.delivery_id,
            external_delivery_id=f"{binding.binding_id}:{message.delivery_id}",
            accepted_at=datetime.now(timezone.utc),
        )

    async def acknowledge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse:
        return ChannelResponse(status_code=200, body=b"accepted")


def test_channel_registry_resolves_a_concrete_adapter() -> None:
    registry = ChannelAdapterRegistry()
    adapter = StubChannelAdapter()

    registry.register(adapter)

    assert registry.resolve("CUSTOM_IM") is adapter
    assert registry.supported_types == ("custom_im", )


def test_channel_adapter_requires_concrete_methods() -> None:

    class IncompleteChannelAdapter(ChannelAdapter):
        channel_type = "incomplete"

    with pytest.raises(TypeError):
        IncompleteChannelAdapter()


def test_channel_registry_rejects_duplicate_and_unknown_adapters() -> None:
    registry = ChannelAdapterRegistry()
    registry.register(StubChannelAdapter())

    with pytest.raises(ChannelAdapterAlreadyRegistered):
        registry.register(StubChannelAdapter())
    with pytest.raises(ChannelAdapterNotFound):
        registry.resolve("telegram")


def test_channel_registry_rejects_names_outside_the_binding_schema() -> None:

    class InvalidNameAdapter(StubChannelAdapter):
        channel_type = "custom-im"

    with pytest.raises(ValueError, match="invalid channel type"):
        ChannelAdapterRegistry().register(InvalidNameAdapter())


@pytest.mark.anyio
async def test_resolved_channel_adapter_executes_provider_code() -> None:
    registry = ChannelAdapterRegistry()
    registry.register(StubChannelAdapter())
    binding = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        channel_type="custom_im",
    )
    adapter = registry.resolve(binding.channel_type)

    incoming = await adapter.decode(
        IncomingEnvelope(binding_public_id="public-id", body=b"hello"),
        binding,
    )
    acknowledgement = await adapter.acknowledge(
        IncomingEnvelope(binding_public_id="public-id", body=b"hello"),
        binding,
    )
    receipt = await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-1",
            conversation_id=incoming.conversation_id,
            kind=MessageKind.TEXT,
            text="world",
        ),
        binding,
    )

    assert incoming.text == "hello"
    assert acknowledgement.status_code == 200
    assert receipt.delivery_id == "delivery-1"


@pytest.mark.anyio
async def test_channel_adapter_has_an_optional_webhook_challenge_hook() -> None:
    adapter = StubChannelAdapter()
    binding = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        channel_type="custom_im",
    )

    response = await adapter.verify_challenge(
        IncomingEnvelope(binding_public_id="public-id", body=b""),
        binding,
    )

    assert response is None


def test_channel_binding_accepts_a_new_adapter_name_without_schema_changes() -> None:
    binding = ChannelBindingCreate(
        agent_app_id=uuid4(),
        channel_type=" Custom_IM ",
        external_account_hash="sha256:custom-account",
    )

    assert binding.channel_type == "custom_im"


def test_channel_binding_rejects_non_string_adapter_name_as_validation_error() -> None:
    with pytest.raises(ValidationError):
        ChannelBindingCreate.model_validate({
            "agent_app_id": uuid4(),
            "channel_type": 123,
            "external_account_hash": "sha256:custom-account",
        })
