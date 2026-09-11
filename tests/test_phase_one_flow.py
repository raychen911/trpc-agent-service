"""First-phase two-tenant callback-to-Runner acceptance tests."""

from datetime import datetime
from datetime import timezone

import pytest

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.agent import AgentChannelEventType
from trpc_service.agent import RunnerRegistry
from trpc_service.agent import run_normalized_message
from trpc_service.agent import tenant_app_name
from trpc_service.channels import CallbackVerificationError
from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback

from .conftest import build_tenant


class FakeAdapter:
    def __init__(self, message: NormalizedInboundMessage, *, signature_valid: bool = True):
        self.message = message
        self.signature_valid = signature_valid
        self.calls = []

    async def verify_callback(self, request, binding):
        self.calls.append(("verify", binding.tenant_id))
        if not self.signature_valid:
            raise CallbackVerificationError("invalid callback signature")

    async def parse_inbound(self, request, binding):
        self.calls.append(("parse", binding.tenant_id))
        return self.message


class FakeRunner:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.calls = []

    async def run_async(self, **kwargs):
        self.calls.append(kwargs)
        yield Event(id="thought", content=Content(parts=[Part(text="private reasoning", thought=True)]))
        yield Event(
            id="tool_call",
            content=Content(parts=[Part.from_function_call(name="search_order", args={"token": "secret-input"})]),
        )
        yield Event(
            id="tool_result",
            content=Content(
                parts=[Part.from_function_response(name="search_order", response={"token": "secret-output"})]),
        )
        yield Event(id="final", content=Content(parts=[Part.from_text(text="Safe final answer")]))


def inbound(webhook_public_id: str, request_id: str) -> NormalizedInboundMessage:
    return NormalizedInboundMessage(
        channel=ChannelType.WECOM,
        webhook_public_id=webhook_public_id,
        external_message_id=f"message_{request_id}",
        external_user_id="same_external_user",
        conversation_type=ConversationType.DIRECT,
        text="Where is my order?",
        received_at=datetime.now(timezone.utc),
        request_id=request_id,
    )


@pytest.mark.asyncio
async def test_two_tenants_run_only_their_exact_runner_with_safe_output():
    tenant_a = build_tenant("tenant_acme", "callback_acme")
    tenant_b = build_tenant("tenant_beta", "callback_beta")
    tenants = [tenant_a, tenant_b]
    binding_registry = ChannelBindingRegistry(tenants)
    identity_keys = {tenant.tenant_id: tenant.tenant_id.encode().ljust(32, b"_") for tenant in tenants}
    router = MessageRouter(binding_registry, SessionIdentityFactory(identity_keys.__getitem__))
    runner_registry = RunnerRegistry()
    runner_a = FakeRunner(tenant_app_name("tenant_acme", "customer_support"))
    runner_b = FakeRunner(tenant_app_name("tenant_beta", "customer_support"))
    runner_registry.register("tenant_acme", "customer_support", 3, runner_a)
    runner_registry.register("tenant_beta", "customer_support", 3, runner_b)

    results = []
    for webhook_id, request_id in (("callback_acme", "request_a"), ("callback_beta", "request_b")):
        adapter = FakeAdapter(inbound(webhook_id, request_id))
        message, route = await verify_parse_and_route_callback(
            adapter=adapter,
            raw_request=object(),
            channel=ChannelType.WECOM,
            webhook_public_id=webhook_id,
            registry=binding_registry,
            router=router,
        )
        events = [
            event
            async for event in run_normalized_message(
                runner_registry=runner_registry,
                route=route,
                message=message,
            )
        ]
        results.append((adapter, route, events))

    adapter_a, route_a, events_a = results[0]
    adapter_b, route_b, events_b = results[1]
    assert adapter_a.calls == [("verify", "tenant_acme"), ("parse", "tenant_acme")]
    assert adapter_b.calls == [("verify", "tenant_beta"), ("parse", "tenant_beta")]
    assert route_a.internal_user_id != route_b.internal_user_id
    assert route_a.session_id != route_b.session_id
    assert len(runner_a.calls) == len(runner_b.calls) == 1
    assert runner_a.calls[0]["user_id"] == route_a.internal_user_id
    assert runner_b.calls[0]["user_id"] == route_b.internal_user_id
    assert runner_a.calls[0]["agent_context"].metadata["tenant_id"] == "tenant_acme"
    assert runner_b.calls[0]["agent_context"].metadata["tenant_id"] == "tenant_beta"
    assert [event.type for event in events_a] == [
        AgentChannelEventType.TOOL_CALL,
        AgentChannelEventType.TOOL_RESULT,
        AgentChannelEventType.TEXT,
    ]
    assert events_a[-1].text == events_b[-1].text == "Safe final answer"
    assert "private reasoning" not in repr((events_a, events_b))
    assert "secret-input" not in repr((events_a, events_b))
    assert "secret-output" not in repr((events_a, events_b))


@pytest.mark.asyncio
async def test_failed_signature_never_calls_a_runner():
    tenant = build_tenant()
    binding_registry = ChannelBindingRegistry([tenant])
    router = MessageRouter(binding_registry, SessionIdentityFactory(lambda _: b"a" * 32))
    runner_registry = RunnerRegistry()
    runner = FakeRunner(tenant_app_name("tenant_acme", "customer_support"))
    runner_registry.register("tenant_acme", "customer_support", 3, runner)
    adapter = FakeAdapter(inbound("callback_acme", "request_bad"), signature_valid=False)

    with pytest.raises(CallbackVerificationError, match="invalid callback signature"):
        await verify_parse_and_route_callback(
            adapter=adapter,
            raw_request=object(),
            channel=ChannelType.WECOM,
            webhook_public_id="callback_acme",
            registry=binding_registry,
            router=router,
        )

    assert adapter.calls == [("verify", "tenant_acme")]
    assert runner.calls == []
