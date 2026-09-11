"""Tests that exercise the native tRPC-Agent Content and Event contracts."""

from datetime import datetime
from datetime import timezone

import pytest

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.agent import AgentChannelEventType
from trpc_service.agent import RunnerRegistry
from trpc_service.agent import RunnerRouteMismatchError
from trpc_service.agent import build_agent_context
from trpc_service.agent import build_user_content
from trpc_service.agent import project_agent_event
from trpc_service.agent import run_normalized_message
from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.tenant import ResolvedRoute


def inbound_message() -> NormalizedInboundMessage:
    return NormalizedInboundMessage(
        channel=ChannelType.WECOM,
        webhook_public_id="callback_acme",
        external_message_id="message_1",
        external_user_id="external_user_42",
        conversation_type=ConversationType.DIRECT,
        text="Where is my order?",
        received_at=datetime.now(timezone.utc),
        request_id="request_1",
    )


def route() -> ResolvedRoute:
    return ResolvedRoute(
        tenant_id="tenant_acme",
        config_version=3,
        storage_revision=4,
        agent_app_id="customer_support",
        channel_binding_id="binding_primary",
        actor_id="actor_internal",
        internal_user_id="usr_internal",
        session_id="ses_internal",
        partition_key="tenant_acme:ses_internal",
    )


def test_build_user_content_uses_native_trpc_types():
    content = build_user_content(inbound_message())

    assert isinstance(content, Content)
    assert content.role == "user"
    assert content.parts[0].text == "Where is my order?"


def test_project_agent_event_covers_stream_tool_and_final_text():
    delta = project_agent_event(Event(id="e1", partial=True, content=Content(parts=[Part.from_text(text="Checking")])))
    tool_call = project_agent_event(
        Event(id="e2", content=Content(parts=[Part.from_function_call(name="search_order", args={"id": "1"})])))
    tool_result = project_agent_event(
        Event(
            id="e3",
            content=Content(parts=[Part.from_function_response(name="search_order", response={"status": "sent"})]),
        ))
    final = project_agent_event(Event(id="e4", content=Content(parts=[Part.from_text(text="It has shipped.")])))

    assert delta[0].type is AgentChannelEventType.TEXT_DELTA
    assert tool_call[0].type is AgentChannelEventType.TOOL_CALL
    assert "data" not in tool_call[0].model_dump()
    assert tool_result[0].type is AgentChannelEventType.TOOL_RESULT
    assert "data" not in tool_result[0].model_dump()
    assert final[0].type is AgentChannelEventType.TEXT
    assert final[0].final is True


def test_thought_parts_are_not_projected_to_im():
    event = Event(id="e1", content=Content(parts=[Part(text="hidden reasoning", thought=True)]))
    assert project_agent_event(event) == ()


def test_internal_error_details_are_not_projected_to_im():
    event = Event(id="e1", error_code="database_password_exposed", error_message="secret stack trace")

    projected = project_agent_event(event)

    assert projected[0].text == "Agent execution failed"
    assert projected[0].error_type == "agent_error"
    assert "secret stack trace" not in repr(projected[0])


def test_agent_context_is_created_with_trusted_platform_metadata():
    context = build_agent_context(route(), inbound_message())

    assert context.metadata == {
        "tenant_id": "tenant_acme",
        "agent_app_id": "customer_support",
        "config_version": 3,
        "storage_revision": 4,
        "channel_binding_id": "binding_primary",
        "actor_id": "actor_internal",
        "memory_scope_id": "usr_internal",
        "session_id": route().session_id,
        "request_id": "request_1",
        "external_message_id": "message_1",
    }


def test_conflicting_platform_metadata_is_rejected():
    context = AgentContext()
    context.with_metadata("tenant_id", "tenant_other")

    with pytest.raises(ValueError, match="conflicts with trusted tenant_id"):
        build_agent_context(route(), inbound_message(), context)


@pytest.mark.asyncio
async def test_run_normalized_message_passes_platform_route_to_runner():
    class FakeRunner:
        def __init__(self):
            self.app_name = "tenant_acme:customer_support"
            self.call = None

        async def run_async(self, **kwargs):
            self.call = kwargs
            yield Event(id="e1", content=Content(parts=[Part.from_text(text="Done")]))

    runner = FakeRunner()
    registry = RunnerRegistry()
    registry.register("tenant_acme", "customer_support", 3, runner)
    events = [
        event
        async for event in run_normalized_message(
            runner_registry=registry,
            route=route(),
            message=inbound_message(),
        )
    ]

    assert runner.call["user_id"] == "usr_internal"
    assert runner.call["session_id"] == "ses_internal"
    assert runner.call["new_message"].parts[0].text == "Where is my order?"
    assert runner.call["agent_context"].metadata["tenant_id"] == "tenant_acme"
    assert runner.call["agent_context"].metadata["request_id"] == "request_1"
    assert events[0].text == "Done"


@pytest.mark.asyncio
async def test_runner_app_name_must_match_route():
    class FakeRunner:
        app_name = "tenant_other:customer_support"

        async def run_async(self, **kwargs):
            yield Event(id="never")

    registry = RunnerRegistry()
    registry.register("tenant_acme", "customer_support", 3, FakeRunner())

    with pytest.raises(RunnerRouteMismatchError, match="does not match"):
        _ = [
            event
            async for event in run_normalized_message(
                runner_registry=registry,
                route=route(),
                message=inbound_message(),
            )
        ]
