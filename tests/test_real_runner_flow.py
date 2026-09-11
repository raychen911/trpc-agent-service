"""End-to-end tests using the real tRPC-Agent Runner and in-memory sessions."""

from datetime import datetime
from datetime import timezone
from typing import List

import pytest

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.agent import AgentChannelEventType
from trpc_service.agent import RunnerRegistry
from trpc_service.agent import run_normalized_message
from trpc_service.agent import select_tools_for_runner
from trpc_service.agent import tenant_app_name
from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback

from .conftest import build_tenant


class DeterministicOrderModel(LLMModel):
    """Request a real tool call, then return deterministic final text."""

    def __init__(self, tenant_id: str, config_version: int):
        super().__init__(model_name=f"deterministic-{tenant_id}-v{config_version}")
        self.tenant_id = tenant_id
        self.config_version = config_version
        self.requests = []

    @classmethod
    def supported_models(cls) -> List[str]:
        return [r"deterministic-.*"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.requests.append(request.model_copy(deep=True))
        last_content = request.contents[-1]
        has_tool_result = any(part.function_response for part in last_content.parts)
        if has_tool_result:
            tenant_id = ctx.agent_context.get_metadata("tenant_id")
            request_id = ctx.agent_context.get_metadata("request_id")
            yield LlmResponse(
                content=Content(
                    role="model",
                    parts=[Part.from_text(text=f"{tenant_id} completed {request_id}")],
                ),
                partial=False,
            )
            return

        call = Part.from_function_call(name="search_order", args={"order_id": "order-42"})
        call.function_call.id = f"call-{len(self.requests)}"
        yield LlmResponse(content=Content(role="model", parts=[call]), partial=False)

    def validate_request(self, request):
        super().validate_request(request)


class SensitiveErrorModel(LLMModel):
    """Return provider details that must not cross the Channel boundary."""

    @classmethod
    def supported_models(cls) -> List[str]:
        return [r"sensitive-error-.*"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        yield LlmResponse(
            error_code="provider_timeout_with_internal_details",
            error_message="private API key and provider stack trace",
        )

    def validate_request(self, request):
        super().validate_request(request)


class FakeAdapter:
    def __init__(self, message: NormalizedInboundMessage):
        self.message = message
        self.calls = []

    async def verify_callback(self, request, binding):
        self.calls.append(("verify", binding.tenant_id))

    async def parse_inbound(self, request, binding):
        self.calls.append(("parse", binding.tenant_id))
        return self.message


def inbound(webhook_public_id: str, request_id: str, text: str) -> NormalizedInboundMessage:
    return NormalizedInboundMessage(
        channel=ChannelType.WECOM,
        webhook_public_id=webhook_public_id,
        external_message_id=f"message_{request_id}",
        external_user_id="same_external_user",
        conversation_type=ConversationType.DIRECT,
        text=text,
        received_at=datetime.now(timezone.utc),
        request_id=request_id,
    )


def build_real_runner(tenant, session_service, tool_calls):
    async def search_order(order_id: str, tool_context: InvocationContext) -> dict:
        """Search one tenant's order system."""

        metadata = tool_context.agent_context.metadata
        tool_calls.append({
            "tenant_id": metadata["tenant_id"],
            "config_version": metadata["config_version"],
            "request_id": metadata["request_id"],
            "user_id": tool_context.user_id,
            "session_id": tool_context.session.id,
            "order_id": order_id,
        })
        return {
            "status": "shipped",
            "backend_secret": f"{metadata['tenant_id']}-private-result",
        }

    async def execute_refund(order_id: str) -> dict:
        """Execute a refund that requires confirmation."""

        raise AssertionError("confirmation-required tool must not be registered")

    async def internal_admin() -> dict:
        """Run an internal-only operation."""

        raise AssertionError("default-denied tool must not be registered")

    available_tools = tuple(
        FunctionTool(tool)
        for tool in (search_order, execute_refund, internal_admin)
    )
    application = tenant.applications[0]
    selected_tools = select_tools_for_runner(available_tools, application.tool_policy)
    model = DeterministicOrderModel(tenant.tenant_id, tenant.config_version)
    agent = LlmAgent(
        name=application.agent_name,
        model=model,
        instruction=application.instruction,
        tools=list(selected_tools),
    )
    runner = Runner(
        app_name=tenant_app_name(tenant.tenant_id, application.app_id),
        agent=agent,
        session_service=session_service,
        enable_post_turn_processing=False,
        close_session_service_on_close=False,
    )
    return runner, model, selected_tools


async def execute_callback(adapter, binding_registry, router, runner_registry):
    message, route = await verify_parse_and_route_callback(
        adapter=adapter,
        raw_request=object(),
        channel=adapter.message.channel,
        webhook_public_id=adapter.message.webhook_public_id,
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
    return route, events


def session_texts(session):
    return [
        part.text
        for event in session.events
        if event.content
        for part in event.content.parts
        if part.text
    ]


@pytest.mark.asyncio
async def test_real_runner_keeps_two_tenants_tools_and_multiturn_sessions_isolated():
    tenant_a = build_tenant("tenant_acme", "callback_acme")
    tenant_b = build_tenant("tenant_beta", "callback_beta")
    tenants = [tenant_a, tenant_b]
    binding_registry = ChannelBindingRegistry(tenants)
    identity_keys = {tenant.tenant_id: tenant.tenant_id.encode().ljust(32, b"_") for tenant in tenants}
    router = MessageRouter(binding_registry, SessionIdentityFactory(identity_keys.__getitem__))
    session_service = InMemorySessionService()
    runner_registry = RunnerRegistry()
    tool_calls = []

    runner_a, model_a, tools_a = build_real_runner(tenant_a, session_service, tool_calls)
    runner_b, model_b, tools_b = build_real_runner(tenant_b, session_service, tool_calls)
    runner_registry.register("tenant_acme", "customer_support", 3, runner_a)
    runner_registry.register("tenant_beta", "customer_support", 3, runner_b)

    adapters = [
        FakeAdapter(inbound("callback_acme", "acme_1", "Acme first question")),
        FakeAdapter(inbound("callback_beta", "beta_1", "Beta first question")),
        FakeAdapter(inbound("callback_acme", "acme_2", "Acme second question")),
        FakeAdapter(inbound("callback_beta", "beta_2", "Beta second question")),
    ]
    results = [
        await execute_callback(adapter, binding_registry, router, runner_registry)
        for adapter in adapters
    ]

    route_a = results[0][0]
    route_b = results[1][0]
    assert route_a.internal_user_id != route_b.internal_user_id
    assert route_a.session_id != route_b.session_id
    assert [tool.name for tool in tools_a] == ["search_order"]
    assert [tool.name for tool in tools_b] == ["search_order"]
    assert len(model_a.requests) == len(model_b.requests) == 4
    assert len(tool_calls) == 4
    assert {call["request_id"] for call in tool_calls} == {"acme_1", "beta_1", "acme_2", "beta_2"}
    expected_tenants = {
        "acme_1": "tenant_acme",
        "acme_2": "tenant_acme",
        "beta_1": "tenant_beta",
        "beta_2": "tenant_beta",
    }
    assert all(call["tenant_id"] == expected_tenants[call["request_id"]] for call in tool_calls)

    for adapter, (_, events) in zip(adapters, results):
        tenant_id = "tenant_acme" if "acme" in adapter.message.request_id else "tenant_beta"
        assert adapter.calls == [("verify", tenant_id), ("parse", tenant_id)]
        assert [event.type for event in events] == [
            AgentChannelEventType.TOOL_CALL,
            AgentChannelEventType.TOOL_RESULT,
            AgentChannelEventType.TEXT,
        ]
        assert events[-1].text == f"{tenant_id} completed {adapter.message.request_id}"
        assert "order-42" not in repr(events)
        assert "private-result" not in repr(events)

    session_a = await session_service.get_session(
        app_name=runner_a.app_name,
        user_id=route_a.internal_user_id,
        session_id=route_a.session_id,
    )
    session_b = await session_service.get_session(
        app_name=runner_b.app_name,
        user_id=route_b.internal_user_id,
        session_id=route_b.session_id,
    )
    assert session_a.conversation_count == session_b.conversation_count == 2
    assert "Acme first question" in session_texts(session_a)
    assert "Acme second question" in session_texts(session_a)
    assert "Beta first question" not in session_texts(session_a)
    assert "Beta first question" in session_texts(session_b)
    assert "Beta second question" in session_texts(session_b)
    assert "Acme first question" not in session_texts(session_b)

    await runner_a.close()
    await runner_b.close()


@pytest.mark.asyncio
async def test_real_runner_projects_model_failure_without_provider_details():
    tenant = build_tenant()
    binding_registry = ChannelBindingRegistry([tenant])
    router = MessageRouter(binding_registry, SessionIdentityFactory(lambda _: b"a" * 32))
    session_service = InMemorySessionService()
    agent = LlmAgent(
        name="support_agent",
        model=SensitiveErrorModel(model_name="sensitive-error-model"),
        instruction="Return an error for testing.",
    )
    runner = Runner(
        app_name=tenant_app_name("tenant_acme", "customer_support"),
        agent=agent,
        session_service=session_service,
        enable_post_turn_processing=False,
    )
    runner_registry = RunnerRegistry()
    runner_registry.register("tenant_acme", "customer_support", 3, runner)
    adapter = FakeAdapter(inbound("callback_acme", "error_1", "Trigger the model"))

    _, events = await execute_callback(adapter, binding_registry, router, runner_registry)

    assert len(events) == 1
    assert events[0].type is AgentChannelEventType.ERROR
    assert events[0].text == "Agent execution failed"
    assert events[0].error_type == "agent_error"
    assert "private API key" not in repr(events)
    assert "provider_timeout" not in repr(events)
    await runner.close()


@pytest.mark.asyncio
async def test_real_runner_rollout_and_rollback_keep_the_same_session_namespace():
    tenant_v3 = build_tenant()
    tenant_v4_data = tenant_v3.model_dump()
    tenant_v4_data["config_version"] = 4
    tenant_v4 = type(tenant_v3).model_validate(tenant_v4_data)
    registry_v3 = ChannelBindingRegistry([tenant_v3])
    registry_v4 = ChannelBindingRegistry([tenant_v4])
    identities = SessionIdentityFactory(lambda _: b"a" * 32)
    router_v3 = MessageRouter(registry_v3, identities)
    router_v4 = MessageRouter(registry_v4, identities)
    session_service = InMemorySessionService()
    runner_registry = RunnerRegistry()
    tool_calls = []
    runner_v3, model_v3, _ = build_real_runner(tenant_v3, session_service, tool_calls)
    runner_v4, model_v4, _ = build_real_runner(tenant_v4, session_service, tool_calls)
    runner_registry.register("tenant_acme", "customer_support", 3, runner_v3)
    runner_registry.register("tenant_acme", "customer_support", 4, runner_v4)

    route_v3, _ = await execute_callback(
        FakeAdapter(inbound("callback_acme", "version_3", "Run version three")),
        registry_v3,
        router_v3,
        runner_registry,
    )
    route_v4, _ = await execute_callback(
        FakeAdapter(inbound("callback_acme", "version_4", "Run version four")),
        registry_v4,
        router_v4,
        runner_registry,
    )
    route_rollback, _ = await execute_callback(
        FakeAdapter(inbound("callback_acme", "rollback_3", "Roll back to version three")),
        registry_v3,
        router_v3,
        runner_registry,
    )

    assert route_v3.config_version == route_rollback.config_version == 3
    assert route_v4.config_version == 4
    assert route_v3.internal_user_id == route_v4.internal_user_id == route_rollback.internal_user_id
    assert route_v3.session_id == route_v4.session_id == route_rollback.session_id
    assert runner_v3.app_name == runner_v4.app_name == "tenant_acme:customer_support"
    assert len(model_v3.requests) == 4
    assert len(model_v4.requests) == 2
    assert [call["config_version"] for call in tool_calls] == [3, 4, 3]

    session = await session_service.get_session(
        app_name=runner_v3.app_name,
        user_id=route_v3.internal_user_id,
        session_id=route_v3.session_id,
    )
    assert session.conversation_count == 3
    assert "Run version three" in session_texts(session)
    assert "Run version four" in session_texts(session)
    assert "Roll back to version three" in session_texts(session)
    await runner_v3.close()
    await runner_v4.close()
