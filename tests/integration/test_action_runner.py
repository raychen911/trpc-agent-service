"""Actual upstream Runner stops after a proposal, without a second model request."""

import pytest
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.context import InvocationContext, AgentContext
from trpc_agent_sdk.models import LLMModel, LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part

from tests.reliability.test_actions import action_system as action_fixture, system as system_fixture
from tests.reliability.test_actions import spec, approve, worker
from trpc_service.agent import RunnerRegistry, run_normalized_message, tenant_app_name
from trpc_service.agent.tool_assembly import assemble_tools
from trpc_service.governance.filters import TenantBoundaryFilter
from trpc_service.persistence.models import PendingActionRow
from sqlalchemy import select

action_system, system = action_fixture, system_fixture


class ProposingModel(LLMModel):

    def __init__(self):
        super().__init__(model_name="proposal-contract")
        self.calls = 0

    @classmethod
    def supported_models(cls):
        return ["proposal-contract"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.calls += 1
        assert self.calls == 1, "original turn must end at confirmation boundary"
        part = Part.from_function_call(name="execute_refund", args={"order_id": "opaque-order"})
        part.function_call.id = "call-refund"
        yield LlmResponse(content=Content(role="model", parts=[part]))


@pytest.mark.asyncio
async def test_sdk_proposal_ends_turn_then_worker_executes(action_system, tenant):
    system, service, route, execution = action_system
    from tests.reliability.test_im_ingress import make_event
    event = make_event(system[1])
    definition = spec()

    async def execute_refund(order_id: str, tool_context: InvocationContext) -> dict:
        """Request a refund subject to explicit IM confirmation."""
        raise AssertionError("the declaration must never execute")

    model = ProposingModel()
    app = tenant.applications[0].model_copy(
        update={
            "tool_policy": tenant.applications[0].tool_policy.model_copy(
                update={"allow": frozenset({"execute_refund"})})
        })
    tools = assemble_tools(service.database,
                           tenant,
                           app, (execute_refund, ),
                           model=model,
                           action_service=service,
                           action_definitions=(definition, ))
    agent = LlmAgent(name="approval_agent",
                     model=model,
                     tools=list(tools),
                     filters=[TenantBoundaryFilter(route.tenant_id, route.agent_app_id)])
    sessions = InMemorySessionService()
    runner = Runner(app_name=tenant_app_name(route.tenant_id, route.agent_app_id),
                    agent=agent,
                    session_service=sessions)
    await sessions.create_session(app_name=runner.app_name, user_id=route.internal_user_id, session_id=route.session_id)
    registry = RunnerRegistry()
    registry.register(route.tenant_id, route.agent_app_id, route.config_version, runner)
    context = AgentContext()
    context.with_metadata("execution_id", execution)
    projected = [
        item async for item in run_normalized_message(
            runner_registry=registry, route=route, message=event.message, agent_context=context)
    ]
    assert model.calls == 1 and projected[-1].final and projected[-1].action_id
    assert projected[-1].text is None
    definition.execute.assert_not_awaited()
    with service.database.sessions() as session:
        action = service.view(session.scalar(select(PendingActionRow)))
        assert action["status"] == "pending"
    approve(service, route, action)
    assert await worker(service, route, definition).run_once()
    definition.execute.assert_awaited_once()
    assert model.calls == 1


def test_required_confirmation_cannot_silently_disappear(action_system, tenant):
    _, service, _, _ = action_system

    async def execute_refund(order_id: str, tool_context: InvocationContext) -> dict:
        """Declaration for a confirmed action."""
        raise AssertionError("declaration must not execute")

    with pytest.raises(ValueError, match="registered durable action"):
        assemble_tools(service.database, tenant, tenant.applications[0], (execute_refund, ), model=ProposingModel())
