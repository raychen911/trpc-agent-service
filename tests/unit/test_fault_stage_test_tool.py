from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from trpc_agent_sdk.context import new_agent_context

from tests.conftest import envelope, repository, tenant_config
from trpc_service.agent.factory import DevelopmentAgentLoader, ProductionAgentLoader
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.config.secrets import LocalSecretProvider
from trpc_service.config.settings import Environment
from trpc_service.faults import FaultStage, FaultStageEvent
from trpc_service.runtime import TenantRuntime
from trpc_service.tenant.models import ModelPolicy, ToolPolicy, ToolRisk
from trpc_service.tool.confirmation import ConfirmationTokenService, InMemoryConfirmationLedger
from trpc_service.tool.execution import InMemoryExecutionLedger, ToolExecutor
from trpc_service.tool.governance import (
    GovernancePipeline,
    InMemoryBudgetLedger,
    SdkToolSafetyScanner,
)
from trpc_service.tool.integration import GovernedTool
from trpc_service.tool.test_tool import (
    DETERMINISTIC_FAULT_TOOL_NAME,
    build_fault_stage_test_tools,
)

KEY = b"f" * 32


class RecordingController:
    def __init__(self) -> None:
        self.events: list[FaultStageEvent] = []

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        self.events.append(event)
        return False


@pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.PRODUCTION])
def test_default_development_and_production_do_not_register_tool(environment: Environment) -> None:
    assert (
        build_fault_stage_test_tools(
            environment=environment,
            fault_injection_enabled=False,
        )
        == {}
    )


@pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.PRODUCTION])
def test_non_test_enabled_tool_registration_fails_closed(environment: Environment) -> None:
    with pytest.raises(ValueError, match="test environment"):
        build_fault_stage_test_tools(
            environment=environment,
            fault_injection_enabled=True,
        )


def test_deterministic_tool_call_loader_is_strictly_test_only() -> None:
    production = ProductionAgentLoader(LocalSecretProvider(allow_literal=True))
    with pytest.raises(ValueError, match="test environment"):
        DevelopmentAgentLoader(
            production,
            environment=Environment.DEVELOPMENT,
            fault_injection_enabled=True,
        )
    with pytest.raises(ValueError, match="test fault injection"):
        DevelopmentAgentLoader(
            production,
            environment=Environment.TEST,
            deterministic_tool_call=True,
        )
    with pytest.raises(ValueError, match="test environment"):
        DevelopmentAgentLoader(
            production,
            environment=Environment.PRODUCTION,
            fault_injection_enabled=True,
        )


@pytest.mark.asyncio
async def test_test_enabled_tool_is_fixed_bounded_and_content_free() -> None:
    tools = build_fault_stage_test_tools(
        environment=Environment.TEST,
        fault_injection_enabled=True,
    )
    assert set(tools) == {DETERMINISTIC_FAULT_TOOL_NAME}
    tool = tools[DETERMINISTIC_FAULT_TOOL_NAME]
    assert tool.name == DETERMINISTIC_FAULT_TOOL_NAME
    value = await tool._run_async_impl(tool_context=SimpleNamespace(), args={})
    assert value == {"status": "ok", "result": "deterministic"}

    with pytest.raises(ValueError, match="delay"):
        build_fault_stage_test_tools(
            environment=Environment.TEST,
            fault_injection_enabled=True,
            delay_seconds=0.201,
        )


@pytest.mark.asyncio
async def test_production_loader_exposes_tool_only_when_tenant_allows_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "trpc_service.agent.factory.OpenAIModel",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "trpc_service.agent.factory.LlmAgent",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    tools = build_fault_stage_test_tools(
        environment=Environment.TEST,
        fault_injection_enabled=True,
    )
    pipeline = GovernancePipeline(
        InMemoryBudgetLedger(),
        SdkToolSafetyScanner(),
        ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
    )
    loader = ProductionAgentLoader(
        LocalSecretProvider(allow_literal=True),
        tools=tools,
        governance=pipeline,
        tool_executor=ToolExecutor(KEY, InMemoryExecutionLedger(), worker_id="worker"),
    )
    config = tenant_config().model_copy(
        update={
            "model": ModelPolicy(provider="openai", model="test"),
            "tools": ToolPolicy(allow=frozenset({DETERMINISTIC_FAULT_TOOL_NAME})),
        }
    )

    agent = await loader(config)

    assert len(agent.tools) == 1
    assert isinstance(agent.tools[0], GovernedTool)
    assert agent.tools[0].name == DETERMINISTIC_FAULT_TOOL_NAME


@pytest.mark.asyncio
async def test_governed_test_tool_uses_executor_checkpoint_and_allowlist() -> None:
    accepted = await TenantRuntime(repository(), routing_key=KEY).accept(
        "binding-unpredictable-a", envelope("tool-stage")
    )
    config = tenant_config().model_copy(
        update={
            "tools": ToolPolicy(
                allow=frozenset({DETERMINISTIC_FAULT_TOOL_NAME}),
                classifications={DETERMINISTIC_FAULT_TOOL_NAME: ToolRisk.IDEMPOTENT},
            )
        }
    )
    controller = RecordingController()
    governed = GovernedTool(
        build_fault_stage_test_tools(
            environment=Environment.TEST,
            fault_injection_enabled=True,
        )[DETERMINISTIC_FAULT_TOOL_NAME],
        config=config,
        governance=GovernancePipeline(
            InMemoryBudgetLedger(),
            SdkToolSafetyScanner(),
            ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
        ),
        executor=ToolExecutor(
            KEY,
            InMemoryExecutionLedger(),
            fault_stages=controller,
            worker_id="worker-tool-stage",
        ),
    )
    metadata: dict[str, Any] = {
        "tenant_id": accepted.context.tenant_id,
        "app_id": accepted.context.app_id,
        "config_version": accepted.context.config_version,
        "binding_id": accepted.context.channel_binding_id,
        "principal_id": accepted.context.principal_id,
        "session_id": accepted.context.session_id,
        "turn_id": "turn-tool-stage",
        "request_id": accepted.context.request_id,
        "trace_id": accepted.context.trace_id,
    }
    invocation = SimpleNamespace(
        agent_context=new_agent_context(metadata=metadata),
        agent=SimpleNamespace(
            before_tool_callback=None,
            after_tool_callback=None,
            parallel_tool_calls=False,
        ),
    )

    result = await governed._run_async_impl(tool_context=invocation, args={})

    assert result == {"status": "ok", "result": "deterministic"}
    assert len(controller.events) == 1
    event = controller.events[0]
    assert event.stage is FaultStage.TOOL
    assert event.tenant_id == accepted.context.tenant_id
    assert event.turn_id == "turn-tool-stage"
    assert event.worker_id == "worker-tool-stage"
    assert event.execution_key

    denied_controller = RecordingController()
    denied = GovernedTool(
        build_fault_stage_test_tools(
            environment=Environment.TEST,
            fault_injection_enabled=True,
        )[DETERMINISTIC_FAULT_TOOL_NAME],
        config=config.model_copy(update={"tools": ToolPolicy()}),
        governance=GovernancePipeline(
            InMemoryBudgetLedger(),
            SdkToolSafetyScanner(),
            ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
        ),
        executor=ToolExecutor(
            KEY,
            InMemoryExecutionLedger(),
            fault_stages=denied_controller,
            worker_id="worker-tool-stage",
        ),
    )
    denied_result = await denied._run_async_impl(tool_context=invocation, args={})
    assert denied_result == {"error": "tool_not_allowed", "status": "deny"}
    assert denied_controller.events == []


@pytest.mark.asyncio
async def test_worker_test_loader_reaches_tool_checkpoint_through_governance() -> None:
    runtime_repository = repository()
    runtime_repository.add_config(
        tenant_config().model_copy(
            update={
                "model": ModelPolicy(provider="offline", model="offline"),
                "tools": ToolPolicy(
                    allow=frozenset({DETERMINISTIC_FAULT_TOOL_NAME}),
                    classifications={DETERMINISTIC_FAULT_TOOL_NAME: ToolRisk.IDEMPOTENT},
                ),
            }
        )
    )
    accepted = await TenantRuntime(runtime_repository, routing_key=KEY).accept(
        "binding-unpredictable-a", envelope("worker-tool-stage")
    )
    controller = RecordingController()
    governance = GovernancePipeline(
        InMemoryBudgetLedger(),
        SdkToolSafetyScanner(),
        ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
    )
    production = ProductionAgentLoader(
        LocalSecretProvider(allow_literal=True),
        tools=build_fault_stage_test_tools(
            environment=Environment.TEST,
            fault_injection_enabled=True,
        ),
        governance=governance,
        tool_executor=ToolExecutor(
            KEY,
            InMemoryExecutionLedger(),
            fault_stages=controller,
            worker_id="worker-tool-stage",
        ),
    )
    loader = DevelopmentAgentLoader(
        production,
        delay_seconds=0,
        environment=Environment.TEST,
        fault_injection_enabled=True,
        deterministic_tool_call=True,
    )

    result = await AgentWorker(
        runtime_repository,
        worker_id="worker-tool-stage",
        agent_loader=loader,
        lease_for=timedelta(seconds=10),
    ).process(accepted)

    assert result.status is ProcessStatus.COMMITTED
    assert result.commit is not None
    assert result.commit.outbound_id
    assert [event.stage for event in controller.events] == [FaultStage.TOOL]
    event = controller.events[0]
    assert event.tenant_id == accepted.context.tenant_id
    assert event.turn_id
    assert event.worker_id == "worker-tool-stage"
    assert event.execution_key
