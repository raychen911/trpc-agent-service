from __future__ import annotations

from types import SimpleNamespace

import pytest
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.tools import FunctionTool

from tests.conftest import envelope, repository, tenant_config
from trpc_service.runtime import TenantRuntime
from trpc_service.tenant.models import ToolPolicy, ToolRisk
from trpc_service.tool.confirmation import (
    ConfirmationError,
    ConfirmationScope,
    ConfirmationTokenService,
    InMemoryConfirmationLedger,
    arguments_hash,
)
from trpc_service.tool.execution import (
    ExecutionRecord,
    ExecutionStatus,
    HumanReviewRequired,
    InMemoryExecutionLedger,
    ToolExecutor,
)
from trpc_service.tool.governance import (
    Decision,
    GovernancePipeline,
    InMemoryBudgetLedger,
    SdkToolSafetyScanner,
)
from trpc_service.tool.integration import GovernedTool

KEY = b"t" * 32


@pytest.mark.asyncio
async def test_confirmation_token_is_scoped_and_one_time() -> None:
    service = ConfirmationTokenService(KEY, InMemoryConfirmationLedger(), ttl_seconds=60)
    scope = ConfirmationScope(
        tenant_id="tenant",
        principal_id="principal",
        session_id="session",
        tool_name="delete_record",
        arguments_hash=arguments_hash({"id": 1}),
    )
    token = await service.issue(scope)
    await service.consume(token, scope)
    with pytest.raises(ConfirmationError, match="already"):
        await service.consume(token, scope)
    with pytest.raises(ConfirmationError):
        await service.consume(token + "x", scope)


@pytest.mark.asyncio
async def test_sdk_safety_guard_denies_destructive_script() -> None:
    scanner = SdkToolSafetyScanner()
    assert await scanner.scan("shell", {"command": "rm -rf /"}) == Decision.DENY
    assert await scanner.scan("calculator", {"value": 2}) == Decision.ALLOW


@pytest.mark.asyncio
async def test_governance_order_enforces_allowlist_budget_and_confirmation() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=KEY).accept(
        "binding-unpredictable-a", envelope()
    )
    config = tenant_config().model_copy(
        update={
            "tools": ToolPolicy(
                allow=frozenset({"shell"}),
                require_confirmation=frozenset({"shell"}),
            )
        }
    )
    confirmations = ConfirmationTokenService(KEY, InMemoryConfirmationLedger())
    pipeline = GovernancePipeline(InMemoryBudgetLedger(), SdkToolSafetyScanner(), confirmations)
    denied = await pipeline.evaluate(
        context=accepted.context,
        config=config,
        tool_name="unknown",
        arguments={},
    )
    assert denied.decision == Decision.DENY

    arguments = {"command": "echo safe"}
    needs = await pipeline.evaluate(
        context=accepted.context,
        config=config,
        tool_name="shell",
        arguments=arguments,
    )
    assert needs.decision == Decision.NEEDS_CONFIRMATION
    scope = ConfirmationScope(
        tenant_id=accepted.context.tenant_id,
        principal_id=accepted.context.principal_id,
        session_id=accepted.context.session_id,
        tool_name="shell",
        arguments_hash=arguments_hash(arguments),
    )
    token = await confirmations.issue(scope)
    allowed = await pipeline.evaluate(
        context=accepted.context,
        config=config,
        tool_name="shell",
        arguments=arguments,
        confirmation_token=token,
    )
    assert allowed.decision == Decision.ALLOW
    assert allowed.audit_arguments["command"] == "echo safe"


@pytest.mark.asyncio
async def test_governance_audits_each_policy_decision_without_arguments() -> None:
    accepted = await TenantRuntime(repository(), routing_key=KEY).accept(
        "binding-unpredictable-a", envelope()
    )
    config = tenant_config().model_copy(update={"tools": ToolPolicy(allow=frozenset({"read"}))})

    class Audit:
        def __init__(self) -> None:
            self.records: list[dict[str, object]] = []

        async def record(self, **record: object) -> None:
            self.records.append(record)

    audit = Audit()
    pipeline = GovernancePipeline(
        InMemoryBudgetLedger(),
        SdkToolSafetyScanner(),
        ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
        audit,
    )
    result = await pipeline.evaluate(
        context=accepted.context,
        config=config,
        tool_name="read",
        arguments={"api_key": "must-not-be-audited"},
    )
    assert result.decision == Decision.ALLOW
    assert audit.records[0]["decision"] == Decision.ALLOW
    assert "arguments" not in audit.records[0]


@pytest.mark.asyncio
async def test_tool_execution_retries_only_idempotent_operations() -> None:
    repo = repository()
    context = (
        await TenantRuntime(repo, routing_key=KEY).accept("binding-unpredictable-a", envelope())
    ).context
    ledger = InMemoryExecutionLedger()
    executor = ToolExecutor(KEY, ledger)
    calls = 0

    async def uncertain():
        nonlocal calls
        calls += 1
        raise TimeoutError

    with pytest.raises(HumanReviewRequired):
        await executor.execute(
            context,
            turn_id="turn",
            tool_name="charge",
            arguments={"amount": 1},
            risk=ToolRisk.NON_IDEMPOTENT,
            call=uncertain,
        )
    with pytest.raises(HumanReviewRequired):
        await executor.execute(
            context,
            turn_id="turn",
            tool_name="charge",
            arguments={"amount": 1},
            risk=ToolRisk.NON_IDEMPOTENT,
            call=uncertain,
        )
    assert calls == 1

    attempts = 0

    async def retryable():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError
        return "ok"

    with pytest.raises(TimeoutError):
        await executor.execute(
            context,
            turn_id="turn-2",
            tool_name="read",
            arguments={},
            risk=ToolRisk.IDEMPOTENT,
            call=retryable,
        )
    assert (
        await executor.execute(
            context,
            turn_id="turn-2",
            tool_name="read",
            arguments={},
            risk=ToolRisk.IDEMPOTENT,
            call=retryable,
        )
        == "ok"
    )

    missing_result = executor.key_for(
        context,
        turn_id="turn-3",
        tool_name="charge",
        arguments={"amount": 2},
    )
    ledger.records[missing_result] = ExecutionRecord(
        missing_result, ExecutionStatus.SUCCEEDED, result=None
    )
    with pytest.raises(HumanReviewRequired, match="result is unavailable"):
        await executor.execute(
            context,
            turn_id="turn-3",
            tool_name="charge",
            arguments={"amount": 2},
            risk=ToolRisk.NON_IDEMPOTENT,
            call=lambda: retryable(),
        )

    rebuilt = 0

    async def rebuild_result() -> str:
        nonlocal rebuilt
        rebuilt += 1
        return "reconstructed"

    assert (
        await executor.execute(
            context,
            turn_id="turn-3",
            tool_name="charge",
            arguments={"amount": 2},
            risk=ToolRisk.IDEMPOTENT,
            call=rebuild_result,
        )
        == "reconstructed"
    )
    assert rebuilt == 1
    assert ledger.records[missing_result].status == ExecutionStatus.SUCCEEDED
    assert ledger.records[missing_result].result is None

    async def failed_rebuild() -> str:
        raise TimeoutError

    with pytest.raises(TimeoutError):
        await executor.execute(
            context,
            turn_id="turn-3",
            tool_name="charge",
            arguments={"amount": 2},
            risk=ToolRisk.IDEMPOTENT,
            call=failed_rebuild,
        )
    assert ledger.records[missing_result].status == ExecutionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_governed_sdk_tool_blocks_before_side_effect_and_executes_allowed_call() -> None:
    accepted = await TenantRuntime(repository(), routing_key=KEY).accept(
        "binding-unpredictable-a", envelope()
    )
    config = tenant_config().model_copy(
        update={
            "tools": ToolPolicy(
                allow=frozenset({"write_value"}),
                classifications={"write_value": ToolRisk.IDEMPOTENT},
            )
        }
    )
    calls: list[int] = []

    async def write_value(value: int) -> dict[str, int]:
        """Write one test value."""

        calls.append(value)
        return {"value": value}

    governed = GovernedTool(
        FunctionTool(write_value),
        config=config,
        governance=GovernancePipeline(
            InMemoryBudgetLedger(),
            SdkToolSafetyScanner(),
            ConfirmationTokenService(KEY, InMemoryConfirmationLedger()),
        ),
        executor=ToolExecutor(KEY, InMemoryExecutionLedger()),
    )
    metadata = {
        "tenant_id": accepted.context.tenant_id,
        "app_id": accepted.context.app_id,
        "config_version": accepted.context.config_version,
        "binding_id": accepted.context.channel_binding_id,
        "principal_id": accepted.context.principal_id,
        "session_id": accepted.context.session_id,
        "turn_id": "turn-1",
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

    assert await governed._run_async_impl(tool_context=invocation, args={"value": 7}) == {
        "value": 7
    }
    assert calls == [7]

    invocation.agent_context.with_metadata("tenant_id", "different-tenant")
    denied = await governed._run_async_impl(tool_context=invocation, args={"value": 8})
    assert denied == {"error": "identity_mismatch", "status": "deny"}
    assert calls == [7]
