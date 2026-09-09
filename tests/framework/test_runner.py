# mypy: disable-error-code="import-untyped"
"""Real Runner integration tests using deterministic fake LLMModel instances."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent import (
    AgentFactory,
    AgentTurnTimeoutError,
    ExecutionLimits,
    MissingFinalResponseError,
    TenantAgentRunner,
)
from trpc_service.channels.contracts import ReplyKind
from trpc_service.tenant.models import GovernancePolicy

from .helpers import (
    BlockingFakeModel,
    EmptyFakeModel,
    ErrorFakeModel,
    RecordingMemoryService,
    RecordingSessionService,
    SensitiveFakeModel,
    StreamingFakeModel,
    echo,
    make_app,
    make_context,
)


def _metric_value(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.mark.asyncio
async def test_real_runner_consumes_full_stream_and_persists_only_final() -> None:
    model = StreamingFakeModel()
    resolved_custom_data: list[dict[str, object]] = []

    async def dynamic_model_factory(custom_data: dict[str, object]) -> LLMModel:
        resolved_custom_data.append(dict(custom_data))
        return model

    factory = AgentFactory(
        model_resolver=lambda context, app: dynamic_model_factory,
        registered_tools={"echo": echo},
    )
    sessions = RecordingSessionService()
    memory = RecordingMemoryService()
    runtime = TenantAgentRunner(
        agent_factory=factory,
        session_service=sessions,
        memory_service=memory,
    )
    context = make_context()

    result = await runtime.run_turn(
        tenant_context=context,
        app=make_app(),
        new_message="hi",
        run_id="run-a",
        in_reply_to_delivery_id="delivery-a",
    )

    assert model.drained, "the consumer must request the item after the final yield"
    assert [event.partial for event in result.framework_events] == [True, False]
    assert len(result.platform_events) == 1
    assert result.platform_events[0].payload["text"] == "hello"
    assert result.reply_intent.text == "hello"
    assert result.reply_intent.kind is ReplyKind.FINAL
    assert "helhello" not in result.reply_intent.text
    assert resolved_custom_data == [
        {
            "tenant_id": "tenant-a",
            "app_id": "support",
            "app_revision": 3,
            "request_id": "request-a",
            "trace_id": "trace-a",
        }
    ]
    assert model.requests and "echo" in model.requests[0].tools_dict

    session = await sessions.get_session(
        app_name=result.build.app_name,
        user_id=context.principal_id,
        session_id=context.session_id,
    )
    assert session is not None
    assert [event.get_text() for event in session.events] == ["hi", "hello"]
    assert all(not event.partial for event in session.events)
    assert memory.store_calls == 0, "post-turn processing must be disabled"
    assert memory.close_calls == sessions.close_calls == 0


@pytest.mark.asyncio
async def test_governance_filter_redacts_input_output_and_records_usage() -> None:
    model = SensitiveFakeModel()
    sessions = RecordingSessionService()
    runtime = TenantAgentRunner(
        agent_factory=AgentFactory(
            model_resolver=lambda context, app: model,
            registered_tools={"echo": echo},
        ),
        session_service=sessions,
    )
    context = make_context()
    app = make_app(governance=GovernancePolicy(blocked_output_terms=("internal-only",)))
    input_before = _metric_value(
        "agent_platform_model_tokens_total",
        {"tenant": "tenant-a", "app": "support", "direction": "input"},
    )
    output_before = _metric_value(
        "agent_platform_model_tokens_total",
        {"tenant": "tenant-a", "app": "support", "direction": "output"},
    )

    result = await runtime.run_turn(
        tenant_context=context,
        app=app,
        new_message="contact client@example.com or 13900139000",
        run_id="run-governed",
        in_reply_to_delivery_id="delivery-governed",
    )

    request_text = "".join(part.text or "" for part in model.requests[0].contents[-1].parts or [])
    assert request_text == ("contact [EMAIL_REDACTED] or [PHONE_REDACTED]")
    assert result.reply_intent.text == (
        "send to [EMAIL_REDACTED] or [PHONE_REDACTED]; [BLOCKED_TERM]"
    )
    session = await sessions.get_session(
        app_name=result.build.app_name,
        user_id=context.principal_id,
        session_id=context.session_id,
    )
    assert session is not None
    assert all("example.com" not in event.get_text() for event in session.events)
    assert (
        _metric_value(
            "agent_platform_model_tokens_total",
            {"tenant": "tenant-a", "app": "support", "direction": "input"},
        )
        == input_before + 7
    )
    assert (
        _metric_value(
            "agent_platform_model_tokens_total",
            {"tenant": "tenant-a", "app": "support", "direction": "output"},
        )
        == output_before + 11
    )


@pytest.mark.asyncio
async def test_timeout_covers_async_generator_consumption_and_cancels_model() -> None:
    model = BlockingFakeModel()
    sessions = RecordingSessionService()
    runtime = TenantAgentRunner(
        agent_factory=AgentFactory(
            model_resolver=lambda context, app: model,
            registered_tools={"echo": echo},
        ),
        session_service=sessions,
    )

    with pytest.raises(AgentTurnTimeoutError, match="exceeded"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message="wait",
            run_id="run-timeout",
            in_reply_to_delivery_id="delivery-timeout",
            timeout_seconds=0.02,
        )

    assert model.cancelled
    assert sessions.close_calls == 0


@pytest.mark.asyncio
async def test_sdk_error_event_becomes_sanitized_platform_error_reply() -> None:
    runtime = TenantAgentRunner(
        agent_factory=AgentFactory(
            model_resolver=lambda context, app: ErrorFakeModel(model_name="fake-error"),
            registered_tools={"echo": echo},
        ),
        session_service=RecordingSessionService(),
    )

    result = await runtime.run_turn(
        tenant_context=make_context(),
        app=make_app(),
        new_message="fail safely",
        run_id="run-error",
        in_reply_to_delivery_id="delivery-error",
    )

    assert result.sdk_error
    assert result.reply_intent.kind is ReplyKind.ERROR
    assert result.reply_intent.text is not None
    assert "secret-provider-token" not in result.reply_intent.text
    assert "secret-provider-token" not in str(result.platform_events)


def test_execution_limits_must_be_finite() -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        ExecutionLimits(max_llm_calls=0)


@pytest.mark.asyncio
async def test_runtime_validates_boundary_inputs() -> None:
    runtime = TenantAgentRunner(
        agent_factory=AgentFactory(
            model_resolver=lambda context, app: StreamingFakeModel(),
            registered_tools={"echo": echo},
        ),
        session_service=RecordingSessionService(),
    )

    with pytest.raises(ValueError, match="run_id"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message="hi",
            run_id="",
            in_reply_to_delivery_id="delivery-a",
        )
    with pytest.raises(ValueError, match="timeout_seconds"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message="hi",
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
            timeout_seconds=0,
        )
    with pytest.raises(ValueError, match="attempt_no"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message="hi",
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
            attempt_no=0,
        )
    with pytest.raises(ValueError, match="new_message"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message=" ",
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
        )


@pytest.mark.asyncio
async def test_runtime_accepts_sdk_content_and_fails_closed_without_reply() -> None:
    runtime = TenantAgentRunner(
        agent_factory=AgentFactory(
            model_resolver=lambda context, app: EmptyFakeModel(model_name="fake-empty"),
            registered_tools={"echo": echo},
        ),
        session_service=RecordingSessionService(),
    )

    with pytest.raises(MissingFinalResponseError, match="user-visible"):
        await runtime.run_turn(
            tenant_context=make_context(),
            app=make_app(),
            new_message=Content(role="user", parts=[Part.from_text(text="hello")]),
            run_id="run-empty",
            in_reply_to_delivery_id="delivery-empty",
        )
