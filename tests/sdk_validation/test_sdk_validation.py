"""Behavioral tests for the offline tRPC-Agent SDK validation."""

from __future__ import annotations

import socket

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent._deterministic_validation_model import DeterministicValidationModel
from trpc_service.agent.sdk_validation import (
    SDK_EXPECTED_VERSION,
    SdkValidationRunner,
    ValidationErrorType,
    ValidationFailure,
    ValidationStage,
    collect_turn,
    load_sdk_baseline,
    run_validation,
)


def _event(text: str) -> Event:
    return Event(
        author="sdk_validation_agent",
        invocation_id="test-invocation",
        content=Content(role="model", parts=[Part.from_text(text=text)]),
    )


def test_sdk_distribution_and_module_versions_match_baseline() -> None:
    baseline = load_sdk_baseline()

    assert baseline.expected_version == SDK_EXPECTED_VERSION
    assert baseline.distribution_version == SDK_EXPECTED_VERSION
    assert baseline.module_version == SDK_EXPECTED_VERSION
    assert baseline.matches is True


def test_version_mismatch_is_classified_without_running_agent() -> None:
    baseline = load_sdk_baseline(expected_version="0.0.0-invalid")

    assert baseline.matches is False
    with pytest.raises(ValidationFailure) as captured:
        baseline.require_match()
    assert captured.value.stage is ValidationStage.VERSION
    assert captured.value.error_type is ValidationErrorType.VERSION_MISMATCH


@pytest.mark.asyncio
async def test_official_runner_produces_one_non_empty_final_response() -> None:
    runtime = SdkValidationRunner()
    try:
        await runtime.initialize()
        turn = await runtime.execute_turn(
            session_id="session-a",
            turn_index=1,
            input_text="Remember validation token ALPHA.",
            expected_text="stored:ALPHA",
        )
        assert isinstance(runtime.runner, Runner)
        assert isinstance(runtime.session_service, InMemorySessionService)
    finally:
        await runtime.close()

    assert turn.observations
    assert sum(item.final_response for item in turn.observations) == 1
    assert turn.final_text == "stored:ALPHA"


def test_event_collection_rejects_multiple_final_responses() -> None:
    with pytest.raises(ValidationFailure) as captured:
        collect_turn(
            [_event("first"), _event("second")],
            turn_index=1,
            input_text="input",
            expected_text="first",
        )
    assert captured.value.error_type is ValidationErrorType.FINAL_RESPONSE_MULTIPLE


def test_event_collection_rejects_empty_final_response() -> None:
    with pytest.raises(ValidationFailure) as captured:
        collect_turn(
            [_event("   ")],
            turn_index=1,
            input_text="input",
            expected_text="expected",
        )
    assert captured.value.error_type is ValidationErrorType.FINAL_RESPONSE_EMPTY


@pytest.mark.asyncio
async def test_same_session_recalls_its_first_turn_token() -> None:
    runtime = SdkValidationRunner()
    try:
        await runtime.initialize()
        conversation = await runtime.run_conversation("session-a", "ALPHA")
    finally:
        await runtime.close()

    assert len(conversation.turns) == 2
    assert conversation.turns[0].final_text == "stored:ALPHA"
    assert conversation.turns[1].final_text == "recalled:ALPHA"


@pytest.mark.asyncio
async def test_two_sessions_are_isolated_and_full_run_has_four_finals() -> None:
    runtime = SdkValidationRunner()
    try:
        await runtime.initialize()
        conversations = await runtime.run_validation_conversations()
    finally:
        await runtime.close()

    assert [item.expected_token for item in conversations] == ["ALPHA", "BRAVO"]
    assert conversations[0].turns[1].final_text == "recalled:ALPHA"
    assert conversations[1].turns[1].final_text == "recalled:BRAVO"
    finals = [turn.final_text for item in conversations for turn in item.turns]
    assert len(finals) == 4
    assert all(finals)


@pytest.mark.asyncio
async def test_new_runtime_has_no_residual_session_history() -> None:
    first = SdkValidationRunner()
    try:
        await first.initialize()
        await first.run_conversation("session-a", "ALPHA")
    finally:
        await first.close()

    second = SdkValidationRunner()
    try:
        await second.initialize()
        with pytest.raises(ValidationFailure) as captured:
            await second.execute_turn(
                session_id="session-a",
                turn_index=1,
                input_text="Recall the validation token.",
                expected_text="recalled:ALPHA",
                mismatch_stage=ValidationStage.SESSION_CONTINUITY,
            )
    finally:
        await second.close()
    assert captured.value.error_type is ValidationErrorType.CONTEXT_MISSING


class NoFinalResponseModel(DeterministicValidationModel):
    async def _generate_async_impl(self, request, stream=False, ctx=None):
        del request, stream, ctx
        yield LlmResponse(
            content=Content(role="model", parts=[Part.from_text(text="partial")]),
            partial=True,
        )


class ContextLosingModel(DeterministicValidationModel):
    async def _generate_async_impl(self, request, stream=False, ctx=None):
        texts = [part.text for content in request.contents for part in (content.parts or []) if part.text]
        latest = texts[-1] if texts else ""
        if latest == "Recall the validation token.":
            yield LlmResponse(
                content=Content(role="model", parts=[Part.from_text(text="context-missing")])
            )
            return
        async for response in super()._generate_async_impl(request, stream, ctx):
            yield response


@pytest.mark.asyncio
async def test_version_mismatch_stops_run_at_version_stage() -> None:
    report = await run_validation(expected_version="0.0.0-invalid")

    assert report.status.value == "failed"
    assert report.stage(ValidationStage.VERSION).error_type is ValidationErrorType.VERSION_MISMATCH
    assert all(item.status.value == "skipped" for item in report.stages[1:])


@pytest.mark.asyncio
async def test_no_final_event_is_diagnosed_at_event_finalization() -> None:
    report = await run_validation(model=NoFinalResponseModel())

    assert report.status.value == "failed"
    stage = report.stage(ValidationStage.EVENT_FINALIZATION)
    assert stage.status.value == "failed"
    assert stage.error_type is ValidationErrorType.FINAL_RESPONSE_MISSING


@pytest.mark.asyncio
async def test_context_loss_is_diagnosed_at_continuity_stage() -> None:
    report = await run_validation(model=ContextLosingModel())

    assert report.status.value == "failed"
    stage = report.stage(ValidationStage.SESSION_CONTINUITY)
    assert stage.status.value == "failed"
    assert stage.error_type is ValidationErrorType.CONTEXT_MISSING


def test_external_socket_attempt_is_blocked(offline_validation_environment: list[str]) -> None:
    with socket.socket() as client:
        with pytest.raises(AssertionError, match="external socket access"):
            client.connect(("203.0.113.1", 443))
    assert offline_validation_environment == ["('203.0.113.1', 443)"]
