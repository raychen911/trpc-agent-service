# mypy: disable-error-code="import-untyped"
"""Pure SDK Event projection tests."""

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.events import (
    DEFAULT_PUBLIC_ERROR_MESSAGE,
    framework_event_to_event_data,
    framework_event_to_reply_intent,
)
from trpc_service.channels.contracts import ReplyKind

from .helpers import make_context


def test_partial_delta_is_neither_persisted_nor_replied() -> None:
    event = Event(
        id="partial-1",
        author="agent",
        content=Content(role="model", parts=[Part.from_text(text="hel")]),
        partial=True,
    )

    assert framework_event_to_event_data(event, run_id="run-a", sequence=1) is None
    assert (
        framework_event_to_reply_intent(
            event,
            tenant_context=make_context(),
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
        )
        is None
    )


def test_final_projection_excludes_model_thoughts() -> None:
    event = Event(
        id="final-1",
        invocation_id="invocation-a",
        author="agent",
        content=Content(
            role="model",
            parts=[
                Part(text="private reasoning", thought=True),
                Part.from_text(text="public answer"),
            ],
        ),
        partial=False,
    )

    durable = framework_event_to_event_data(event, run_id="run-a", sequence=1)
    reply = framework_event_to_reply_intent(
        event,
        tenant_context=make_context(),
        run_id="run-a",
        in_reply_to_delivery_id="delivery-a",
    )

    assert durable is not None
    assert durable.event_id == "run-a:attempt:1:sdk:000001"
    assert durable.event_type == "assistant"
    assert durable.role == "assistant"
    assert durable.payload["text"] == "public answer"
    assert "private reasoning" not in str(durable.payload)
    assert reply is not None
    assert reply.kind is ReplyKind.FINAL
    assert reply.text == "public answer"


def test_tool_arguments_are_hashed_and_tool_event_is_not_a_reply() -> None:
    event = Event(
        id="tool-1",
        author="agent",
        content=Content(
            role="model",
            parts=[
                Part.from_function_call(
                    name="create_ticket",
                    args={"password": "must-not-be-persisted"},
                )
            ],
        ),
        partial=False,
    )

    durable = framework_event_to_event_data(event, run_id="run-a", sequence=1)

    assert durable is not None
    assert durable.event_type == "tool_call"
    assert durable.payload["tool_calls"][0]["name"] == "create_ticket"
    assert "must-not-be-persisted" not in str(durable.payload)
    assert (
        framework_event_to_reply_intent(
            event,
            tenant_context=make_context(),
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
        )
        is None
    )


def test_error_projection_never_exposes_sdk_error_message() -> None:
    event = Event(
        id="error-1",
        author="agent",
        error_code="provider_error",
        error_message="api-key-secret",
        partial=False,
    )

    durable = framework_event_to_event_data(event, run_id="run-a", sequence=1)
    reply = framework_event_to_reply_intent(
        event,
        tenant_context=make_context(),
        run_id="run-a",
        in_reply_to_delivery_id="delivery-a",
    )

    assert durable is not None
    assert durable.payload["error_code"] == "provider_error"
    assert "api-key-secret" not in str(durable.payload)
    assert reply is not None
    assert reply.kind is ReplyKind.ERROR
    assert reply.text == DEFAULT_PUBLIC_ERROR_MESSAGE


def test_tool_response_is_hashed_and_empty_framework_event_is_typed() -> None:
    response_part = Part.from_function_response(
        name="lookup",
        response={"private_record": "do-not-copy"},
    )
    response_part.function_response.id = "call-a"
    response_event = Event(
        content=Content(role="user", parts=[response_part]),
        partial=False,
    )

    durable = framework_event_to_event_data(response_event, run_id="run-a", sequence=2)
    empty = framework_event_to_event_data(Event(partial=False), run_id="run-a", sequence=3)

    assert durable is not None and durable.event_type == "tool_result"
    assert durable.payload["tool_responses"][0]["name"] == "lookup"
    assert "do-not-copy" not in str(durable.payload)
    assert empty is not None and empty.event_type == "framework" and empty.role is None


def test_projection_validates_identifiers_and_public_error_text() -> None:
    event = Event(error_code="failed", partial=False)

    with pytest.raises(ValueError, match="sequence"):
        framework_event_to_event_data(event, run_id="run-a", sequence=0)
    with pytest.raises(ValueError, match="attempt_no"):
        framework_event_to_event_data(
            event,
            run_id="run-a",
            sequence=1,
            attempt_no=0,
        )
    with pytest.raises(ValueError, match="run_id"):
        framework_event_to_event_data(event, run_id="", sequence=1)
    with pytest.raises(ValueError, match="run_id"):
        framework_event_to_reply_intent(
            event,
            tenant_context=make_context(),
            run_id="",
            in_reply_to_delivery_id="delivery-a",
        )
    with pytest.raises(ValueError, match="public_error_message"):
        framework_event_to_reply_intent(
            event,
            tenant_context=make_context(),
            run_id="run-a",
            in_reply_to_delivery_id="delivery-a",
            public_error_message=" ",
        )


def test_hidden_or_empty_final_event_is_not_a_reply() -> None:
    hidden = Event(
        content=Content(role="model", parts=[Part.from_text(text="hidden")]),
        visible=False,
        partial=False,
    )
    empty = Event(content=Content(role="model", parts=[]), partial=False)

    for event in (hidden, empty):
        assert (
            framework_event_to_reply_intent(
                event,
                tenant_context=make_context(),
                run_id="run-a",
                in_reply_to_delivery_id="delivery-a",
            )
            is None
        )
