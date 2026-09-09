# mypy: disable-error-code="import-untyped"
"""Pure projections from SDK events to durable platform contracts."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from trpc_agent_sdk.events import Event

from trpc_service.channels.contracts import ReplyIntent, ReplyKind
from trpc_service.reliability.types import EventData
from trpc_service.tenant.context import TenantContext

DEFAULT_PUBLIC_ERROR_MESSAGE = "Agent 服务暂时不可用, 请稍后重试。"


def framework_event_to_event_data(
    event: Event,
    *,
    run_id: str,
    sequence: int,
    attempt_no: int = 1,
) -> EventData | None:
    """Project one non-partial SDK event into a deterministic durable event.

    Partial text is deliberately transient: tRPC-Agent emits a final non-partial
    event containing the accumulated answer, so persisting or concatenating both
    would duplicate content. Raw tool arguments, tool results, custom metadata, and
    exception messages are not copied into the platform event.
    """

    if event.partial:
        return None
    if sequence < 1:
        raise ValueError("sequence must be positive")
    if attempt_no < 1:
        raise ValueError("attempt_no must be positive")
    if not run_id:
        raise ValueError("run_id must not be empty")

    function_calls = event.get_function_calls()
    function_responses = event.get_function_responses()
    text = _visible_text(event)
    payload: dict[str, Any] = {
        "author": event.author,
        "invocation_id": event.invocation_id,
        "request_id": event.request_id,
    }
    if text:
        payload["text"] = text
    if function_calls:
        payload["tool_calls"] = [
            {
                "id": call.id,
                "name": call.name,
                "args_sha256": _stable_hash(call.args or {}),
            }
            for call in function_calls
        ]
    if function_responses:
        payload["tool_responses"] = [
            {
                "id": response.id,
                "name": response.name,
                "response_sha256": _stable_hash(response.response or {}),
            }
            for response in function_responses
        ]
    if event.is_error():
        payload["error_code"] = event.error_code or "sdk_error"
    if event.usage_metadata is not None:
        payload["usage"] = event.usage_metadata.model_dump(mode="json", exclude_none=True)

    framework_event_id = event.id or None
    # ReliabilityRepository intentionally forbids reviving an event from a
    # superseded worker attempt. Attempt-scoped keys preserve that fence while
    # remaining deterministic for a database retry within the same attempt.
    event_id = f"{run_id}:attempt:{attempt_no}:sdk:{sequence:06d}"
    return EventData(
        event_id=event_id,
        event_key=event_id,
        event_type=_event_type(event),
        payload={key: value for key, value in payload.items() if value is not None},
        role=_platform_role(event),
        state_delta=dict(event.actions.state_delta),
        framework_event_id=framework_event_id,
    )


def framework_event_to_reply_intent(
    event: Event,
    *,
    tenant_context: TenantContext,
    run_id: str,
    in_reply_to_delivery_id: str,
    revision: int = 1,
    public_error_message: str = DEFAULT_PUBLIC_ERROR_MESSAGE,
) -> ReplyIntent | None:
    """Project only a complete user-visible SDK event into an outbound intent."""

    if event.partial or not event.visible:
        return None
    if not run_id or not in_reply_to_delivery_id:
        raise ValueError("run_id and in_reply_to_delivery_id must not be empty")

    if event.is_error():
        text = public_error_message.strip()
        if not text:
            raise ValueError("public_error_message must not be empty")
        kind = ReplyKind.ERROR
    else:
        if (
            not event.is_final_response()
            or event.get_function_calls()
            or event.get_function_responses()
        ):
            return None
        text = _visible_text(event).strip()
        if not text:
            return None
        kind = ReplyKind.FINAL

    suffix = "error" if kind is ReplyKind.ERROR else "final"
    return ReplyIntent(
        intent_id=f"{run_id}:reply:{suffix}",
        tenant_id=tenant_context.tenant_id,
        binding_id=tenant_context.binding_id,
        session_id=tenant_context.session_id,
        run_id=run_id,
        in_reply_to_delivery_id=in_reply_to_delivery_id,
        kind=kind,
        text=text,
        revision=revision,
        final=True,
        idempotency_key=f"{run_id}:reply:{suffix}",
    )


def _visible_text(event: Event) -> str:
    if event.content is None or not event.content.parts:
        return ""
    return "".join(
        part.text
        for part in event.content.parts
        if part.text and not bool(getattr(part, "thought", False))
    )


def _event_type(event: Event) -> str:
    if event.is_error():
        return "error"
    if event.get_function_calls():
        return "tool_call"
    if event.get_function_responses():
        return "tool_result"
    if event.is_final_response() and _visible_text(event):
        return "assistant"
    return "framework"


def _platform_role(event: Event) -> str | None:
    if event.content is None or not event.content.role:
        return None
    return "assistant" if event.content.role == "model" else event.content.role


def _stable_hash(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()
