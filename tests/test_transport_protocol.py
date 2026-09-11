"""RED tests for the strict shared transport protocol models."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from trpc_service.transport.models import (
    PROTOCOL_VERSION,
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
    WorkerToolCallData,
    WorkerToolResultData,
)

VALID_TASK_KWARGS = dict(
    protocol_version=1,
    request_id=uuid.uuid4(),
    tenant_id="tenant_a",
    app_id="app_demo",
    config_version=1,
    user_id="user_default",
    channel="web",
    session_id="s1",
    message_id="msg_1",
    message="hello",
)


class TestProtocolVersion:

    def test_protocol_version_is_one(self) -> None:
        assert PROTOCOL_VERSION == 1


class TestWorkerErrorCode:

    def test_finite_set(self) -> None:
        expected = {
            "tenant_config_mismatch",
            "tenant_agent_configuration",
            "model_configuration",
            "model_runtime",
            "worker_unavailable",
            "worker_timeout",
            "invalid_worker_response",
            "session_busy",
            "tenant_repository_unavailable",
            "message_in_progress",
            "idempotency_conflict",
            "approval_not_available",
            "approval_in_progress",
            "approval_conflict",
            "approval_config_stale",
            "approval_repository_unavailable",
            "approval_execution_failed",
            "content_input_blocked",
            "usage_budget_exceeded",
            "channel_delivery_failed",
        }
        assert {e.value for e in WorkerErrorCode} == expected

    def test_is_string(self) -> None:
        assert isinstance(WorkerErrorCode.MODEL_RUNTIME, str)


class TestWorkerTask:

    def test_accepts_valid_task(self) -> None:
        task = WorkerTask(**VALID_TASK_KWARGS)
        assert task.protocol_version == 1
        assert task.tenant_id == "tenant_a"
        assert task.message == "hello"

    def test_rejects_wrong_protocol_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "protocol_version": 2})

    def test_rejects_string_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "protocol_version": "1"})

    def test_rejects_bool_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "protocol_version": True})

    def test_rejects_float_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "protocol_version": 1.0})

    def test_rejects_invalid_uuid(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "request_id": "not-a-uuid"})

    def test_rejects_invalid_tenant_id(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "tenant_id": ""})

    def test_rejects_tenant_id_with_uppercase(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "tenant_id": "Tenant_A"})

    def test_rejects_blank_app_id(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "app_id": "  "})

    def test_rejects_blank_user_id(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "user_id": ""})

    def test_rejects_blank_channel(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "channel": "  "})

    def test_rejects_blank_session_id(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "session_id": ""})

    def test_rejects_blank_message(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "message": "  "})

    def test_rejects_zero_config_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "config_version": 0})

    def test_rejects_negative_config_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "config_version": -1})

    def test_rejects_bool_config_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "config_version": True})

    def test_rejects_string_config_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "config_version": "1"})

    def test_rejects_float_config_version(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "config_version": 1.0})

    def test_rejects_message_over_8000_chars(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "message": "x" * 8001})

    def test_accepts_message_exactly_8000_chars(self) -> None:
        task = WorkerTask(**{**VALID_TASK_KWARGS, "message": "x" * 8000})
        assert len(task.message) == 8000

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(Exception):
            WorkerTask(**{**VALID_TASK_KWARGS, "unknown_field": "value"})

    def test_does_not_transport_sdk_user_id(self) -> None:
        task = WorkerTask(**VALID_TASK_KWARGS)
        assert not hasattr(task, "sdk_user_id")

    def test_does_not_transport_prompt_or_model(self) -> None:
        task = WorkerTask(**VALID_TASK_KWARGS)
        assert not hasattr(task, "instruction")
        assert not hasattr(task, "model_profile")
        assert not hasattr(task, "allowed_tools")

    def test_strips_whitespace_in_string_fields(self) -> None:
        task = WorkerTask(**{**VALID_TASK_KWARGS, "app_id": "  app_demo  "})
        assert task.app_id == "app_demo"


class TestWorkerToolCallData:

    def test_accepts_valid_call(self) -> None:
        data = WorkerToolCallData(kind="call", name="get_current_time", args={})
        assert data.kind == "call"
        assert data.name == "get_current_time"

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(Exception):
            WorkerToolCallData(kind="call", name="tool", args={}, extra="bad")

    def test_rejects_wrong_kind(self) -> None:
        with pytest.raises(Exception):
            WorkerToolCallData(kind="result", name="tool", args={})


class TestWorkerToolResultData:

    def test_accepts_valid_result(self) -> None:
        data = WorkerToolResultData(kind="result", name="get_current_time", response={"result": "12:00"})
        assert data.kind == "result"

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(Exception):
            WorkerToolResultData(kind="result", name="tool", response={}, extra="bad")

    def test_rejects_wrong_kind(self) -> None:
        with pytest.raises(Exception):
            WorkerToolResultData(kind="call", name="tool", response={})


class TestWorkerChatResult:

    def test_accepts_success(self) -> None:
        result = WorkerChatResult(
            protocol_version=1,
            request_id=uuid.uuid4(),
            response="hello",
            error_code=None,
        )
        assert result.response == "hello"
        assert result.error_code is None

    def test_accepts_error(self) -> None:
        result = WorkerChatResult(
            protocol_version=1,
            request_id=uuid.uuid4(),
            response="",
            error_code=WorkerErrorCode.MODEL_CONFIGURATION,
        )
        assert result.response == ""
        assert result.error_code == WorkerErrorCode.MODEL_CONFIGURATION

    def test_rejects_success_with_error_code(self) -> None:
        with pytest.raises(Exception):
            WorkerChatResult(
                protocol_version=1,
                request_id=uuid.uuid4(),
                response="hello",
                error_code=WorkerErrorCode.MODEL_RUNTIME,
            )

    def test_rejects_error_with_nonempty_response(self) -> None:
        with pytest.raises(Exception):
            WorkerChatResult(
                protocol_version=1,
                request_id=uuid.uuid4(),
                response="leaked detail",
                error_code=WorkerErrorCode.MODEL_RUNTIME,
            )

    def test_allows_empty_response_without_error_code(self) -> None:
        result = WorkerChatResult(
            protocol_version=1,
            request_id=uuid.uuid4(),
            response="",
            error_code=None,
        )
        assert result.response == ""
        assert result.error_code is None

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(Exception):
            WorkerChatResult(
                protocol_version=1,
                request_id=uuid.uuid4(),
                response="ok",
                error_code=None,
                extra="bad",
            )

    def test_rejects_wrong_protocol_version(self) -> None:
        with pytest.raises(Exception):
            WorkerChatResult(
                protocol_version=2,
                request_id=uuid.uuid4(),
                response="ok",
                error_code=None,
            )


class TestWorkerEvent:

    def test_accepts_delta(self) -> None:
        event = WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="delta",
            data="hello",
            error_code=None,
        )
        assert event.type == "delta"
        assert event.data == "hello"

    def test_accepts_tool_call(self) -> None:
        tool_data = WorkerToolCallData(kind="call", name="get_current_time", args={})
        event = WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="tool",
            data=tool_data,
            error_code=None,
        )
        assert event.type == "tool"

    def test_accepts_tool_result(self) -> None:
        tool_data = WorkerToolResultData(kind="result", name="get_current_time", response={"r": "ok"})
        event = WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="tool",
            data=tool_data,
            error_code=None,
        )
        assert event.type == "tool"

    def test_accepts_done(self) -> None:
        event = WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="done",
            data=None,
            error_code=None,
        )
        assert event.type == "done"
        assert event.data is None

    def test_accepts_error(self) -> None:
        event = WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="error",
            data=None,
            error_code=WorkerErrorCode.MODEL_RUNTIME,
        )
        assert event.type == "error"
        assert event.error_code == WorkerErrorCode.MODEL_RUNTIME

    def test_rejects_delta_without_data(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="delta",
                data=None,
                error_code=None,
            )

    def test_rejects_delta_with_non_string_data(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="delta",
                data=123,
                error_code=None,
            )

    def test_rejects_tool_without_data(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="tool",
                data=None,
                error_code=None,
            )

    def test_rejects_done_with_data(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="done",
                data="unexpected",
                error_code=None,
            )

    def test_rejects_error_without_error_code(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="error",
                data=None,
                error_code=None,
            )

    def test_rejects_error_with_data(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="error",
                data="leaked",
                error_code=WorkerErrorCode.MODEL_RUNTIME,
            )

    def test_rejects_unknown_type(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="unknown",
                data=None,
                error_code=None,
            )

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="done",
                data=None,
                error_code=None,
                extra="bad",
            )

    def test_rejects_success_event_with_error_code(self) -> None:
        with pytest.raises(Exception):
            WorkerEvent(
                protocol_version=1,
                request_id=uuid.uuid4(),
                type="delta",
                data="text",
                error_code=WorkerErrorCode.MODEL_RUNTIME,
            )


# ── Stage 6A2: approval protocol (RED) ───────────────────────────────────────

import uuid as _uuid  # noqa: E402

from trpc_service.transport.models import (  # noqa: E402
    WorkerApprovalData, WorkerApprovalResult, WorkerApprovalTask,
)


def _approval_task(**over):
    base = {
        "protocol_version": 1,
        "request_id": _uuid.uuid4(),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "usr_v1_" + "a" * 48,
        "channel": "web_console",
        "session_id": "ses_v1_" + "b" * 48,
        "message_id": "decide-msg-1",
        "approval_id": _uuid.uuid4(),
        "decision": "approve",
    }
    base.update(over)
    return base


class TestApprovalProtocol:

    def test_worker_event_approval_combination_valid(self):
        from trpc_service.transport.models import WorkerEvent
        ev = WorkerEvent(
            protocol_version=1,
            request_id=_uuid.uuid4(),
            type="approval",
            data=WorkerApprovalData(approval_id=_uuid.uuid4(), tool_name="get_current_time"),
        )
        assert ev.type == "approval"

    def test_worker_event_approval_requires_structured_data(self):
        from trpc_service.transport.models import WorkerEvent
        with pytest.raises(ValidationError):
            WorkerEvent(protocol_version=1, request_id=_uuid.uuid4(), type="approval", data="not structured")
        with pytest.raises(ValidationError):
            WorkerEvent(protocol_version=1, request_id=_uuid.uuid4(), type="approval", data=None)

    def test_worker_event_approval_rejects_error_code(self):
        from trpc_service.transport.models import WorkerEvent
        with pytest.raises(ValidationError):
            WorkerEvent(
                protocol_version=1,
                request_id=_uuid.uuid4(),
                type="approval",
                data=WorkerApprovalData(approval_id=_uuid.uuid4(), tool_name="t"),
                error_code=WorkerErrorCode.MODEL_RUNTIME,
            )

    def test_approval_data_rejects_blank_tool_and_missing_fields(self):
        with pytest.raises(ValidationError):
            WorkerApprovalData(approval_id=_uuid.uuid4(), tool_name="   ")
        with pytest.raises(ValidationError):
            WorkerApprovalData(approval_id="not-a-uuid", tool_name="t")

    def test_approval_task_strict_fields(self):
        WorkerApprovalTask.model_validate(_approval_task())
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(extra_field=1))
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(decision="maybe"))
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(protocol_version=2))
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(approval_id="bad"))
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(message_id=""))
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate(_approval_task(message_id="m" * 201))

    def test_approval_result_error_combination(self):
        rid = _uuid.uuid4()
        WorkerApprovalResult(protocol_version=1, request_id=rid, response="final text", error_code=None)
        WorkerApprovalResult(
            protocol_version=1,
            request_id=rid,
            response="",
            error_code=WorkerErrorCode.APPROVAL_CONFLICT,
        )
        with pytest.raises(ValidationError):
            WorkerApprovalResult(protocol_version=1,
                                 request_id=rid,
                                 response="text",
                                 error_code=WorkerErrorCode.APPROVAL_CONFLICT)

    def test_new_error_codes_present(self):
        for name, value in (
            ("APPROVAL_NOT_AVAILABLE", "approval_not_available"),
            ("APPROVAL_IN_PROGRESS", "approval_in_progress"),
            ("APPROVAL_CONFLICT", "approval_conflict"),
            ("APPROVAL_CONFIG_STALE", "approval_config_stale"),
            ("APPROVAL_REPOSITORY_UNAVAILABLE", "approval_repository_unavailable"),
            ("APPROVAL_EXECUTION_FAILED", "approval_execution_failed"),
        ):
            assert getattr(WorkerErrorCode, name).value == value


class TestApprovalTaskProtocolStrictness:
    """P1-2: identical before-validation surface as WorkerTask."""

    def test_rejects_bool_protocol_version(self):
        import uuid as _uuid
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate({
                "protocol_version": True,
                "request_id": str(_uuid.uuid4()),
                "tenant_id": "tenant_a",
                "app_id": "app",
                "config_version": 1,
                "user_id": "u",
                "channel": "web_console",
                "session_id": "s",
                "message_id": "m",
                "approval_id": str(_uuid.uuid4()),
                "decision": "approve",
            })

    def test_rejects_float_protocol_version(self):
        import uuid as _uuid
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate({
                "protocol_version": 1.0,
                "request_id": str(_uuid.uuid4()),
                "tenant_id": "tenant_a",
                "app_id": "app",
                "config_version": 1,
                "user_id": "u",
                "channel": "web_console",
                "session_id": "s",
                "message_id": "m",
                "approval_id": str(_uuid.uuid4()),
                "decision": "reject",
            })

    def test_rejects_bool_config_version(self):
        import uuid as _uuid
        with pytest.raises(ValidationError):
            WorkerApprovalTask.model_validate({
                "protocol_version": 1,
                "request_id": str(_uuid.uuid4()),
                "tenant_id": "tenant_a",
                "app_id": "app",
                "config_version": True,
                "user_id": "u",
                "channel": "web_console",
                "session_id": "s",
                "message_id": "m",
                "approval_id": str(_uuid.uuid4()),
                "decision": "approve",
            })
