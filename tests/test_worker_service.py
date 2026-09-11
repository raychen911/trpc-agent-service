"""Tests for WorkerService: task resolution, chat/stream error mapping, protocol events."""

from __future__ import annotations

import uuid

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, FunctionCall, Part

from trpc_service.agent.app import AgentApp
from trpc_service.config import ModelConfigurationError
from trpc_service.config.tenant import TenantConfig
from trpc_service.storage.message_repository import (
    MessageClaim,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
)
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerTask,
)
from trpc_service.worker.service import WorkerService

from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    FakeTenantConfigRepository,
    make_in_memory_state_backend,
    make_default_test_configs,
)


def _make_mock_state_backend():
    return make_in_memory_state_backend()


def _task(
    tenant_id: str = "tenant_default",
    app_id: str = "app_demo",
    config_version: int = 1,
    **overrides,
) -> WorkerTask:
    defaults = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "app_id": app_id,
        "config_version": config_version,
        "user_id": "user_default",
        "channel": "web",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": "hello",
    }
    defaults.update(overrides)
    return WorkerTask(**defaults)


def _make_service(
    model: FakeLLMModel | None = None,
    configs: dict[str, TenantConfig] | None = None,
) -> tuple[WorkerService, FakeLLMModel]:
    if model is None:
        model = FakeLLMModel()
    repo = FakeTenantConfigRepository(configs or make_default_test_configs())
    provider = FakeModelProvider({"default": model})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    return WorkerService(tenant_repository=repo, agent_app=agent_app), model


class _CompletingReceiptRepository:
    """Claim execution, then simulate a terminal audit persistence failure."""

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        del task, message_text
        return MessageClaim(
            action=ReceiptAction.EXECUTE,
            receipt_id=uuid.uuid4(),
            response_text=None,
            error_code=None,
        )

    async def complete(self, receipt_id, response_text, latency_ms, execution_events=()) -> None:
        del receipt_id, response_text, latency_ms, execution_events
        raise MessageReceiptRepositoryUnavailableError("database unavailable")

    async def fail(self, receipt_id, error_code, latency_ms, execution_events=()) -> None:
        del receipt_id, error_code, latency_ms, execution_events


class _FailedReplayReceiptRepository:
    """Return a terminal failed receipt without entering model execution."""

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        del task, message_text
        return MessageClaim(
            action=ReceiptAction.REPLAY,
            receipt_id=uuid.uuid4(),
            response_text=None,
            error_code=WorkerErrorCode.MODEL_RUNTIME,
        )


def _make_service_with_receipts(receipt_repository: object) -> tuple[WorkerService, FakeLLMModel]:
    model = FakeLLMModel()
    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": model}),
        state_backend=_make_mock_state_backend(),
    )
    return WorkerService(
        tenant_repository=tenant_repository,
        agent_app=agent_app,
        receipt_repository=receipt_repository,
    ), model


# ---------------------------------------------------------------------------
# chat() — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_success_returns_response_text() -> None:
    model = FakeLLMModel()
    service, _ = _make_service(model)
    task = _task()
    result = await service.chat(task)
    assert isinstance(result, WorkerChatResult)
    assert result.protocol_version == 1
    assert result.request_id == task.request_id
    assert result.response == "OK"
    assert result.error_code is None
    assert model.call_count == 1


@pytest.mark.asyncio
async def test_chat_preserves_request_id() -> None:
    service, _ = _make_service()
    rid = uuid.uuid4()
    task = _task(request_id=rid)
    result = await service.chat(task)
    assert result.request_id == rid


@pytest.mark.asyncio
async def test_chat_terminal_audit_failure_returns_safe_repository_error() -> None:
    """A completed model result is not reported successful when audit commit fails."""
    service, model = _make_service_with_receipts(_CompletingReceiptRepository())

    result = await service.chat(_task())

    assert model.call_count == 1
    assert result.response == ""
    assert result.error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_terminal_audit_failure_never_emits_done() -> None:
    """The stream terminal success marker follows durable completion, not model output."""
    service, model = _make_service_with_receipts(_CompletingReceiptRepository())

    events = [event async for event in service.stream(_task())]

    assert model.call_count == 1
    assert [event.type for event in events] == ["delta", "error"]
    assert events[-1].error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_failed_receipt_replays_its_terminal_error() -> None:
    """A failed receipt must not be presented as a successful empty stream."""
    model = FakeLLMModel()
    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": model}),
        state_backend=_make_mock_state_backend(),
    )
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=agent_app,
        receipt_repository=_FailedReplayReceiptRepository(),
    )

    events = [event async for event in service.stream(_task())]

    assert model.call_count == 0
    assert [event.type for event in events] == ["error"]
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


@pytest.mark.asyncio
async def test_stream_completed_receipt_replays_cached_response() -> None:
    """A completed receipt must replay only delta + done, no tool events."""

    class _CompletedReplayReceiptRepository:
        """Return a completed receipt with cached response."""

        async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
            del task, message_text
            return MessageClaim(
                action=ReceiptAction.REPLAY,
                receipt_id=uuid.uuid4(),
                response_text="cached response text",
                error_code=None,
            )

        async def complete(self, receipt_id, response_text, latency_ms, execution_events=()) -> None:
            del receipt_id, response_text, latency_ms

        async def fail(self, receipt_id, error_code, latency_ms, execution_events=()) -> None:
            del receipt_id, error_code, latency_ms

    model = FakeLLMModel()
    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": model}),
        state_backend=_make_mock_state_backend(),
    )
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=agent_app,
        receipt_repository=_CompletedReplayReceiptRepository(),
    )

    events = [event async for event in service.stream(_task())]

    # Model should not be called
    assert model.call_count == 0
    # Should emit delta with cached response, then done
    assert len(events) == 2
    assert events[0].type == "delta"
    assert events[0].data == "cached response text"
    assert events[1].type == "done"
    # No tool events should be present
    assert all(event.type in ("delta", "done") for event in events)


@pytest.mark.asyncio
async def test_chat_queries_repository_exactly_once() -> None:
    """Verify that chat() queries the repository exactly once."""
    repo = FakeTenantConfigRepository(make_default_test_configs())
    query_count = {"count": 0}
    original_get = repo.get

    async def counting_get(tenant_id: str):
        query_count["count"] += 1
        return await original_get(tenant_id)

    repo.get = counting_get
    model = FakeLLMModel()
    provider = FakeModelProvider({"default": model})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    task = _task()
    await service.chat(task)
    assert query_count["count"] == 1


# ---------------------------------------------------------------------------
# chat() — tenant resolution errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_unknown_tenant_returns_tenant_config_mismatch() -> None:
    service, model = _make_service()
    task = _task(tenant_id="nonexistent")
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH
    assert result.response == ""
    assert model.call_count == 0


@pytest.mark.asyncio
async def test_chat_disabled_tenant_returns_tenant_config_mismatch() -> None:
    service, model = _make_service()
    task = _task(tenant_id="tenant_disabled")
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH
    assert model.call_count == 0


@pytest.mark.asyncio
async def test_chat_app_id_mismatch_returns_tenant_config_mismatch() -> None:
    service, model = _make_service()
    task = _task(app_id="wrong_app")
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH
    assert model.call_count == 0


@pytest.mark.asyncio
async def test_chat_config_version_mismatch_returns_tenant_config_mismatch() -> None:
    service, model = _make_service()
    task = _task(config_version=999)
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH
    assert model.call_count == 0


@pytest.mark.asyncio
async def test_chat_repository_exception_returns_model_runtime() -> None:
    """When repository raises unexpected exception, chat returns model_runtime error."""

    class _RaisingRepository:

        async def get(self, tenant_id: str):
            raise RuntimeError("Database connection failed")

    agent_app = AgentApp(model_provider=FakeModelProvider({}), state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=_RaisingRepository(), agent_app=agent_app)
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.MODEL_RUNTIME
    assert result.response == ""


# ---------------------------------------------------------------------------
# chat() — agent/model errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_tenant_agent_configuration_error() -> None:
    """When AgentApp raises TenantAgentConfigurationError, result maps to tenant_agent_configuration."""
    from trpc_service.agent.errors import TenantAgentConfigurationError

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise TenantAgentConfigurationError()
            yield  # pragma: no cover

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.TENANT_AGENT_CONFIGURATION
    assert result.response == ""


@pytest.mark.asyncio
async def test_chat_model_configuration_error() -> None:
    """When AgentApp raises ModelConfigurationError, result maps to model_configuration."""

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise ModelConfigurationError("missing")
            yield  # pragma: no cover

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.MODEL_CONFIGURATION
    assert result.response == ""


@pytest.mark.asyncio
async def test_chat_generic_exception_returns_model_runtime() -> None:
    """Unexpected exceptions map to model_runtime."""

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise RuntimeError("secret internal detail")
            yield  # pragma: no cover

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.MODEL_RUNTIME
    assert result.response == ""


@pytest.mark.asyncio
async def test_chat_event_error_code_returns_model_runtime() -> None:
    """When the agent yields an event with error_code, result maps to model_runtime."""
    from unittest.mock import MagicMock

    error_event = MagicMock(spec=Event)
    error_event.error_code = "model_error"
    error_event.content = None
    error_event.is_final_response = MagicMock(return_value=False)

    class _ErrorAgentApp:

        def __init__(self):
            pass

        async def run(self, **kwargs):
            yield error_event

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_ErrorAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.MODEL_RUNTIME
    assert result.response == ""


# ---------------------------------------------------------------------------
# stream() — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_success_emits_delta_then_done() -> None:
    service, model = _make_service()
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)

    assert len(events) >= 2
    delta_events = [e for e in events if e.type == "delta"]
    assert len(delta_events) >= 1
    assert events[-1].type == "done"
    assert events[-1].data is None
    assert events[-1].error_code is None
    for e in events:
        assert e.request_id == task.request_id
        assert e.protocol_version == 1


@pytest.mark.asyncio
async def test_stream_delta_data_is_text() -> None:
    service, _ = _make_service()
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    delta_events = [e for e in events if e.type == "delta"]
    for e in delta_events:
        assert isinstance(e.data, str)


@pytest.mark.asyncio
async def test_chat_empty_final_response() -> None:
    """Verify that chat() can return empty response string when model produces no text."""
    from unittest.mock import MagicMock

    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    empty_event = MagicMock(spec=Event)
    empty_event.error_code = None
    empty_event.content = Content(parts=[Part(text="")])
    empty_event.partial = False
    empty_event.id = "evt-1"
    empty_event.is_final_response = MagicMock(return_value=True)

    class _EmptyAgentApp:

        def __init__(self):
            pass

        async def run(self, **kwargs):
            yield empty_event

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_EmptyAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.response == ""
    assert result.error_code is None


# ---------------------------------------------------------------------------
# stream() — tenant resolution errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_unknown_tenant_emits_error_event() -> None:
    service, model = _make_service()
    task = _task(tenant_id="nonexistent")
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH
    assert model.call_count == 0


@pytest.mark.asyncio
async def test_stream_disabled_tenant_emits_error_event() -> None:
    service, model = _make_service()
    task = _task(tenant_id="tenant_disabled")
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH


@pytest.mark.asyncio
async def test_stream_app_id_mismatch_emits_error_event() -> None:
    service, _ = _make_service()
    task = _task(app_id="wrong_app")
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH


@pytest.mark.asyncio
async def test_stream_version_mismatch_emits_error_event() -> None:
    service, _ = _make_service()
    task = _task(config_version=999)
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_CONFIG_MISMATCH


@pytest.mark.asyncio
async def test_stream_repository_exception_emits_error_event() -> None:
    """When repository raises unexpected exception, stream emits error/model_runtime."""

    class _RaisingRepository:

        async def get(self, tenant_id: str):
            raise RuntimeError("Database connection failed")

    agent_app = AgentApp(model_provider=FakeModelProvider({}), state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=_RaisingRepository(), agent_app=agent_app)
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


@pytest.mark.asyncio
async def test_stream_partial_final_dedup() -> None:
    """Verify that final event with same ID as partial is skipped."""
    from unittest.mock import MagicMock

    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    partial_event = MagicMock(spec=Event)
    partial_event.error_code = None
    partial_event.content = Content(parts=[Part(text="partial")])
    partial_event.partial = True
    partial_event.id = "evt-dup"

    final_event = MagicMock(spec=Event)
    final_event.error_code = None
    final_event.content = Content(parts=[Part(text="final")])
    final_event.partial = False
    final_event.id = "evt-dup"

    class _DedupAgentApp:

        def __init__(self):
            pass

        async def run(self, **kwargs):
            yield partial_event
            yield final_event

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_DedupAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    delta_events = [e for e in events if e.type == "delta"]
    assert len(delta_events) == 1
    assert delta_events[0].data == "partial"


# ---------------------------------------------------------------------------
# stream() — agent/model errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_tenant_agent_configuration_error() -> None:
    from trpc_service.agent.errors import TenantAgentConfigurationError

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise TenantAgentConfigurationError()
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_AGENT_CONFIGURATION


@pytest.mark.asyncio
async def test_stream_model_configuration_error() -> None:

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise ModelConfigurationError("missing")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_CONFIGURATION


@pytest.mark.asyncio
async def test_stream_generic_exception_returns_model_runtime() -> None:

    class _RaisingAgentApp:

        async def run(self, **kwargs):
            raise RuntimeError("secret")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_RaisingAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


# ---------------------------------------------------------------------------
# stream() — tool call/result forwarding
# ---------------------------------------------------------------------------


class _SequenceLLMModel(FakeLLMModel):
    """Serves a scripted sequence of model calls for multi-turn tool tests."""

    def __init__(self, calls: list[list]) -> None:
        super().__init__()
        self._calls = list(calls)
        self._call_index = 0

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.calls.append(list(request.contents))
        if self._call_index >= len(self._calls):
            raise RuntimeError("sequence exhausted")
        responses = self._calls[self._call_index]
        self._call_index += 1
        for r in responses:
            yield r


@pytest.mark.asyncio
async def test_stream_emits_tool_call_and_result_events() -> None:
    """Two-call flow: function_call → tool result → final text."""
    from trpc_agent_sdk.models import LlmResponse

    model = _SequenceLLMModel([
        [
            LlmResponse(
                content=Content(
                    role="model",
                    parts=[Part(function_call=FunctionCall(name="get_current_time", args={}))],
                ),
                partial=False,
            ),
        ],
        [
            LlmResponse(
                content=Content(role="model", parts=[Part.from_text(text="done")]),
                partial=False,
            ),
        ],
    ])
    service, _ = _make_service(model=model)
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)

    types = [e.type for e in events]
    assert "error" not in types
    tool_events = [e for e in events if e.type == "tool"]
    assert len(tool_events) == 2
    call_event = tool_events[0]
    result_event = tool_events[1]
    assert call_event.data.kind == "call"
    assert call_event.data.name == "get_current_time"
    assert result_event.data.kind == "result"
    assert result_event.data.name == "get_current_time"
    delta_events = [e for e in events if e.type == "delta"]
    assert delta_events[-1].data == "done"
    assert events[-1].type == "done"


@pytest.mark.asyncio
async def test_stream_event_error_code_emits_error_event() -> None:
    """When agent yields an event with error_code during stream, emit error event."""
    from unittest.mock import MagicMock

    error_event = MagicMock(spec=Event)
    error_event.error_code = "model_error"
    error_event.content = None
    error_event.is_final_response = MagicMock(return_value=False)

    class _ErrorAgentApp:

        async def run(self, **kwargs):
            yield error_event

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_ErrorAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


# ---------------------------------------------------------------------------
# chat()/stream() — session_busy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_session_busy_returns_session_busy() -> None:
    from trpc_service.agent.execution_coordinator import SessionBusyError

    class _BusyAgentApp:

        async def run(self, **kwargs):
            raise SessionBusyError("busy")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_BusyAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.SESSION_BUSY
    assert result.response == ""


@pytest.mark.asyncio
async def test_chat_session_execution_lost_returns_model_runtime() -> None:
    from trpc_service.agent.execution_coordinator import SessionExecutionLostError

    class _LostAgentApp:

        async def run(self, **kwargs):
            raise SessionExecutionLostError("lease lost")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_LostAgentApp())
    task = _task()
    result = await service.chat(task)
    assert result.error_code == WorkerErrorCode.MODEL_RUNTIME
    assert result.response == ""


@pytest.mark.asyncio
async def test_stream_session_busy_emits_error_event() -> None:
    from trpc_service.agent.execution_coordinator import SessionBusyError

    class _BusyAgentApp:

        async def run(self, **kwargs):
            raise SessionBusyError("busy")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_BusyAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.SESSION_BUSY


@pytest.mark.asyncio
async def test_stream_session_execution_lost_emits_model_runtime() -> None:
    from trpc_service.agent.execution_coordinator import SessionExecutionLostError

    class _LostAgentApp:

        async def run(self, **kwargs):
            raise SessionExecutionLostError("lease lost")
            yield  # pragma: no cover

        async def close(self):
            pass

    repo = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(tenant_repository=repo, agent_app=_LostAgentApp())
    task = _task()
    events = []
    async for event in service.stream(task):
        events.append(event)
    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_delegates_to_agent_app() -> None:
    service, _ = _make_service()
    closed = {"count": 0}
    original_close = service._agent_app.close  # noqa: SLF001

    async def tracking_close():
        closed["count"] += 1
        await original_close()

    service._agent_app.close = tracking_close  # noqa: SLF001
    await service.close()
    assert closed["count"] == 1


# ---------------------------------------------------------------------------
# chat()/stream() — fail() exception handling
# ---------------------------------------------------------------------------


class _FailingFailReceiptRepository:
    """Claim execution, then fail() raises an exception."""

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        del task, message_text
        return MessageClaim(
            action=ReceiptAction.EXECUTE,
            receipt_id=uuid.uuid4(),
            response_text=None,
            error_code=None,
        )

    async def complete(self, receipt_id: uuid.UUID, response_text: str, latency_ms: int, execution_events=()) -> None:
        del receipt_id, response_text, latency_ms

    async def fail(self, receipt_id, error_code, latency_ms, execution_events=()) -> None:
        del receipt_id, error_code, latency_ms, execution_events
        raise MessageReceiptRepositoryUnavailableError("database unavailable")


@pytest.mark.asyncio
async def test_chat_fail_exception_returns_safe_repository_error() -> None:
    """When fail() raises, chat() must return tenant_repository_unavailable, not leak exception."""
    from trpc_service.agent.errors import TenantAgentConfigurationError

    class _ConfigErrorAgentApp:

        async def run(self, **kwargs):
            raise TenantAgentConfigurationError("bad config")
            yield  # pragma: no cover

        async def close(self):
            pass

    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=_ConfigErrorAgentApp(),
        receipt_repository=_FailingFailReceiptRepository(),
    )

    result = await service.chat(_task())

    assert result.response == ""
    assert result.error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_fail_exception_returns_safe_repository_error() -> None:
    """When fail() raises, stream() must emit error with tenant_repository_unavailable."""
    from trpc_service.agent.errors import TenantAgentConfigurationError

    class _ConfigErrorAgentApp:

        async def run(self, **kwargs):
            raise TenantAgentConfigurationError("bad config")
            yield  # pragma: no cover

        async def close(self):
            pass

    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=_ConfigErrorAgentApp(),
        receipt_repository=_FailingFailReceiptRepository(),
    )

    events = [event async for event in service.stream(_task())]

    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_chat_model_runtime_fail_exception_returns_safe_error() -> None:
    """When model raises generic Exception and fail() raises, return tenant_repository_unavailable."""

    class _RuntimeErrorAgentApp:

        async def run(self, **kwargs):
            raise RuntimeError("unexpected error")
            yield  # pragma: no cover

        async def close(self):
            pass

    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=_RuntimeErrorAgentApp(),
        receipt_repository=_FailingFailReceiptRepository(),
    )

    result = await service.chat(_task())

    assert result.response == ""
    assert result.error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_chat_event_error_fail_exception_returns_safe_error() -> None:
    """When event stream yields error and fail() raises, return tenant_repository_unavailable."""

    class _EventErrorAgentApp:

        async def run(self, **kwargs):
            from trpc_agent_sdk.events import Event

            error_event = Event(
                id="evt-1",
                session_id="sess-1",
                content=None,
                partial=False,
                error_code="model_error",
            )
            yield error_event

        async def close(self):
            pass

    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=_EventErrorAgentApp(),
        receipt_repository=_FailingFailReceiptRepository(),
    )

    result = await service.chat(_task())

    assert result.response == ""
    assert result.error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_event_error_fail_raises_emits_exactly_one_error() -> None:
    """When agent yields error event and fail() raises, stream yields exactly one error.

    The single error must be TENANT_REPOSITORY_UNAVAILABLE (not the original
    business error), proving the persist-before-yield contract.
    """

    class _EventErrorStreamAgentApp:

        async def run(self, **kwargs):
            from unittest.mock import MagicMock

            error_event = MagicMock(spec=Event)
            error_event.error_code = "model_error"
            error_event.content = None
            error_event.is_final_response = MagicMock(return_value=False)
            yield error_event

        async def close(self):
            pass

    tenant_repository = FakeTenantConfigRepository(make_default_test_configs())
    service = WorkerService(
        tenant_repository=tenant_repository,
        agent_app=_EventErrorStreamAgentApp(),
        receipt_repository=_FailingFailReceiptRepository(),
    )

    events = []
    async for event in service.stream(_task()):
        events.append(event)

    error_events = [e for e in events if e.type == "error"]
    assert len(error_events) == 1, (f"Expected exactly 1 terminal error event, got {len(error_events)}: "
                                    f"{[(e.type, e.error_code) for e in error_events]}")
    assert error_events[0].error_code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE
