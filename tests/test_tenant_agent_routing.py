"""Tests for TenantAgentRuntime: instruction, tools, app_name, context validation."""

from __future__ import annotations

import asyncio

import pytest
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.runtime import TenantAgentRuntime
from trpc_service.agent.tools import get_current_time
from trpc_service.tenant.context import TenantContext
from tests.tenant_helpers import (
    FakeLLMModel,
    make_app_config,
    make_in_memory_state_backend,
    make_tenant_config,
)


def _make_mock_state_backend():
    return make_in_memory_state_backend()


def _context(
    tenant_id: str = "tenant_a",
    app_id: str = "app_demo",
) -> TenantContext:
    return TenantContext(tenant_id=tenant_id, app_id=app_id, user_id="user_default", channel="web")


def _consume(runtime: TenantAgentRuntime, context: TenantContext, session_id: str, user_input: str) -> list:

    async def _drive() -> list:
        events = []
        async for event in runtime.run(context=context, session_id=session_id, user_input=user_input):
            events.append(event)
        return events

    return asyncio.run(_drive())


def test_runtime_configured_instruction_reaches_agent():
    config = make_tenant_config("tenant_a", app=make_app_config(instruction="Custom prompt here."))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    assert runtime._agent.instruction == "Custom prompt here."  # noqa: SLF001


def test_runtime_tools_are_exactly_supplied():
    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    tool = FunctionTool(get_current_time)
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[tool], state_backend=mock_backend)
    # preload_memory_tool is always added, so expect 2 tools total
    assert len(runtime._agent.tools) == 2  # noqa: SLF001
    assert runtime._agent.tools[0] is tool  # noqa: SLF001


def test_runtime_runner_app_name_from_config():
    config = make_tenant_config("tenant_a", app=make_app_config(app_id="my_app"))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    # app_name uses namespace format: {tenant_id}:{app_id}:v{version}
    assert runtime._runner.app_name == "tenant_a:my_app:v1"  # noqa: SLF001


def test_runtime_rejects_mismatched_tenant_id():
    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    bad_ctx = _context(tenant_id="tenant_b")

    with pytest.raises(TenantAgentConfigurationError):
        _consume(runtime, bad_ctx, "s1", "hi")
    assert model.call_count == 0


def test_runtime_rejects_mismatched_app_id():
    config = make_tenant_config("tenant_a", app=make_app_config(app_id="app_demo"))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    bad_ctx = _context(app_id="wrong_app")

    with pytest.raises(TenantAgentConfigurationError):
        _consume(runtime, bad_ctx, "s1", "hi")
    assert model.call_count == 0


def test_runtime_rejects_non_identifier_app_id():
    config = make_tenant_config("tenant_a", app=make_app_config(app_id="not valid!"))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    with pytest.raises(TenantAgentConfigurationError):
        TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)


def test_runtime_rejects_reserved_user_app_id():
    config = make_tenant_config("tenant_a", app=make_app_config(app_id="user"))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    with pytest.raises(TenantAgentConfigurationError):
        TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)


def test_runtime_error_does_not_leak_app_id():
    config = make_tenant_config("tenant_a", app=make_app_config(app_id="secret-app"))
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    with pytest.raises(TenantAgentConfigurationError) as exc_info:
        TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    assert "secret-app" not in str(exc_info.value)


def test_runtime_memory_is_isolated_across_sessions_for_same_user():
    """Regression: same user's direct chat memory must not leak into group chat."""
    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=backend)

    user_id = "usr_v1_abc"
    direct_ctx = TenantContext(
        tenant_id="tenant_a",
        app_id="app_demo",
        user_id=user_id,
        channel="wecom",
        session_id="ses_v1_direct",
    )
    group_ctx = TenantContext(
        tenant_id="tenant_a",
        app_id="app_demo",
        user_id=user_id,
        channel="wecom",
        session_id="ses_v1_group",
    )

    _consume(runtime, direct_ctx, "session_direct", "call me 一号")
    _consume(runtime, group_ctx, "session_group", "hi")

    # The second model call is for the group session; it must not contain the
    # direct-session memory injected by preload_memory_tool.
    second_call = model.calls[1]
    all_text = ""
    for content in second_call:
        for part in content.parts:
            if part.text:
                all_text += part.text
    assert "一号" not in all_text


def test_runtime_run_produces_events():
    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    ctx = _context()
    events = _consume(runtime, ctx, "s1", "hello")
    assert events
    assert model.call_count == 1


def test_runtime_close_calls_runner_close():
    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    runtime = TenantAgentRuntime(config=config, model=model, tools=[], state_backend=mock_backend)
    closed = {"count": 0}
    original_close = runtime._runner.close  # noqa: SLF001

    async def tracking_close():
        closed["count"] += 1
        await original_close()

    runtime._runner.close = tracking_close  # noqa: SLF001
    asyncio.run(runtime.close())
    assert closed["count"] == 1


# ---------------------------------------------------------------------------
# Coordinator integration
# ---------------------------------------------------------------------------


class _FakeLease:

    def __init__(self, lose_after: int | None = None) -> None:
        self._lose_after = lose_after
        self._event_count = 0
        self._lost = False

    def ensure_valid(self) -> None:
        self._event_count += 1
        if self._lose_after is not None and self._event_count >= self._lose_after:
            self._lost = True
            raise _LostError("lease lost")


class _LostError(Exception):
    pass


class _FakeCoordinator:

    def __init__(
        self,
        acquire_order: list | None = None,
        release_order: list | None = None,
        lose_after: int | None = None,
        busy: bool = False,
    ) -> None:
        self._acquire_order = acquire_order
        self._release_order = release_order
        self._lose_after = lose_after
        self._busy = busy
        self.close_count = 0

    async def close(self) -> None:
        self.close_count += 1

    @property
    def acquire(self):
        coord = self

        class _CM:

            def __init__(self, identity):
                self._identity = identity

            async def __aenter__(self):
                if coord._acquire_order is not None:
                    coord._acquire_order.append("redis_lock")
                if coord._busy:
                    from trpc_service.agent.execution_coordinator import SessionBusyError
                    raise SessionBusyError("busy")
                self._lease = _FakeLease(lose_after=coord._lose_after)
                return self._lease

            async def __aexit__(self, *exc_info):
                if coord._release_order is not None:
                    coord._release_order.append("redis_release")
                return False

        return _AcquireFactory(_CM)


class _AcquireFactory:

    def __init__(self, cm_class):
        self._cm_class = cm_class

    def __call__(self, identity):
        return self._cm_class(identity)


def test_runtime_with_coordinator_acquires_local_then_redis():
    """Lock ordering: local lock → Redis lock."""
    order: list[str] = []

    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    coordinator = _FakeCoordinator(acquire_order=order)
    runtime = TenantAgentRuntime(config=config,
                                 model=model,
                                 tools=[],
                                 state_backend=mock_backend,
                                 coordinator=coordinator)

    original_run = runtime._runner.run_async  # noqa: SLF001

    async def tracking_run(*args, **kwargs):
        order.append("runner_start")
        async for event in original_run(*args, **kwargs):
            yield event

    runtime._runner.run_async = tracking_run  # noqa: SLF001
    ctx = _context()
    events = _consume(runtime, ctx, "s1", "hello")
    assert events
    assert order.index("runner_start") > order.index("redis_lock")


def test_runtime_coordinator_busy_raises_session_busy():
    """When coordinator raises SessionBusyError, it propagates out of run()."""
    from trpc_service.agent.execution_coordinator import SessionBusyError

    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    coordinator = _FakeCoordinator(busy=True)
    runtime = TenantAgentRuntime(config=config,
                                 model=model,
                                 tools=[],
                                 state_backend=mock_backend,
                                 coordinator=coordinator)
    ctx = _context()

    with pytest.raises(SessionBusyError):
        _consume(runtime, ctx, "s1", "hello")
    assert model.call_count == 0


def test_runtime_coordinator_lease_loss_stops_events():
    """When lease is lost mid-stream, no further events are yielded and error propagates."""
    from unittest.mock import MagicMock
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    coordinator = _FakeCoordinator(lose_after=2)
    runtime = TenantAgentRuntime(config=config,
                                 model=model,
                                 tools=[],
                                 state_backend=mock_backend,
                                 coordinator=coordinator)

    event_1 = MagicMock(spec=Event)
    event_1.error_code = None
    event_1.content = Content(parts=[Part.from_text(text="first")])
    event_1.partial = False
    event_1.id = "evt-1"
    event_1.is_final_response = MagicMock(return_value=True)

    event_2 = MagicMock(spec=Event)
    event_2.error_code = None
    event_2.content = Content(parts=[Part.from_text(text="second")])
    event_2.partial = False
    event_2.id = "evt-2"
    event_2.is_final_response = MagicMock(return_value=True)

    async def fake_run_async(**kwargs):
        yield event_1
        yield event_2

    runtime._runner.run_async = fake_run_async  # noqa: SLF001
    ctx = _context()

    collected: list = []

    async def _drive():
        async for event in runtime.run(context=ctx, session_id="s1", user_input="hello"):
            collected.append(event)

    with pytest.raises(_LostError):
        asyncio.run(_drive())
    assert len(collected) == 1
    assert collected[0].content.parts[0].text == "first"


def test_runtime_same_session_serialized_with_coordinator():
    """Two runs on the same session are serialized by the local lock."""
    execution_order: list[str] = []

    config = make_tenant_config("tenant_a")
    model = FakeLLMModel()
    mock_backend = _make_mock_state_backend()
    coordinator = _FakeCoordinator()
    runtime = TenantAgentRuntime(config=config,
                                 model=model,
                                 tools=[],
                                 state_backend=mock_backend,
                                 coordinator=coordinator)

    original_run = runtime._runner.run_async  # noqa: SLF001

    async def tracking_run_a(*args, **kwargs):
        execution_order.append("a_start")
        await asyncio.sleep(0.05)
        execution_order.append("a_end")
        async for event in original_run(*args, **kwargs):
            yield event

    async def tracking_run_b(*args, **kwargs):
        execution_order.append("b_start")
        execution_order.append("b_end")
        async for event in original_run(*args, **kwargs):
            yield event

    ctx = _context()

    async def _drive():
        runtime._runner.run_async = tracking_run_a  # noqa: SLF001
        task_a = asyncio.create_task(_collect_events(runtime, ctx, "s1", "hello"))
        await asyncio.sleep(0.01)
        runtime._runner.run_async = tracking_run_b  # noqa: SLF001
        task_b = asyncio.create_task(_collect_events(runtime, ctx, "s1", "hello"))
        await asyncio.gather(task_a, task_b)

    asyncio.run(_drive())
    a_end = execution_order.index("a_end")
    b_start = execution_order.index("b_start")
    assert a_end < b_start


async def _collect_events(runtime, ctx, session_id, user_input):
    events = []
    async for event in runtime.run(context=ctx, session_id=session_id, user_input=user_input):
        events.append(event)
    return events
