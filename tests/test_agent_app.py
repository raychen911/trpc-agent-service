"""Stage 3A AgentApp tests: pure runtime manager without repository."""

from __future__ import annotations

import asyncio
import datetime as _dt
import unittest.mock

import pytest

from trpc_service.agent.app import AgentApp
from trpc_service.storage.state_backend import AgentStateBackend
from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.tools import get_current_time
from trpc_service.config.tenant import TenantConfig
from trpc_service.tenant.context import TenantContext
from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    make_backend_profile,
    make_in_memory_state_backend,
    make_app_config,
    make_default_test_configs,
    make_governance,
    make_tenant_config,
)


def test_in_memory_test_backend_disables_cleanup_tasks() -> None:
    backend = make_in_memory_state_backend()

    assert not backend.session_service._session_config.need_ttl_expire()  # noqa: SLF001
    assert not backend.memory_service._memory_service_config.ttl.need_ttl_expire()  # noqa: SLF001

    asyncio.run(backend.close())


def _context(tenant_id: str = "tenant_default", app_id: str = "app_demo") -> TenantContext:
    return TenantContext(tenant_id=tenant_id, app_id=app_id, user_id="user_default", channel="web")


def _make_mock_backend() -> AgentStateBackend:
    return make_in_memory_state_backend()


def _make_app(
    models: dict | None = None,
    state_backend: AgentStateBackend | None = None,
) -> tuple[AgentApp, FakeModelProvider]:
    if models is None:
        models = {"default": FakeLLMModel()}
    provider = FakeModelProvider(models)
    if state_backend is None:
        state_backend = _make_mock_backend()
    app = AgentApp(model_provider=provider, state_backend=state_backend)
    return app, provider


def _consume(
    app: AgentApp,
    config: TenantConfig,
    context: TenantContext,
    session_id: str,
    user_input: str,
) -> None:

    async def _drive() -> None:
        async for _ in app.run(config=config, context=context, session_id=session_id, user_input=user_input):
            pass

    asyncio.run(_drive())


def _config(configs: dict[str, TenantConfig], tenant_id: str) -> TenantConfig:
    return configs[tenant_id]


async def _get_session(app: AgentApp, config: TenantConfig, context: TenantContext, session_id: str):
    runtime = app._get_or_create_runtime(config, context)  # noqa: SLF001
    # Use the same app_name format as the runtime
    app_name = f"{config.tenant_id}:{config.app.app_id}:v{config.version}"
    return await runtime._state_backend.session_service.get_session(  # noqa: SLF001
        app_name=app_name,
        user_id=context.sdk_user_id,
        session_id=session_id,
    )


def _history_texts(session) -> list[str]:
    return [
        event.content.parts[0].text for event in session.events
        if event.content and event.content.parts and event.content.parts[0].text
    ]


def test_get_current_time_is_timezone_aware_iso_string() -> None:
    result = get_current_time()
    parsed = _dt.datetime.fromisoformat(result)
    assert parsed.tzinfo is not None


def test_agent_app_run_yields_events() -> None:
    app, provider = _make_app()
    configs = make_default_test_configs()
    ctx = _context("tenant_default")
    cfg = _config(configs, "tenant_default")

    async def consume() -> list:
        events = []
        async for event in app.run(config=cfg, context=ctx, session_id="s1", user_input="hi"):
            events.append(event)
        return events

    events = asyncio.run(consume())
    assert events
    assert provider.call_count == 1


def test_agent_app_reuses_session_for_same_tenant_and_session() -> None:
    model = FakeLLMModel()
    app, _ = _make_app(models={"default": model})
    configs = make_default_test_configs()
    ctx = _context("tenant_default")
    cfg = _config(configs, "tenant_default")
    _consume(app, cfg, ctx, "shared", "turn-one")
    _consume(app, cfg, ctx, "shared", "turn-two")
    session = asyncio.run(_get_session(app, cfg, ctx, "shared"))
    history = _history_texts(session)
    assert "turn-one" in history
    assert "turn-two" in history
    assert model.call_count == 2


def test_agent_app_isolates_different_session_ids() -> None:
    model = FakeLLMModel()
    app, _ = _make_app(models={"default": model})
    configs = make_default_test_configs()
    ctx = _context("tenant_default")
    cfg = _config(configs, "tenant_default")
    _consume(app, cfg, ctx, "alpha", "only-in-alpha")
    _consume(app, cfg, ctx, "beta", "only-in-beta")
    alpha = asyncio.run(_get_session(app, cfg, ctx, "alpha"))
    beta = asyncio.run(_get_session(app, cfg, ctx, "beta"))
    hist_a = _history_texts(alpha)
    hist_b = _history_texts(beta)
    assert "only-in-alpha" in hist_a
    assert "only-in-beta" not in hist_a
    assert "only-in-beta" in hist_b
    assert "only-in-alpha" not in hist_b


def test_agent_app_isolates_different_tenants_same_session() -> None:
    model = FakeLLMModel()
    app, _ = _make_app(models={"default": model})
    configs = make_default_test_configs()
    ctx_a = _context("tenant_a")
    ctx_b = _context("tenant_b")
    cfg_a = _config(configs, "tenant_a")
    cfg_b = _config(configs, "tenant_b")
    _consume(app, cfg_a, ctx_a, "shared-session", "message-for-tenant-a")
    _consume(app, cfg_b, ctx_b, "shared-session", "message-for-tenant-b")

    session_a = asyncio.run(_get_session(app, cfg_a, ctx_a, "shared-session"))
    session_b = asyncio.run(_get_session(app, cfg_b, ctx_b, "shared-session"))
    hist_a = _history_texts(session_a)
    hist_b = _history_texts(session_b)
    assert "message-for-tenant-a" in hist_a
    assert "message-for-tenant-b" not in hist_a
    assert "message-for-tenant-b" in hist_b
    assert "message-for-tenant-a" not in hist_b


def test_agent_app_same_key_reuses_one_runtime() -> None:
    model = FakeLLMModel()
    app, provider = _make_app(models={"default": model})
    configs = make_default_test_configs()
    ctx = _context("tenant_default")
    cfg = _config(configs, "tenant_default")
    _consume(app, cfg, ctx, "s1", "first")
    _consume(app, cfg, ctx, "s2", "second")
    assert provider.call_count == 1
    assert len(app._cache) == 1  # noqa: SLF001


def test_agent_app_different_tenants_create_separate_runtimes() -> None:
    model = FakeLLMModel()
    app, provider = _make_app(models={"default": model})
    configs = make_default_test_configs()
    cfg_a = _config(configs, "tenant_a")
    cfg_b = _config(configs, "tenant_b")
    _consume(app, cfg_a, _context("tenant_a"), "s1", "msg-a")
    _consume(app, cfg_b, _context("tenant_b"), "s1", "msg-b")
    assert provider.call_count == 2
    assert len(app._cache) == 2  # noqa: SLF001


def test_agent_app_different_versions_create_separate_runtimes() -> None:
    model = FakeLLMModel()
    app, provider = _make_app(models={"default": model})
    ctx = _context("tenant_v")
    cfg_v1 = make_tenant_config("tenant_v", version=1)
    cfg_v2 = make_tenant_config("tenant_v", version=2)

    _consume(app, cfg_v1, ctx, "s1", "msg-v1")
    assert provider.call_count == 1
    assert len(app._cache) == 1  # noqa: SLF001

    _consume(app, cfg_v2, ctx, "s1", "msg-v2")
    assert provider.call_count == 2
    # New version retires the old same-tenant runtime: only the new key remains.
    assert len(app._cache) == 1  # noqa: SLF001
    assert ("tenant_v", "app_demo", 2) in app._cache  # noqa: SLF001


def test_agent_app_different_profiles_select_different_models() -> None:
    model_a = FakeLLMModel(model_name="model-a")
    model_b = FakeLLMModel(model_name="model-b")
    configs = {
        "tenant_a": make_tenant_config("tenant_a", app=make_app_config(model_profile="profile_a")),
        "tenant_b": make_tenant_config("tenant_b", app=make_app_config(model_profile="profile_b")),
    }
    provider = FakeModelProvider({"profile_a": model_a, "profile_b": model_b})
    backend = _make_mock_backend()
    app = AgentApp(model_provider=provider, state_backend=backend)

    _consume(app, configs["tenant_a"], _context("tenant_a"), "s1", "msg-a")
    _consume(app, configs["tenant_b"], _context("tenant_b"), "s1", "msg-b")
    assert model_a.call_count == 1
    assert model_b.call_count == 1


def test_agent_app_empty_tools_reaches_runtime() -> None:
    model = FakeLLMModel()
    app, _ = _make_app(models={"default": model})
    configs = make_default_test_configs()
    ctx = _context("tenant_b")
    cfg = _config(configs, "tenant_b")
    _consume(app, cfg, ctx, "s1", "hi")
    runtime = app._get_or_create_runtime(cfg, ctx)  # noqa: SLF001
    # Runtime always includes preload_memory_tool even when allowed_tools is empty
    assert len(runtime._agent.tools) == 1  # noqa: SLF001


def test_agent_app_unknown_profile_creates_no_runtime() -> None:
    cfg = make_tenant_config("tenant_bad", app=make_app_config(model_profile="unknown_profile"))
    provider = FakeModelProvider({})
    backend = _make_mock_backend()
    app = AgentApp(model_provider=provider, state_backend=backend)

    with pytest.raises(TenantAgentConfigurationError):
        _consume(app, cfg, _context("tenant_bad"), "s1", "hi")
    assert provider.call_count == 1
    assert len(app._cache) == 0  # noqa: SLF001


def test_agent_app_unknown_tool_creates_no_runtime() -> None:
    cfg = make_tenant_config("tenant_bad", app=make_app_config(allowed_tools=("nonexistent_tool", )))
    model = FakeLLMModel()
    provider = FakeModelProvider({"default": model})
    backend = _make_mock_backend()
    app = AgentApp(model_provider=provider, state_backend=backend)

    with pytest.raises(TenantAgentConfigurationError):
        _consume(app, cfg, _context("tenant_bad"), "s1", "hi")
    assert model.call_count == 0
    assert len(app._cache) == 0  # noqa: SLF001


def test_agent_app_close_closes_all_runtimes() -> None:
    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    configs = make_default_test_configs()
    _consume(app, _config(configs, "tenant_a"), _context("tenant_a"), "s1", "msg-a")
    _consume(app, _config(configs, "tenant_b"), _context("tenant_b"), "s1", "msg-b")
    assert len(app._cache) == 2  # noqa: SLF001

    close_count = {"count": 0}
    for runtime in app._cache.values():  # noqa: SLF001
        original = runtime.close

        async def tracking_close(_orig=original):
            close_count["count"] += 1
            await _orig()

        runtime.close = tracking_close

    asyncio.run(app.close())
    assert close_count["count"] == 2
    assert len(app._cache) == 0  # noqa: SLF001


def test_agent_app_close_one_failure_does_not_prevent_others() -> None:
    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    configs = make_default_test_configs()
    _consume(app, _config(configs, "tenant_a"), _context("tenant_a"), "s1", "msg-a")
    _consume(app, _config(configs, "tenant_b"), _context("tenant_b"), "s1", "msg-b")

    close_attempts = []

    for i, runtime in enumerate(app._cache.values()):  # noqa: SLF001
        original = runtime.close

        async def tracking_close(_orig=original, idx=i):
            close_attempts.append(idx)
            if idx == 0:
                raise RuntimeError("close failed")
            await _orig()

        runtime.close = tracking_close

    asyncio.run(app.close())
    assert len(close_attempts) == 2
    assert len(app._cache) == 0  # noqa: SLF001


def test_agent_app_close_log_sanitization() -> None:
    """Verify that close() only logs exception type, not full exception details."""

    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    configs = make_default_test_configs()
    _consume(app, _config(configs, "tenant_default"), _context("tenant_default"), "s1", "msg")

    for runtime in app._cache.values():  # noqa: SLF001

        async def raising_close(_orig=runtime.close):
            raise RuntimeError("SECRET: database password is hunter2")

        runtime.close = raising_close

    with unittest.mock.patch("trpc_service.agent.app.logger") as mock_logger:
        asyncio.run(app.close())
        assert mock_logger.error.called
        log_call_args = str(mock_logger.error.call_args)
        assert "SECRET" not in log_call_args
        assert "hunter2" not in log_call_args
        assert "RuntimeError" in log_call_args


def test_agent_app_repeated_close_calls_each_runtime_once() -> None:
    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    configs = make_default_test_configs()
    _consume(app, _config(configs, "tenant_default"), _context("tenant_default"), "s1", "msg")

    close_count = {"count": 0}
    for runtime in app._cache.values():  # noqa: SLF001
        original = runtime.close

        async def tracking_close(_orig=original):
            close_count["count"] += 1
            await _orig()

        runtime.close = tracking_close

    asyncio.run(app.close())
    asyncio.run(app.close())
    asyncio.run(app.close())
    assert close_count["count"] == 1
    assert len(app._cache) == 0  # noqa: SLF001


def test_agent_app_run_after_close_raises() -> None:
    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    configs = make_default_test_configs()
    cfg = _config(configs, "tenant_default")
    ctx = _context("tenant_default")
    _consume(app, cfg, ctx, "s1", "msg")
    asyncio.run(app.close())

    with pytest.raises(TenantAgentConfigurationError):
        _consume(app, cfg, ctx, "s2", "hi")
    assert model.call_count == 1


def test_agent_app_from_env_lazy_model() -> None:
    from trpc_service.agent.model_provider import DefaultModelProvider

    env = {
        "TRPC_MODEL_PROVIDER": "openai-compatible",
        "TRPC_MODEL_NAME": "fake-from-env",
        "TRPC_MODEL_BASE_URL": "https://example.invalid/v1",
        "TRPC_MODEL_API_KEY": "test-key-not-used",
    }
    app = AgentApp.from_env(environ=env)
    assert isinstance(app._model_provider, DefaultModelProvider)  # noqa: SLF001


def test_agent_app_rejects_app_id_mismatch() -> None:
    cfg = make_tenant_config("tenant_a", app=make_app_config(app_id="app_demo"))
    model = FakeLLMModel()
    backend = _make_mock_backend()
    app, _ = _make_app(models={"default": model}, state_backend=backend)
    ctx = _context("tenant_a", app_id="wrong_app")

    with pytest.raises(TenantAgentConfigurationError):
        _consume(app, cfg, ctx, "s1", "hi")
    assert model.call_count == 0


class GatedFakeLLMModel(FakeLLMModel):
    """Fake model that blocks inside the model call until released."""

    def __init__(self, model_name: str = "fake") -> None:
        super().__init__(model_name=model_name)
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.entered.set()
        await self.gate.wait()
        async for response in super()._generate_async_impl(request, stream, ctx):
            yield response


def _track_close(runtime) -> dict:
    """Wrap runtime.close with a counter and return the counter dict."""
    counter = {"count": 0}
    original = runtime.close

    async def tracking_close():
        counter["count"] += 1
        await original()

    runtime.close = tracking_close
    return counter


async def _collect(app: AgentApp, config: TenantConfig, context: TenantContext, session_id: str,
                   user_input: str) -> list:
    return [
        event async for event in app.run(config=config, context=context, session_id=session_id, user_input=user_input)
    ]


def test_agent_app_new_version_retires_idle_old_runtime_exactly_once() -> None:
    models = {"default": FakeLLMModel(), "profile_v2": FakeLLMModel()}
    app, _ = _make_app(models=models)
    cfg_v1 = make_tenant_config("tenant_v", version=1)
    cfg_v2 = make_tenant_config(
        "tenant_v",
        version=2,
        app=make_app_config(app_id="app_demo2", model_profile="profile_v2"),
    )
    _consume(app, cfg_v1, _context("tenant_v", "app_demo"), "s1", "v1")
    assert len(app._cache) == 1  # noqa: SLF001
    v1_key = next(iter(app._cache.keys()))
    v1_counter = _track_close(app._cache[v1_key])  # noqa: SLF001

    _consume(app, cfg_v2, _context("tenant_v", "app_demo2"), "s1", "v2")

    assert v1_counter["count"] == 1, "idle old runtime must be closed exactly once"
    assert v1_key not in app._cache  # noqa: SLF001
    assert len(app._cache) == 1  # noqa: SLF001
    _consume(app, cfg_v2, _context("tenant_v", "app_demo2"), "s2", "v2-again")


def test_agent_app_inflight_old_runtime_not_closed_until_run_finishes() -> None:
    gated = GatedFakeLLMModel()
    plain = FakeLLMModel()
    app, _ = _make_app(models={"profile_v1": gated, "profile_v2": plain})
    cfg_v1 = make_tenant_config("tenant_v", version=1, app=make_app_config(model_profile="profile_v1"))
    cfg_v2 = make_tenant_config(
        "tenant_v",
        version=2,
        app=make_app_config(app_id="app_demo2", model_profile="profile_v2"),
    )
    ctx_v1 = _context("tenant_v", "app_demo")
    ctx_v2 = _context("tenant_v", "app_demo2")

    async def _scenario() -> None:
        inflight = asyncio.ensure_future(_collect(app, cfg_v1, ctx_v1, "s1", "blocked"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)
        v1_key = next(key for key in app._cache  # noqa: SLF001
                      if key[0] == "tenant_v" and key[2] == 1)
        v1_counter = _track_close(app._cache[v1_key])  # noqa: SLF001

        await _collect(app, cfg_v2, ctx_v2, "s1", "publish-v2")

        assert v1_counter["count"] == 0, "in-flight old runtime must not close early"
        assert v1_key in app._cache  # noqa: SLF001

        gated.gate.set()
        await asyncio.wait_for(inflight, timeout=5)

        assert v1_counter["count"] == 1, "old runtime must close exactly once after drain"
        assert v1_key not in app._cache  # noqa: SLF001

        with pytest.raises(TenantAgentConfigurationError):
            await _collect(app, cfg_v1, ctx_v1, "s2", "stale")

        events = await _collect(app, cfg_v2, ctx_v2, "s3", "fresh")
        assert events

    asyncio.run(_scenario())


def test_agent_app_close_during_drain_closes_each_runtime_once() -> None:
    gated = GatedFakeLLMModel()
    plain = FakeLLMModel()
    app, _ = _make_app(models={"profile_v1": gated, "profile_v2": plain})
    cfg_v1 = make_tenant_config("tenant_v", version=1, app=make_app_config(model_profile="profile_v1"))
    cfg_v2 = make_tenant_config(
        "tenant_v",
        version=2,
        app=make_app_config(app_id="app_demo2", model_profile="profile_v2"),
    )

    async def _scenario() -> int:
        inflight = asyncio.ensure_future(_collect(app, cfg_v1, _context("tenant_v", "app_demo"), "s1", "blocked"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)
        v1_key = next(key for key in app._cache  # noqa: SLF001
                      if key[0] == "tenant_v" and key[2] == 1)
        v1_counter = _track_close(app._cache[v1_key])  # noqa: SLF001

        await _collect(app, cfg_v2, _context("tenant_v", "app_demo2"), "s1", "publish-v2")

        # Start close() in background - it should wait for inflight to complete
        close_task = asyncio.ensure_future(app.close())

        # Give close() time to start and enter the wait loop
        # Use 2 seconds to simulate a real model request (typically 10-60s)
        # This proves close() waits indefinitely, not just a short timeout
        await asyncio.sleep(2.0)

        # Critical assertion: close() must NOT have closed the inflight runtime yet
        # This proves close() is waiting indefinitely, not timing out
        assert v1_counter["count"] == 0, "close() must wait for inflight requests before closing runtimes"

        # Now release the gate so the inflight request can complete
        gated.gate.set()

        # Wait for both the inflight request and close() to complete
        await asyncio.wait_for(inflight, timeout=5)
        await asyncio.wait_for(close_task, timeout=5)

        return v1_counter["count"]

    close_count = asyncio.run(_scenario())
    assert close_count == 1, "shutdown close must not double-close a draining runtime"


def test_agent_app_close_no_timeout_on_inflight() -> None:
    """close() must wait indefinitely for in-flight requests, not timeout after 5s."""
    gated = GatedFakeLLMModel()
    app, _ = _make_app(models={"default": gated})
    cfg = make_tenant_config("tenant_t1")

    async def _scenario() -> None:
        # Start an in-flight request
        inflight = asyncio.ensure_future(_collect(app, cfg, _context("tenant_t1"), "s1", "long-running"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)

        # Start close() in background
        close_task = asyncio.ensure_future(app.close())

        # Wait for 6 seconds (longer than the old 5s timeout)
        await asyncio.sleep(6.0)

        # Critical: close() must still be waiting, not have closed the runtime
        assert not close_task.done(), "close() must not timeout on in-flight requests"

        # Release the gate and verify close completes
        gated.gate.set()
        await asyncio.wait_for(inflight, timeout=5)
        await asyncio.wait_for(close_task, timeout=5)

    asyncio.run(_scenario())


def test_agent_app_concurrent_close_both_wait() -> None:
    """Two concurrent close() calls must both wait for drain completion."""
    gated = GatedFakeLLMModel()
    app, _ = _make_app(models={"default": gated})
    cfg = make_tenant_config("tenant_t2")

    async def _scenario() -> None:
        # Start an in-flight request
        inflight = asyncio.ensure_future(_collect(app, cfg, _context("tenant_t2"), "s1", "blocked"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)

        # Start two concurrent close() calls
        close_task1 = asyncio.ensure_future(app.close())
        close_task2 = asyncio.ensure_future(app.close())

        # Give both time to start
        await asyncio.sleep(0.5)

        # Neither should complete while in-flight request is running
        assert not close_task1.done(), "First close() should wait for drain"
        assert not close_task2.done(), "Second close() should wait for drain"

        # Release the gate
        gated.gate.set()
        await asyncio.wait_for(inflight, timeout=5)

        # Both close() calls should complete
        await asyncio.wait_for(close_task1, timeout=5)
        await asyncio.wait_for(close_task2, timeout=5)

        # Both should have completed successfully
        assert close_task1.done() and not close_task1.exception()
        assert close_task2.done() and not close_task2.exception()

    asyncio.run(_scenario())


def test_agent_app_close_after_closing_rejects_new_runs() -> None:
    """After close() starts, new run() calls must be rejected immediately."""
    gated = GatedFakeLLMModel()
    plain = FakeLLMModel()
    app, _ = _make_app(models={"default": gated, "other": plain})
    cfg = make_tenant_config("tenant_t3")

    async def _scenario() -> None:
        # Start an in-flight request
        inflight = asyncio.ensure_future(_collect(app, cfg, _context("tenant_t3"), "s1", "blocked"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)

        # Start close() in background
        close_task = asyncio.ensure_future(app.close())
        await asyncio.sleep(0.2)  # Let close() enter closing state

        # Try to start a new run - should be rejected
        cfg2 = make_tenant_config("tenant_t3", version=2, app=make_app_config(model_profile="other"))
        with pytest.raises(TenantAgentConfigurationError):
            await _collect(app, cfg2, _context("tenant_t3"), "s2", "should-fail")

        # Release the gate and let close complete
        gated.gate.set()
        await asyncio.wait_for(inflight, timeout=5)
        await asyncio.wait_for(close_task, timeout=5)

    asyncio.run(_scenario())


def test_agent_app_cancelled_close_caller_does_not_abandon_cleanup() -> None:
    """Cancelling one close caller must not cancel or falsely complete cleanup."""
    gated = GatedFakeLLMModel()
    app, _ = _make_app(models={"default": gated})
    cfg = make_tenant_config("tenant_close_cancel")
    ctx = _context("tenant_close_cancel")

    async def _scenario() -> None:
        inflight = asyncio.create_task(_collect(app, cfg, ctx, "s1", "blocked"))
        await asyncio.wait_for(gated.entered.wait(), timeout=5)
        key = (ctx.tenant_id, ctx.app_id, cfg.version)
        counter = _track_close(app._cache[key])  # noqa: SLF001

        caller = asyncio.create_task(app.close())
        await asyncio.sleep(0.2)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller

        assert counter["count"] == 0
        assert app._close_task is not None  # noqa: SLF001
        assert not app._close_task.done()  # noqa: SLF001

        gated.gate.set()
        await asyncio.wait_for(inflight, timeout=5)
        await asyncio.wait_for(app.close(), timeout=5)
        assert counter["count"] == 1
        assert not app._cache  # noqa: SLF001

    asyncio.run(_scenario())


def test_agent_app_each_runtime_closed_exactly_once_after_drain() -> None:
    """After drain completes, each runtime must be closed exactly once."""
    # Use separate gated models for each tenant
    gated_models = {f"profile_{i}": GatedFakeLLMModel() for i in range(3)}
    app, _ = _make_app(models=gated_models)

    async def _scenario() -> dict[str, int]:
        # Create multiple runtimes for different tenants (to avoid retirement)
        counters = {}
        tasks = []
        for i in range(3):
            tenant_id = f"tenant_t4_{i}"
            ctx = _context(tenant_id)
            cfg_i = make_tenant_config(tenant_id, app=make_app_config(model_profile=f"profile_{i}"))
            # Start in-flight requests for each
            task = asyncio.ensure_future(_collect(app, cfg_i, ctx, f"s{i}", f"msg{i}"))
            tasks.append(task)
            # Wait for this specific model to be entered
            await asyncio.wait_for(gated_models[f"profile_{i}"].entered.wait(), timeout=5)
            # Track close calls for this runtime
            key = (ctx.tenant_id, ctx.app_id, cfg_i.version)
            counters[tenant_id] = _track_close(app._cache[key])  # noqa: SLF001

        # Start close() in background
        close_task = asyncio.ensure_future(app.close())

        # Release all gates
        for model in gated_models.values():
            model.gate.set()

        # Wait for close to complete
        await asyncio.wait_for(close_task, timeout=10)

        return counters

    counters = asyncio.run(_scenario())
    # Each runtime should be closed exactly once
    for tenant_id, counter in counters.items():
        assert counter["count"] == 1, f"{tenant_id} runtime closed {counter['count']} times, expected 1"


def test_agent_app_metadata_scales_with_active_tenants_not_history() -> None:
    """After many version cycles, metadata should scale with active tenants, not history."""
    app, _ = _make_app()

    async def _scenario() -> dict[str, int]:
        # Simulate 10 tenants, each cycling through 20 versions
        for tenant_idx in range(10):
            tenant_id = f"tenant_meta_{tenant_idx}"
            for version in range(1, 21):
                cfg = make_tenant_config(tenant_id, version=version)
                ctx = _context(tenant_id)
                await _collect(app, cfg, ctx, f"s{version}", f"msg{version}")

        # After all versions, check metadata size
        return {
            "cache_size": len(app._cache),  # noqa: SLF001
            "active_runs_size": len(app._active_runs),  # noqa: SLF001
            "retired_size": len(app._retired),  # noqa: SLF001
            "max_versions_size": len(app._max_versions),  # noqa: SLF001
        }

    metadata = asyncio.run(_scenario())

    # _max_versions should have exactly 10 entries (one per tenant), not 200 (10 tenants * 20 versions)
    assert metadata["max_versions_size"] == 10, f"Expected 10 max_versions entries, got {metadata['max_versions_size']}"

    # _active_runs should be empty (all runs completed)
    assert metadata["active_runs_size"] == 0, f"Expected 0 active_runs, got {metadata['active_runs_size']}"

    # _retired should be empty (all retired runtimes closed and cleaned up)
    assert metadata["retired_size"] == 0, f"Expected 0 retired, got {metadata['retired_size']}"

    # _cache should only have 10 entries (latest version per tenant)
    assert metadata["cache_size"] == 10, f"Expected 10 cache entries, got {metadata['cache_size']}"


def test_agent_app_old_version_rejected_after_newer_seen() -> None:
    """After seeing v5 for a tenant, v3 should be rejected."""
    app, _ = _make_app()

    async def _scenario() -> None:
        tenant_id = "tenant_v_reject"
        ctx = _context(tenant_id)

        # Create v5
        cfg_v5 = make_tenant_config(tenant_id, version=5)
        await _collect(app, cfg_v5, ctx, "s1", "msg_v5")

        # Try to create v3 - should be rejected
        cfg_v3 = make_tenant_config(tenant_id, version=3)
        with pytest.raises(TenantAgentConfigurationError):
            await _collect(app, cfg_v3, ctx, "s2", "msg_v3")

        # v6 should still work
        cfg_v6 = make_tenant_config(tenant_id, version=6)
        await _collect(app, cfg_v6, ctx, "s3", "msg_v6")

    asyncio.run(_scenario())


# ── Stage 6A1: per-config-version governance isolation ──────────────────────


def test_runtime_tools_carry_governance_decisions_per_config_version() -> None:
    from trpc_service.agent.tool_registry import AllowedToolRegistry
    from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

    backend = make_in_memory_state_backend()
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(model_provider=provider, state_backend=backend, tool_registry=AllowedToolRegistry.default())

    cfg_v1 = make_tenant_config("tenant_default", version=1, app=make_app_config())
    cfg_v2 = make_tenant_config(
        "tenant_default",
        version=2,
        app=make_app_config(),
        governance=make_governance(tool_decisions={"get_current_time": "deny"}),
    )

    rt1 = app._get_or_create_runtime(cfg_v1, _context())  # noqa: SLF001
    rt2 = app._get_or_create_runtime(cfg_v2, _context())  # noqa: SLF001

    def _decisions_of(runtime) -> dict:
        for tool in runtime._agent.tools:  # noqa: SLF001
            for f in getattr(tool, "filters", []):
                if isinstance(f, TenantToolGovernanceFilter):
                    return dict(f._decisions)  # noqa: SLF001
        raise AssertionError("governance filter missing from runtime tools")

    assert _decisions_of(rt1) == {}
    assert _decisions_of(rt2) == {"get_current_time": "deny"}


def test_different_tenants_do_not_share_governance_filters() -> None:
    from trpc_service.agent.tool_registry import AllowedToolRegistry
    from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

    backend = make_in_memory_state_backend()
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(model_provider=provider, state_backend=backend, tool_registry=AllowedToolRegistry.default())

    cfg_a = make_tenant_config(
        "tenant_a",
        app=make_app_config(),
        governance=make_governance(tool_decisions={"get_current_time": "review"}),
    )
    cfg_b = make_tenant_config("tenant_b", app=make_app_config(allowed_tools=()))
    rt_a = app._get_or_create_runtime(cfg_a, _context("tenant_a"))  # noqa: SLF001
    rt_b = app._get_or_create_runtime(cfg_b, _context("tenant_b"))  # noqa: SLF001

    filters_a = [
        f for t in rt_a._agent.tools for f in getattr(t, "filters", [])  # noqa: SLF001
        if isinstance(f, TenantToolGovernanceFilter)
    ]
    filters_b = [
        f for t in rt_b._agent.tools for f in getattr(t, "filters", [])  # noqa: SLF001
        if isinstance(f, TenantToolGovernanceFilter)
    ]
    assert filters_a and all(f is not g for f in filters_a for g in filters_b)
    assert dict(filters_a[0]._decisions) == {"get_current_time": "review"}  # noqa: SLF001


# --- R1A: per-tenant backend selection via TenantStateBackendResolver ------


class _RecordingBackend:
    """AgentStateBackend double recording closes."""

    def __init__(self) -> None:
        self.session_service = object()
        self.memory_service = object()
        self.close_calls = 0

    def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        self.close_calls += 1


def _make_fake_resolver() -> tuple[unittest.mock.Mock, _RecordingBackend, _RecordingBackend]:
    redis_backend = _RecordingBackend()
    sql_backend = _RecordingBackend()
    resolver = unittest.mock.Mock()
    resolver.resolve.side_effect = lambda profile: redis_backend if profile.state_backend == "redis" \
        else sql_backend
    return resolver, redis_backend, sql_backend


def test_resolver_selection_is_per_tenant_profile() -> None:
    resolver, redis_backend, sql_backend = _make_fake_resolver()
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(model_provider=provider, backend_resolver=resolver)

    cfg_redis = make_tenant_config("tenant_a", backend_profile=make_backend_profile("redis"))
    cfg_sql = make_tenant_config("tenant_b", backend_profile=make_backend_profile("sql"))
    rt_a = app._get_or_create_runtime(cfg_redis, _context("tenant_a"))  # noqa: SLF001
    rt_b = app._get_or_create_runtime(cfg_sql, _context("tenant_b"))  # noqa: SLF001

    assert rt_a._state_backend is redis_backend  # noqa: SLF001
    assert rt_b._state_backend is sql_backend  # noqa: SLF001
    assert resolver.resolve.call_count == 2


def test_injected_state_backend_wins_over_resolver() -> None:
    resolver, redis_backend, _ = _make_fake_resolver()
    injected = make_in_memory_state_backend()
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(model_provider=provider, state_backend=injected, backend_resolver=resolver)

    cfg_sql = make_tenant_config("tenant_b", backend_profile=make_backend_profile("sql"))
    rt = app._get_or_create_runtime(cfg_sql, _context("tenant_b"))  # noqa: SLF001
    assert rt._state_backend is injected  # noqa: SLF001
    resolver.resolve.assert_not_called()


def test_close_closes_resolver_not_runtime_services_once() -> None:
    resolver, redis_backend, sql_backend = _make_fake_resolver()
    closed = {"count": 0}

    async def _resolver_close() -> None:
        closed["count"] += 1
        await redis_backend.close()
        await sql_backend.close()

    resolver.close = _resolver_close
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(model_provider=provider, backend_resolver=resolver)
    cfg = make_tenant_config("tenant_a")
    app._get_or_create_runtime(cfg, _context("tenant_a"))  # noqa: SLF001

    asyncio.run(app.close())
    asyncio.run(app.close())
    assert closed["count"] == 1
    assert redis_backend.close_calls == 1
    assert sql_backend.close_calls == 1


def test_capabilities_resolver_passes_tenant_knowledge_only_to_allowlisted_tool_build():
    """Knowledge must enter the Agent only through its tenant capability bundle."""
    backend = make_in_memory_state_backend()
    tenant_knowledge = object()
    resolver = unittest.mock.Mock()
    resolver.resolve.return_value = unittest.mock.Mock(
        artifact=object(),
        knowledge=tenant_knowledge,
    )
    resolver.resolve_state_backend.return_value = backend
    registry = unittest.mock.Mock()
    registry.build_tools.return_value = []
    provider = FakeModelProvider({"default": FakeLLMModel()})
    app = AgentApp(
        model_provider=provider,
        tool_registry=registry,
        backend_capabilities_resolver=resolver,
    )
    cfg = make_tenant_config(
        "tenant_a",
        app=make_app_config(allowed_tools=("knowledge_search", )),
    )

    app._get_or_create_runtime(cfg, _context("tenant_a"))  # noqa: SLF001

    registry.build_tools.assert_called_once_with(
        ("knowledge_search", ),
        cfg.governance.tool_decisions,
        knowledge_base=tenant_knowledge,
    )
