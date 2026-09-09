"""Regression evidence for the interrupted V3 implementation.

Purpose: execute real SDK Runner with OfflineModel, plus HTTP/queue/Outbox and
admission faults. No credentials, model fees or real IM are needed. Assertions
cover visible result/state, actual invocation counts and denied mutations.
Failures point first to web/app, gateway/service/dispatcher or SDK wrappers.
"""

import asyncio
import contextvars
import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel, LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent import AgentWorker, TenantRuntime, TenantRuntimeManager
from trpc_service.agent.model_observer import instrument_model_call_accounting
from trpc_service.agent.worker import classify_sdk_error
from trpc_service.config import ChannelBindingConfig, ServiceSettings
from trpc_service.demos import OFFLINE_SCENARIOS, run_demo
from trpc_service.gateway import AgentTaskEnvelope, InMemoryIdempotencyStore
from trpc_service.gateway import InMemoryOutboxStore, RequestState
from trpc_service.gateway import InMemoryRequestStore, RequestRecord
from trpc_service.gateway.dispatcher import AgentTaskProcessor
from trpc_service.gateway.models import AgentRequest, AgentStreamEvent, NormalizedInboundMessage, StreamEventType
from trpc_service.gateway.service import AgentExecutionError, DuplicateRequestError, GatewayService
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.storage import InMemorySessionExecutionGuard, SessionLockLostError
from trpc_service.storage.guard import SessionLease, lease_scope
from trpc_service.storage.session_wrapper import RequestTaggingSessionService
from trpc_service.tenant.approval import InMemoryApprovalStore
from trpc_service.tenant.filters import ToolConfirmationFilter
from trpc_service.web import build_container, create_app

pytestmark = pytest.mark.component


@pytest.mark.parametrize(("code", "message", "expected"), [
    ("STREAMING_ERROR", "Request timed out.", "APITimeoutError"),
    ("STREAMING_ERROR", "Error code: 429 - rate limited", "RateLimitError"),
    ("API_ERROR", "Connection lost", "APIConnectionError"),
    ("LLM_CALL_ERROR", "Error code: 502", "UpstreamServerError"),
    ("run_cancelled", "Request timed out.", "run_cancelled"),
])
def test_generic_sdk_model_errors_are_classified(code, message, expected):
    assert classify_sdk_error(code, message) == expected


def offline_container(tenant):
    container = build_container(ServiceSettings(), [tenant])
    factory = OfflineRuntimeFactory()
    container.runtimes = TenantRuntimeManager(container.registry, factory)
    container.gateway = GatewayService(container.registry, AgentWorker(container.runtimes, container.guard),
                                       InMemoryIdempotencyStore())
    container.task_processor = AgentTaskProcessor(container.queue, container.gateway, container.outbox, consumer="test")
    return container, factory


def body(key="request"):
    return {
        "tenant_id": "tenant-a",
        "app_id": "assistant",
        "user_id": "student",
        "session_id": "lesson",
        "message": "hello",
        "idempotency_key": key
    }


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_async_chat_reaches_real_runner_and_status_result(tenant_config):
    container, factory = offline_container(tenant_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                 base_url="http://test") as client:
        accepted = await client.post("/api/v1/chat/async", json=body())
        assert accepted.status_code == 202
        assert await container.task_processor.process_one(0.1)
        status = await client.get(accepted.json()["status_url"])
        assert status.json()["state"] == "succeeded"
        assert "echo:hello" in status.json()["result"]["text"]
        assert status.json()["attempts"] == 1
        assert status.json()["model_attempts"] == 1
        assert status.json()["successful_model_calls"] == 1
        assert status.json()["recovery_count"] == 0
        repeated = await client.post("/api/v1/chat/async", json=body())
        assert repeated.json()["request_id"] == accepted.json()["request_id"]
        assert sum(model.calls for model in factory.models) == 1
        rendered = container.metrics.render()
        assert 'trpc_service_requests_total{channel="web",tenant="tenant-a"} 2' in rendered
        assert ('trpc_service_gateway_admissions_total'
                '{channel="web",result="queued",tenant="tenant-a"} 1') in rendered
        assert ('trpc_service_gateway_admissions_total'
                '{channel="web",result="duplicate",tenant="tenant-a"} 1') in rendered
        assert ('trpc_service_gateway_admission_duration_seconds_count'
                '{channel="web",result="queued",tenant="tenant-a"} 1') in rendered
    await container.close()


@pytest.mark.asyncio
async def test_async_gateway_metrics_record_enqueue_error(tenant_config):
    container, _ = offline_container(tenant_config)
    container.queue.enqueue = AsyncMock(side_effect=ConnectionError("queue unavailable"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                 base_url="http://test") as client:
        with pytest.raises(ConnectionError, match="queue unavailable"):
            await client.post("/api/v1/chat/async", json=body("queue-error"))
        rendered = container.metrics.render()
        assert 'trpc_service_requests_total{channel="web",tenant="tenant-a"} 1' in rendered
        assert ('trpc_service_gateway_admissions_total'
                '{channel="web",result="error",tenant="tenant-a"} 1') in rendered
        assert ('trpc_service_gateway_admission_duration_seconds_count'
                '{channel="web",result="error",tenant="tenant-a"} 1') in rendered
    await container.close()


@pytest.mark.asyncio
async def test_worker_loop_survives_one_cancelled_execution_and_reclaims_first(tenant_config):
    """A lease-loss cancellation stops one Run, not the long-lived Worker."""
    container, _ = offline_container(tenant_config)
    calls = []

    class Processor:
        attempts = 0

        async def reclaim_stale(self, **_kwargs):
            calls.append("reclaim")
            return 0

        async def process_one(self, **_kwargs):
            calls.append("process")
            self.attempts += 1
            if self.attempts == 1:
                raise asyncio.CancelledError
            container._stopping.set()
            return False

    class Repair:

        async def repair_stale(self, **_kwargs):
            calls.append("repair")
            return 0

    processor = Processor()
    container.task_processor = processor
    container.request_repair = Repair()

    await asyncio.wait_for(container._worker_loop(), timeout=3)

    assert processor.attempts == 2
    assert calls == ["reclaim", "repair", "process", "reclaim", "repair", "process"]
    assert container.metrics.render().count("execution_cancelled") == 1
    await container.close()


@pytest.mark.asyncio
async def test_sse_persists_result_and_completed_replay_does_not_call_model(tenant_config):
    container, factory = offline_container(tenant_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                 base_url="http://test") as client:
        stream = await client.post("/api/v1/chat/stream", json=body())
        assert "event: completed" in stream.text
        replay = await client.post("/api/v1/chat", json=body())
        assert replay.status_code == 200
        assert "echo:hello" in replay.json()["text"]
        assert sum(model.calls for model in factory.models) == 1
    await container.close()


@pytest.mark.asyncio
async def test_im_redelivery_ignores_receive_timestamp_and_trace(tenant_config):
    tenant_config.channels = [ChannelBindingConfig(binding_id="bot", app_id="assistant", channel="telegram")]
    container, _ = offline_container(tenant_config)
    payload = dict(message_id="10",
                   binding_id="bot",
                   channel="telegram",
                   external_user_id="u",
                   external_conversation_id="c",
                   text="hi")
    first, _ = await container.gateway.inbound_request(NormalizedInboundMessage(**payload))
    with pytest.raises(DuplicateRequestError) as duplicate:
        await container.gateway.inbound_request(NormalizedInboundMessage(**payload))
    assert duplicate.value.request_id == first.request_id
    await container.close()


@pytest.mark.asyncio
async def test_two_workers_read_shared_sdk_history_and_request_tags(tenant_config):
    container, factory = offline_container(tenant_config)
    other_manager = TenantRuntimeManager(container.registry, factory)
    other_gateway = GatewayService(container.registry, AgentWorker(other_manager, container.guard),
                                   InMemoryIdempotencyStore())
    for index, gateway in enumerate((container.gateway, other_gateway)):
        request, key = await gateway.web_request(tenant_id="tenant-a",
                                                 app_id="assistant",
                                                 external_user_id="u",
                                                 session_id="s",
                                                 text=f"turn-{index}")
        result = await gateway.chat(request, key)
        assert f"user_turns={index + 1}" in result.text
    runtime = await other_manager.get("tenant-a", "assistant", 1)
    session = await runtime.runner.session_service.get_session(app_name=runtime.runner.app_name,
                                                               user_id=request.user_id,
                                                               session_id=request.session_id)
    assert len(session.events) == 4
    assert session.events[-1].request_id == request.request_id
    await other_manager.close()
    await container.close()


@pytest.mark.asyncio
async def test_twenty_real_sdk_turns_do_not_lose_history(tenant_config):
    # This test counts raw history. Compression is tested separately.
    tenant_config.apps["assistant"].runtime.summary_enabled = False
    container, _ = offline_container(tenant_config)

    async def turn(index):
        request, key = await container.gateway.web_request(tenant_id="tenant-a",
                                                           app_id="assistant",
                                                           external_user_id="u",
                                                           session_id="s",
                                                           text=str(index))
        return await container.gateway.chat(request, key)

    results = await asyncio.gather(*(turn(index) for index in range(20)))
    counts = sorted(int(item.text.split("user_turns=")[-1]) for item in results)
    assert counts == list(range(1, 21))
    await container.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_outbox_failure_reuses_saved_result_instead_of_model(tenant_config):

    class FailOnceOutbox(InMemoryOutboxStore):
        calls = 0

        async def add(self, message):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("injected database outage")
            return await super().add(message)

    container, factory = offline_container(tenant_config)
    request, key = await container.gateway.web_request(tenant_id="tenant-a",
                                                       app_id="assistant",
                                                       external_user_id="u",
                                                       session_id="s",
                                                       text="hi")
    request.binding_id = "bot"
    request.metadata["external_conversation_id"] = "chat"
    await container.gateway.prepare(request)
    await container.queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key=key))
    processor = AgentTaskProcessor(container.queue, container.gateway, FailOnceOutbox(), consumer="fault")
    assert not await processor.process_one(0.1)
    assert (await container.gateway.request_status("tenant-a", request.request_id)).state != RequestState.SUCCEEDED
    assert await processor.process_one(0.1)
    assert sum(model.calls for model in factory.models) == 1
    record = await container.gateway.request_status("tenant-a", request.request_id)
    assert record.attempts == 2
    assert record.model_attempts == 1
    assert record.successful_model_calls == 1
    assert record.recovery_count == 1
    await container.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_error_event_is_not_followed_by_successful_request(tenant_config):

    class ErrorWorker:

        async def stream(self, request):
            yield AgentStreamEvent(request_id=request.request_id,
                                   sequence=0,
                                   type=StreamEventType.ERROR,
                                   data={"error_code": "model_timeout"})
            yield AgentStreamEvent(request_id=request.request_id, sequence=1, type=StreamEventType.COMPLETED)

    container, _ = offline_container(tenant_config)
    gateway = GatewayService(container.registry, ErrorWorker(), InMemoryIdempotencyStore())
    request, key = await gateway.web_request(tenant_id="tenant-a",
                                             app_id="assistant",
                                             external_user_id="u",
                                             session_id="s",
                                             text="hi")
    with pytest.raises(RuntimeError, match="model_timeout"):
        await gateway.chat(request, key)
    record = await gateway.request_status("tenant-a", request.request_id)
    assert record.state == RequestState.RETRYABLE_FAILED
    assert record.error_code == "model_timeout"
    await container.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_incomplete_recovery_preserves_original_model_error(tenant_config):

    class RootFailureThenIncompleteWorker:

        def __init__(self):
            self.calls = 0

        async def stream(self, request):
            self.calls += 1
            if False:  # pragma: no cover - make this an async generator
                yield None
            if self.calls == 1:
                raise AgentExecutionError("model_rate_limited")
            from trpc_service.agent.runtime import IncompleteRunError
            raise IncompleteRunError("partial_session_turn_requires_review")

    container, _ = offline_container(tenant_config)
    worker = RootFailureThenIncompleteWorker()
    gateway = GatewayService(container.registry, worker, InMemoryIdempotencyStore())
    processor = AgentTaskProcessor(container.queue, gateway, container.outbox, consumer="error-preservation")
    request, key = await gateway.web_request(tenant_id="tenant-a",
                                             app_id="assistant",
                                             external_user_id="u",
                                             session_id="s",
                                             text="hi")
    await gateway.prepare(request)
    await container.queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key=key))
    await gateway.mark_queued(request)

    assert not await processor.process_one(0.1)
    assert not await processor.process_one(0.1)
    record = await gateway.request_status("tenant-a", request.request_id)
    assert record.state == RequestState.FAILED
    assert record.error_code == "model_rate_limited"
    assert record.attempts == 2
    assert record.recovery_count == 1
    await container.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_lost_lease_prevents_session_append():
    delegate = AsyncMock()
    service = RequestTaggingSessionService(delegate)
    lost = asyncio.Event()
    lost.set()
    with lease_scope(SessionLease(token="old-owner", lost=lost)):
        with pytest.raises(SessionLockLostError):
            await service.append_event(object(), Event(author="assistant"))
    delegate.append_event.assert_not_called()


@pytest.mark.asyncio
async def test_worker_closes_streaming_sdk_generator_in_its_owner_task():
    """Streaming Trace ContextVars must be created and closed by one task."""
    trace_context = contextvars.ContextVar("test_sdk_trace_context", default="outside")

    class StreamingRuntime:
        app = SimpleNamespace(runtime=SimpleNamespace(max_run_seconds=30))
        opened_by = None
        closed_by = None
        closed_context = ""

        async def run(self, **_kwargs):
            self.opened_by = asyncio.current_task()
            token = trace_context.set("inside-sdk-stream")
            try:
                yield Event(author="assistant", partial=True, content=Content(parts=[Part.from_text(text="chunk")]))
                await asyncio.Future()
            finally:
                self.closed_by = asyncio.current_task()
                self.closed_context = trace_context.get()
                trace_context.reset(token)

        async def cancel(self, **_kwargs):
            return True

    class RuntimeProvider:

        def __init__(self, runtime):
            self.runtime = runtime

        async def get(self, *_args):
            return self.runtime

    runtime = StreamingRuntime()
    worker = AgentWorker(RuntimeProvider(runtime), InMemorySessionExecutionGuard())
    request = AgentRequest(request_id="trace-owner",
                           tenant_id="tenant-a",
                           config_version=1,
                           app_id="assistant",
                           user_id="user",
                           session_id="session",
                           text="hello",
                           channel="web")
    stream = worker.stream(request)
    assert (await anext(stream)).type == StreamEventType.STARTED
    assert (await anext(stream)).text == "chunk"
    await stream.aclose()

    assert runtime.opened_by is runtime.closed_by
    assert runtime.opened_by is not asyncio.current_task()
    assert runtime.closed_context == "inside-sdk-stream"


@pytest.mark.asyncio
async def test_session_lock_wait_covers_one_bounded_agent_run(tenant_config):
    """A queued same-session turn must not time out after the old fixed 10s."""
    observed = {}

    class CapturingGuard:

        @asynccontextmanager
        async def hold(self, key, *, wait_timeout, lease_seconds):
            observed.update(key=key, wait_timeout=wait_timeout, lease_seconds=lease_seconds)
            yield SessionLease("owner", asyncio.Event(), epoch=1, key=key)

    class EmptyRuntime:

        def __init__(self):
            app = tenant_config.apps["assistant"].model_copy(deep=True)
            app.runtime.max_run_seconds = 45
            self.app = app

        async def run(self, **_kwargs):
            if False:
                yield None

    class RuntimeProvider:

        async def get(self, *_args):
            return EmptyRuntime()

    request = AgentRequest(request_id="lock-wait-policy",
                           tenant_id="tenant-a",
                           config_version=1,
                           app_id="assistant",
                           user_id="user",
                           session_id="session",
                           text="hello",
                           channel="web")
    events = [event async for event in AgentWorker(RuntimeProvider(), CapturingGuard()).stream(request)]
    assert [event.type for event in events] == [StreamEventType.STARTED, StreamEventType.COMPLETED]
    assert observed["wait_timeout"] == 55
    assert observed["lease_seconds"] == 30


@pytest.mark.fault
@pytest.mark.asyncio
async def test_sdk_trace_context_is_clean_when_session_lease_is_lost(tenant_config, caplog):
    """A real streaming SDK Runner must close all spans in its owner task."""

    class SlowStreamingModel(LLMModel):

        @classmethod
        def supported_models(cls):
            return [r"slow-test"]

        def validate_request(self, request):
            return None

        async def _generate_async_impl(self, request, stream=False, ctx=None):
            del request, stream, ctx
            for _ in range(1000):
                await asyncio.sleep(.01)
                yield LlmResponse(partial=True, content=Content(role="model", parts=[Part.from_text(text="chunk")]))
            yield LlmResponse(content=Content(role="model", parts=[Part.from_text(text="done")]))

    class LosingGuard:

        @asynccontextmanager
        async def hold(self, key, **_kwargs):
            lease = SessionLease("owner", asyncio.Event(), epoch=1, key=key)

            async def lose_soon():
                await asyncio.sleep(.05)
                lease.lost.set()

            loss = asyncio.create_task(lose_soon())
            try:
                yield lease
                lease.assert_owned()
            finally:
                loss.cancel()
                await asyncio.gather(loss, return_exceptions=True)

    storage = OfflineRuntimeFactory().storage
    model = SlowStreamingModel(model_name="slow-test")
    instrument_model_call_accounting(model)
    agent = LlmAgent(name="slow_agent", model=model)
    runner = Runner(app_name="trace-context-test",
                    agent=agent,
                    session_service=storage.session_service,
                    memory_service=storage.memory_service,
                    enable_post_turn_processing=False)
    app_config = tenant_config.apps["assistant"].model_copy(deep=True)
    app_config.runtime.summary_enabled = False
    runtime = TenantRuntime(tenant_config, app_config, runner, asyncio.Semaphore(1))

    class RuntimeProvider:

        async def get(self, *_args):
            return runtime

    request = AgentRequest(request_id="trace-lease-loss",
                           tenant_id="tenant-a",
                           config_version=1,
                           app_id="assistant",
                           user_id="user",
                           session_id="session",
                           text="hello",
                           channel="web")
    caplog.set_level(logging.ERROR)
    caplog.set_level(logging.ERROR, logger="opentelemetry.context")
    request_store = InMemoryRequestStore()
    await request_store.create(
        RequestRecord(request_id=request.request_id,
                      tenant_id=request.tenant_id,
                      state=RequestState.RUNNING,
                      request=request))
    worker = AgentWorker(RuntimeProvider(), LosingGuard())
    worker.set_request_store(request_store)
    with pytest.raises(SessionLockLostError):
        async for _event in worker.stream(request):
            pass
    await runner.close()

    counters = await request_store.get(request.tenant_id, request.request_id)
    assert counters.model_attempts == 1
    assert counters.successful_model_calls == 0

    assert "Failed to detach context" not in caplog.text
    assert "created in a different Context" not in caplog.text
    assert "asynchronous generator is already running" not in caplog.text


@pytest.mark.asyncio
async def test_approval_matches_actual_tool_arguments_and_is_consumed_once():
    arguments = {"path": "allowed.txt"}
    digest = InMemoryApprovalStore.arguments_hash(json.dumps(arguments))
    context = new_agent_context(metadata={"approved_arguments": {"write": digest}})
    filter_ = ToolConfirmationFilter("write")
    wrong = FilterResult()
    await filter_._before(context, {"path": "other.txt"}, wrong)
    assert isinstance(wrong.error, PermissionError)
    first, repeated = FilterResult(), FilterResult()
    await filter_._before(context, arguments, first)
    await filter_._before(context, arguments, repeated)
    assert first.is_continue and first.error is None
    assert isinstance(repeated.error, PermissionError)


def test_production_cannot_use_local_composition(tenant_config):
    with pytest.raises(ValueError, match="in-memory fallback is forbidden"):
        build_container(ServiceSettings(environment="production"), [tenant_config])


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", OFFLINE_SCENARIOS)
async def test_each_offline_demo_is_executable(scenario):
    result = await run_demo(scenario)
    assert result


@pytest.mark.asyncio
async def test_terminal_state_cannot_be_overwritten_by_late_gateway_update(tenant_config):
    container, _ = offline_container(tenant_config)
    request, key = await container.gateway.web_request(tenant_id="tenant-a",
                                                       app_id="assistant",
                                                       external_user_id="u",
                                                       session_id="s",
                                                       text="hi")
    await container.gateway.chat(request, key)
    await container.gateway.mark_queued(request)
    assert (await container.gateway.request_status("tenant-a", request.request_id)).state == RequestState.SUCCEEDED
    await container.close()


@pytest.mark.asyncio
async def test_crash_after_only_user_event_is_discarded_before_safe_retry(tenant_config):
    from trpc_agent_sdk.types import Content, Part
    container, factory = offline_container(tenant_config)
    runtime = await container.runtimes.get("tenant-a", "assistant", 1)
    session = await runtime.runner.session_service.create_session(app_name=runtime.runner.app_name,
                                                                  user_id="u",
                                                                  session_id="s")
    session.conversation_count = 1
    await runtime.runner.session_service.append_event(
        session, Event(author="user", request_id="crashed", content=Content(parts=[Part.from_text(text="hi")])))
    request = AgentRequest(request_id="crashed",
                           tenant_id="tenant-a",
                           config_version=1,
                           app_id="assistant",
                           user_id="u",
                           session_id="s",
                           text="hi",
                           channel="web")
    events = [event async for event in AgentWorker(container.runtimes, container.guard).stream(request)]
    assert events[-1].type == StreamEventType.COMPLETED
    assert any("user_turns=1" in event.text for event in events)
    assert sum(model.calls for model in factory.models) == 1
    restored = await runtime.runner.session_service.get_session(app_name=runtime.runner.app_name,
                                                                user_id="u",
                                                                session_id="s")
    matching = [event for event in restored.events if event.request_id == "crashed"]
    assert [event.author for event in matching] == ["user", "offline_assistant"]
    assert restored.conversation_count == 1
    await container.close()


@pytest.mark.asyncio
async def test_crash_after_agent_or_tool_evidence_still_requires_review(tenant_config):
    from trpc_agent_sdk.types import Content, FunctionCall, Part
    from trpc_service.agent.runtime import IncompleteRunError
    container, _ = offline_container(tenant_config)
    runtime = await container.runtimes.get("tenant-a", "assistant", 1)
    session = await runtime.runner.session_service.create_session(app_name=runtime.runner.app_name,
                                                                  user_id="u",
                                                                  session_id="unsafe")
    await runtime.runner.session_service.append_event(
        session, Event(author="user", request_id="crashed", content=Content(parts=[Part.from_text(text="write")])))
    await runtime.runner.session_service.append_event(
        session,
        Event(author="assistant",
              request_id="crashed",
              content=Content(parts=[Part(function_call=FunctionCall(name="write", args={"value": "x"}))])))
    with pytest.raises(IncompleteRunError, match="partial_session_turn_requires_review"):
        await runtime.replay_result(request_id="crashed", user_id="u", session_id="unsafe")
    await container.close()


@pytest.mark.asyncio
async def test_side_effect_filter_reuses_result_and_quarantines_unknown():
    from trpc_service.tool.execution import InMemoryToolExecutionStore
    from trpc_service.tool.execution_filter import ToolExecutionFilter
    from trpc_service.tenant import TenantContext, tenant_scope
    store = InMemoryToolExecutionStore()
    filter_ = ToolExecutionFilter("write", store)
    handler = AsyncMock(return_value="saved")
    with tenant_scope(TenantContext("tenant", "app", 1, "request")):
        first = await filter_.run(None, {"file": "a"}, handler)
        replay = await filter_.run(None, {"file": "a"}, handler)
    assert first.rsp == replay.rsp == "saved"
    assert handler.await_count == 1
    broken = AsyncMock(side_effect=ConnectionError("outcome uncertain"))
    with tenant_scope(TenantContext("tenant", "app", 1, "unknown")):
        with pytest.raises(ConnectionError):
            await filter_.run(None, {"file": "a"}, broken)
        retry = await filter_.run(None, {"file": "a"}, broken)
    assert isinstance(retry.error, PermissionError)
    assert broken.await_count == 1


@pytest.mark.asyncio
async def test_knowledge_tool_uses_trusted_context_not_model_tenant_argument():
    from trpc_service.resources import InMemoryKnowledgeProvider, KnowledgeDocument
    from trpc_service.tool import ToolRegistry
    from trpc_service.config import ToolPolicy
    from trpc_service.tenant import TenantContext, tenant_scope
    provider = InMemoryKnowledgeProvider()
    await provider.add(KnowledgeDocument(tenant_id="a", app_id="app", title="secret", text="apple"))
    tool = ToolRegistry(knowledge_provider=provider).resolve(ToolPolicy(allowed=["knowledge_search"]))[0]
    with tenant_scope(TenantContext("b", "app", 1, "r")):
        assert await tool.func("apple") == []
    with tenant_scope(TenantContext("a", "app", 1, "r")):
        assert len(await tool.func("apple")) == 1


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_actual_offline_queue_chain_propagates_one_trace(monkeypatch):
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from trpc_service.metrics import telemetry
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry, "_TRACER", provider.get_tracer("test-platform"))
    await run_demo("e2e")
    spans = exporter.get_finished_spans()
    assert {span.name
            for span in spans} >= {
                "gateway.channel.normalize", "queue.agent_task.process", "agent.worker.execute",
                "channel.outbound.deliver", "storage.session.get", "storage.session.append_event",
                "storage.memory.store_session"
            }
    assert len({span.context.trace_id for span in spans}) == 1
    provider.shutdown()


@pytest.mark.asyncio
async def test_telegram_attachment_materializes_as_tenant_object():
    from trpc_service.channels import TelegramChannelAdapter
    from trpc_service.resources import InMemoryArtifactStore, ArtifactNotFoundError
    from trpc_service.resources.attachments import AttachmentIngestor
    from trpc_service.gateway.models import Attachment, AgentRequest

    async def response(request):
        if request.url.path.endswith("getFile"):
            return httpx.Response(200, json={"ok": True, "result": {"file_path": "documents/test.txt"}})
        return httpx.Response(200, content=b"attachment-data")

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        adapter = TelegramChannelAdapter("fake", "secret", client)
        store = InMemoryArtifactStore()
        request = AgentRequest(request_id="r",
                               tenant_id="tenant",
                               app_id="app",
                               config_version=1,
                               user_id="u",
                               session_id="s",
                               text="hi",
                               channel="telegram",
                               binding_id="bot",
                               attachments=[
                                   Attachment(attachment_id="file",
                                              kind="file",
                                              name="a.txt",
                                              mime_type="text/plain",
                                              source_url="telegram://file/123")
                               ])
        await AttachmentIngestor(store, {"bot": adapter}).materialize(request)
        attachment = request.attachments[0]
        assert (await store.get("tenant", attachment.attachment_id))[1] == b"attachment-data"
        with pytest.raises(ArtifactNotFoundError):
            await store.get("another-tenant", attachment.attachment_id)


@pytest.mark.asyncio
async def test_unsigned_wecom_http_frame_is_not_accepted(tenant_config):
    from trpc_service.channels import WeComChannelAdapter
    tenant_config.channels = [ChannelBindingConfig(binding_id="wecom", app_id="assistant", channel="wecom")]
    container, _ = offline_container(tenant_config)
    container.channel_adapters["wecom"] = WeComChannelAdapter()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                 base_url="http://test") as client:
        response = await client.post("/api/v1/channels/wecom/webhook", json={})
    assert response.status_code == 405
    await container.close()


@pytest.mark.asyncio
async def test_local_artifact_roundtrip_with_spaces_and_namespace_validation(tmp_path):
    from trpc_service.resources import LocalArtifactStore
    store = LocalArtifactStore(tmp_path / "object storage")
    metadata = await store.put("tenant", "app", "../note.txt", "text/plain", b"data")
    assert (await store.get("tenant", metadata.artifact_id))[1] == b"data"
    with pytest.raises(ValueError, match="namespace"):
        await store.put("../other", "app", "note.txt", "text/plain", b"data")


def test_trace_drops_untrusted_attributes_baggage_and_exception_payload(monkeypatch):
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from trpc_service.metrics import telemetry
    from trpc_service.gateway.models import TraceContext
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry, "_TRACER", provider.get_tracer("redaction-test"))
    with pytest.raises(RuntimeError):
        with telemetry.platform_span("safe", TraceContext(baggage="token=secret-value"), {"prompt": "secret-value"}):
            raise RuntimeError("secret-value")
    span = exporter.get_finished_spans()[0]
    assert "secret-value" not in str(span.attributes)
    assert not span.events
    provider.shutdown()
