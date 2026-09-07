from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from opentelemetry.sdk.trace import Event as SpanEvent
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import Status, StatusCode
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import (
    Content,
    FunctionCall,
    FunctionResponse,
    GenerateContentResponseUsageMetadata,
    Part,
)

from tenant_agent.agent.deterministic import DeterministicEngine
from tenant_agent.agent.trpc import TrpcAgentEngine, _RunnerBundle
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.ids import IdentityDeriver
from tenant_agent.observability import RedactingSpanExporter
from tenant_agent.security import CompositeSecretResolver, Redactor, SecretRegistry
from tenant_agent.services.dispatcher import GatewayRouter
from tenant_agent.settings import Settings
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.memory import InMemoryPlane
from tests.helpers import make_envelope, make_tenant


class CapturingExporter(SpanExporter):
    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []
        self.closed = False

    def export(self, spans: object) -> SpanExportResult:
        self.spans.extend(spans)  # type: ignore[arg-type]
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        self.closed = True


def plane(memory: InMemoryPlane) -> TenantDataPlane:
    return TenantDataPlane(
        sessions=memory,
        memories=memory,
        summaries=memory,
        artifacts=memory,
        knowledge=memory,
        audit=memory,
        receipts=memory,
        usage=memory,
        concurrency=memory,
        outbox=memory,
        leases=memory,
    )


def test_span_exporter_drops_framework_content_and_exact_secrets() -> None:
    registry = SecretRegistry()
    registry.register("model-secret-value")
    delegate = CapturingExporter()
    exporter = RedactingSpanExporter(delegate, Redactor(registry=registry))
    span = ReadableSpan(
        name="call_llm model-secret-value",
        attributes={
            "trpc.python.agent.runner.input": "private prompt",
            "trpc.python.agent.llm_response": "confidential business answer",
            "trpc.python.agent.tool_response": "private tenant memory",
            "trpc.python.agent.stream_function_calls.raw": '{"token":"tool-secret"}',
            "http.url": "https://agent.example/callback?msg_signature=secret&echostr=ciphertext",
            "url.query": "token=secret",
            "custom": "model-secret-value and alice@example.com",
            "exception.message": "database password=model-secret-value",
        },
        events=(
            SpanEvent(
                "exception model-secret-value",
                {"exception.stacktrace": "model-secret-value"},
            ),
        ),
        status=Status(StatusCode.ERROR, "model-secret-value"),
    )
    assert exporter.export((span,)) is SpanExportResult.SUCCESS
    exported = delegate.spans[0]
    assert exported.attributes["trpc.python.agent.runner.input"] == "[REDACTED]"  # type: ignore[index]
    assert exported.attributes["trpc.python.agent.llm_response"] == "[REDACTED]"  # type: ignore[index]
    assert exported.attributes["trpc.python.agent.tool_response"] == "[REDACTED]"  # type: ignore[index]
    assert exported.attributes["trpc.python.agent.stream_function_calls.raw"] == "[REDACTED]"  # type: ignore[index]
    assert exported.attributes["url.query"] == "[REDACTED]"  # type: ignore[index]
    assert "msg_signature" not in str(exported.attributes["http.url"])  # type: ignore[index]
    assert "model-secret-value" not in str(exported.attributes)
    assert "alice@example.com" not in str(exported.attributes)
    assert "model-secret-value" not in exported.name
    assert exported.status.description == "[REDACTED]"
    exporter.shutdown()
    assert delegate.closed


@pytest.mark.asyncio
async def test_trpc_event_translation_without_external_model(
    tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    memory = InMemoryPlane()
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant), tenant
    )
    engine = TrpcAgentEngine(
        settings=Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=None,
            session_hmac_key="a-long-enough-test-hmac-key",
            model_timeout_seconds=2,
        ),
        secrets=CompositeSecretResolver(file_root=tmp_path),  # type: ignore[arg-type]
        governance=GovernanceService(Redactor()),
        confirmations=ConfirmationManager(b"a-long-enough-test-hmac-key", memory),
    )
    captured_run: dict[str, object] = {}

    class FakeRunner:
        async def run_async(self, **kwargs: object) -> object:
            captured_run.update(kwargs)
            yield Event(
                id="partial",
                author="agent",
                partial=True,
                content=Content(parts=[Part.from_text(text="hello ")]),
            )
            yield Event(
                id="call",
                author="agent",
                content=Content(parts=[Part(function_call=FunctionCall(name="calculator", args={"x": 1}))]),
            )
            yield Event(
                id="result",
                author="agent",
                content=Content(
                    parts=[
                        Part(function_response=FunctionResponse(name="calculator", response={"result": 1}))
                    ]
                ),
            )
            yield Event(
                id="final",
                author="agent",
                content=Content(parts=[Part.from_text(text="hello world")]),
                usage_metadata=GenerateContentResponseUsageMetadata(
                    prompt_token_count=3,
                    candidates_token_count=2,
                    total_token_count=5,
                ),
            )

    async def fake_bundle(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return SimpleNamespace(
            cache_key=("alpha", "assistant", 1, 0),
            runner=FakeRunner(),
            model_name="fake",
        )

    monkeypatch.setattr(engine, "_bundle", fake_bundle)
    events = [
        event
        async for event in engine.stream(
            tenant=tenant,
            routed=routed,
            effective_text="hello",
            plane=plane(memory),
        )
    ]
    assert [event.event_type.value for event in events] == [
        "text_delta",
        "tool_start",
        "tool_result",
        "text_final",
    ]
    assert events[-1].token_input == 3
    assert events[-1].token_output == 2
    run_config = captured_run["run_config"]
    assert run_config.max_llm_calls == tenant.governance.budget.max_llm_calls_per_request  # type: ignore[attr-defined]
    assert run_config.max_tool_calls == tenant.governance.budget.max_tool_calls_per_request  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_trpc_native_session_result_is_recovered_before_model_rerun(
    tmp_path: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    memory = InMemoryPlane()
    tenant = make_tenant()
    envelope = make_envelope(tenant, message_id="native-recovery")
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(envelope, tenant)
    user = Event(
        id="native-user",
        invocation_id="invocation",
        author="user",
        request_id=envelope.message_id,
        content=Content(parts=[Part.from_text(text="hello")]),
    )
    final = Event(
        id="native-final",
        invocation_id="invocation",
        author="agent",
        content=Content(parts=[Part.from_text(text="recovered answer")]),
        usage_metadata=GenerateContentResponseUsageMetadata(
            prompt_token_count=5,
            candidates_token_count=3,
            total_token_count=8,
        ),
    )
    tool_selection = Event(
        id="native-tool-selection",
        invocation_id="invocation",
        author="agent",
        content=Content(parts=[Part(function_call=FunctionCall(name="calculator", args={"x": 1}))]),
        usage_metadata=GenerateContentResponseUsageMetadata(
            prompt_token_count=10,
            candidates_token_count=1,
            total_token_count=11,
        ),
    )

    class NativeSessionService:
        async def get_session(self, **kwargs: object) -> object:
            del kwargs
            return SimpleNamespace(historical_events=[], events=[user, tool_selection, final])

    async def forbidden_run(**kwargs: object) -> object:
        del kwargs
        raise AssertionError("model was rerun despite native final response")
        yield

    runner = SimpleNamespace(
        app_name="app",
        session_service=NativeSessionService(),
        run_async=forbidden_run,
    )
    bundle = _RunnerBundle(
        cache_key=(tenant.tenant_id, "assistant", 1, 0),
        runner=runner,
        model_name="fake",
        last_used_monotonic=0,
    )
    engine = TrpcAgentEngine(
        settings=Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=None,
            session_hmac_key="a-long-enough-test-hmac-key",
        ),
        secrets=CompositeSecretResolver(file_root=tmp_path),  # type: ignore[arg-type]
        governance=GovernanceService(Redactor()),
        confirmations=ConfirmationManager(b"a-long-enough-test-hmac-key", memory),
    )
    recovered = await engine._recover_native_result(
        bundle,
        routed,
        new_agent_context(metadata={"message_id": envelope.message_id}),
    )
    assert recovered is not None
    assert recovered.text == "recovered answer"
    assert recovered.token_input == 15
    assert recovered.token_output == 4
    assert recovered.payload["recovered_from_native_session"] is True

    async def fake_bundle(*args: object, **kwargs: object) -> _RunnerBundle:
        del args, kwargs
        return bundle

    monkeypatch.setattr(engine, "_bundle", fake_bundle)
    streamed = [
        event
        async for event in engine.stream(
            tenant=tenant,
            routed=routed,
            effective_text="hello",
            plane=plane(memory),
        )
    ]
    assert len(streamed) == 1
    assert streamed[0].text == "recovered answer"


@pytest.mark.asyncio
async def test_deterministic_engine_reports_usage() -> None:
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant, text="hello"), tenant
    )
    memory = InMemoryPlane()
    events = [
        event
        async for event in DeterministicEngine().stream(
            tenant=tenant,
            routed=routed,
            effective_text="hello",
            plane=plane(memory),
        )
    ]
    assert events[-1].token_input > 0 and events[-1].token_output > 0


@pytest.mark.asyncio
async def test_deterministic_engine_answers_common_prompts_without_echoing() -> None:
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant, text="你叫什么名字"), tenant
    )
    memory = InMemoryPlane()
    events = [
        event
        async for event in DeterministicEngine().stream(
            tenant=tenant,
            routed=routed,
            effective_text="你叫什么名字",
            plane=plane(memory),
        )
    ]
    assert events[-1].text == "我是 TestAssistant。"
    assert "你叫什么名字" not in events[-1].text


@pytest.mark.asyncio
async def test_trpc_error_and_timeout_events_preserve_billed_usage(tmp_path: object) -> None:
    memory = InMemoryPlane()
    tenant = make_tenant()
    routed = GatewayRouter(IdentityDeriver("a-long-enough-test-hmac-key")).route(
        make_envelope(tenant, message_id="usage-error"),
        tenant,
    )
    engine = TrpcAgentEngine(
        settings=Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=None,
            session_hmac_key="a-long-enough-test-hmac-key",
            model_timeout_seconds=0.01,
        ),
        secrets=CompositeSecretResolver(file_root=tmp_path),  # type: ignore[arg-type]
        governance=GovernanceService(Redactor()),
        confirmations=ConfirmationManager(b"a-long-enough-test-hmac-key", memory),
    )
    usage = GenerateContentResponseUsageMetadata(
        prompt_token_count=7,
        candidates_token_count=2,
        total_token_count=9,
    )

    class ErrorRunner:
        async def run_async(self, **kwargs: object) -> object:
            del kwargs
            yield SimpleNamespace(
                id="error",
                usage_metadata=usage,
                error_code="provider_error",
                content=None,
            )

    class TimeoutRunner:
        async def run_async(self, **kwargs: object) -> object:
            del kwargs
            yield SimpleNamespace(
                id="usage-before-timeout",
                usage_metadata=usage,
                error_code=None,
                content=None,
            )
            await asyncio.sleep(1)

    class ExceptionRunner:
        async def run_async(self, **kwargs: object) -> object:
            del kwargs
            yield SimpleNamespace(
                id="usage-before-exception",
                usage_metadata=usage,
                error_code=None,
                content=None,
            )
            raise RuntimeError("runner failed")

    async def collect(runner: object) -> list[object]:
        bundle = _RunnerBundle(
            cache_key=("alpha", "assistant", 1, 0),
            runner=runner,
            model_name="fake",
            last_used_monotonic=0,
        )
        return [
            event
            async for event in engine._stream_impl(
                bundle=bundle,
                tenant=tenant,
                routed=routed,
                effective_text="hello",
                plane=plane(memory),
            )
        ]

    error_events = await collect(ErrorRunner())
    assert error_events[-1].payload["error_type"] == "model_error"  # type: ignore[attr-defined]
    assert error_events[-1].token_input == 7  # type: ignore[attr-defined]
    assert error_events[-1].token_output == 2  # type: ignore[attr-defined]

    timeout_events = await collect(TimeoutRunner())
    assert timeout_events[-1].payload["error_type"] == "model_timeout"  # type: ignore[attr-defined]
    assert timeout_events[-1].token_input == 7  # type: ignore[attr-defined]
    assert timeout_events[-1].token_output == 2  # type: ignore[attr-defined]

    exception_events = await collect(ExceptionRunner())
    assert exception_events[-1].payload["error_type"] == "model_error"  # type: ignore[attr-defined]
    assert exception_events[-1].token_input == 7  # type: ignore[attr-defined]
    assert exception_events[-1].token_output == 2  # type: ignore[attr-defined]
