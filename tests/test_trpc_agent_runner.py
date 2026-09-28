from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from pydantic import SecretStr
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import RunConfig, Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, GenerateContentResponseUsageMetadata, Part

from trpc_service.agent import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentExecutionRequest,
    AgentInputArtifact,
    AgentRuntimeConfig,
    AgentToolCall,
    AgentToolInvoker,
    AgentToolResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.adapters.trpc import (
    TRPCAgentRunner,
    _SDKRuntime,
    _close_sdk_runtime,
    _resolve_secret,
    _run_sdk_turn,
)
from trpc_service.channels import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.config import Settings
from trpc_service.skill import BuiltinSkillCatalog
from trpc_service.storage import (
    KnowledgeDocument,
    KnowledgeHit,
    SessionEvent,
    SessionSnapshot,
)
from trpc_service.tenant import TenantContext


class RecordingSDKRunner:

    def __init__(self) -> None:
        self.user_id = ""
        self.session_id = ""
        self.message = ""
        self.shared_context = ""
        self.parts: list[Part] = []

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig,
    ) -> AsyncIterator[Event]:
        del run_config
        self.user_id = user_id
        self.session_id = session_id
        current = new_message[-1] if isinstance(new_message, list) else new_message
        self.parts = list(current.parts)
        self.message = current.parts[0].text or ""
        if isinstance(new_message, list):
            self.shared_context = new_message[0].parts[0].text or ""
        yield Event(
            author="assistant",
            content=Content(role="model", parts=[Part.from_text(text="Agent 回复")]),
            partial=False,
            usageMetadata=GenerateContentResponseUsageMetadata(
                promptTokenCount=12,
                candidatesTokenCount=4,
                totalTokenCount=16,
            ),
        )


class ErrorSDKRunner:

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig,
    ) -> AsyncIterator[Event]:
        del user_id, session_id, new_message, run_config
        yield Event(author="assistant", errorCode="MODEL_ERROR")


class RecoverableToolErrorSDKRunner:
    """Model an SDK tool miss followed by the model's corrected response."""

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig,
    ) -> AsyncIterator[Event]:
        del user_id, session_id, new_message, run_config
        response = Part.from_function_response(
            name="invented_tool",
            response={
                "error": "tool_not_found",
                "status": "failed"
            },
        )
        yield Event(
            author="assistant",
            content=Content(role="user", parts=[response]),
            errorCode="tool_not_found",
            errorMessage="Tool 'invented_tool' not found",
        )
        yield Event(
            author="assistant",
            content=Content(role="model", parts=[Part.from_text(text="该工具不可用，请换一种查询方式。")]),
        )


class EmptySDKRunner:

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig,
    ) -> AsyncIterator[Event]:
        del user_id, session_id, new_message, run_config
        if False:
            yield Event(author="assistant")


class AmbiguitySensitiveSDKRunner:
    """Model double that exposes prior-history/current-turn role ambiguity."""

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig,
    ) -> AsyncIterator[Event]:
        del user_id, session_id, run_config
        # Passing the transcript and the current question as a list makes the
        # SDK merge two consecutive user-role entries. This deterministic
        # double models the observed failure: the old answer wins because the
        # current question is no longer a distinct conversation turn.
        text = "上一轮回复" if isinstance(new_message, list) else "当前问题回复"
        yield Event(
            author="assistant",
            content=Content(role="model", parts=[Part.from_text(text=text)]),
        )


class UnusedToolInvoker(AgentToolInvoker):

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        raise AssertionError("the text-only Agent must not invoke tools")


def _sdk_runner(double: object) -> Runner:
    """Keep SDK-shaped test doubles explicit without weakening production types."""

    return cast(Runner, double)


def _context() -> AgentExecutionContext:
    tenant_id = uuid4()
    agent_app_id = uuid4()
    tenant = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_app_id,
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )
    return AgentExecutionContext(
        request=AgentExecutionRequest(
            tenant=tenant,
            session_id="session-1",
            incoming=IncomingMessage(
                external_message_id="message-1",
                principal_id="browser-user-1",
                conversation_id="conversation-1",
                kind=MessageKind.TEXT,
                occurred_at=datetime.now(timezone.utc),
                text="用户消息",
            ),
            channel=ChannelBindingConfig(
                binding_id=uuid4(),
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                channel_type="wecom",
            ),
        ),
        config=AgentRuntimeConfig(config_version=1, runner_name="trpc_agent"),
        policy=PolicyDecision(action=PolicyAction.ALLOW),
        claim=AgentExecutionClaim(claim_id="claim-1"),
    )


@pytest.mark.anyio
async def test_trpc_agent_runner_maps_context_to_final_text_reply() -> None:
    sdk_runner = RecordingSDKRunner()
    context = _context()

    result = await _run_sdk_turn(_sdk_runner(sdk_runner), context, UnusedToolInvoker())

    assert sdk_runner.user_id.endswith(":browser-user-1")
    assert sdk_runner.session_id.endswith(":session-1")
    assert sdk_runner.message == "用户消息"
    assert len(result.replies) == 1
    assert result.replies[0].kind is MessageKind.TEXT
    assert result.replies[0].text == "Agent 回复"
    assert result.usage.input_tokens == 12
    assert result.usage.output_tokens == 4
    assert result.usage.total_tokens == 16


@pytest.mark.anyio
async def test_trpc_agent_runner_does_not_expose_im_files_as_knowledge_mutations() -> None:
    """An IM file caption remains ordinary model input without RAG write hints."""

    sdk_runner = RecordingSDKRunner()
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            knowledge={"knowledge_base_names": ["handbook"]},
        ),
        request=replace(
            context.request,
            incoming=replace(
                context.request.incoming,
                kind=MessageKind.FILE,
                text="把这个文件加入 handbook 知识库",
                artifact_refs=("tenant-artifact-1", ),
            ),
        ),
    )

    result = await _run_sdk_turn(_sdk_runner(sdk_runner), context, UnusedToolInvoker())

    assert sdk_runner.message == ""
    assert "租户管理员" in (result.replies[0].text or "")
    assert result.state == {}


@pytest.mark.anyio
async def test_trpc_agent_runner_rehydrates_shared_session_history() -> None:
    """A stateless Worker makes durable prior turns visible to the model."""

    sdk_runner = RecordingSDKRunner()
    context = _context()
    context = replace(
        context,
        session=SessionSnapshot(
            session_id=context.request.session_id,
            version=2,
            events=(
                SessionEvent(
                    event_id="event-1",
                    event_type="message.received",
                    occurred_at=datetime.now(timezone.utc),
                    payload={"text": "我叫小白"},
                ),
                SessionEvent(
                    event_id="event-2",
                    event_type="agent.replied",
                    occurred_at=datetime.now(timezone.utc),
                    payload={"text": "你好，小白"},
                ),
            ),
        ),
    )

    sessions = InMemorySessionService()
    await _run_sdk_turn(
        _sdk_runner(sdk_runner),
        context,
        UnusedToolInvoker(),
        sessions,
    )

    session = await sessions.get_session(
        app_name="trpc-agent-service",
        user_id=sdk_runner.user_id,
        session_id=sdk_runner.session_id,
    )
    assert session is not None
    assert [(event.content.role, event.get_text()) for event in session.events] == [
        ("user", "我叫小白"),
        ("model", "你好，小白"),
    ]


@pytest.mark.anyio
async def test_trpc_agent_runner_does_not_answer_the_previous_turn_again() -> None:
    """Durable history must not be merged into the current user turn."""

    context = _context()
    context = replace(
        context,
        session=SessionSnapshot(
            session_id=context.request.session_id,
            version=2,
            events=(
                SessionEvent(
                    event_id="event-previous-user",
                    event_type="message.received",
                    occurred_at=datetime.now(timezone.utc),
                    payload={"text": "上一个问题"},
                ),
                SessionEvent(
                    event_id="event-previous-agent",
                    event_type="agent.replied",
                    occurred_at=datetime.now(timezone.utc),
                    payload={"text": "上一轮回复"},
                ),
            ),
        ),
    )

    result = await _run_sdk_turn(
        _sdk_runner(AmbiguitySensitiveSDKRunner()),
        context,
        UnusedToolInvoker(),
    )

    assert result.replies[0].text == "当前问题回复"


@pytest.mark.anyio
async def test_trpc_agent_runner_injects_citable_tenant_knowledge() -> None:
    sdk_runner = RecordingSDKRunner()
    base_context = _context()
    context = replace(
        base_context,
        config=replace(
            base_context.config,
            knowledge={"knowledge_base_names": ["handbook"]},
        ),
        knowledge=(KnowledgeHit(
            document=KnowledgeDocument(
                document_id="document-1:0",
                knowledge_base_id="base-1",
                content="员工每年享有十二天年假。",
                attributes={
                    "filename": "leave-policy.md",
                    "version": 2,
                    "chunk_index": 0,
                },
            ),
            score=0.91,
        ), ),
    )

    await _run_sdk_turn(_sdk_runner(sdk_runner), context, UnusedToolInvoker())

    assert "[知识1] 员工每年享有十二天年假。" in sdk_runner.message
    assert "来源：leave-policy.md，第 2 版" in sdk_runner.message
    assert "回答中使用知识时请标注 [知识序号]" in sdk_runner.message
    assert "当前用户请求：\n用户消息" in sdk_runner.message
    assert "knowledge_list 或 knowledge_search" in sdk_runner.message


@pytest.mark.anyio
async def test_trpc_agent_runner_does_not_expose_text_attachment_identifiers_to_the_model() -> None:
    sdk_runner = RecordingSDKRunner()
    context = _context()
    incoming = replace(context.request.incoming, artifact_refs=("secret-artifact-id", ))

    await _run_sdk_turn(
        _sdk_runner(sdk_runner),
        replace(context, request=replace(context.request, incoming=incoming)),
        UnusedToolInvoker(),
    )

    assert sdk_runner.message == "用户消息"
    assert "secret-artifact-id" not in sdk_runner.message


@pytest.mark.anyio
async def test_trpc_runner_directs_bare_im_files_to_tenant_management() -> None:
    """A bare IM file cannot create hidden RAG mutation state."""

    sdk_runner = RecordingSDKRunner()
    context = _context()
    incoming = replace(
        context.request.incoming,
        kind=MessageKind.FILE,
        text=None,
        artifact_refs=("tenant-artifact-1", ),
    )

    result = await _run_sdk_turn(
        _sdk_runner(sdk_runner),
        replace(context, request=replace(context.request, incoming=incoming)),
        UnusedToolInvoker(),
    )

    assert sdk_runner.message == ""
    assert "租户管理员" in (result.replies[0].text or "")
    assert "管理台" in (result.replies[0].text or "")
    assert result.state == {}


@pytest.mark.anyio
async def test_trpc_agent_runner_sends_tenant_image_bytes_to_multimodal_model() -> None:
    sdk_runner = RecordingSDKRunner()
    context = _context()
    incoming = replace(
        context.request.incoming,
        kind=MessageKind.IMAGE,
        text=None,
        artifact_refs=("image-artifact-id", ),
    )
    context = replace(
        context,
        request=replace(context.request, incoming=incoming),
        input_artifacts=(AgentInputArtifact(
            artifact_id="image-artifact-id",
            media_type="image/png",
            filename="photo.png",
            content=b"\x89PNG\r\n\x1a\nimage",
        ), ),
    )

    await _run_sdk_turn(_sdk_runner(sdk_runner), context, UnusedToolInvoker())

    assert sdk_runner.parts[0].text == "请描述这张图片。"
    assert sdk_runner.parts[1].inline_data is not None
    assert sdk_runner.parts[1].inline_data.mime_type == "image/png"
    assert sdk_runner.parts[1].inline_data.data == b"\x89PNG\r\n\x1a\nimage"


@pytest.mark.anyio
async def test_trpc_agent_runner_fails_closed_on_invalid_or_empty_model_output() -> None:
    context = _context()
    invalid_request = replace(
        context.request,
        incoming=replace(context.request.incoming, kind=MessageKind.IMAGE, text=None),
    )

    with pytest.raises(ValueError, match="not resolved from tenant storage"):
        await _run_sdk_turn(
            _sdk_runner(RecordingSDKRunner()),
            replace(context, request=invalid_request),
            UnusedToolInvoker(),
        )
    with pytest.raises(RuntimeError, match="MODEL_ERROR"):
        await _run_sdk_turn(
            _sdk_runner(ErrorSDKRunner()),
            context,
            UnusedToolInvoker(),
        )
    with pytest.raises(RuntimeError, match="no final text"):
        await _run_sdk_turn(
            _sdk_runner(EmptySDKRunner()),
            context,
            UnusedToolInvoker(),
        )


@pytest.mark.anyio
async def test_trpc_agent_runner_allows_sdk_to_recover_from_tool_errors() -> None:
    result = await _run_sdk_turn(
        _sdk_runner(RecoverableToolErrorSDKRunner()),
        _context(),
        UnusedToolInvoker(),
    )

    assert result.replies[0].text == "该工具不可用，请换一种查询方式。"


@pytest.mark.anyio
async def test_agent_factory_builds_bailian_runner_from_runtime_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cover both supported SecretRef resolvers and SDK composition."""

    secret_file = tmp_path / "model-key"
    secret_file.write_text("file-secret", encoding="utf-8")
    monkeypatch.setenv("TENANT_MODEL_KEY", "environment-secret")
    settings = Settings(
        _env_file=None,
        dashscope_api_key=SecretStr("dashscope-secret"),
    )
    runtime = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        application={"instruction": "Runtime instruction"},
        model={
            "model_name": "qwen-max",
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "api_key_ref": "env://TENANT_MODEL_KEY",
            "temperature": 0.4,
            "max_output_tokens": 128,
        },
    )

    runner = TRPCAgentRunner(settings, skills=BuiltinSkillCatalog())
    context = replace(_context(), config=runtime)
    sdk_runtime = await runner._build_runtime(context, UnusedToolInvoker())

    assert isinstance(runner, TRPCAgentRunner)
    assert _resolve_secret(settings, "env://DASHSCOPE_API_KEY") == "dashscope-secret"
    assert _resolve_secret(settings, f"file://{secret_file}") == "file-secret"
    with pytest.raises(RuntimeError, match="resolver is not configured"):
        _resolve_secret(settings, "vault://models/key")
    with pytest.raises(RuntimeError, match="credential is empty"):
        _resolve_secret(settings, "env://MISSING_MODEL_KEY")
    await _close_sdk_runtime(sdk_runtime)


@pytest.mark.anyio
async def test_agent_runner_rejects_non_numeric_generation_parameters() -> None:
    settings = Settings(_env_file=None, dashscope_api_key=SecretStr("dashscope-secret"))
    invalid_temperature = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        model={
            "temperature": {},
            "api_key_ref": "env://DASHSCOPE_API_KEY"
        },
    )
    invalid_tokens = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        model={
            "max_output_tokens": [],
            "api_key_ref": "env://DASHSCOPE_API_KEY"
        },
    )

    runner = TRPCAgentRunner(settings, skills=BuiltinSkillCatalog())
    with pytest.raises(ValueError, match="temperature must be numeric"):
        await runner._build_runtime(
            replace(_context(), config=invalid_temperature),
            UnusedToolInvoker(),
        )
    with pytest.raises(ValueError, match="max_output_tokens must be numeric"):
        await runner._build_runtime(
            replace(_context(), config=invalid_tokens),
            UnusedToolInvoker(),
        )


@pytest.mark.anyio
async def test_trpc_runner_closes_request_scoped_sdk_runner(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    closed = False

    class ClosingSDKRunner(RecordingSDKRunner):

        async def close(self) -> None:
            nonlocal closed
            closed = True

    sdk_runner = ClosingSDKRunner()
    runner = TRPCAgentRunner(Settings(_env_file=None), skills=BuiltinSkillCatalog())

    async def build_runtime(
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> _SDKRuntime:
        del context, tools
        return _SDKRuntime(
            runner=_sdk_runner(sdk_runner),
            sessions=InMemorySessionService(),
        )

    monkeypatch.setattr(runner, "_build_runtime", build_runtime)

    result = await runner.run(_context(), UnusedToolInvoker())

    assert result.replies[0].text == "Agent 回复"
    assert closed is True


@pytest.mark.anyio
async def test_reasoning_parts_never_enter_reply_or_durable_history() -> None:

    class ReasoningRunner:

        async def run_async(self, **kwargs: object) -> AsyncIterator[Event]:
            yield Event(
                author="assistant",
                content=Content(role="model",
                                parts=[
                                    Part(text="private reasoning", thought=True),
                                    Part(text="最终答案"),
                                ]),
            )

    result = await _run_sdk_turn(_sdk_runner(ReasoningRunner()), _context(), UnusedToolInvoker())
    assert result.replies[0].text == "最终答案"
    assert result.events[-1].payload["text"] == "最终答案"


@pytest.mark.anyio
async def test_reasoning_only_response_is_not_a_successful_reply() -> None:

    class ReasoningRunner:

        async def run_async(self, **kwargs: object) -> AsyncIterator[Event]:
            yield Event(author="assistant",
                        content=Content(role="model",
                                        parts=[
                                            Part(text="private reasoning", thought=True),
                                        ]))

    with pytest.raises(RuntimeError, match="no final text"):
        await _run_sdk_turn(_sdk_runner(ReasoningRunner()), _context(), UnusedToolInvoker())


@pytest.mark.anyio
async def test_sdk_error_stream_finishes_in_consuming_task() -> None:
    from contextvars import ContextVar

    marker: ContextVar[str] = ContextVar("sdk_stream_marker", default="outside")
    finished = []

    class ErrorRunner:

        async def run_async(self, **kwargs: object) -> AsyncIterator[Event]:
            token = marker.set("inside")
            try:
                yield Event(author="assistant", errorCode="MODEL_ERROR")
            finally:
                marker.reset(token)
                finished.append(True)

    with pytest.raises(RuntimeError, match="MODEL_ERROR"):
        await _run_sdk_turn(_sdk_runner(ErrorRunner()), _context(), UnusedToolInvoker())
    assert finished == [True]
    assert marker.get() == "outside"


@pytest.mark.anyio
async def test_tool_turn_usage_includes_every_model_call() -> None:

    class ToolTurnRunner:

        async def run_async(self, **kwargs: object) -> AsyncIterator[Event]:
            yield Event(
                author="assistant",
                content=Content(role="model",
                                parts=[
                                    Part.from_function_call(name="calculate",
                                                            args={"expression": "2+3"}),
                                ]),
                usageMetadata=GenerateContentResponseUsageMetadata(promptTokenCount=100,
                                                                   candidatesTokenCount=20,
                                                                   totalTokenCount=120),
            )
            yield Event(
                author="assistant",
                content=Content(role="model", parts=[Part(text="5")]),
                usageMetadata=GenerateContentResponseUsageMetadata(promptTokenCount=150,
                                                                   candidatesTokenCount=10,
                                                                   totalTokenCount=160),
            )

    result = await _run_sdk_turn(_sdk_runner(ToolTurnRunner()), _context(), UnusedToolInvoker())
    assert result.usage.input_tokens == 250
    assert result.usage.output_tokens == 30
    assert result.usage.total_tokens == 280


@pytest.mark.anyio
async def test_real_sdk_preserves_restored_conversation_roles(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the SDK history processor, not only the stored Event objects."""
    from trpc_agent_sdk.models import LlmRequest, LlmResponse, OpenAIModel

    messages = []

    async def capture_model(self: object, request: LlmRequest, stream: bool,
                            ctx: object) -> AsyncIterator[LlmResponse]:
        messages.extend((content.role, "".join(part.text or "" for part in content.parts))
                        for content in request.contents)
        yield LlmResponse(content=Content(role="model", parts=[Part(text="当前答案")]))

    monkeypatch.setattr(OpenAIModel, "_generate_async_impl", capture_model)
    context = _context()
    context = replace(context,
                      session=SessionSnapshot(
                          session_id=context.request.session_id,
                          version=2,
                          events=(
                              SessionEvent(event_id="old-user",
                                           event_type="message.received",
                                           occurred_at=datetime.now(timezone.utc),
                                           payload={"text": "旧问题"}),
                              SessionEvent(event_id="old-model",
                                           event_type="agent.replied",
                                           occurred_at=datetime.now(timezone.utc),
                                           payload={"text": "旧答案"}),
                          )))
    runner = TRPCAgentRunner(Settings(_env_file=None, dashscope_api_key=SecretStr("test")),
                             skills=BuiltinSkillCatalog())
    result = await runner.run(context, UnusedToolInvoker())
    assert result.replies[0].text == "当前答案"
    assert messages == [("user", "旧问题"), ("model", "旧答案"), ("user", "用户消息")]
