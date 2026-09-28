"""Direct tRPC-Agent-Python Runner integration for the platform pipeline."""

from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from inspect import isawaitable
import os
from pathlib import Path
from uuid import uuid4

from trpc_agent_sdk.abc import SessionServiceABC, ToolABC
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import RunConfig, Runner
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig
from trpc_agent_sdk.skills import BaseSkillRepository
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.types import Content, GenerateContentConfig, Part

from trpc_service.agent.adapters.model import PlatformOpenAIModel
from trpc_service.agent.adapters.trpc_tools import CapabilityCallSequence, TRPCToolBridge
from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentReply,
    AgentRunResult,
    AgentUsage,
)
from trpc_service.agent.ports import AgentToolInvoker
from trpc_service.channels.contracts import MessageKind
from trpc_service.config import Settings
from trpc_service.mcp import TenantMCPService
from trpc_service.skill import BuiltinSkillCatalog, KnowledgeOnlySkillToolset
from trpc_service.storage.types import SessionEvent


@dataclass(frozen=True, slots=True)
class _SDKRuntime:
    """Request-scoped SDK resources owned by one platform Runner call."""

    runner: Runner
    sessions: SessionServiceABC
    model: PlatformOpenAIModel | None = None


def _resolve_secret(settings: Settings, reference: str) -> str:
    """Resolve supported local SecretRefs without persisting the secret value."""

    scheme, _, target = reference.partition("://")
    if scheme == "env":
        value = (settings.dashscope_api_key.get_secret_value()
                 if target == "DASHSCOPE_API_KEY" else os.environ.get(target, ""))
    elif scheme == "file":
        value = Path(target).read_text(encoding="utf-8").strip()
    else:
        raise RuntimeError(f"SecretRef resolver is not configured for scheme: {scheme}")
    if value.strip() == "":
        raise RuntimeError("resolved model credential is empty")
    return value


def _shared_context(context: AgentExecutionContext) -> Content | None:
    """Convert durable Session and Memory facts into model-visible context."""

    lines: list[str] = []
    if context.memories:
        lines.append("与当前用户相关的长期记忆：")
        lines.extend(f"- {hit.record.content}" for hit in context.memories[:10])
    if context.knowledge:
        lines.append("当前租户知识库检索结果：")
        for index, hit in enumerate(context.knowledge[:10], start=1):
            filename = hit.document.attributes.get("filename", "未知文件")
            version = hit.document.attributes.get("version", "未知")
            lines.append(f"[知识{index}] {hit.document.content}")
            lines.append(f"来源：{filename}，第 {version} 版")
        lines.append("回答中使用知识时请标注 [知识序号]；知识不足时应明确说明。")
    if not lines:
        return None
    return Content(
        role="user",
        parts=[Part.from_text(text="以下是本轮可参考的补充资料：\n" + "\n".join(lines))],
    )


async def _hydrate_session(
    sessions: SessionServiceABC | None,
    context: AgentExecutionContext,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
) -> None:
    """Restore durable turns as real SDK user/model history entries."""

    if sessions is None or context.session is None:
        return
    session = await sessions.get_session(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    if session is not None:
        return
    session = await sessions.create_session(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    # The platform cache already bounds recent events. Keep a second adapter-side
    # bound for alternate durable Session implementations.
    for event in context.session.events[-40:]:
        text = event.payload.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        if event.event_type == "message.received":
            author, role = "user", "user"
        elif event.event_type == "agent.replied":
            author, role = "assistant", "model"
        else:
            continue
        await sessions.append_event(
            session,
            Event(
                id=event.event_id,
                invocation_id=event.event_id,
                author=author,
                branch="assistant" if role == "model" else None,
                timestamp=event.occurred_at.timestamp(),
                content=Content(role=role, parts=[Part.from_text(text=text)]),
            ),
        )


async def _run_sdk_turn(
    runner: Runner,
    context: AgentExecutionContext,
    tools: AgentToolInvoker,
    sessions: SessionServiceABC | None = None,
    *,
    app_name: str = "trpc-agent-service",
) -> AgentRunResult:
    """Translate one platform turn directly to and from the SDK Runner."""

    # Tools have already been converted to SDK FunctionTool instances while the
    # request-scoped Runner was built. Keep the argument to satisfy the stable
    # platform Runner boundary used by the pipeline.
    del tools
    incoming = context.request.incoming
    if incoming.kind not in {MessageKind.TEXT, MessageKind.IMAGE, MessageKind.FILE}:
        raise ValueError("the tRPC Agent runner supports text, image, and file messages only")
    if incoming.kind is MessageKind.TEXT and incoming.text is None:
        raise ValueError("text message content cannot be empty")
    if incoming.kind is MessageKind.IMAGE and not context.input_artifacts:
        raise ValueError("image message content was not resolved from tenant storage")
    if incoming.kind is MessageKind.FILE and not incoming.artifact_refs:
        raise ValueError("file message does not contain a persisted Artifact")

    if incoming.kind is MessageKind.FILE:
        # Ordinary IM users may send files, but only a tenant administrator can
        # mutate knowledge sources through the management console.
        reply_text = "文件已接收，但当前不会解析 IM 文件内容。知识库文件请由租户管理员在管理台上传和维护。"
        occurred_at = datetime.now(timezone.utc)
        return AgentRunResult(
            replies=(AgentReply(kind=MessageKind.TEXT, text=reply_text), ),
            events=(
                SessionEvent(
                    event_id=str(uuid4()),
                    event_type="message.received",
                    occurred_at=incoming.occurred_at,
                    payload={
                        "kind": incoming.kind.value,
                        "text": incoming.text,
                        "artifact_refs": list(incoming.artifact_refs),
                    },
                ),
                SessionEvent(
                    event_id=str(uuid4()),
                    event_type="agent.replied",
                    occurred_at=occurred_at,
                    payload={
                        "kind": MessageKind.TEXT.value,
                        "text": reply_text
                    },
                ),
            ),
        )

    tenant = context.request.tenant
    scoped_user_id = f"{tenant.tenant_id}:{incoming.principal_id}"
    scoped_session_id = f"{tenant.agent_app_id}:{context.request.session_id}"
    current_text = ((incoming.text or "请描述这张图片。")
                    if incoming.kind is MessageKind.IMAGE else incoming.text or "")
    raw_base_names = context.config.knowledge.get("knowledge_base_names", ())
    base_names = (tuple(
        name.strip() for name in raw_base_names
        if isinstance(name, str) and name.strip()) if isinstance(raw_base_names, Sequence)
                  and not isinstance(raw_base_names, (str, bytes)) else ())
    if base_names:
        # Resource names come from the immutable Agent configuration. Make them
        # explicit so the model never invents a knowledge-base alias.
        current_text += ("\n\n[平台提示：已授权知识库：" + "、".join(base_names) +
                         "；调用 knowledge_list 或 knowledge_search 时，"
                         "knowledge_base_name 必须从上述名称中选择。]")
    parts = [Part.from_text(text=current_text)]
    parts.extend(
        Part.from_bytes(data=artifact.content, mime_type=artifact.media_type)
        for artifact in context.input_artifacts)
    content = Content(role="user", parts=parts)
    shared_context = _shared_context(context)
    if shared_context is not None:
        supplemental = shared_context.parts[0].text or ""
        current = content.parts[0].text or ""
        content.parts[0].text = f"{supplemental}\n\n当前用户请求：\n{current}"
    await _hydrate_session(
        sessions,
        context,
        app_name=app_name,
        user_id=scoped_user_id,
        session_id=scoped_session_id,
    )
    final_text = ""
    usage = AgentUsage()
    model_error: RuntimeError | None = None
    async for event in runner.run_async(
            user_id=scoped_user_id,
            session_id=scoped_session_id,
            new_message=content,
            run_config=RunConfig(save_history_enabled=True),
    ):
        if event.is_error():
            # Tool errors are model-visible function responses. Let the model
            # correct its call; provider/model errors still fail closed.
            recoverable_tool_error = (event.error_code
                                      in {"tool_not_found", "tool_execution_error"}
                                      and bool(event.get_function_responses()))
            if recoverable_tool_error:
                continue
            # Let the SDK unwind its nested generators in this task. Raising
            # at a yield leaves tracing ContextVars to a foreign GC task.
            if model_error is None:
                model_error = RuntimeError(
                    f"tRPC Agent execution failed: {event.error_code or 'unknown'}")
            continue
        if event.is_final_response():
            # SDK get_text() also includes thought=True reasoning parts.
            # Only the public answer may enter Session facts or the Outbox.
            final_text = "".join(part.text for part in event.content.parts
                                 if part.text and not part.thought) if event.content else ""
        if event.usage_metadata is not None and not event.partial:
            metadata = event.usage_metadata
            usage = AgentUsage(
                input_tokens=usage.input_tokens + (metadata.prompt_token_count or 0),
                output_tokens=usage.output_tokens + (metadata.candidates_token_count or 0),
                total_tokens=usage.total_tokens + (metadata.total_token_count or 0),
            )

    if model_error is not None:
        raise model_error
    if final_text.strip() == "":
        raise RuntimeError("tRPC Agent returned no final text response")

    occurred_at = datetime.now(timezone.utc)
    return AgentRunResult(
        replies=(AgentReply(kind=MessageKind.TEXT, text=final_text), ),
        events=(
            SessionEvent(
                event_id=str(uuid4()),
                event_type="message.received",
                occurred_at=incoming.occurred_at,
                payload={
                    "kind": incoming.kind.value,
                    "text": incoming.text
                },
            ),
            SessionEvent(
                event_id=str(uuid4()),
                event_type="agent.replied",
                occurred_at=occurred_at,
                payload={
                    "kind": MessageKind.TEXT.value,
                    "text": final_text
                },
            ),
        ),
        usage=usage,
    )


async def _close_sdk_runtime(runtime: _SDKRuntime) -> None:
    """Release resources owned by a request-scoped SDK Runner."""

    close = getattr(runtime.runner, "close", None)
    if close is None:
        return
    result: Awaitable[object] | object = close()
    if isawaitable(result):
        await result


class TRPCAgentRunner:
    """Build and execute the tenant-scoped tRPC SDK Runner for each request."""

    def __init__(
        self,
        settings: Settings,
        *,
        mcp: TenantMCPService | None = None,
        skills: BuiltinSkillCatalog,
    ) -> None:
        self._settings = settings
        self._mcp = mcp
        self._skills = skills

    async def _build_runtime(
        self,
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> _SDKRuntime:
        """Create the SDK model, tools, skills and in-memory session view."""

        model_config = dict(context.config.model)
        has_images = bool(context.input_artifacts)
        model_field = "vision_model_name" if has_images else "model_name"
        model_default = (self._settings.llm.vision_model_name
                         if has_images else self._settings.llm.model_name)
        model_name = str(model_config.get(model_field, model_default))
        base_url = str(model_config.get("base_url", self._settings.llm.base_url))
        api_key_ref = str(model_config.get("api_key_ref", self._settings.llm.api_key_ref))
        raw_temperature = model_config.get("temperature", self._settings.llm.temperature)
        raw_max_tokens = model_config.get(
            "max_output_tokens",
            self._settings.llm.max_output_tokens,
        )
        if not isinstance(raw_temperature, (str, int, float)):
            raise ValueError("model temperature must be numeric")
        if not isinstance(raw_max_tokens, (str, int, float)):
            raise ValueError("model max_output_tokens must be numeric")
        thinking = model_config.get("enable_thinking")
        if thinking is not None and not isinstance(thinking, bool):
            raise ValueError("enable_thinking must be a boolean")
        generation_config = GenerateContentConfig(
            temperature=float(raw_temperature),
            max_output_tokens=int(raw_max_tokens),
            http_options=None
            if thinking is None else {"extra_body": {
                "enable_thinking": thinking
            }},
        )
        model = PlatformOpenAIModel(
            model_name=model_name,
            api_key=_resolve_secret(self._settings, api_key_ref),
            base_url=base_url,
            generate_content_config=generation_config,
        )
        instruction = str(
            context.config.application.get("instruction", self._settings.agent_instruction))
        sdk_tools: list[object] = []
        skill_repository: BaseSkillRepository | None = None
        if not has_images:
            sequence = CapabilityCallSequence()
            sdk_tools.extend(
                FunctionTool(function)
                for function in TRPCToolBridge(context, tools, sequence).functions())
            mcp_tools: list[ToolABC] = ([] if self._mcp is None else await self._mcp.tools_for(
                context,
                tools,
                sequence,
            ))
            sdk_tools.extend(mcp_tools)
            skill_repository = self._skills.repository_for(context.config)
            if skill_repository.summaries():
                sdk_tools.append(KnowledgeOnlySkillToolset(skill_repository))
        agent = LlmAgent(
            name="assistant",
            description="Tenant-scoped assistant shared by configured IM channels.",
            instruction=instruction,
            model=model,
            tools=sdk_tools,
            skill_repository=skill_repository,
            generate_content_config=generation_config,
        )
        sessions = InMemorySessionService(session_config=SessionServiceConfig(
            max_events=self._settings.session_cache_max_events,
            num_recent_events=self._settings.session_cache_max_events,
        ))
        runner = Runner(
            app_name=self._settings.service_name,
            agent=agent,
            session_service=sessions,
            # Platform Session/Memory/Outbox adapters own post-turn persistence.
            enable_post_turn_processing=False,
        )
        return _SDKRuntime(runner=runner, sessions=sessions, model=model)

    async def run(
        self,
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> AgentRunResult:
        """Execute one request with the tenant's immutable runtime configuration."""

        runtime = await self._build_runtime(context, tools)
        try:
            return await _run_sdk_turn(
                runtime.runner,
                context,
                tools,
                runtime.sessions,
                app_name=self._settings.service_name,
            )
        except RuntimeError:
            if runtime.model is not None and runtime.model.configuration_error is not None:
                raise runtime.model.configuration_error from None
            raise
        finally:
            await _close_sdk_runtime(runtime)
