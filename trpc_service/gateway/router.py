"""Stateless Gateway and local Worker implementation."""

from __future__ import annotations

import os
from dataclasses import asdict, replace
from datetime import timedelta
from time import monotonic, sleep
from uuid import uuid4

from trpc_service.agent.model_client import ModelResponse, ResponsesModelClient
from trpc_service.channels.base import InboundMessage
from trpc_service.gateway.session_id import (
    build_idempotency_key,
    session_id_for_message,
)
from trpc_service.gateway.worker_queue import WorkerQueue
from trpc_service.policy.approval import approval_id, approval_token, verify_approval_token
from trpc_service.policy.quota import QuotaEnforcer, QuotaExceeded
from trpc_service.policy.tenant_filter import PolicyDenied, TenantPolicy
from trpc_service.security.secrets import SecretManager
from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary, now_utc
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.locking import (
    SessionLease,
    SessionLeaseHeartbeat,
    renew_session_lease,
    session_lease,
    validate_session_lease,
)
from trpc_service.storage.manager import TenantStorageManager
from trpc_service.storage.retry import retry_delay_seconds
from trpc_service.storage.tool_governance import (
    ApprovalStatus,
    ToolExecutionStatus,
    arguments_hash,
)
from trpc_service.telemetry.metrics import (
    observe_cost,
    observe_error,
    observe_model_latency,
    observe_request,
    observe_tokens,
    observe_tool,
    observe_tool_latency,
)
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.tenant.models import (
    AgentEvent,
    RunRequest,
    TenantConfig,
    TenantContext,
    UserInput,
)
from trpc_service.tenant.service import TenantService
from trpc_service.tool.runtime import ToolRegistry, ToolResult


class GatewayError(RuntimeError):
    pass


class _SessionMailboxV2Adapter:
    """Keep the gateway's synchronous mailbox contract over session v2."""

    def __init__(self, store) -> None:
        self.store = store

    def enqueue(self, tenant_id, session_id, message_id, dedupe_key, payload):
        del dedupe_key
        return self.store.accept(
            tenant_id,
            session_id,
            message_id,
            priority=int(payload.get("priority", 0)),
            trace_id=str(payload.get("trace_id") or message_id),
        )

    def claim_next(self, tenant_id, session_id, owner, lease_seconds=None):
        result = self.store.claim_session(
            tenant_id,
            session_id,
            owner,
            lease_seconds or int(os.getenv("MAILBOX_LEASE_SECONDS", "180")),
        )
        return result.lease if result.claimed else None

    def has_unresolved_message(self, tenant_id, session_id, message_id):
        return self.store.has_unresolved_message(tenant_id, session_id, message_id)

    def complete(self, lease):
        return self.store.commit(lease)

    def fail(self, lease, error, retry_after_seconds=0):
        failure_count = lease.retry_count + 1
        try:
            max_attempts = int(os.getenv("SESSION_MAILBOX_MAX_ATTEMPTS", "5"))
        except ValueError as exc:
            raise GatewayError("SESSION_MAILBOX_MAX_ATTEMPTS must be an integer") from exc
        if max_attempts < 1:
            raise GatewayError("SESSION_MAILBOX_MAX_ATTEMPTS must be positive")
        if failure_count >= max_attempts:
            return self.store.dead_letter(lease, error)
        delay = retry_after_seconds or retry_delay_seconds(
            failure_count,
            identity=f"{lease.tenant_id}:{lease.session_id}:{lease.message_id}",
            base_env="SESSION_MAILBOX_RETRY_BASE_SECONDS",
            cap_env="SESSION_MAILBOX_RETRY_MAX_SECONDS",
            default_base=2,
            default_cap=120,
        )
        retry_at = (
            now_utc() + timedelta(seconds=max(0, delay))
            if delay
            else None
        )
        return self.store.retry(lease, retry_at=retry_at)


def _mailbox_failure_is_terminal(result, lease) -> bool:
    return bool(
        result is not None
        and hasattr(result, "resolved_sequence")
        and int(result.resolved_sequence) >= int(lease.sequence)
    )


def _max_tool_rounds() -> int:
    try:
        value = int(os.getenv("AGENT_MAX_TOOL_ROUNDS", "8"))
    except ValueError as exc:
        raise GatewayError("AGENT_MAX_TOOL_ROUNDS must be an integer") from exc
    if value < 1 or value > 32:
        raise GatewayError("AGENT_MAX_TOOL_ROUNDS must be between 1 and 32")
    return value


def _model_tool_messages(calls: list[dict], events: list[AgentEvent]) -> list[dict]:
    tool_calls = [
        {
            "id": str(call["call_id"]),
            "type": "function",
            "function": {
                "name": str(call["name"]),
                "arguments": __import__("json").dumps(call["arguments"], ensure_ascii=False),
            },
        }
        for call in calls
    ]
    messages: list[dict] = [{"role": "assistant", "content": "", "tool_calls": tool_calls}]
    results = {
        str(event.metadata.get("call_id", "")): event
        for event in events
        if event.event_type == "tool_call"
    }
    for call in calls:
        event = results.get(str(call["call_id"]))
        if event is None:
            raise GatewayError(f"tool {call['name']} did not return a result")
        messages.append(
            {
                "role": "tool",
                "tool_call_id": str(call["call_id"]),
                "content": event.content,
            }
        )
    return messages


def _combine_usage(previous: ModelResponse, current: ModelResponse) -> ModelResponse:
    return ModelResponse(
        current.text,
        previous.input_tokens + current.input_tokens,
        previous.output_tokens + current.output_tokens,
        previous.total_tokens + current.total_tokens,
        current.model or previous.model,
        current.tool_calls,
        current.response_id,
    )


class AgentWorker:
    """A stateless worker: every request reloads state from StorageBundle."""

    def __init__(
        self,
        storage: StorageBundle,
        telemetry: TraceRecorder | None = None,
        model_client: ResponsesModelClient | None = None,
    ) -> None:
        self.storage = storage
        self.telemetry = telemetry or TraceRecorder()
        self.model_client = model_client
        self.secrets = SecretManager()
        self.executions = 0
        self.tools = ToolRegistry()
        self.tools.register("search_knowledge", self._search_knowledge)

    def run(
        self,
        request: RunRequest,
        config: TenantConfig,
        storage: StorageBundle | None = None,
    ) -> list[AgentEvent]:
        storage = storage or self.storage
        context = request.tenant_context
        with session_lease(
            storage.session,
            context.tenant_id,
            context.session_id or "",
        ) as lease:
            if isinstance(lease, SessionLease):
                with SessionLeaseHeartbeat(storage.session, lease):
                    return self._run_unlocked(request, config, storage, lease)
            return self._run_unlocked(request, config, storage, lease)

    def _run_unlocked(
        self,
        request: RunRequest,
        config: TenantConfig,
        storage: StorageBundle | None = None,
        lease: SessionLease | None = None,
    ) -> list[AgentEvent]:
        started = monotonic()
        context = request.tenant_context
        app = config.app(context.agent_app_id)
        storage = storage or self.storage
        for server in app.metadata.get("mcp_servers", []):
            self.tools.register_mcp_server(**server, tenant_id=context.tenant_id)
        policy = TenantPolicy(config, context.agent_app_id)
        events: list[AgentEvent] = []
        with self.telemetry.span("runner.run", context):
            try:
                policy.check_input(request.user_input.text)
            except PolicyDenied as exc:
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=context.tenant_id,
                        channel=context.channel,
                        user_id=context.user_id,
                        session_id=context.session_id,
                        agent_name=app.agent_name,
                        decision="deny",
                        error_type="policy_denied",
                        trace_id=context.trace_id,
                        metadata={"reason": str(exc)},
                    )
                )
                raise
            with self.telemetry.span("storage.session.load_events", context):
                prior = storage.session.load_events(context.tenant_id, context.session_id or "", after_seq=0)
            with self.telemetry.span("storage.summary.latest", context):
                latest_summary = storage.summary.latest(context.tenant_id, context.session_id or "")
            with self.telemetry.span("storage.memory.search", context):
                memories = storage.memory.search(
                    context.tenant_id,
                    request.user_input.text,
                    limit=5,
                    scope_keys=tuple(
                        value
                        for value in (
                            context.session_id or "",
                            f"user:{context.user_id}" if context.user_id else "",
                            f"tenant:{context.tenant_id}",
                        )
                        if value
                    ),
                )
            event = SessionEvent(
                tenant_id=context.tenant_id,
                session_id=context.session_id or "",
                event_id=str(uuid4()),
                event_type="user_message",
                payload={
                    "text": request.user_input.text,
                    "metadata": request.user_input.metadata,
                },
                trace_id=context.trace_id,
                idempotency_key=request.idempotency_key,
            )
            lease = self._checkpoint_lease(storage, lease)
            with self.telemetry.span("storage.session.append_event", context):
                seq = storage.session.append_event(
                    event,
                    fencing_token=self._fencing_token(lease),
                )

            tool_events, tool_context = self._run_tools(request, app, policy, storage, lease=lease)
            if any(event.event_type == "approval_required" for event in tool_events):
                self.executions += 1
                return tool_events

            with self.telemetry.span("model.generate", context):
                model_started = monotonic()
                tool_schemas = self.tools.tool_schemas(
                    set(app.tool_policy.allowlist).intersection(self.tools.registered_names)
                )
                generated = self._generate_answer(
                    request.user_input.text,
                    app,
                    prior,
                    policy.redact(tool_context),
                    summary_context=policy.redact(latest_summary.content if latest_summary else ""),
                    memory_context="\n".join(policy.redact(item.content) for item in memories),
                    redact=policy.redact,
                    tools=tool_schemas,
                )
                model_response = (
                    generated
                    if isinstance(generated, ModelResponse)
                    else ModelResponse(
                        str(generated),
                        len((request.user_input.text + tool_context).split()),
                        len(str(generated).split()),
                        len((request.user_input.text + tool_context + str(generated)).split()),
                    )
                )
                model_events: list[AgentEvent] = []
                conversation = self._build_conversation(
                    request.user_input.text,
                    prior,
                    policy.redact(tool_context),
                    policy.redact(latest_summary.content if latest_summary else ""),
                    "\n".join(policy.redact(item.content) for item in memories),
                    policy.redact,
                )
                max_rounds = _max_tool_rounds()
                round_index = 0
                while model_response.tool_calls:
                    if round_index >= max_rounds:
                        self._raise_model_tool_error(
                            storage,
                            request,
                            app,
                            "tool_round_limit",
                            f"model exceeded AGENT_MAX_TOOL_ROUNDS={max_rounds}",
                        )
                    calls = []
                    for call in model_response.tool_calls:
                        if call.arguments_error:
                            self._raise_model_tool_error(
                                storage,
                                request,
                                app,
                                "tool_arguments_invalid",
                                f"model tool call {call.name or '<unknown>'}: {call.arguments_error}",
                            )
                        call_id = call.call_id or f"round-{round_index}-call-{len(calls)}"
                        calls.append(
                            {
                                "name": call.name,
                                "arguments": call.arguments,
                                "call_id": call_id,
                                "_tool_key": f"{request.idempotency_key}:model:{round_index}:{call_id}",
                                # A model-selected non-read-only tool is treated
                                # conservatively after recovery.
                                "side_effect": call.name != "search_knowledge",
                            }
                        )
                    model_events_for_round, _ = self._run_tools(
                        request,
                        app,
                        policy,
                        storage,
                        calls=calls,
                        lease=lease,
                    )
                    model_events.extend(model_events_for_round)
                    if any(event.event_type == "approval_required" for event in model_events_for_round):
                        self.executions += 1
                        return [*tool_events, *model_events_for_round]
                    conversation.extend(_model_tool_messages(calls, model_events_for_round))
                    round_index += 1
                    next_response = self._generate_from_conversation(app, conversation, tool_schemas)
                    model_response = _combine_usage(model_response, next_response)
                answer = model_response.text
                observe_model_latency(context.tenant_id, monotonic() - model_started)
            answer = policy.redact(answer)
            events.extend(tool_events)
            events.extend(model_events)
            events.append(
                AgentEvent(
                    "message_end",
                    answer,
                    {
                        "input_tokens": model_response.input_tokens,
                        "output_tokens": model_response.output_tokens,
                        "total_tokens": model_response.total_tokens,
                    },
                )
            )

            lease = self._checkpoint_lease(storage, lease)
            with self.telemetry.span("storage.session.append_event", context):
                storage.session.append_event(
                    SessionEvent(
                        tenant_id=context.tenant_id,
                        session_id=context.session_id or "",
                        event_id=str(uuid4()),
                        event_type="assistant_message",
                        payload={"text": answer},
                        trace_id=context.trace_id,
                        idempotency_key=request.idempotency_key,
                    ),
                    fencing_token=self._fencing_token(lease),
                )
            lease = self._checkpoint_lease(storage, lease)
            with self.telemetry.span("storage.session.load_state", context):
                state = storage.session.load_state(context.tenant_id, context.session_id or "")
            state_value = dict(state.state)
            state_value["turn_count"] = state_value.get("turn_count", 0) + 1
            state_value["last_user_input"] = request.user_input.text
            state_value["last_answer"] = answer
            updated = False
            for _ in range(3):
                lease = self._checkpoint_lease(storage, lease)
                with self.telemetry.span("storage.session.compare_and_set", context):
                    updated_state = storage.session.compare_and_set_state(
                        context.tenant_id,
                        context.session_id or "",
                        state.state_version,
                        state_value,
                        fencing_token=self._fencing_token(lease),
                    )
                if updated_state:
                    updated = True
                    break
                with self.telemetry.span("storage.session.load_state", context):
                    state = storage.session.load_state(context.tenant_id, context.session_id or "")
                state_value = dict(state.state)
                state_value["turn_count"] = state_value.get("turn_count", 0) + 1
                state_value["last_user_input"] = request.user_input.text
                state_value["last_answer"] = answer
            if not updated:
                raise GatewayError("session state changed concurrently; retry request")

            memory_item = MemoryItem(
                tenant_id=context.tenant_id,
                memory_id=f"{context.session_id}:{seq}",
                scope_key=context.session_id or "",
                content=request.user_input.text,
                metadata={"trace_id": context.trace_id},
            )
            with self.telemetry.span("storage.memory.put", context):
                lease = self._checkpoint_lease(storage, lease)
                self._write_with_compensation(
                    storage,
                    "memory.put",
                    memory_item,
                    lambda: storage.memory.put(memory_item),
                    context,
                )
            with self.telemetry.span("storage.session.load_state", context):
                latest_seq = storage.session.load_state(context.tenant_id, context.session_id or "").latest_event_seq
            summary = Summary(
                tenant_id=context.tenant_id,
                session_id=context.session_id or "",
                content=f"Conversation has {latest_seq} events. Last answer: {answer}",
                source_event_seq=latest_seq,
            )
            with self.telemetry.span("storage.summary.put", context):
                lease = self._checkpoint_lease(storage, lease)
                self._write_with_compensation(
                    storage,
                    "summary.put",
                    summary,
                    lambda: storage.summary.put(summary),
                    context,
                )
            audit = AuditRecord(
                audit_id=str(uuid4()),
                tenant_id=context.tenant_id,
                channel=context.channel,
                user_id=context.user_id,
                session_id=context.session_id,
                agent_name=app.agent_name,
                decision="allow",
                latency_ms=int((monotonic() - started) * 1000),
                token_usage=model_response.total_tokens,
                cost=model_response.total_tokens * app.model_config.cost_per_1k_tokens / 1000,
                trace_id=context.trace_id,
            )
            with self.telemetry.span("storage.audit.append", context):
                lease = self._checkpoint_lease(storage, lease)
                self._write_with_compensation(
                    storage,
                    "audit.append",
                    audit,
                    lambda: storage.audit.append(audit),
                    context,
                )
            self.executions += 1
        return events

    def _write_with_compensation(self, storage, operation, value, writer, context) -> None:
        try:
            writer()
        except Exception:
            payload = asdict(value)
            for key, item in list(payload.items()):
                if hasattr(item, "isoformat"):
                    payload[key] = item.isoformat()
            task_id = ":".join(
                (
                    operation,
                    str(getattr(value, "tenant_id", context.tenant_id)),
                    str(getattr(value, "session_id", "")),
                    str(
                        getattr(
                            value,
                            "memory_id",
                            getattr(value, "audit_id", getattr(value, "source_event_seq", "")),
                        )
                    ),
                )
            )
            storage.compensation.enqueue(
                context.tenant_id,
                operation,
                payload,
                task_id=task_id,
            )
            observe_error(context.tenant_id, context.channel or "unknown", f"{operation}_deferred")

    def _run_tools(
        self,
        request,
        app,
        policy,
        storage,
        calls=None,
        lease: SessionLease | None = None,
    ) -> tuple[list[AgentEvent], str]:
        calls = list(calls if calls is not None else request.user_input.metadata.get("tool_calls", []))
        if not calls and request.user_input.text.startswith("/search "):
            calls = [{"name": "search_knowledge", "arguments": {"query": request.user_input.text[8:]}}]
        if not calls:
            return [], ""
        context_parts = []
        events = []
        governance = getattr(storage, "tool_governance", None)
        prior_events = storage.session.load_events(
            request.tenant_context.tenant_id,
            request.tenant_context.session_id or "",
            after_seq=0,
        )
        for index, call in enumerate(calls):
            name = str(call.get("name", ""))
            execution = None
            tool_key = ""
            raw_arguments = call.get("arguments", {})
            if not isinstance(raw_arguments, dict):
                raise GatewayError(f"tool {name or '<unknown>'} arguments must be an object")
            arguments = dict(raw_arguments)
            side_effect = bool(call.get("side_effect", name != "search_knowledge"))
            tool_key = str(call.get("_tool_key") or f"{request.idempotency_key}:tool:{index}")
            if governance is not None:
                try:
                    governance.reserve_call(
                        request.tenant_context.tenant_id,
                        request.idempotency_key,
                        tool_key,
                        side_effect,
                        app.tool_policy.max_calls_per_request,
                        app.tool_policy.max_side_effect_calls_per_request,
                    )
                except RuntimeError as exc:
                    raise PolicyDenied(str(exc)) from exc
            else:
                policy.check_tool_budget(
                    index + 1,
                    int(
                        sum(
                            bool(item.get("side_effect", str(item.get("name", "")) != "search_knowledge"))
                            for item in calls[: index + 1]
                        )
                    ),
                )
            tool_started = monotonic()
            try:
                requires_approval = policy.requires_tool_approval(name)
                approval = approval_id(
                    request.tenant_context.tenant_id,
                    request.tenant_context.session_id or "",
                    name,
                    sorted(arguments),
                )
                approval_state = None
                if governance is not None and requires_approval:
                    approval_state = governance.create_or_get(
                        request.tenant_context.tenant_id,
                        approval,
                        request.tenant_context.session_id or "",
                        request.idempotency_key,
                        name,
                        arguments_hash(arguments),
                    )
                approved = False
                if requires_approval:
                    approval_requested = (
                        approval_state is not None
                        and approval_state.status in {
                            ApprovalStatus.PENDING,
                            ApprovalStatus.APPROVED,
                            ApprovalStatus.CONSUMED,
                        }
                    ) or any(
                        event.event_type == "tool_approval_requested"
                        and event.payload.get("approval_id") == approval
                        for event in prior_events
                    )
                    approved = (
                        approval_requested
                        and verify_approval_token(
                            str(call.get("approval_token", "")),
                            str(call.get("approval_id", approval)),
                            request.tenant_context.tenant_id,
                        )
                        and str(call.get("approval_id", approval)) == approval
                    )
                    if approved and governance is not None:
                        try:
                            governance.consume(
                                request.tenant_context.tenant_id,
                                approval,
                                request.idempotency_key,
                                arguments_hash(arguments),
                            )
                        except (KeyError, RuntimeError) as exc:
                            approved = False
                            if approval_state is not None and approval_state.status == ApprovalStatus.AMBIGUOUS:
                                raise PolicyDenied("approval is ambiguous") from exc
                    if not approved:
                        lease = self._checkpoint_lease(storage, lease)
                        storage.session.append_event(
                            SessionEvent(
                                tenant_id=request.tenant_context.tenant_id,
                                session_id=request.tenant_context.session_id or "",
                                event_id=str(uuid4()),
                                event_type="tool_approval_requested",
                                payload={
                                    "approval_id": approval,
                                    "tool_name": name,
                                    "arguments_keys": sorted(arguments),
                                    "risk": policy.tool_risk(name),
                                    "expires_at": (
                                        approval_state.expires_at.isoformat()
                                        if approval_state is not None
                                        else None
                                    ),
                                },
                                trace_id=request.tenant_context.trace_id,
                                idempotency_key=tool_key,
                            ),
                            fencing_token=self._fencing_token(lease),
                        )
                        approval_record = AuditRecord(
                            audit_id=str(uuid4()),
                            tenant_id=request.tenant_context.tenant_id,
                            channel=request.tenant_context.channel,
                            user_id=request.tenant_context.user_id,
                            session_id=request.tenant_context.session_id,
                            agent_name=app.agent_name,
                            tool_name=name,
                            decision="approval_required",
                            latency_ms=int((monotonic() - tool_started) * 1000),
                            trace_id=request.tenant_context.trace_id,
                            metadata={"approval_id": approval, "arguments_keys": sorted(arguments)},
                        )
                        self._write_with_compensation(
                            storage,
                            "audit.append",
                            approval_record,
                            lambda: storage.audit.append(approval_record),
                            request.tenant_context,
                        )
                        events.append(
                            AgentEvent(
                                "approval_required",
                                f"Tool approval required: {name}",
                                {
                                    "title": "需要确认工具执行",
                                    "tool_name": name,
                                    "approval_id": approval,
                                    "approval_token": approval_token(approval, request.tenant_context.tenant_id),
                                    "buttons": [{"label": "确认执行", "approval_id": approval}],
                                },
                            )
                        )
                        continue
                policy.check_tool(name, approved)
                previous_result = next(
                    (
                        event
                        for event in prior_events
                        if event.event_type == "tool_call" and event.idempotency_key == tool_key
                    ),
                    None,
                )
                if previous_result is not None:
                    result = ToolResult(
                        name,
                        str(previous_result.payload.get("content", "")),
                        dict(previous_result.payload.get("metadata", {})),
                    )
                    if result.content:
                        context_parts.append(result.content)
                    events.append(
                        AgentEvent(
                            "tool_call",
                            result.content,
                            {
                                "tool_name": result.name,
                                "metadata": result.metadata,
                                "replayed": True,
                                "call_id": call.get("call_id", ""),
                            },
                        )
                    )
                    continue
                prior_intent = next(
                    (
                        event
                        for event in prior_events
                        if event.event_type == "tool_intent" and event.idempotency_key == tool_key
                    ),
                    None,
                )
                if (
                    prior_intent is not None
                    and bool(prior_intent.payload.get("side_effect", False))
                    and not bool(call.get("idempotent", False))
                ):
                    raise GatewayError(f"tool {name} requires idempotency confirmation after worker recovery")
                if governance is not None and hasattr(governance, "begin_execution"):
                    execution_id = str(uuid4())
                    execution = governance.begin_execution(
                        request.tenant_context.tenant_id,
                        execution_id,
                        request.idempotency_key,
                        request.tenant_context.session_id or "",
                        name,
                        tool_key,
                        arguments_hash(arguments),
                        side_effect,
                        self._fencing_token(lease),
                    )
                    if execution.status == ToolExecutionStatus.AMBIGUOUS:
                        raise PolicyDenied("tool execution identity is ambiguous")
                    if execution.status == ToolExecutionStatus.SUCCEEDED:
                        stored = execution.result or {}
                        result = ToolResult(
                            name,
                            str(stored.get("content", "")),
                            dict(stored.get("metadata", {})),
                        )
                        context_parts.append(result.content)
                        events.append(
                            AgentEvent(
                                "tool_call",
                                result.content,
                                {
                                    "tool_name": result.name,
                                    "metadata": result.metadata,
                                    "replayed": True,
                                    "ledger_replay": True,
                                    "call_id": call.get("call_id", ""),
                                },
                            )
                        )
                        continue
                    if execution.status == ToolExecutionStatus.RUNNING and execution.attempt > 1:
                        raise GatewayError(
                            f"tool {name} execution is already running after recovery; "
                            "manual reconciliation is required"
                        )
                    if (
                        execution.status == ToolExecutionStatus.RUNNING
                        and execution.execution_id != execution_id
                    ):
                        raise GatewayError(
                            f"tool {name} execution is already owned by another worker"
                        )
                lease = self._checkpoint_lease(storage, lease)
                storage.session.append_event(
                    SessionEvent(
                        tenant_id=request.tenant_context.tenant_id,
                        session_id=request.tenant_context.session_id or "",
                        event_id=str(uuid4()),
                        event_type="tool_intent",
                        payload={
                            "tool_name": name,
                            "arguments_keys": sorted(arguments),
                            "side_effect": side_effect,
                            "risk": policy.tool_risk(name),
                        },
                        trace_id=request.tenant_context.trace_id,
                        idempotency_key=tool_key,
                    ),
                    fencing_token=self._fencing_token(lease),
                )
                with self.telemetry.span("tool.call", request.tenant_context):
                    if name == "search_knowledge":
                        arguments.setdefault("query", request.user_input.text)
                        result = self.tools.call(
                            name,
                            storage=storage,
                            tenant_id=request.tenant_context.tenant_id,
                            query=str(arguments["query"]),
                        )
                    else:
                        if self.tools.has_local_tool(name):
                            result = self.tools.call(
                                name,
                                tenant_id=request.tenant_context.tenant_id,
                                **arguments,
                                request_id=request.tenant_context.trace_id,
                                idempotency_key=tool_key,
                            )
                        else:
                            result = self.tools.call(
                                name,
                                tenant_id=request.tenant_context.tenant_id,
                                arguments=arguments,
                                request_id=request.tenant_context.trace_id,
                                idempotency_key=tool_key,
                            )
                if result.content:
                    context_parts.append(result.content)
                if governance is not None and execution is not None:
                    governance.complete_execution(
                        request.tenant_context.tenant_id,
                        tool_key,
                        {"content": result.content, "metadata": result.metadata},
                        self._fencing_token(lease),
                    )
                events.append(
                    AgentEvent(
                        "tool_call",
                        result.content,
                        {
                            "tool_name": result.name,
                            "metadata": result.metadata,
                            "call_id": call.get("call_id", ""),
                        },
                    )
                )
                lease = self._checkpoint_lease(storage, lease)
                storage.session.append_event(
                    SessionEvent(
                        tenant_id=request.tenant_context.tenant_id,
                        session_id=request.tenant_context.session_id or "",
                        event_id=str(uuid4()),
                        event_type="tool_call",
                        payload={
                            "tool_name": result.name,
                            "content": result.content,
                            "metadata": result.metadata,
                            "arguments_keys": sorted(arguments),
                        },
                        trace_id=request.tenant_context.trace_id,
                        idempotency_key=tool_key,
                    ),
                    fencing_token=self._fencing_token(lease),
                )
                observe_tool(request.tenant_context.tenant_id, name, "ok")
                tool_latency = monotonic() - tool_started
                observe_tool_latency(request.tenant_context.tenant_id, name, tool_latency)
                lease = self._checkpoint_lease(storage, lease)
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=request.tenant_context.tenant_id,
                        channel=request.tenant_context.channel,
                        user_id=request.tenant_context.user_id,
                        session_id=request.tenant_context.session_id,
                        agent_name=app.agent_name,
                        tool_name=name,
                        decision="allow",
                        latency_ms=int(tool_latency * 1000),
                        trace_id=request.tenant_context.trace_id,
                        metadata={"arguments_keys": sorted(arguments)},
                    )
                )
            except Exception as exc:
                if governance is not None and execution is not None:
                    try:
                        governance.fail_execution(
                            request.tenant_context.tenant_id,
                            tool_key,
                            type(exc).__name__,
                            str(exc),
                            self._fencing_token(lease),
                        )
                    except Exception:
                        pass
                observe_tool(request.tenant_context.tenant_id, name, "error")
                observe_error(
                    request.tenant_context.tenant_id,
                    request.tenant_context.channel or "unknown",
                    type(exc).__name__,
                )
                tool_latency = monotonic() - tool_started
                observe_tool_latency(request.tenant_context.tenant_id, name, tool_latency)
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=request.tenant_context.tenant_id,
                        channel=request.tenant_context.channel,
                        user_id=request.tenant_context.user_id,
                        session_id=request.tenant_context.session_id,
                        agent_name=app.agent_name,
                        tool_name=name,
                        decision="deny" if isinstance(exc, PolicyDenied) else "error",
                        latency_ms=int(tool_latency * 1000),
                        error_type="policy_denied" if isinstance(exc, PolicyDenied) else "tool_execution_failed",
                        trace_id=request.tenant_context.trace_id,
                    )
                )
                raise
        return events, "\n".join(context_parts)

    @staticmethod
    def _fencing_token(lease: SessionLease | None) -> int | None:
        if lease is None or not lease.fencing_token:
            return None
        return int(lease.fencing_token)

    @staticmethod
    def _checkpoint_lease(storage, lease: SessionLease | None) -> SessionLease | None:
        lease = renew_session_lease(storage.session, lease)
        validate_session_lease(storage.session, lease)
        return lease

    @staticmethod
    def _search_knowledge(storage, tenant_id: str, query: str) -> ToolResult:
        collection = getattr(storage, "knowledge_collection", "default")
        chunks = storage.knowledge.search(tenant_id, collection, query, limit=3)
        content = "\n".join(chunk.text for chunk in chunks)
        return ToolResult("search_knowledge", content, {"count": len(chunks)})

    def _generate_answer(
        self,
        text: str,
        app,
        prior: list[SessionEvent],
        tool_context: str = "",
        summary_context: str = "",
        memory_context: str = "",
        redact=None,
        tools: list[dict] | None = None,
    ) -> ModelResponse:
        if not text.strip():
            return "请输入需要处理的内容。"
        if text.lower().startswith("/help"):
            return "支持多租户会话、历史记忆、幂等处理和审计记录。"
        client = self.model_client or ResponsesModelClient.from_config(
            app.model_config,
            self.secrets,
        )
        if client is None:
            context_hint = f"（已恢复 {len(prior)} 条会话事件）" if prior else ""
            return "当前未配置真实模型，处于本地演示模式。\n\n" f"已收到：{text}{context_hint}"
        conversation = self._build_conversation(
            text,
            prior,
            tool_context,
            summary_context,
            memory_context,
            redact,
        )
        return self._generate_from_conversation(app, conversation, tools)

    def _build_conversation(
        self,
        text: str,
        prior: list[SessionEvent],
        tool_context: str,
        summary_context: str,
        memory_context: str,
        redact,
    ) -> list[dict]:
        conversation: list[dict] = []
        redact = redact or (lambda value: value)
        for event in prior[-20:]:
            if event.event_type == "user_message":
                conversation.append({"role": "user", "content": redact(str(event.payload.get("text", "")))})
            elif event.event_type == "assistant_message":
                conversation.append({"role": "assistant", "content": redact(str(event.payload.get("text", "")))})
        if summary_context:
            conversation.append({"role": "system", "content": f"Session summary:\n{summary_context}"})
        if memory_context:
            conversation.append({"role": "system", "content": f"Relevant tenant memory:\n{memory_context}"})
        if tool_context:
            conversation.append({"role": "system", "content": f"Knowledge context:\n{tool_context}"})
        conversation.append({"role": "user", "content": redact(text)})
        return conversation

    def _generate_from_conversation(self, app, conversation: list[dict], tools: list[dict] | None) -> ModelResponse:
        client = self.model_client or ResponsesModelClient.from_config(
            app.model_config,
            self.secrets,
        )
        if client is None:
            text = str(conversation[-1].get("content", "")) if conversation else ""
            return ModelResponse(
                f"Local demo mode received: {text}",
                len(text.split()),
                5,
                len(text.split()) + 5,
            )
        return client.generate_with_usage(
            model=app.model_config.model,
            system_prompt=app.prompt,
            conversation=conversation,
            temperature=app.model_config.temperature,
            max_output_tokens=app.model_config.max_output_tokens,
            tools=tools,
        )

    def _raise_model_tool_error(self, storage, request, app, error_type: str, message: str) -> None:
        storage.audit.append(
            AuditRecord(
                audit_id=str(uuid4()),
                tenant_id=request.tenant_context.tenant_id,
                channel=request.tenant_context.channel,
                user_id=request.tenant_context.user_id,
                session_id=request.tenant_context.session_id,
                agent_name=app.agent_name,
                decision="error",
                error_type=error_type,
                trace_id=request.tenant_context.trace_id,
                metadata={"reason": message},
            )
        )
        raise GatewayError(message)


class AgentGateway:
    def __init__(
        self,
        tenants: TenantService,
        storage: StorageBundle,
        workers: list[AgentWorker] | None = None,
        telemetry: TraceRecorder | None = None,
        storage_manager: TenantStorageManager | None = None,
        worker_queue: WorkerQueue | None = None,
        quota: QuotaEnforcer | None = None,
    ) -> None:
        self.tenants = tenants
        self.storage = storage
        self.storage_manager = storage_manager
        self.telemetry = telemetry or TraceRecorder()
        self.workers = workers or [AgentWorker(storage, self.telemetry)]
        self._worker_index = 0
        self.worker_queue = worker_queue
        self.quota = quota or QuotaEnforcer()

    @staticmethod
    def _durable_inbox_enabled() -> bool:
        default = "0" if os.getenv("TRPC_AGENT_RUNTIME_MODE", "trpc").strip().lower() == "local" else "1"
        return os.getenv("DURABLE_INBOX_OUTBOX", default).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _inbox_payload(message: InboundMessage) -> dict:
        return {
            "channel": message.channel,
            "account_id": message.account_id,
            "external_message_id": message.external_message_id,
            "external_user_id": message.external_user_id,
            "text": message.text,
            "group_id": message.group_id,
            "attachments": [asdict(item) for item in message.attachments],
            "received_at": message.received_at.isoformat(),
            "raw_event": message.raw_event,
            "internal_user_id": message.internal_user_id,
        }

    def dispatch(
        self,
        message: InboundMessage,
        trace_id: str | None = None,
        traceparent: str | None = None,
    ) -> tuple[str, list[AgentEvent], str]:
        trace_id = trace_id or str(uuid4())
        binding = self.tenants.resolve_binding(message.channel, message.account_id)
        message.internal_user_id = binding.resolve_user_id(message.external_user_id)
        config = self.tenants.get_tenant(binding.tenant_id)
        if config.status.value != "active":
            raise GatewayError(f"tenant is not active: {config.tenant_id}")
        route_session_id = session_id_for_message(config.tenant_id, binding.agent_app_id, message)
        config = self.tenants.resolve_runtime_config(binding.tenant_id, route_session_id)
        binding = config.channel_binding(message.channel, message.account_id)
        storage = self.storage_manager.get(config) if self.storage_manager is not None else self.storage
        session_id = session_id_for_message(config.tenant_id, binding.agent_app_id, message)
        idempotency_key = build_idempotency_key(
            config.tenant_id,
            message.channel,
            message.account_id,
            message.external_message_id,
        )
        durable = getattr(storage, "inbox_outbox", None) if self._durable_inbox_enabled() else None
        mailbox = getattr(storage, "mailbox", None) if durable is not None else None
        mailbox_v2 = getattr(storage, "session_mailbox_v2", None) if durable is not None else None
        if (
            mailbox_v2 is not None
            and os.getenv("SESSION_MAILBOX_V2_ENABLED", "1").strip().lower()
            in {"1", "true", "yes", "on"}
        ):
            mailbox = _SessionMailboxV2Adapter(mailbox_v2)
        if self._durable_inbox_enabled() and durable is None:
            raise GatewayError("durable Inbox/Outbox is enabled but the storage backend does not support it")
        if durable is not None and mailbox is None:
            raise GatewayError("durable Inbox/Outbox requires an ordered mailbox")
        inbox_owner = f"gateway:{uuid4()}"
        inbox_claimed = False
        mailbox_record = None
        if durable is not None:
            inbox_record, inbox_claimed = durable.accept_inbox(
                config.tenant_id,
                idempotency_key,
                session_id,
                self._inbox_payload(message),
                inbox_owner,
                lease_seconds=int(os.getenv("INBOX_LEASE_SECONDS", "180")),
            )
            if not inbox_claimed:
                if inbox_record.status == "completed" and inbox_record.result:
                    result = inbox_record.result
                    return (
                        session_id,
                        [AgentEvent(**item) for item in result.get("events", [])]
                        or [AgentEvent("message_end", str(result.get("text", "")))],
                        str(result.get("response_ref", "")),
                    )
                if inbox_record.status == "processing":
                    raise GatewayError("message is already being processed")
                if inbox_record.status == "dead":
                    raise GatewayError("message is dead-lettered and requires operator review")
            mailbox.enqueue(
                config.tenant_id,
                session_id,
                inbox_record.message_id,
                idempotency_key,
                self._inbox_payload(message),
            )
            if (
                hasattr(mailbox, "has_unresolved_message")
                and not mailbox.has_unresolved_message(
                    config.tenant_id, session_id, inbox_record.message_id
                )
            ):
                durable.dead_inbox(
                    config.tenant_id,
                    idempotency_key,
                    inbox_owner,
                    "session mailbox message is already terminal",
                )
                inbox_claimed = False
                raise GatewayError("message is already terminal in the session mailbox")
            if os.getenv("SESSION_READY_ASYNC", "0").strip().lower() in {"1", "true", "yes", "on"}:
                # The independent v2 consumer will claim and execute this
                # mailbox item after the ready outbox notice is published.
                # Reserve the response idempotency record before handing the
                # Inbox lease to the worker.  The async worker completes this
                # record after execution; without the reservation its fenced
                # completion would fail with a missing-record KeyError.
                storage.idempotency.start(
                    config.tenant_id,
                    idempotency_key,
                    trace_id,
                    lease_seconds=int(os.getenv("IDEMPOTENCY_LEASE_SECONDS", "180")),
                )
                release = getattr(durable, "release_inbox", None)
                if release is None:
                    raise GatewayError("async session-ready requires inbox ownership handoff support")
                release(config.tenant_id, idempotency_key, inbox_owner)
                inbox_claimed = False
                response_ref = f"{message.channel}:{message.external_message_id}:queued"
                return (
                    session_id,
                    [AgentEvent("message_end", "", {"queued": True, "durable": True})],
                    response_ref,
                )
            order_deadline = monotonic() + max(
                0.0,
                float(os.getenv("MAILBOX_ORDER_WAIT_SECONDS", "30")),
            )
            while mailbox_record is None:
                mailbox_record = mailbox.claim_next(
                    config.tenant_id,
                    session_id,
                    inbox_owner,
                    lease_seconds=int(os.getenv("MAILBOX_LEASE_SECONDS", "180")),
                )
                if mailbox_record is not None or monotonic() >= order_deadline:
                    break
                sleep(0.05)
            if mailbox_record is None:
                durable.fail_inbox(
                    config.tenant_id,
                    idempotency_key,
                    inbox_owner,
                    "mailbox order wait timeout",
                )
                inbox_claimed = False
                raise GatewayError("message is queued behind an active session mailbox lease")
        with self.telemetry.span(
            "gateway.route",
            TenantContext(
                tenant_id=config.tenant_id,
                agent_app_id=binding.agent_app_id,
                config_version=config.config_version,
                trace_id=trace_id,
                session_id=session_id,
                channel=message.channel,
                user_id=message.effective_user_id,
                traceparent=traceparent,
            ),
        ):
            record = storage.idempotency.start(
                config.tenant_id,
                idempotency_key,
                trace_id,
                lease_seconds=int(os.getenv("IDEMPOTENCY_LEASE_SECONDS", "180")),
            )
            if record.status.value == "completed":
                result = record.result or {}
                if durable is not None and inbox_claimed:
                    durable.complete_inbox(
                        config.tenant_id,
                        idempotency_key,
                        inbox_owner,
                        {
                            "text": str(result.get("text", "")),
                            "response_ref": record.response_ref or "",
                            "events": [{"event_type": "message_end", "content": str(result.get("text", ""))}],
                        },
                    )
                    inbox_claimed = False
                if mailbox_record is not None:
                    mailbox.complete(mailbox_record)
                    mailbox_record = None
                return session_id, [AgentEvent("message_end", str(result.get("text", "")))], record.response_ref or ""
            if record.status.value == "processing" and record.trace_id != trace_id:
                raise GatewayError("message is already being processed")

            if message.is_revoke:
                revoked_event = SessionEvent(
                    tenant_id=config.tenant_id,
                    session_id=session_id,
                    event_id=str(uuid4()),
                    event_type="message_revoked",
                    payload={
                        "target_message_id": message.raw_event.get("target_message_id") or message.external_message_id,
                        "external_message_id": message.external_message_id,
                    },
                    trace_id=trace_id,
                    idempotency_key=idempotency_key,
                )
                with session_lease(storage.session, config.tenant_id, session_id) as lease:
                    storage.session.append_event(
                        revoked_event,
                        fencing_token=AgentWorker._fencing_token(lease) if isinstance(lease, SessionLease) else None,
                    )
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=config.tenant_id,
                        channel=message.channel,
                        user_id=message.effective_user_id,
                        session_id=session_id,
                        agent_name=config.app(binding.agent_app_id).agent_name,
                        decision="revoked",
                        trace_id=trace_id,
                        metadata={"target_message_id": revoked_event.payload["target_message_id"]},
                    )
                )
                response_ref = f"{message.channel}:{message.external_message_id}:revoked"
                storage.idempotency.complete(
                    config.tenant_id,
                    idempotency_key,
                    response_ref,
                    {"text": "", "revoked": True, "trace_id": trace_id},
                )
                if durable is not None and inbox_claimed:
                    durable.complete_inbox(
                        config.tenant_id,
                        idempotency_key,
                        inbox_owner,
                        {
                            "text": "",
                            "response_ref": response_ref,
                            "events": [{"event_type": "message_revoked", "content": "", "metadata": {"revoked": True}}],
                        },
                    )
                    inbox_claimed = False
                if mailbox_record is not None:
                    mailbox.complete(mailbox_record)
                    mailbox_record = None
                observe_request(config.tenant_id, message.channel, "revoked")
                return session_id, [AgentEvent("message_revoked", "", {"revoked": True})], response_ref

            # Use tiktoken for accurate token estimation (supports Chinese and all languages)
            # Fallback to word split if tiktoken is not available
            try:
                import tiktoken
                encoding = tiktoken.get_encoding("cl100k_base")  # GPT-3.5/4 encoding
                estimated_input_tokens = len(encoding.encode(message.text or ""))
            except (ImportError, Exception):
                # Rough estimate: 1 Chinese char ≈ 2 tokens, 1 English word ≈ 1.3 tokens
                text = message.text or ""
                chinese_chars = sum(1 for c in text if '一' <= c <= '鿿')
                other_chars = len(text) - chinese_chars
                estimated_input_tokens = chinese_chars * 2 + other_chars // 4

            # Reserve tokens for max_output to prevent budget overflow
            model_config = config.app(binding.agent_app_id).model_config
            max_output_tokens = model_config.max_output_tokens
            estimated_total_tokens = estimated_input_tokens + max_output_tokens
            estimated_cost = (
                estimated_total_tokens * model_config.cost_per_1k_tokens / 1000
            )
            try:
                self.quota.check(
                    config.tenant_id,
                    config.quota_policy,
                    requested_tokens=estimated_input_tokens,
                    requested_cost=estimated_cost,
                )
            except QuotaExceeded:
                mailbox_terminal = False
                if mailbox_record is not None:
                    mailbox_result = mailbox.fail(mailbox_record, "quota_exceeded")
                    mailbox_terminal = _mailbox_failure_is_terminal(
                        mailbox_result, mailbox_record
                    )
                    mailbox_record = None
                if durable is not None and inbox_claimed:
                    update_inbox = (
                        durable.dead_inbox
                        if mailbox_terminal and hasattr(durable, "dead_inbox")
                        else durable.fail_inbox
                    )
                    update_inbox(
                        config.tenant_id,
                        idempotency_key,
                        inbox_owner,
                        "quota_exceeded",
                    )
                    inbox_claimed = False
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=config.tenant_id,
                        channel=message.channel,
                        user_id=message.effective_user_id,
                        session_id=session_id,
                        agent_name=config.app(binding.agent_app_id).agent_name,
                        decision="deny",
                        error_type="quota_exceeded",
                        trace_id=trace_id,
                    )
                )
                observe_request(config.tenant_id, message.channel, "quota_denied")
                raise

            context = TenantContext(
                tenant_id=config.tenant_id,
                agent_app_id=binding.agent_app_id,
                config_version=config.config_version,
                trace_id=trace_id,
                session_id=session_id,
                channel=message.channel,
                user_id=message.effective_user_id,
            )
            context = replace(
                context,
                traceparent=self.telemetry.inject_traceparent() or traceparent,
            )
            request = RunRequest(
                tenant_context=context,
                user_input=UserInput(
                    text=message.text or "",
                    metadata={
                        "external_message_id": message.external_message_id,
                        "group_id": message.group_id,
                        "attachments": [asdict(item) for item in message.attachments],
                        "tool_calls": message.raw_event.get("tool_calls", []),
                    },
                ),
                idempotency_key=idempotency_key,
            )
            try:
                if self.worker_queue is not None:
                    event_payloads = self.worker_queue.submit(request, config)
                    events = [AgentEvent(**item) for item in event_payloads]
                else:
                    worker = self._next_worker()
                    events = worker.run(request, config, storage)
                text = next(
                    (event.content for event in reversed(events) if event.event_type == "message_end"),
                    "",
                )
                response_ref = f"{message.channel}:{message.external_message_id}:response"
                durable_result = {
                    "text": text,
                    "response_ref": response_ref,
                    "events": [asdict(event) for event in events],
                    "trace_id": trace_id,
                    "channel": message.channel,
                    "account_id": message.account_id,
                    "external_user_id": message.external_user_id,
                    "group_id": message.group_id,
                    "idempotency_key": idempotency_key,
                    "agent_name": config.app(binding.agent_app_id).agent_name,
                }
                if mailbox_record is not None:
                    mailbox.complete(mailbox_record)
                    mailbox_record = None
                if durable is not None and inbox_claimed:
                    complete_and_enqueue = getattr(durable, "complete_inbox_and_enqueue_outbox", None)
                    if complete_and_enqueue is not None:
                        complete_and_enqueue(
                            config.tenant_id,
                            idempotency_key,
                            inbox_owner,
                            durable_result,
                            "agent.response",
                            session_id,
                            f"{idempotency_key}:agent-response",
                        )
                    else:
                        durable.enqueue_outbox(
                            config.tenant_id,
                            "agent.response",
                            session_id,
                            durable_result,
                            event_id=f"{idempotency_key}:agent-response",
                        )
                        durable.complete_inbox(
                            config.tenant_id,
                            idempotency_key,
                            inbox_owner,
                            durable_result,
                        )
                    inbox_claimed = False
                storage.idempotency.complete(
                    config.tenant_id,
                    idempotency_key,
                    response_ref,
                    {"text": text, "trace_id": trace_id},
                )
                message_event = next(
                    (event for event in reversed(events) if event.event_type == "message_end"),
                    None,
                )
                token_usage = int(
                    (message_event.metadata if message_event else {}).get(
                        "total_tokens",
                        len((message.text or "").split()) + len(text.split()),
                    )
                )
                actual_cost = token_usage * (config.app(binding.agent_app_id).model_config.cost_per_1k_tokens / 1000)
                self.quota.record(
                    config.tenant_id,
                    token_usage,
                    actual_cost,
                    reserved_tokens=estimated_input_tokens,
                    reserved_cost=estimated_cost,
                )
                observe_cost(
                    config.tenant_id,
                    actual_cost,
                )
                observe_tokens(config.tenant_id, token_usage)
                observe_request(config.tenant_id, message.channel, "success")
                return session_id, events, response_ref
            except Exception as exc:
                mailbox_terminal = False
                if mailbox_record is not None:
                    try:
                        mailbox_result = mailbox.fail(
                            mailbox_record, type(exc).__name__
                        )
                        mailbox_terminal = _mailbox_failure_is_terminal(
                            mailbox_result, mailbox_record
                        )
                    except Exception:
                        pass
                if durable is not None and inbox_claimed:
                    update_inbox = (
                        durable.dead_inbox
                        if mailbox_terminal and hasattr(durable, "dead_inbox")
                        else durable.fail_inbox
                    )
                    update_inbox(
                        config.tenant_id,
                        idempotency_key,
                        inbox_owner,
                        type(exc).__name__,
                    )
                    inbox_claimed = False
                storage.idempotency.fail(config.tenant_id, idempotency_key, type(exc).__name__)
                self.quota.release(
                    config.tenant_id,
                    estimated_input_tokens,
                    estimated_cost,
                )
                observe_request(config.tenant_id, message.channel, "error")
                observe_error(config.tenant_id, message.channel, type(exc).__name__)
                raise

    def _next_worker(self) -> AgentWorker:
        worker = self.workers[self._worker_index % len(self.workers)]
        self._worker_index += 1
        return worker
