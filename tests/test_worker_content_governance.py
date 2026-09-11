"""Stage 6B2 Task 1 Steps 4-5: content governance + execution audit wiring.

Proves, with scripted agent streams and capturing repository doubles:

- the input gate runs AFTER the receipt claim and BEFORE any agent/model
  call (zero run() invocations), failing the receipt with the fixed
  ``content_input_blocked`` code; the same message replays that terminal;
- output enforcement buffers deltas inside the Worker and emits AT MOST one
  safe delta plus ``done``; the sensitive original never appears in any
  public event, receipt body or (sync) return value;
- sync chat() and SSE stream() take the same decision path (fixed texts and
  events identical);
- audit facts ride the SAME transaction as the receipt terminal:
  content_decision / agent_result / tool_decision via complete()/fail(),
  review_pending via the pause transaction, and the approval terminal via
  finalize;
- Gateway ingress records delivery_result (receipt_id NULL) on delivery
  terminals and a missing/unreachable audit backend cannot alter replies.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from trpc_agent_sdk.events import Event, LongRunningEvent
from trpc_agent_sdk.types import Content, FunctionCall, FunctionResponse, Part

from tests.tenant_helpers import (
    FakeTenantConfigRepository,
    make_app_config,
    make_governance,
    make_tenant_config,
)
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.channels.models import InboundMessage
from trpc_service.gateway.channel_service import ChannelIngressService
from trpc_service.gateway.client import WorkerClientError
from trpc_service.gateway.errors import map_worker_error
from trpc_service.governance.approval import (
    ApprovalAction,
    ApprovalClaim,
    ApprovalRequest,
)
from trpc_service.governance.content_policy import (
    CONTENT_INPUT_BLOCKED_TEXT,
    CONTENT_OUTPUT_BLOCKED_TEXT,
    ContentPolicyConfig,
)
from trpc_service.storage.message_repository import MessageClaim, ReceiptAction
from trpc_service.transport.models import (
    WorkerApprovalResult,
    WorkerApprovalTask,
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)
from trpc_service.worker.approval_service import ToolApprovalService
from trpc_service.worker.service import WorkerService

SENTINEL = "SENTINEL-PRIVATE-TOKEN-9f3a"
CREDENTIAL_INPUT = f"here is my bearer {SENTINEL}{'x' * 24} please use it"
CREDENTIAL_OUTPUT = f"the secret is sk-{'A' * 20}{'Z' * 8}"

TENANT = "tenant_default"


def _task(message: str = "hello", **overrides) -> WorkerTask:
    defaults = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
        "tenant_id": TENANT,
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web_console",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": message,
    }
    defaults.update(overrides)
    return WorkerTask(**defaults)


def _text_event(text: str, event_id: str | None = None) -> MagicMock:
    event = MagicMock(spec=Event)
    event.error_code = None
    event.content = Content(role="model", parts=[Part.from_text(text=text)])
    event.partial = False
    event.id = event_id or f"evt-{uuid.uuid4().hex[:8]}"
    return event


def _tool_call_event(name: str) -> MagicMock:
    event = MagicMock(spec=Event)
    event.error_code = None
    event.content = Content(role="model", parts=[Part(function_call=FunctionCall(name=name, args={}))])
    event.partial = False
    event.id = f"evt-{uuid.uuid4().hex[:8]}"
    return event


def _tool_result_event(name: str) -> MagicMock:
    event = MagicMock(spec=Event)
    event.error_code = None
    event.content = Content(role="model",
                            parts=[Part(function_response=FunctionResponse(name=name, response={"ok": True}))])
    event.partial = False
    event.id = f"evt-{uuid.uuid4().hex[:8]}"
    return event


def _long_running_event(tool_name: str) -> MagicMock:
    event = MagicMock(spec=LongRunningEvent)
    event.error_code = None
    event.content = None
    event.partial = False
    event.id = f"evt-{uuid.uuid4().hex[:8]}"
    event.function_call = FunctionCall(id="call-1", name=tool_name, args={})
    event.function_response = FunctionResponse(name=tool_name, response={"status": "approval_required"})
    return event


class _ScriptAgent:

    def __init__(self, events) -> None:
        self._events = list(events)
        self.run_calls = 0

    async def run(self, **kwargs):
        self.run_calls += 1
        for event in self._events:
            yield event

    async def resume(self, **kwargs):
        self.run_calls += 1
        for event in self._events:
            yield event

    async def close(self) -> None:
        return None


class _CaptureReceiptRepo:

    def __init__(self, claim_result: MessageClaim | None = None) -> None:
        self.receipt_id = uuid.uuid4()
        self.claim_result = claim_result or MessageClaim(
            action=ReceiptAction.EXECUTE,
            receipt_id=self.receipt_id,
            response_text=None,
            error_code=None,
        )
        self.completed: list[tuple] = []
        self.failed: list[tuple] = []

    async def claim(self, task, message_text):
        return self.claim_result

    async def complete(self, receipt_id, response_text, latency_ms, execution_events=()):
        self.completed.append((receipt_id, response_text, latency_ms, tuple(execution_events)))

    async def fail(self, receipt_id, error_code, latency_ms, execution_events=()):
        self.failed.append((receipt_id, error_code, latency_ms, tuple(execution_events)))


class _CaptureApprovalRepo:

    def __init__(self) -> None:
        self.pauses: list[dict] = []

    async def pause_first_request(self, **kwargs):
        self.pauses.append(kwargs)
        return None


def _configs(policy: ContentPolicyConfig | None = None, tool_decisions: dict | None = None):
    governance = make_governance(tool_decisions=tool_decisions or {}, content_policy=policy or ContentPolicyConfig())
    cfg = make_tenant_config(TENANT, app=make_app_config(), governance=governance)
    return {TENANT: cfg}


def _service(
    events,
    receipts: _CaptureReceiptRepo | None = None,
    approvals: _CaptureApprovalRepo | None = None,
    policy: ContentPolicyConfig | None = None,
    tool_decisions: dict | None = None,
) -> tuple[WorkerService, _ScriptAgent]:
    agent = _ScriptAgent(events)
    service = WorkerService(
        tenant_repository=FakeTenantConfigRepository(_configs(policy, tool_decisions)),
        agent_app=agent,
        receipt_repository=receipts,
        approval_repository=approvals,
    )
    return service, agent


def _events_of(call_tuple) -> tuple[ExecutionAuditEvent, ...]:
    return call_tuple[3]


# ---------------------------------------------------------------------------
# input governance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_input_blocked_zero_agent_calls_and_atomic_fail():
    receipts = _CaptureReceiptRepo()
    service, agent = _service([_text_event("never")], receipts)
    result = await service.chat(_task(CREDENTIAL_INPUT))
    assert result.error_code == WorkerErrorCode.CONTENT_INPUT_BLOCKED
    assert agent.run_calls == 0
    assert receipts.completed == []
    rid, code, _latency, events = receipts.failed[0]
    assert code == WorkerErrorCode.CONTENT_INPUT_BLOCKED
    assert [(e.event_type, e.outcome, e.category) for e in events] == [("content_decision", "blocked", "credential")]
    assert events[0].receipt_id == rid
    assert events[0].tenant_id == TENANT
    assert events[0].trace_id is None  # no active span in unit tests


@pytest.mark.asyncio
async def test_input_blocked_stream_emits_error_terminal():
    receipts = _CaptureReceiptRepo()
    service, agent = _service([_text_event("never")], receipts)
    events = [e async for e in service.stream(_task(CREDENTIAL_INPUT))]
    assert agent.run_calls == 0
    assert [e.type for e in events] == ["error"]
    assert events[0].error_code == WorkerErrorCode.CONTENT_INPUT_BLOCKED
    assert len(receipts.failed) == 1


@pytest.mark.asyncio
async def test_input_action_allow_records_verdict_and_executes():
    receipts = _CaptureReceiptRepo()
    policy = ContentPolicyConfig(input_action="allow")
    service, agent = _service([_text_event("answered")], receipts, policy=policy)
    result = await service.chat(_task(CREDENTIAL_INPUT))
    assert result.error_code is None
    assert agent.run_calls == 1
    events = _events_of(receipts.completed[0])
    assert (events[0].event_type, events[0].outcome, events[0].category) == ("content_decision", "allow", "credential")


@pytest.mark.asyncio
async def test_disabled_policy_emits_no_content_events():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event("ok")], receipts, policy=ContentPolicyConfig(enabled=False))
    await service.chat(_task(CREDENTIAL_INPUT))
    events = _events_of(receipts.completed[0])
    assert [e.event_type for e in events] == ["agent_result"]


@pytest.mark.asyncio
async def test_replay_of_blocked_message_is_stable_and_fixed_text():
    task = _task(CREDENTIAL_INPUT)
    first = _CaptureReceiptRepo()
    service, _agent = _service([], first)
    result1 = await service.chat(task)
    assert result1.error_code == WorkerErrorCode.CONTENT_INPUT_BLOCKED
    # a redelivery replays the stored terminal
    replay = _CaptureReceiptRepo(
        MessageClaim(
            action=ReceiptAction.REPLAY,
            receipt_id=first.receipt_id,
            response_text=None,
            error_code=WorkerErrorCode.CONTENT_INPUT_BLOCKED,
        ))
    service2, agent2 = _service([], replay)
    result2 = await service2.chat(_task(CREDENTIAL_INPUT, message_id=task.message_id))
    assert result2.error_code == result1.error_code
    assert agent2.run_calls == 0
    assert map_worker_error(result2.error_code) == CONTENT_INPUT_BLOCKED_TEXT
    assert map_worker_error(result1.error_code) == map_worker_error(result2.error_code)


@pytest.mark.asyncio
async def test_input_gate_applies_without_receipt_repository():
    service, agent = _service([_text_event("never")])
    result = await service.chat(_task(CREDENTIAL_INPUT))
    assert result.error_code == WorkerErrorCode.CONTENT_INPUT_BLOCKED
    assert agent.run_calls == 0


# ---------------------------------------------------------------------------
# output governance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_output_blocked_single_safe_delta():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event(CREDENTIAL_OUTPUT)], receipts)
    events = [e async for e in service.stream(_task())]
    assert [e.type for e in events] == ["delta", "done"]
    assert events[0].data == CONTENT_OUTPUT_BLOCKED_TEXT
    assert SENTINEL not in str([e.data for e in events])
    assert "sk-" not in (events[0].data or "")
    rid, text, _latency, audit = _events_of(receipts.completed[0]) and receipts.completed[0] or (None, None, None, ())
    assert text == CONTENT_OUTPUT_BLOCKED_TEXT
    kinds = [(e.event_type, e.outcome) for e in audit]
    assert kinds == [
        ("content_decision", "allow"),  # input
        ("content_decision", "blocked"),  # output
        ("agent_result", "success"),
    ]
    assert audit[1].category == "credential"


@pytest.mark.asyncio
async def test_stream_output_allowed_collapses_to_one_merged_delta():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event("alpha "), _text_event("beta")], receipts)
    events = [e async for e in service.stream(_task())]
    deltas = [e for e in events if e.type == "delta"]
    assert len(deltas) == 1
    assert deltas[0].data == "alpha beta"
    assert events[-1].type == "done"
    assert receipts.completed[0][1] == "alpha beta"


@pytest.mark.asyncio
async def test_no_output_enforcement_keeps_live_streaming():
    receipts = _CaptureReceiptRepo()
    policy = ContentPolicyConfig(output_action="allow")
    service, _agent = _service([_text_event("alpha "), _text_event("beta")], receipts, policy=policy)
    events = [e async for e in service.stream(_task())]
    deltas = [e for e in events if e.type == "delta"]
    assert [d.data for d in deltas] == ["alpha ", "beta"]  # untouched live stream
    kinds = [(e.event_type, e.outcome) for e in _events_of(receipts.completed[0])]
    assert kinds == [("content_decision", "allow"), ("agent_result", "success")]


@pytest.mark.asyncio
async def test_sync_chat_uses_same_output_decision_path():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event(CREDENTIAL_OUTPUT)], receipts)
    result = await service.chat(_task())
    assert result.response == CONTENT_OUTPUT_BLOCKED_TEXT
    assert result.error_code is None
    assert receipts.completed[0][1] == CONTENT_OUTPUT_BLOCKED_TEXT


@pytest.mark.asyncio
async def test_stream_output_blocked_replaces_without_leaking_buffered_tools():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service(
        [
            _tool_call_event("get_current_time"),
            _tool_result_event("get_current_time"),
            _text_event(CREDENTIAL_OUTPUT),
        ],
        receipts,
        tool_decisions={"get_current_time": "allow"},
    )
    events = [e async for e in service.stream(_task())]
    assert all(e.type != "tool" for e in events)  # tool events suppressed with blocked output
    assert [e.type for e in events] == ["delta", "done"]
    assert events[0].data == CONTENT_OUTPUT_BLOCKED_TEXT


# ---------------------------------------------------------------------------
# tool + approval audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_decision_allow_recorded_for_allowed_calls():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service(
        [
            _tool_call_event("get_current_time"),
            _tool_result_event("get_current_time"),
            _text_event("time shown"),
        ],
        receipts,
        tool_decisions={"get_current_time": "allow"},
    )
    await service.chat(_task())
    events = _events_of(receipts.completed[0])
    tool = [e for e in events if e.event_type == "tool_decision"]
    assert [(e.outcome, e.tool_name) for e in tool] == [("allow", "get_current_time")]


@pytest.mark.asyncio
async def test_review_pause_writes_review_pending_in_pause_transaction():
    receipts = _CaptureReceiptRepo()
    approvals = _CaptureApprovalRepo()
    service, _agent = _service(
        [_long_running_event("get_current_time")],
        receipts,
        approvals,
        tool_decisions={"get_current_time": "review"},
    )
    result = await service.chat(_task())
    assert result.error_code is None
    kwargs = approvals.pauses[0]
    events = kwargs["execution_events"]
    kinds = [(e.event_type, e.outcome) for e in events]
    assert kinds == [("content_decision", "allow"), ("tool_decision", "review_pending")]
    assert events[1].tool_name == "get_current_time"
    assert all(e.receipt_id == receipts.receipt_id for e in events)
    # the receipt terminal is the pause transaction itself
    assert receipts.completed == []
    assert result.response == kwargs["pending_response"]


@pytest.mark.asyncio
async def test_error_terminal_carries_agent_result_error_with_code():
    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event("x")], receipts)

    class _Boom:

        async def run(self, **kwargs):
            raise RuntimeError("boom")
            yield

        async def close(self) -> None:
            return None

    service._agent_app = _Boom()  # type: ignore[assignment]
    result = await service.chat(_task())
    assert result.error_code == WorkerErrorCode.MODEL_RUNTIME
    events = _events_of(receipts.failed[0])
    assert [(e.event_type, e.outcome, e.error_code) for e in events] == [
        ("content_decision", "allow", None),
        ("agent_result", "error", "model_runtime"),
    ]


# ---------------------------------------------------------------------------
# approval decide terminal
# ---------------------------------------------------------------------------


def _approval_request(receipt_id: uuid.UUID) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=uuid.uuid4(),
        tenant_id=TENANT,
        app_id="app_demo",
        config_version=1,
        channel="web_console",
        user_id="user_default",
        session_id="sess-1",
        receipt_id=receipt_id,
        function_call_id="call-1",
        tool_name="get_current_time",
        args_digest="a" * 64,
        state="executing",
        decision="approve",
        decision_message_id="msg-dec",
        response_text=None,
    )


class _DecideApprovalRepo:

    def __init__(self, request: ApprovalRequest) -> None:
        self.request = request
        self.finalized: list[dict] = []

    async def claim_decision(self, approval_id, **ident):
        return ApprovalClaim(
            action=ApprovalAction.EXECUTE,
            approval_id=approval_id,
            state="executing",
            response_text=None,
        )

    async def get(self, approval_id):
        return self.request

    async def get_tool_args(self, approval_id):
        return {}

    async def finalize(self,
                       approval_id,
                       *,
                       receipt_id,
                       terminal_state,
                       response_text,
                       error_code,
                       latency_ms,
                       execution_events=()):
        self.finalized.append({
            "terminal_state": terminal_state,
            "response_text": response_text,
            "execution_events": tuple(execution_events),
        })


class _DecideReceiptRepo(_CaptureReceiptRepo):

    async def claim(self, task, message_text):  # decision-message claim
        return MessageClaim(
            action=ReceiptAction.EXECUTE,
            receipt_id=self.receipt_id,
            response_text=None,
            error_code=None,
        )


@pytest.mark.asyncio
async def test_decide_approve_records_allow_and_agent_result_in_finalize():
    receipts = _DecideReceiptRepo()
    request = _approval_request(receipts.receipt_id)
    approvals = _DecideApprovalRepo(request)
    agent = _ScriptAgent([_text_event("approved output")])
    service = ToolApprovalService(
        tenant_repository=FakeTenantConfigRepository(_configs(tool_decisions={"get_current_time": "review"})),
        receipt_repository=receipts,
        approval_repository=approvals,
        agent_app=agent,
        tool_registry=AllowedToolRegistry.default(),
    )
    task = WorkerApprovalTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=TENANT,
        app_id="app_demo",
        config_version=1,
        user_id="user_default",
        channel="web_console",
        session_id="sess-1",
        message_id="msg-dec",
        approval_id=request.approval_id,
        decision="approve",
    )
    result = await service.decide(task)
    assert result.error_code is None
    entry = approvals.finalized[0]
    kinds = [(e.event_type, e.outcome) for e in entry["execution_events"]]
    assert kinds == [
        ("content_decision", "allow"),  # default policy: output enforced, clean text
        ("tool_decision", "allow"),
        ("agent_result", "success"),
    ]
    assert all(e.receipt_id == receipts.receipt_id and e.request_id == task.request_id
               for e in entry["execution_events"])


@pytest.mark.asyncio
async def test_decide_output_blocked_replies_safe_text():
    receipts = _DecideReceiptRepo()
    request = _approval_request(receipts.receipt_id)
    approvals = _DecideApprovalRepo(request)
    agent = _ScriptAgent([_text_event(CREDENTIAL_OUTPUT)])
    service = ToolApprovalService(
        tenant_repository=FakeTenantConfigRepository(_configs(tool_decisions={"get_current_time": "review"})),
        receipt_repository=receipts,
        approval_repository=approvals,
        agent_app=agent,
        tool_registry=AllowedToolRegistry.default(),
    )
    task = WorkerApprovalTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=TENANT,
        app_id="app_demo",
        config_version=1,
        user_id="user_default",
        channel="web_console",
        session_id="sess-1",
        message_id="msg-dec",
        approval_id=request.approval_id,
        decision="approve",
    )
    result = await service.decide(task)
    assert result.response == CONTENT_OUTPUT_BLOCKED_TEXT
    entry = approvals.finalized[0]
    assert entry["response_text"] == CONTENT_OUTPUT_BLOCKED_TEXT
    kinds = [(e.event_type, e.outcome) for e in entry["execution_events"]]
    assert kinds == [
        ("content_decision", "blocked"),
        ("tool_decision", "allow"),
        ("agent_result", "success"),
    ]


# ---------------------------------------------------------------------------
# gateway delivery audit
# ---------------------------------------------------------------------------


class _FakeExecRepo:

    def __init__(self) -> None:
        self.appended: list[ExecutionAuditEvent] = []
        self.raise_on_append = False

    async def append(self, event: ExecutionAuditEvent) -> None:
        if self.raise_on_append:
            raise RuntimeError("audit db down")
        self.appended.append(event)


class _FakeChannelClient:

    def __init__(self) -> None:
        self.chat_result: WorkerChatResult | None = None
        self.chat_error: Exception | None = None
        self.stream_events: list[WorkerEvent] = []
        self.stream_error: Exception | None = None
        self.decide_result: WorkerApprovalResult | None = None

    async def chat(self, task):
        if self.chat_error is not None:
            raise self.chat_error
        return self.chat_result or WorkerChatResult(protocol_version=1, request_id=task.request_id, response="hi")

    async def stream(self, task):
        if self.stream_error is not None:
            raise self.stream_error
        for event in self.stream_events:
            yield event

    async def decide(self, task):
        return self.decide_result or WorkerApprovalResult(
            protocol_version=1,
            request_id=task.request_id,
            response="ok",
        )


def _inbound(text="hello", **overrides) -> InboundMessage:
    defaults = {
        "tenant_id": TENANT,
        "channel": "web_console",
        "external_user_id": "user_abc",
        "external_conversation_id": "conv_1",
        "external_message_id": "msg_1",
        "text": text,
    }
    defaults.update(overrides)
    return InboundMessage(**defaults)


def _ingress(client: _FakeChannelClient, exec_repo: _FakeExecRepo | None):
    return ChannelIngressService(
        FakeTenantConfigRepository(_configs()),
        client,
        execution_repository=exec_repo,
    )


@pytest.mark.asyncio
async def test_delivery_delivered_recorded_on_chat_success():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    service = _ingress(client, exec_repo)
    await service.chat(_inbound())
    event = exec_repo.appended[0]
    assert (event.event_type, event.outcome, event.receipt_id, event.error_code) == ("delivery_result", "delivered",
                                                                                     None, None)
    assert event.tenant_id == TENANT
    assert event.config_version == 1


@pytest.mark.asyncio
async def test_delivery_failed_recorded_with_fixed_error_code():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    task = _task()
    client.chat_result = WorkerChatResult(
        protocol_version=1,
        request_id=task.request_id,
        response="",
        error_code=WorkerErrorCode.SESSION_BUSY,
    )
    service = _ingress(client, exec_repo)
    reply = await service.chat(_inbound())
    assert reply.response == map_worker_error(WorkerErrorCode.SESSION_BUSY)
    event = exec_repo.appended[0]
    assert (event.outcome, event.error_code) == ("failed", "session_busy")


@pytest.mark.asyncio
async def test_delivery_stream_done_and_error_terminals():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    client.stream_events = [
        WorkerEvent(protocol_version=1, request_id=uuid.uuid4(), type="done", data=None, error_code=None)
    ]
    service = _ingress(client, exec_repo)
    _ = [e async for e in service.stream(_inbound())]
    assert exec_repo.appended[0].outcome == "delivered"

    client2, exec2 = _FakeChannelClient(), _FakeExecRepo()
    rid = uuid.uuid4()
    client2.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.SESSION_BUSY,
        ),
    ]
    service2 = _ingress(client2, exec2)
    _ = [e async for e in service2.stream(_inbound())]
    assert (exec2.appended[0].outcome, exec2.appended[0].error_code) == ("failed", "session_busy")


@pytest.mark.asyncio
async def test_delivery_worker_client_error_failed():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    client.stream_error = WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
    service = _ingress(client, exec_repo)
    _ = [e async for e in service.stream(_inbound())]
    assert (exec_repo.appended[0].outcome, exec_repo.appended[0].error_code) == ("failed", "worker_unavailable")


@pytest.mark.asyncio
async def test_delivery_generic_exception_writes_no_event():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    client.chat_error = RuntimeError("internal")
    service = _ingress(client, exec_repo)
    reply = await service.chat(_inbound())
    assert exec_repo.appended == []
    from trpc_service.gateway.errors import SAFE_ERROR_TEXT
    assert reply.response == SAFE_ERROR_TEXT  # fixed text only, no exception detail


@pytest.mark.asyncio
async def test_audit_append_failure_cannot_alter_reply():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    exec_repo.raise_on_append = True
    service = _ingress(client, exec_repo)
    reply = await service.chat(_inbound())
    assert reply.response == "hi"


@pytest.mark.asyncio
async def test_no_execution_repository_means_no_delivery_events():
    client = _FakeChannelClient()
    service = _ingress(client, None)
    await service.chat(_inbound())  # must not raise


@pytest.mark.asyncio
async def test_decide_delivery_recorded():
    client, exec_repo = _FakeChannelClient(), _FakeExecRepo()
    approval_id = uuid.uuid4()
    service = _ingress(client, exec_repo)
    _ = [e async for e in service.stream(_inbound(text=f"/approve {approval_id}"))]
    assert exec_repo.appended[0].event_type == "delivery_result"
    assert exec_repo.appended[0].outcome == "delivered"


@pytest.mark.asyncio
async def test_trace_id_populated_from_active_span():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import use_span

    receipts = _CaptureReceiptRepo()
    service, _agent = _service([_text_event("ok")], receipts)
    provider = TracerProvider()
    try:
        span = provider.get_tracer("test").start_span("worker.request")
        with use_span(span):
            await service.chat(_task())
        span.end()
    finally:
        provider.shutdown()
    events = _events_of(receipts.completed[0])
    assert all(e.trace_id and len(e.trace_id) == 32 for e in events)
    assert len({e.trace_id for e in events}) == 1
