"""Stage 6A2 Task 3 (RED): first-request pause and Worker approval state machine.

Contracts pinned here:
- LongRunningEvent whose response is exactly {"status": "approval_required"}
  pauses: one atomic pause transaction (pending approval + first receipt
  completed with the fixed pending reply), SSE = approval -> done.
- Any other LongRunningEvent content must NOT create an approval.
- decide: identity CAS via repository; EXECUTE approve re-validates config
  then executes the controlled entry exactly once and resumes the Runner;
  EXECUTE reject never executes but resumes; REPLAY returns the stored text
  with zero side effects; IN_PROGRESS/CONFLICT/NOT_AVAILABLE/CONFIG_STALE
  map to fixed error codes, fail the decision receipt, and NEVER re-run the
  tool or auto-reset executing.
"""

from __future__ import annotations

import asyncio
import uuid

from trpc_agent_sdk.events import Event, LongRunningEvent
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.types import Content, FunctionCall, FunctionResponse, Part

from tests.tenant_helpers import make_app_config, make_governance, make_tenant_config
from trpc_service.governance.approval import (
    ApprovalAction,
    ApprovalClaim,
    ApprovalRequest,
)
from trpc_service.storage.message_repository import (
    MessageClaim,
    ReceiptAction,
)
from trpc_service.transport.models import (
    WorkerApprovalTask,
    WorkerErrorCode,
    WorkerTask,
)

TENANT = "tenant_default"


def _config(version: int = 1, review: bool = True, tools: tuple[str, ...] = ("get_current_time", )) -> object:
    decisions = {"get_current_time": "review"} if review else {}
    return make_tenant_config(
        TENANT,
        version=version,
        app=make_app_config(allowed_tools=tools),
        governance=make_governance(
            allowed_channels=("web_console", "web", "wecom", "feishu"),
            tool_decisions=decisions,
        ),
    )


def _worker_task(message_id: str = "m1", text: str = "what time?") -> WorkerTask:
    cfg = _config()
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=TENANT,
        app_id=cfg.app.app_id,
        config_version=cfg.version,
        user_id="usr_v1_" + "a" * 48,
        channel="web_console",
        session_id="ses_v1_" + "b" * 48,
        message_id=message_id,
        message=text,
    )


def _approval_req(**over) -> ApprovalRequest:
    base = dict(
        approval_id=uuid.uuid4(),
        tenant_id=TENANT,
        app_id="app_demo",
        config_version=1,
        channel="web_console",
        user_id="usr_v1_" + "a" * 48,
        session_id="ses_v1_" + "b" * 48,
        receipt_id=uuid.uuid4(),
        function_call_id="call-1",
        tool_name="get_current_time",
        args_digest="d" * 64,
        state="executing",
        decision=None,
        decision_message_id=None,
        response_text=None,
    )
    base.update(over)
    return ApprovalRequest(**base)


def _lr_event(response: dict, call_id: str = "call-1") -> LongRunningEvent:
    return LongRunningEvent(
        invocation_id="inv-1",
        author="app_demo",
        function_call=FunctionCall(id=call_id, name="get_current_time", args={}),
        function_response=FunctionResponse(id=call_id, name="get_current_time", response=response),
    )


def _tool_events_then_pause(response: dict = None) -> list:
    resp = response if response is not None else {"status": "approval_required"}
    tool_part = Event(
        invocation_id="inv-1",
        author="app_demo",
        content=Content(
            role="model",
            parts=[Part(function_response=FunctionResponse(
                id="call-1",
                name="get_current_time",
                response=resp,
            ))]),
    )
    return [tool_part, _lr_event(resp)]


class FakeTenantRepo:

    def __init__(self, config) -> None:
        self.config = config
        self.query_count = 0

    async def get(self, tenant_id: str):
        self.query_count += 1
        return self.config

    async def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        return None


class FakeReceiptRepo:

    def __init__(self, claim: MessageClaim | None = None) -> None:
        self.claim_result = claim or MessageClaim(
            action=ReceiptAction.EXECUTE,
            receipt_id=uuid.uuid4(),
            response_text=None,
            error_code=None,
        )
        self.claims: list = []
        self.completed: list = []
        self.failed: list = []

    async def claim(self, task, message_text):
        self.claims.append((task, message_text))
        return self.claim_result

    async def complete(self, receipt_id, response_text, latency_ms, execution_events=()):
        self.completed.append((receipt_id, response_text))

    async def fail(self, receipt_id, error_code, latency_ms, execution_events=()):
        self.failed.append((receipt_id, error_code))

    async def list_audit(self, tenant_id, message_id, limit):
        return ()

    async def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        return None


class FakeApprovalRepo:

    def __init__(self, *, claim_result: ApprovalClaim | None = None, request: ApprovalRequest | None = None) -> None:
        self.claim_result = claim_result or ApprovalClaim(
            action=ApprovalAction.EXECUTE,
            approval_id=uuid.uuid4(),
            state="executing",
            response_text=None,
        )
        self.request = request or _approval_req()
        self.pauses: list = []
        self.claims: list = []
        self.finalized: list = []
        self.finalize_error: Exception | None = None
        self.tool_args = {"city_hint": "unused"}
        self.get_error: Exception | None = None
        self.args_error: Exception | None = None

    async def pause_first_request(self, **kwargs):
        self.pauses.append(kwargs)
        return _approval_req(approval_id=kwargs["approval_id"])

    async def claim_decision(self, approval_id, **ident):
        self.claims.append((approval_id, ident))
        claim = self.claim_result
        if claim.approval_id is None and claim.action == ApprovalAction.NOT_AVAILABLE:
            return claim
        return ApprovalClaim(claim.action, approval_id, claim.state, claim.response_text)

    async def finalize(self,
                       approval_id,
                       *,
                       receipt_id,
                       terminal_state,
                       response_text,
                       error_code,
                       latency_ms,
                       execution_events=()):
        self.finalized.append((approval_id, terminal_state, response_text, error_code))
        if self.finalize_error is not None:
            raise self.finalize_error

    async def get(self, approval_id):
        if self.get_error is not None:
            raise self.get_error
        return self.request

    async def get_tool_args(self, approval_id):
        if self.args_error is not None:
            raise self.args_error
        return dict(self.tool_args)

    async def list_audit(self, approval_id, limit):
        return ()

    async def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        return None


class FakeRegistry:

    def __init__(self, *, result: str = "2026-09-04T12:00:00+08:00", error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.executed: list = []

    async def execute_approved(self, name, args):
        self.executed.append((name, args))
        if self.error is not None:
            raise self.error
        return self.result


class FakeAgentApp:

    def __init__(self, *, resume_events=None, resume_error: Exception | None = None) -> None:
        self._first_events = list(resume_events or [])
        self.resolved_results: list = []
        self.resume_error = resume_error
        self.resume_calls: list = []
        self.run_calls: list = []

    async def run(self, *, config, context, session_id, user_input):
        self.run_calls.append((config, context, session_id, user_input))
        for event in self._first_events:
            yield event

    async def resume(self, *, config, context, session_id, function_call_id, tool_name, tool_result=None, execute=None):
        # recorded when the generator body actually starts (inside lease in
        # production); executor must not have run yet at THIS moment
        entry_state = {"tool_result": tool_result, "has_execute": execute is not None}
        self.resume_calls.append(
            dict(
                config=config,
                context=context,
                session_id=session_id,
                function_call_id=function_call_id,
                tool_name=tool_name,
                **entry_state,
            ))
        resolved = tool_result
        if execute is not None:
            resolved = await execute()
        self.resolved_results.append(resolved)
        if self.resume_error is not None:
            raise self.resume_error
        yield LlmResponse(
            content=Content(role="model", parts=[Part.from_text(text="RESUMED-FINAL")]),
            invocation_id="inv-2",
            author="app_demo",
        )

    async def close(self) -> None:
        return None


def _text_event(text: str) -> Event:
    return Event(
        invocation_id="inv-2",
        author="app_demo",
        content=Content(role="model", parts=[Part.from_text(text=text)]),
    )


def _build_service(
    *,
    config=None,
    first_events=None,
    receipt_repo=None,
    approval_repo=None,
    registry=None,
    agent_app=None,
):
    from trpc_service.worker.approval_service import ToolApprovalService
    from trpc_service.worker.service import WorkerService

    config = config or _config()
    tenant_repo = FakeTenantRepo(config)
    agent_app = agent_app or FakeAgentApp(resume_events=[])
    # the agent_app.run used by WorkerService must yield the scripted first-run
    # events; FakeAgentApp already replays _first_events for run()
    if agent_app._first_events == [] and first_events is not None:
        agent_app._first_events = list(first_events)
    service = WorkerService(
        tenant_repo,
        agent_app,
        receipt_repository=receipt_repo or FakeReceiptRepo(),
        approval_repository=approval_repo or FakeApprovalRepo(),
    )
    approval_service = ToolApprovalService(
        tenant_repository=tenant_repo,
        receipt_repository=receipt_repo or service._receipt_repository,  # noqa: SLF001
        approval_repository=approval_repo or service._approval_repository,  # noqa: SLF001
        agent_app=agent_app,
        tool_registry=registry or FakeRegistry(),
    )
    return service, approval_service, agent_app


async def _collect(agen) -> list:
    return [e async for e in agen]


class TestFirstRequestPause:

    def test_pause_emits_approval_then_done_and_completes_receipt_with_pending_text(self, caplog):
        task = _worker_task()
        receipt = FakeReceiptRepo()
        approval = FakeApprovalRepo()
        service, _, _ = _build_service(
            first_events=_tool_events_then_pause(),
            receipt_repo=receipt,
            approval_repo=approval,
        )
        with caplog.at_level("INFO"):
            events = asyncio.run(_collect(service.stream(task)))

        assert "approval request created (tenant=tenant_default, tool=get_current_time, state=pending)" in caplog.text

        types = [e.type for e in events]
        assert types[-2:] == ["approval", "done"]
        assert types.count("error") == 0
        pause = approval.pauses
        assert len(pause) == 1
        assert pause[0]["function_call_id"] == "call-1"
        assert pause[0]["tool_name"] == "get_current_time"
        assert pause[0]["tool_args"] == {} or isinstance(pause[0]["tool_args"], dict)
        assert pause[0]["receipt_id"] is not None
        # receipt completion happens atomically INSIDE pause_first_request
        # (proven against real PostgreSQL in the integration suite); the
        # service must hand it the fixed pending text with the opaque id.
        assert receipt.completed == []
        pending_text = pause[0]["pending_response"]
        assert str(pause[0]["approval_id"]) in pending_text
        assert "/approve" in pending_text and "/reject" in pending_text
        # no further model/tool work after pause
        assert "approval_required" not in "".join(e.data or "" for e in events if e.type == "delta")

    def test_chat_returns_pending_reply_on_pause(self):
        task = _worker_task()
        approval = FakeApprovalRepo()
        service, _, _ = _build_service(
            first_events=_tool_events_then_pause(),
            approval_repo=approval,
        )
        result = asyncio.run(service.chat(task))
        assert result.error_code is None
        assert str(approval.pauses[0]["approval_id"]) in result.response

    def test_long_running_event_with_foreign_response_is_runtime_error_no_approval(self):
        task = _worker_task()
        approval = FakeApprovalRepo()
        service, _, _ = _build_service(
            first_events=_tool_events_then_pause({"status": "unexpected"}),
            approval_repo=approval,
        )
        events = asyncio.run(_collect(service.stream(task)))
        assert [e.type for e in events if e.type == "error"]
        assert approval.pauses == []

    def test_pause_without_approval_repo_is_runtime_error(self):
        from trpc_service.worker.service import WorkerService

        task = _worker_task()
        service = WorkerService(
            FakeTenantRepo(_config()),
            FakeAgentApp(resume_events=None),
            receipt_repository=FakeReceiptRepo(),
        )
        service._agent_app._first_events = _tool_events_then_pause()  # noqa: SLF001
        events = asyncio.run(_collect(service.stream(task)))
        assert any(e.type == "error" for e in events)


class TestDecideApprove:

    def _task(self, approval: ApprovalRequest, decision: str = "approve") -> WorkerApprovalTask:
        return WorkerApprovalTask(
            protocol_version=1,
            request_id=uuid.uuid4(),
            tenant_id=TENANT,
            app_id="app_demo",
            config_version=1,
            user_id=approval.user_id,
            channel=approval.channel,
            session_id=approval.session_id,
            message_id=f"decide-{uuid.uuid4().hex[:6]}",
            approval_id=approval.approval_id,
            decision=decision,
        )

    def test_approve_executes_once_resumes_and_completes(self, caplog):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        approval.tool_args = {}
        registry = FakeRegistry()
        agent_app = FakeAgentApp()
        agent_app.resume_final_events = None
        receipt = FakeReceiptRepo()
        service, approval_service, agent_app = _build_service(
            receipt_repo=receipt,
            approval_repo=approval,
            registry=registry,
            agent_app=agent_app,
        )
        # resume must yield final text
        agent_app.resume = _resume_ok(agent_app)
        task = self._task(approval_req)
        with caplog.at_level("INFO"):
            result = asyncio.run(approval_service.decide(task))

        assert caplog.text.count(
            "approval executed (tenant=tenant_default, tool=get_current_time, decision=approve)") == 1
        assert registry.result not in caplog.text  # tool RESULT value never logged

        assert result.error_code is None
        assert "RESUMED-FINAL" in result.response
        assert registry.executed == [("get_current_time", {})]
        call = agent_app.resume_calls[0]
        assert call["session_id"] == approval_req.session_id
        assert call["function_call_id"] == "call-1"
        assert call["tool_name"] == "get_current_time"
        # P0-1: decide must hand an EXECUTE CALLABLE to the runtime, not a
        # pre-computed result; the tool may only run inside the lease.
        assert call["has_execute"] is True and call["tool_result"] is None
        assert agent_app.resolved_results == [{"output": registry.result}]
        assert len(registry.executed) == 1
        # P0-2: ONE finalize call atomically covers approval + decision receipt
        assert approval.finalized == [(approval_req.approval_id, "completed", None, None)] or \
            approval.finalized[0][0] == approval_req.approval_id
        state = approval.finalized[0][1]
        assert state == "completed"
        assert approval.finalized[0][2] and "RESUMED-FINAL" in approval.finalized[0][2]
        assert receipt.completed == [] and receipt.failed == []

    def test_reject_zero_tool_calls_resumes_with_fixed_verdict(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        registry = FakeRegistry()
        receipt = FakeReceiptRepo()
        _, approval_service, agent_app = _build_service(
            receipt_repo=receipt,
            approval_repo=approval,
            registry=registry,
        )
        agent_app.resume = _resume_ok(agent_app)
        task = self._task(approval_req, decision="reject")
        result = asyncio.run(approval_service.decide(task))

        assert result.error_code is None
        assert registry.executed == []
        call = agent_app.resume_calls[0]
        assert call["tool_result"] == {"status": "rejected"}
        assert approval.finalized and approval.finalized[0][1] == "rejected"

    def test_config_version_change_blocks_execution(self):
        approval_req = _approval_req(config_version=1)
        approval = FakeApprovalRepo(request=approval_req)
        registry = FakeRegistry()
        receipt = FakeReceiptRepo()
        # current head advanced to v2
        service, approval_service, agent_app = _build_service(
            config=_config(version=2),
            receipt_repo=receipt,
            approval_repo=approval,
            registry=registry,
        )
        agent_app.resume = _resume_ok(agent_app)
        task = self._task(approval_req)
        result = asyncio.run(approval_service.decide(task))

        assert result.error_code == WorkerErrorCode.APPROVAL_CONFIG_STALE
        assert registry.executed == []
        assert agent_app.resume_calls == []
        assert approval.finalized == [(approval_req.approval_id, "failed", None, "approval_config_stale")]
        assert receipt.failed == [] and receipt.completed == []

    def test_decision_no_longer_review_blocks(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        registry = FakeRegistry()
        config_now = _config(review=False)
        _, approval_service, agent_app = _build_service(
            config=config_now,
            approval_repo=approval,
            registry=registry,
        )
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code == WorkerErrorCode.APPROVAL_CONFIG_STALE
        assert registry.executed == []

    def test_replay_returns_stored_text_without_side_effects(self):
        approval_req = _approval_req(state="completed", decision="approve", response_text="CACHED")
        approval = FakeApprovalRepo(
            request=approval_req,
            claim_result=ApprovalClaim(ApprovalAction.REPLAY, None, "completed", "CACHED"),
        )
        registry = FakeRegistry()
        _, approval_service, agent_app = _build_service(
            approval_repo=approval,
            registry=registry,
        )
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.response == "CACHED"
        assert result.error_code is None
        assert registry.executed == []
        assert agent_app.resume_calls == []

    def test_decision_message_receipt_replay_short_circuits(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        receipt = FakeReceiptRepo(claim=MessageClaim(
            action=ReceiptAction.REPLAY,
            receipt_id=uuid.uuid4(),
            response_text="DECISION-REPLAYED",
            error_code=None,
        ))
        _, approval_service, agent_app = _build_service(receipt_repo=receipt, approval_repo=approval)
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.response == "DECISION-REPLAYED"
        assert approval.claims == []  # never reached decision CAS

    def test_in_progress_and_conflict_and_not_available_map_to_fixed_codes(self):
        approval_req = _approval_req()
        cases = {
            ApprovalAction.IN_PROGRESS: WorkerErrorCode.APPROVAL_IN_PROGRESS,
            ApprovalAction.CONFLICT: WorkerErrorCode.APPROVAL_CONFLICT,
            ApprovalAction.NOT_AVAILABLE: WorkerErrorCode.APPROVAL_NOT_AVAILABLE,
        }
        for action, code in cases.items():
            claim = ApprovalClaim(
                action,
                approval_req.approval_id if action != ApprovalAction.NOT_AVAILABLE else None,
                "executing" if action == ApprovalAction.IN_PROGRESS else "completed",
                None,
            )
            approval = FakeApprovalRepo(request=approval_req, claim_result=claim)
            receipt = FakeReceiptRepo()
            registry = FakeRegistry()
            _, approval_service, agent_app = _build_service(
                approval_repo=approval,
                registry=registry,
                receipt_repo=receipt,
            )
            agent_app.resume = _resume_ok(agent_app)
            result = asyncio.run(approval_service.decide(self._task(approval_req)))
            assert result.error_code == code, action
            assert registry.executed == []
            assert agent_app.resume_calls == []
            assert receipt.failed, action
            assert receipt.failed[-1][1] == code
            assert approval.finalized == []

    def test_execution_failure_fails_approval_and_no_resume(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        registry = FakeRegistry(error=RuntimeError("tool blew up"))
        receipt = FakeReceiptRepo()
        _, approval_service, agent_app = _build_service(
            approval_repo=approval,
            registry=registry,
            receipt_repo=receipt,
        )
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code == WorkerErrorCode.APPROVAL_EXECUTION_FAILED
        # P0-1 semantics: execution happens INSIDE the resume generator, so a
        # failed tool aborts that single resume attempt — exactly one attempt,
        # no event text produced, and never re-executed.
        assert len(agent_app.resume_calls) == 1
        assert agent_app.resolved_results == []
        assert registry.executed == [("get_current_time", {"city_hint": "unused"})]
        assert approval.finalized == [(approval_req.approval_id, "failed", None, "approval_execution_failed")]
        assert receipt.failed == []

    def test_resume_failure_fails_approval_without_reexecution(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        registry = FakeRegistry()
        receipt = FakeReceiptRepo()
        from trpc_service.agent.errors import TenantAgentConfigurationError
        _, approval_service, agent_app = _build_service(
            approval_repo=approval,
            registry=registry,
            receipt_repo=receipt,
        )

        async def broken_resume(*, execute=None, tool_result=None, **kwargs):
            agent_app.resume_calls.append({**kwargs, "has_execute": execute is not None})
            # simulate failing AFTER the approved execution ran inside the lease
            if execute is not None:
                await execute()
            raise TenantAgentConfigurationError()
            yield  # pragma: no cover (makes this an async generator)

        agent_app.resume = broken_resume
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code == WorkerErrorCode.TENANT_AGENT_CONFIGURATION
        assert len(registry.executed) == 1  # executed once, never re-run
        assert approval.finalized == [(approval_req.approval_id, "failed", None, "tenant_agent_configuration")]
        assert receipt.failed == []

    def test_identity_fields_reach_cas_unchanged(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(
            request=approval_req,
            claim_result=ApprovalClaim(ApprovalAction.NOT_AVAILABLE, None, None, None),
        )
        _, approval_service, _ = _build_service(approval_repo=approval)
        asyncio.run(approval_service.decide(self._task(approval_req)))
        _aid, ident = approval.claims[0]
        assert ident["tenant_id"] == approval_req.tenant_id
        assert ident["channel"] == approval_req.channel
        assert ident["user_id"] == approval_req.user_id
        assert ident["session_id"] == approval_req.session_id
        assert ident["decision"] == "approve"


def _resume_ok(agent_app):

    async def resume(*, execute=None, tool_result=None, **kwargs):
        agent_app.resume_calls.append({
            **kwargs,
            "tool_result": tool_result,
            "has_execute": execute is not None,
        })
        resolved = tool_result if tool_result is not None else await execute()
        agent_app.resolved_results.append(resolved)
        yield _text_event("RESUMED-FINAL")

    return resume


class TestFinalizeAndQueryFailures:

    def test_finalize_failure_never_reports_success(self):
        """P0-2: a DataError while finalizing must NOT be swallowed into a
        successful result; the caller returns a fixed error and the approval
        remains executing (fail-closed)."""
        approval_req = _approval_req()
        from trpc_service.storage.approval_repository import ToolApprovalRepositoryDataError
        approval = FakeApprovalRepo(request=approval_req)
        approval.finalize_error = ToolApprovalRepositoryDataError("finalize race")
        registry = FakeRegistry()
        receipt = FakeReceiptRepo()
        _, approval_service, agent_app = _build_service(approval_repo=approval, registry=registry, receipt_repo=receipt)
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task_static(approval_req)))
        assert result.error_code in (
            WorkerErrorCode.APPROVAL_CONFLICT,
            WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE,
            WorkerErrorCode.APPROVAL_EXECUTION_FAILED,
        )
        assert result.response == ""
        # never claims success; executed exactly once (no re-run)
        assert len(registry.executed) == 1

    def _task_static(self, approval_req):
        return self._task(approval_req) if hasattr(self, "_task") else TestDecideApprove._task(self, approval_req)

    _task = TestDecideApprove._task

    def test_approval_get_unavailable_maps_fixed_and_finalizes_failed(self):
        approval_req = _approval_req()
        from trpc_service.storage.approval_repository import ToolApprovalRepositoryUnavailableError
        approval = FakeApprovalRepo(request=approval_req)
        approval.get_error = ToolApprovalRepositoryUnavailableError("db gone")
        _, approval_service, agent_app = _build_service(approval_repo=approval, registry=FakeRegistry())
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code == WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE
        assert agent_app.resume_calls == []
        assert approval.finalized and approval.finalized[0][1] == "failed"

    def test_tenant_get_unavailable_maps_fixed(self):
        approval_req = _approval_req()
        from trpc_service.config.tenant_repository import TenantRepositoryUnavailableError
        _, approval_service, agent_app = _build_service(registry=FakeRegistry())

        async def boom(tenant_id):
            raise TenantRepositoryUnavailableError("gone")

        approval_service._tenant_repository.get = boom  # noqa: SLF001
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code in (
            WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE,
            WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE,
        )
        assert agent_app.resume_calls == []

    def test_get_tool_args_none_finalizes_failed(self):
        approval_req = _approval_req()
        approval = FakeApprovalRepo(request=approval_req)
        approval.tool_args = None
        registry = FakeRegistry()
        _, approval_service, agent_app = _build_service(approval_repo=approval, registry=registry)
        agent_app.resume = _resume_ok(agent_app)
        result = asyncio.run(approval_service.decide(self._task(approval_req)))
        assert result.error_code == WorkerErrorCode.APPROVAL_EXECUTION_FAILED
        assert registry.executed == []
        assert approval.finalized and approval.finalized[0][1] == "failed"


class TestReviewToolEventSuppression:
    """P1-1: a review tool's raw call/result events (which carry tool ARGS)
    must never surface as public tool events; only approval -> done."""

    SENTINEL = "SECRET_SENTINEL_ARGS"

    def _events_with_sentinel_call(self):
        call_part = Event(
            invocation_id="inv-1",
            author="app_demo",
            content=Content(
                role="model",
                parts=[
                    Part(function_call=FunctionCall(
                        id="call-1",
                        name="get_current_time",
                        args={"q": self.SENTINEL},
                    ))
                ],
            ),
        )
        return [call_part] + _tool_events_then_pause()

    def test_worker_stream_suppresses_review_tool_events(self):
        task = _worker_task()
        service, _, _ = _build_service(first_events=self._events_with_sentinel_call())
        events = asyncio.run(_collect(service.stream(task)))
        assert [e.type for e in events if e.type not in ("tool", )] == [
            "approval",
            "done",
        ]
        assert not any(e.type == "tool" for e in events)
        joined = " ".join(repr(e) for e in events)
        assert self.SENTINEL not in joined

    def test_non_review_tool_events_still_flow(self):
        # allow decision: the normal (non-review) path keeps 6A1 behavior
        from tests.tenant_helpers import make_app_config, make_governance, make_tenant_config

        config = make_tenant_config(
            TENANT,
            app=make_app_config(allowed_tools=("get_current_time", )),
            governance=make_governance(tool_decisions={}),
        )
        task = _worker_task()
        call_part = Event(
            invocation_id="inv-1",
            author="app_demo",
            content=Content(
                role="model",
                parts=[Part(function_call=FunctionCall(id="c2", name="get_current_time", args={}))],
            ),
        )
        service, _, _ = _build_service(config=config, first_events=[call_part])
        events = asyncio.run(_collect(service.stream(task)))
        assert any(e.type == "tool" for e in events)
