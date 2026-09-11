"""Worker-side approval state machine (Stage 6A2).

Decision flow (approve/reject):

1. Stage 4C receipt claim on the DECISION message (platform redelivery of the
   same decision replays; in-progress/conflict keep 4C semantics);
2. identity-scoped CAS on the approval (tenant/channel/internal user/session);
   NOT_AVAILABLE never distinguishes missing vs cross-tenant;
3. EXECUTE: re-validate the CURRENT config (app id, config version, tool
   still allowed AND still ``review``) BEFORE anything runs; staleness or
   lookup failure finalizes the approval ``failed`` — no auto-switch, no
   cross-endpoint retry;
4. approve hands an ``execute`` callback to ``AgentApp.resume`` so the
   controlled execution (via ApprovedToolExecutor) and the resume turn share
   ONE per-session local lock + Redis lease (P0-1); reject passes the fixed
   verdict as ``tool_result``;
5. terminal state is written by a single ``finalize`` transaction covering
   approval + audit + decision receipt + message audit (P0-2). A DataError
   there is reported as APPROVAL_CONFLICT (never swallowed into success);
   the approval observably stays ``executing`` and every later claim returns
   IN_PROGRESS until a real terminal transition lands.

Also hosts ``make_pause_handler`` (first-request pause) so WorkerService
stays slim (Codex P2).
"""

from __future__ import annotations

import logging
import time
import uuid

from trpc_service.agent.app import AgentApp
from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.execution_coordinator import SessionBusyError, SessionExecutionLostError
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.config import ModelConfigurationError
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant_repository import (TenantConfigRepository, TenantRepositoryUnavailableError)
from trpc_service.governance.approval import (
    APPROVAL_REJECTED_RESULT,
    ApprovalAction,
    ApprovalRequest,
    pending_reply_for,
)
from trpc_service.storage.approval_repository import (
    ToolApprovalRepository,
    ToolApprovalRepositoryDataError,
    ToolApprovalRepositoryUnavailableError,
)
from trpc_service.storage.message_repository import (
    MessageReceiptRepository,
    MessageReceiptRepositoryDataError,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
)
from trpc_service.tenant.context import TenantContext
from trpc_service.transport.models import (
    WorkerApprovalResult,
    WorkerApprovalTask,
    WorkerErrorCode,
    WorkerTask,
)
from trpc_service.worker.governance import ContentGovernance, ExecutionRecorder

logger = logging.getLogger(__name__)

_ERROR_DEFAULT = WorkerErrorCode.MODEL_RUNTIME


def make_pause_handler(approval_repository: ToolApprovalRepository,
                       task: WorkerTask,
                       receipt_id: uuid.UUID,
                       started_monotonic: float,
                       review_pending_events=None):
    """First-request pause closure (kept out of WorkerService, Codex P2).

    Returns ``async (call_id, tool_name, args) -> (approval_id|None,
    WorkerErrorCode|None)``; on success persists the pending approval and
    completes the original receipt atomically.  ``review_pending_events`` is
    an optional callable ``(tool_name) -> tuple[ExecutionAuditEvent, ...]``
    (Stage 6B2): its events ride the SAME pause transaction as the receipt
    terminal, so a governance trail is never missing for a "completed".
    """

    async def _pause(call_id: str, tool_name: str, args: dict):
        approval_id = uuid.uuid4()
        pending = pending_reply_for(approval_id)
        latency_ms = int((time.monotonic() - started_monotonic) * 1000)
        execution_events = review_pending_events(tool_name) if review_pending_events is not None else ()
        try:
            await approval_repository.pause_first_request(
                approval_id=approval_id,
                tenant_id=task.tenant_id,
                app_id=task.app_id,
                config_version=task.config_version,
                channel=task.channel,
                user_id=task.user_id,
                session_id=task.session_id,
                receipt_id=receipt_id,
                function_call_id=call_id,
                tool_name=tool_name,
                tool_args=args,
                pending_response=pending,
                latency_ms=latency_ms,
                execution_events=execution_events,
            )
        except ToolApprovalRepositoryUnavailableError:
            logger.warning("worker approval repository unavailable on pause")
            return None, WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE
        except (ToolApprovalRepositoryDataError, ValueError):
            logger.warning("worker approval pause rejected")
            return None, WorkerErrorCode.MODEL_RUNTIME
        # sanitized lifecycle line: tenant/tool/state only (acceptance hook)
        logger.warning(
            "approval request created (tenant=%s, tool=%s, state=pending)",
            task.tenant_id,
            tool_name,
        )
        return approval_id, None

    return _pause


class ToolApprovalService:

    def __init__(
        self,
        *,
        tenant_repository: TenantConfigRepository,
        receipt_repository: MessageReceiptRepository,
        approval_repository: ToolApprovalRepository,
        agent_app: AgentApp,
        tool_registry: AllowedToolRegistry,
        usage_repository=None,
        pricing=None,
        telemetry=None,
    ) -> None:
        self._tenant_repository = tenant_repository
        self._receipt_repository = receipt_repository
        self._approval_repository = approval_repository
        self._agent_app = agent_app
        self._tool_registry = tool_registry
        # Stage 6C: the resume turn consumes the model too and must observe
        # the same budget gate and accounting as chat/stream (shared seam in
        # trpc_service.worker.governance — no logic drift between entrypoints).
        self._usage_repository = usage_repository
        self._pricing = pricing
        self._telemetry = telemetry

    def _metrics(self):
        from trpc_service.telemetry.metrics import NoopMetricsRecorder

        if self._telemetry is None:
            return NoopMetricsRecorder()
        try:
            return self._telemetry.metrics_recorder()
        except Exception:
            return NoopMetricsRecorder()

    async def decide(self, task: WorkerApprovalTask) -> WorkerApprovalResult:
        rid = task.request_id
        start = time.monotonic()

        def _elapsed_ms() -> int:
            return int((time.monotonic() - start) * 1000)

        async def _fail_receipt_only(error_code: WorkerErrorCode) -> None:
            try:
                await self._receipt_repository.fail(receipt_holder["receipt_id"], error_code, _elapsed_ms())
            except (MessageReceiptRepositoryUnavailableError, MessageReceiptRepositoryDataError):
                logger.warning("worker decision receipt fail failed")

        async def _finalize_failed(error_code: WorkerErrorCode) -> None:
            """One atomic terminal: approval executing -> failed + decision
            receipt processing -> failed. Best-effort; failures are logged and
            the state remains executing (fail-closed, never auto-reset)."""
            rec = _audit_holder.get("recorder")
            failure_events = ()
            if rec is not None:
                failure_events = rec.snapshot() + (rec.derive("agent_result", "error", error_code=error_code), )
            acc = _audit_holder.get("usage_acc")
            cfg = _audit_holder.get("config")
            if acc is not None and cfg is not None:
                from trpc_service.worker.governance import record_usage

                await record_usage(task, cfg, acc, self._usage_repository, self._pricing, self._metrics())
            self._metrics().record_counter(
                "trpc.requests",
                operation="decide",
                result="error",
                error_code=error_code.value,
            )
            try:
                await self._approval_repository.finalize(
                    task.approval_id,
                    receipt_id=receipt_holder["receipt_id"],
                    terminal_state="failed",
                    response_text=None,
                    error_code=error_code.value,
                    latency_ms=_elapsed_ms(),
                    execution_events=failure_events,
                )
            except Exception as exc:
                logger.warning(
                    "worker approval terminal-failed write incomplete type=%s",
                    type(exc).__name__,
                )

        # 1. decision-message receipt claim (4C idempotency)
        message_text = f"/{task.decision} {task.approval_id}"
        try:
            claim = await self._receipt_repository.claim(task, message_text)
        except MessageReceiptRepositoryUnavailableError:
            logger.warning("worker decision receipt repository unavailable")
            return WorkerApprovalResult(protocol_version=1,
                                        request_id=rid,
                                        response="",
                                        error_code=WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE)
        except MessageReceiptRepositoryDataError:
            logger.warning("worker decision receipt claim data error")
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=_ERROR_DEFAULT)

        if claim.action == ReceiptAction.REPLAY:
            return WorkerApprovalResult(
                protocol_version=1,
                request_id=rid,
                response=claim.response_text or "",
                error_code=claim.error_code,
            )
        if claim.action == ReceiptAction.IN_PROGRESS:
            return WorkerApprovalResult(protocol_version=1,
                                        request_id=rid,
                                        response="",
                                        error_code=WorkerErrorCode.MESSAGE_IN_PROGRESS)
        if claim.action == ReceiptAction.CONFLICT:
            return WorkerApprovalResult(protocol_version=1,
                                        request_id=rid,
                                        response="",
                                        error_code=WorkerErrorCode.IDEMPOTENCY_CONFLICT)

        receipt_holder = {"receipt_id": claim.receipt_id}
        # Stage 6B2: the recorder exists as soon as the decision receipt is
        # ours; pause/finalize transactions commit its events atomically.
        _audit_holder: dict = {"recorder": None, "usage_acc": None, "config": None}

        # 2. identity-scoped CAS on the approval
        try:
            decision_claim = await self._approval_repository.claim_decision(
                task.approval_id,
                tenant_id=task.tenant_id,
                channel=task.channel,
                user_id=task.user_id,
                session_id=task.session_id,
                decision=task.decision,
                decision_message_id=task.message_id,
            )
        except ToolApprovalRepositoryUnavailableError:
            logger.warning("worker approval repository unavailable")
            code = WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE
            await _fail_receipt_only(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except (ToolApprovalRepositoryDataError, ValueError):
            logger.warning("worker approval claim rejected")
            await _fail_receipt_only(_ERROR_DEFAULT)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=_ERROR_DEFAULT)

        if decision_claim.action == ApprovalAction.REPLAY:
            text = decision_claim.response_text or ""
            try:
                await self._receipt_repository.complete(receipt_holder["receipt_id"], text, _elapsed_ms())
            except (MessageReceiptRepositoryUnavailableError, MessageReceiptRepositoryDataError):
                logger.warning("worker decision receipt completion failed")
                return WorkerApprovalResult(protocol_version=1,
                                            request_id=rid,
                                            response="",
                                            error_code=WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response=text, error_code=None)

        if decision_claim.action != ApprovalAction.EXECUTE:
            code = {
                ApprovalAction.IN_PROGRESS: WorkerErrorCode.APPROVAL_IN_PROGRESS,
                ApprovalAction.CONFLICT: WorkerErrorCode.APPROVAL_CONFLICT,
                ApprovalAction.NOT_AVAILABLE: WorkerErrorCode.APPROVAL_NOT_AVAILABLE,
            }[decision_claim.action]
            await _fail_receipt_only(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)

        # 3. post-CAS lookups: ANY failure is fixed-mapped and finalized
        try:
            approval = await self._approval_repository.get(task.approval_id)
            get_version = getattr(self._tenant_repository, "get_version", None)
            config = (await get_version(task.tenant_id, task.config_version)
                      if get_version is not None else await self._tenant_repository.get(task.tenant_id))
        except (TenantRepositoryUnavailableError, ToolApprovalRepositoryUnavailableError):
            code = WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except Exception:
            logger.warning("worker approval post-claim lookup failed")
            code = WorkerErrorCode.APPROVAL_EXECUTION_FAILED
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)

        if approval is None:
            code = WorkerErrorCode.APPROVAL_EXECUTION_FAILED
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        if not self._config_still_matches(approval, config):
            code = WorkerErrorCode.APPROVAL_CONFIG_STALE
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        assert config is not None

        # Budget gate for the approved execution (same decision as chat/stream).
        from trpc_service.usage.models import UsageAccumulator
        from trpc_service.worker.governance import budget_block_reason

        budget_code = await budget_block_reason(task, config, self._usage_repository, self._pricing)
        if budget_code is not None:
            self._metrics().record_counter(
                "trpc.budget.rejections",
                operation="decide",
                result="rejected",
                error_code=budget_code.value,
            )
            await _finalize_failed(budget_code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=budget_code)

        usage_acc = UsageAccumulator()
        recorder = ExecutionRecorder(task, claim.receipt_id)
        _audit_holder["recorder"] = recorder
        _audit_holder["usage_acc"] = usage_acc
        _audit_holder["config"] = config
        governance = ContentGovernance(config.governance.content_policy)

        context = TenantContext(
            tenant_id=approval.tenant_id,
            app_id=approval.app_id,
            user_id=approval.user_id,
            channel=approval.channel,
            session_id=approval.session_id,
        )

        if task.decision == "reject":
            agen = self._agent_app.resume(
                config=config,
                context=context,
                session_id=approval.session_id,
                function_call_id=approval.function_call_id,
                tool_name=approval.tool_name,
                tool_result=dict(APPROVAL_REJECTED_RESULT),
            )
        else:
            # P1-3: tool args are read BEFORE the lease (restricted read);
            # any failure finalizes failed without ever calling the function.
            try:
                args = await self._approval_repository.get_tool_args(task.approval_id)
            except Exception:
                logger.warning("worker approval args lookup failed")
                code = WorkerErrorCode.APPROVAL_EXECUTION_FAILED
                await _finalize_failed(code)
                return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
            if args is None:
                code = WorkerErrorCode.APPROVAL_EXECUTION_FAILED
                await _finalize_failed(code)
                return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)

            # P0-1: execution happens INSIDE runtime.resume's lock+lease.
            async def _execute_in_lease() -> dict:
                try:
                    raw = await self._tool_registry.execute_approved(approval.tool_name, args)
                except Exception:
                    logger.warning("worker approved tool execution failed")
                    raise _ResumeFailedError(WorkerErrorCode.APPROVAL_EXECUTION_FAILED) from None
                return raw if isinstance(raw, dict) else {"output": str(raw)}

            agen = self._agent_app.resume(
                config=config,
                context=context,
                session_id=approval.session_id,
                function_call_id=approval.function_call_id,
                tool_name=approval.tool_name,
                execute=_execute_in_lease,
            )

        final_text = ""
        try:
            async for event in agen:
                usage_acc.add_event(getattr(event, "id", None), getattr(event, "usage_metadata", None))
                if event.error_code:
                    raise _ResumeFailedError(_ERROR_DEFAULT)
                if not event.content or not event.content.parts:
                    continue
                for part in event.content.parts:
                    # partials are superseded by the final accumulated event
                    if part.text and not event.partial:
                        final_text += part.text
        except _ResumeFailedError as exc:
            await _finalize_failed(exc.code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=exc.code)
        except TenantAgentConfigurationError:
            code = WorkerErrorCode.TENANT_AGENT_CONFIGURATION
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except ModelConfigurationError:
            code = WorkerErrorCode.MODEL_CONFIGURATION
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except SessionBusyError:
            code = WorkerErrorCode.SESSION_BUSY
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except SessionExecutionLostError:
            logger.warning("worker approval resume lost session execution")
            code = _ERROR_DEFAULT
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)
        except Exception:
            logger.warning("worker approval resume unexpected failure")
            code = _ERROR_DEFAULT
            await _finalize_failed(code)
            return WorkerApprovalResult(protocol_version=1, request_id=rid, response="", error_code=code)

        # 5. single atomic terminal transition (P0-2), Stage 6B2: the output
        # is checked before anything persists, and the decision receipt's
        # audit facts (tool outcome + output decision + agent result) ride
        # the SAME finalize transaction.
        terminal = "completed" if task.decision == "approve" else "rejected"
        latency_ms = _elapsed_ms()
        if governance.output_enforced:
            out_blocked, out_decision = governance.inspect_output(final_text)
            recorder.add(
                "content_decision",
                "blocked" if out_blocked else "allow",
                category=out_decision.category,
            )
            if out_blocked:
                final_text = governance.safe_output_text()
        recorder.add(
            "tool_decision",
            "allow" if task.decision == "approve" else "deny_blocked",
            tool_name=approval.tool_name,
        )
        recorder.add("agent_result", "success", latency_ms=latency_ms)
        try:
            await self._approval_repository.finalize(
                task.approval_id,
                receipt_id=receipt_holder["receipt_id"],
                terminal_state=terminal,
                response_text=final_text,
                error_code=None,
                latency_ms=latency_ms,
                execution_events=recorder.snapshot(),
            )
        except ToolApprovalRepositoryDataError:
            # concurrent terminal: never claim success
            logger.warning("worker approval finalize conflict")
            return WorkerApprovalResult(protocol_version=1,
                                        request_id=rid,
                                        response="",
                                        error_code=WorkerErrorCode.APPROVAL_CONFLICT)
        except ToolApprovalRepositoryUnavailableError:
            logger.warning("worker approval finalize unavailable")
            return WorkerApprovalResult(protocol_version=1,
                                        request_id=rid,
                                        response="",
                                        error_code=WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE)
        from trpc_service.worker.governance import record_usage

        await record_usage(task, config, usage_acc, self._usage_repository, self._pricing, self._metrics())
        self._metrics().record_counter("trpc.requests", operation="decide", result="ok")
        if task.decision == "approve":
            logger.warning(
                "approval executed (tenant=%s, tool=%s, decision=approve)",
                approval.tenant_id,
                approval.tool_name,
            )
        return WorkerApprovalResult(protocol_version=1, request_id=rid, response=final_text, error_code=None)

    @staticmethod
    def _config_still_matches(approval: ApprovalRequest, config: TenantConfig | None) -> bool:
        if config is None or not config.enabled:
            return False
        if config.app.app_id != approval.app_id:
            return False
        if config.version != approval.config_version:
            return False
        if approval.tool_name not in config.app.allowed_tools:
            return False
        return config.governance.tool_decisions.get(approval.tool_name) == "review"


class _ResumeFailedError(Exception):

    def __init__(self, code: WorkerErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


__all__ = ["ToolApprovalService", "make_pause_handler"]
