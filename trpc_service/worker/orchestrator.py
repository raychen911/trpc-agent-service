# mypy: disable-error-code="import-untyped"
"""End-to-end Worker orchestration for one durable Inbox claim."""

from __future__ import annotations

import asyncio
import math
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content

from trpc_service.agent import (
    AgentExecutionError,
    AgentFactory,
    ExecutionLimits,
    GovernanceViolationError,
    TenantAgentRunner,
    TurnResult,
)
from trpc_service.channels.contracts import ReplyIntent, ReplyKind
from trpc_service.metrics import METRICS
from trpc_service.reliability.types import (
    AuditData,
    ClaimInput,
    ReplyPart,
    SessionClaim,
    StaleClaimError,
    StaleVersionError,
)
from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tenant.models import AgentAppSpec, IdentityPolicy, TenantSpec
from trpc_service.worker.contracts import (
    EncryptedEventCodec,
    FailureClass,
    FailureClassifier,
    InboundMessageDecoder,
    ResolvedTenantTurn,
    TenantSpecLoader,
    TenantTurnResolver,
    TurnExecutor,
    TurnExecutorFactory,
    WorkerPort,
)
from trpc_service.worker.session import FencedSessionService, RuntimeSessionView

_PUBLIC_FAILURE_MESSAGE = "Agent 服务暂时不可用, 请稍后重试。"
_SYSTEM_RANDOM = secrets.SystemRandom()


class WorkerConfigurationError(ValueError):
    """A durable claim cannot be mapped to a published tenant configuration."""


class InboundPayloadError(ValueError):
    """The normalized Inbox payload cannot be passed safely to the Agent."""


class LeaseLostError(RuntimeError):
    """The current Worker no longer owns the session fencing capability."""


class WorkerOutcome(StrEnum):
    """Sanitized result of one polling iteration."""

    IDLE = "idle"
    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    REJECTED = "rejected"
    LOST_CLAIM = "lost_claim"


@dataclass(frozen=True, slots=True)
class WorkerRunResult:
    """Operationally useful result containing no prompt, reply, or secret data."""

    outcome: WorkerOutcome
    tenant_id: str
    worker_id: str
    run_id: str | None = None
    inbox_id: str | None = None
    attempt_no: int | None = None
    error_type: str | None = None


class ActiveTenantTurnResolver:
    """Resolve a claim against its pinned immutable tenant specification.

    The exact ``config_revision`` controls app and binding behavior across a later
    rollout or rollback.  Tenant-wide emergency suspension remains an independent
    runtime governance gate rather than silently changing the claimed revision.
    """

    def __init__(self, loader: TenantSpecLoader) -> None:
        self._loader = loader

    async def resolve(self, claim_input: ClaimInput) -> ResolvedTenantTurn:
        spec = await self._loader.load_revision(
            claim_input.tenant_id,
            claim_input.config_revision,
        )
        _validate_tenant_spec_identity(spec, claim_input)
        if spec.revision != claim_input.config_revision:
            raise WorkerConfigurationError("tenant loader returned the wrong revision")
        if spec.status != "active":
            raise WorkerConfigurationError("tenant is not active")

        app = next(
            (
                candidate
                for candidate in spec.apps
                if candidate.app_id == claim_input.app_id
                and candidate.revision == claim_input.app_revision
            ),
            None,
        )
        if app is None:
            raise WorkerConfigurationError("claimed app revision is not published")
        binding = next(
            (
                candidate
                for candidate in spec.channels
                if candidate.binding_id == claim_input.binding_id
            ),
            None,
        )
        if binding is None or not binding.enabled:
            raise WorkerConfigurationError("claimed channel binding is not active")
        if binding.app_id != app.app_id or binding.app_revision != app.revision:
            raise WorkerConfigurationError("binding and claimed app revision differ")
        try:
            scope = ConversationScope(claim_input.scope)
        except ValueError as exc:
            raise WorkerConfigurationError("claimed conversation scope is invalid") from exc
        _enforce_identity_policy(
            principal_id=claim_input.principal_id,
            scope=scope,
            policy=binding.identity_policy,
        )

        context = TenantContext(
            tenant_id=claim_input.tenant_id,
            app_id=claim_input.app_id,
            app_revision=claim_input.app_revision,
            binding_id=claim_input.binding_id,
            binding_revision=claim_input.config_revision,
            principal_id=claim_input.principal_id,
            session_id=claim_input.session_id,
            scope=scope,
            request_id=claim_input.request_id,
            trace_id=claim_input.trace_id,
        )
        return ResolvedTenantTurn(
            tenant_context=context,
            app=app,
            channel=binding.channel.value,
            config_revision=claim_input.config_revision,
            policy_revision=app.revision,
        )


class TextInboundMessageDecoder:
    """Decode the text member of a channel-normalized payload.

    Media-only messages need an attachment retrieval/scanning pipeline and must use
    another ``InboundMessageDecoder`` rather than passing an opaque locator to a model.
    """

    def decode(self, claim_input: ClaimInput) -> str:
        text = claim_input.payload.get("text")
        if not isinstance(text, str) or not text.strip():
            raise InboundPayloadError("normalized payload has no supported text content")
        return text


class DefaultFailureClassifier:
    """Fail closed on identity/config errors and retry bounded runtime failures."""

    def classify(self, error: Exception) -> FailureClass:
        if isinstance(error, (StaleClaimError, StaleVersionError, LeaseLostError)):
            return FailureClass.LOST_CLAIM
        if isinstance(
            error,
            (WorkerConfigurationError, InboundPayloadError, GovernanceViolationError),
        ):
            return FailureClass.PERMANENT
        if isinstance(error, (AgentExecutionError, TimeoutError, ConnectionError)):
            return FailureClass.RETRYABLE
        # Unknown implementation/provider failures are retried only up to the
        # orchestrator's finite attempt budget; their message is never persisted.
        return FailureClass.RETRYABLE


class TenantAgentExecutorFactory:
    """Production adapter that creates a real ``TenantAgentRunner`` per turn.

    ``AgentFactory.build_for_context`` is invoked once to obtain the exact opaque SDK
    app namespace before SessionService construction.  The runner intentionally builds
    a fresh graph again when execution starts; model resolvers should therefore return
    lightweight/cached clients and must remain side-effect free.
    """

    def __init__(
        self,
        *,
        agent_factory: AgentFactory,
        memory_service: BaseMemoryService | None = None,
        limits: ExecutionLimits | None = None,
    ) -> None:
        self._agent_factory = agent_factory
        self._memory_service = memory_service
        self._limits = limits

    def app_name_for(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        approved_tools: frozenset[str],
    ) -> str:
        return self._agent_factory.build_for_context(
            tenant_context=tenant_context,
            app=app,
            approved_tools=approved_tools,
        ).app_name

    def create(
        self,
        *,
        session_service: BaseSessionService,
    ) -> TurnExecutor:
        return TenantAgentRunner(
            agent_factory=self._agent_factory,
            session_service=session_service,
            memory_service=self._memory_service,
            limits=self._limits,
        )


class WorkerOrchestrator:
    """Connect claim, replay, Runner execution, CAS events, and atomic finalization."""

    def __init__(
        self,
        *,
        port: WorkerPort,
        event_codec: EncryptedEventCodec,
        tenant_resolver: TenantTurnResolver,
        executor_factory: TurnExecutorFactory,
        message_decoder: InboundMessageDecoder | None = None,
        failure_classifier: FailureClassifier | None = None,
        lease_ttl: timedelta = timedelta(seconds=30),
        heartbeat_interval: timedelta = timedelta(seconds=10),
        max_attempts: int = 3,
        retry_base_delay: timedelta = timedelta(seconds=1),
        retry_max_delay: timedelta = timedelta(minutes=1),
        clock: Callable[[], datetime] | None = None,
        jitter: Callable[[float, float], float] | None = None,
        public_failure_message: str = _PUBLIC_FAILURE_MESSAGE,
    ) -> None:
        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        if heartbeat_interval <= timedelta(0) or heartbeat_interval >= lease_ttl:
            raise ValueError("heartbeat_interval must be positive and shorter than lease_ttl")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if retry_base_delay <= timedelta(0):
            raise ValueError("retry_base_delay must be positive")
        if retry_max_delay < retry_base_delay:
            raise ValueError("retry_max_delay must not be shorter than retry_base_delay")
        if not public_failure_message.strip():
            raise ValueError("public_failure_message must not be empty")
        self._port = port
        self._event_codec = event_codec
        self._tenant_resolver = tenant_resolver
        self._executor_factory = executor_factory
        self._message_decoder = message_decoder or TextInboundMessageDecoder()
        self._failure_classifier = failure_classifier or DefaultFailureClassifier()
        self._lease_ttl = lease_ttl
        self._heartbeat_interval = heartbeat_interval
        self._max_attempts = max_attempts
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay
        self._clock = clock or (lambda: datetime.now(UTC))
        self._jitter = jitter or _SYSTEM_RANDOM.uniform
        self._public_failure_message = public_failure_message.strip()

    async def run_once(self, *, tenant_id: str, worker_id: str) -> WorkerRunResult:
        """Claim and process at most one Inbox item for a tenant."""

        if not tenant_id or not worker_id:
            raise ValueError("tenant_id and worker_id must not be empty")
        try:
            claim = await self._port.claim_next(
                tenant_id,
                worker_id,
                lease_ttl=self._lease_ttl,
            )
        except Exception as error:
            return WorkerRunResult(
                WorkerOutcome.RETRY_WAIT,
                tenant_id,
                worker_id,
                error_type=type(error).__name__,
            )
        if claim is None:
            return WorkerRunResult(WorkerOutcome.IDLE, tenant_id, worker_id)

        METRICS.session_leases.labels(worker_id).inc()
        started = time.monotonic()
        claim_input: ClaimInput | None = None
        committed = None
        service: FencedSessionService | None = None
        resolved: ResolvedTenantTurn | None = None
        try:
            claim_input = await self._port.load_claim_input(claim)
            _validate_claim_input(claim, claim_input)
            committed = await self._port.load_committed_session(claim)
            resolved = await self._tenant_resolver.resolve(claim_input)
            _validate_resolved_turn(claim_input, resolved)
            app_name = self._executor_factory.app_name_for(
                tenant_context=resolved.tenant_context,
                app=resolved.app,
                approved_tools=resolved.approved_tools,
            )
            runtime_view = RuntimeSessionView.from_committed(
                committed,
                app_name=app_name,
                user_id=claim_input.principal_id,
                app_state=resolved.app_state,
                user_state=resolved.user_state,
            )
            service = FencedSessionService(
                port=self._port,
                codec=self._event_codec,
                claim=claim,
                view=runtime_view,
            )
            executor = self._executor_factory.create(session_service=service)
            new_message = self._message_decoder.decode(claim_input)
            result = await self._run_with_heartbeat(
                executor,
                claim=claim,
                claim_input=claim_input,
                resolved=resolved,
                new_message=new_message,
            )
            if not await self._port.renew_lease(claim, lease_ttl=self._lease_ttl):
                raise LeaseLostError("lease was lost before finalization")
            await self._finalize_success(
                claim,
                claim_input=claim_input,
                resolved=resolved,
                service=service,
                result=result,
                latency_ms=_elapsed_ms(started),
            )
            return _result(WorkerOutcome.SUCCEEDED, claim)
        except asyncio.CancelledError:
            await _abort_on_cancellation(self._port, claim)
            raise
        except Exception as error:
            try:
                classification = self._failure_classifier.classify(error)
            except Exception:
                classification = FailureClass.RETRYABLE
            if classification is FailureClass.LOST_CLAIM:
                return _result(
                    WorkerOutcome.LOST_CLAIM,
                    claim,
                    error_type=type(error).__name__,
                )

            permanent = (
                classification is FailureClass.PERMANENT or claim.attempt_no >= self._max_attempts
            )
            if not permanent or claim_input is None or committed is None:
                try:
                    await self._defer_retry(claim, error_type=type(error).__name__)
                except (StaleClaimError, StaleVersionError):
                    return _result(
                        WorkerOutcome.LOST_CLAIM,
                        claim,
                        error_type="StaleClaimError",
                    )
                except Exception as defer_error:
                    return _result(
                        WorkerOutcome.RETRY_WAIT,
                        claim,
                        error_type=type(defer_error).__name__,
                    )
                return _result(
                    WorkerOutcome.RETRY_WAIT,
                    claim,
                    error_type=type(error).__name__,
                )

            abort_succeeded = await _abort_best_effort(self._port, claim)
            if not abort_succeeded:
                return _result(
                    WorkerOutcome.RETRY_WAIT,
                    claim,
                    error_type=type(error).__name__,
                )

            try:
                await self._finalize_rejection(
                    claim,
                    claim_input=claim_input,
                    committed_state=committed.state,
                    resolved=resolved,
                    error_type=type(error).__name__,
                    latency_ms=_elapsed_ms(started),
                )
            except (StaleClaimError, StaleVersionError):
                return _result(
                    WorkerOutcome.LOST_CLAIM,
                    claim,
                    error_type="StaleClaimError",
                )
            except Exception as finalize_error:
                return _result(
                    WorkerOutcome.RETRY_WAIT,
                    claim,
                    error_type=type(finalize_error).__name__,
                )
            return _result(
                WorkerOutcome.REJECTED,
                claim,
                error_type=type(error).__name__,
            )
        finally:
            if service is not None:
                await service.close()
            METRICS.session_leases.labels(worker_id).dec()

    async def _defer_retry(self, claim: SessionClaim, *, error_type: str) -> None:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("retry clock must return a timezone-aware datetime")
        exponent = min(max(0, claim.attempt_no - 1), 30)
        cap_seconds = min(
            self._retry_max_delay.total_seconds(),
            self._retry_base_delay.total_seconds() * (2**exponent),
        )
        lower = cap_seconds / 2
        sampled = self._jitter(lower, cap_seconds)
        if not math.isfinite(sampled):
            sampled = cap_seconds
        delay_seconds = min(cap_seconds, max(lower, sampled))
        await self._port.defer_run_retry(
            claim,
            next_attempt_at=now.astimezone(UTC) + timedelta(seconds=delay_seconds),
            error_type=error_type[:128],
        )

    async def _run_with_heartbeat(
        self,
        executor: TurnExecutor,
        *,
        claim: SessionClaim,
        claim_input: ClaimInput,
        resolved: ResolvedTenantTurn,
        new_message: str | Content | list[Content],
    ) -> TurnResult:
        stop = asyncio.Event()
        turn_task = asyncio.create_task(
            executor.run_turn(
                tenant_context=resolved.tenant_context,
                app=resolved.app,
                new_message=new_message,
                run_id=claim.run_id,
                in_reply_to_delivery_id=_delivery_id(claim_input),
                attempt_no=claim.attempt_no,
                approved_tools=resolved.approved_tools,
            )
        )
        heartbeat_task = asyncio.create_task(self._heartbeat(claim, stop=stop))
        try:
            done, _ = await asyncio.wait(
                {turn_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                heartbeat_error = heartbeat_task.exception()
                if heartbeat_error is not None:
                    turn_task.cancel()
                    await asyncio.gather(turn_task, return_exceptions=True)
                    raise heartbeat_error
            if turn_task not in done:
                # The heartbeat exits normally only after ``stop`` is set, which
                # cannot happen here. Treat any unexpected early exit as fail-closed.
                turn_task.cancel()
                await asyncio.gather(turn_task, return_exceptions=True)
                raise LeaseLostError("lease heartbeat stopped before Agent completion")
            return await turn_task
        finally:
            stop.set()
            if not turn_task.done():
                turn_task.cancel()
                await asyncio.gather(turn_task, return_exceptions=True)
            if not heartbeat_task.done():
                await heartbeat_task

    async def _heartbeat(self, claim: SessionClaim, *, stop: asyncio.Event) -> None:
        interval = self._heartbeat_interval.total_seconds()
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                if not await self._port.renew_lease(claim, lease_ttl=self._lease_ttl):
                    raise LeaseLostError("session lease heartbeat lost its fence") from None

    async def _finalize_success(
        self,
        claim: SessionClaim,
        *,
        claim_input: ClaimInput,
        resolved: ResolvedTenantTurn,
        service: FencedSessionService,
        result: TurnResult,
        latency_ms: int,
    ) -> None:
        reply_part = _reply_part(result.reply_intent)
        audit = AuditData(
            channel=resolved.channel,
            user_id=claim_input.principal_id,
            agent_name=resolved.app.name,
            decision="degraded" if result.sdk_error else "allow",
            action="agent.turn.finalize",
            resource=f"agent:{resolved.app.app_id}@{resolved.app.revision}",
            config_revision=resolved.config_revision,
            policy_revision=resolved.policy_revision,
            latency_ms=latency_ms,
            error_type="sdk_error" if result.sdk_error else None,
            idempotency_key=claim.run_id,
            detail={"attempt_no": claim.attempt_no, "reply_kind": result.reply_intent.kind.value},
        )
        await self._port.finalize_run(
            claim,
            final_state=service.final_session_state,
            final_event_id=service.last_event_id,
            reply_parts=(reply_part,),
            audit=audit,
        )

    async def _finalize_rejection(
        self,
        claim: SessionClaim,
        *,
        claim_input: ClaimInput,
        committed_state: dict[str, Any],
        resolved: ResolvedTenantTurn | None,
        error_type: str,
        latency_ms: int,
    ) -> None:
        intent = ReplyIntent(
            intent_id=f"{claim.run_id}:reply:error",
            tenant_id=claim.tenant_id,
            binding_id=claim_input.binding_id,
            session_id=claim.session_id,
            run_id=claim.run_id,
            in_reply_to_delivery_id=_delivery_id(claim_input),
            kind=ReplyKind.ERROR,
            text=self._public_failure_message,
            final=True,
            idempotency_key=f"{claim.run_id}:reply:error",
        )
        app_name = resolved.app.name if resolved is not None else "unresolved_agent"
        app_id = resolved.app.app_id if resolved is not None else claim_input.app_id
        app_revision = resolved.app.revision if resolved is not None else claim_input.app_revision
        audit = AuditData(
            channel=resolved.channel if resolved is not None else "unknown",
            user_id=claim_input.principal_id,
            agent_name=app_name,
            decision="deny",
            action="agent.turn.finalize",
            resource=f"agent:{app_id}@{app_revision}",
            config_revision=(
                resolved.config_revision if resolved is not None else claim_input.config_revision
            ),
            policy_revision=(
                resolved.policy_revision if resolved is not None else claim_input.app_revision
            ),
            reason="permanent_execution_failure",
            latency_ms=latency_ms,
            error_type=error_type,
            idempotency_key=claim.run_id,
            detail={"attempt_no": claim.attempt_no, "reply_kind": "error"},
        )
        await self._port.finalize_run(
            claim,
            final_state=_detached_object(committed_state),
            final_event_id=None,
            reply_parts=(_reply_part(intent),),
            audit=audit,
        )


def _validate_tenant_spec_identity(spec: TenantSpec, claim_input: ClaimInput) -> None:
    if spec.tenant_id != claim_input.tenant_id:
        raise WorkerConfigurationError("tenant loader returned a cross-tenant spec")


def _enforce_identity_policy(
    *,
    principal_id: str,
    scope: ConversationScope,
    policy: IdentityPolicy,
) -> None:
    if scope.value not in policy.allowed_scopes:
        raise WorkerConfigurationError("conversation scope is denied by channel policy")
    if principal_id in policy.deny_principals:
        raise WorkerConfigurationError("principal is denied by channel policy")
    if policy.default_action == "deny" and principal_id not in policy.allow_principals:
        raise WorkerConfigurationError("principal is not allowed by channel policy")


def _validate_claim_input(claim: SessionClaim, value: ClaimInput) -> None:
    if (
        value.tenant_id != claim.tenant_id
        or value.session_id != claim.session_id
        or value.inbox_id != claim.inbox_id
        or value.run_id != claim.run_id
        or value.attempt_no != claim.attempt_no
        or value.fencing_token != claim.fencing_token
    ):
        raise StaleClaimError("loaded Inbox input does not match the supplied claim")


def _validate_resolved_turn(
    claim_input: ClaimInput,
    resolved: ResolvedTenantTurn,
) -> None:
    context = resolved.tenant_context
    if (
        context.tenant_id != claim_input.tenant_id
        or context.app_id != claim_input.app_id
        or context.app_revision != claim_input.app_revision
        or context.binding_id != claim_input.binding_id
        or context.binding_revision != claim_input.config_revision
        or context.principal_id != claim_input.principal_id
        or context.session_id != claim_input.session_id
        or context.request_id != claim_input.request_id
        or context.trace_id != claim_input.trace_id
        or resolved.app.app_id != claim_input.app_id
        or resolved.app.revision != claim_input.app_revision
        or resolved.config_revision != claim_input.config_revision
    ):
        raise WorkerConfigurationError("tenant resolver returned a cross-claim identity")


def _reply_part(intent: ReplyIntent) -> ReplyPart:
    if intent.text is None or not intent.text:
        raise InboundPayloadError("the current Outbox contract supports text replies only")
    return ReplyPart(
        reply_id=intent.intent_id,
        part_no=0,
        payload={"schema_version": 1, "kind": "text", "text": intent.text},
    )


def _delivery_id(claim_input: ClaimInput) -> str:
    if not claim_input.external_delivery_id:
        raise InboundPayloadError("claimed external delivery id is empty")
    return claim_input.external_delivery_id


def _detached_object(value: dict[str, Any]) -> dict[str, Any]:
    import json

    decoded = json.loads(
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    )
    if not isinstance(decoded, dict):  # pragma: no cover - static type plus JSON round-trip
        raise WorkerConfigurationError("committed session state is invalid")
    return decoded


async def _abort_best_effort(port: WorkerPort, claim: SessionClaim) -> bool:
    try:
        await port.abort_staged_events(claim)
    except (StaleClaimError, StaleVersionError):
        return False
    except Exception:
        # Preserve the original sanitized failure class.  A later lease takeover
        # will abort this attempt before replay; never finalize a rejection unless
        # this attempt's staged events were confirmed aborted.
        return False
    return True


async def _abort_on_cancellation(port: WorkerPort, claim: SessionClaim) -> None:
    task = asyncio.create_task(_abort_best_effort(port, claim))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        # The task remains alive long enough to issue the fence-checked abort.  Never
        # convert caller cancellation into an apparent successful Worker outcome.
        await task


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1_000))


def _result(
    outcome: WorkerOutcome,
    claim: SessionClaim,
    *,
    error_type: str | None = None,
) -> WorkerRunResult:
    return WorkerRunResult(
        outcome=outcome,
        tenant_id=claim.tenant_id,
        worker_id=claim.worker_id,
        run_id=claim.run_id,
        inbox_id=claim.inbox_id,
        attempt_no=claim.attempt_no,
        error_type=error_type,
    )
