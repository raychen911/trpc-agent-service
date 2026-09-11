"""Gateway channel ingress service: admission + WorkerTask construction."""

from __future__ import annotations

import logging
import uuid
from contextlib import aclosing
from dataclasses import dataclass
from typing import AsyncIterator

from trpc_service.channels.approval_commands import parse_approval_command
from trpc_service.channels.identity import ChannelIdentity, project_identity
from trpc_service.channels.delivery import ChannelExecutionStream
from trpc_service.channels.ingress import ChannelIngress
from trpc_service.channels.models import InboundMessage, PublicChannelEvent
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.rollout import select_config_version
from trpc_service.config.tenant_repository import (
    TenantConfigRepository,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)
from trpc_service.gateway.client import WorkerClient, WorkerClientError
from trpc_service.gateway.errors import (
    ACCESS_DENIED_TEXT,
    RATE_LIMITED_TEXT,
    SAFE_ERROR_TEXT,
    TENANT_SERVICE_UNAVAILABLE_TEXT,
    map_worker_error,
)
from trpc_service.governance.limits import RateLimiterUnavailableError, TenantRateLimiter
from trpc_service.audit.models import delivery_event_factory
from trpc_service.storage.execution_audit_repository import ExecutionAuditRepository
from trpc_service.transport.models import (
    WorkerApprovalTask,
    WorkerErrorCode,
    WorkerTask,
)

logger = logging.getLogger(__name__)


class ChannelTenantFormatError(Exception):
    """Raised when tenant ID format is invalid."""


class ChannelTenantNotFoundError(Exception):
    """Raised when tenant is unknown or disabled."""


class ChannelTenantUnavailableError(Exception):
    """Raised when tenant repository is unavailable."""


class ChannelAccessDeniedError(Exception):
    """Governance admission denied (Stage 6A1).

    Carries only the fixed public text; never distinguishes channel-vs-user
    denial and never includes raw identifiers.
    """

    def __init__(self) -> None:
        super().__init__(ACCESS_DENIED_TEXT)


@dataclass(frozen=True, slots=True)
class ChannelReply:
    response: str


class ChannelIngressService(ChannelIngress):
    """Gateway implementation of the channels-layer ``ChannelIngress``
    contract (see ``trpc_service.channels.ingress``): admission, rate
    gating, rollout-pinned config choice and Worker routing."""

    def __init__(
        self,
        tenant_repository: TenantConfigRepository,
        worker_client: WorkerClient,
        execution_repository: ExecutionAuditRepository | None = None,
        rate_limiter: TenantRateLimiter | None = None,
        telemetry=None,
        rollout_repository=None,
    ) -> None:
        self._tenant_repository = tenant_repository
        self._worker_client = worker_client
        # Stage 6B2: delivery facts occur AFTER the message reached its
        # terminal state, so they go through the standalone append channel
        # (receipt_id NULL, queryable by (tenant_id, request_id)).
        self._execution_repository = execution_repository
        # Stage 6C: atomic fixed-window rate limiting (Redis-backed).  A
        # tenant WITH limits and NO reachable limiter fails closed — never
        # unlimited fall-through.
        self._rate_limiter = rate_limiter
        self._telemetry = telemetry
        self._rollout_repository = rollout_repository

    def _metrics(self):
        from trpc_service.telemetry.metrics import NoopMetricsRecorder

        if self._telemetry is None:
            return NoopMetricsRecorder()
        try:
            return self._telemetry.metrics_recorder()
        except Exception:
            return NoopMetricsRecorder()

    async def _rate_gate(self, config: TenantConfig, operation: str) -> str | None:
        """Returns fixed public rejection text when the request must not
        reach the Worker, else ``None``."""
        limits = config.governance.limits
        if limits is None:
            return None
        metrics = self._metrics()
        if self._rate_limiter is None:
            metrics.record_counter("trpc.rate_limit.rejections", operation=operation, result="unavailable")
            return TENANT_SERVICE_UNAVAILABLE_TEXT
        try:
            allowed = await self._rate_limiter.acquire(config.tenant_id, limits.requests_per_minute)
        except RateLimiterUnavailableError:
            metrics.record_counter("trpc.rate_limit.rejections", operation=operation, result="unavailable")
            return TENANT_SERVICE_UNAVAILABLE_TEXT
        except Exception:
            # The limiter's own error taxonomy is fixed; anything unexpected
            # still fails closed with the same public outcome.
            metrics.record_counter("trpc.rate_limit.rejections", operation=operation, result="unavailable")
            return TENANT_SERVICE_UNAVAILABLE_TEXT
        if not allowed:
            metrics.record_counter("trpc.rate_limit.rejections", operation=operation, result="limited")
            return RATE_LIMITED_TEXT
        return None

    async def _record_delivery(
        self,
        task: WorkerTask | WorkerApprovalTask,
        error_code: WorkerErrorCode | None,
    ) -> None:
        # External IM delivery is recorded only by its SDK terminal writer.
        # Console retains the historical ingress-boundary behavior.
        if task.channel in {"wecom", "feishu"}:
            return
        self._metrics().record_counter(
            "trpc.delivery",
            operation="delivery",
            result="failed" if error_code is not None else "delivered",
            error_code=error_code.value if error_code is not None else None,
        )
        if self._execution_repository is None:
            return
        try:
            await self._execution_repository.append(delivery_event_factory(task)(error_code))
        except Exception:
            # Audit-append failure must never alter or delay the user reply;
            # fixed log line, no payload (identity already lives in the row).
            logger.warning("channel delivery audit append failed")

    async def record_external_delivery(
        self,
        execution: ChannelExecutionStream,
        error_code: WorkerErrorCode | None,
    ) -> None:
        """Append exactly one IM SDK-terminal delivery fact when configured.

        Audit failures are deliberately isolated from the reply chain: they
        cannot cause a resend or a second Worker execution.
        """
        task = execution.task
        if task is None:
            return
        self._metrics().record_counter(
            "trpc.delivery",
            operation="delivery",
            result="failed" if error_code is not None else "delivered",
            error_code=error_code.value if error_code is not None else None,
        )
        if self._execution_repository is None:
            return
        if error_code is None and execution.delivery_events == "failures":
            return
        try:
            await self._execution_repository.append(delivery_event_factory(task)(error_code))
        except Exception:
            logger.warning("channel delivery audit append failed")

    async def _admit(self, inbound: InboundMessage) -> tuple[TenantConfig, ChannelIdentity]:
        """One repository query -> identity projection -> governance verdict.

        Returns the config and projected identity only when the request is
        admitted.  Ordering per Stage 6A1 design: never query config twice.
        """
        from trpc_service.tenant.context import InvalidTenantIdError, validate_tenant_id

        try:
            validate_tenant_id(inbound.tenant_id)
        except InvalidTenantIdError:
            raise ChannelTenantFormatError("Invalid tenant ID format.")

        try:
            config = await self._tenant_repository.get(inbound.tenant_id)
        except (TenantRepositoryUnavailableError, TenantRepositoryDataError):
            raise ChannelTenantUnavailableError(TENANT_SERVICE_UNAVAILABLE_TEXT)

        if config is None or not config.enabled:
            raise ChannelTenantNotFoundError("Tenant is not available.")
        # R3B: the Gateway makes exactly one deterministic choice before the
        # request gets a receipt. Workers then read that immutable version.
        if self._rollout_repository is not None:
            try:
                rollout = await self._rollout_repository.get_running(inbound.tenant_id)
                if rollout is not None:
                    version = select_config_version(inbound.tenant_id,
                                                    inbound.channel,
                                                    inbound.external_message_id,
                                                    active_version=rollout.active_version,
                                                    candidate_version=rollout.candidate_version,
                                                    candidate_percent=rollout.candidate_percent)
                    config = await self._tenant_repository.get_version(inbound.tenant_id, version)
            except (TenantRepositoryUnavailableError, TenantRepositoryDataError):
                raise ChannelTenantUnavailableError(TENANT_SERVICE_UNAVAILABLE_TEXT)
            except Exception:
                raise ChannelTenantUnavailableError(TENANT_SERVICE_UNAVAILABLE_TEXT)
            if config is None or not config.enabled:
                raise ChannelTenantUnavailableError(TENANT_SERVICE_UNAVAILABLE_TEXT)
        if inbound.app_id is not None and inbound.app_id != config.app.app_id:
            raise ChannelAccessDeniedError()

        identity = project_identity(
            inbound.channel,
            inbound.external_user_id,
            inbound.external_conversation_id,
            binding_id=inbound.binding_id,
            conversation_kind=inbound.conversation_kind,
        )

        governance = config.governance
        channel_allowed = inbound.channel in governance.allowed_channels
        user_allowed = (not governance.allowed_user_ids) or (identity.user_id in governance.allowed_user_ids)
        if not (channel_allowed and user_allowed):
            # tenant_id/channel are internal identifiers; never log the
            # projected user, raw IDs, message content or denial subtype.
            logger.info(
                "channel ingress denied (tenant=%s, channel=%s, verdict=governance)",
                inbound.tenant_id,
                inbound.channel,
            )
            raise ChannelAccessDeniedError()

        return config, identity

    def _build_task(
        self,
        inbound: InboundMessage,
        config: TenantConfig,
        identity: ChannelIdentity,
    ) -> WorkerTask:
        return WorkerTask(
            protocol_version=1,
            request_id=uuid.uuid4(),
            tenant_id=inbound.tenant_id,
            app_id=config.app.app_id,
            config_version=config.version,
            user_id=identity.user_id,
            channel=inbound.channel,
            session_id=identity.session_id,
            message_id=inbound.external_message_id,
            message=inbound.text,
        )

    def _build_approval_task(
        self,
        inbound: InboundMessage,
        config: TenantConfig,
        identity: ChannelIdentity,
        command,
    ) -> WorkerApprovalTask:
        decision, approval_id = command
        return WorkerApprovalTask(
            protocol_version=1,
            request_id=uuid.uuid4(),
            tenant_id=inbound.tenant_id,
            app_id=config.app.app_id,
            config_version=config.version,
            user_id=identity.user_id,
            channel=inbound.channel,
            session_id=identity.session_id,
            message_id=inbound.external_message_id,
            approval_id=approval_id,
            decision=decision,
        )

    async def _decide_events(
        self,
        inbound: InboundMessage,
        config: TenantConfig,
        identity: ChannelIdentity,
        command,
        execution: ChannelExecutionStream | None = None,
    ) -> AsyncIterator[PublicChannelEvent]:
        """One decide round-trip: exactly one Worker call, no retry."""
        task = self._build_approval_task(inbound, config, identity, command)
        if execution is not None:
            execution.task = task
            execution.delivery_events = config.audit_policy.delivery_events
        try:
            result = await self._worker_client.decide(task)
        except WorkerClientError as exc:
            await self._record_delivery(task, exc.code)
            yield PublicChannelEvent(type="error", data=map_worker_error(exc.code))
            return
        except Exception:
            yield PublicChannelEvent(type="error", data=SAFE_ERROR_TEXT)
            return
        if result.error_code is not None:
            await self._record_delivery(task, result.error_code)
            yield PublicChannelEvent(type="error", data=map_worker_error(result.error_code))
            return
        await self._record_delivery(task, None)
        yield PublicChannelEvent(type="delta", data=result.response)
        yield PublicChannelEvent(type="done", data=None)

    async def chat(self, inbound: InboundMessage) -> ChannelReply:
        try:
            config, identity = await self._admit(inbound)
        except (ChannelTenantFormatError, ChannelTenantNotFoundError, ChannelTenantUnavailableError,
                ChannelAccessDeniedError):
            raise
        except Exception:
            raise ChannelTenantUnavailableError(TENANT_SERVICE_UNAVAILABLE_TEXT)

        rejection = await self._rate_gate(config, "chat")
        if rejection is not None:
            return ChannelReply(response=rejection)

        command = parse_approval_command(inbound.text)
        if command is not None:
            text = ""
            # aclosing: early return must still close the inner decide stream
            # (its worker CLIENT span ends promptly, Stage 6B1 rule).
            async with aclosing(self._decide_events(inbound, config, identity, command)) as events:
                async for event in events:
                    if event.type == "error":
                        return ChannelReply(response=str(event.data))
                    if event.type == "delta" and isinstance(event.data, str):
                        text = event.data
            return ChannelReply(response=text)

        task = self._build_task(inbound, config, identity)
        try:
            result = await self._worker_client.chat(task)
        except WorkerClientError as exc:
            await self._record_delivery(task, exc.code)
            return ChannelReply(response=map_worker_error(exc.code))
        except Exception:
            return ChannelReply(response=SAFE_ERROR_TEXT)

        if result.error_code is not None:
            await self._record_delivery(task, result.error_code)
            return ChannelReply(response=map_worker_error(result.error_code))
        await self._record_delivery(task, None)
        return ChannelReply(response=result.response)

    def stream(self, inbound: InboundMessage) -> ChannelExecutionStream:
        """Return public events plus internal task identity for IM delivery.

        Existing callers still consume this as an async iterator.  The extra
        identity is not serialized and is only used by bound IM services.
        """
        holder: ChannelExecutionStream

        async def _events() -> AsyncIterator[PublicChannelEvent]:
            async for event in self._stream_events(inbound, holder):
                yield event

        holder = ChannelExecutionStream(_events())
        return holder

    async def _stream_events(
        self,
        inbound: InboundMessage,
        execution: ChannelExecutionStream,
    ) -> AsyncIterator[PublicChannelEvent]:
        try:
            config, identity = await self._admit(inbound)
        except ChannelTenantFormatError:
            yield PublicChannelEvent(type="error", data="Invalid tenant ID format.")
            return
        except ChannelTenantNotFoundError:
            yield PublicChannelEvent(type="error", data="Tenant is not available.")
            return
        except ChannelAccessDeniedError:
            yield PublicChannelEvent(type="error", data=ACCESS_DENIED_TEXT)
            return
        except ChannelTenantUnavailableError as exc:
            yield PublicChannelEvent(type="error", data=str(exc))
            return
        except Exception:
            yield PublicChannelEvent(type="error", data=TENANT_SERVICE_UNAVAILABLE_TEXT)
            return

        rejection = await self._rate_gate(config, "stream")
        if rejection is not None:
            yield PublicChannelEvent(type="error", data=rejection)
            return

        command = parse_approval_command(inbound.text)
        if command is not None:
            async with aclosing(self._decide_events(inbound, config, identity, command, execution)) as events:
                async for event in events:
                    yield event
            return

        task = self._build_task(inbound, config, identity)
        execution.task = task
        execution.delivery_events = config.audit_policy.delivery_events
        try:
            # aclosing: consumer disconnect (or the done/error early returns
            # below) closes the worker SSE stream so its CLIENT span ends now,
            # not at GC time (Stage 6B1 rule).
            stream = self._worker_client.stream(task)
            execution.set_nested_iterator(stream)
            async with aclosing(stream):
                async for event in stream:
                    if event.type == "delta":
                        yield PublicChannelEvent(type="delta", data=event.data)
                    elif event.type == "approval":
                        yield PublicChannelEvent(
                            type="approval",
                            data={
                                "approval_id": str(event.data.approval_id),
                                "tool_name": event.data.tool_name,
                            },
                        )
                    elif event.type == "tool":
                        yield PublicChannelEvent(
                            type="tool",
                            data={
                                "kind":
                                event.data.kind,
                                "name":
                                event.data.name,
                                "args" if event.data.kind == "call" else "response":
                                (event.data.args if event.data.kind == "call" else event.data.response),
                            })
                    elif event.type == "done":
                        await self._record_delivery(task, None)
                        yield PublicChannelEvent(type="done", data=None)
                        return
                    elif event.type == "error":
                        await self._record_delivery(task, event.error_code)
                        yield PublicChannelEvent(type="error", data=map_worker_error(event.error_code))
                        return
            execution.set_nested_iterator(None)
        except WorkerClientError as exc:
            await self._record_delivery(task, exc.code)
            yield PublicChannelEvent(type="error", data=map_worker_error(exc.code))
        except Exception:
            yield PublicChannelEvent(type="error", data=SAFE_ERROR_TEXT)


__all__ = [
    "ChannelAccessDeniedError",
    "ChannelIngressService",
    "ChannelReply",
    "ChannelTenantFormatError",
    "ChannelTenantNotFoundError",
    "ChannelTenantUnavailableError",
]
