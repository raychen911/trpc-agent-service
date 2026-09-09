# mypy: disable-error-code="import-untyped"
"""Strict test doubles for claim-bound Worker tests."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.runtime import TurnResult
from trpc_service.channels.contracts import ReplyIntent, ReplyKind
from trpc_service.reliability.types import (
    AppendDisposition,
    AuditData,
    ClaimInput,
    CommittedEvent,
    CommittedSessionView,
    EventAppend,
    EventData,
    FinalizeDisposition,
    FinalizeResult,
    ReplyPart,
    SessionClaim,
    StaleClaimError,
)
from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tenant.models import AgentAppSpec, ModelRoute, ToolPolicy
from trpc_service.worker.contracts import (
    EncryptedEventCodec,
    EventCodecContext,
    ResolvedTenantTurn,
)


def make_claim(**overrides: Any) -> SessionClaim:
    values: dict[str, Any] = {
        "tenant_id": "tenant-a",
        "session_id": "session-a",
        "inbox_id": "inbox-a",
        "run_id": "run-a",
        "worker_id": "worker-a",
        "fencing_token": 7,
        "attempt_no": 1,
        "expected_version": 0,
        "lease_expires_at": datetime.now(UTC) + timedelta(minutes=1),
        "request_id": "request-a",
        "trace_id": "trace-a",
    }
    values.update(overrides)
    return SessionClaim(**values)


def make_claim_input(claim: SessionClaim, **overrides: Any) -> ClaimInput:
    values: dict[str, Any] = {
        "tenant_id": claim.tenant_id,
        "session_id": claim.session_id,
        "inbox_id": claim.inbox_id,
        "run_id": claim.run_id,
        "binding_id": "binding-a",
        "app_id": "support",
        "app_revision": 3,
        "config_revision": 5,
        "scope": "private",
        "principal_id": "principal-a",
        "accepted_seq": 1,
        "external_delivery_id": "delivery-a",
        "payload": {"delivery_id": "delivery-a", "text": "hello"},
        "request_id": claim.request_id,
        "trace_id": claim.trace_id,
        "attempt_no": claim.attempt_no,
        "fencing_token": claim.fencing_token,
    }
    values.update(overrides)
    return ClaimInput(**values)


def make_view(claim: SessionClaim, **overrides: Any) -> CommittedSessionView:
    values: dict[str, Any] = {
        "tenant_id": claim.tenant_id,
        "session_id": claim.session_id,
        "state": {"counter": 1},
        "state_version": claim.expected_version,
        "log_version": claim.expected_version,
        "events": (),
    }
    values.update(overrides)
    return CommittedSessionView(**values)


def make_app() -> AgentAppSpec:
    return AgentAppSpec(
        app_id="support",
        revision=3,
        name="support_agent",
        prompt="Answer clearly.",
        model=ModelRoute(
            provider="fake",
            model="fake-model",
            timeout_seconds=2,
            token_ceiling=512,
        ),
        tools=ToolPolicy(),
    )


def make_resolved(claim_input: ClaimInput) -> ResolvedTenantTurn:
    return ResolvedTenantTurn(
        tenant_context=TenantContext(
            tenant_id=claim_input.tenant_id,
            app_id=claim_input.app_id,
            app_revision=claim_input.app_revision,
            binding_id=claim_input.binding_id,
            binding_revision=5,
            principal_id=claim_input.principal_id,
            session_id=claim_input.session_id,
            scope=ConversationScope.PRIVATE,
            request_id=claim_input.request_id,
            trace_id=claim_input.trace_id,
        ),
        app=make_app(),
        channel="telegram",
        config_revision=5,
        policy_revision=3,
    )


class MemoryEncryptedCodec(EncryptedEventCodec):
    """Opaque-reference codec double that enforces exact authenticated context."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[EventCodecContext, Event]] = {}
        self.seal_calls = 0
        self.open_calls = 0

    async def seal(self, event: Event, *, context: EventCodecContext) -> str:
        self.seal_calls += 1
        reference = f"enc-event://object-{self.seal_calls}"
        self._objects[reference] = (context, event.model_copy(deep=True))
        return reference

    async def open(self, content_ref: str, *, context: EventCodecContext) -> Event:
        self.open_calls += 1
        stored_context, event = self._objects[content_ref]
        if stored_context != context:
            raise ValueError("authenticated context mismatch")
        return event.model_copy(deep=True)


class FakeWorkerPort:
    """In-memory state machine that rejects stale fences and stale versions."""

    def __init__(
        self,
        *,
        claim: SessionClaim | None = None,
        claim_input: ClaimInput | None = None,
        view: CommittedSessionView | None = None,
    ) -> None:
        self.claim = claim
        self.claim_input = claim_input or (make_claim_input(claim) if claim else None)
        self.view = view or (make_view(claim) if claim else None)
        self._claimed = False
        self._lock = asyncio.Lock()
        self.events: list[EventData] = []
        self.aborted = 0
        self.renewals = 0
        self.lease_valid = True
        self.raise_stale_on_append = False
        self.raise_stale_on_defer = False
        self.deferred: list[tuple[datetime, str]] = []
        self.finalized: list[dict[str, Any]] = []

    async def claim_next(
        self,
        tenant_id: str,
        worker_id: str,
        *,
        lease_ttl: timedelta,
    ) -> SessionClaim | None:
        del lease_ttl
        async with self._lock:
            if self._claimed or self.claim is None or self.claim.tenant_id != tenant_id:
                return None
            self._claimed = True
            return SessionClaim(
                tenant_id=self.claim.tenant_id,
                session_id=self.claim.session_id,
                inbox_id=self.claim.inbox_id,
                run_id=self.claim.run_id,
                worker_id=worker_id,
                fencing_token=self.claim.fencing_token,
                attempt_no=self.claim.attempt_no,
                expected_version=self.claim.expected_version,
                lease_expires_at=self.claim.lease_expires_at,
                request_id=self.claim.request_id,
                trace_id=self.claim.trace_id,
            )

    async def load_claim_input(self, claim: SessionClaim) -> ClaimInput:
        self._check(claim)
        assert self.claim_input is not None
        return self.claim_input

    async def load_committed_session(self, claim: SessionClaim) -> CommittedSessionView:
        self._check(claim)
        assert self.view is not None
        return self.view

    async def renew_lease(
        self,
        claim: SessionClaim,
        *,
        lease_ttl: timedelta,
    ) -> bool:
        del lease_ttl
        self._check(claim)
        self.renewals += 1
        return self.lease_valid

    async def append_event_cas(
        self,
        claim: SessionClaim,
        expected_version: int,
        event: EventData,
    ) -> EventAppend:
        self._check(claim)
        if self.raise_stale_on_append:
            raise StaleClaimError("stale")
        actual_version = claim.expected_version + len(self.events)
        if expected_version != actual_version:
            raise AssertionError("fake observed stale expected version")
        self.events.append(event)
        version = actual_version + 1
        return EventAppend(AppendDisposition.APPENDED, event.event_id, version, version)

    async def abort_staged_events(self, claim: SessionClaim) -> int:
        self._check(claim)
        self.aborted += len(self.events)
        return len(self.events)

    async def defer_run_retry(
        self,
        claim: SessionClaim,
        *,
        next_attempt_at: datetime,
        error_type: str,
    ) -> None:
        self._check(claim)
        if self.raise_stale_on_defer:
            raise StaleClaimError("stale")
        self.aborted += len(self.events)
        self.deferred.append((next_attempt_at, error_type))

    async def finalize_run(
        self,
        claim: SessionClaim,
        *,
        final_state: dict[str, Any],
        final_event_id: str | None,
        reply_parts: tuple[ReplyPart, ...],
        audit: AuditData,
    ) -> FinalizeResult:
        self._check(claim)
        if not self.lease_valid:
            raise StaleClaimError("stale")
        self.finalized.append(
            {
                "state": deepcopy(final_state),
                "final_event_id": final_event_id,
                "reply_parts": reply_parts,
                "audit": audit,
            }
        )
        return FinalizeResult(
            FinalizeDisposition.FINALIZED,
            claim.run_id,
            claim.expected_version + len(self.events),
            ("outbox-a",),
        )

    def _check(self, claim: SessionClaim) -> None:
        if self.claim is None or (
            claim.tenant_id,
            claim.session_id,
            claim.run_id,
            claim.fencing_token,
        ) != (
            self.claim.tenant_id,
            self.claim.session_id,
            self.claim.run_id,
            self.claim.fencing_token,
        ):
            raise StaleClaimError("stale")


class StaticResolver:
    def __init__(self, resolved: ResolvedTenantTurn) -> None:
        self.resolved = resolved

    async def resolve(self, claim_input: ClaimInput) -> ResolvedTenantTurn:
        del claim_input
        return self.resolved


class FakeExecutor:
    def __init__(
        self,
        sessions: BaseSessionService,
        *,
        crash_after_first_append: bool = False,
        delay: float = 0,
    ) -> None:
        self._sessions = sessions
        self._crash = crash_after_first_append
        self._delay = delay

    async def run_turn(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        new_message: str | Content | list[Content],
        run_id: str,
        in_reply_to_delivery_id: str,
        attempt_no: int,
        approved_tools: frozenset[str],
        timeout_seconds: float | None = None,
    ) -> TurnResult:
        del new_message, attempt_no, approved_tools, timeout_seconds
        session = await self._sessions.get_session(
            app_name="tenant-app-a",
            user_id=tenant_context.principal_id,
            session_id=tenant_context.session_id,
        )
        user = Event(
            author="user",
            content=Content(role="user", parts=[Part.from_text(text="private prompt")]),
        )
        await self._sessions.append_event(session, user)
        if self._crash:
            raise ConnectionError("secret backend message")
        if self._delay:
            await asyncio.sleep(self._delay)
        assistant = Event(
            author=app.name,
            content=Content(role="model", parts=[Part.from_text(text="answer")]),
            turn_complete=True,
        )
        await self._sessions.append_event(session, assistant)
        intent = ReplyIntent(
            intent_id=f"{run_id}:reply:final",
            tenant_id=tenant_context.tenant_id,
            binding_id=tenant_context.binding_id,
            session_id=tenant_context.session_id,
            run_id=run_id,
            in_reply_to_delivery_id=in_reply_to_delivery_id,
            kind=ReplyKind.FINAL,
            text="answer",
            idempotency_key=f"{run_id}:reply:final",
        )
        return TurnResult(
            build=cast(Any, object()),
            framework_events=(assistant,),
            platform_events=(),
            reply_intent=intent,
            sdk_error=False,
        )


class FakeExecutorFactory:
    def __init__(self, *, crash: bool = False, delay: float = 0) -> None:
        self._crash = crash
        self._delay = delay

    def app_name_for(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        approved_tools: frozenset[str],
    ) -> str:
        del tenant_context, app, approved_tools
        return "tenant-app-a"

    def create(self, *, session_service: BaseSessionService) -> FakeExecutor:
        return FakeExecutor(
            session_service,
            crash_after_first_append=self._crash,
            delay=self._delay,
        )


def committed_event_from_append(event: EventData, *, seq: int) -> CommittedEvent:
    return CommittedEvent(
        seq=seq,
        event_id=event.event_id,
        event_type=event.event_type,
        role=event.role,
        content_ref=event.content_ref,
        payload=event.payload,
        state_delta=event.state_delta,
        framework_event_id=event.framework_event_id,
        created_at=datetime.now(UTC),
    )
