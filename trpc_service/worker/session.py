# mypy: disable-error-code="import-untyped"
"""Claim-bound tRPC-Agent SessionService with OCC and fencing on every event."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import BaseSessionService, ListSessionsResponse, Session, State

from trpc_service.reliability.types import CommittedSessionView, EventData, SessionClaim
from trpc_service.worker.contracts import EncryptedEventCodec, EventCodecContext, WorkerPort


class SessionIdentityError(PermissionError):
    """The SDK attempted to access a session outside the bound claim identity."""


class SessionReplayError(RuntimeError):
    """A committed event could not be authenticated or coherently replayed."""


class SessionServiceClosedError(RuntimeError):
    """A closed per-turn service was reused."""


@dataclass(frozen=True, slots=True)
class RuntimeEventView:
    """Minimal immutable metadata needed to authenticate a replayed SDK event."""

    seq: int
    event_id: str
    event_type: str
    role: str | None
    content_ref: str
    state_delta_json: str = field(repr=False)
    framework_event_id: str | None = None

    def state_delta_copy(self) -> dict[str, Any]:
        """Return a detached copy of the persisted normalized state delta."""

        value = json.loads(self.state_delta_json)
        if not isinstance(value, dict):  # pragma: no cover - constructor enforces this
            raise SessionReplayError("persisted state delta is not an object")
        return value


@dataclass(frozen=True, slots=True)
class RuntimeSessionView:
    """Read-only, claim-time replay snapshot.

    Mutable JSON values are stored canonically and exposed only as detached copies,
    so an SDK invocation cannot mutate the repository read view by aliasing a dict.
    """

    tenant_id: str
    session_id: str
    app_name: str
    user_id: str
    state_version: int
    log_version: int
    events: tuple[RuntimeEventView, ...]
    _session_state_json: str = field(repr=False)
    _app_state_json: str = field(repr=False, default="{}")
    _user_state_json: str = field(repr=False, default="{}")

    @classmethod
    def from_committed(
        cls,
        committed: CommittedSessionView,
        *,
        app_name: str,
        user_id: str,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> RuntimeSessionView:
        """Validate and detach the SQL read model before SDK reconstruction."""

        if not app_name or not user_id:
            raise ValueError("app_name and user_id must not be empty")
        if committed.state_version < 0 or committed.log_version < committed.state_version:
            raise SessionReplayError("session watermarks are inconsistent")

        previous_seq = 0
        runtime_events: list[RuntimeEventView] = []
        for event in committed.events:
            if event.seq <= previous_seq or event.seq > committed.state_version:
                raise SessionReplayError("committed event sequence is inconsistent")
            if not event.content_ref:
                raise SessionReplayError("committed SDK event has no encrypted content ref")
            runtime_events.append(
                RuntimeEventView(
                    seq=event.seq,
                    event_id=event.event_id,
                    event_type=event.event_type,
                    role=event.role,
                    content_ref=event.content_ref,
                    state_delta_json=_canonical_json(event.state_delta),
                    framework_event_id=event.framework_event_id,
                )
            )
            previous_seq = event.seq

        return cls(
            tenant_id=committed.tenant_id,
            session_id=committed.session_id,
            app_name=app_name,
            user_id=user_id,
            state_version=committed.state_version,
            log_version=committed.log_version,
            events=tuple(runtime_events),
            _session_state_json=_canonical_json(committed.state),
            _app_state_json=_canonical_json(app_state or {}),
            _user_state_json=_canonical_json(user_state or {}),
        )

    def session_state_copy(self) -> dict[str, Any]:
        return _json_object(self._session_state_json)

    def merged_state_copy(self) -> dict[str, Any]:
        state = self.session_state_copy()
        state.update(
            {f"{State.APP_PREFIX}{key}": value for key, value in self.app_state_copy().items()}
        )
        state.update(
            {f"{State.USER_PREFIX}{key}": value for key, value in self.user_state_copy().items()}
        )
        return state

    def app_state_copy(self) -> dict[str, Any]:
        return _json_object(self._app_state_json)

    def user_state_copy(self) -> dict[str, Any]:
        return _json_object(self._user_state_json)


class FencedSessionService(BaseSessionService):
    """A single-use SessionService bound to one live ``SessionClaim``.

    The SDK receives normal ``BaseSessionService`` behavior in memory.  Every
    non-partial event is then sealed by the injected codec and appended through the
    repository's CAS + fencing operation.  The service never lists or mutates any
    other tenant/user/session.
    """

    def __init__(
        self,
        *,
        port: WorkerPort,
        codec: EncryptedEventCodec,
        claim: SessionClaim,
        view: RuntimeSessionView,
    ) -> None:
        super().__init__()
        if view.tenant_id != claim.tenant_id or view.session_id != claim.session_id:
            raise SessionIdentityError("runtime view does not belong to the supplied claim")
        if view.log_version != claim.expected_version:
            raise SessionReplayError("runtime log version does not match the claim")
        self._port = port
        self._codec = codec
        self._claim = claim
        self._view = view
        self._expected_version = claim.expected_version
        self._append_ordinal = 0
        self._session: Session | None = None
        self._closed = False
        self._last_event_id: str | None = None

    @property
    def expected_version(self) -> int:
        """Current in-process OCC watermark after successful appends."""

        return self._expected_version

    @property
    def last_event_id(self) -> str | None:
        """Return the latest successfully fenced platform event id."""

        return self._last_event_id

    @property
    def initial_session_state(self) -> dict[str, Any]:
        """Return the committed pre-attempt state for safe rejection finalization."""

        return self._view.session_state_copy()

    @property
    def final_session_state(self) -> dict[str, Any]:
        """Return session-scope state only; app/user/temp prefixes are excluded."""

        if self._session is None:
            return self.initial_session_state
        return _session_scope(self._session.state)

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: dict[str, Any] | None = None,
        session_id: str | None = None,
        agent_context: AgentContext | None = None,
    ) -> Session:
        """Create only the already-authorized logical session when replay is empty."""

        del agent_context
        self._ensure_open()
        chosen_session_id = session_id.strip() if session_id else ""
        self._validate_identity(app_name, user_id, chosen_session_id)
        if self._session is not None:
            raise SessionIdentityError("claim-bound session already exists")
        if self._view.events or self._view.state_version:
            raise SessionReplayError("cannot create over a non-empty committed session")
        initial = self._view.merged_state_copy()
        if state:
            initial.update(json.loads(_canonical_json(state)))
        self._session = Session(
            id=self._view.session_id,
            app_name=self._view.app_name,
            user_id=self._view.user_id,
            state=initial,
            last_update_time=time.time(),
            save_key=f"{self._view.app_name}/{self._view.user_id}",
        )
        return self._session

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: AgentContext | None = None,
    ) -> Session:
        """Hydrate exactly one committed session and return the same turn-local object."""

        del agent_context
        self._ensure_open()
        self._validate_identity(app_name, user_id, session_id)
        if self._session is None:
            self._session = await self._restore_session()
        return self._session

    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: str | None = None,
    ) -> ListSessionsResponse:
        """Return at most the bound session; cross-user enumeration is forbidden."""

        self._ensure_open()
        if user_id is None:
            raise SessionIdentityError("claim-bound service forbids cross-user listing")
        self._validate_identity(app_name, user_id, self._view.session_id)
        session = await self.get_session(
            app_name=app_name,
            user_id=user_id,
            session_id=self._view.session_id,
        )
        metadata = Session(
            id=session.id,
            app_name=session.app_name,
            user_id=session.user_id,
            state={},
            last_update_time=session.last_update_time,
            save_key=session.save_key,
            conversation_count=session.conversation_count,
        )
        return ListSessionsResponse(sessions=[metadata])

    async def delete_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> None:
        """Reject deletion: a run claim grants append/finalize authority only."""

        self._ensure_open()
        self._validate_identity(app_name, user_id, session_id)
        raise SessionIdentityError("claim-bound service cannot delete sessions")

    async def append_event(self, session: Session, event: Event) -> Event:
        """Normalize in memory, seal content, then append using OCC + fencing."""

        self._ensure_open()
        self._validate_session_object(session)
        if event.partial:
            return event

        normalized = await super().append_event(session=session, event=event)
        self._append_ordinal += 1
        event_id = (
            f"{self._claim.run_id}:attempt:{self._claim.attempt_no}:"
            f"sdk-session:{self._append_ordinal:06d}"
        )
        next_seq = self._expected_version + 1
        context = EventCodecContext(
            tenant_id=self._claim.tenant_id,
            session_id=self._claim.session_id,
            event_id=event_id,
            seq=next_seq,
        )
        content_ref = await self._codec.seal(normalized, context=context)
        _validate_content_ref(content_ref)
        event_data = EventData(
            event_id=event_id,
            event_key=event_id,
            event_type=_event_type(normalized),
            payload=_safe_event_metadata(normalized),
            role=_event_role(normalized),
            content_ref=content_ref,
            state_delta=dict(normalized.actions.state_delta),
            framework_event_id=normalized.id or None,
        )
        appended = await self._port.append_event_cas(
            self._claim,
            self._expected_version,
            event_data,
        )
        if appended.version != next_seq or appended.seq != next_seq:
            raise SessionReplayError("append port returned an unexpected session version")
        self._expected_version = appended.version
        self._last_event_id = appended.event_id
        return normalized

    async def update_session(self, session: Session) -> None:
        """Validate the turn-local object; durable state is published by finalize only."""

        self._ensure_open()
        self._validate_session_object(session)
        # Canonicalization is a fail-closed serializability check.  It also ensures
        # later finalization receives a detached value rather than SDK-owned aliases.
        _canonical_json(_session_scope(session.state))

    async def close(self) -> None:
        """Make accidental cross-turn reuse fail closed."""

        self._closed = True

    async def _restore_session(self) -> Session:
        decoded: list[Event] = []
        last_update = 0.0
        for item in self._view.events:
            context = EventCodecContext(
                tenant_id=self._claim.tenant_id,
                session_id=self._claim.session_id,
                event_id=item.event_id,
                seq=item.seq,
            )
            try:
                event = await self._codec.open(item.content_ref, context=context)
            except Exception:
                raise SessionReplayError("encrypted session event could not be opened") from None
            _validate_replayed_event(item, event)
            decoded.append(event)
            last_update = max(last_update, event.timestamp)

        if not decoded:
            last_update = time.time()

        return Session(
            id=self._view.session_id,
            app_name=self._view.app_name,
            user_id=self._view.user_id,
            state=self._view.merged_state_copy(),
            last_update_time=last_update,
            save_key=f"{self._view.app_name}/{self._view.user_id}",
            conversation_count=sum(event.author == "user" for event in decoded),
            events=decoded,
        )

    def _validate_identity(self, app_name: str, user_id: str, session_id: str) -> None:
        if (
            app_name != self._view.app_name
            or user_id != self._view.user_id
            or session_id != self._view.session_id
        ):
            raise SessionIdentityError("requested SDK identity is outside the bound claim")

    def _validate_session_object(self, session: Session) -> None:
        self._validate_identity(session.app_name, session.user_id, session.id)
        if session is not self._session:
            raise SessionIdentityError("foreign Session object is not authorized by this claim")

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionServiceClosedError("claim-bound SessionService is closed")


def _validate_replayed_event(item: RuntimeEventView, event: Event) -> None:
    if event.partial:
        raise SessionReplayError("partial events must never appear in committed replay")
    if item.framework_event_id is not None and event.id != item.framework_event_id:
        raise SessionReplayError("encrypted event identity differs from committed metadata")
    if _event_type(event) != item.event_type or _event_role(event) != item.role:
        raise SessionReplayError("encrypted event shape differs from committed metadata")
    if dict(event.actions.state_delta) != item.state_delta_copy():
        raise SessionReplayError("encrypted event state differs from committed metadata")
    if any(key.startswith(State.TEMP_PREFIX) for key in event.actions.state_delta):
        raise SessionReplayError("temporary state appeared in committed replay")


def _safe_event_metadata(event: Event) -> dict[str, Any]:
    """Build allow-listed metadata; full content stays behind ``content_ref``."""

    serialized = event.model_dump_json(by_alias=True, exclude_none=True).encode()
    function_calls = event.get_function_calls()
    function_responses = event.get_function_responses()
    payload: dict[str, Any] = {
        "content_sha256": hashlib.sha256(serialized).hexdigest(),
        "has_content": event.content is not None,
        "part_count": len(event.content.parts) if event.content is not None else 0,
        "tool_call_count": len(function_calls),
        "tool_response_count": len(function_responses),
        "is_error": event.is_error(),
        "visible": event.visible,
    }
    if event.usage_metadata is not None:
        usage = event.usage_metadata.model_dump(mode="json", exclude_none=True)
        payload["usage"] = {
            key: value
            for key, value in usage.items()
            if key
            in {
                "cached_content_token_count",
                "candidates_token_count",
                "prompt_token_count",
                "thoughts_token_count",
                "tool_use_prompt_token_count",
                "total_token_count",
            }
        }
    return payload


def _event_type(event: Event) -> str:
    if event.is_error():
        return "error"
    if event.get_function_calls():
        return "tool_call"
    if event.get_function_responses():
        return "tool_result"
    if event.content is not None and event.content.role == "user":
        return "user"
    if event.is_final_response():
        return "assistant"
    return "framework"


def _event_role(event: Event) -> str | None:
    if event.content is None or event.content.role is None:
        return None
    return "assistant" if event.content.role == "model" else event.content.role


def _session_scope(state: dict[str, Any]) -> dict[str, Any]:
    scoped = {
        key: value
        for key, value in state.items()
        if not key.startswith((State.APP_PREFIX, State.USER_PREFIX, State.TEMP_PREFIX))
    }
    return _json_object(_canonical_json(scoped))


def _validate_content_ref(content_ref: str) -> None:
    if not content_ref or len(content_ref) > 2_048 or any(char.isspace() for char in content_ref):
        raise SessionReplayError("event codec returned an invalid opaque reference")


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError):
        raise SessionReplayError("session data is not canonical JSON") from None


def _json_object(value: str) -> dict[str, Any]:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise SessionReplayError("session state is not an object")
    return decoded
