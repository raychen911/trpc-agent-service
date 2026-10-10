"""Per-turn tRPC-Agent SessionService that buffers all mutations."""

from __future__ import annotations

import time
from typing import Any

from trpc_agent_sdk.abc import ListSessionsResponse
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import BaseSessionService, Session
from trpc_agent_sdk.types import Content, Part

from trpc_service.storage.models import SessionSnapshot, StoredEvent


class TurnBufferSessionService(BaseSessionService):
    """Expose an SDK session while deferring persistence until fenced commit."""

    def __init__(self, snapshot: SessionSnapshot) -> None:
        super().__init__(summarizer_manager=None)
        self._snapshot = snapshot
        self._session: Session | None = None
        self._buffer: list[StoredEvent] = []

    @property
    def buffered_events(self) -> tuple[StoredEvent, ...]:
        return tuple(self._buffer)

    @property
    def state(self) -> dict[str, Any]:
        state = dict(self._session.state) if self._session else dict(self._snapshot.state)
        return {key: value for key, value in state.items() if not key.startswith("temp:")}

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: dict[str, Any] | None = None,
        session_id: str | None = None,
        agent_context: AgentContext | None = None,
    ) -> Session:
        session_id = session_id or self._snapshot.session_id
        if session_id != self._snapshot.session_id:
            raise ValueError("runner attempted to create an unexpected session")
        self._session = Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=dict(state or self._snapshot.state),
            events=[],
            last_update_time=time.time(),
            save_key=f"{app_name}:{user_id}",
        )
        return self._session.model_copy(deep=True)

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: AgentContext | None = None,
    ) -> Session | None:
        if session_id != self._snapshot.session_id:
            return None
        if self._session is None:
            events = [Event.model_validate(item.event) for item in self._snapshot.events]
            last_update = max((event.timestamp for event in events), default=time.time())
            self._session = Session(
                id=session_id,
                app_name=app_name,
                user_id=user_id,
                state=dict(self._snapshot.state),
                events=events,
                last_update_time=last_update,
                save_key=f"{app_name}:{user_id}",
            )
        return self.filter_events(self._session, need_copy=True)

    async def list_sessions(
        self, *, app_name: str, user_id: str | None = None
    ) -> ListSessionsResponse:
        if self._session is None:
            return ListSessionsResponse()
        if user_id is not None and user_id != self._session.user_id:
            return ListSessionsResponse()
        item = self._session.model_copy(deep=True)
        item.events = []
        item.historical_events = []
        return ListSessionsResponse(sessions=[item])

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        if self._session and self._session.id == session_id:
            self._session = None
            self._buffer.clear()

    async def append_event(self, session: Session, event: Event) -> Event:
        if event.partial:
            return event
        event = await super().append_event(session, event)
        self._session = session
        state_delta = dict(event.actions.state_delta) if event.actions else {}
        self._buffer.append(
            StoredEvent(
                event_id=event.id,
                author=event.author,
                timestamp=event.timestamp,
                event=_event_for_storage(event).model_dump(mode="json", by_alias=True),
                state_delta=state_delta,
            )
        )
        return event

    async def update_session(self, session: Session) -> None:
        self._session = session


def _event_for_storage(event: Event) -> Event:
    """Keep binary model input in memory for this turn, not in PostgreSQL events."""

    if event.content is None or not event.content.parts:
        return event
    parts: list[Part] = []
    changed = False
    for part in event.content.parts:
        if part.inline_data is None:
            parts.append(part)
            continue
        changed = True
        mime_type = part.inline_data.mime_type or "application/octet-stream"
        parts.append(Part(text=f"[{mime_type} media persisted as a tenant artifact]"))
    if not changed:
        return event
    content = Content(role=event.content.role, parts=parts)
    return event.model_copy(update={"content": content}, deep=True)


__all__ = ["TurnBufferSessionService"]
