# mypy: disable-error-code="import-untyped"
"""Fencing, identity, normalization, and encrypted replay tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.types import Content, EventActions, Part

from trpc_service.reliability import CommittedEvent
from trpc_service.worker import (
    FencedSessionService,
    RuntimeSessionView,
    SessionIdentityError,
    SessionReplayError,
    SessionServiceClosedError,
)

from .helpers import (
    FakeWorkerPort,
    MemoryEncryptedCodec,
    committed_event_from_append,
    make_claim,
    make_view,
)


@pytest.mark.asyncio
async def test_every_non_partial_event_is_normalized_sealed_and_fenced() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    codec = MemoryEncryptedCodec()
    service = FencedSessionService(
        port=port,
        codec=codec,
        claim=claim,
        view=RuntimeSessionView.from_committed(
            make_view(claim),
            app_name="tenant-app-a",
            user_id="principal-a",
            app_state={"shared": 2},
            user_state={"preference": "brief"},
        ),
    )
    session = await service.get_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
    )
    partial = Event(
        partial=True,
        content=Content(role="model", parts=[Part.from_text(text="sec")]),
    )
    await service.append_event(session, partial)
    assert codec.seal_calls == 0
    assert not port.events

    complete = Event(
        id="sdk-event-a",
        author="support_agent",
        content=Content(role="model", parts=[Part.from_text(text="secret answer")]),
        actions=EventActions(
            state_delta={
                "counter": 2,
                "temp:scratch": "never-store",
                "app:shared": 3,
                "user:preference": "long",
            }
        ),
        turn_complete=True,
    )
    await service.append_event(session, complete)

    assert codec.seal_calls == 1
    assert service.expected_version == 1
    assert service.final_session_state == {"counter": 2}
    assert "temp:scratch" in session.state, "temp state remains visible in this invocation"
    stored = port.events[0]
    assert stored.content_ref == "enc-event://object-1"
    assert stored.state_delta == {
        "counter": 2,
        "app:shared": 3,
        "user:preference": "long",
    }
    assert "secret answer" not in str(stored.payload)
    assert "never-store" not in str(stored)


@pytest.mark.asyncio
async def test_identity_is_strict_and_foreign_session_objects_are_rejected() -> None:
    claim = make_claim()
    service = FencedSessionService(
        port=FakeWorkerPort(claim=claim),
        codec=MemoryEncryptedCodec(),
        claim=claim,
        view=RuntimeSessionView.from_committed(
            make_view(claim),
            app_name="tenant-app-a",
            user_id="principal-a",
        ),
    )
    with pytest.raises(SessionIdentityError):
        await service.get_session(
            app_name="tenant-app-a",
            user_id="other-user",
            session_id="session-a",
        )
    with pytest.raises(SessionIdentityError):
        await service.list_sessions(app_name="tenant-app-a", user_id=None)
    with pytest.raises(SessionIdentityError):
        await service.delete_session(
            app_name="tenant-app-a",
            user_id="principal-a",
            session_id="other-session",
        )


@pytest.mark.asyncio
async def test_committed_event_round_trips_through_authenticated_codec() -> None:
    first_claim = make_claim()
    first_port = FakeWorkerPort(claim=first_claim)
    codec = MemoryEncryptedCodec()
    first_service = FencedSessionService(
        port=first_port,
        codec=codec,
        claim=first_claim,
        view=RuntimeSessionView.from_committed(
            make_view(first_claim, state={}),
            app_name="tenant-app-a",
            user_id="principal-a",
        ),
    )
    session = await first_service.get_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
    )
    await first_service.append_event(
        session,
        Event(
            id="framework-a",
            author="user",
            content=Content(role="user", parts=[Part.from_text(text="private prompt")]),
        ),
    )

    committed = committed_event_from_append(first_port.events[0], seq=1)
    second_claim = make_claim(
        run_id="run-b",
        inbox_id="inbox-b",
        fencing_token=8,
        expected_version=1,
    )
    second_service = FencedSessionService(
        port=FakeWorkerPort(claim=second_claim),
        codec=codec,
        claim=second_claim,
        view=RuntimeSessionView.from_committed(
            make_view(
                second_claim,
                state={},
                state_version=1,
                log_version=1,
                events=(committed,),
            ),
            app_name="tenant-app-a",
            user_id="principal-a",
        ),
    )
    restored = await second_service.get_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
    )
    assert codec.open_calls == 1
    assert [event.get_text() for event in restored.events] == ["private prompt"]


@pytest.mark.asyncio
async def test_claim_bound_session_lifecycle_is_single_session_and_single_use() -> None:
    claim = make_claim()
    service = FencedSessionService(
        port=FakeWorkerPort(claim=claim),
        codec=MemoryEncryptedCodec(),
        claim=claim,
        view=RuntimeSessionView.from_committed(
            make_view(claim, state={"existing": 1}),
            app_name="tenant-app-a",
            user_id="principal-a",
            app_state={"shared": 2},
            user_state={"preference": "brief"},
        ),
    )
    assert service.initial_session_state == {"existing": 1}
    assert service.final_session_state == {"existing": 1}
    session = await service.create_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
        state={"created": True},
    )
    assert session.state == {
        "existing": 1,
        "app:shared": 2,
        "user:preference": "brief",
        "created": True,
    }
    listed = await service.list_sessions(
        app_name="tenant-app-a",
        user_id="principal-a",
    )
    assert len(listed.sessions) == 1
    assert listed.sessions[0].state == {}
    await service.update_session(session)
    assert service.final_session_state == {"existing": 1, "created": True}
    with pytest.raises(SessionIdentityError, match="already exists"):
        await service.create_session(
            app_name="tenant-app-a",
            user_id="principal-a",
            session_id="session-a",
        )
    with pytest.raises(SessionIdentityError, match="cannot delete"):
        await service.delete_session(
            app_name="tenant-app-a",
            user_id="principal-a",
            session_id="session-a",
        )
    foreign = Session(
        id=session.id,
        app_name=session.app_name,
        user_id=session.user_id,
        save_key=session.save_key,
    )
    with pytest.raises(SessionIdentityError, match="foreign"):
        await service.append_event(foreign, Event(author="user"))
    await service.close()
    with pytest.raises(SessionServiceClosedError):
        await service.get_session(
            app_name="tenant-app-a",
            user_id="principal-a",
            session_id="session-a",
        )


def test_runtime_view_and_claim_watermarks_fail_closed() -> None:
    claim = make_claim()
    base = make_view(claim)
    with pytest.raises(ValueError, match="app_name"):
        RuntimeSessionView.from_committed(base, app_name="", user_id="principal-a")
    with pytest.raises(SessionReplayError, match="watermarks"):
        RuntimeSessionView.from_committed(
            replace(base, state_version=-1),
            app_name="tenant-app-a",
            user_id="principal-a",
        )
    event = CommittedEvent(
        seq=1,
        event_id="event-a",
        event_type="user",
        role="user",
        content_ref="enc-event://a",
        payload={},
        state_delta={},
        framework_event_id="sdk-a",
        created_at=datetime.now(UTC),
    )
    with pytest.raises(SessionReplayError, match="sequence"):
        RuntimeSessionView.from_committed(
            replace(base, events=(event,)),
            app_name="tenant-app-a",
            user_id="principal-a",
        )
    with pytest.raises(SessionReplayError, match="content ref"):
        RuntimeSessionView.from_committed(
            replace(
                base,
                state_version=1,
                log_version=1,
                events=(replace(event, content_ref=None),),
            ),
            app_name="tenant-app-a",
            user_id="principal-a",
        )
    view = RuntimeSessionView.from_committed(
        base,
        app_name="tenant-app-a",
        user_id="principal-a",
    )
    with pytest.raises(SessionIdentityError, match="runtime view"):
        FencedSessionService(
            port=FakeWorkerPort(claim=claim),
            codec=MemoryEncryptedCodec(),
            claim=replace(claim, session_id="other-session"),
            view=view,
        )
    with pytest.raises(SessionReplayError, match="log version"):
        FencedSessionService(
            port=FakeWorkerPort(claim=claim),
            codec=MemoryEncryptedCodec(),
            claim=replace(claim, expected_version=1),
            view=view,
        )
